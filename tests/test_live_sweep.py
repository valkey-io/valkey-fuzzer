"""``valkey-fuzzer sweep`` against real servers: a SIGKILLed fuzzer's leftovers go, nothing else does.

An untagged valkey-server and the servers and directory of a live fuzzer (this
test process) must survive. Chains and tmpfs mounts need Linux and root.
"""

import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
import valkey
import yaml

from tests.live_support import BINARY, free_port_block, scenario
from valkey_fuzzer import hoststate
from valkey_fuzzer.deployment.local.models import DeploymentConfig

pytestmark = [pytest.mark.live, pytest.mark.slow]
REPOSITORY = str(Path(__file__).resolve().parents[1])

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)


def _sweep(*argv, **kwargs):
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, (REPOSITORY, os.environ.get("PYTHONPATH"))))}
    return subprocess.Popen([sys.executable, "-m", "valkey_fuzzer.cli", *argv], env=env, cwd=REPOSITORY, **kwargs)


def _report(*argv):
    process = _sweep("sweep", *argv, "--format", "json", stdout=subprocess.PIPE)
    report = json.loads(process.communicate(timeout=60)[0])
    assert process.returncode == 0, report
    return report


def _servers_of(pid):
    owners = ((found, hoststate.server_owner(command)) for found, _, command in hoststate.own_processes())
    return [found for found, owner in owners if owner is not None and owner.pid == pid]


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _pings(port):
    try:
        with valkey.Valkey(port=port, socket_timeout=2) as client:
            return client.ping()
    except valkey.ValkeyError:
        return False


def _server(port, directory, *arguments):
    command = [BINARY, "--port", str(port), "--bind", "127.0.0.1", "--dir", str(directory), "--save", "", *arguments]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 15
    while not _pings(port):
        assert process.poll() is None and time.monotonic() < deadline, "server did not start"
        time.sleep(0.1)
    return process, port


def test_a_killed_fuzzer_is_swept_and_nothing_else_is(tmp_path, tmp_path_factory):
    # Logs stay under <base_data_dir>/logs after a run, so a live test's deployments default to a temporary one.
    assert os.path.commonpath([DeploymentConfig().base_data_dir, str(tmp_path_factory.getbasetemp())]) == str(
        tmp_path_factory.getbasetemp()
    )
    root = tmp_path / "data"
    untagged_dir, live_dir = root / "untagged", root / "live-run"
    untagged_dir.mkdir(parents=True)
    live_dir.mkdir()
    hoststate.tag_directory(str(live_dir), "deployment", "this test's own live run")
    survivors = [
        _server(free_port_block(1), untagged_dir),
        _server(free_port_block(1), live_dir, *hoststate.server_tag_arguments()),
    ]
    fuzzer = None
    try:
        # A long write workload: the fuzzer is still running when it is killed.
        run = scenario(
            "valkey.snapshot-equivalence", inputs={"operations": 1000000, "mutation_rate": 50}, replicas=1, seed=5
        )
        run["deployment"].update(base_port=free_port_block(3), base_data_dir=str(root))
        (tmp_path / "scenario.yaml").write_text(yaml.safe_dump(run))
        fuzzer = _sweep(
            "run",
            str(tmp_path / "scenario.yaml"),
            "--artifact-dir",
            str(tmp_path / "reports"),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 60
        while len(_servers_of(fuzzer.pid)) < 2:
            assert fuzzer.poll() is None and time.monotonic() < deadline, "the fuzzer's servers did not start"
            time.sleep(0.1)
        servers = _servers_of(fuzzer.pid)
        deployments = sorted(path for path in root.glob("deployment-*") if hoststate.read_tag(str(path)))
        assert deployments, "the fuzzer tags its deployment directory"
        fuzzer.send_signal(signal.SIGKILL)
        fuzzer.wait()
        assert all(_alive(pid) for pid in servers), "SIGKILL leaves the servers behind"

        listed = _report("--root", str(root), "--dry-run")
        assert all(_alive(pid) for pid in servers) and all(path.exists() for path in deployments)
        mine = [
            item for item in _report("--root", str(root))["leftovers"] if item["owner"].startswith(f"{fuzzer.pid}-")
        ]

        assert sorted(int(item["target"]) for item in mine if item["kind"] == "server") == sorted(servers)
        assert sorted(item["target"] for item in mine if item["kind"] == "directory") == [str(p) for p in deployments]
        assert {item["action"] for item in mine} == {"removed"}
        assert {(item["kind"], item["target"]) for item in listed["leftovers"]} >= {
            (item["kind"], item["target"]) for item in mine
        }
        assert not any(_alive(pid) for pid in servers) and not any(path.exists() for path in deployments)
        # What the sweep must never touch.
        assert all(process.poll() is None and _pings(port) for process, port in survivors)
        assert untagged_dir.exists() and hoststate.read_tag(str(live_dir)) and (root / "logs").exists()
        again = _report("--root", str(root))["leftovers"]
        assert not [item for item in again if item["owner"].startswith(f"{fuzzer.pid}-")]
    finally:
        if fuzzer is not None and fuzzer.poll() is None:
            fuzzer.kill()
        for process, _ in survivors:
            process.kill()
            process.wait()
        for pid in _servers_of(fuzzer.pid) if fuzzer is not None else ():
            os.kill(pid, signal.SIGKILL)


@pytest.mark.skipif(
    platform.system() != "Linux" or os.geteuid() != 0 or not shutil.which("iptables"),
    reason="iptables chains and tmpfs mounts need Linux and root",
)
def test_a_dead_owners_chain_and_mount_are_swept_as_root(tmp_path):
    finished = subprocess.Popen(["true"])
    finished.wait()
    dead = hoststate.Owner(finished.pid, hoststate.current_owner().start)
    chain, live_chain = f"{hoststate.CHAIN_PREFIX}{dead.token}-01234567", hoststate.chain_name("live")
    mount_point = Path(hoststate.mount_parent()).parent / dead.token / "valkey-fuzzer-tmpfs-test"
    mount_point.mkdir(parents=True)
    try:
        for name in (chain, live_chain):
            assert hoststate.run_iptables("-N", name).returncode == 0
            assert hoststate.run_iptables("-I", "INPUT", "1", "-j", name).returncode == 0
        assert subprocess.run(["mount", "-t", "tmpfs", "-o", "size=1m", "tmpfs", str(mount_point)]).returncode == 0

        report = _report("--root", str(tmp_path))

        removed = {(item["kind"], item["target"]) for item in report["leftovers"] if item["action"] == "removed"}
        assert {("chain", chain), ("mount", str(mount_point))} <= removed and not mount_point.exists()
        assert hoststate.run_iptables("-S", chain).returncode != 0
        assert hoststate.run_iptables("-S", live_chain).returncode == 0
    finally:
        hoststate.remove_chain(live_chain)
        hoststate.remove_chain(chain)
        if os.path.ismount(mount_point):
            subprocess.run(["umount", str(mount_point)])
        shutil.rmtree(mount_point.parent, ignore_errors=True)
        hoststate.release_mount_parent(os.path.join(hoststate.mount_parent(), "x"))
