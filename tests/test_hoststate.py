"""Host-state tags and ``valkey-fuzzer sweep``, with the host faked.

The sweep removes what a dead fuzzer process tagged and nothing else: not an
untagged server or directory, not what a live fuzzer owns, not a process that
merely mentions the tag. tests/test_live_sweep.py checks it against real servers.
"""

import json
import os
import signal
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest

from valkey_fuzzer import hoststate
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.deployment.local import processes
from valkey_fuzzer.deployment.local.models import DeploymentConfig
from valkey_fuzzer.deployment.local.processes import ProcessManager
from valkey_fuzzer.deployment.local.server_config import normalize_server_option
from valkey_fuzzer.hoststate import Owner, current_owner, sweep

DEAD, OTHER = Owner(999_999, "0a1b2c"), Owner(999_998, "0a1b2c")


@pytest.fixture
def host(monkeypatch):
    """A fake host: processes, kill signals, and mounts; ``dead`` owners are not running."""

    state = SimpleNamespace(processes=[], killed=[], mounts=[], unmounted=[], dead={DEAD.pid, OTHER.pid})

    def kill(pid, sig):
        if sig == signal.SIGKILL:
            state.killed.append(pid)
            state.processes = [item for item in state.processes if item[0] != pid]
        elif not any(item[0] == pid for item in state.processes):
            raise ProcessLookupError(pid)

    def run(command, **kwargs):
        if command[0] == "umount":
            state.unmounted.append(command[1])
            return subprocess.CompletedProcess(command, 0, "", "")
        lines = [line for found, _, line in state.processes if found == int(command[-1])]  # ps -ww -p PID
        return subprocess.CompletedProcess(command, 0 if lines else 1, "\n".join(lines), "")

    monkeypatch.setattr(Owner, "alive", lambda owner: owner.pid not in state.dead)
    monkeypatch.setattr(hoststate.os, "kill", kill)
    monkeypatch.setattr(hoststate, "run_bounded", run)
    monkeypatch.setattr(hoststate, "own_processes", lambda: list(state.processes))
    monkeypatch.setattr(hoststate, "list_mounts", lambda: list(state.mounts))
    monkeypatch.setattr(hoststate.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(hoststate, "_is_root", lambda: False)
    return state


def _title(owner, port, binary="/opt/valkey/bin/valkey-server"):
    return f"{binary} 127.0.0.1:{port}  {hoststate.TITLE_TAG}{owner.token}"


def _tagged_dir(root, name, owner):
    (root / name).mkdir()
    tag = {"owner": owner.token, "pid": owner.pid, "kind": "deployment", "label": name, "created_at": 1.0}
    (root / name / hoststate.OWNER_FILE).write_text(json.dumps(tag))
    return root / name


def _found(report):
    return [(item.kind, item.target, item.action) for item in report.leftovers]


def test_an_owner_is_alive_only_while_its_process_runs_with_the_same_start():
    me = current_owner()
    assert me.pid == os.getpid() and me.alive() and Owner.parse(me.token) == me
    # A reused PID: running, but started at another time.
    assert not Owner(os.getpid(), "ffffff" if me.start != "ffffff" else "eeeeee").alive()
    assert Owner(os.getpid(), "000000").alive(), "an unknown start time decides nothing"
    finished = subprocess.Popen(["true"])
    finished.wait()
    assert not Owner(finished.pid, me.start).alive()
    assert Owner.parse("12-xyz") is None and Owner.parse(None) is None


def test_importing_the_host_state_module_runs_nothing():
    # Outside Linux an owner's start time comes from ps: only a process that tags something should pay for it.
    code = "import valkey_fuzzer.hoststate as hoststate; print(len(hoststate._CURRENT))"
    assert subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout == "0\n"


def test_an_owner_is_the_same_under_any_time_zone(monkeypatch):
    # Outside Linux the start time must not be `ps -o lstart` in local time: a sweep run under another TZ would
    # take a live owner for a dead one and remove what it owns.
    monkeypatch.setitem(os.environ, "TZ", "UTC")
    started = hoststate.process_start(os.getpid())
    monkeypatch.setitem(os.environ, "TZ", "Asia/Kolkata")
    assert started is not None and hoststate.process_start(os.getpid()) == started
    assert current_owner().alive()


def test_servers_chains_and_mounts_name_their_owner_and_scenarios_cannot_replace_the_tag(tmp_path, monkeypatch):
    owner = Owner(4242, "abcdef")
    assert hoststate.server_owner(_title(owner, 7000, binary="/usr/bin/redis-server")) == owner
    # With set-proc-title no, the tag is still in the arguments.
    assert hoststate.server_owner(" ".join(["valkey-server", *hoststate.server_tag_arguments()])) == current_owner()
    # A save or rewrite child carries its parent's title, tag included, and outlives a SIGKILLed parent: taken
    # for no server, it would be left writing temp-<pid>.rdb into a removed directory.
    for child in ("valkey-rdb-bgsave", "valkey-aof-rewrite", "valkey-rdb-to-replicas", "redis-rdb-to-slaves",
                  "valkey-module-fork", "valkey-slot-migration-to-target"):  # fmt: skip
        assert hoststate.server_owner(f"{child} 127.0.0.1:7000  {hoststate.TITLE_TAG}{owner.token}") == owner, child
    for command in (
        "/usr/bin/valkey-server 127.0.0.1:7001",
        f"/usr/bin/valkey-server *:7000 {hoststate.TITLE_TAG}4242-abcdefX",
    ):
        assert hoststate.server_owner(command) is None, command
    with pytest.raises(ValueError, match="managed by the local deployment"):
        normalize_server_option("proc-title-template")
    monkeypatch.setattr(hoststate.tempfile, "gettempdir", lambda: str(tmp_path))
    parent = hoststate.mount_parent()
    assert hoststate.mount_owner(os.path.join(parent, "valkey-fuzzer-tmpfs-x")) == current_owner()
    assert hoststate.mount_owner("/mnt/valkey-fuzzer-tmpfs-x") is None
    assert hoststate.mount_owner(f"/elsewhere/{current_owner().token}/x") is None
    hoststate.release_mount_parent(os.path.join(parent, "x"))
    assert not os.path.exists(parent)
    monkeypatch.setitem(hoststate._CURRENT, os.getpid(), Owner(4_194_304, "abcdef"))
    name = hoststate.chain_name("component/" + "x" * 200)
    assert name.startswith("VF-4194304-abcdef-") and len(name) <= 28 and name != hoststate.chain_name("b")


def test_the_sweep_kills_only_old_enough_tagged_servers_of_gone_owners(host, tmp_path, monkeypatch):
    host.processes = [
        (101, 500.0, _title(DEAD, 7000)),
        (102, 5.0, _title(DEAD, 7001)),  # too young
        (103, 500.0, "/usr/bin/valkey-server 127.0.0.1:7100"),  # untagged
        (104, 500.0, _title(current_owner(), 7200)),  # a live fuzzer's
        (105, 500.0, f"grep {hoststate.TITLE_TAG}{DEAD.token}"),  # not a server, though it mentions the tag
    ]
    monkeypatch.setattr(hoststate.time, "time", lambda: 1001.0)
    assert host.killed == [] and {action for _, _, action in _found(sweep([str(tmp_path)], dry_run=True))} == {
        "would remove"
    }

    report = sweep([str(tmp_path)], older_than=60)

    assert host.killed == [101] and report.ok
    assert {(item.target, item.action, item.reason) for item in report.leftovers} == {
        ("101", "removed", ""),
        ("102", "skipped", "younger than 60s"),
    }
    # A server whose PID was reused since the listing is not killed.
    listed = [(106, 500.0, _title(DEAD, 7002))]
    host.processes = [(106, 1.0, "/usr/bin/python3 something-else")]
    monkeypatch.setattr(hoststate, "own_processes", lambda: listed)
    sweep([str(tmp_path)])
    assert host.killed == [101]


def test_the_sweep_acts_only_on_this_users_processes_in_this_pid_namespace(monkeypatch):
    # Run as root, the sweep judged a container fuzzer's PID in its own namespace (dead, or reused) and killed
    # every user's live tagged servers.
    listing = "\n".join(
        [
            f"  10   500    08:20 {_title(DEAD, 7000)}",
            f"  11     0    08:20 {_title(DEAD, 7001)}",  # another user's
            f"  12   500    08:20 {_title(DEAD, 7002)}",  # in a container's PID namespace
            "  bad",
        ]
    )
    monkeypatch.setattr(hoststate.os, "geteuid", lambda: 500)
    monkeypatch.setattr(
        hoststate, "run_bounded", lambda command, **_: subprocess.CompletedProcess(command, 0, listing, "")
    )
    monkeypatch.setattr(hoststate, "_pid_namespace", lambda pid: "pid:[2]" if pid == 12 else "pid:[1]")
    assert hoststate.own_processes() == [(10, 500.0, _title(DEAD, 7000))]


def test_the_sweep_removes_only_tagged_directories_of_gone_owners_on_one_filesystem(host, tmp_path, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    dead = _tagged_dir(root, "deployment-dead", DEAD)
    (dead / "valkey-0" / "data").mkdir(parents=True)
    os.chmod(dead / "valkey-0" / "data", 0o500)  # A deny-writes fault left the data directory read-only.
    kept = [
        _tagged_dir(root, "deployment-live", current_owner()),
        _tagged_dir(tmp_path, "outside-the-root", DEAD),
        _tagged_dir(root, "deployment-of-another-run", OTHER),
        root / "deployment-untagged",
    ]
    kept[-1].mkdir()
    (root / "link-to-elsewhere").symlink_to(kept[1])

    # A batch worker's sweep (``owners``) leaves what another dead owner left.
    report = sweep([str(root)], owners={DEAD.token})

    assert _found(report) == [("directory", str(dead), "removed")] and not dead.exists()
    assert all(path.exists() for path in kept) and report.leftovers[0].detail == "deployment deployment-dead"
    # A directory a new owner tagged between the listing and the removal is that owner's now.
    taken = _tagged_dir(root, "deployment-taken-over", DEAD)
    listed = hoststate._find_directories

    def take_over(*args):
        found = listed(*args)
        hoststate.tag_directory(str(taken), "deployment")
        return found

    monkeypatch.setattr(hoststate, "_find_directories", take_over)
    report = sweep([str(root)], owners={DEAD.token})
    assert taken.exists() and hoststate.read_tag(str(taken))["owner"] == current_owner().token
    assert _found(report) == [("directory", str(taken), "skipped")]
    (kept[2] / "mounted").mkdir()
    real_open, real_fstat, mounted = os.open, os.fstat, set()

    def open_(path, *args, **kwargs):
        handle = real_open(path, *args, **kwargs)
        if str(path).endswith("mounted"):
            mounted.add(handle)
        return handle

    def fstat(handle):
        info = real_fstat(handle)
        return (
            os.stat_result((info.st_mode, info.st_ino, info.st_dev + 1, *tuple(info)[3:]))
            if handle in mounted
            else info
        )

    monkeypatch.setattr(hoststate.os, "open", open_)
    monkeypatch.setattr(hoststate.os, "fstat", fstat)
    (leftover,) = sweep([str(root)]).leftovers
    assert kept[2].exists() and leftover.action == "failed" and "another filesystem" in leftover.reason


# 0o500: what a deny-writes fault leaves, opened as it is; 0o077: unreadable, so made readable first.
@pytest.mark.parametrize("mode", [0o500, 0o077])
def test_the_sweep_never_chmods_through_a_symlink_and_leaves_other_users_tags(host, tmp_path, monkeypatch, mode):
    victim = tmp_path / "shadow"  # Stands in for /etc/shadow.
    victim.write_text("secret\n")
    victim.chmod(0o600)
    root = tmp_path / "root"
    root.mkdir()
    forged = _tagged_dir(root, "forged", DEAD)
    sub = forged / "sub"
    sub.mkdir()
    sub.chmod(mode)
    real_lstat, real_open, swapped = os.lstat, os.open, []

    def swap_after(call):
        # Whatever the sweep first does to "sub", the subdirectory is swapped for a symlink right after it.
        def looked(path, *args, **kwargs):
            result = call(path, *args, **kwargs)
            if not swapped and os.path.basename(str(path)) == "sub":
                swapped.append(path)
                os.rename(sub, forged / "sub.gone")
                os.symlink(victim, sub)
            return result

        return looked

    # Its own patch context, undone on its own: the host stays faked for the second sweep below.
    with pytest.MonkeyPatch.context() as racing:
        racing.setattr(hoststate.os, "lstat", swap_after(real_lstat))
        racing.setattr(hoststate.os, "open", swap_after(real_open))
        sweep([str(root)])
    # The sweep never lstat()s "sub" and then chmod()s it by path, through the symlink, to 0o777.
    assert swapped and oct(victim.stat().st_mode & 0o777) == oct(0o600)
    if (forged / "sub.gone").exists():  # Opened unreadable: the sweep failed on the symlink and left the tree.
        (forged / "sub.gone").chmod(0o700)

    # A tag in a directory, or a tag file, of another user's is not this user's to act on: a forged tag naming a
    # dead PID must not make the sweep remove (or, as non-root, fail on) another user's directory.
    other = _tagged_dir(root, "another-users", DEAD)
    monkeypatch.setattr(hoststate.os, "geteuid", lambda: os.getuid() + 1)
    monkeypatch.setattr(hoststate, "_is_root", lambda: False)
    report = sweep([str(root)])
    assert other.exists() and report.ok and report.leftovers == []


def test_the_default_roots_and_their_fixed_children_are_shared_and_a_runs_own_directories_are_not(
    host, tmp_path, monkeypatch
):
    # A root run (partitions and tmpfs need root) must not leave /tmp/valkey-fuzzer and its runs/ and logs/ 0755
    # and root's: every later run of another user would fail with "Permission denied" making its folder there.
    def shared(path):
        return stat.S_IMODE(os.lstat(path).st_mode) == 0o1777

    monkeypatch.setattr(hoststate.tempfile, "gettempdir", lambda: str(tmp_path))
    run = hoststate.new_run_folder(os.path.join(hoststate.default_root(), "runs"), "x")
    assert shared(hoststate.default_root()) and shared(os.path.dirname(run)) and not shared(run)
    data = tmp_path / "data"
    monkeypatch.setattr(processes, "DEFAULT_BASE_DATA_DIR", str(data))
    _, log_file = ProcessManager(DeploymentConfig(base_data_dir=str(data))).create_instance_directories("valkey-0")
    (deployment,) = data.glob("deployment-*")
    assert shared(data) and shared(os.path.dirname(log_file)) and not shared(deployment)
    mounts = hoststate.mount_parent()
    assert shared(os.path.dirname(mounts)) and not shared(mounts)
    # A root the user names stays theirs, and so does a directory the fuzzer did not just make.
    mine = hoststate.new_run_folder(str(tmp_path / "mine" / "runs"), "x")
    assert not any(shared(path) for path in (mine, os.path.dirname(mine), tmp_path / "mine"))
    (tmp_path / "old").mkdir(mode=0o755)
    hoststate.shared_directory(str(tmp_path / "old"))
    assert stat.S_IMODE((tmp_path / "old").stat().st_mode) == 0o755
    # Even with a dead owner's tag in it, the sweep never takes a shared directory for one run's leftover.
    for path in (hoststate.default_root(), data):
        _tagged_dir(tmp_path, "tagged", DEAD)
        os.replace(tmp_path / "tagged" / hoststate.OWNER_FILE, os.path.join(path, hoststate.OWNER_FILE))
        os.rmdir(tmp_path / "tagged")
    assert _found(sweep([str(tmp_path)])) == []
    assert os.path.isdir(hoststate.default_root()) and data.is_dir()


def test_an_empty_mount_point_of_a_gone_owner_is_removed(host, tmp_path):
    # A tmpfs provider killed between making its mount point and mounting on it leaves an empty directory: its
    # owner is the name of the directory it is in.
    mounts = tmp_path / hoststate.MOUNT_ROOT_NAME
    empty, mounted = mounts / DEAD.token / "valkey-fuzzer-tmpfs-a", mounts / DEAD.token / "valkey-fuzzer-tmpfs-b"
    live, unparsed = mounts / current_owner().token / "valkey-fuzzer-tmpfs-c", mounts / "not-an-owner" / "d"
    for path in (empty, mounted, live, unparsed):
        path.mkdir(parents=True)
    host.mounts = [(str(mounted), "tmpfs")]  # Unmounted by the "mount" leftover, which needs root.

    report = sweep([str(tmp_path)])

    assert ("mount point", str(empty), "removed") in _found(report) and not empty.exists()
    assert ("mount", str(mounted), "failed") in _found(report)
    assert all(path.exists() for path in (mounted, live, unparsed)) and len(report.leftovers) == 2


def test_chains_and_mounts_of_gone_owners_are_removed_only_as_root(host, tmp_path, monkeypatch, capsys):
    live_chain, dead_chain = hoststate.chain_name("live"), f"VF-{DEAD.token}-01234567"
    calls = []

    def iptables(*args):
        calls.append(args)
        listing = f"-N {dead_chain}\n-N {live_chain}\n-N DOCKER\n-N VF-custom\n" if args == ("-S",) else ""
        return SimpleNamespace(returncode=1 if args[0] == "-C" else 0, stdout=listing, stderr="")

    mounts = tmp_path / hoststate.MOUNT_ROOT_NAME
    dead, live = mounts / DEAD.token / "valkey-fuzzer-tmpfs-a", mounts / current_owner().token / "valkey-fuzzer-tmpfs-b"
    for path in (dead, live):
        path.mkdir(parents=True)
    host.mounts = [(str(dead), "tmpfs"), (str(live), "tmpfs"), ("/mnt/scratch", "tmpfs"), (str(tmp_path), "ext4")]
    monkeypatch.setattr(hoststate.platform, "system", lambda: "Linux")
    monkeypatch.setattr(hoststate.shutil, "which", lambda name: f"/usr/sbin/{name}")
    monkeypatch.setattr(hoststate, "run_iptables", iptables)

    # Without root: chains cannot even be listed (reported, not guessed), mounts not unmounted; the command fails
    # with a harness error (never exit 1, a FAIL).
    args = create_parser().parse_args(["sweep", "--root", str(tmp_path / "none"), "--format", "json"])
    assert args.handler(args) == 2
    report = json.loads(capsys.readouterr().out)
    assert [(item["target"], item["action"], item["reason"]) for item in report["leftovers"]] == [
        (str(dead), "failed", "unmounting needs root")
    ]
    assert (
        report["unchecked"] == ["iptables chains: listing them needs root"] and host.unmounted == [] and dead.exists()
    )

    monkeypatch.setattr(hoststate, "_is_root", lambda: True)
    report = sweep([str(tmp_path / "none")])
    assert _found(report) == [("chain", dead_chain, "removed"), ("mount", str(dead), "removed")] and report.ok
    assert ("-X", dead_chain) in calls and all(
        chain not in call for call in calls for chain in (live_chain, "DOCKER", "VF-custom")
    )
    assert host.unmounted == [str(dead)] and not dead.parent.exists() and live.exists()


@pytest.mark.parametrize("enable_cleanup", [True, False])
def test_a_deployment_tags_its_directories_and_servers_and_a_kept_one_loses_its_tags(tmp_path, enable_cleanup):
    manager = ProcessManager(DeploymentConfig(base_data_dir=str(tmp_path), enable_cleanup=enable_cleanup))
    tag, contents = hoststate.tag_directory, []

    def tag_directory(path, kind, label=""):
        contents.append(sorted(os.listdir(path)))
        tag(path, kind, label)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("valkey_fuzzer.deployment.local.processes.tag_directory", tag_directory)
        data_dir, log_file = manager.create_instance_directories("valkey-0")
    # Tagged while still empty, before its instance directories: a kill in between would otherwise leave an
    # untagged tree no sweep touches.
    assert contents == [[]]
    (tmp_path / "disk").mkdir()
    manager.create_instance_directories("valkey-1", data_root=str(tmp_path / "disk"))
    deployment_dir = os.path.dirname(os.path.dirname(data_dir))
    for path in (deployment_dir, str(tmp_path / "disk" / f"deployment-{manager.deployment_id}")):
        assert hoststate.read_tag(path)["owner"] == current_owner().token
    command = manager.build_instance_command(7000, data_dir, log_file, valkey_binary="/usr/bin/valkey-server")
    assert hoststate.server_owner(" ".join(command)) == current_owner()

    manager.cleanup_deployment([])

    # A deployment kept on purpose is untagged, so the sweep leaves it.
    assert os.path.exists(deployment_dir) is not enable_cleanup
    assert enable_cleanup or hoststate.read_tag(deployment_dir) is None


def test_temporary_directory_resources_and_private_servers_are_tagged(tmp_path):
    from valkey_fuzzer.components.providers import TempDirectoryProvider
    from valkey_fuzzer.components.server import PrivateServer

    spec = SimpleNamespace(id="scratch", provider_id="valkey.resource.tmpdir")
    resource = TempDirectoryProvider().acquire(None, spec, {"prefix": "t-", "parent": str(tmp_path), "keep": True})
    assert hoststate.read_tag(resource.outputs["path"])["kind"] == "tmpdir"
    TempDirectoryProvider().cleanup(None, spec, resource)
    assert hoststate.read_tag(resource.outputs["path"]) is None, "kept on purpose, so untagged"
    server = PrivateServer("/usr/bin/valkey-server")
    try:
        assert hoststate.read_tag(server.workdir)["kind"] == "private-server"
    finally:
        server.stop()
    assert not os.path.exists(server.workdir)


def test_a_sweep_for_some_owners_checks_no_other_owner_is_alive(tmp_path, host, monkeypatch):
    # The batch sweeps each worker after its run while the others' servers run; on macOS each liveness
    # check spawns ps.
    checked = []
    monkeypatch.setattr(Owner, "alive", lambda owner: checked.append(owner.token) or owner.pid not in host.dead)
    host.processes = [(4242, 60.0, _title(OTHER, 7000))]
    host.mounts = [(f"/tmp/{hoststate.MOUNT_ROOT_NAME}/{OTHER.token}/disk", "tmpfs")]
    _tagged_dir(tmp_path, "deployment-other", OTHER)
    _tagged_dir(tmp_path, "deployment-dead", DEAD)

    report = sweep([str(tmp_path)], owners={DEAD.token})

    assert _found(report) == [("directory", str(tmp_path / "deployment-dead"), "removed")]
    assert set(checked) == {DEAD.token}
