"""The local deployment's servers: their commands, spawning, restarts, ports, formation, output, and cleanup."""

import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager, nullcontext
from unittest.mock import Mock, call, patch

import pytest
import valkey

from tests.deployment_support import PROCESSES, client_context, fake_client, formed_cluster, instance, process_manager
from tests.support import VirtualClock
from valkey_fuzzer.commands.run import cleanup_logs
from valkey_fuzzer.deployment.local import cluster_nodes, processes, queries
from valkey_fuzzer.deployment.local.formation import Formation
from valkey_fuzzer.deployment.local.models import DeploymentMode, LocalDeploymentError
from valkey_fuzzer.deployment.local.planner import plan_deployment
from valkey_fuzzer.deployment.local.ports import PortUnavailableError
from valkey_fuzzer.deployment.local.processes import VALKEY_SOURCE_REF, InstanceIdentityError, installed_valkey_binary
from valkey_fuzzer.deployment.local.queries import WaitInterruptedError, valkey_client
from valkey_fuzzer.deployment.local.server_config import DEFAULT_MAXMEMORY


def _running(pid):
    return Mock(pid=pid, **{"poll.return_value": None})


def _replica_info(port=7000, link="up", pid=999):
    return dict(process_id=pid, role="slave", master_host="127.0.0.1", master_port=port, master_link_status=link)


@pytest.mark.parametrize("mode", list(DeploymentMode))
def test_the_instance_command_carries_the_mode_the_config_and_the_address(mode):
    manager = process_manager(mode=mode, valkey_binary="/default")

    command = manager.build_instance_command(7000, "/d", "/l", {"loglevel": "debug", "replica-read-only": False})
    isolated = manager.build_instance_command(7000, "/d", "/l", {"maxmemory": "2gb"}, "/node", "127.0.0.3")

    def arg(option):
        return command[command.index(option) + 1]

    assert command[0] == "/default" and isolated[0] == "/node"
    assert arg("--cluster-enabled") == ("yes" if mode is DeploymentMode.CLUSTER else "no")
    assert command[-4:] == ["--loglevel", "debug", "--replica-read-only", "no"]
    # A memory cap but no hidden eviction policy; the scenario's comes last, and Valkey applies the last.
    assert arg("--maxmemory") == DEFAULT_MAXMEMORY and "--maxmemory-policy" not in command
    assert isolated[-2:] == ["--maxmemory", "2gb"] and "--bind-source-addr" not in command
    assert isolated[isolated.index("--bind-source-addr") + 1] == "127.0.0.3"


@pytest.mark.parametrize(
    ("failure", "error"),
    [
        ("second-spawn", OSError),
        ("foreign-server", InstanceIdentityError),  # another server answers on the port of the one just started
        ("interrupted-waiting", KeyboardInterrupt),
        ("interrupted-untracked", KeyboardInterrupt),  # after the process started, before the manager tracks it
    ],
)
def test_a_failed_spawn_stops_the_started_servers_and_removes_their_data(tmp_path, failure, error):
    manager = process_manager(tmp_path, replicas_per_primary=1)
    first = _running(101)
    answers = {
        "foreign-server": {"return_value": client_context(fake_client({"process_id": 4242}))},
        "interrupted-waiting": {"side_effect": KeyboardInterrupt()},
    }
    untracked = failure == "interrupted-untracked"

    with (
        patch("valkey_fuzzer.deployment.local.ports.port_unavailable_reason", return_value=None),
        patch(
            f"{PROCESSES}.subprocess.Popen",
            side_effect=[first, error("x") if failure == "second-spawn" else _running(102)],
        ),
        patch(f"{PROCESSES}.valkey_client", **answers.get(failure, {"return_value": client_context(fake_client())})),
        patch(f"{PROCESSES}.InstanceInfo", side_effect=KeyboardInterrupt()) if untracked else nullcontext(),
        pytest.raises(error),
    ):
        manager.spawn_all(plan_deployment(manager.config))

    first.terminate.assert_called_once()
    assert manager.spawned_instances == []
    assert not (tmp_path / f"deployment-{manager.deployment_id}").exists()


def test_binaries_must_exist(tmp_path):
    binary = tmp_path / "valkey-server"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    replica_binary = {"replica": str(tmp_path / "missing")}
    manager = process_manager(
        tmp_path, valkey_binary=str(binary), replicas_per_primary=1, instance_binaries=replica_binary
    )

    assert manager.resolve_valkey_binary() == str(binary)
    with pytest.raises(FileNotFoundError, match=r"instance_binaries for valkey-1"):
        manager.resolve_instance_binaries(plan_deployment(manager.config))
    with pytest.raises(FileNotFoundError, match="missing-valkey-server"):
        process_manager(tmp_path, valkey_binary=str(tmp_path / "missing-valkey-server")).resolve_valkey_binary()


def test_components_find_valkey_server_and_read_slot_ranges_as_the_deployment_does(tmp_path):
    binary = tmp_path / "valkey-server"
    binary.write_text("#!/bin/sh\n")
    with pytest.raises(PermissionError, match="probe binary"):
        processes.resolve_binary_path(str(binary), "probe binary")
    binary.chmod(0o755)
    assert processes.resolve_binary_path(str(binary)) == str(binary)
    built = os.path.join(processes.SOURCE_BUILD_DIR, "valkey", "src", "valkey-server")
    with patch(f"{PROCESSES}.shutil.which", return_value=None), patch(f"{PROCESSES}.os.path.exists", return_value=True):
        assert installed_valkey_binary() == built, "the source build the deployment falls back to"
    assert cluster_nodes.parse_slot_ranges(["0-5", "7", "[8->-abc]"]) == [(0, 5), (7, 7)]


def test_the_source_build_is_pinned_parallel_and_built_aside_under_a_lock(tmp_path):
    # Concurrent batch workers without a binary must not race on one clone, or build an unpinned HEAD.
    commands, timeouts = [], []

    def run(command, cwd=None, check=False, timeout=None):
        commands.append((command, cwd))
        timeouts.append(timeout)
        if command[0] == "make":
            binary = os.path.join(cwd, "src", "valkey-server")
            os.makedirs(os.path.dirname(binary))
            open(binary, "w").close()

    manager = process_manager(tmp_path, valkey_binary="")
    with (
        patch(f"{PROCESSES}.shutil.which", return_value=None),
        patch(f"{PROCESSES}.subprocess.run", side_effect=run),
        patch(f"{PROCESSES}.fcntl.flock") as flock,
    ):
        binary = manager.resolve_valkey_binary(base_dir=str(tmp_path / "build"), build_timeout=600.0)

    (clone, _), (make, make_dir) = commands
    assert clone[:2] == ["git", "clone"] and "--depth" in clone
    assert clone[clone.index("--branch") + 1] == VALKEY_SOURCE_REF
    assert make[0] == "make" and make[1].startswith("-j")
    assert make_dir == clone[-1] != str(tmp_path / "build" / "valkey"), "built aside, then renamed into place"
    assert binary == str(tmp_path / "build" / "valkey" / "src" / "valkey-server") and os.path.isfile(binary)
    assert flock.call_count == 2  # taken, then released
    # A hung clone or make cannot hold the run past its deadline.
    assert timeouts[0] == 600.0 and 0 < timeouts[1] <= 600.0


def test_spawn_and_formation_waits_end_at_the_runs_deadline(tmp_path, monkeypatch):
    VirtualClock().install(monkeypatch, queries)
    cancelled = Mock(deadline=None, **{"sleep.return_value": False})
    manager = process_manager(tmp_path)
    with (
        patch.object(manager, "_start_process", return_value=_running(4242)),
        patch.object(manager, "_answers", side_effect=valkey.ConnectionError("not yet")),
        pytest.raises(WaitInterruptedError, match="cancelled"),
    ):
        manager.spawn_all(plan_deployment(manager.config), cancelled)
    # Formation does not mistake the interruption for a formation failure.
    formation = Formation(cancelled)
    with (
        patch.object(formation, "_standalone_problem", return_value="not yet"),
        pytest.raises(WaitInterruptedError, match="cancelled"),
    ):
        formation.form([instance()], mode=DeploymentMode.STANDALONE)


def test_a_server_whose_termination_failed_keeps_its_data_and_a_retry_stops_it(tmp_path):
    manager = process_manager(tmp_path, mode=DeploymentMode.CLUSTER, primaries=3)
    stuck, other = instance("valkey-0"), instance("valkey-1", port=7001)
    stuck.process.terminate.side_effect = [PermissionError("not permitted"), None]
    manager._spawned_instances = {"valkey-0": stuck, "valkey-1": other}
    data_dir = tmp_path / f"deployment-{manager.deployment_id}"
    data_dir.mkdir()

    with pytest.raises(RuntimeError, match="completed with failures"):
        manager.cleanup_deployment(manager.spawned_instances)

    # Every server was attempted; the one that may still run stays owned, with its data.
    other.process.terminate.assert_called_once()
    assert manager.spawned_instances == [stuck] and data_dir.exists()
    manager.cleanup_deployment(manager.spawned_instances)
    assert manager.spawned_instances == [] and not data_dir.exists()


def test_a_kept_deployment_whose_tag_cannot_be_removed_is_reported_like_any_cleanup_failure(tmp_path):
    manager = process_manager(tmp_path, enable_cleanup=False)
    server = instance()
    manager._spawned_instances = {"valkey-0": server}
    with patch(f"{PROCESSES}.untag_directory", side_effect=PermissionError("read-only")):
        # A failed spawn's cleanup must not replace the spawn's own error with its own.
        manager._cleanup_failed_spawn([server])
        server.process.terminate.assert_called_once()
        with pytest.raises(RuntimeError, match="completed with failures: untag"):
            manager.cleanup_deployment([server])


# A server that refuses SIGTERM, as a replica writing its first AOF does, and has forked a child.
_FORKING_SERVER = """
import os, signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = os.fork()
if child == 0:
    time.sleep(60)
    os._exit(0)
print(child, flush=True)
time.sleep(60)
"""


def _exited(pid):
    # A zombie has exited too, whoever is left to reap it.
    status = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return not status or status.startswith("Z")


def test_a_server_killed_at_cleanup_takes_the_child_it_forked_along():
    # Issue #34: a SIGKILLed server's AOF rewrite child kept running past the run.
    server = subprocess.Popen([sys.executable, "-c", _FORKING_SERVER], stdout=subprocess.PIPE, text=True)
    child = int(server.stdout.readline())
    try:
        processes.kill_server(server)
        assert server.wait(timeout=10) == -signal.SIGKILL
        deadline = time.monotonic() + 10
        while not _exited(child) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert _exited(child)
    finally:
        server.kill()
        server.wait()
        server.stdout.close()
        try:
            os.kill(child, signal.SIGKILL)
        except ProcessLookupError:
            pass


def test_a_server_that_refuses_sigterm_is_killed_with_its_children():
    server = Mock(**{"wait.side_effect": [subprocess.TimeoutExpired("valkey-server", 5), -9]})
    with patch(f"{PROCESSES}.kill_server") as kill_server:
        processes._stop_process(server)
    kill_server.assert_called_once_with(server)
    server.kill.assert_not_called()


def test_two_deployments_with_the_same_seed_write_separate_logs_that_cleanup_finds(tmp_path):
    # Concurrent runs of one seed must not interleave their instance logs: log checks would read the other's lines.
    paths = [process_manager(tmp_path, seed=7).create_instance_directories("valkey-0")[1] for _ in range(2)]
    assert paths[0] != paths[1]
    for path in paths:
        open(path, "w").close()
    assert cleanup_logs(7, str(tmp_path), force=True) == 0
    assert not any(os.path.exists(path) for path in paths)


def test_process_output_goes_to_one_file_per_instance_that_a_startup_failure_reports(tmp_path):
    manager = process_manager(tmp_path)
    log_file = str(tmp_path / "1-valkey-0.log")
    for marker in ("first", "second"):
        process = manager._start_process(
            [sys.executable, "-c", f"import sys; print({marker!r}, file=sys.stderr)"], log_file
        )
        assert process.stdout is None and process.stderr is None
        process.wait(timeout=10)

    assert (tmp_path / "1-valkey-0.stdio.log").read_text().split() == ["first", "second"]
    assert "second" in manager._process_output_tail(log_file)


@contextmanager
def _held_port():
    """A port another server holds; the OS picks a free one with room for a cluster's bus ports above it."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    while listener.getsockname()[1] > 65000:
        listener.close()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    try:
        yield listener.getsockname()[1]
    finally:
        listener.close()


@pytest.mark.parametrize(("mode", "kind", "offset"), [("standalone", "client", 0), ("cluster", "cluster bus", 10000)])
def test_a_port_another_server_holds_stops_a_spawn_or_restart_before_anything_starts(tmp_path, mode, kind, offset):
    with _held_port() as port, patch(f"{PROCESSES}.subprocess.Popen") as popen:
        primaries = 3 if mode == "cluster" else 1
        manager = process_manager(tmp_path, mode=DeploymentMode(mode), primaries=primaries, base_port=port - offset)
        with pytest.raises(PortUnavailableError, match=f"{kind} port {port}"):
            manager.spawn_all(plan_deployment(manager.config))
        # The port was taken while the instance was down.
        node = instance(port=port - offset, pid=100)
        node.process.poll.return_value = 0
        with pytest.raises(PortUnavailableError):
            manager.restart_instance(node, wait_ready=False)

    popen.assert_not_called()


def _pair(tmp_path):
    manager = process_manager(tmp_path, replicas_per_primary=1)
    primary, replica = instance("valkey-0"), instance("valkey-1", "replica", 7001, primary_id="valkey-0")
    for node in (primary, replica):
        node.log_file = str(tmp_path / f"{node.instance_id}.log")
    manager._spawned_instances = {"valkey-0": primary, "valkey-1": replica}
    return manager, replica


@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (_replica_info(), None),
        (_replica_info(port=7999), "failed to become ready"),  # it replicates another primary
        (_replica_info(pid=4242), "served by pid 4242"),  # another server answers on its port
    ],
)
def test_a_replica_restarts_after_its_old_server_stopped_and_must_rejoin_its_primary(tmp_path, answer, error):
    manager, replica = _pair(tmp_path)
    old, new = replica.process, _running(999)
    events = Mock()
    events.attach_mock(old.terminate, "terminate_old")
    events.attach_mock(Mock(return_value=new), "popen")

    with (
        patch(f"{PROCESSES}.subprocess.Popen", events.popen),
        patch(f"{PROCESSES}.valkey_client", return_value=client_context(fake_client(answer))),
        patch.object(manager, "ensure_ports_available"),
        pytest.raises(Exception, match=error) if error else nullcontext(),
    ):
        manager.restart_instance(replica, wait_ready=True, ready_timeout=0.3)

    # Two servers never compete for one port and data directory.
    assert [name for name, *_ in events.mock_calls][:2] == ["terminate_old", "popen"]
    command = events.popen.call_args.args[0]
    assert command[command.index("--replicaof") :][:3] == ["--replicaof", "127.0.0.1", "7000"]
    assert new.terminate.called is bool(error)


_OK = "cluster_state:ok\r\ncluster_known_nodes:1\r\n"
_ME = "abc 127.0.0.1:7000@17000 myself,master{} - 0 0 1 connected 0-8191\ndef 127.0.0.1:7001@17001 master - 0 0 2 connected"


@pytest.mark.parametrize(
    ("role", "info", "replies", "primary_alive", "ready"),
    [
        # A primary waits for its running replicas.
        ("primary", {"role": "master", "connected_slaves": 1}, None, True, True),
        ("primary", {"role": "slave"}, None, True, False),
        # A replica waits for its link, unless the primary it follows was killed: then the link cannot come up.
        ("replica", _replica_info(pid=7001, link="down"), None, True, False),
        ("replica", _replica_info(pid=7001, link="down"), None, False, True),
        ("cluster", {"cluster_enabled": 1}, [_OK, "abc", _ME.format("")], True, True),
        ("cluster", {"cluster_enabled": 1}, [_OK, "abc", _ME.format(",fail")], True, False),
        ("cluster", {"cluster_enabled": 1}, ["cluster_state:fail\r\n"], True, False),
        ("cluster", {"cluster_enabled": 1}, [_OK, "other-id"], True, InstanceIdentityError),
    ],
)
def test_a_restarted_server_is_ready_once_it_rejoins_in_its_role(tmp_path, role, info, replies, primary_alive, ready):
    manager, replica = _pair(tmp_path)
    manager._spawned_instances["valkey-0"].process.poll.return_value = None if primary_alive else -9
    node = replica if role == "replica" else instance(cluster_node_id="abc")
    if role == "cluster":
        manager.config.mode = DeploymentMode.CLUSTER
        manager._spawned_instances = {"valkey-0": node}
    client = fake_client({"process_id": node.pid, **info}, replies)

    if ready is InstanceIdentityError:
        with pytest.raises(InstanceIdentityError, match="expected abc"):
            manager._restart_is_ready(node, client, manager.config.mode)
    else:
        assert manager._restart_is_ready(node, client, manager.config.mode) is ready


@pytest.mark.parametrize("failure", [RuntimeError("formation failed"), KeyboardInterrupt()])
def test_a_failed_formation_closes_its_cached_clients(failure):
    formation = Formation()
    client = formation.connections["valkey-0"] = Mock()

    with (
        patch.object(formation, "reset_cluster_state", side_effect=failure),
        pytest.raises(LocalDeploymentError if isinstance(failure, Exception) else KeyboardInterrupt),
    ):
        formation.form([instance()])

    client.close.assert_called_once()
    assert formation.connections == {}


def test_formation_points_each_replica_at_the_primary_and_resets_only_its_own_servers():
    primary, replica = instance("valkey-0"), instance("valkey-1", "replica", 7001, primary_id="valkey-0")
    clients = {
        "valkey-0": fake_client({"role": "master", "connected_slaves": 1}),
        "valkey-1": fake_client(_replica_info()),
    }
    formation = Formation()
    formation.get_client = lambda node: clients[node.instance_id]

    formation.form([primary, replica], mode=DeploymentMode.STANDALONE)
    assert clients["valkey-1"].execute_command.call_args_list == [call("REPLICAOF", "127.0.0.1", 7000)]
    clients["valkey-1"].info.return_value = _replica_info(port=7999)
    assert "replicates 127.0.0.1:7999" in formation._primary_replica_problem(primary, [replica])
    # A primary that does not report its replicas is not taken to have them all.
    clients["valkey-1"].info.return_value = _replica_info()
    clients["valkey-0"].info.return_value = {"role": "master"}
    assert formation._primary_replica_problem(primary, [replica]) == "valkey-0 reports 0 of 1 replicas connected"

    # CLUSTER RESET HARD wipes a node: never send it to a server another process runs, or after ours exited.
    clients = {"valkey-0": fake_client({"process_id": 7000}), "valkey-1": fake_client({"process_id": 999})}
    with pytest.raises(InstanceIdentityError, match="valkey-1"):
        formation.reset_cluster_state([primary, replica])
    assert clients["valkey-0"].execute_command.call_args_list == [call("CLUSTER", "RESET", "HARD")]
    clients["valkey-1"].execute_command.assert_not_called()
    primary.process.poll.return_value = 0
    with pytest.raises(InstanceIdentityError, match="has exited"):
        formation.reset_cluster_state([primary])


def test_each_replica_link_seen_up_counts_although_another_replicas_info_times_out(monkeypatch):
    """84 instances at load 400: every link was up 6 s in, yet one slow INFO failed each round of the old
    all-replicas poll, so provisioning never passed."""

    clock = VirtualClock().install(monkeypatch, queries)
    replicas = [instance(f"valkey-{n}", "replica", 7000 + n, primary_id="valkey-0") for n in (1, 2)]
    down = {**_replica_info(), "master_link_status": "down"}
    clients = {"valkey-1": Mock(), "valkey-2": Mock()}
    # valkey-1 is up at once, then its INFO times out; valkey-2 comes up on the second round.
    clients["valkey-1"].info.side_effect = [_replica_info(), *[TimeoutError("Timeout reading from socket")] * 10]
    clients["valkey-2"].info.side_effect = [down, _replica_info(), _replica_info()]
    formation = Formation()
    formation.get_client = lambda node: clients[node.instance_id]
    formation.setup_and_sync_replication(replicas, {"valkey-0": "n0"}, timeout=5)
    assert clock.elapsed == pytest.approx(1.0), "done on the second round"

    # A link never seen up still fails provisioning, naming the replica.
    clients["valkey-1"].info.side_effect = [_replica_info()]
    clients["valkey-2"].info.side_effect = [down] * 10
    with pytest.raises(LocalDeploymentError, match=r"did not come up within 1.00s \(valkey-2\)"):
        formation.setup_and_sync_replication(replicas, {"valkey-0": "n0"}, timeout=1)
    # A replica whose primary has no cluster ID cannot be configured: it fails at once, not 60 s later.
    started = clock.elapsed
    with pytest.raises(LocalDeploymentError, match="valkey-1's primary 'valkey-0' has no cluster ID"):
        formation.setup_and_sync_replication(replicas, {}, timeout=60)
    assert clock.elapsed == started


@pytest.mark.slow
def test_a_killed_primary_of_a_real_cluster_restarts_as_a_new_process_the_cluster_takes_back():
    with formed_cluster(3, 1, 7700) as cluster:
        primary = next(node for node in cluster.nodes if node.shard_id == 0 and node.role == "primary")
        old_pid = primary.pid
        primary.process.kill()
        primary.process.wait()
        time.sleep(3)  # let the cluster see it down

        cluster.processes.restart_instance(primary, wait_ready=True, ready_timeout=30.0)

        assert primary.pid != old_pid and primary.process.poll() is None
        with valkey_client(primary.host, primary.port, timeout=2.0) as client:
            assert int(client.info("server")["process_id"]) == primary.pid
