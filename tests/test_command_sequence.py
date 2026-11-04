"""``valkey.action.commands`` without Valkey processes: fake private servers of two builds behind the real raw
connection, so arguments go out as bytes and replies come back as the server sent them, and the verdicts."""

import hashlib
import os
import shutil
import tempfile

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import ResponseError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from tests.valkey_experiments_support import registration_for, resolved, unit_plan
from valkey_fuzzer._data import canonical_json, to_plain_data
from valkey_fuzzer.components import actions, capabilities
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.schema import ConfigError, load_yaml
from valkey_fuzzer.testing import FakeContext

# What a fake build does with a command (see Build).
DIE, TIMEOUT = "<die>", "<timeout>"


class Error(str):
    """An error reply."""


class Build:
    """A fake valkey-server build: the options it rejects at start, and its reply to each command, keyed by
    the arguments as sent (strings, or bytes for a binary argument); an unlisted command gets ``OK``. A reply
    may be an :class:`Error`, :data:`DIE` (the server exits without answering) or :data:`TIMEOUT`."""

    def __init__(self, replies=None, rejects=()):
        self.replies, self.rejects = dict(replies or {}), set(rejects)
        self.starts: list[list[str]] = []
        self.sent: list[tuple] = []
        self.connections = 0


BUILDS: dict[str, Build] = {}
SERVERS: dict[str, "FakeServer"] = {}


class FakeProcess:
    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode


class FakeServer:
    """Stands in for PrivateServer: starts unless its build rejects an option, and logs a line."""

    def __init__(self, binary, arguments=(), *, timeout_s=15.0, prefix="x-"):
        self.binary, self.arguments, self.build = binary, list(arguments), BUILDS[binary]
        self.workdir = tempfile.mkdtemp(prefix=prefix)
        self.socket_path = os.path.join(self.workdir, "s.sock")
        self.log_path = os.path.join(self.workdir, "server.log")
        self.output_path = os.path.join(self.workdir, "output.log")
        self.process, self.client, self.stopped = FakeProcess(), None, False

    def start(self):
        self.build.starts.append(self.arguments)
        rejected = [name for name in self.arguments[::2] if name[2:] in self.build.rejects]
        if rejected:
            return f"*** FATAL CONFIG FILE ERROR *** Bad directive or wrong number of arguments: {rejected[0]}"
        with open(self.log_path, "w") as log:
            log.write(f"1:M 03 Nov 2026 10:00:00.000 * Ready to accept connections ({self.binary})\n")
        with open(self.output_path, "w") as output:
            output.write("")
        SERVERS[self.socket_path] = self
        return None

    def stop(self):
        self.stopped = True
        shutil.rmtree(self.workdir, ignore_errors=True)

    def die(self):
        with open(self.log_path, "a") as log:
            log.write("1:M 03 Nov 2026 10:00:01.000 # Valkey 9.0.0 crashed by signal: 11, si_code: 0\n")
        self.process.returncode = -11


class FakeWire:
    """A valkey-py connection to one fake server: ``send_command``, ``read_response``, ``disconnect``."""

    def __init__(self, server):
        self.server, self.pending = server, None
        server.build.connections += 1

    def send_command(self, *args):
        self.pending = tuple(item if isinstance(item, bytes) else str(item) for item in args)
        self.server.build.sent.append(self.pending)

    def read_response(self):
        reply = self.server.build.replies.get(self.pending, b"OK")
        if reply == DIE:
            self.server.die()
            raise ValkeyConnectionError("Connection closed by server.")
        if reply == TIMEOUT:
            raise ValkeyTimeoutError("Timeout reading from socket")
        if isinstance(reply, Error):
            raise ResponseError(str(reply))
        return reply

    def disconnect(self):
        pass


def fake_open_raw_connection(resp, timeout_s, *, path=None, host=None, port=None):
    server = SERVERS[path] if path is not None else SERVERS[f"{host}:{port}"]
    return actions.RawConnection(FakeWire(server))


@pytest.fixture
def builds(monkeypatch, tmp_path):
    """Executable paths for the ``candidate`` and ``parent`` builds, whose behaviour the test sets in BUILDS."""

    BUILDS.clear()
    SERVERS.clear()
    monkeypatch.setattr(actions, "PrivateServer", FakeServer)
    monkeypatch.setattr(capabilities, "PrivateServer", FakeServer)
    monkeypatch.setattr(actions, "open_raw_connection", fake_open_raw_connection)
    paths = {}
    for label in ("candidate", "parent"):
        path = tmp_path / f"{label}-valkey-server"
        path.write_text("#!/bin/sh\n")
        path.chmod(0o755)
        paths[label] = str(path)
        BUILDS[str(path)] = Build()
    return paths


def _spec(**config):
    spec = ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.commands", id="seq", config=config)
    return resolved(spec)


def run(builds, commands, **config):
    """The action on both builds, with ``commands``; the result and its evidence value."""

    spec = _spec(compare_with=builds["parent"], commands=commands, **config)
    context = FakeContext(spec, deployment={"valkey_binary": builds["candidate"]})
    result = actions.CommandsAction().run(context, spec)
    return result, (to_plain_data(result.evidence[0].value) if result.evidence else None)


def _replies(side):
    return [{k: v for k, v in record.items() if k != "index"} for record in side["replies"]]


# ---------------------------------------------------------------------------
# Replies, bytes, digests, connections
# ---------------------------------------------------------------------------


def test_replies_are_recorded_in_order_as_the_server_sent_them_with_bytes_in_and_out(builds):
    for build in BUILDS.values():
        build.replies = {
            ("GET", "bin"): b"\xff\x00\xff",
            ("GET", "text"): b"caf\xc3\xa9",
            ("HGETALL", "h"): [b"f", b"v"],
            ("INCR", "n"): 7,
            ("GET", "missing"): None,
            ("EXEC",): [b"OK", ResponseError("ERR inner")],
        }
    commands = [
        {"command": ["SET", "bin", {"$base64": "/wD/"}]},
        {"command": ["GET", "bin"]},
        {"command": ["GET", "text"]},
        {"command": ["HGETALL", "h"]},
        {"command": ["INCR", "n"]},
        {"command": ["GET", "missing"]},
        {"command": ["EXEC"]},
    ]
    result, value = run(builds, commands)

    assert result.status.value == "PASS", result.summary
    assert BUILDS[builds["candidate"]].sent[0] == ("SET", "bin", b"\xff\x00\xff")
    assert _replies(value["candidate"]) == [
        {"reply": "OK"},
        {"reply": {"$base64": "/wD/"}},
        {"reply": "café"},
        {"reply": ["f", "v"]},
        {"reply": 7},
        {"reply": None},
        {"reply": ["OK", {"$error": "ERR inner"}]},
    ]
    assert [record["index"] for record in value["candidate"]["replies"]] == list(range(7))
    assert value["parent"]["replies"] == value["candidate"]["replies"]
    assert not any("at" in record for record in value["candidate"]["replies"])


def test_the_digest_is_the_sha256_of_the_compared_replies_and_two_runs_digest_the_same(builds):
    commands = [{"command": ["PING"]}, {"command": ["TIME"], "compare": "skip"}]
    for build in BUILDS.values():
        build.replies = {("TIME",): [b"1", b"2"]}
    _, first = run(builds, commands)
    BUILDS[builds["candidate"]].replies = {("TIME",): [b"3", b"4"]}
    _, second = run(builds, commands)

    expected = hashlib.sha256(canonical_json([{"reply": "OK"}]).encode()).hexdigest()
    assert first["candidate"]["digest"] == first["parent"]["digest"] == second["candidate"]["digest"] == expected


def test_one_connection_per_server_carries_the_whole_sequence(builds):
    commands = [{"command": ["MULTI"]}, {"command": ["SET", "k", "v"]}, {"command": ["EXEC"]}]
    result, _ = run(builds, commands)

    assert result.status.value == "PASS"
    for build in BUILDS.values():
        assert build.connections == 1
        assert build.sent == [("MULTI",), ("SET", "k", "v"), ("EXEC",)]
    # Every private server was stopped, its directory removed, and its logs kept in the component's folder.
    assert all(server.stopped and not os.path.exists(server.workdir) for server in SERVERS.values())


# ---------------------------------------------------------------------------
# Comparison modes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "candidate", "parent", "status"),
    [
        ("equal", [b"a", b"b"], [b"b", b"a"], "ERROR"),
        ("unordered", [b"a", b"b"], [b"b", b"a"], "PASS"),
        ("unordered-pairs", [b"f1", b"v1", b"f2", b"v2"], [b"f2", b"v2", b"f1", b"v1"], "PASS"),
        # Values swapped between fields differ, however the pairs are ordered.
        ("unordered-pairs", [b"f1", b"v1", b"f2", b"v2"], [b"f1", b"v2", b"f2", b"v1"], "ERROR"),
        ("unordered-pairs", {b"f1": b"v1", b"f2": b"v2"}, {b"f2": b"v2", b"f1": b"v1"}, "PASS"),
        ("skip", [b"a"], [b"b"], "PASS"),
        # JSON text compares by the value it holds: key order, spacing, and 1.0 against 1 do not matter ...
        ("json", b'{"a":1,"b":[1,2]}', b'{"b": [1, 2], "a": 1.0}', "PASS"),
        ("equal", b'{"a":1,"b":[1,2]}', b'{"b": [1, 2], "a": 1.0}', "ERROR"),
        ("json", [b'{"a":"x"}', None], [b'{ "a" : "x" }', None], "PASS"),
        # ... but every value does, true is not 1, and text that is not JSON compares as itself.
        ("json", b'{"a":[1,2]}', b'{"a":[2,1]}', "ERROR"),
        ("json", b"[true]", b"[1]", "ERROR"),
        ("json", b"not json", b"not json", "PASS"),
    ],
)
def test_compare_modes(builds, mode, candidate, parent, status):
    BUILDS[builds["candidate"]].replies = {("REPLY",): candidate}
    BUILDS[builds["parent"]].replies = {("REPLY",): parent}
    result, value = run(builds, [{"command": ["REPLY"], "compare": mode}])

    assert result.status.value == status, result.summary
    assert (value["unexplained"] == [0]) is (status == "ERROR")
    assert (value["candidate"]["digest"] == value["parent"]["digest"]) is (status == "PASS")


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


def test_an_expect_the_candidate_violates_is_fail(builds):
    BUILDS[builds["candidate"]].replies = {("LRANGE", "l", "0", "-1"): [b"a", b"x"]}
    BUILDS[builds["parent"]].replies = {("LRANGE", "l", "0", "-1"): [b"a", b"x"]}
    commands = [
        {"command": ["LRANGE", "l", 0, -1], "expect": {"equals": ["a", "b"]}},
        {"command": ["PING"], "expect": {"error": True}},
    ]
    result, _ = run(builds, commands)

    assert result.status.value == "FAIL"
    assert 'command 0 (LRANGE l 0 -1) replied {"reply":["a","x"]}, not {"equals":["a","b"]}' in result.summary
    assert "command 1 (PING)" in result.summary


def test_a_candidate_reply_that_never_came_leaves_the_run_untested(builds):
    BUILDS[builds["candidate"]].replies = {("SLOW",): TIMEOUT}
    result, value = run(builds, [{"command": ["SLOW"]}, {"command": ["PING"]}])

    assert result.status.value == "ERROR"
    assert "command 0 (SLOW) got no reply" in result.summary
    # The sequence stops there: the connection's state is unknown.
    assert _replies(value["candidate"]) == [{"error": "TimeoutError: Timeout reading from socket", "unanswered": True}]
    assert "exited" not in value["candidate"]


def test_marked_commands_whose_replies_are_equal_did_not_reach_the_change(builds):
    commands = [{"command": ["PING"], "expect_difference": True, "expect": {"equals": "OK"}}]
    result, value = run(builds, commands)

    assert result.status.value == "ERROR"
    assert dict(result.diagnostics) == {"reason": "unreached-change"}
    assert "did not reach the change: 0 (PING)" in result.summary
    assert value["reached"] == []


def test_an_unmarked_difference_is_an_unexplained_difference_with_both_replies(builds):
    BUILDS[builds["candidate"]].replies = {("LSET", "l", "9", "x"): Error("ERR index out of range")}
    commands = [
        {"command": ["LSET", "l", 9, "x"], "expect_difference": True, "expect": {"error": "out of range"}},
        {"command": ["LRANGE", "l", 0, -1]},
    ]
    BUILDS[builds["candidate"]].replies[("LRANGE", "l", "0", "-1")] = [b"a"]
    BUILDS[builds["parent"]].replies[("LRANGE", "l", "0", "-1")] = [b"x"]
    result, value = run(builds, commands)

    assert result.status.value == "ERROR"
    assert to_plain_data(result.diagnostics) == {
        "reason": "unexplained-difference",
        "commands": [
            {"index": 1, "command": "LRANGE l 0 -1", "candidate": {"reply": ["a"]}, "parent": {"reply": ["x"]}}
        ],
    }
    assert (value["reached"], value["unexplained"]) == ([0], [1])


def test_a_boolean_reply_is_not_the_same_as_an_integer_one(builds):
    # A pull request that turns a RESP3 integer reply into a boolean changed the reply.
    BUILDS[builds["candidate"]].replies = {("EXISTS", "k"): True}
    BUILDS[builds["parent"]].replies = {("EXISTS", "k"): 1}
    result, value = run(builds, [{"command": ["EXISTS", "k"]}])

    assert result.status.value == "ERROR"
    assert result.diagnostics["reason"] == "unexplained-difference" and value["unexplained"] == [0]


def test_a_parent_that_dies_on_a_marked_command_reached_the_change(builds):
    BUILDS[builds["parent"]].replies = {("RESTORE", "k", "0", b"\x00\x0e"): DIE}
    commands = [
        {"command": ["RESTORE", "k", 0, {"$base64": "AA4="}], "expect_difference": True, "expect": {"error": True}},
        {"command": ["PING"]},
    ]
    BUILDS[builds["candidate"]].replies = {("RESTORE", "k", "0", b"\x00\x0e"): Error("ERR Bad data format")}
    result, value = run(builds, commands)

    assert result.status.value == "PASS", result.summary
    assert value["reached"] == [0] and value["parent"]["exited"] == -11
    assert _replies(value["parent"]) == [{"error": "ConnectionError: Connection closed by server.", "unanswered": True}]
    assert "exited" not in value["candidate"]


def test_a_parent_that_dies_on_an_unmarked_command_is_an_unexplained_difference(builds):
    BUILDS[builds["parent"]].replies = {("PING",): DIE}
    result, value = run(builds, [{"command": ["PING"]}])

    assert result.status.value == "ERROR"
    assert result.diagnostics["reason"] == "unexplained-difference"
    assert value["unexplained"] == [0]


def test_a_candidate_that_dies_is_a_crash_whose_logs_survive_the_stop(builds):
    BUILDS[builds["candidate"]].replies = {("DEBUG", "SEGFAULT"): DIE}
    result, value = run(builds, [{"command": ["SET", "k", "v"]}, {"command": ["DEBUG", "SEGFAULT"]}])

    assert result.status.value == "FAIL" and result.crash
    assert result.summary == "the candidate exited on command 1 (DEBUG SEGFAULT): crashed by signal: 11"
    assert dict(result.diagnostics["crashes"]) == {"candidate": "crashed by signal: 11"}
    assert value["candidate"]["exited"] == -11
    kept = {os.path.basename(path): path for path in result.evidence[0].artifacts}
    assert set(kept) == {"candidate.server.log", "candidate.output.log", "parent.server.log", "parent.output.log"}
    with open(kept["candidate.server.log"]) as log:
        assert "crashed by signal: 11" in log.read()
    assert all(server.stopped for server in SERVERS.values())


# ---------------------------------------------------------------------------
# Configuration of the private servers
# ---------------------------------------------------------------------------


def test_an_option_the_parent_rejects_is_dropped_on_both_sides_and_recorded(builds):
    BUILDS[builds["parent"]].rejects = {"new-option"}
    result, value = run(
        builds, [{"command": ["PING"]}], server_config={"new-option": "yes", "list-max-listpack-size": 4}
    )

    assert result.status.value == "PASS", result.summary
    assert value["dropped_config"] == ["new-option"]
    for build in BUILDS.values():
        running = build.starts[-1]
        assert "--new-option" not in running
        assert running[running.index("--list-max-listpack-size") + 1] == "4"
        assert running[running.index("--sanitize-dump-payload") + 1] == "yes"
        assert running[running.index("--enable-debug-command") + 1] == "yes"
    assert "dropped new-option" in result.summary


def test_a_build_that_does_not_start_leaves_the_run_untested(builds):
    BUILDS[builds["candidate"]].rejects = {"list-max-listpack-size"}
    result, value = run(builds, [{"command": ["PING"]}], server_config={"list-max-listpack-size": 4})

    assert result.status.value == "ERROR" and value is None
    assert result.summary.startswith("the candidate build")


def test_cluster_starts_both_servers_as_one_node_clusters_serving_every_slot(builds):
    for build in BUILDS.values():
        build.replies = {("CLUSTER", "INFO"): b"cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"}
    result, value = run(builds, [{"command": ["SET", "k", "v"], "expect": {"equals": "OK"}}], cluster=True)

    assert result.status.value == "PASS", result.summary
    assert value["cluster"] is True
    ports = set()
    for build in BUILDS.values():
        running = build.starts[-1]
        assert running[running.index("--cluster-enabled") + 1] == "yes"
        ports.add(running[running.index("--cluster-port") + 1])
        assert build.sent[:2] == [("CLUSTER", "ADDSLOTSRANGE", "0", "16383"), ("CLUSTER", "INFO")]
        assert build.sent[-1] == ("SET", "k", "v")
    assert len(ports) == 2
    assert _replies(value["candidate"]) == [{"reply": "OK"}]


def test_a_cluster_that_never_reaches_ok_leaves_the_run_untested(builds):
    for build in BUILDS.values():
        build.replies = {("CLUSTER", "INFO"): b"cluster_state:fail\r\n"}
    result, value = run(builds, [{"command": ["PING"]}], cluster=True, timeout_s=0.2)

    assert result.status.value == "ERROR" and value is None
    assert result.summary.startswith("the candidate server: cluster_state is not ok within 0.2s")


# ---------------------------------------------------------------------------
# Without compare_with: the deployment's instances
# ---------------------------------------------------------------------------


class FakeInstance:
    def __init__(self, instance_id, role, port):
        self.instance_id, self.role, self.host, self.port = instance_id, role, "127.0.0.1", port


class FakeHandle:
    def __init__(self):
        self.instances = (FakeInstance("valkey-0", "primary", 7000), FakeInstance("valkey-1", "replica", 7001))

    def primaries(self):
        return ["valkey-0"]

    def replicas(self):
        return ["valkey-1"]

    def instance(self, instance_id):
        return next(item for item in self.instances if item.instance_id == instance_id)


def test_without_compare_with_the_sequence_runs_on_each_selected_instance(builds):
    handle = FakeHandle()
    for instance in handle.instances:
        SERVERS[f"{instance.host}:{instance.port}"] = FakeServer(builds["candidate"])
    BUILDS[builds["candidate"]].replies = {("DBSIZE",): 3}
    spec = _spec(instances="all", commands=[{"command": ["PING"]}, {"command": ["DBSIZE"], "expect": {"min": 1}}])
    context = FakeContext(spec, services={"valkey.deployment": handle})
    result = actions.CommandsAction().run(context, spec)

    assert result.status.value == "PASS", result.summary
    assert [item.value["instance_id"] for item in result.evidence] == ["valkey-0", "valkey-1"]
    assert all(item.value["parent"] is None and item.value["cluster"] is False for item in result.evidence)
    assert BUILDS[builds["candidate"]].connections == 2
    assert not BUILDS[builds["candidate"]].starts


def test_without_compare_with_a_wrong_reply_fails_and_no_reply_is_untested(builds):
    handle = FakeHandle()
    SERVERS["127.0.0.1:7000"] = FakeServer(builds["candidate"])
    BUILDS[builds["candidate"]].replies = {("PING",): TIMEOUT, ("GET", "k"): b"v"}
    spec = _spec(commands=[{"command": ["GET", "k"], "expect": {"equals": "w"}}, {"command": ["PING"]}])
    context = FakeContext(spec, services={"valkey.deployment": handle})

    assert actions.CommandsAction().run(context, spec).status.value == "FAIL"
    spec = _spec(commands=[{"command": ["PING"]}])
    assert actions.CommandsAction().run(context, spec).status.value == "ERROR"


# ---------------------------------------------------------------------------
# Compile-time checks
# ---------------------------------------------------------------------------

_PARENT = {"compare_with": "/opt/parent/valkey-server"}


@pytest.mark.parametrize(
    ("config", "error"),
    [
        ({"commands": []}, "at least one command"),
        ({"commands": [{"command": []}]}, r"commands\[0\]\.command must be a non-empty list"),
        ({"commands": [{"command": ["SET", "k", True]}]}, r"commands\[0\]\.command must be a non-empty list"),
        ({"commands": [{"command": ["PING"], "expect": {}}]}, "needs one predicate"),
        ({"commands": [{"command": ["PING"], "expect": {"error": True, "equals": 1}}]}, "cannot be combined"),
        ({"commands": [{"command": ["PING"], "compare": "sorted"}]}, "must be one of"),
        ({"commands": [{"command": ["PING"], "expect_difference": True}]}, "needs compare_with"),
        ({**_PARENT, "commands": [{"command": ["PING"], "expect_difference": True}]}, "marked expect_difference needs an expect"),
        ({"cluster": True, "commands": [{"command": ["PING"]}]}, "cluster needs compare_with"),
        ({**_PARENT, "server_config": {"dir": "/x"}, "commands": [{"command": ["PING"]}]}, "server_config: .*'dir' is managed"),
        ({**_PARENT, "server_config": {"cluster-enabled": "yes"}, "commands": [{"command": ["PING"]}]}, "'cluster-enabled' is managed"),
        ({**_PARENT, "commands": [{"command": ["SHUTDOWN", "NOSAVE"]}]}, r"commands\[0\]: SHUTDOWN is refused: it stops the server"),
        ({**_PARENT, "commands": [{"command": ["DEBUG", "RESTART"]}]}, "DEBUG RESTART is refused: it ends or restarts"),
        ({**_PARENT, "commands": [{"command": ["debug", "crash-and-recover", 0]}]}, "DEBUG CRASH-AND-RECOVER is refused"),
        ({**_PARENT, "commands": [{"command": ["DEBUG", "RELOAD"]}]}, "DEBUG RELOAD is refused"),
        ({**_PARENT, "timeout_s": 5, "commands": [{"command": ["DEBUG", "SLEEP", 6]}]}, r"DEBUG SLEEP is refused: it sleeps longer than timeout_s \(5s\)"),
        ({**_PARENT, "commands": [{"command": ["MODULE", "LOAD", "/x.so"]}]}, "MODULE LOAD is refused: it loads or unloads a module"),
        ({**_PARENT, "commands": [{"command": ["MODULE", "LOADEX", "/x.so"]}]}, "MODULE LOADEX is refused"),
        ({**_PARENT, "commands": [{"command": ["MODULE", "UNLOAD", "x"]}]}, "MODULE UNLOAD is refused"),
        ({**_PARENT, "commands": [{"command": ["CONFIG", "SET", "maxmemory", "1mb", "Dir", "/x"]}]}, "CONFIG SET is refused: dir is an option the harness manages"),
        ({**_PARENT, "commands": [{"command": ["MONITOR"]}]}, "MONITOR is refused: it holds the connection"),
        ({**_PARENT, "commands": [{"command": ["SUBSCRIBE", "c"]}]}, "SUBSCRIBE is refused"),
        ({**_PARENT, "commands": [{"command": ["PSUBSCRIBE", "c*"]}]}, "PSUBSCRIBE is refused"),
        ({**_PARENT, "commands": [{"command": ["SSUBSCRIBE", "c"]}]}, "SSUBSCRIBE is refused"),
        ({"commands": [{"command": ["REPLICAOF", "NO", "ONE"]}]}, "REPLICAOF is refused: it changes the deployment's topology"),
        ({"commands": [{"command": ["CLUSTER", "RESET"]}]}, "CLUSTER RESET is refused"),
        ({"commands": [{"command": ["REPLCONF", "ACK", 0]}]}, "REPLCONF is refused"),
        ({"commands": [{"command": ["PSYNC", "?", -1]}]}, "PSYNC is refused"),
        ({"modules": ["/m.so"], "commands": [{"command": ["PING"]}]}, "modules needs compare_with or modules_parent: the deployment's instances load modules through deployment.server_config.loadmodule"),
        ({"modules_parent": ["/m.so"], "commands": [{"command": ["PING"]}]}, "modules_parent needs modules"),
        ({**_PARENT, "server_config": {"loadmodule": "/m.so"}, "commands": [{"command": ["PING"]}]}, r"server_config\.loadmodule: list the modules in modules"),
    ],
)  # fmt: skip
def test_a_bad_sequence_config_fails_to_compile(config, error):
    spec = ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.commands", id="seq", config=config)
    with pytest.raises(ConfigError, match=error):
        registration_for("valkey.action.commands").resolve_config(spec, unit_plan((spec,)))


@pytest.mark.parametrize(
    "config",
    [
        {**_PARENT, "commands": [{"command": ["REPLICAOF", "NO", "ONE"]}, {"command": ["CLUSTER", "RESET"]}]},
        {**_PARENT, "commands": [{"command": ["DEBUG", "SEGFAULT"]}, {"command": ["DEBUG", "SLEEP", 1]}]},
        {**_PARENT, "commands": [{"command": ["CONFIG", "SET", "maxmemory", "1mb"]}]},
        {**_PARENT, "cluster": True, "commands": [{"command": ["RESTORE", "k", 0, {"$base64": "AA4="}]}]},
        {"commands": [{"command": ["PING"], "expect": {"equals": ["a", "b"]}}]},
        # Two module builds on the deployment's binary: private servers, so what they take is allowed.
        {"modules": ["/n.so"], "modules_parent": ["/o.so"], "commands": [{"command": ["REPLICAOF", "NO", "ONE"]}]},
    ],
)
def test_the_private_servers_take_what_the_deployment_refuses(config):
    spec = ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.commands", id="seq", config=config)
    resolved_config = registration_for("valkey.action.commands").resolve_config(spec, unit_plan((spec,)))
    assert resolved_config["commands"][0]["compare"] == "equal"


def test_a_yaml_binary_argument_loads_as_base64_data():
    document = load_yaml('command: [RESTORE, k, 0, !!binary "AA4BAgMEBQ=="]', "x")
    assert document == {"command": ["RESTORE", "k", 0, {"$base64": "AA4BAgMEBQ=="}]}


# ---------------------------------------------------------------------------
# Modules
# ---------------------------------------------------------------------------


def _loaded(arguments):
    return [value for name, value in zip(arguments[::2], arguments[1::2]) if name == "--loadmodule"]


def test_both_servers_load_the_modules_and_the_parent_loads_modules_parent_when_given(builds):
    result, value = run(builds, [{"command": ["PING"]}], modules=["/m/a.so", "/m/b.so"])

    assert result.status.value == "PASS", result.summary
    assert value["modules"] == {"candidate": ["/m/a.so", "/m/b.so"], "parent": ["/m/a.so", "/m/b.so"]}
    # Every start loads them, the probes' included, so an option is judged with the modules loaded.
    for build in BUILDS.values():
        assert build.starts and all(_loaded(arguments) == ["/m/a.so", "/m/b.so"] for arguments in build.starts)

    result, value = run(builds, [{"command": ["PING"]}], modules=["/m/new.so"], modules_parent=["/m/old.so"])

    assert result.status.value == "PASS", result.summary
    assert value["modules"] == {"candidate": ["/m/new.so"], "parent": ["/m/old.so"]}
    assert _loaded(BUILDS[builds["candidate"]].starts[-1]) == ["/m/new.so"]
    assert _loaded(BUILDS[builds["parent"]].starts[-1]) == ["/m/old.so"]


def test_modules_parent_without_compare_with_compares_two_module_builds_on_the_deployments_binary(builds):
    BUILDS[builds["candidate"]].replies = {("M.GET", "k"): b"fixed"}
    spec = _spec(
        modules=["/m/new.so"],
        modules_parent=["/m/old.so"],
        commands=[{"command": ["M.GET", "k"], "expect_difference": True, "expect": {"equals": "fixed"}}],
    )
    context = FakeContext(spec, deployment={"valkey_binary": builds["candidate"]})

    result = actions.CommandsAction().run(context, spec)

    # Both servers ran the candidate build, one per module build; the parent build never started.
    assert result.status.value == "ERROR", result.summary
    assert result.diagnostics["reason"] == "unreached-change", "one fake build answers both the same"
    candidate = BUILDS[builds["candidate"]]
    assert {tuple(_loaded(arguments)) for arguments in candidate.starts} == {("/m/new.so",), ("/m/old.so",)}
    assert candidate.connections == 2 and not BUILDS[builds["parent"]].starts
