"""``valkey.action.commands`` without Valkey processes: fake private servers of two builds behind the real raw
connection, so arguments go out as bytes and replies come back as the server sent them, and the verdicts."""

import hashlib
import os
import shutil
import subprocess
import tempfile

import libvalkey
import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import InvalidResponse, ResponseError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from tests.valkey_experiments_support import registration_for, resolved, unit_plan
from valkey_fuzzer._data import canonical_json, to_plain_data
from valkey_fuzzer.components import actions, capabilities, checks, server
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.schema import ConfigError, load_yaml
from valkey_fuzzer.testing import FakeContext

# What a fake build does with a command (see Build).
DIE, TIMEOUT, SILENT, CLOSE = "<die>", "<timeout>", "<silent>", "<close>"


class Error(str):
    """An error reply."""


class ByResp(dict):
    """A reply by the protocol of the connection it goes to: ``{2: ..., 3: ...}``."""


class Wire(bytes):
    """A reply's bytes as the server sends them, which the client parses as valkey-py does, with libvalkey."""


class Build:
    """A fake valkey-server build: the options it rejects at start, and its reply to each command, keyed by
    the arguments as sent (strings, or bytes for a binary argument; raw bytes as they are); an unlisted command
    gets ``OK`` (CLUSTER MYID the server's ID, CLUSTER INFO a state ok knowing every server of the build;
    SUBSCRIBE and PUBLISH work, on the connections of one server). A
    reply may be an :class:`Error`, a :class:`Wire`, :data:`DIE` (the server exits without answering), :data:`CLOSE` (it closes the
    connection without answering), :data:`TIMEOUT`,
    :data:`SILENT` (no reply begins), or a function of the :class:`FakeServer` that returns the reply."""

    def __init__(self, replies=None, rejects=()):
        self.replies, self.rejects = dict(replies or {}), set(rejects)
        # Its servers exit at a second start, as from a config file they cannot read.
        self.restart_exits = False
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
        if self.returncode is None and timeout is not None:
            raise subprocess.TimeoutExpired("valkey-server", timeout)
        return self.returncode


class FakeServer:
    """Stands in for PrivateServer: starts unless its build rejects an option, and logs a line."""

    def __init__(self, binary, arguments=(), *, timeout_s=15.0, prefix="x-", files=None, config_file=False):
        self.binary, self.arguments, self.build = binary, list(arguments), BUILDS[binary]
        self.files, self.config_file, self.shut_downs = dict(files or {}), config_file, 0
        self.workdir = tempfile.mkdtemp(prefix=prefix)
        # A node of a side, not a probe of whether the build starts.
        self.node = prefix == "vfc-"
        self.id = hashlib.sha1(self.workdir.encode()).hexdigest()
        self.port = int(self.option("port") or 0)
        self.sent: list[tuple] = []
        self.wires: list[FakeWire] = []
        self.socket_path = os.path.join(self.workdir, "s.sock")
        self.log_path = os.path.join(self.workdir, "server.log")
        self.output_path = os.path.join(self.workdir, "output.log")
        self.process, self.client, self.stopped = FakeProcess(), None, False

    def start(self):
        self.build.starts.append(self.arguments)
        if self.shut_downs and self.build.restart_exits:
            with open(self.log_path, "a") as log:
                log.write("1:M 03 Nov 2026 10:00:02.000 # Fatal error, can't open config file 'valkey.conf'\n")
            self.process.returncode = 1
            return "Fatal error, can't open config file 'valkey.conf'"
        rejected = [name for name in self.arguments[::2] if name[2:] in self.build.rejects]
        if rejected:
            return f"*** FATAL CONFIG FILE ERROR *** Bad directive or wrong number of arguments: {rejected[0]}"
        with open(self.log_path, "w") as log:
            log.write(f"1:M 03 Nov 2026 10:00:00.000 * Ready to accept connections ({self.binary})\n")
        with open(self.output_path, "w") as output:
            output.write("")
        SERVERS[self.socket_path] = self
        return None

    def shut_down(self, timeout_s):
        self.shut_downs += 1
        return True

    def stop(self):
        self.stopped = True
        shutil.rmtree(self.workdir, ignore_errors=True)

    def option(self, name):
        names = self.arguments[::2]
        return self.arguments[names.index(f"--{name}") * 2 + 1] if f"--{name}" in names else None

    def die(self):
        with open(self.log_path, "a") as log:
            log.write("1:M 03 Nov 2026 10:00:01.000 # Valkey 9.0.0 crashed by signal: 11, si_code: 0\n")
        self.process.returncode = -11


class FakeWire:
    """A valkey-py connection to one fake server: ``send_command``, ``send_packed_command``, ``can_read``,
    ``read_response``, ``disconnect``, and its own ``_sock``, which ``recv`` a :class:`Wire` reply's bytes."""

    def __init__(self, server, resp=2):
        self.server, self.pending, self.resp = server, None, resp
        self._sock = self
        # Its channels, and the messages published to them, waiting to be read.
        self.channels: set[str] = set()
        self.waiting: list = []
        server.build.connections += 1
        server.wires.append(self)

    def send_command(self, *args):
        self.pending = tuple(item if isinstance(item, bytes) else str(item) for item in args)
        self.server.build.sent.append(self.pending)
        self.server.sent.append(self.pending)

    def send_packed_command(self, command, check_health=True):
        self.pending = b"".join(command)
        self.server.build.sent.append(self.pending)
        self.server.sent.append(self.pending)

    def can_read(self, timeout=0):
        if self.pending is None:
            return bool(self.waiting)
        return self.server.build.replies.get(self.pending) != SILENT

    def read_response(self, push_request=False):
        if self.pending is None:
            return self.waiting.pop(0)
        pending, self.pending = self.pending, None
        known = len(_nodes(self.server.build))
        defaults = {
            ("CLUSTER", "MYID"): self.server.id.encode(),
            ("CLUSTER", "INFO"): f"cluster_state:ok\r\ncluster_known_nodes:{known}\r\n".encode(),
        }
        if pending[0] == "SUBSCRIBE":
            self.channels.add(pending[1])
            defaults[pending] = [b"subscribe", pending[1].encode(), len(self.channels)]
        if pending[0] == "PUBLISH":
            receivers = [wire for wire in self.server.wires if pending[1] in wire.channels]
            for wire in receivers:
                wire.waiting.append([b"message", pending[1].encode(), pending[2].encode()])
            defaults[pending] = len(receivers)
        reply = self.server.build.replies.get(pending, defaults.get(pending, b"OK"))
        if callable(reply):
            reply = reply(self.server)
        if isinstance(reply, ByResp):
            reply = reply[self.resp]
        if reply == DIE:
            self.server.die()
            raise ValkeyConnectionError("Connection closed by server.")
        if reply == TIMEOUT:
            raise ValkeyTimeoutError("Timeout reading from socket")
        if reply == CLOSE:
            raise ValkeyConnectionError("Connection closed by server.")
        if isinstance(reply, Error):
            raise ResponseError(str(reply))
        if isinstance(reply, Wire):
            reader = libvalkey.Reader(protocolError=InvalidResponse, replyError=ResponseError)
            reader.feed(reply)
            return reader.gets()
        return reply

    def settimeout(self, timeout):
        pass

    def recv(self, size):
        pending, self.pending = self.pending, None
        return bytes(self.server.build.replies.get(pending, b""))[:size] if pending is not None else b""

    def disconnect(self):
        pass


def fake_open_raw_connection(resp, timeout_s, *, path=None, host=None, port=None):
    server = SERVERS[path] if path is not None else SERVERS[f"{host}:{port}"]
    return actions.RawConnection(FakeWire(server, resp))


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
    # The candidate replies differently from the parent, and wrongly: the change broke it.
    BUILDS[builds["candidate"]].replies = {("LRANGE", "l", "0", "-1"): [b"a", b"x"]}
    BUILDS[builds["parent"]].replies = {("LRANGE", "l", "0", "-1"): [b"a", b"b"]}
    commands = [
        {"command": ["LRANGE", "l", 0, -1], "expect_difference": True, "expect": {"equals": ["a", "b"]}},
        {"command": ["PING"], "expect": {"error": True}},
    ]
    result, _ = run(builds, commands)

    assert result.status.value == "FAIL"
    assert 'command 0 (LRANGE l 0 -1) replied {"reply":["a","x"]}, not {"equals":["a","b"]}' in result.summary
    # PING violates its expect alike on both builds: not about the change, so not part of the failure.
    assert "command 1 (PING)" not in result.summary


def test_an_expect_both_builds_violate_alike_is_disputed_not_fail(builds):
    # As in a proposed test for valkey-json#112: the expectation is wrong on both builds.
    BUILDS[builds["candidate"]].replies = {("LRANGE", "l", "0", "-1"): [b"a", b"x"]}
    BUILDS[builds["parent"]].replies = {("LRANGE", "l", "0", "-1"): [b"a", b"x"]}
    commands = [{"command": ["LRANGE", "l", 0, -1], "expect": {"equals": ["a", "b"]}}]
    result, _ = run(builds, commands)

    assert result.status.value == "ERROR"
    assert to_plain_data(result.diagnostics) == {
        "reason": "expect-disputed",
        "commands": [
            {
                "index": 0,
                "command": "LRANGE l 0 -1",
                "expect": {"equals": ["a", "b"]},
                "candidate": {"reply": ["a", "x"]},
                "parent": {"reply": ["a", "x"]},
            }
        ],
    }


@pytest.mark.parametrize(
    ("reply", "equals", "holds"),
    [
        # As in the proposed tests for valkey-json#109 and #120: JSON.GET's text against the value it holds.
        (b"[]", [], True),
        (b"[[0]]", [[0]], True),
        (b'{"b": [1, 2], "a": 1.0}', '{"a":1,"b":[1,2]}', True),
        (b"[true]", [1], False),
        ([b"[1]", None], [[1], None], True),
        ([b"[1]", 2], ["[1]", 2], True),
        (2, 2, True),
        (b"OK", "OK", True),
    ],
)
def test_under_json_an_expect_holds_for_the_value_the_reply_holds(builds, reply, equals, holds):
    for build in builds.values():
        BUILDS[build].replies = {("REPLY",): reply}
    result, _ = run(builds, [{"command": ["REPLY"], "compare": "json", "expect": {"equals": equals}}])

    assert (result.status.value == "PASS") is holds, result.summary


@pytest.mark.parametrize(("reply", "holds"), [(None, True), (b"", False), (0, False)])
def test_an_expect_equal_to_null_holds_for_a_null_reply(builds, reply, holds):
    # As in a proposed test for valkey-io/valkey#4424, which validation refused.
    for build in builds.values():
        BUILDS[build].replies = {("REPLY",): reply}
    result, _ = run(builds, [{"command": ["REPLY"], "expect": {"equals": None}}])

    assert (result.status.value == "PASS") is holds, result.summary


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
        build.replies = {("CLUSTER", "INFO"): b"cluster_state:ok\r\ncluster_known_nodes:1\r\n"}
    result, value = run(builds, [{"command": ["SET", "k", "v"], "expect": {"equals": "OK"}}], cluster=True)

    assert result.status.value == "PASS", result.summary
    assert (value["cluster"], value["nodes"]) == (True, 1)
    ports = set()
    for build in BUILDS.values():
        running = build.starts[-1]
        assert running[running.index("--cluster-enabled") + 1] == "yes"
        ports.add(running[running.index("--cluster-port") + 1])
        ports.add(running[running.index("--port") + 1])
        assert running[running.index("--bind") + 1] == "127.0.0.1"
        assert build.sent[:3] == [("CLUSTER", "ADDSLOTSRANGE", "0", "16383"), ("CLUSTER", "MYID"), ("CLUSTER", "INFO")]
        assert build.sent[-1] == ("SET", "k", "v")
    assert len(ports) == 4
    assert _replies(value["candidate"]) == [{"reply": "OK"}]


def test_a_cluster_that_never_reaches_ok_leaves_the_run_untested(builds):
    for build in BUILDS.values():
        build.replies = {("CLUSTER", "INFO"): b"cluster_state:fail\r\n"}
    result, value = run(builds, [{"command": ["PING"]}], cluster=True, timeout_s=0.2)

    assert result.status.value == "ERROR" and value is None
    assert result.summary.startswith("the candidate server: cluster_state is not ok with 1 known node(s) within 0.2s")


# ---------------------------------------------------------------------------
# Several nodes, and placeholders
# ---------------------------------------------------------------------------


def _nodes(build):
    """The fake servers of ``build`` in the order they started: its nodes."""

    return [server for server in SERVERS.values() if server.build is build and server.node]


def test_several_nodes_form_one_cluster_and_each_command_goes_to_its_node(builds):
    commands = [{"command": ["SET", "k", "v"]}, {"command": ["CLUSTER", "COUNTKEYSINSLOT", 1], "node": 2}]
    result, value = run(builds, commands, cluster=True, nodes=3)

    assert result.status.value == "PASS", result.summary
    assert value["nodes"] == 3
    ports = set()
    for build in BUILDS.values():
        first, *others = nodes = _nodes(build)
        assert len(nodes) == 3 and all(node.option("cluster-enabled") == "yes" for node in nodes)
        ports |= {(node.option("port"), node.option("cluster-port")) for node in nodes}
        # Node 0 serves every slot, and the others meet it.
        assert ("CLUSTER", "ADDSLOTSRANGE", "0", "16383") in first.sent
        for node in others:
            assert ("CLUSTER", "MEET", "127.0.0.1", first.option("port"), first.option("cluster-port")) in node.sent
        assert first.sent[-1] == ("SET", "k", "v") and nodes[2].sent[-1] == ("CLUSTER", "COUNTKEYSINSLOT", "1")
    assert len(ports) == 6


def test_placeholders_name_each_sides_own_nodes_and_replies_name_them_back(builds):
    def nodes_line(server):
        return f"{server.id} 127.0.0.1:{server.port}@{server.option('cluster-port')} myself,master".encode()

    for build in BUILDS.values():
        build.replies = {
            ("CLUSTER", "NODES"): nodes_line,
            ("CLUSTER", "SLOTS"): lambda server: [[0, 1, [b"127.0.0.1", server.port, server.id.encode()]]],
        }
    commands = [
        {"command": ["CLUSTER", "SETSLOT", 1, "NODE", "<node.1.id>"]},
        {"command": ["CLUSTER", "MEET", "<node.1.host>", "<node.1.port>", "<node.1.cluster-port>"]},
        {"command": ["CLUSTER", "NODES"], "node": 1, "expect": {"contains": "<node.1.id> 127.0.0.1:<node.1.port>@"}},
        {"command": ["CLUSTER", "SLOTS"], "node": 1},
    ]
    result, value = run(builds, commands, cluster=True, nodes=2)

    assert result.status.value == "PASS", result.summary
    for build in BUILDS.values():
        first, second = _nodes(build)
        assert ("CLUSTER", "SETSLOT", "1", "NODE", second.id) in first.sent
        assert ("CLUSTER", "MEET", "127.0.0.1", str(second.port), second.option("cluster-port")) in first.sent
    # Each side's own IDs and ports, named back, so the sides compare equal.
    assert value["candidate"]["digest"] == value["parent"]["digest"]
    assert _replies(value["candidate"])[2:] == [
        {"reply": "<node.1.id> 127.0.0.1:<node.1.port>@<node.1.cluster-port> myself,master"},
        {"reply": [[0, 1, ["127.0.0.1", "<node.1.port>", "<node.1.id>"]]]},
    ]


def test_standalone_nodes_take_their_topology_from_the_sequence(builds):
    result, _ = run(builds, [{"command": ["REPLICAOF", "<node.0.host>", "<node.0.port>"], "node": 1}], nodes=2)

    assert result.status.value == "PASS", result.summary
    for build in BUILDS.values():
        first, second = _nodes(build)
        assert first.option("cluster-enabled") is None and first.sent == []
        assert second.sent == [("REPLICAOF", "127.0.0.1", str(first.port))]


def test_a_node_that_dies_fails_the_candidate_with_its_own_logs_kept(builds):
    BUILDS[builds["candidate"]].replies = {("DEBUG", "SEGFAULT"): DIE}
    result, value = run(builds, [{"command": ["PING"]}, {"command": ["DEBUG", "SEGFAULT"], "node": 1}], nodes=2)

    assert result.status.value == "FAIL" and result.crash
    assert (
        result.summary == "the candidate's node 1 exited on command 1 (DEBUG SEGFAULT on node 1): crashed by signal: 11"
    )
    assert (value["candidate"]["exited"], value["candidate"]["exited_node"]) == (-11, 1)
    kept = {os.path.basename(path) for path in result.evidence[0].artifacts}
    assert {"candidate.node0.server.log", "candidate.node1.server.log", "parent.node1.output.log"} <= kept


# ---------------------------------------------------------------------------
# Waiting
# ---------------------------------------------------------------------------


def _done_after(attempts, reply):
    """A reply that is ``pending`` on each server until its ``attempts``-th time, ``reply`` from then on."""

    seen = {}

    def answer(server):
        seen[server.workdir] = seen.get(server.workdir, 0) + 1
        return reply if seen[server.workdir] >= attempts else b"pending"

    return answer


def test_a_command_with_wait_s_is_sent_again_until_its_expect_holds(builds):
    for build in BUILDS.values():
        build.replies = {("CLUSTER", "GETSLOTMIGRATIONS"): _done_after(3, b"success")}
    commands = [
        {"command": ["CLUSTER", "GETSLOTMIGRATIONS"], "wait_s": 5, "expect": {"equals": "success"}},
        {"command": ["PING"]},
    ]
    result, value = run(builds, commands)

    assert result.status.value == "PASS", result.summary
    assert _replies(value["candidate"]) == [{"reply": "success"}, {"reply": "OK"}]
    for build in BUILDS.values():
        assert build.sent == [("CLUSTER", "GETSLOTMIGRATIONS")] * 3 + [("PING",)]


def test_a_wait_that_times_out_records_the_last_reply_against_its_expect(builds):
    BUILDS[builds["candidate"]].replies = {("INFO", "replication"): b"master_link_status:down"}
    BUILDS[builds["parent"]].replies = {("INFO", "replication"): b"master_link_status:up"}
    commands = [{"command": ["INFO", "replication"], "wait_s": 0.3, "expect": {"contains": "status:up"}}]
    result, _ = run(builds, commands)

    assert result.status.value == "FAIL"
    assert 'command 0 (INFO replication) replied {"reply":"master_link_status:down"}' in result.summary
    assert len(BUILDS[builds["candidate"]].sent) > 1 and len(BUILDS[builds["parent"]].sent) == 1


# ---------------------------------------------------------------------------
# Raw bytes
# ---------------------------------------------------------------------------


def test_raw_bytes_go_out_as_they_are_and_one_reply_or_none_is_recorded(builds):
    nul = b"SET foo bar\x00 ex 100\r\n"
    BUILDS[builds["candidate"]].replies = {nul: Error("ERR Protocol error: embedded NUL byte in inline request")}
    BUILDS[builds["parent"]].replies = {nul: SILENT}
    for build in BUILDS.values():
        build.replies[b"PING\r\n"] = b"PONG"
    commands = [
        {"raw": "PING\r\n", "expect": {"equals": "PONG"}},
        {"raw": {"$base64": "UElORw0K"}},
        {"raw": "SET foo bar\x00 ex 100\r\n", "expect_difference": True, "expect": {"error": "NUL"}},
    ]
    result, value = run(builds, commands)

    assert result.status.value == "PASS", result.summary
    assert value["reached"] == [2]
    assert BUILDS[builds["candidate"]].sent == [b"PING\r\n", b"PING\r\n", nul]
    assert _replies(value["candidate"]) == [
        {"reply": "PONG"},
        {"reply": "PONG"},
        {"error": "ERR Protocol error: embedded NUL byte in inline request"},
    ]
    assert _replies(value["parent"])[2] == {"no_reply": True}


def test_raw_bytes_no_reply_answers_violate_the_candidates_expect(builds):
    BUILDS[builds["candidate"]].replies = {b"PI": SILENT}
    result, _ = run(builds, [{"raw": "PI", "expect": {"error": True}}])

    assert result.status.value == "FAIL"
    assert 'command 0 (raw "PI") replied {"no_reply":true}, not {"error":true}' in result.summary


def test_a_connection_a_live_server_closed_is_its_answer_and_opens_anew(builds, monkeypatch):
    monkeypatch.setattr(actions, "_EXIT_WAIT_S", 0.01)
    BUILDS[builds["candidate"]].replies = {("RESET",): CLOSE}
    BUILDS[builds["parent"]].replies = {("RESET",): DIE}
    commands = [
        {"command": ["CLUSTER", "SYNCSLOTS", "ESTABLISH"], "connection": "job"},
        {"command": ["RESET"], "connection": "job", "expect_difference": True, "expect": {"exists": False}},
        {"command": ["PING"], "connection": "job"},
    ]
    result, value = run(builds, commands)

    # The parent died on it; the candidate closed the connection and went on, on a new one.
    assert result.status.value == "PASS", result.summary
    assert value["reached"] == [1] and value["parent"]["exited"] == -11
    assert _replies(value["candidate"]) == [{"reply": "OK"}, {"closed": True}, {"reply": "OK"}]
    assert BUILDS[builds["candidate"]].connections == 3


def test_a_reply_the_client_cannot_parse_is_a_protocol_error_with_its_bytes_on_either_build(builds):
    BUILDS[builds["candidate"]].replies = {("ZSCORE", "z", "m"): Wire(b",abc\r\n")}
    commands = [{"command": ["SELECT", 3]}, {"command": ["ZSCORE", "z", "m"]}, {"command": ["PING"]}]
    result, value = run(builds, commands, resp=3)

    assert result.status.value == "FAIL", result.summary
    assert result.summary == "command 1 (ZSCORE z m) got a reply the client cannot parse: Bad double value"
    broken = {"protocol_error": "Bad double value", "raw": ",abc\\r\\n"}
    assert to_plain_data(result.diagnostics) == {
        "reason": "protocol-error",
        "commands": [{"index": 1, "command": "ZSCORE z m", **broken}],
    }
    assert _replies(value["candidate"]) == [{"reply": "OK"}, broken, {"reply": "OK"}]
    # Sent again on a fresh connection in the same database, then the next command on another one.
    assert BUILDS[builds["candidate"]].sent[-3:] == [("SELECT", "3"), ("ZSCORE", "z", "m"), ("PING",)]
    assert BUILDS[builds["candidate"]].connections == 3

    # The same malformed reply on the parent is still a finding.
    BUILDS[builds["parent"]].replies = dict(BUILDS[builds["candidate"]].replies)
    result, _ = run(builds, commands, resp=3)
    assert result.status.value == "FAIL" and result.diagnostics["reason"] == "protocol-error"


# ---------------------------------------------------------------------------
# Connections and Pub/Sub
# ---------------------------------------------------------------------------


def test_a_named_connection_opens_on_first_use_and_a_read_takes_what_waits_on_it(builds):
    commands = [
        {"command": ["SUBSCRIBE", "news"], "connection": "sub", "expect": {"equals": ["subscribe", "news", 1]}},
        {"command": ["PUBLISH", "news", "hi"], "expect": {"equals": 1}},
        {"command": ["PUBLISH", "news", "again"]},
        {
            "read": True,
            "connection": "sub",
            "expect": {"equals": [["message", "news", "hi"], ["message", "news", "again"]]},
        },
        {"read": True, "connection": "sub", "expect": {"equals": []}},
        {"read": True, "connection": "idle"},
    ]
    result, value = run(builds, commands)

    assert result.status.value == "PASS", result.summary
    for build in BUILDS.values():
        assert build.connections == 3
        (server,) = _nodes(build)
        main, sub, _ = server.wires
        assert sub.channels == {"news"} and not main.channels
    assert _replies(value["candidate"])[5] == {"reply": []}


def test_a_summary_names_a_commands_connection_and_node(builds):
    BUILDS[builds["candidate"]].replies = {("SUBSCRIBE", "news"): Error("ERR no")}
    result, _ = run(builds, [{"command": ["SUBSCRIBE", "news"], "connection": "sub", "node": 1}], nodes=2)

    assert result.status.value == "FAIL"
    assert result.summary.startswith("command 0 (SUBSCRIBE news on connection sub on node 1) replied")


# ---------------------------------------------------------------------------
# Files, the config file, and restarts
# ---------------------------------------------------------------------------


def test_files_go_into_every_servers_directory_and_nodes_start_from_a_config_file(builds):
    files = {"users.acl": "user default on nopass ~* +@all\n", "rdb/dump.rdb": {"$base64": "/wD/"}}
    result, _ = run(
        builds, [{"command": ["ACL", "LOAD"]}], files=files, server_config={"aclfile": "users.acl"}, nodes=2
    )

    assert result.status.value == "PASS", result.summary
    for build in BUILDS.values():
        assert all(running[running.index("--aclfile") + 1] == "users.acl" for running in build.starts)
        written = [server for server in SERVERS.values() if server.build is build]
        # The probes of whether the build starts get them too, since an option may name one.
        assert any(not server.node for server in written)
        assert all(
            server.files == {"users.acl": b"user default on nopass ~* +@all\n", "rdb/dump.rdb": b"\xff\x00\xff"}
            for server in written
        )
        assert [server.config_file for server in written if server.node] == [True, True]


def test_a_restart_stops_the_node_and_starts_it_again_with_new_connections(builds):
    commands = [
        {"command": ["CONFIG", "REWRITE"]},
        {"command": ["SUBSCRIBE", "c"], "connection": "sub", "node": 1},
        {"restart": True, "node": 1},
        {"command": ["CONFIG", "GET", "maxmemory"], "node": 1},
    ]
    result, value = run(builds, commands, nodes=2)

    assert result.status.value == "PASS", result.summary
    assert _replies(value["candidate"])[2] == {"reply": "OK"}
    for build in BUILDS.values():
        first, second = _nodes(build)
        assert (first.shut_downs, second.shut_downs) == (0, 1)
        # The main and sub connections before the restart, a new main one after it.
        assert len(second.wires) == 3 and second.sent[-1] == ("CONFIG", "GET", "maxmemory")


def test_a_candidate_that_does_not_come_back_from_a_restart_fails(builds):
    BUILDS[builds["candidate"]].restart_exits = True
    result, value = run(builds, [{"restart": True}, {"command": ["PING"]}])

    assert result.status.value == "FAIL" and result.crash
    assert result.summary.startswith("the candidate exited on command 0 (restart): ")
    assert result.summary.endswith("# Fatal error, can't open config file 'valkey.conf'")
    assert _replies(value["candidate"]) == [
        {"error": "the server exited at startup: Fatal error, can't open config file 'valkey.conf'", "unanswered": True}
    ]


def test_a_config_file_line_means_what_the_command_line_does():
    assert server.config_value('127.0.0.1 1\nmaxmemory "1"\\\x00é') == '"127.0.0.1 1\\nmaxmemory \\"1\\"\\\\\\x00é"'
    assert server.config_value("") == '""'


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
    commands = [{"command": ["PING"]}, {"command": ["DBSIZE"], "expect": {"min": 1}, "connection": "other"}]
    spec = _spec(instances="all", commands=commands)
    context = FakeContext(spec, services={"valkey.deployment": handle})
    result = actions.CommandsAction().run(context, spec)

    assert result.status.value == "PASS", result.summary
    assert [item.value["instance_id"] for item in result.evidence] == ["valkey-0", "valkey-1"]
    assert all(item.value["parent"] is None and item.value["cluster"] is False for item in result.evidence)
    assert BUILDS[builds["candidate"]].connections == 4
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


def test_without_compare_with_a_reply_the_client_cannot_parse_fails(builds):
    handle = FakeHandle()
    SERVERS["127.0.0.1:7000"] = FakeServer(builds["candidate"])
    BUILDS[builds["candidate"]].replies = {("ZSCORE", "z", "m"): Wire(b",abc\r\n")}
    spec = _spec(resp=3, commands=[{"command": ["ZSCORE", "z", "m"]}, {"command": ["PING"]}])
    context = FakeContext(spec, services={"valkey.deployment": handle})
    result = actions.CommandsAction().run(context, spec)

    assert result.status.value == "FAIL", result.summary
    assert to_plain_data(result.diagnostics) == {
        "reason": "protocol-error",
        "commands": [
            {
                "instance_id": "valkey-0",
                "index": 0,
                "command": "ZSCORE z m",
                "protocol_error": "Bad double value",
                "raw": ",abc\\r\\n",
            }
        ],
    }
    assert to_plain_data(result.evidence[0].value)["candidate"]["replies"][1] == {"index": 1, "reply": "OK"}


# ---------------------------------------------------------------------------
# Compile-time checks
# ---------------------------------------------------------------------------

_PARENT = {"compare_with": "/opt/parent/valkey-server"}


@pytest.mark.parametrize(
    ("config", "error"),
    [
        ({"commands": []}, "at least one command"),
        ({"commands": [{"command": []}]}, r"commands\[0\]\.command must be a non-empty list"),
        ({"commands": [{"expect": {"equals": 1}}]}, r"commands\[0\] needs one of command, raw, read, and restart"),
        ({"commands": [{"command": ["PING"], "raw": "PING\r\n"}]}, r"commands\[0\] needs one of command, raw, read, and restart"),
        ({"commands": [{"raw": ""}]}, r"commands\[0\]\.raw must be a non-empty string or a !!binary value"),
        ({"commands": [{"raw": {"x": 1}}]}, r"commands\[0\]\.raw must be a non-empty string"),
        ({"commands": [{"raw": "PING\r\n", "wait_s": 1, "expect": {"equals": "PONG"}}]}, "wait_s sends a command again; raw bytes, a read, and a restart happen once"),
        ({**_PARENT, "commands": [{"raw": "CLUSTER SETSLOT 1 NODE <node.0.id>\r\n"}]}, r"<node\.0\.id> needs cluster"),
        ({"commands": [{"command": ["SET", "k", True]}]}, r"commands\[0\]\.command must be a non-empty list"),
        ({"commands": [{"command": ["PING"], "expect": {}}]}, "needs one predicate"),
        ({"commands": [{"command": ["PING"], "expect": {"error": True, "equals": 1}}]}, "cannot be combined"),
        ({"commands": [{"command": ["PING"], "compare": "sorted"}]}, "must be one of"),
        ({"commands": [{"command": ["PING"], "expect_difference": True}]}, "needs compare_with"),
        ({"commands": [{"command": ["PING"], "wait_s": 5}]}, r"commands\[0\]: wait_s needs an expect to wait for"),
        ({"commands": [{"command": ["PING"], "wait_s": 0, "expect": {"equals": "PONG"}}]}, "wait_s"),
        ({"nodes": 2, "commands": [{"command": ["PING"]}]}, "nodes needs compare_with or modules_parent"),
        ({**_PARENT, "nodes": 7, "commands": [{"command": ["PING"]}]}, "nodes"),
        ({**_PARENT, "nodes": 2, "commands": [{"command": ["PING"], "node": 2}]}, r"commands\[0\]\.node is 2, but each side runs on 2 node\(s\)"),
        ({"commands": [{"command": ["PING"], "node": 1}]}, r"commands\[0\]\.node is 1, but each side runs on 1 node\(s\)"),
        ({**_PARENT, "nodes": 2, "cluster": True, "commands": [{"command": ["ECHO", "<node.2.id>"]}]}, r"commands\[0\]: <node\.2\.id> names node 2, but each side runs on 2 node\(s\)"),
        ({**_PARENT, "commands": [{"command": ["ECHO", "<node.0.ip>"]}]}, "<node.0.ip> names no field of a node; the fields are id, host, port, cluster-port"),
        ({**_PARENT, "commands": [{"command": ["ECHO", "<node.0.id>"]}]}, r"<node\.0\.id> needs cluster"),
        ({"commands": [{"command": ["ECHO", "x<node.0.port>"]}]}, "needs compare_with or modules_parent: it names a private server"),
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
        ({**_PARENT, "commands": [{"command": ["SUBSCRIBE", "c"]}]}, "SUBSCRIBE is refused on the main connection, which it would hold; send it on a connection of its own"),
        ({**_PARENT, "commands": [{"command": ["PSUBSCRIBE", "c*"], "connection": "main"}]}, "PSUBSCRIBE is refused on the main connection"),
        ({**_PARENT, "commands": [{"command": ["SSUBSCRIBE", "c"]}]}, "SSUBSCRIBE is refused on the main connection"),
        ({**_PARENT, "commands": [{"command": ["SUBSCRIBE", "a", "b"], "connection": "sub"}]}, r"commands\[0\]: SUBSCRIBE takes one channel: each is confirmed by a reply of its own"),
        ({**_PARENT, "commands": [{"command": ["UNSUBSCRIBE"]}]}, "UNSUBSCRIBE takes one channel"),
        ({"commands": [{"read": True, "command": ["PING"]}]}, r"commands\[0\] needs one of command, raw, read, and restart"),
        ({"commands": [{"read": True, "wait_s": 1, "expect": {"equals": 1}}]}, "wait_s sends a command again"),
        ({"commands": [{"command": ["PING"], "connection": ""}]}, r"commands\[0\]\.connection must name a connection"),
        ({"commands": [{"restart": True}]}, r"commands\[0\]: restart needs compare_with or modules_parent"),
        ({**_PARENT, "commands": [{"restart": True, "command": ["PING"]}]}, r"commands\[0\] needs one of command, raw, read, and restart"),
        ({**_PARENT, "commands": [{"restart": True, "connection": "sub"}]}, "restart restarts a node, every connection to it; it names none"),
        ({**_PARENT, "commands": [{"restart": True, "wait_s": 5, "expect": {"equals": "OK"}}]}, "wait_s sends a command again"),
        ({"files": {"a": "x"}, "commands": [{"command": ["PING"]}]}, "files needs compare_with or modules_parent"),
        ({**_PARENT, "files": {"../a": "x"}, "commands": [{"command": ["PING"]}]}, "files: '../a' must be a plain relative path inside the server's directory"),
        ({**_PARENT, "files": {"/etc/a": "x"}, "commands": [{"command": ["PING"]}]}, "files: '/etc/a' must be a plain relative path"),
        ({**_PARENT, "files": {"a/../b": "x"}, "commands": [{"command": ["PING"]}]}, "files: 'a/../b' must be a plain relative path"),
        ({**_PARENT, "files": {"./a": "x"}, "commands": [{"command": ["PING"]}]}, "files: './a' must be a plain relative path"),
        ({**_PARENT, "files": {"valkey.conf": "x"}, "commands": [{"command": ["PING"]}]}, "none of valkey.conf, server.log, output.log, s.sock"),
        ({**_PARENT, "files": {"a": 1}, "commands": [{"command": ["PING"]}]}, "files: 'a' must be a string or a !!binary value"),
        ({"commands": [{"command": ["REPLICAOF", "NO", "ONE"]}]}, "REPLICAOF is refused: it changes the deployment's topology"),
        ({"commands": [{"command": ["CLUSTER", "RESET"]}]}, "CLUSTER RESET is refused"),
        ({"commands": [{"command": ["REPLCONF", "ACK", 0]}]}, "REPLCONF is refused"),
        ({"commands": [{"command": ["PSYNC", "?", -1]}]}, "PSYNC is refused"),
        ({"modules": ["/m.so"], "commands": [{"command": ["PING"]}]}, "modules needs compare_with or modules_parent: the deployment's instances load modules through deployment.server_config.loadmodule"),
        ({"modules_parent": ["/m.so"], "commands": [{"command": ["PING"]}]}, "modules_parent needs modules"),
        ({**_PARENT, "server_config": {"loadmodule": "/m.so"}, "commands": [{"command": ["PING"]}]}, r"server_config\.loadmodule: list the modules in modules"),
        ({"checks": {"variants": True}, "commands": [{"command": ["PING"]}]}, "checks needs compare_with or modules_parent"),
        ({"modules": ["/n.so"], "modules_parent": ["/o.so"], "checks": {"compat": True}, "commands": [{"command": ["PING"]}]}, "checks.compat needs compare_with"),
        ({**_PARENT, "checks": {"concurrency": 17}, "commands": [{"command": ["PING"]}]}, "concurrency"),
        ({**_PARENT, "checks": {"acl": "yes"}, "commands": [{"command": ["PING"]}]}, "acl"),
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


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def _check(value, name):
    return value["checks"][name]


def _replicating(digest=b"d1"):
    """Every server's replication caught up, with ``digest`` (a reply, or a function of the server)."""

    for build in BUILDS.values():
        build.replies[("INFO", "replication")] = b"# Replication\r\nmaster_link_status:up\r\nmaster_repl_offset:42\r\n"
        build.replies[("DEBUG", "DIGEST")] = digest


def _encodings_off(server):
    return server.option("hash-max-listpack-entries") == "0"


def test_variants_compare_replies_as_multisets_of_their_resp2_values(builds):
    for build in BUILDS.values():
        build.replies = {
            ("HGETALL", "h"): ByResp({2: [b"f1", b"v1", b"f2", b"v2"], 3: {b"f2": b"v2", b"f1": b"v1"}}),
            ("ZRANGE", "z", "0", "-1", "WITHSCORES"): ByResp(
                {2: [b"a", b"1.5", b"b", b"2"], 3: [[b"a", 1.5], [b"b", 2.0]]}
            ),
            ("EVAL", "return true", "0"): ByResp({2: 1, 3: True}),
            ("SMEMBERS", "s"): lambda server: [b"b", b"a"] if _encodings_off(server) else [b"a", b"b"],
            ("OBJECT", "ENCODING", "s"): lambda server: b"hashtable" if _encodings_off(server) else b"listpack",
        }
    commands = [
        {"command": ["HGETALL", "h"]},
        {"command": ["ZRANGE", "z", 0, -1, "WITHSCORES"]},
        {"command": ["EVAL", "return true", 0]},
        {"command": ["SMEMBERS", "s"], "compare": "unordered"},
        {"command": ["OBJECT", "ENCODING", "s"]},
    ]
    result, value = run(builds, commands, checks={"variants": True})

    assert result.status.value == "PASS", result.summary
    assert _check(value, "variants") == {
        "outcome": "pass",
        "encodings": {"outcome": "pass"},
        "protocol": {"outcome": "pass"},
    }
    assert result.summary.endswith(", variants pass")
    encodings, protocol = [server for server in SERVERS.values() if server.node][2:]
    assert [encodings.option(name) for name in ("set-max-intset-entries", "list-max-listpack-size")] == ["0", "0"]
    assert {wire.resp for wire in protocol.wires} == {3} and protocol.option("hash-max-listpack-entries") is None
    kept = {os.path.basename(path) for path in result.evidence[0].artifacts}
    assert {"candidate.encodings.server.log", "candidate.protocol.server.log"} <= kept


def test_a_reply_that_changes_with_the_encoding_is_a_variant_mismatch(builds):
    for build in BUILDS.values():
        build.replies = {("SCARD", "s"): lambda server: 0 if _encodings_off(server) else 2}
    result, value = run(
        builds, [{"command": ["SADD", "s", 1, 2]}, {"command": ["SCARD", "s"]}], checks={"variants": True}
    )

    assert result.status.value == "FAIL" and not result.crash
    assert result.summary.startswith(
        'command 1 (SCARD s) replies otherwise with server_config {"hash-max-listpack-entries"'
    )
    assert result.diagnostics["reason"] == "variant-mismatch" and result.diagnostics["variant"] == "encodings"
    assert to_plain_data(result.diagnostics["commands"]) == [
        {"index": 1, "command": "SCARD s", "base": {"reply": 2}, "variant": {"reply": 0}}
    ]
    assert (_check(value, "variants")["outcome"], _check(value, "variants")["protocol"]) == (
        "fail",
        {"outcome": "pass"},
    )


@pytest.mark.parametrize(
    "command", [["DEBUG", "HTSTATS-KEY", "z", "full"], ["OBJECT", "FREQ", "z"], ["CONFIG", "GET", "*"]]
)
def test_variants_leave_internals_and_configuration_alone(builds, command):
    # As in the walk's test of valkey-io/valkey#5170: DEBUG HTSTATS-KEY reports a hashtable's buckets, by design
    # not the same once the encodings are off.
    for build in BUILDS.values():
        build.replies = {tuple(command): lambda server: "off" if _encodings_off(server) else "on"}
    result, _ = run(builds, [{"command": command}], checks={"variants": True})

    assert result.status.value == "PASS", result.summary


def test_encodings_are_not_varied_with_modules_loaded(builds):
    result, value = run(builds, [{"command": ["PING"]}], modules=["/m/a.so"], checks={"variants": True})

    assert result.status.value == "PASS", result.summary
    assert _check(value, "variants")["encodings"] == {"outcome": "skipped", "reason": "modules are loaded"}


def test_a_replica_of_node_0_reaches_the_primarys_digest(builds):
    _replicating()
    result, value = run(builds, [{"command": ["SET", "k", "v"]}], checks={"replica": True})

    assert result.status.value == "PASS", result.summary
    assert _check(value, "replica") == {"outcome": "pass"}
    primary, replica = _nodes(BUILDS[builds["candidate"]])[1:]
    assert primary.option("repl-diskless-sync-delay") == "0"
    assert ("REPLICAOF", "127.0.0.1", str(primary.port)) in replica.sent
    assert ("SET", "k", "v") in primary.sent and ("SET", "k", "v") not in replica.sent


def test_a_replica_whose_digest_stays_apart_diverged(builds, monkeypatch):
    monkeypatch.setattr(checks, "_SYNC_TIMEOUT_S", 0.2)
    # The fake replica never receives the write.
    _replicating(lambda server: b"d2" if ("SET", "k", "v") in server.sent else b"d1")
    result, value = run(builds, [{"command": ["SET", "k", "v"]}], checks={"replica": True})

    assert result.status.value == "FAIL" and not result.crash
    assert result.diagnostics["reason"] == "replica-diverged"
    assert result.diagnostics["primary"]["digest"] == {"reply": "d2"}
    assert result.diagnostics["replica"]["digest"] == {"reply": "d1"}
    assert _check(value, "replica")["outcome"] == "fail"


def test_the_replica_check_skips_several_nodes(builds):
    result, value = run(builds, [{"command": ["PING"]}], nodes=2, checks={"replica": True})

    assert result.status.value == "PASS", result.summary
    assert _check(value, "replica")["outcome"] == "skipped"


_CATEGORIES = {"SET": [b"@write", b"@string", b"@slow"], "GET": [b"@read", b"@string", b"@fast"], "PING": [b"@fast"]}
_ACL_SEQUENCE = [{"command": ["SET", "k", "v"]}, {"command": ["GET", "k"]}, {"command": ["PING"]}]


def _enforcing_acls(wrong=()):
    """Every build answers COMMAND INFO and GETKEYS for SET k v, GET k, and PING, and refuses each as the default
    user's last ACL rule says; the commands in ``wrong`` the other way."""

    def answer(name, reply):
        def respond(server):
            rule = next((sent[3:] for sent in reversed(server.sent) if sent[:3] == ("ACL", "SETUSER", "default")), ())
            refused = any(part[1:].encode() in _CATEGORIES[name] for part in rule if part.startswith("-")) or (
                "resetkeys" in rule and name != "PING"
            )
            if refused != (name in wrong):
                return Error(f"NOPERM User default has no permissions to run the '{name.lower()}' command")
            return reply

        return respond

    for build in BUILDS.values():
        build.replies = {
            ("SET", "k", "v"): answer("SET", b"OK"),
            ("GET", "k"): answer("GET", b"v"),
            ("PING",): answer("PING", b"PONG"),
            ("COMMAND", "GETKEYS", "SET", "k", "v"): [b"k"],
            ("COMMAND", "GETKEYS", "GET", "k"): [b"k"],
            ("COMMAND", "GETKEYS", "PING"): Error("ERR The command has no key arguments"),
        }
        for name, categories in _CATEGORIES.items():
            build.replies[("COMMAND", "INFO", name)] = [
                [name.lower().encode(), -1, [], 0, 0, 0, categories, [], [], []]
            ]


def test_acl_users_are_refused_exactly_the_commands_their_rules_deny(builds):
    _enforcing_acls()
    result, value = run(builds, _ACL_SEQUENCE, checks={"acl": True})

    assert result.status.value == "PASS", result.summary
    acl = _check(value, "acl")
    # @string covers two of the three commands; @fast and @slow are never chosen.
    assert acl["category"] == {"outcome": "pass", "user": "default +@all -@string"}
    assert acl["keys"] == {"outcome": "pass", "user": "default +@all resetkeys ~nomatch-*"}
    rules = [sent[3:] for sent in BUILDS[builds["candidate"]].sent if sent[:3] == ("ACL", "SETUSER", "default")]
    assert rules == [("+@all", "-@string"), ("+@all", "resetkeys", "~nomatch-*")]


@pytest.mark.parametrize(
    ("wrong", "reason", "command"), [("GET", "acl-bypass", "GET k"), ("PING", "acl-overreach", "PING")]
)
def test_a_command_run_against_its_rule_or_refused_by_none_fails(builds, wrong, reason, command):
    _enforcing_acls((wrong,))
    result, value = run(builds, _ACL_SEQUENCE, checks={"acl": True})

    assert result.status.value == "FAIL" and not result.crash
    assert (result.diagnostics["reason"], result.diagnostics["user"]) == (reason, "default +@all -@string")
    (found,) = result.diagnostics["commands"]
    assert found["command"] == command
    assert _check(value, "acl")["outcome"] == "fail"


def test_the_acl_check_skips_a_sequence_that_manages_users(builds):
    result, value = run(builds, [{"command": ["ACL", "SETUSER", "u", "on"]}], checks={"acl": True})

    assert result.status.value == "PASS", result.summary
    assert _check(value, "acl") == {"outcome": "skipped", "reason": "the sequence manages users itself"}


def test_concurrent_connections_each_run_the_whole_sequence(builds):
    for build in BUILDS.values():
        build.replies = {("INCR", "n"): 1}
    commands = [{"command": ["SET", "k", "v"]}, {"command": ["INCR", "n"], "wait_s": 5, "expect": {"equals": 1}}]
    result, value = run(builds, commands, checks={"concurrency": 3})

    assert result.status.value == "PASS", result.summary
    assert _check(value, "concurrency") == {"outcome": "pass"}
    side = _nodes(BUILDS[builds["candidate"]])[1]
    # Its main connection, and one per client.
    assert len(side.wires) == 4 and side.sent.count(("SET", "k", "v")) == side.sent.count(("INCR", "n")) == 3


def _concurrent(server):
    """Whether ``server`` serves several clients: the concurrency check's."""

    return len(server.wires) > 1


def test_a_command_unanswered_while_the_server_answers_no_ping_is_a_hang(builds):
    for build in BUILDS.values():
        build.replies = {
            ("BLPOP", "l", "1"): lambda server: TIMEOUT if _concurrent(server) else None,
            ("PING",): lambda server: TIMEOUT if _concurrent(server) else b"PONG",
        }
    result, _ = run(builds, [{"command": ["BLPOP", "l", 1]}], checks={"concurrency": 2})

    assert result.status.value == "FAIL" and result.crash
    assert result.summary.startswith("command 0 (BLPOP l 1) on connection 0 of 2 got no reply, and the server")
    assert result.diagnostics["reason"] == "hang" and len(result.diagnostics["commands"]) == 2


def test_a_command_unanswered_while_the_server_answers_others_is_no_hang(builds):
    for build in BUILDS.values():
        build.replies = {("BLPOP", "l", "0"): lambda server: TIMEOUT if _concurrent(server) else None}
    result, _ = run(builds, [{"command": ["BLPOP", "l", 0]}], checks={"concurrency": 2})

    assert result.status.value == "PASS", result.summary


def test_a_crash_during_a_check_is_a_crash_with_its_cause(builds):
    for build in BUILDS.values():
        build.replies = {("INCR", "n"): lambda server: DIE if _concurrent(server) else 1}
    result, value = run(builds, [{"command": ["INCR", "n"]}], checks={"concurrency": 2, "variants": True})

    assert result.status.value == "FAIL" and result.crash
    assert result.summary == "the candidate exited during the concurrency check: crashed by signal: 11"
    assert to_plain_data(result.diagnostics) == {
        "check": "concurrency",
        "connections": 2,
        "crashes": {"candidate": "crashed by signal: 11"},
    }
    assert _check(value, "variants")["outcome"] == "pass"
    assert "candidate.concurrency.server.log" in {os.path.basename(path) for path in result.evidence[0].artifacts}


def _saving(build):
    def save(server):
        with open(os.path.join(server.workdir, "dump.rdb"), "wb") as rdb:
            rdb.write(b"RDB")
        return b"OK"

    build.replies[("SAVE",)] = save


@pytest.mark.parametrize(
    ("loads", "syncs", "reason"),
    [(True, True, None), (False, True, "compat-load"), (True, False, "compat-sync")],
)
def test_the_parents_data_loads_and_replicates_into_the_candidate(builds, monkeypatch, loads, syncs, reason):
    monkeypatch.setattr(checks, "_SYNC_TIMEOUT_S", 0.2)

    def digest(server):
        if server.build is BUILDS[builds["parent"]]:
            return b"d1" if ("SET", "k", "v") in server.sent else b"empty"
        if server.files.get("dump.rdb") == b"RDB":
            return b"d1" if loads else b"empty"
        return b"d1" if syncs and any(sent[0] == "REPLICAOF" for sent in server.sent) else b"empty"

    _replicating(digest)
    _saving(BUILDS[builds["parent"]])
    result, value = run(builds, [{"command": ["SET", "k", "v"]}], checks={"compat": True})

    compat = _check(value, "compat")
    assert (compat["load"]["outcome"], compat["sync"]["outcome"]) == (
        "pass" if loads else "fail",
        "pass" if syncs else "fail",
    )
    rdb = next(path for path in result.evidence[0].artifacts if os.path.basename(path) == "parent.compat.rdb")
    # The candidate that loads it loads it again as a replica, which keeps keys expired since the parent saved.
    (loaded,) = [server for server in _nodes(BUILDS[builds["candidate"]]) if server.files.get("dump.rdb")]
    assert [sent[0] for sent in loaded.sent] == ["REPLICAOF", "DEBUG", "DEBUG"]
    assert loaded.sent[1] == ("DEBUG", "RELOAD", "NOSAVE")
    if reason is None:
        assert result.status.value == "PASS", result.summary
        return
    assert result.status.value == "FAIL"
    assert result.diagnostics["reason"] == reason
    if reason == "compat-load":
        assert to_plain_data(result.diagnostics) == {
            "reason": "compat-load",
            "check": "compat",
            "rdb": rdb,
            "parent": {"reply": "d1"},
            "candidate": {"reply": "empty"},
        }


def test_checks_are_skipped_when_the_sequence_did_not_complete(builds):
    BUILDS[builds["candidate"]].replies = {("SLOW",): TIMEOUT}
    result, value = run(builds, [{"command": ["SLOW"]}], checks={"variants": True, "concurrency": 2})

    assert result.status.value == "ERROR"
    assert value["checks"] == {
        name: {"outcome": "skipped", "reason": "the sequence did not complete on the candidate"}
        for name in ("variants", "concurrency")
    }
