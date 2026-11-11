"""The command-table workload: which commands it sends, how it draws their arguments and keys, where it sends them,
which replies it holds to Valkey's reply schemas and how the reply-schema oracle judges them, and how the chaos
experiments turn it on."""

import random
import time
from types import SimpleNamespace

import pytest

from tests.test_replication_durability import _component
from tests.valkey_experiments_support import fake_context, registration_for, unit_plan, valkey_service
from valkey_fuzzer.components import command_table
from valkey_fuzzer.components.command_table import (
    EVIDENCE,
    CommandGenerator,
    CommandTableWorkload,
    ReplySchemaOracle,
    answered,
    read_command_table,
)
from valkey_fuzzer.components.reply_schemas import reply_schemas
from valkey_fuzzer.generator import generate
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.schema import ConfigError, plain_config


def _flat(mapping):
    """``mapping`` as RESP2 sends a map: ``[key, value, ...]``, nested maps too."""

    flat = []
    for key, value in mapping.items():
        if key == "arguments":
            value = [_flat(item) for item in value]
        elif key == "subcommands":
            value = _flat({name: _flat(doc) for name, doc in value.items()})
        flat += [key, value]
    return flat


def _info(name, flags, subcommands=()):
    """A ``COMMAND`` entry."""

    return [name, -1, list(flags), 1, 1, 1, [], [], [], list(subcommands)]


_KEY = {"name": "key", "type": "key", "key_spec_index": 0}
_INFO = [
    _info("get", ["readonly", "fast"]),
    _info("flushall", ["write"]),
    _info("shutdown", ["admin", "noscript"]),
    _info("config", [], [_info("config|get", ["admin", "noscript"]), _info("config|set", ["admin", "noscript"])]),
    _info("client", [], [_info("client|reply", ["noscript"]), _info("client|id", ["noscript"])]),
    _info("zunionstore", ["write", "denyoom"]),
    _info("blpop", ["write", "blocking"]),
    _info("scan", ["readonly"]),
    _info("multi", ["noscript", "fast"]),
    _info("exec", ["noscript"]),
]
_DOCS = {
    "get": {"group": "string", "arguments": [_KEY]},
    "flushall": {"group": "server"},
    "shutdown": {"group": "server"},
    "config": {
        "group": "server",
        "subcommands": {
            "config|get": {
                "group": "server",
                "arguments": [{"name": "parameter", "type": "string", "flags": ["multiple"]}],
            },
            "config|set": {"group": "server"},
        },
    },
    "client": {"group": "connection", "subcommands": {"client|reply": {"group": "connection"}, "client|id": {}}},
    "zunionstore": {
        "group": "sorted_set",
        "arguments": [
            {"name": "destination", "type": "key", "key_spec_index": 0},
            {"name": "numkeys", "type": "integer"},
            {"name": "key", "type": "key", "key_spec_index": 1, "flags": ["multiple"]},
            {"name": "weight", "type": "integer", "token": "WEIGHTS", "flags": ["optional", "multiple"]},
            {
                "name": "aggregate",
                "type": "oneof",
                "token": "AGGREGATE",
                "flags": ["optional"],
                "arguments": [
                    {"name": "sum", "type": "pure-token", "token": "SUM"},
                    {"name": "min", "type": "pure-token", "token": "MIN"},
                ],
            },
        ],
    },
    "blpop": {
        "group": "list",
        "arguments": [{**_KEY, "flags": ["multiple"]}, {"name": "timeout", "type": "double"}],
    },
    "scan": {
        "group": "generic",
        "arguments": [
            {"name": "cursor", "type": "integer"},
            {"name": "pattern", "type": "pattern", "token": "MATCH", "flags": ["optional"]},
        ],
    },
    "multi": {"group": "transactions"},
    "exec": {"group": "transactions"},
}
_DOCS_REPLY = _flat({name: _flat(doc) for name, doc in _DOCS.items()})


def _table():
    return read_command_table(_INFO, _DOCS_REPLY)


def test_the_table_sends_every_leaf_command_but_admin_ones_that_write_and_the_excluded_ones():
    table, skipped = _table()
    names = ["BLPOP", "CLIENT ID", "CONFIG GET", "EXEC", "GET", "MULTI", "SCAN", "ZUNIONSTORE"]
    assert [name for name, _, _ in table] == names
    assert skipped == ["CLIENT REPLY", "CONFIG SET", "FLUSHALL", "SHUTDOWN"]
    groups = {name: group for name, group, _ in table}
    # A command without docs (a module's may have none) is generic and takes no arguments.
    assert groups["ZUNIONSTORE"] == "sorted_set" and groups["CLIENT ID"] == "generic"
    assert not command_table.ADMIN_READS & set(command_table.EXCLUDED)


def _generator(seed=1, table=None):
    return CommandGenerator(table or _table()[0], random.Random(seed), "vf:commands", 5)


def _draws(generator, count=2000):
    return [generator.next() for _ in range(count)]


def test_every_key_is_under_the_prefix_with_one_hash_tag_per_command_and_numkeys_counts_the_keys():
    keys_of = {"GET": slice(1, 2), "BLPOP": slice(1, -1)}
    seen = set()
    for tag, commands in _draws(_generator()):
        for name, command in commands:
            seen.add(name)
            keys = [word for word in command if word.startswith("vf:")]
            assert all(key.startswith(f"vf:commands:{{{tag}}}:") for key in keys if not key.endswith("*"))
            if name in keys_of:
                assert keys == command[keys_of[name]]
            if name == "ZUNIONSTORE":
                numkeys = int(command[2])
                assert command[3 : 3 + numkeys] == keys[1:] and 1 <= numkeys <= 3
                if "WEIGHTS" in command:
                    weights = command[command.index("WEIGHTS") + 1 :]
                    assert len(weights[: weights.index("AGGREGATE")] if "AGGREGATE" in weights else weights) == numkeys
            if name == "BLPOP":
                assert 0.01 <= float(command[-1]) <= 0.1  # a short block: 0 blocks forever
            if name == "SCAN" and "MATCH" in command:
                assert command[command.index("MATCH") + 1] == "vf:commands:*"
            if name == "GET":
                assert command[1].split(":")[3] == "string"  # the command's group types its keys
    assert seen == {name for name, _, _ in _table()[0]}


def test_multi_runs_as_a_short_transaction_of_one_hash_tag():
    transactions = [commands for _, commands in _draws(_generator()) if commands[0][0] == "MULTI"]
    assert transactions
    for commands in transactions:
        assert commands[0] == ("MULTI", ["MULTI"]) and commands[-1] == ("EXEC", ["EXEC"])
        assert 3 <= len(commands) <= 5
        tags = {key.split(":")[2] for _, command in commands for key in command if key.startswith("vf:commands:{")}
        assert len(tags) <= 1


def test_the_same_seed_draws_the_same_commands():
    assert _draws(_generator(seed=5)) == _draws(_generator(seed=5))
    assert _draws(_generator(seed=5)) != _draws(_generator(seed=6))


class _Server:
    """Raw connections to fake instances: ``log`` holds every command sent, as ``(instance, connection, command)``,
    and ``reply(instance, command)`` answers it; ``protocols`` holds each connection's, and ``resp`` that of the
    connection being answered."""

    def __init__(self, reply=None):
        self.log, self.opened, self.closed, self.protocols, self.resp = [], [], [], [], 2
        self.reply = reply or (lambda instance, command: {"reply": "OK"})

    def sent(self, instance):
        return [command for name, _, command in self.log if name == instance and command[0] != "COMMAND"]

    def open(self, resp, timeout_s, *, host, port):
        instance, number = f"valkey-{port - 7000}", len(self.opened)
        self.opened.append(instance)
        self.protocols.append(resp)
        server = self

        class Connection:
            def send(self, command):
                server.log.append((instance, number, list(command)))
                server.resp = resp
                if command == ["COMMAND"]:
                    return {"reply": _INFO}
                if command == ["COMMAND", "DOCS"]:
                    return {"reply": _DOCS_REPLY}
                return server.reply(instance, command)

            def close(self):
                server.closed.append(instance)

        return Connection()


def _handle(mode="standalone", primaries=("valkey-0",), instances=1):
    return SimpleNamespace(
        mode=SimpleNamespace(value=mode),
        instances=[
            SimpleNamespace(instance_id=f"valkey-{n}", host="127.0.0.1", port=7000 + n) for n in range(instances)
        ],
        primaries=lambda: list(primaries),
    )


def _run(tmp_path, monkeypatch, server, handle=None, commands=300, **config):
    monkeypatch.setattr(command_table, "open_raw_connection", server.open)
    spec = _component("valkey.workload.command-table", "commands", ComponentRole.WORKLOAD, {"rate": 5000, **config})
    context = fake_context(handle or _handle(), spec, artifact_dir=str(tmp_path))
    workload = CommandTableWorkload()
    started = workload.start(context, spec)
    assert started.status.value == "PASS", started.summary
    deadline = time.monotonic() + 10
    while workload._thread and sum(workload._outcomes.values()) < commands and time.monotonic() < deadline:
        time.sleep(0.01)
    result = workload.stop(context, spec)
    return result, plain_config(result.evidence[0].value) if result.evidence else None


def test_error_replies_are_counted_and_never_fail_the_run(tmp_path, monkeypatch):
    def reply(instance, command):
        return {"error": "ERR syntax error"} if command[0] == "GET" else {"reply": "OK"}

    server = _Server(reply)
    result, summary = _run(tmp_path, monkeypatch, server)
    assert result.status.value == "PASS" and result.evidence[0].evidence_id == EVIDENCE
    assert summary["outcomes"]["error"] > 0 and summary["outcomes"]["replied"] > 0
    assert summary["sent"] == sum(summary["outcomes"].values()) >= 300
    assert summary["commands"] == 8 and summary["skipped"] == ["CLIENT REPLY", "CONFIG SET", "FLUSHALL", "SHUTDOWN"]
    assert summary["distinct_sent"] == 8 and summary["prefix"] == "vf:commands"
    assert server.closed == ["valkey-0", "valkey-0"]  # one connection a protocol, closed at teardown


def test_a_command_without_a_reply_drops_the_connection_and_the_rest_of_its_transaction(tmp_path, monkeypatch):
    def reply(instance, command):
        if command[0] in ("MULTI", "GET"):
            return {"error": "TimeoutError: Timeout reading from socket", "unanswered": True}
        if command[0] == "SCAN":
            return {"protocol_error": "Protocol Error: b'?'"}
        return {"reply": "OK"}

    server = _Server(reply)
    result, summary = _run(tmp_path, monkeypatch, server)
    assert result.status.value == "PASS"
    assert summary["outcomes"]["unanswered"] > 0 and summary["outcomes"]["protocol_error"] > 0
    assert summary["protocol_errors"][0]["command"][0] == "SCAN" and len(summary["protocol_errors"]) <= 10
    # Nothing follows a command that got no reply on its connection: the next command opens a new one.
    for (_, connection, command), (_, following, _) in zip(server.log, server.log[1:]):
        assert command[0] not in ("MULTI", "GET", "SCAN") or following != connection
    assert len(server.opened) > 1


def test_in_cluster_mode_a_moved_reply_routes_the_hash_tag_to_the_primary_it_names(tmp_path, monkeypatch):
    def reply(instance, command):
        return {"error": "MOVED 1234 127.0.0.1:7001"} if instance == "valkey-0" else {"reply": "OK"}

    server = _Server(reply)
    handle = _handle("cluster", primaries=("valkey-0", "valkey-1"), instances=2)
    result, summary = _run(tmp_path, monkeypatch, server, handle=handle)
    assert result.status.value == "PASS"
    # A hash tag goes to valkey-0 until it answers MOVED, then only to valkey-1.
    moved = set()
    for instance, _, command in server.log:
        tags = {key.split(":")[2] for key in command if key.startswith("vf:commands:{")}
        if instance == "valkey-1":
            moved |= tags
        else:
            assert not tags & moved
    assert len(server.sent("valkey-1")) > len(server.sent("valkey-0"))
    assert summary["outcomes"]["replied"] == len(server.sent("valkey-1"))


def test_rate_0_sends_nothing_and_a_table_that_cannot_be_read_is_an_error(tmp_path, monkeypatch):
    server = _Server()
    result, summary = _run(tmp_path, monkeypatch, server, rate=0)
    assert result.status.value == "PASS" and not server.opened and summary["sent"] == 0

    monkeypatch.setattr(command_table, "open_raw_connection", server.open)
    spec = _component("valkey.workload.command-table", "commands", ComponentRole.WORKLOAD, {})
    context = fake_context(_handle(primaries=()), spec, artifact_dir=str(tmp_path))
    result = CommandTableWorkload().start(context, spec)
    assert result.status.value == "ERROR" and "no reachable primary" in result.summary


@pytest.mark.parametrize("experiment", ["valkey.cluster-chaos", "valkey.replication-chaos", "valkey.cluster-partition"])
def test_the_chaos_experiments_send_no_commands_unless_a_base_turns_them_on(experiment):
    runtime = valkey_service()

    def rate(**kwargs):
        plan = generate(runtime.catalog, 3, compile_fn=runtime.compile, experiment=experiment, **kwargs).plan
        [component] = [item for item in plan.components if item.use == "valkey.workload.command-table"]
        return component.config["rate"]

    assert rate() == 0
    assert rate(base={"experiment": {"inputs": {"command_rate": 250}}}) == 250
    # The oracle that judges the workload's replies comes with it.
    plan = generate(runtime.catalog, 3, compile_fn=runtime.compile, experiment=experiment).plan
    [oracle] = [item for item in plan.components if item.use == "valkey.oracle.reply-schema"]
    assert oracle.id == "reply-schemas" and oracle.config["workload"] == "commands"


def _name(command):
    return " ".join(command[:2]) if command[0] in ("CLIENT", "CONFIG") else command[0]


def _valkey(server, replies=None, queued_replies=None):
    """A reply function that answers the fake table's commands as Valkey does, transactions too; ``replies`` are
    other replies by command name, and ``queued_replies`` those of commands that run in a transaction."""

    correct = {
        "BLPOP": lambda: None,
        "CLIENT ID": lambda: 7,
        "CONFIG GET": lambda: {} if server.resp == 3 else [],
        "GET": lambda: "value",
        "SCAN": lambda: ["0", []],
        "ZUNIONSTORE": lambda: 2,
    }
    state = {"queued": None}

    def one(command, others):
        name = _name(command)
        return (others or {}).get(name, (replies or {}).get(name, correct[name]()))

    def reply(instance, command):
        queued = state["queued"]
        if command[0] == "MULTI":
            if queued is not None:
                return {"error": "ERR Command not allowed inside a transaction"}
            state["queued"] = []
            return {"reply": "OK"}
        if command[0] == "EXEC":
            if queued is None:
                return {"error": "ERR EXEC without MULTI"}
            state["queued"] = None
            return {"reply": [one(item, queued_replies) for item in queued]}
        if queued is not None:
            queued.append(command)
            return {"reply": "QUEUED"}
        return {"reply": one(command, None)}

    return reply


def test_replies_go_over_both_protocols_and_those_that_match_their_schemas_are_no_mismatch(tmp_path, monkeypatch):
    server = _Server()
    server.reply = _valkey(server)
    result, summary = _run(tmp_path, monkeypatch, server)
    protocol_of = dict(enumerate(server.protocols))
    carried = {protocol_of[number] for _, number, command in server.log if command[0] != "COMMAND"}
    assert carried == {2, 3}
    # The table is read as RESP2 sends it.
    assert {protocol_of[number] for _, number, command in server.log if command[0] == "COMMAND"} == {2}
    # Every reply but an error is held to a schema: a QUEUED is not the reply of the command it answers.
    assert summary["schema_mismatches"] == []
    assert summary["validated"] > 0 and summary["outcomes"]["error"] > 0
    assert f"{summary['validated']} replies held to their reply schemas" in result.summary


def test_a_reply_that_does_not_match_its_schema_is_recorded_once_for_its_command_and_protocol(tmp_path, monkeypatch):
    server = _Server()
    server.reply = _valkey(server, replies={"GET": 5, "SCAN": {"cursor": "0"}})
    result, summary = _run(tmp_path, monkeypatch, server)
    assert result.status.value == "PASS"  # the oracle judges, not the workload
    found = {(item["name"], item["resp"]): item for item in summary["schema_mismatches"]}
    assert sorted(found) == [("GET", 2), ("GET", 3), ("SCAN", 2), ("SCAN", 3)]
    for resp in (2, 3):
        get = found["GET", resp]
        assert get["count"] > 1 and get["command"][0] == "GET" and get["command"][1].startswith("vf:commands:{")
        assert get["reply"] == "5" and get["schema"] == reply_schemas()["GET"]
        assert get["problem"].startswith("reply matches none of the 2 alternatives (reply is not of type string: 5")
        assert found["SCAN", resp]["problem"] == 'reply is not of type array: {"cursor":"0"}'

    spec = _component("valkey.oracle.reply-schema", "reply-schemas", ComponentRole.ORACLE, {"workload": "commands"})
    judged = ReplySchemaOracle().evaluate(fake_context(_handle(), spec), spec, result.evidence)
    assert judged.status.value == "FAIL"
    assert judged.summary == "; ".join(
        f"{name} (RESP{resp}) reply does not match its reply schema" for name, resp in sorted(found)
    )
    assert plain_config(judged.diagnostics)["mismatches"] == summary["schema_mismatches"]


def test_the_reply_schema_oracle_passes_matching_replies_and_cannot_judge_without_the_workloads_evidence(
    tmp_path, monkeypatch
):
    server = _Server()
    server.reply = _valkey(server)
    result, summary = _run(tmp_path, monkeypatch, server)
    spec = _component("valkey.oracle.reply-schema", "reply-schemas", ComponentRole.ORACLE, {"workload": "commands"})
    context = fake_context(_handle(), spec)
    judged = ReplySchemaOracle().evaluate(context, spec, result.evidence)
    assert judged.status.value == "PASS"
    assert judged.summary == f"{summary['validated']} replies match their commands' reply schemas"
    other = _component("valkey.oracle.reply-schema", "reply-schemas", ComponentRole.ORACLE, {"workload": "writes"})
    for evidence, oracle in (((), spec), (result.evidence, other)):
        judged = ReplySchemaOracle().evaluate(context, oracle, evidence)
        assert judged.status.value == "ERROR" and "no valkey.workload.command-table evidence" in judged.summary


def test_the_reply_schema_oracle_names_a_command_table_workload_of_its_plan():
    workload = ComponentSpec(role=ComponentRole.WORKLOAD, use="valkey.workload.command-table", id="commands")
    writes = ComponentSpec(role=ComponentRole.WORKLOAD, use="valkey.workload.key-value", id="writes")
    registration = registration_for("valkey.oracle.reply-schema")

    def oracle(workload_id):
        config = {"workload": workload_id}
        return ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.reply-schema", id="schemas", config=config)

    registration.resolve_config(oracle("commands"), unit_plan((workload, writes, oracle("commands"))))
    for other in ("writes", "missing"):
        with pytest.raises(ConfigError, match=f"workload '{other}' is not a valkey.workload.command-table"):
            registration.resolve_config(oracle(other), unit_plan((workload, writes, oracle(other))))


def test_a_queued_commands_reply_is_its_item_of_the_execs_reply(tmp_path, monkeypatch):
    server = _Server()
    # In a transaction a GET replies an integer, and a ZUNIONSTORE an error: only the first is a mismatch.
    wrong = {"GET": 5, "ZUNIONSTORE": {"$error": "WRONGTYPE Operation against a key holding the wrong kind of value"}}
    server.reply = _valkey(server, queued_replies=wrong)
    _, summary = _run(tmp_path, monkeypatch, server, commands=600)
    assert sorted({item["name"] for item in summary["schema_mismatches"]}) == ["GET"]
    assert all(item["reply"] == "5" and item["command"][0] == "GET" for item in summary["schema_mismatches"])


def test_what_each_reply_of_a_connection_answers():
    def exchanges(*pairs):
        return [(name, [name], record) for name, record in pairs]

    ok, queued, error = {"reply": "OK"}, {"reply": "QUEUED"}, {"error": "ERR syntax error"}
    # Outside a transaction every reply answers its command; an error is no reply.
    assert list(answered(exchanges(("GET", {"reply": None}), ("SCAN", error), ("GET", queued)))) == [
        ("GET", ["GET"], None),
        ("GET", ["GET"], "QUEUED"),
    ]
    # Inside one, an EXEC's items answer the commands that were queued, in order; an error among them none.
    transaction = exchanges(
        ("MULTI", ok),
        ("GET", queued),
        ("SCAN", error),
        ("CLIENT ID", queued),
        ("BLPOP", queued),
        ("EXEC", {"reply": ["v", 7, {"$error": "WRONGTYPE"}]}),
        ("GET", {"reply": "after"}),
    )
    assert [(name, reply) for name, _, reply in answered(transaction)] == [
        ("MULTI", "OK"),
        ("EXEC", ["v", 7, {"$error": "WRONGTYPE"}]),
        ("GET", "v"),
        ("CLIENT ID", 7),
        ("GET", "after"),
    ]
    # An EXEC that fails, and a DISCARD, end the transaction: what follows answers for itself.
    for end in (("EXEC", {"error": "EXECABORT Transaction discarded because of previous errors."}), ("DISCARD", ok)):
        ended = exchanges(("MULTI", ok), ("GET", queued), end, ("GET", queued))
        assert [(name, reply) for name, _, reply in answered(ended)][-1] == ("GET", "QUEUED")
        assert ("GET", "QUEUED") not in [(name, reply) for name, _, reply in answered(ended[:-1])]
    # An EXEC outside a transaction, and one whose reply is not one item a queued command, answer only themselves.
    assert [name for name, _, _ in answered(exchanges(("EXEC", {"reply": ["v"]})))] == ["EXEC"]
    watched = exchanges(("MULTI", ok), ("GET", queued), ("EXEC", {"reply": None}))
    assert [name for name, _, _ in answered(watched)] == ["MULTI", "EXEC"]


def test_a_command_that_leaves_its_connection_unpaired_closes_it_and_ends_its_transaction(tmp_path, monkeypatch):
    assert set(command_table.LEAVES) == {"RESET", "CLIENT TRACKING", "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE"}
    assert not set(command_table.LEAVES) & set(command_table.EXCLUDED)
    monkeypatch.setitem(command_table.LEAVES, "GET", "stands in for RESET, which the fake table lacks")
    server = _Server()
    server.reply = _valkey(server)
    result, summary = _run(tmp_path, monkeypatch, server)
    assert result.status.value == "PASS" and summary["outcomes"]["unanswered"] == 0
    last = {number: index for index, (_, number, _) in enumerate(server.log)}
    gets = [(index, number) for index, (_, number, command) in enumerate(server.log) if command[0] == "GET"]
    # Nothing follows an answered GET on its connection, not the EXEC of its transaction either.
    assert gets and all(last[number] == index for index, number in gets)
    assert len(server.closed) == len(server.opened) > len(gets)


def test_a_reply_valkey_is_known_to_send_against_its_schema_is_no_mismatch(monkeypatch):
    known = [
        ("GEOHASH", {"reply": [None, "sqc8b49rny0"]}),
        ("OBJECT FREQ", {"reply": None}),
        ("OBJECT IDLETIME", {"reply": None}),
        ("OBJECT REFCOUNT", {"reply": None}),
    ]
    assert [name for name, _ in known] == sorted(command_table.KNOWN_GAPS)
    for resp in (2, 3):
        workload = CommandTableWorkload()
        workload._judge(resp, [(name, name.split(), record) for name, record in known])
        assert workload._validated == 4 and not workload._mismatches
        # Anything else of its command is still a mismatch.
        workload._judge(resp, [(name, name.split(), {"reply": "text"}) for name, _ in known])
        assert sorted(workload._mismatches) == [(name, resp) for name, _ in known]
    # Each is a gap of the shipped schema: once Valkey's describes the reply, its entry goes.
    monkeypatch.setattr(command_table, "KNOWN_GAPS", {})
    workload = CommandTableWorkload()
    workload._judge(3, [(name, name.split(), record) for name, record in known])
    assert sorted(workload._mismatches) == [(name, 3) for name, _ in known]
