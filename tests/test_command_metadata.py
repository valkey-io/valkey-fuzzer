"""The command-metadata oracle without Valkey processes: fake private servers that declare a command table and
answer by it, or against it where a test says so; how the oracle reads key specs; and the experiments it joins."""

import dataclasses
import json
from types import SimpleNamespace

import pytest
from valkey.crc import key_slot

from tests.test_command_sequence import (
    BUILDS,
    CLOSE,
    DIE,
    SERVERS,
    TIMEOUT,
    Build,
    Error,
    FakeServer,
    _nodes,
    fake_open_raw_connection,
)
from tests.test_command_table import _flat
from tests.valkey_experiments_support import fake_context, registration_for, resolved, unit_plan, valkey_service
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.components import actions, command_metadata, command_table
from valkey_fuzzer.components.command_metadata import CommandMetadataOracle, range_positions, spec_positions
from valkey_fuzzer.generator import generate
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.schema import ConfigError

READONLY = Error("READONLY You can't write against a read only replica.")
ARITY = Error("ERR wrong number of arguments for 'x' command")


def _index(position):
    return ["type", "index", "spec", ["index", position]]


def _keyword(keyword, startfrom):
    return ["type", "keyword", "spec", ["keyword", keyword, "startfrom", startfrom]]


def _range(lastkey=0, step=1, limit=0):
    return ["type", "range", "spec", ["lastkey", lastkey, "keystep", step, "limit", limit]]


def _keynum(keynumidx=0, firstkey=1, step=1):
    return ["type", "keynum", "spec", ["keynumidx", keynumidx, "firstkey", firstkey, "keystep", step]]


def _key_spec(begin, find, *flags):
    return ["flags", list(flags), "begin_search", begin, "find_keys", find]


def _entry(name, arity, flags, positions=(0, 0, 0), specs=(), subcommands=()):
    """A ``COMMAND`` entry."""

    return [name, arity, list(flags), *positions, [], [], list(specs), list(subcommands)]


_ONE_KEY = {"positions": (1, 1, 1), "specs": [_key_spec(_index(1), _range(), "RO", "access")]}
GET = _entry("get", 2, ["readonly", "fast"], **_ONE_KEY)
SET = _entry("set", -3, ["write", "denyoom"], (1, 1, 1), [_key_spec(_index(1), _range(), "OW", "update")])
PING = _entry("ping", -1, ["fast"])
MULTI = _entry("multi", 1, ["noscript", "fast"])
EXEC = _entry("exec", 1, ["noscript"])
MSET = _entry("mset", -3, ["write"], (1, -1, 2), [_key_spec(_index(1), _range(-1, 2), "OW", "update")])
# Declared as Valkey declares them: a key its own specs say it writes, a script that runs other commands, a
# shard channel, and keys counted by an argument (so no first, last, and step).
PFCOUNT = _entry("pfcount", -2, ["readonly"], (1, -1, 1), [_key_spec(_index(1), _range(-1), "RW", "access")])
EVAL_RO = _entry("eval_ro", -3, ["readonly", "movablekeys"], specs=[_key_spec(_index(2), _keynum(), "RO", "access")])
SPUBLISH = _entry("spublish", 3, ["pubsub", "fast"], (1, 1, 1), [_key_spec(_index(1), _range(), "not_key")])
MSETEX = _entry("msetex", -4, ["write", "movablekeys"], specs=[_key_spec(_index(1), _keynum(0, 1, 2), "OW", "update")])
# Like MSETEX in shape but not a command upstream is fixing, so the oracle still judges its routing: it is
# how the misrouting check is proven now that MSETEX is a known gap (ROUTING_KNOWN_GAPS).
NKSET = _entry("nkset", -4, ["write", "movablekeys"], specs=[_key_spec(_index(1), _keynum(0, 1, 2), "OW", "update")])
XREAD = _entry(
    "xread", -4, ["readonly", "movablekeys"], specs=[_key_spec(_keyword("STREAMS", 1), _range(-1, 1, 2), "RO")]
)
UNSUBSCRIBE = _entry("unsubscribe", -1, ["pubsub", "noscript"])
QUIT = _entry("quit", -1, ["fast"])
# Where each command's keys are, as the fake build finds them: apart from its metadata, which a test may get wrong.
KEYS = {
    "GET": lambda words: words[1:2],
    "SET": lambda words: words[1:2],
    "MSET": lambda words: words[1::2],
    "PFCOUNT": lambda words: words[1:],
    "EVAL_RO": lambda words: words[3 : 3 + int(words[2])],
    "MSETEX": lambda words: words[2 : 2 + 2 * int(words[1]) : 2],
    "NKSET": lambda words: words[2 : 2 + 2 * int(words[1]) : 2],
    "XREAD": lambda words: words[words.index("STREAMS") + 1 :][: (len(words) - words.index("STREAMS") - 1) // 2],
}
# Shard channels: routed like keys, named by no COMMAND GETKEYS.
CHANNELS = {"SPUBLISH": lambda words: words[1:2]}


class Commands(dict):
    """A fake build's replies: the commands it declares (``COMMAND`` entries), which its nodes answer as their
    metadata says, node 0 as the primary that owns every slot and node 1 as its read-only replica, or with
    ``cluster`` as a primary that owns none. ``answers[name]`` is a function ``(node, words)`` that answers a
    command instead (None: as declared), and ``state`` what node 0's ``INFO`` and ``DEBUG DIGEST`` report."""

    def __init__(self, *entries, cluster=False, answers=None, getkeys=None, docs=None):
        super().__init__()
        self.entries, self.cluster = list(entries), cluster
        self.flags = {entry[0].upper(): entry[2] for entry in entries}
        self.answers, self.getkeys = dict(answers or {}), dict(getkeys or {})
        self.docs = _flat({name: _flat(doc) for name, doc in (docs or {}).items()})
        self.state = dict.fromkeys(("digest", "offset", "aof", "expired_keys", "expired_fields", "volatile"), 0)

    def write(self, **moved):
        """Node 0 changes its data and propagates it; or only ``moved``, each by its amount."""

        for name, amount in (moved or {"digest": 1, "offset": 10, "aof": 10}).items():
            self.state[name] += amount

    def get(self, command, default=None):
        return lambda server: self.serve(_nodes(server.build).index(server), command, default)

    def serve(self, node, command, default):
        state = self.state
        if command == ("COMMAND",):
            return self.entries
        if command == ("COMMAND", "DOCS"):
            return self.docs
        if command[:2] == ("COMMAND", "GETKEYS"):
            name, words = command[2], list(command[2:])
            if name in self.getkeys:
                return self.getkeys[name](words)
            return KEYS[name](words) if name in KEYS else Error("ERR The command has no key arguments")
        if command == ("INFO", "replication"):
            return b"role:slave\r\nmaster_link_status:up\r\n"
        if command[0] == "INFO":
            return (
                f"master_repl_offset:{state['offset']}\r\nexpired_keys:{state['expired_keys']}\r\n"
                f"expired_fields:{state['expired_fields']}\r\naof_current_size:{state['aof']}\r\n"
                f"aof_buffer_length:0\r\ndb0:keys=1,expires=0,keys_with_volatile_items={state['volatile']}\r\n"
            ).encode()
        if command == ("DEBUG", "DIGEST"):
            return f"digest-{state['digest']}".encode()
        if command[0] not in self.flags:
            return default
        answer = self.answers.get(command[0], lambda node, words: None)(node, list(command))
        return self.declared(node, list(command)) if answer is None else answer

    def declared(self, node, words):
        """What a node answers a command that keeps to its metadata."""

        flags = self.flags[words[0]]
        routed = KEYS.get(words[0], CHANNELS.get(words[0], lambda words: []))(words)
        if node and self.cluster and routed:
            return Error(f"MOVED {key_slot(routed[0].encode())} 127.0.0.1:7000")
        if node and "write" in flags:
            return READONLY
        if "write" in flags:
            self.write()
        return b"OK"


@pytest.fixture
def builds(monkeypatch, tmp_path):
    """The path of a fake ``valkey-server`` build, whose replies a test sets with :func:`run`."""

    BUILDS.clear()
    SERVERS.clear()
    monkeypatch.setattr(actions, "PrivateServer", FakeServer)
    monkeypatch.setattr(actions, "open_raw_connection", fake_open_raw_connection)
    path = tmp_path / "valkey-server"
    path.write_text("#!/bin/sh\n")
    path.chmod(0o755)
    BUILDS[str(path)] = Build()
    return str(path)


def _one(*words):
    """A draw of the workload: one command."""

    return [(words[0], list(words))]


def _transaction(*commands):
    """A draw of the workload: a transaction of ``commands``."""

    return [("MULTI", ["MULTI"]), *((words[0], list(words)) for words in commands), ("EXEC", ["EXEC"])]


def _specs(rate=200, **config):
    """The command-table workload at ``rate``, and the oracle with ``config``."""

    workload = resolved(
        ComponentSpec(
            role=ComponentRole.WORKLOAD, use="valkey.workload.command-table", id="commands", config={"rate": rate}
        )
    )
    oracle = ComponentSpec(
        role=ComponentRole.ORACLE,
        use="valkey.oracle.command-metadata",
        id="command-metadata",
        config={"workload": "commands", **config},
    )
    return workload, resolved(oracle)


def _context(builds, model, tmp_path, workload, oracle, **kwargs):
    BUILDS[builds].replies = model
    handle = SimpleNamespace(
        mode=SimpleNamespace(value="cluster" if model.cluster else "standalone"),
        instances=[SimpleNamespace(instance_id="valkey-0")],
        binary=lambda instance_id: builds,
    )
    return fake_context(handle, workload, oracle, artifact_dir=str(tmp_path), **kwargs)


def _evaluate(context, oracle):
    result = CommandMetadataOracle().evaluate(context, oracle, [])
    return result, (to_plain_data(result.evidence[0].value) if result.evidence else None)


def run(builds, model, tmp_path, monkeypatch, *draws):
    """The oracle on a fake build that answers as ``model``, with ``draws`` as the workload's; the result and
    its evidence value."""

    script = [list(draw) for draw in draws]
    monkeypatch.setattr(command_table.CommandGenerator, "next", lambda self: (0, script.pop(0)))
    workload, oracle = _specs(commands=sum(map(len, draws)))
    context = _context(builds, model, tmp_path, workload, oracle)
    return _evaluate(context, oracle)


def _sent(node):
    """The commands of the fake build's table that node ``node`` received on its connections, in order."""

    [build] = BUILDS.values()
    return [list(words) for words in _nodes(build)[node].sent if words[0] in build.replies.flags]


def _finding(result, value, reason):
    assert result.status.value == "FAIL", result.summary
    [finding] = value["findings"]
    assert finding["reason"] == reason == result.diagnostics["reason"]
    return finding


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

_DOCS = {
    "get": {"group": "string", "arguments": [{"name": "key", "type": "key"}]},
    "set": {"group": "string", "arguments": [{"name": "key", "type": "key"}, {"name": "value", "type": "string"}]},
    "ping": {"group": "connection"},
    "multi": {"group": "transactions"},
    "exec": {"group": "transactions"},
}


@pytest.mark.parametrize("cluster", [False, True], ids=["standalone", "cluster"])
def test_the_workloads_own_commands_run_again_on_two_private_servers_and_pass(builds, tmp_path, cluster):
    model = Commands(GET, SET, PING, MULTI, EXEC, cluster=cluster, docs=_DOCS)
    workload, oracle = _specs(commands=200)
    context = _context(builds, model, tmp_path, workload, oracle, seed=7)
    # The workload drew its commands from the run's continuing stream; the oracle draws from where that started.
    table = command_table.read_command_table(model.entries, model.docs)[0]
    sending = command_table.CommandGenerator(table, context.rng("commands", "commands"), "vf:commands", 5)
    for _ in range(50):
        sending.next()
    result, value = _evaluate(context, oracle)

    assert result.status.value == "PASS", result.summary
    # The commands the workload drew from the run's seed, from its first, as many as `commands`.
    generator = command_table.CommandGenerator(table, context.plan.rng("commands", "commands"), "vf:commands", 5)
    drawn = []
    while len(drawn) < 200:
        drawn += [words for _, words in generator.next()[1]]
    assert _sent(0) == drawn and value["commands"] == len(drawn)
    with open(value["sequence"], encoding="utf-8") as sequence:
        assert [json.loads(line) for line in sequence] == drawn
    judged = value["judged"]
    assert judged["keys"] and judged["readonly"] and value["findings"] == []
    assert bool(judged["routing"]) == cluster and bool(judged["replica"]) != cluster
    assert f"keys {judged['keys']}, arity 0, readonly {judged['readonly']}" in result.summary
    # Two nodes of the deployment's build, with an AOF: a replica of node 0, or a cluster whose node 0 owns
    # every slot; stopped, their logs kept.
    first, second = _nodes(BUILDS[builds])
    assert all(node.option("appendonly") == "yes" and node.stopped for node in (first, second))
    assert (first.option("cluster-enabled") == "yes") == cluster
    if cluster:
        assert ("CLUSTER", "ADDSLOTSRANGE", "0", "16383") in first.sent
        assert ("CLUSTER", "MEET", "127.0.0.1", first.option("port"), first.option("cluster-port")) in second.sent
    else:
        assert ("REPLICAOF", "127.0.0.1", first.option("port")) in second.sent
    assert len(result.evidence[0].artifacts) == 5
    assert not context.obligations.report()["still_active"]


def test_no_server_starts_where_the_workload_sends_no_commands(builds, tmp_path):
    workload, oracle = _specs(rate=0)
    context = _context(builds, Commands(GET), tmp_path, workload, oracle)
    result, value = _evaluate(context, oracle)

    assert result.status.value == "PASS" and "rate 0" in result.summary
    assert value is None and not SERVERS


def test_the_private_servers_load_the_deployments_modules(builds, tmp_path, monkeypatch):
    monkeypatch.setattr(command_table.CommandGenerator, "next", lambda self: (0, _one("PING")))
    workload, oracle = _specs(commands=1)
    context = _context(builds, Commands(PING), tmp_path, workload, oracle)
    instances = {"valkey-0": {"role": "primary", "server_config": {"loadmodule": ["/m/json.so", "/m/bloom.so"]}}}
    context.plan = dataclasses.replace(context.plan, instances=instances)
    result, _ = _evaluate(context, oracle)

    assert result.status.value == "PASS", result.summary
    for node in _nodes(BUILDS[builds]):
        arguments = list(zip(node.arguments[::2], node.arguments[1::2]))
        assert [value for name, value in arguments if name == "--loadmodule"] == ["/m/json.so", "/m/bloom.so"]


def test_the_oracle_names_a_command_table_workload_of_its_plan():
    registration = registration_for("valkey.oracle.command-metadata")
    writes = ComponentSpec(role=ComponentRole.WORKLOAD, use="valkey.workload.key-value", id="writes")
    for workload in ("writes", "missing"):
        spec = ComponentSpec(
            role=ComponentRole.ORACLE, use=registration.component_id, id="metadata", config={"workload": workload}
        )
        with pytest.raises(ConfigError, match=r"is not a valkey\.workload\.command-table of this plan$"):
            registration.resolve_config(spec, unit_plan((writes, spec)))


@pytest.mark.parametrize("experiment", ["valkey.cluster-chaos", "valkey.replication-chaos", "valkey.cluster-partition"])
def test_every_experiment_with_the_command_table_workload_holds_its_commands_to_their_metadata(experiment):
    runtime = valkey_service()
    plan = generate(runtime.catalog, 3, compile_fn=runtime.compile, experiment=experiment).plan
    [oracle] = [item for item in plan.components if item.use == "valkey.oracle.command-metadata"]
    [workload] = [item for item in plan.components if item.use == "valkey.workload.command-table"]
    assert oracle.config["workload"] == workload.id and oracle.config["commands"] == 1000


# ---------------------------------------------------------------------------
# readonly
# ---------------------------------------------------------------------------


def _reading(model, **moved):
    """Make ``model``'s GET move node 0's ``moved`` counters, each by its amount."""

    def get(node, words):
        if not node:
            model.write(**moved)

    model.answers["GET"] = get
    return model


@pytest.mark.parametrize("moved", ["digest", "offset", "aof"])
def test_a_readonly_command_that_changes_data_or_propagates_fails(builds, tmp_path, monkeypatch, moved):
    model = _reading(Commands(GET, SET), **{moved: 1})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("SET", "k", "v"), _one("GET", "k"))

    finding = _finding(result, value, "readonly-wrote")
    assert (finding["name"], finding["index"], finding["command"]) == ("GET", 1, "GET k")
    assert "readonly" in finding["declared"]["flags"]
    [name] = finding["observed"]["after"]
    assert name == {"offset": "master_repl_offset"}.get(moved, moved)
    assert result.summary == "GET has the readonly flag, yet changed data or propagated"
    # The write before it moved all three, and is no finding: only commands that say readonly are held to it.
    assert value["judged"]["readonly"] == 1


@pytest.mark.parametrize("counter", ["expired_keys", "expired_fields"])
def test_a_readonly_command_is_not_judged_while_a_key_or_a_field_expires(builds, tmp_path, monkeypatch, counter):
    # A key the GET found expired is deleted, and its deletion propagated, by design; Valkey counts it.
    model = _reading(Commands(GET), digest=1, offset=10, aof=10, **{counter: 1})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("GET", "k"))

    assert result.status.value == "PASS", result.summary
    assert value["judged"]["readonly"] == 0


@pytest.mark.parametrize(("volatile", "status"), [(1, "PASS"), (0, "FAIL")])
def test_a_digest_that_moves_alone_is_not_judged_beside_hash_fields_that_expire(
    builds, tmp_path, monkeypatch, volatile, status
):
    # DEBUG DIGEST leaves a hash field out from its expire time on, before the server deletes and counts it.
    model = _reading(Commands(GET), digest=1)
    model.state["volatile"] = volatile
    result, value = run(builds, model, tmp_path, monkeypatch, _one("GET", "k"))

    assert result.status.value == status, result.summary
    assert value["judged"]["readonly"] == 1 - volatile


def test_a_propagating_readonly_command_fails_beside_hash_fields_that_expire(builds, tmp_path, monkeypatch):
    model = _reading(Commands(GET), digest=1, offset=10)
    model.state["volatile"] = 1
    result, value = run(builds, model, tmp_path, monkeypatch, _one("GET", "k"))

    assert sorted(_finding(result, value, "readonly-wrote")["observed"]["after"]) == ["digest", "master_repl_offset"]


@pytest.mark.parametrize(
    ("entry", "words", "status"),
    [
        # PFCOUNT's own key spec says it may write its key; a script may call PFCOUNT.
        (PFCOUNT, ("PFCOUNT", "k"), "PASS"),
        (EVAL_RO, ("EVAL_RO", "return 1", "1", "k"), "PASS"),
        (_entry("pfcount", -2, ["readonly"], (1, -1, 1), [_key_spec(_index(1), _range(-1), "RO")]), ("PFCOUNT", "k"), "FAIL"),
    ],
    ids=["key-spec-writes", "script", "key-spec-reads"],
)  # fmt: skip
def test_a_readonly_command_that_declares_a_write_or_runs_other_commands_is_not_judged(
    builds, tmp_path, monkeypatch, entry, words, status
):
    model = Commands(entry, answers={words[0]: lambda node, words: None if node else model.write()})
    result, value = run(builds, model, tmp_path, monkeypatch, _one(*words))

    assert result.status.value == status, result.summary
    assert value["judged"]["readonly"] == (status == "FAIL")


def test_a_transaction_is_judged_readonly_as_a_whole_when_every_command_in_it_is(builds, tmp_path, monkeypatch):
    # Its commands are queued; the EXEC runs them.
    model = Commands(GET, SET, MULTI, EXEC, answers={"GET": lambda node, words: None if node else b"QUEUED"})
    model.answers["EXEC"] = lambda node, words: model.write()
    reads = _transaction(("GET", "a"), ("GET", "b"))
    result, value = run(builds, model, tmp_path, monkeypatch, _transaction(("GET", "a"), ("SET", "k", "v")), reads)

    finding = _finding(result, value, "readonly-wrote")
    assert (finding["name"], finding["index"]) == ("GET, GET", 4)
    assert finding["command"] == "MULTI ; GET a ; GET b ; EXEC"
    # The transaction with a SET in it was not judged.
    assert value["judged"]["readonly"] == 1 and _sent(0)[:4] == [["MULTI"], ["GET", "a"], ["SET", "k", "v"], ["EXEC"]]


# ---------------------------------------------------------------------------
# replica
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("words", "reply", "reason"),
    [
        (("SET", "k", "v"), b"OK", "replica-wrote"),
        (("SET", "k", "v"), Error("WRONGTYPE Operation against a key holding the wrong kind of value"), "replica-wrote"),
        (("GET", "k"), READONLY, "replica-refused"),
    ],
)  # fmt: skip
def test_a_replica_that_answers_against_a_commands_flags_fails(builds, tmp_path, monkeypatch, words, reply, reason):
    model = Commands(GET, SET, answers={words[0]: lambda node, words: reply if node else None})
    result, value = run(builds, model, tmp_path, monkeypatch, _one(*words))

    finding = _finding(result, value, reason)
    assert finding["name"] == words[0] and finding["declared"]["flags"] == sorted(model.flags[words[0]])
    assert ("error" in finding["observed"]["replica"]) == isinstance(reply, Error)
    assert result.summary.startswith(f"{words[0]} has the {'write' if words[0] == 'SET' else 'readonly'} flag, yet")


def test_a_replica_gets_the_commands_that_say_write_or_readonly_and_no_transaction(builds, tmp_path, monkeypatch):
    model = Commands(GET, SET, PING, MULTI, EXEC)
    draws = [_one("PING"), _transaction(("SET", "k", "v"), ("GET", "k")), _one("GET", "k")]
    result, value = run(builds, model, tmp_path, monkeypatch, *draws)

    assert result.status.value == "PASS", result.summary
    assert _sent(1) == [["SET", "k", "v"], ["GET", "k"], ["GET", "k"]]
    assert (value["judged"]["replica"], value["judged"]["routing"]) == (3, 0)


def test_an_error_valkey_replies_before_it_reads_a_commands_flags_is_not_judged(builds, tmp_path, monkeypatch):
    # GET with one argument too many: its arity refuses it on every node.
    model = Commands(GET, SET, answers={"SET": lambda node, words: ARITY, "GET": lambda node, words: ARITY})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("SET", "k"), _one("GET", "k", "extra"))

    assert result.status.value == "PASS", result.summary
    assert value["judged"]["replica"] == 0 and len(_sent(1)) == 2


# ---------------------------------------------------------------------------
# routing
# ---------------------------------------------------------------------------

_SLOT = key_slot(b"k")
_MSETEX = ("MSETEX", "1", "k", "v")
_NKSET = ("NKSET", "1", "k", "v")


@pytest.mark.parametrize(
    ("words", "reply", "declared"),
    [
        # A node that does not own the slot ran, misrouted, or errored on a keyed command (the shape of
        # valkey-io/valkey#4936, judged here on NKSET since MSETEX itself is a known gap).
        (_NKSET, 1, {"keys": ["k"], "slots": [_SLOT]}),
        (_NKSET, Error(f"MOVED {_SLOT + 1} 127.0.0.1:7000"), {"keys": ["k"], "slots": [_SLOT]}),
        (_NKSET, Error("ERR no such key"), {"keys": ["k"], "slots": [_SLOT]}),
        (("PING",), Error("MOVED 1 127.0.0.1:7000"), {"keys": [], "slots": []}),
        (("PING",), Error("CROSSSLOT Keys in request don't hash to the same slot"), {"keys": [], "slots": []}),
        (("SPUBLISH", "k", "message"), 0, {"keys": ["k"], "slots": [_SLOT]}),
        (("MSET", "k", "v", "other", "v"), Error(f"MOVED {_SLOT} 127.0.0.1:7000"),
         {"keys": ["k", "other"], "slots": sorted([_SLOT, key_slot(b"other")])}),
    ],
    ids=["served", "other-slot", "own-error", "no-key-moved", "no-key-crossslot", "shard-channel", "several-slots"],
)  # fmt: skip
def test_a_node_without_slots_that_does_not_route_a_command_by_what_it_declares_fails(
    builds, tmp_path, monkeypatch, words, reply, declared
):
    model = Commands(NKSET, MSETEX, MSET, PING, SPUBLISH, cluster=True)
    model.answers[words[0]] = lambda node, words: reply if node else None
    result, value = run(builds, model, tmp_path, monkeypatch, _one(*words))

    finding = _finding(result, value, "misrouted")
    assert (finding["name"], finding["declared"]) == (words[0], declared)
    assert finding["observed"]["node_without_slots"] == (
        {"error": str(reply)} if isinstance(reply, Error) else {"reply": reply}
    )
    assert result.summary == f"{words[0]} was not routed by the keys it declares on a node that owns no slot"


def test_a_command_upstream_is_already_fixing_is_not_flagged_as_misrouted(builds, tmp_path, monkeypatch):
    # MSETEX is misrouted on a node that owns no slot (valkey-io/valkey#4936, fix PR #4951): its command JSON has
    # no get_keys_function. It is a known gap, so the oracle does not fail a run on it; remove it when the fix lands.
    from valkey_fuzzer.components.command_metadata import ROUTING_KNOWN_GAPS

    assert "MSETEX" in ROUTING_KNOWN_GAPS
    model = Commands(MSETEX, MSET, PING, cluster=True)
    # Node 1 owns no slot and runs MSETEX anyway, the #4936 behaviour: still not a finding.
    model.answers["MSETEX"] = lambda node, words: 1 if node else None
    result, _ = run(builds, model, tmp_path, monkeypatch, _one(*_MSETEX))

    assert result.status.value == "PASS", result.summary


def test_a_node_without_slots_moves_keys_and_shard_channels_and_serves_the_rest(builds, tmp_path, monkeypatch):
    crossslot = Error("CROSSSLOT Keys in request don't hash to the same slot")
    model = Commands(MSETEX, MSET, PING, SPUBLISH, GET, cluster=True)
    model.answers["MSET"] = lambda node, words: crossslot if node else None
    draws = [_one(*_MSETEX), _one("PING"), _one("SPUBLISH", "k", "m"), _one("MSET", "k", "v", "other", "v")]
    result, value = run(builds, model, tmp_path, monkeypatch, *draws, _one("GET", "{k}1"))

    assert result.status.value == "PASS", result.summary
    assert value["judged"]["routing"] == 5 and _sent(1) == _sent(0)


@pytest.mark.parametrize(("node_1", "status"), [("Unbalanced", "PASS"), ("no such key", "FAIL")])
def test_a_commands_own_error_that_the_slots_owner_replies_too_is_not_judged(
    builds, tmp_path, monkeypatch, node_1, status
):
    # An XREAD with more streams than IDs: Valkey finds no keys to route it by, and lets the command say so.
    replies = [Error("ERR Unbalanced 'xread' list of streams"), Error(f"ERR {node_1} 'xread' list of streams")]
    model = Commands(XREAD, cluster=True, answers={"XREAD": lambda node, words: replies[node]})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("XREAD", "STREAMS", "a", "{a}b", "0", "0", "0"))

    assert result.status.value == status, result.summary
    assert value["judged"]["routing"] == (status == "FAIL")


def test_a_queued_commands_error_on_a_node_without_slots_is_not_judged(builds, tmp_path, monkeypatch):
    # Node 0 replied QUEUED: what the command replies there came with the EXEC's, so there is none to compare.
    replies = [b"QUEUED", Error("ERR Unbalanced 'xread' list of streams")]
    model = Commands(XREAD, MULTI, EXEC, cluster=True, answers={"XREAD": lambda node, words: replies[node]})
    draw = _transaction(("XREAD", "STREAMS", "a", "{a}b", "0", "0", "0"))
    result, value = run(builds, model, tmp_path, monkeypatch, draw)

    assert result.status.value == "PASS", result.summary
    assert value["judged"]["routing"] == 0


def test_a_queued_command_is_held_to_its_keys_by_a_redirect(builds, tmp_path, monkeypatch):
    moved = Error("MOVED 1 127.0.0.1:7000")
    model = Commands(
        PING, MULTI, EXEC, cluster=True, answers={"PING": lambda node, words: moved if node else b"QUEUED"}
    )
    result, value = run(builds, model, tmp_path, monkeypatch, _transaction(("PING",)))

    assert _finding(result, value, "misrouted")["declared"] == {"keys": [], "slots": []}


def test_a_node_without_slots_gets_no_multi(builds, tmp_path, monkeypatch):
    # It would queue what follows instead of answering it; a transaction's own MULTI and EXEC never go there.
    model = Commands(GET, MULTI, EXEC, cluster=True)
    result, _ = run(builds, model, tmp_path, monkeypatch, _transaction(("MULTI",), ("GET", "k")), _one("GET", "k"))

    assert result.status.value == "PASS", result.summary
    assert _sent(1) == [["GET", "k"], ["GET", "k"]]


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cluster", [False, True], ids=["standalone", "cluster"])
def test_keys_that_command_getkeys_names_and_the_key_specs_do_not_are_a_mismatch(
    builds, tmp_path, monkeypatch, cluster
):
    model = Commands(GET, cluster=cluster, getkeys={"GET": lambda words: [b"other"]})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("GET", "k"))

    finding = _finding(result, value, "keys-mismatch")
    assert finding["declared"] == {"key_specs": ["k"], "first_last_step": ["k"]}
    assert finding["observed"] == {"getkeys": ["other"]} and finding["index"] == 0
    assert "COMMAND GETKEYS names ['other'], its key specs ['k']" in finding["detail"]
    # Keys that disagree are none to route by.
    assert value["judged"] == {"keys": 1, "arity": 0, "readonly": 1, "replica": 0 if cluster else 1, "routing": 0}


@pytest.mark.parametrize(
    ("flags", "status"), [(["readonly"], "FAIL"), (["readonly", "movablekeys"], "PASS")], ids=["fixed", "movablekeys"]
)
def test_first_last_and_step_put_the_keys_where_the_key_specs_do_unless_the_keys_are_movable(
    builds, tmp_path, monkeypatch, flags, status
):
    entry = _entry("get", -2, flags, (2, 2, 1), [_key_spec(_index(1), _range(), "RO")])
    result, value = run(builds, Commands(entry), tmp_path, monkeypatch, _one("GET", "k", "other"))

    assert result.status.value == status, result.summary
    if status == "FAIL":
        finding = _finding(result, value, "keys-mismatch")
        assert finding["declared"] == {"key_specs": ["k"], "first_last_step": ["other"]}
        assert "first, last, and step [2, 2, 1] put its keys at ['other'], its key specs at ['k']" in finding["detail"]


@pytest.mark.parametrize(
    "getkeys",
    [
        Error("ERR Invalid arguments specified for command"),
        Error("ERR Invalid number of arguments specified for command"),
    ],
)
def test_arguments_that_are_not_the_commands_have_no_keys_to_judge(builds, tmp_path, monkeypatch, getkeys):
    model = Commands(MSETEX, cluster=True, getkeys={"MSETEX": lambda words: getkeys})
    # A node without slots that serves it is no finding either: its keys are not known.
    model.answers["MSETEX"] = lambda node, words: Error("ERR syntax error")
    result, value = run(builds, model, tmp_path, monkeypatch, _one(*_MSETEX))

    assert result.status.value == "PASS", result.summary
    assert (value["judged"]["keys"], value["judged"]["routing"]) == (0, 0)


@pytest.mark.parametrize(("node_1", "status"), [(b"OK", "FAIL"), (None, "PASS")], ids=["served", "moved"])
def test_a_module_command_that_finds_its_keys_by_a_callback_has_no_specs_to_judge_and_is_routed_by_them(
    builds, tmp_path, monkeypatch, node_1, status
):
    # A getkeys-api command: movablekeys, and no key spec at all.
    entry = _entry("getkeys.command", -2, ["module", "movablekeys"])
    model = Commands(entry, cluster=True, getkeys={"GETKEYS.COMMAND": lambda words: [b"k"]})
    moved = Error(f"MOVED {_SLOT} 127.0.0.1:7000")
    model.answers["GETKEYS.COMMAND"] = lambda node, words: (node_1 or moved) if node else None
    result, value = run(builds, model, tmp_path, monkeypatch, _one("GETKEYS.COMMAND", "key", "k"))

    assert result.status.value == status, result.summary
    assert (value["judged"]["keys"], value["judged"]["routing"]) == (0, 1)
    if status == "FAIL":
        assert _finding(result, value, "misrouted")["declared"] == {"keys": ["k"], "slots": [_SLOT]}


def test_a_node_that_sees_the_cluster_down_routes_nothing_to_judge(builds, tmp_path, monkeypatch):
    down = Error("CLUSTERDOWN The cluster is down")
    model = Commands(GET, PING, cluster=True)
    model.answers = dict.fromkeys(("GET", "PING"), lambda node, words: down if node else None)
    result, value = run(builds, model, tmp_path, monkeypatch, _one("GET", "k"), _one("PING"))

    assert result.status.value == "PASS", result.summary
    assert value["judged"]["routing"] == 0


def _positions(entry, *words):
    [metadata] = command_metadata.read_metadata([entry]).values()
    return metadata, spec_positions(metadata, list(words))


@pytest.mark.parametrize(
    ("entry", "words", "expected"),
    [
        (GET, ("GET", "k"), [(1, False)]),
        (MSET, ("MSET", "a", "1", "b", "2"), [(1, False), (3, False)]),
        (SPUBLISH, ("SPUBLISH", "channel", "m"), [(1, True)]),
        (EVAL_RO, ("EVAL_RO", "script", "2", "a", "b", "argument"), [(3, False), (4, False)]),
        (MSETEX, ("MSETEX", "2", "a", "1", "b", "2", "NX"), [(2, False), (4, False)]),
        # Of what follows STREAMS, the first half are keys; the keyword in any case.
        (XREAD, ("XREAD", "COUNT", "5", "streams", "a", "b", "0", "0"), [(4, False), (5, False)]),
        # Without its keyword a spec has no keys.
        (XREAD, ("XREAD", "COUNT", "5", "a", "0"), []),
        # Searched backwards from the end for a negative startfrom: the last KEYS at or before it.
        (_entry("m", -2, [], specs=[_key_spec(_keyword("KEYS", -2), _range(-1), "RW")]), ("M", "KEYS", "x", "KEYS", "a", "b"), [(4, False), (5, False)]),
        # Two specs: a destination, then keys counted by an argument.
        (_entry("z", -4, [], specs=[_key_spec(_index(1), _range(), "OW"), _key_spec(_index(2), _keynum(), "RO")]), ("Z", "d", "2", "a", "b"), [(1, False), (3, False), (4, False)]),
        # Up to the last argument but one.
        (_entry("b", -3, [], specs=[_key_spec(_index(1), _range(-2), "RW")]), ("B", "a", "b", "1"), [(1, False), (2, False)]),
    ],
)  # fmt: skip
def test_key_specs_find_the_keys_of_a_command(entry, words, expected):
    assert _positions(entry, *words)[1] == expected


@pytest.mark.parametrize(
    ("entry", "words"),
    [
        # A count that is no number, or negative, or counts more keys than there are arguments, or none.
        (EVAL_RO, ("EVAL_RO", "script", "x")),
        (EVAL_RO, ("EVAL_RO", "script", "-1", "a")),
        (EVAL_RO, ("EVAL_RO", "script", "3", "a", "b")),
        (EVAL_RO, ("EVAL_RO", "script", "0")),
        # Arguments that end before the keys.
        (GET, ("GET",)),
        (EVAL_RO, ("EVAL_RO", "script")),
        # Specs that do not say where the keys are.
        (_entry("s", -2, [], specs=[_key_spec(["type", "unknown", "spec", []], ["type", "unknown", "spec", []], "RO")]), ("S", "k")),
        (_entry("g", -2, [], specs=[_key_spec(_index(1), _range(), "RW", "incomplete")]), ("G", "k")),
        (_entry("f", -2, [], specs=[_key_spec(_index(1), ["type", "unknown", "spec", []], "RO")]), ("F", "k")),
    ],
)  # fmt: skip
def test_key_specs_that_do_not_fit_the_arguments_or_do_not_say_find_nothing(entry, words):
    assert _positions(entry, *words)[1] is None


@pytest.mark.parametrize(
    ("entry", "words", "expected"),
    [
        (GET, ("GET", "k"), [1]),
        (MSET, ("MSET", "a", "1", "b", "2"), [1, 3]),
        (PFCOUNT, ("PFCOUNT", "a", "b"), [1, 2]),
        (PING, ("PING",), []),
        (GET, ("GET",), None),
    ],
)
def test_first_last_and_step_find_the_keys_of_a_command(entry, words, expected):
    assert range_positions(_positions(entry, *words)[0], list(words)) == expected


def test_metadata_is_read_by_the_name_the_workload_sends_a_command_under():
    get = _entry("config|get", -3, ["admin"])
    metadata = command_metadata.read_metadata([_entry("config", -2, [], subcommands=[get]), XREAD])

    assert sorted(metadata) == ["CONFIG GET", "XREAD"]
    assert (metadata["CONFIG GET"]["arity"], metadata["CONFIG GET"]["flags"]) == (-3, {"admin"})
    assert metadata["XREAD"]["range"] == (0, 0, 0)
    assert metadata["XREAD"]["specs"] == [
        {
            "flags": {"RO"},
            "begin": ("keyword", {"keyword": "STREAMS", "startfrom": 1}),
            "find": ("range", {"lastkey": -1, "keystep": 1, "limit": 2}),
        }
    ]


# ---------------------------------------------------------------------------
# arity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("words", "reply", "judged", "status"),
    [
        (("GET", "k", "extra"), b"OK", 1, "FAIL"),
        (("GET",), Error("ERR syntax error"), 1, "FAIL"),
        (("SET", "k"), b"OK", 1, "FAIL"),
        (("GET", "k", "extra"), ARITY, 1, "PASS"),
        (("SET", "k", "v", "more"), b"OK", 0, "PASS"),
        (("GET", "k"), b"OK", 0, "PASS"),
    ],
)
def test_a_command_with_a_number_of_arguments_its_arity_excludes_is_refused(
    builds, tmp_path, monkeypatch, words, reply, judged, status
):
    refused = isinstance(reply, Error)
    model = Commands(GET, SET, answers={words[0]: lambda node, words: reply if refused or not node else None})
    result, value = run(builds, model, tmp_path, monkeypatch, _one(*words))

    assert (result.status.value, value["judged"]["arity"]) == (status, judged), result.summary
    if status == "FAIL":
        finding = _finding(result, value, "arity-accepted")
        assert finding["declared"] == {"arity": 2 if words[0] == "GET" else -3}
        assert finding["observed"]["arguments"] == len(words)
        assert result.summary == f"{words[0]} was not refused a number of arguments its arity excludes"


# ---------------------------------------------------------------------------
# Connections, and servers that stop answering
# ---------------------------------------------------------------------------


def test_after_a_pubsub_command_the_next_command_takes_a_new_connection(builds, tmp_path, monkeypatch):
    # UNSUBSCRIBE a b replies once per channel: what follows its first reply would pair with the next command.
    model = Commands(UNSUBSCRIBE, PING, cluster=True)
    draws = [_one("PING"), _one("UNSUBSCRIBE", "a", "b"), _one("PING"), _one("PING")]
    result, _ = run(builds, model, tmp_path, monkeypatch, *draws)

    assert result.status.value == "PASS", result.summary
    # On each node: the connection the cluster formed on, and one more after the UNSUBSCRIBE (node 0 has the
    # oracle's own, too).
    assert [len(node.wires) for node in _nodes(BUILDS[builds])] == [3, 2]


def test_a_connection_the_server_closed_opens_anew(builds, tmp_path, monkeypatch):
    closed = []

    def ping(node, words):
        # The connection a QUIT before it closed.
        if not closed:
            closed.append(node)
            return CLOSE

    model = Commands(QUIT, PING, GET, answers={"PING": ping})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("QUIT"), _one("PING"), _one("GET", "k"))

    assert result.status.value == "PASS", result.summary
    assert _sent(0) == [["QUIT"], ["PING"], ["PING"], ["GET", "k"]] and value["commands"] == 3


@pytest.mark.parametrize("node", [0, 1])
def test_a_node_that_exits_is_a_crash(builds, tmp_path, monkeypatch, node):
    model = Commands(GET, SET, answers={"GET": lambda at, words: DIE if at == node else None})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("SET", "k", "v"), _one("GET", "k"), _one("GET", "j"))

    assert (result.status.value, result.crash) == ("FAIL", True), result.summary
    assert result.summary == (
        f"node {node} exited on command 1 (GET k) of the command-metadata check: crashed by signal: 11"
    )
    assert result.diagnostics["crashes"] == {"candidate": "crashed by signal: 11"}
    assert value["commands"] == 2 and all(server.stopped for server in SERVERS.values())


def test_a_command_without_a_reply_leaves_the_commands_unjudged(builds, tmp_path, monkeypatch):
    model = Commands(GET, SET, answers={"GET": lambda node, words: TIMEOUT})
    result, value = run(builds, model, tmp_path, monkeypatch, _one("SET", "k", "v"), _one("GET", "k"), _one("GET", "j"))

    assert result.status.value == "ERROR"
    assert result.summary.startswith("command 1 (GET k) on node 0 got no reply: TimeoutError")
    assert value["commands"] == 2


def test_servers_that_do_not_start_leave_the_commands_unjudged(builds, tmp_path, monkeypatch):
    BUILDS[builds].rejects = {"appendonly"}
    workload, oracle = _specs()
    context = _context(builds, Commands(GET), tmp_path, workload, oracle)
    result, value = _evaluate(context, oracle)

    assert result.status.value == "ERROR" and "the metadata.node0 server exited at startup" in result.summary
    assert value is None and not context.obligations.report()["still_active"]


def test_a_finding_is_named_once_per_command_and_counted(builds, tmp_path, monkeypatch):
    model = Commands(GET, SET, NKSET, cluster=True, answers={"NKSET": lambda node, words: 1})
    _reading(model, digest=1, offset=1)
    draws = [_one(*_NKSET), _one("GET", "k"), _one("NKSET", "1", "j", "v"), _one("GET", "j"), _one("SET", "k", "v")]
    result, value = run(builds, model, tmp_path, monkeypatch, *draws)

    assert result.status.value == "FAIL"
    # One clause per command and reason, in the same words whatever the arguments: a batch groups its runs by them.
    assert result.summary == (
        "GET has the readonly flag, yet changed data or propagated; "
        "NKSET was not routed by the keys it declares on a node that owns no slot"
    )
    assert [(item["name"], item["index"], item["count"]) for item in value["findings"]] == [
        ("NKSET", 0, 2),
        ("GET", 1, 2),
    ]
    assert to_plain_data(result.diagnostics["findings"]) == value["findings"]


def test_long_arguments_are_cut_short_in_a_findings_command():
    assert command_metadata._text(["SET", "k", "x" * 65]) == f"SET k {'x' * 61}..."
    assert command_metadata._text(["SET", "k", "x" * 64]) == f"SET k {'x' * 64}"
