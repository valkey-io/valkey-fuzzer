"""``valkey.action.commands`` against real builds: one build on both sides replies equal (RESP2 and RESP3), a
marked command with equal replies is unreached, a candidate crash is a FAIL with its logs kept, the private
servers run as one-node and two-node clusters, every check passes on one build, and the LSET example reaches
valkey-io/valkey#4823 on builds of the fix (``VALKEY_FUZZER_LSET_FIX_BINARY``, commit e50b62d0b) and its parent
(``VALKEY_FUZZER_LSET_PARENT_BINARY``).

Needs a valkey-server (``VALKEY_FUZZER_BINARY`` or ``PATH``); the LSET rows need the two builds above, and the
compat row across builds an older build (``VALKEY_FUZZER_OLD_BINARY``).
"""

import os
from pathlib import Path

import pytest

from tests.live_support import BINARY, OLD_BINARY, check, evidence, needs_old_binary, run_scenario, scenario
from tests.valkey_experiments_support import registration_for, valkey_service
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.components import actions
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.testing import FakeContext, resolved_spec

pytestmark = [pytest.mark.live, pytest.mark.slow]

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)

FIX = os.environ.get("VALKEY_FUZZER_LSET_FIX_BINARY")
PARENT = os.environ.get("VALKEY_FUZZER_LSET_PARENT_BINARY")
needs_lset_builds = pytest.mark.skipif(
    not (FIX and PARENT), reason="set VALKEY_FUZZER_LSET_FIX_BINARY and VALKEY_FUZZER_LSET_PARENT_BINARY"
)
EXAMPLE_EXPERIMENTS = Path(__file__).resolve().parents[1] / "examples" / "valkey" / "experiments"

LSET = [
    {"command": ["RPUSH", "l", "a", "b", "c"], "expect": {"equals": 3}},
    {"command": ["LSET", "l", 9223372036854775807, "x"], "expect_difference": True, "expect": {"error": "out of range"}},
    {"command": ["LRANGE", "l", 0, -1], "expect_difference": True, "expect": {"equals": ["a", "b", "c"]}},
]  # fmt: skip


def run(candidate, commands, **config):
    """The action with ``candidate`` as the deployment's binary; the result and its evidence value."""

    spec = ComponentSpec(
        role=ComponentRole.ACTION, use="valkey.action.commands", id="seq", config={"commands": commands, **config}
    )
    spec = resolved_spec(registration_for("valkey.action.commands"), spec)
    context = FakeContext(spec, deployment={"valkey_binary": candidate})
    result = actions.CommandsAction().run(context, spec)
    return result, (to_plain_data(result.evidence[0].value) if result.evidence else None)


@pytest.mark.parametrize("resp", [2, 3])
def test_one_build_on_both_sides_replies_the_same(resp):
    commands = [
        {"command": ["SET", "k", "v"], "expect": {"equals": "OK"}},
        {"command": ["SET", "bin", {"$base64": "/wD/"}]},
        {"command": ["GET", "bin"], "expect": {"equals": {"$base64": "/wD/"}}},
        {"command": ["HSET", "h", "f1", "v1", "f2", "v2"], "expect": {"equals": 2}},
        {"command": ["HGETALL", "h"], "compare": "unordered-pairs"},
        {"command": ["SMEMBERS", "missing"], "compare": "unordered"},
        {"command": ["MULTI"]},
        {"command": ["INCR", "n"]},
        {"command": ["EXEC"], "expect": {"equals": [1]}},
        {"command": ["TIME"], "compare": "skip"},
        {"command": ["LSET", "k", 0, "x"], "expect": {"error": True}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, resp=resp)

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["digest"] == value["parent"]["digest"]
    assert (value["reached"], value["unexplained"], value["dropped_config"]) == ([], [], [])
    replies = {record["index"]: record for record in value["candidate"]["replies"]}
    assert replies[4]["reply"] == (["f1", "v1", "f2", "v2"] if resp == 2 else {"f1": "v1", "f2": "v2"})
    assert replies[8]["reply"] == [1] and "error" in replies[10]


def test_a_marked_command_with_equal_replies_did_not_reach_the_change():
    result, value = run(
        BINARY, [{"command": ["PING"], "expect_difference": True, "expect": {"equals": "PONG"}}], compare_with=BINARY
    )

    assert result.status.value == "ERROR" and result.diagnostics["reason"] == "unreached-change"
    assert value["reached"] == []


def test_an_option_the_parent_rejects_is_dropped_on_both_sides():
    result, value = run(
        BINARY, [{"command": ["CONFIG", "GET", "maxmemory"], "expect": {"equals": ["maxmemory", "104857600"]}}],
        compare_with=BINARY, server_config={"no-such-option": 1, "maxmemory": "100mb"},
    )  # fmt: skip

    assert result.status.value == "PASS", result.summary
    assert value["dropped_config"] == ["no-such-option"]


def test_a_candidate_crash_is_a_fail_whose_logs_survive():
    result, value = run(
        BINARY, [{"command": ["SET", "k", "v"]}, {"command": ["DEBUG", "SEGFAULT"]}], compare_with=BINARY
    )

    assert result.status.value == "FAIL" and result.crash, result.summary
    assert result.summary.startswith("the candidate exited on command 1 (DEBUG SEGFAULT): crashed by signal")
    assert value["candidate"]["exited"] is not None
    kept = {os.path.basename(path): path for path in result.evidence[0].artifacts}
    assert "candidate.server.log" in kept and os.path.exists(kept["candidate.server.log"])


def test_cluster_runs_both_servers_as_one_node_clusters():
    commands = [
        {"command": ["SET", "k", "v"], "expect": {"equals": "OK"}},
        {"command": ["CLUSTER", "COUNTKEYSINSLOT", 7629], "expect": {"equals": 1}},
        {"command": ["CLUSTER", "INFO"], "compare": "skip", "expect": {"contains": "cluster_state:ok"}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, cluster=True)

    assert result.status.value == "PASS", result.summary
    assert value["cluster"] is True


def test_two_node_clusters_compare_equal_through_their_placeholders():
    commands = [
        {"command": ["CLUSTER", "MYID"], "node": 1, "expect": {"equals": "<node.1.id>"}},
        {"command": ["CLUSTER", "SLOTS"], "node": 1},
        {"command": ["SET", "k", "v"], "node": 1, "expect": {"error": "MOVED 7629 127.0.0.1:<node.0.port>"}},
        {"command": ["CLUSTER", "COUNT-FAILURE-REPORTS", "<node.0.id>"], "node": 1, "expect": {"equals": 0}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, cluster=True, nodes=2)

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["digest"] == value["parent"]["digest"]
    (first, last, primary), *_ = value["candidate"]["replies"][1]["reply"]
    assert (first, last, primary[:3]) == (0, 16383, ["127.0.0.1", "<node.0.port>", "<node.0.id>"])


def test_a_slot_migration_is_waited_for():
    commands = [
        {"command": ["SET", "k", "v"], "expect": {"equals": "OK"}},
        {"command": ["CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", 7629, 7629, "NODE", "<node.1.id>"]},
        {"command": ["GET", "k"], "node": 1, "wait_s": 30, "expect": {"equals": "v"}},
        {"command": ["GET", "k"], "expect": {"error": "MOVED 7629 127.0.0.1:<node.1.port>"}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, cluster=True, nodes=2)

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["digest"] == value["parent"]["digest"]


def test_raw_bytes_are_answered_or_left_unanswered():
    commands = [
        {"raw": "PING\r\n", "expect": {"equals": "PONG"}},
        {"raw": "*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n", "expect": {"equals": "hi"}},
        {"raw": 'SET k "a b"\r\n', "expect": {"equals": "OK"}},
        {"command": ["GET", "k"], "expect": {"equals": "a b"}},
        # Not a whole request: the server waits for the rest.
        {"raw": "*2\r\n$4\r\nECHO\r\n"},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY)

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["replies"][-1] == {"index": 4, "no_reply": True}


def test_a_command_that_holds_its_connection_goes_raw_and_a_later_read_takes_its_reply():
    # The walk's test of valkey#2858 sent this EVAL as a command, which waited for a reply SCRIPT KILL never came
    # to send.
    commands = [
        {"command": ["CONFIG", "SET", "busy-reply-threshold", 100], "expect": {"equals": "OK"}},
        {"raw": 'EVAL "while true do end" 0\r\n', "connection": "busy"},
        {"command": ["SCRIPT", "KILL"], "wait_s": 20, "expect": {"equals": "OK"}},
        {"read": True, "connection": "busy"},
        {"command": ["PING"], "expect": {"equals": "PONG"}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY)

    assert result.status.value == "PASS", result.summary
    replies = value["candidate"]["replies"]
    assert replies[1] == {"index": 1, "no_reply": True}
    ((killed,),) = [record["reply"] for record in replies if record["index"] == 3]
    assert "killed" in killed["$error"]


@pytest.mark.parametrize("resp", [2, 3])
def test_a_subscriber_on_its_own_connection_reads_what_was_published(resp):
    commands = [
        {"command": ["SUBSCRIBE", "news"], "connection": "sub", "expect": {"equals": ["subscribe", "news", 1]}},
        {"command": ["PUBLISH", "news", "hi"], "expect": {"equals": 1}},
        {"read": True, "connection": "sub", "expect": {"equals": [["message", "news", "hi"]]}},
        # Bytes the server holds stay on their own connection; the main one goes on.
        {"raw": "*2\r\n$4\r\nECHO\r\n", "connection": "raw"},
        {"command": ["PING"], "expect": {"equals": "PONG"}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, resp=resp)

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["replies"][3] == {"index": 3, "no_reply": True}


def test_a_restart_reads_the_rewritten_config_file_the_acl_file_and_the_rdb_back():
    commands = [
        {"command": ["ACL", "USERS"], "expect": {"equals": ["alice", "default"]}},
        {"command": ["CONFIG", "SET", "maxmemory", 12345678], "expect": {"equals": "OK"}},
        {"command": ["CONFIG", "REWRITE"], "expect": {"equals": "OK"}},
        {"command": ["ACL", "SETUSER", "bob", "on", "nopass"], "expect": {"equals": "OK"}},
        {"command": ["ACL", "SAVE"], "expect": {"equals": "OK"}},
        {"command": ["SET", "k", "v"], "expect": {"equals": "OK"}},
        {"command": ["SAVE"], "expect": {"equals": "OK"}},
        {"restart": True, "expect": {"equals": "OK"}},
        {"command": ["CONFIG", "GET", "maxmemory"], "expect": {"equals": ["maxmemory", "12345678"]}},
        {"command": ["ACL", "USERS"], "expect": {"equals": ["alice", "bob", "default"]}},
        {"command": ["GET", "k"], "expect": {"equals": "v"}},
    ]
    files = {"users.acl": "user default on nopass ~* &* +@all\nuser alice on nopass ~* +@all\n"}
    result, value = run(BINARY, commands, compare_with=BINARY, files=files, server_config={"aclfile": "users.acl"})

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["digest"] == value["parent"]["digest"]


# Every type in its compact encoding, replies whose RESP3 shape differs (a map, doubles, nested pairs, a
# boolean), a transaction, an expiring key, and a script.
CHECKED = [
    {"command": ["HSET", "h", "f1", "v1", "f2", "v2"], "expect": {"equals": 2}},
    {"command": ["HGETALL", "h"], "compare": "unordered-pairs"},
    {"command": ["SADD", "ints", 3, 1, 2], "expect": {"equals": 3}},
    {"command": ["SADD", "s", "a", "b", 1], "expect": {"equals": 3}},
    {"command": ["SMEMBERS", "ints"], "compare": "unordered"},
    {"command": ["SINTERCARD", 2, "ints", "s"], "expect": {"equals": 1}},
    {"command": ["ZADD", "z", 1.5, "a", 2, "b"], "expect": {"equals": 2}},
    {"command": ["ZRANGE", "z", 0, -1, "WITHSCORES"]},
    {"command": ["ZINCRBY", "z", 0.25, "a"]},
    {"command": ["ZSCORE", "z", "a"]},
    {"command": ["RPUSH", "l", "a", "b", "c"], "expect": {"equals": 3}},
    {"command": ["LRANGE", "l", 0, -1], "expect": {"equals": ["a", "b", "c"]}},
    {"command": ["LPOS", "l", "b"], "expect": {"equals": 1}},
    {"command": ["OBJECT", "ENCODING", "h"]},
    {"command": ["SET", "n", 10]},
    {"command": ["INCRBYFLOAT", "n", 0.5]},
    {"command": ["SET", "t", "v", "PX", 100]},
    {"command": ["EVAL", "return redis.call('EXISTS', KEYS[1]) == 1", 1, "h"]},
    {"command": ["MULTI"]},
    {"command": ["INCR", "c"]},
    {"command": ["HDEL", "h", "f1"]},
    {"command": ["EXEC"], "expect": {"equals": [1, 1]}},
    {"command": ["COPY", "z", "z2"], "expect": {"equals": 1}},
    {"command": ["BLPOP", "l", 1], "expect": {"equals": ["l", "a"]}},
    {"command": ["PING"]},
]
ALL_CHECKS = {"variants": True, "replica": True, "acl": True, "concurrency": 4, "compat": True}


@pytest.mark.parametrize(
    "checks",
    [{"variants": True}, {"replica": True}, {"acl": True}, {"concurrency": 4}, {"compat": True}, ALL_CHECKS],
    ids=["variants", "replica", "acl", "concurrency", "compat", "all"],
)
def test_every_check_passes_on_one_build(checks):
    result, value = run(BINARY, CHECKED, compare_with=BINARY, checks=checks)

    assert result.status.value == "PASS", result.summary
    assert {name: outcome["outcome"] for name, outcome in value["checks"].items()} == dict.fromkeys(checks, "pass")
    assert value["checks"].get("variants", {}).get("encodings", {"outcome": "pass"})["outcome"] == "pass"
    if "acl" in checks:
        assert value["checks"]["acl"]["category"]["user"] == "default +@all -@write"
        assert value["checks"]["acl"]["keys"]["outcome"] == "pass"


def test_the_protocol_variant_compares_only_what_ran_before_a_connection_opened_behind_a_password():
    # The walk's test of valkey#2237 (fuzzer#106): in RESP3, a connection first used while a password is required
    # opens with HELLO 3, which Valkey refuses, and the sequence stops there; the main connection's later
    # commands never ran.
    commands = [
        {"command": ["CONFIG", "SET", "requirepass", "s3cret"], "expect": {"equals": "OK"}},
        {"command": ["AUTH", "s3cret"], "expect": {"equals": "OK"}},
        {"command": ["PING"], "connection": "late", "expect": {"error": "NOAUTH"}},
        {"command": ["AUTH", "nope"], "expect": {"error": "WRONGPASS"}},
        {"command": ["CONFIG", "SET", "requirepass", ""], "expect": {"equals": "OK"}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, checks={"variants": True})

    assert result.status.value == "PASS", result.summary
    assert value["checks"]["variants"]["outcome"] == "pass"


def test_checks_on_a_two_node_cluster_skip_what_needs_one_standalone_node():
    commands = [
        {"command": ["SET", "k", "v"], "expect": {"equals": "OK"}},
        {"command": ["HSET", "{k}h", "f", "v"], "expect": {"equals": 1}},
        {"command": ["GET", "k"], "node": 1, "expect": {"error": "MOVED 7629 127.0.0.1:<node.0.port>"}},
        {"command": ["CLUSTER", "COUNTKEYSINSLOT", 7629], "expect": {"equals": 2}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, cluster=True, nodes=2, checks=ALL_CHECKS)

    assert result.status.value == "PASS", result.summary
    assert {name: outcome["outcome"] for name, outcome in value["checks"].items()} == {
        "variants": "pass",
        "replica": "skipped",
        "acl": "pass",
        "concurrency": "pass",
        "compat": "skipped",
    }


def test_a_key_expired_but_not_yet_deleted_when_the_parent_saves_still_compares():
    # A primary drops it loading the RDB; the parent's digest still has it.
    commands = [
        {"command": ["DEBUG", "SET-ACTIVE-EXPIRE", 0], "expect": {"equals": "OK"}},
        {"command": ["SET", "t", "v", "PX", 50], "expect": {"equals": "OK"}},
        {"command": ["DEBUG", "SLEEP", 0.1], "expect": {"equals": "OK"}},
        {"command": ["SET", "k", "v"], "expect": {"equals": "OK"}},
    ]
    result, value = run(BINARY, commands, compare_with=BINARY, checks={"replica": True, "compat": True})

    assert result.status.value == "PASS", result.summary
    assert value["checks"]["compat"]["outcome"] == "pass"


@needs_old_binary
def test_an_older_builds_data_loads_and_replicates_into_the_candidate():
    result, value = run(BINARY, CHECKED, compare_with=OLD_BINARY, checks={"compat": True})

    assert value["checks"]["compat"]["outcome"] == "pass", value["checks"]["compat"]
    assert result.status.value in ("PASS", "ERROR"), result.summary


@needs_lset_builds
@pytest.mark.parametrize("resp", [2, 3])
def test_the_lset_example_reaches_the_change_on_the_fix_and_its_parent(tmp_path, resp):
    runtime = valkey_service(
        experiment_directories=[str(EXAMPLE_EXPERIMENTS)], artifact_dir=str(tmp_path / "artifacts")
    )
    document = scenario(
        "example.commands-compare", inputs={"parent": {"$ref": "artifacts.parent.binary"}, "resp": resp}, binary=FIX
    )
    document["artifacts"] = {"parent": {"use": "valkey.artifact.local-binary", "with": {"path": PARENT}}}
    outcome = run_scenario(runtime, document)

    check(outcome, "PASS")
    value = evidence(outcome, "valkey.command.sequence")
    assert (value["reached"], value["unexplained"]) == ([1, 2, 3, 5], [])
    assert value["candidate"]["digest"] != value["parent"]["digest"]


@needs_lset_builds
def test_an_unmarked_read_of_the_fixed_write_is_an_unexplained_difference():
    commands = [*LSET[:2], {"command": ["LRANGE", "l", 0, -1]}]
    result, value = run(FIX, commands, compare_with=PARENT)

    assert result.status.value == "ERROR", result.summary
    assert result.diagnostics["reason"] == "unexplained-difference"
    assert [item["index"] for item in result.diagnostics["commands"]] == [2]
    assert (value["reached"], value["unexplained"]) == ([1], [2])
