"""The local deployment's state checks and failovers: a PASS case each, and a FAIL case per way to wrongly pass."""

import random
from contextlib import ExitStack
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from valkey.crc import key_slot
from valkey.exceptions import ResponseError

from tests.deployment_support import (
    MIGRATED_SLOTS,
    MIGRATED_TOPOLOGY,
    FakeCluster,
    client_context,
    deployment_view,
    fake_client,
    instance,
    migrated_connection,
    node_row,
    planned_cluster_instances,
    snapshot_of,
)
from tests.support import VirtualClock
from valkey_fuzzer.deployment.local import failover, queries
from valkey_fuzzer.deployment.local.cluster_nodes import parse_cluster_nodes, split_slots
from valkey_fuzzer.deployment.local.failover import failover_shard, wait_until_settled
from valkey_fuzzer.deployment.local.models import (
    DeploymentConfig,
    DeploymentMode,
    LocalDeployment,
    LocalDeploymentError,
)
from valkey_fuzzer.deployment.local.queries import WaitInterruptedError
from valkey_fuzzer.deployment.local.service import ValkeyDeploymentService
from valkey_fuzzer.deployment.local.topology import DeploymentConnection
from valkey_fuzzer.deployment.local.validation import check as check_module
from valkey_fuzzer.deployment.local.validation import snapshot
from valkey_fuzzer.deployment.local.validation.check import SYNC_WAIT_S, DeploymentStateCheck, syncing_replicas
from valkey_fuzzer.deployment.local.validation.cluster_state import check_cluster_status, check_slot_coverage
from valkey_fuzzer.deployment.local.validation.config import (
    ClusterStatusValidationConfig,
    DataConsistencyValidationConfig,
    ReplicationValidationConfig,
    SlotCoverageValidationConfig,
    StateValidationConfig,
    ViewConsistencyValidationConfig,
)
from valkey_fuzzer.deployment.local.validation.data import _check_replicas, check_data, write_test_keys
from valkey_fuzzer.deployment.local.validation.logs import LogNode, LogValidationResult, check_logs
from valkey_fuzzer.deployment.local.validation.replication import check_replication
from valkey_fuzzer.deployment.local.validation.results import (
    ReplicationValidation,
    RoleMismatch,
    RoleValidation,
    StateValidationResult,
)
from valkey_fuzzer.deployment.local.validation.snapshot import Snapshot
from valkey_fuzzer.deployment.local.validation.views import ExpectedRoles, check_roles, check_views

LOCAL = "valkey_fuzzer.deployment.local."
CHECK = f"{LOCAL}validation.check"
CHECKS = ("replication", "cluster_status", "slot_coverage", "roles", "view_consistency", "data_consistency", "logs")


def _only(*checks, **config):
    return StateValidationConfig(**{f"check_{name}": name in checks for name in CHECKS}, **config)


def _by_port(answers):
    """A ``valkey_client`` whose client on each port is ``answers[port]``; other ports refuse."""

    def connect(host, port, *args, **kwargs):
        if port not in answers:
            raise ConnectionError(port)
        return client_context(answers[port])

    return connect


def _validate_once(config, connection, instances=(), stopped=()):
    """One validation attempt of ``config`` over ``connection``, with ``stopped`` instances stopped on purpose."""
    check = DeploymentStateCheck(connection, list(instances), {}, config, rng=random.Random(0))
    for stopped_instance in stopped:
        check.mark_stopped(stopped_instance.instance_id, stopped_instance.role)
    return check._validate_once(None)


def _row(port, slots=1, status="connected"):
    return node_row(f"p{port}", port, shard_id=port, status=status, slots=tuple(range(slots)))


_OK, _FAIL = "cluster_state:ok\r\n", "cluster_state:fail\r\n"


@pytest.mark.parametrize(
    ("rows", "replies", "accept", "message"),
    [
        # Primaries stopped on purpose whose replicas took their slots are slotless and have no vote.
        ([_row(0, 0, "failed"), _row(1, 0, "failed"), _row(2), _row(3), _row(4)], {2: _OK, 3: _OK, 4: _OK}, None, None),
        ([_row(0), _row(1)], {0: {b"cluster_state": b"ok"}, 1: _FAIL}, ["ok", "fail"], None),  # RESP3 map
        # Slotless primaries cannot make up a majority of the slot owners.
        ([_row(0), _row(1, 1, "failed"), _row(2, 1, "failed"), _row(3, 0)], {0: _OK, 3: _OK}, None, "1/3 slot-owning"),
        ([_row(0), _row(1, 1, "failed")], {0: "cluster_known_nodes:2\r\n"}, ["ok", "unknown"], "missing or blank"),
        ([_row(0), _row(1, 1, "failed")], {0: "cluster_state:unknown\r\n"}, None, "unacceptable states"),
        # Nodes that disagree are not majority-voted.
        ([_row(0), _row(1), _row(2)], {0: _OK, 1: _FAIL, 2: _OK}, None, "Cluster state disagreement"),
        # Valkey's fail state passes only when listed; "degraded" is no state Valkey reports.
        ([_row(0), _row(1)], {0: _FAIL, 1: _FAIL}, None, "unacceptable states"),
        ([_row(0), _row(1)], {0: _FAIL, 1: _FAIL}, ["ok", "fail"], None),
        (
            [_row(0), _row(1)],
            {0: "cluster_state:degraded\r\n", 1: "cluster_state:degraded\r\n"},
            ["ok", "fail"],
            "unacceptable",
        ),
    ],
)
def test_cluster_status_needs_a_quorum_of_slot_owners_agreeing_on_an_acceptable_state(rows, replies, accept, message):
    killed = {row.address for row in rows if row.failed}
    config = ClusterStatusValidationConfig(**({"acceptable_states": accept} if accept else {}))
    result = check_cluster_status(snapshot_of(*rows, cluster_info=replies), config, killed)

    assert result.success is (message is None), result.error_message
    assert message is None or message in result.error_message


_SHARDS = (
    "a 127.0.0.1:7000@17000 {}master - 0 0 1 connected 0-8191\nb 127.0.0.1:7001@17001 {}master{} - 0 0 2 {} 8192-{}"
)


@pytest.mark.parametrize("stopped", [False, True])
def test_every_slot_must_be_covered_in_every_live_view_unless_its_whole_shard_was_stopped(stopped):
    views = {
        7000: _SHARDS.format(
            "myself,", "", ",fail" if stopped else "", "disconnected" if stopped else "connected", 16383
        ),
        7001: _SHARDS.format("", "myself,", "", "connected", 16000),  # misses slots node a sees covered
    }
    rows = [node_row("a", 7000), node_row("b", 7001, shard_id=1, status="failed" if stopped else "connected")]
    kills = ({"127.0.0.1:7001"}, {1}) if stopped else ((), ())

    result = check_slot_coverage(snapshot_of(*rows, cluster_nodes=views), SlotCoverageValidationConfig(), *kills)

    assert result.success is stopped
    assert stopped or ("127.0.0.1:7001" in result.error_message and "16001-16383" in result.error_message)


def test_slots_on_a_stopped_shard_fail_while_a_live_member_that_migrated_into_it_could_take_them():
    # Every planned member of shard 0 was stopped, but valkey-4 had migrated into shard 0 and is alive. Excusing
    # slots 0-4095 because the plan's shard 0 is gone would hide a failed failover.
    text = MIGRATED_TOPOLOGY.replace("slave n4 0 0 14 connected", "master,fail - 0 0 13 disconnected 0-4095").replace(
        "master - 0 0 14 connected 0-4095", "slave n2 0 0 13 connected"
    )
    planned = planned_cluster_instances()
    answering = fake_client({"role": "master"}, lambda *args: text if args[1] == "NODES" else _OK)
    with (
        patch(f"{LOCAL}topology.query_cluster_nodes", return_value=parse_cluster_nodes(text)),
        patch(f"{LOCAL}validation.snapshot.valkey_client", return_value=client_context(answering)),
    ):
        connection = migrated_connection()
        result = _validate_once(_only("slot_coverage", stabilization_wait=0), connection, planned, planned[:3])
        result = result.slot_coverage

    assert result.success is False and "4096 slots still assigned to stopped nodes" in result.error_message


def _view(me, replica_of="a", epoch_b=2, c_primary=False):
    flag = {node: "myself," if node == me else "" for node in "abc"}
    c_role = "master -" if c_primary else f"slave {replica_of}"
    return (
        f"a 127.0.0.1:7000@17000 {flag['a']}master - 0 0 1 connected 0-8191\n"
        f"b 127.0.0.1:7001@17001 {flag['b']}master - 0 0 {epoch_b} connected 8192-16383\n"
        f"c 127.0.0.1:7002@17002 {flag['c']}{c_role} 0 0 1 connected"
    )


# A primary has no primary, and its own config epoch.
_ROLE_FLIP = [("role", "c"), ("primary", "c"), ("config_epoch", "c")]


@pytest.mark.parametrize(
    ("views", "success", "found"),
    [
        ({7000: _view("a"), 7001: _view("b", "b"), 7002: _view("c")}, False, [("primary", "c")]),
        # A stopped replica's stale primary is recorded, not failed.
        ({7000: _view("a"), 7001: _view("b", "b")}, True, [("primary", "c")]),
        # A live node seen as a primary by one peer and a replica by another: the views disagree.
        ({7000: _view("a"), 7001: _view("b", c_primary=True), 7002: _view("c")}, False, _ROLE_FLIP),
        # A node demoted and then stopped, whose demotion one peer undid on a late packet: it can no longer
        # correct that view, and serves nothing.
        ({7000: _view("a"), 7001: _view("b", c_primary=True)}, True, _ROLE_FLIP),
    ],
    ids=["live-replicas-primary", "stopped-replicas-primary", "live-role", "stopped-role"],
)
def test_live_nodes_must_agree_on_each_replicas_primary_and_each_primarys_epoch(views, success, found):
    rows = [node_row("a", 7000), node_row("b", 7001, shard_id=1), node_row("c", 7002, "replica")][: len(views)]
    killed = {"127.0.0.1:7002"} if len(views) == 2 else set()

    result = check_views(snapshot_of(*rows, cluster_nodes=views), ViewConsistencyValidationConfig(), killed)

    assert result.success is success
    assert [(item.discrepancy_type, item.subject_node) for item in result.view_discrepancies] == found


@pytest.mark.parametrize("allow_transient", [True, False])
def test_views_that_disagree_fail_the_check(allow_transient):
    rows = [node_row("a", 7000), node_row("b", 7001, shard_id=1), node_row("c", 7002, "replica")]
    views = {7000: _view("a"), 7001: _view("b", c_primary=True), 7002: _view("c")}
    config = ViewConsistencyValidationConfig(allow_transient_inconsistency=allow_transient)
    result = check_views(snapshot_of(*rows, cluster_nodes=views), config)
    assert (result.success, result.consistent_views) == (False, False), result.error_message


@pytest.mark.parametrize(("allow_transient", "attempts"), [(True, 3), (False, 1)])
def test_views_that_disagree_are_retried_only_with_allow_transient_inconsistency(
    monkeypatch, allow_transient, attempts
):
    clock = VirtualClock().install(monkeypatch, check_module, queries)
    runs = []

    def run(*args):
        runs.append(1)
        # The views disagree, and on the second attempt replication also fails, so no two attempts fail alike.
        failed = ["view_consistency"] + (["replication"] if len(runs) == 2 else [])
        return StateValidationResult(False, 0, failed_checks=failed)

    config = StateValidationConfig(retry_delay=0.1, max_retries=2)
    config.view_consistency_config.allow_transient_inconsistency = allow_transient
    check = DeploymentStateCheck(Mock(), [], {}, config, rng=random.Random(0))
    with (
        patch.object(check, "_validate_once", side_effect=run),
        patch(f"{CHECK}.syncing_replicas", return_value=[]),
    ):
        result = check._validate_with_retry(None)

    assert not result.overall_success and len(runs) == attempts and clock.elapsed < 1


@pytest.mark.parametrize("split", [False, True])
def test_a_view_a_gossip_round_behind_a_failover_is_retried_but_two_primaries_serving_a_slot_are_a_split_brain(split):
    # Node b has not yet heard that c replaced the stopped a: it still gives a (flagged failed) slots 0-8191.
    a_line = "a 127.0.0.1:7000@17000 master,fail - 0 0 1 disconnected"
    c_owns = "c 127.0.0.1:7002@17002 {}master - 0 0 3 connected 0-8191"
    views = {
        7001: f"{a_line} 0-8191\nb 127.0.0.1:7001@17001 myself,master - 0 0 2 connected 8192-16383\n"
        "c 127.0.0.1:7002@17002 slave a 0 0 1 connected",
        7002: f"{a_line}\nb 127.0.0.1:7001@17001 master - 0 0 2 connected 8192-16383\n{c_owns.format('myself,')}",
    }
    if split:  # b claims c's slots in its own view
        views[7001] = views[7001].replace("8192-16383", "0-16383")
    rows = [node_row("a", 7000, status="failed"), node_row("b", 7001, shard_id=1), node_row("c", 7002)]
    snapshot = snapshot_of(*rows, cluster_nodes=views)
    stopped = {"127.0.0.1:7000"}

    coverage = check_slot_coverage(snapshot, SlotCoverageValidationConfig(), stopped)
    views_result = check_views(snapshot, ViewConsistencyValidationConfig(), stopped)
    result = StateValidationResult(False, 0, slot_coverage=coverage, view_consistency=views_result)

    assert (views_result.success, views_result.split_brain_detected) == (False, split)
    assert coverage.success is not split and len(coverage.conflicting_slots) == (8192 if split else 0)
    assert result.is_critical_failure() is split


def test_parse_replication_reads_the_replication_id_failover_state_and_replica_entries():
    # INFO replication of a primary as valkey-py parses it: each slaveN line is a mapping, numbers are ints.
    primary = queries.parse_replication(
        {
            "role": "master",
            "connected_slaves": 2,
            "slave10": {"ip": "127.0.0.3", "port": 7003, "state": "online", "offset": 90, "lag": 1},
            "slave2": {"ip": "127.0.0.2", "port": 7002, "state": "wait_bgsave", "offset": 0, "lag": 0},
            "slave0": {"ip": "127.0.0.1", "port": 7001, "state": "online", "offset": 100, "lag": 0,
                       "type": "replica", "repl_compression": "lz4"},
            "master_failover_state": "no-failover",
            "master_replid": "8a1b2c3d4e5f60718293a4b5c6d7e8f901234567",
            "master_repl_offset": 100,
        }
    )  # fmt: skip

    assert (primary.replication_id, primary.failover_state) == (
        "8a1b2c3d4e5f60718293a4b5c6d7e8f901234567",
        "no-failover",
    )
    assert [(entry.ip, entry.port, entry.state, entry.offset, entry.lag) for entry in primary.replicas] == [
        ("127.0.0.1", 7001, "online", 100, 0),
        ("127.0.0.2", 7002, "wait_bgsave", 0, 0),
        ("127.0.0.3", 7003, "online", 90, 1),
    ], "by their slaveN index"
    assert (primary.replicas[0].type, primary.replicas[1].type) == ("replica", None)
    assert primary.replicas[0].fields["repl_compression"] == "lz4", "a field without a name of its own is kept"
    replica = queries.parse_replication({"role": "slave", "master_host": "127.0.0.1", "master_port": 7000})
    assert (replica.replication_id, replica.failover_state, replica.replicas) == (None, None, ())


def test_the_exported_split_slots_finds_both_kinds_of_split_and_ignores_a_failed_owner():
    # In x's view a (failed) and c both own 0-9 and c and d both own 20-29; y and z each claim 40-49 as their own.
    x = parse_cluster_nodes(
        "x 127.0.0.1:1@2 myself,slave c 0 0 3 connected\n"
        "a 127.0.0.1:3@4 master,fail - 0 0 1 disconnected 0-9\n"
        "c 127.0.0.1:5@6 master - 0 0 3 connected 0-9 20-29\n"
        "d 127.0.0.1:7@8 master - 0 0 4 connected 20-29 [30->-c]"
    )
    y = parse_cluster_nodes("y 127.0.0.1:9@10 myself,master - 0 0 5 connected 40-49")
    z = parse_cluster_nodes("z 127.0.0.1:11@12 myself,master - 0 0 6 connected 45")

    split = split_slots({"valkey-0": x, "valkey-1": y, "valkey-2": z})

    assert split == {**{slot: {"c", "d"} for slot in range(20, 30)}, 45: {"y", "z"}}
    assert split_slots({"valkey-0": x[:3]}) == {}, "a primary its view flags failed serves nothing"
    suspected = parse_cluster_nodes(
        "x 127.0.0.1:1@2 myself,slave c 0 0 3 connected\n"
        "a 127.0.0.1:3@4 master,fail? - 0 0 1 connected 0-9\n"
        "c 127.0.0.1:5@6 master - 0 0 3 connected 5"
    )
    assert split_slots({"valkey-0": suspected}) == {5: {"a", "c"}}, "a suspected primary still serves"


@pytest.mark.parametrize(
    ("master_port", "status", "primary", "offset", "message"),
    [
        (7999, "connected", {"connected_slaves": "0"}, False, "attached to 127.0.0.1:7999"),
        (7000, "connected", {"connected_slaves": "1"}, True, "could not compare the replication offsets"),
        (7000, "connected", {"connected_slaves": "0"}, False, "reports 0 connected replicas, expected 1"),
        (7000, "failed", None, False, "Recorded primary 127.0.0.1:7000 is not connected"),
        (7000, "connected", None, False, "Unable to query replication INFO from primary"),
    ],
    ids=["wrong-primary", "no-offset", "replica-count", "primary-gone", "primary-silent"],
)
def test_replication_check_rejects_broken_standalone_replication(master_port, status, primary, offset, message):
    replica = {"role": "slave", "master_link_status": "up", "master_host": "127.0.0.1", "master_port": master_port}
    snapshot = snapshot_of(
        node_row("valkey-0", 7000, status=status),
        node_row("valkey-1", 7001, "replica"),
        mode=DeploymentMode.STANDALONE,
        instances=[instance(), instance("valkey-1", "replica", 7001, primary_id="valkey-0")],
        replication={7001: replica, **({7000: {"role": "master", **primary}} if primary else {})},
    )

    result = check_replication(snapshot, ReplicationValidationConfig(check_replication_offset=offset))

    assert result.success is False and message in result.error_message


@pytest.mark.parametrize(
    ("primary_reply", "inconclusive", "message"),
    [
        # A gossip round behind a failover: the primary the replica follows is no primary in the topology read.
        (None, True, "could not compare the replication offsets of 127.0.0.1:7001 and its primary unknown"),
        # The primary did not answer, or was demoted between the topology read and its INFO.
        ({}, True, "could not compare the replication offsets of 127.0.0.1:7001"),
        ({"role": "slave", "master_repl_offset": 90}, True, "could not compare the replication offsets"),
        # Read after its replica, the primary is behind it: Valkey lost what the replica had applied.
        ({"role": "master", "master_repl_offset": 90}, False, "127.0.0.1:7001 is ahead of its primary"),
    ],
    ids=["unknown-primary", "silent-primary", "demoted-primary", "ahead"],
)
def test_offsets_that_could_not_be_compared_are_inconclusive_but_a_replica_ahead_of_its_primary_fails(
    primary_reply, inconclusive, message
):
    known = primary_reply is not None
    rows = [node_row("p", 7000), node_row("r", 7001, "replica")] + ([node_row("q", 7002, shard_id=1)] if known else [])
    replica = {"role": "slave", "master_link_status": "up", "slave_repl_offset": 100}
    replies = {7000: {"role": "master", "master_repl_offset": 100}, 7001: replica}
    snapshot = snapshot_of(
        *rows,
        replication={**replies, **({7002: primary_reply} if primary_reply else {})},
        primaries={7001: "q"},
    )

    result = check_replication(snapshot, ReplicationValidationConfig())

    assert (result.success, result.inconclusive) == (False, inconclusive) and message in result.error_message


@pytest.mark.parametrize(
    ("rows", "live_primary", "link", "lag", "connected", "success"),
    [
        # A replica whose primary was killed may have its link down; once a new primary is live it may not.
        ([node_row("p", 7000, status="failed")], "p", "down", None, "0", True),
        ([node_row("p", 7000, status="failed"), node_row("q", 7002)], "q", "down", None, "0", False),
        # IO lag alone does not fail when the offsets match, and a cluster primary's replica count is advisory.
        ([node_row("p", 7000)], "p", "up", "10", "0", True),
    ],
)
def test_replication_check_judges_cluster_replica_links_and_lag(rows, live_primary, link, lag, connected, success):
    replica = {"role": "slave", "master_link_status": link, "master_last_io_seconds_ago": lag or "-1"}
    primary = {"role": "master", "connected_slaves": connected, "master_repl_offset": 100}
    replies = {row.port: primary for row in rows if not row.failed}
    snapshot = snapshot_of(
        *rows,
        node_row("r", 7001, "replica"),
        replication={**replies, 7001: {**replica, "slave_repl_offset": 100}},
        primaries={7001: live_primary},
    )

    result = check_replication(
        snapshot,
        ReplicationValidationConfig(
            max_acceptable_lag=5.0, check_replication_offset=lag is not None, require_all_replicas_synced=False
        ),
        {"127.0.0.1:7000"} if rows[0].failed else set(),
    )

    assert result.success is success, result.error_message
    assert success or "replication link is down while their primary is live" in result.error_message


def test_a_replica_whose_link_is_down_fails_the_replication_check_whatever_else_was_stopped():
    # Its shard keeps another replica, and nothing was stopped: still a failure.
    replica = {"role": "slave", "master_link_status": "down", "slave_repl_offset": 100}
    snapshot = snapshot_of(
        node_row("p", 7000),
        node_row("r1", 7001, "replica"),
        node_row("r2", 7002, "replica"),
        replication={7000: {"role": "master", "connected_slaves": 1, "master_repl_offset": 100},
                     7001: replica, 7002: {**replica, "master_link_status": "up"}},
        primaries={7001: "p", 7002: "p"},
    )  # fmt: skip

    result = check_replication(snapshot, ReplicationValidationConfig())

    assert result.success is False and "['127.0.0.1:7001']" in result.error_message
    assert result.redundancy_lost is False


def test_the_role_check_alone_judges_nodes_that_failed_or_live_on_after_a_stop():
    rows = [node_row("a", 7000), node_row("b", 7001, "replica", status="failed"), node_row("c", 7002, "replica")]
    snapshot = snapshot_of(*rows)
    expected = ExpectedRoles(num_primaries=1, num_replicas=2)

    # b failed though nobody stopped it, and c was stopped but is still live.
    result = check_roles(snapshot, expected, {"127.0.0.1:7002"}, {"127.0.0.1:7002": "replica"})
    status = check_cluster_status(
        snapshot_of(*rows, cluster_info={7000: _OK, 7002: _OK}), ClusterStatusValidationConfig()
    )

    assert {m.mismatch_type for m in result.role_mismatches} >= {"unexpected_failure", "stopped_alive"}
    assert "still live: ['127.0.0.1:7002']" in result.error_message and "not stopped on purpose" in result.error_message
    assert status.success and status.nodes_in_fail_state == ["127.0.0.1:7001"]


def test_a_critical_failure_is_retried_while_the_cluster_converges(monkeypatch):
    # Right after a kill the cluster has one primary too few until Valkey promotes a replica, which on a loaded
    # host takes longer than the stabilization wait; the same critical failure twice is still convergence.
    clock = VirtualClock().install(monkeypatch, check_module, queries)
    primaries = RoleValidation(False, role_mismatches=[RoleMismatch("primary_count", "cluster", "3", "2")])
    results = [
        StateValidationResult(False, 0, roles=primaries, failed_checks=["roles"]),
        StateValidationResult(False, 0, roles=primaries, failed_checks=["roles"]),
        StateValidationResult(True, 0),
    ]
    assert results[0].is_critical_failure()
    check = DeploymentStateCheck(Mock(), [], {}, StateValidationConfig(retry_delay=0.1), rng=random.Random(0))
    with (
        patch.object(check, "_validate_once", side_effect=results) as attempts,
        patch(f"{CHECK}.syncing_replicas", return_value=[]),
    ):
        assert check._validate_with_retry(None).overall_success
    assert attempts.call_count == 3 and clock.elapsed < 1


def test_a_sync_seen_after_the_sync_deadline_is_not_waited_for(monkeypatch):
    # A replica flapping in and out of full syncs must not restart validation until the run's deadline.
    VirtualClock().install(monkeypatch, check_module, queries)
    check = DeploymentStateCheck(Mock(), [], {}, StateValidationConfig(), rng=random.Random(0))
    with patch(f"{CHECK}.syncing_replicas", side_effect=[["127.0.0.1:7001"], []]):
        assert check._wait_for_syncs(check_module.time.monotonic() - 1.0) is False


@pytest.mark.parametrize(
    ("sync_s", "primary_answers", "success"), [(5.0, True, True), (2 * SYNC_WAIT_S, True, False), (5.0, False, False)]
)
def test_validation_waits_for_a_replica_in_a_sync_but_judges_one_that_never_ends_or_whose_primary_is_gone(
    monkeypatch, sync_s, primary_answers, success
):
    # A replica that migrated to an orphaned primary has its link down and the old shard's keys until its sync ends.
    clock = VirtualClock().install(monkeypatch, check_module, queries)
    ends, attempts = clock.monotonic() + sync_s, []

    def run(*args):
        attempts.append(1)
        ok = clock.monotonic() >= ends
        return StateValidationResult(ok, 0, failed_checks=[] if ok else ["replication", "data_consistency"])

    check = DeploymentStateCheck(Mock(), [], {}, StateValidationConfig(retry_delay=0.1), rng=random.Random(0))
    syncing = lambda _: ["127.0.0.1:11961"] if primary_answers and clock.monotonic() < ends else []  # noqa: E731
    with (
        patch.object(check, "_validate_once", side_effect=run),
        patch(f"{CHECK}.syncing_replicas", side_effect=syncing),
    ):
        result = check._validate_with_retry(None)

    assert result.overall_success is success and (not success or len(attempts) == 2)
    # It waited for the sync to end, up to SYNC_WAIT_S; with the primary gone, not at all.
    waited = min(sync_s, SYNC_WAIT_S) if primary_answers else 0
    assert waited <= clock.elapsed < waited + 1, clock.elapsed


def test_a_replica_is_in_a_sync_only_while_its_link_is_down_and_its_primary_answers():
    def info(port, link, role="slave"):
        return fake_client({"role": role, "master_host": "127.0.0.1", "master_port": port, "master_link_status": link})

    answers = {
        11961: info(11967, "down"),
        11962: info(11967, "up"),
        11963: info(11999, "down"),
        11967: info(0, "", "master"),
    }
    connection = SimpleNamespace(instances=[SimpleNamespace(host="127.0.0.1", port=port) for port in answers])
    with patch(f"{CHECK}.valkey_client", side_effect=_by_port(answers)):
        assert syncing_replicas(connection) == ["127.0.0.1:11961"]


_KEYS = {f"fuzzer:test:{index}": f"value-{index}" for index in range(100)}


def _cross_replica(mode, connect, slots=None, connection=None):
    """``_check_replicas`` over ``connection``'s topology, or a primary owning ``slots`` (default: every key's)."""
    found, unreachable = [], []
    if connection is None:
        slots = [key_slot(key.encode()) for key in _KEYS] if slots is None else slots
        primary = node_row("p", 7000, slots=tuple(slots))
        snapshot = snapshot_of(primary, node_row("r", 7001, "replica", primary_id="p"), mode=mode)
    else:
        snapshot = Snapshot(mode, connection.instances, connection.get_current_nodes(include_failed=True))
    with patch(f"{LOCAL}validation.data.valkey_client", side_effect=connect):
        _check_replicas(snapshot, _KEYS, DataConsistencyValidationConfig(), found, unreachable)
    return sorted((item.key, item.inconsistency_type) for item in found), unreachable


def _owner(key):
    slot = key_slot(key.encode())
    return {0: 7004, 1: 7003, 2: 7006, 3: 7011}[next(s for s, (lo, hi) in MIGRATED_SLOTS.items() if lo <= slot <= hi)]


@pytest.mark.parametrize("broken", [False, True])
def test_a_migrated_and_promoted_replica_is_checked_against_its_live_shard(broken):
    replica_of = {7002: 7004, 7005: 7003, 7007: 7006, 7008: 7006, 7010: 7011}
    lost = next(key for key in sorted(_KEYS) if _owner(key) == 7004)
    diverged = next(key for key in sorted(_KEYS) if _owner(key) == 7011)
    wrong = {(7002, lost): None, (7010, diverged): "stale"} if broken else {}

    def connect(host, port, timeout=None):
        def read(key):  # a replica answers MOVED for a key outside its primary's slots, as Valkey does
            if _owner(key) != replica_of[port]:
                return ResponseError("MOVED")
            return wrong.get((port, key), _KEYS[key])

        return client_context(fake_client(read=read))

    with patch(f"{LOCAL}topology.query_cluster_nodes", return_value=parse_cluster_nodes(MIGRATED_TOPOLOGY)):
        found, unreachable = _cross_replica(DeploymentMode.CLUSTER, connect, connection=migrated_connection())

    assert found == (sorted([(lost, "missing_on_replica"), (diverged, "replica_divergence")]) if broken else [])
    assert unreachable == []


def test_cluster_test_keys_cover_every_primary_and_each_replica_needs_one():
    slot_map = {key_slot(b"fuzzer:test:0"): "127.0.0.1:7000", key_slot(b"fuzzer:test:1"): "127.0.0.1:7002"}
    primaries = deployment_view(
        node_row("p0", 7000, slots=(key_slot(b"fuzzer:test:0"),)),
        node_row("p1", 7002, shard_id=1, slots=(key_slot(b"fuzzer:test:1"),)),
    )
    # Too few keys for the primaries, or a write that fails, leaves the seeding incomplete.
    acks = Mock(**{"set.side_effect": [True, True, True, RuntimeError("SET failed")]})
    with patch(f"{LOCAL}validation.data.valkey.ValkeyCluster", return_value=acks):
        keys = write_test_keys(primaries, DataConsistencyValidationConfig(num_test_keys=2), random.Random(7))
        for count, error, message in ((1, ValueError, "insufficient"), (2, RuntimeError, "wrote 1 of 2")):
            with pytest.raises(error, match=message):
                write_test_keys(primaries, DataConsistencyValidationConfig(num_test_keys=count), random.Random(7))
    assert {slot_map[key_slot(key.encode())] for key in keys} == {"127.0.0.1:7000", "127.0.0.1:7002"}

    with pytest.raises(RuntimeError, match="cross-replica coverage is incomplete"):
        _cross_replica(DeploymentMode.CLUSTER, _by_port({}), [key_slot(b"unseeded")])


@pytest.mark.parametrize(
    ("line", "severity"),
    [
        # Under load a replica's election can expire while another replica wins; the state checks judge the outcome.
        ("Currently unable to failover: Failover attempt expired.", "warning"),
        ("Failover election in progress for epoch 0", "error"),
    ],
)
def test_a_lost_election_is_a_warning_but_an_election_for_epoch_0_is_an_error(tmp_path, line, severity):
    def node(name, role, text):
        (tmp_path / name).write_text(f"1:S 29 Oct 2026 00:00:00.000 * {text}\n")
        return LogNode(name, name, None, role, 0, str(tmp_path / name))

    nodes = [node("v1", "primary", "Failover election won (epoch 7)."), node("v2", "replica", line)]
    findings = check_logs(nodes, {0}, (), ()).findings

    assert [finding.severity for finding in findings] == [severity]


@pytest.mark.parametrize(
    ("lines", "severity"),
    [
        # The detection scrolled out of the last 500 lines of a long run: the cluster still noticed.
        (["Marking node n1 (127.0.0.1) as failing (quorum reached)."] + ["Background saving started"] * 600, None),
        # Noticed at an earlier stop, cleared when it came back, and never noticed after this stop.
        (
            ["Marking node n1 as failing (quorum reached).", "Clear FAIL state for node n1: is reachable again."],
            "error",
        ),
    ],
    ids=["long-run", "earlier-stop"],
)
def test_failure_detection_is_the_watchers_last_word_about_a_stopped_node(tmp_path, lines, severity):
    (tmp_path / "v0").write_text("".join(f"1:M 29 Oct 2026 00:00:00.000 * {line}\n" for line in lines))
    (tmp_path / "v1").write_text("")
    nodes = [LogNode("v0", "a:0", "n0", "primary", 0, str(tmp_path / "v0")),
             LogNode("v1", "a:1", "n1", "replica", 0, str(tmp_path / "v1"))]  # fmt: skip

    findings = check_logs(nodes, set(), {"a:1"}, ()).findings

    assert [(finding.severity, finding.shard_id) for finding in findings] == ([(severity, None)] if severity else [])


def test_failure_detection_counts_every_watcher_including_stopped_ones_and_the_fail_message(tmp_path):
    # n2 reached quorum on n1, then was killed itself and detected by n0; n0 only heard of n1 by FAIL message
    # (cluster_legacy.c logs "Marking node ... as failing" on the node that reached quorum alone).
    def write(name, *lines):
        (tmp_path / name).write_text("".join(f"1:M 29 Oct 2026 00:00:00.000 * {line}\n" for line in lines))

    n2_failing = "Marking node n2 (127.0.0.1) as failing (quorum reached)."
    write("v0", "FAIL message received from n2 (127.0.0.1) about n1 (127.0.0.1)", n2_failing)
    write("v1")
    write("v2", "Marking node n1 (127.0.0.1) as failing (quorum reached).")
    nodes = [
        LogNode(f"v{index}", f"a:{index}", f"n{index}", "primary", index, str(tmp_path / f"v{index}"))
        for index in range(3)
    ]
    for stopped in ({"a:1", "a:2"}, {"a:1"}):
        result = check_logs(nodes, set(), stopped, ())
        assert result.success and not result.findings, result.findings
    # n2's own quorum line still counts after n2 was stopped: its log outlives it.
    write("v0", n2_failing)
    assert check_logs(nodes, set(), {"a:1", "a:2"}, ()).success


def test_the_log_check_counts_only_the_shards_it_examined(tmp_path):
    (tmp_path / "v0").write_text("1:M 29 Oct 2026 00:00:00.000 * Failover election won (epoch 7).\n")
    (tmp_path / "v1").write_text("")
    nodes = [LogNode("v0", "a:0", "n0", "primary", 0, str(tmp_path / "v0")),
             LogNode("v1", "a:1", "n1", "replica", 1, str(tmp_path / "v1"))]  # fmt: skip

    # Shard 1 only lost a replica, and shard 5 has no live member: only shard 0's logs are read.
    result = check_logs(nodes, {0, 5}, {"a:1"}, ())
    assert result.shards_checked == 1


def test_the_log_check_groups_nodes_by_their_live_shard(tmp_path):
    # valkey-1 was planned in shard 0 but migrated to shard 1's primary: a failover of shard 1 reads its log.
    def info(index, role, planned_shard, line):
        log_file = tmp_path / f"valkey-{index}.log"
        log_file.write_text(f"1:S 29 Oct 2026 00:00:00.000 * {line}\n")
        fields = {"shard_id": planned_shard, "cluster_node_id": f"n{index}", "log_file": str(log_file)}
        info = instance(f"valkey-{index}", role, index + 1, **fields)
        # No server runs, so none counts as one that did not answer: only the logs are judged.
        info.process.poll.return_value = 0
        return info

    infos = [
        info(0, "primary", 0, "Failover election won (epoch 3)."),
        info(1, "replica", 0, "Failover election in progress for epoch 0"),
        info(2, "primary", 1, "Failover election won (epoch 4)."),
    ]
    rows = [
        node_row("n0", 1, instance_id="valkey-0"),
        node_row("n1", 2, "replica", 1, primary_id="n2", instance_id="valkey-1"),
        node_row("n2", 3, shard_id=1, instance_id="valkey-2"),
    ]
    connection = Mock(spec=DeploymentConnection, mode=DeploymentMode.CLUSTER, instances=infos)
    connection.get_current_nodes.return_value = rows
    config = _only("logs", stabilization_wait=0, retry_on_transient_failure=False)
    check = DeploymentStateCheck(connection, infos, {}, config, rng=random.Random(0))

    findings = check.validate([{"shard": 1}])["checks"]["logs"]["findings"]

    assert [(item["instance_id"], item["shard"], item["severity"]) for item in findings] == [("valkey-1", 1, "error")]


def test_a_stopped_primary_has_the_log_check_read_the_shard_it_served_when_stopped():
    # valkey-1 was planned in shard 0, migrated to shard 1, and was its primary when it was killed.
    infos = [instance("valkey-0", "primary", 1), instance("valkey-1", "replica", 2)]
    for info in infos:
        info.process.poll.return_value = 0  # nothing runs: only the logs are judged
    connection = Mock(spec=DeploymentConnection, mode=DeploymentMode.CLUSTER, instances=infos)
    connection.get_current_nodes.return_value = [node_row("n0", 1, instance_id="valkey-0")]
    config = _only("logs", stabilization_wait=0, retry_on_transient_failure=False)
    check = DeploymentStateCheck(connection, infos, {}, config, rng=random.Random(0))
    check.mark_stopped("valkey-1", "primary", shard=1)
    check.mark_stopped("valkey-0", "replica", shard=0)
    with patch(f"{CHECK}.check_logs", return_value=LogValidationResult(True, [], 0)) as logs:
        check.validate([])
    assert logs.call_args.args[3] == {1}


def test_a_snapshot_records_a_node_that_did_not_answer_but_lets_a_harness_bug_through():
    rows = [node_row("a", 7000, instance_id="valkey-0")]
    connection = Mock(mode=DeploymentMode.CLUSTER, instances=[instance("valkey-0")])
    connection.get_current_nodes.return_value = rows
    with patch(f"{LOCAL}validation.snapshot.valkey_client", side_effect=ConnectionError("refused")):
        assert "ConnectionError" in snapshot.take_snapshot(connection, 1.0).unanswered["127.0.0.1:7000"]
    # A reply the harness cannot parse is its own bug, not a node that did not answer.
    with (
        patch(f"{LOCAL}validation.snapshot.valkey_client", return_value=client_context(fake_client())),
        patch(f"{LOCAL}validation.snapshot.query_cluster_info", side_effect=KeyError("parser bug")),
        pytest.raises(KeyError, match="parser bug"),
    ):
        snapshot.take_snapshot(connection, 1.0)


def test_validation_stops_at_its_timeout_during_the_stabilization_wait_and_between_checks(monkeypatch):
    clock = VirtualClock().install(monkeypatch, check_module, queries)
    result = _validate_once(_only(stabilization_wait=30.0, validation_timeout=0.5), Mock(spec=DeploymentConnection))
    # The budget is gone before the first check: nothing was judged.
    assert clock.elapsed == pytest.approx(0.5)
    assert (result.overall_success, result.inconclusive, result.failed_checks) == (False, True, ["validation_timeout"])

    # The budget is enforced between checks: a slow read is not reported as the next check's failure, and the
    # validation stops with validation_timeout.
    config = _only("replication", "cluster_status", stabilization_wait=0, validation_timeout=0.3)
    config.replication_config.min_replicas_per_shard = 0

    def slow_topology(include_failed=True, timeout=2.0):
        clock.sleep(0.6)
        return [node_row("a", 7000)]

    connection = Mock(spec=DeploymentConnection, mode=DeploymentMode.CLUSTER, instances=[])
    connection.get_current_nodes.side_effect = slow_topology
    result = _validate_once(config, connection)
    assert result.failed_checks == ["validation_timeout"], result.error_messages


# s100121: valkey-1 migrated to the orphaned primary on port 12923 0.7 s before FAILOVER FORCE, at offset 0.
_SYNCING = dict(
    role="slave", master_host="127.0.0.1", master_port=12923, master_link_status="down", slave_repl_offset=0
)
_SYNCED = {**_SYNCING, "master_link_status": "up", "slave_repl_offset": 1359049}


@pytest.mark.parametrize(
    ("infos", "primary_answers", "promoted"),
    [
        ({12921: [_SYNCING]}, True, None),  # Valkey would let it win and wipe the shard
        ({12921: [_SYNCING, _SYNCING, _SYNCED]}, True, 12921),  # waits for the sync to finish
        ({12921: [_SYNCING], 12925: [_SYNCED]}, True, 12925),
        ({12921: [_SYNCING]}, False, None),  # stops waiting once the primary is gone
        # Replicating another primary, a primary itself, mid-sync, or at offset 0 is not synced.
        ({12921: [{**_SYNCED, "master_port": 12920}], 12925: [{**_SYNCED, "role": "master"}]}, True, None),
        (
            {12921: [{**_SYNCED, "master_sync_in_progress": 1}], 12925: [{**_SYNCED, "slave_repl_offset": 0}]},
            True,
            None,
        ),
    ],
)
def test_a_forced_failover_goes_only_to_a_replica_synced_with_the_shards_primary(
    monkeypatch, infos, primary_answers, promoted
):
    clock = VirtualClock().install(monkeypatch, failover, queries)
    nodes = [
        node_row("p", 12923, shard_id=1),
        *(node_row(f"r{port}", port, "replica", 1, primary_id="p") for port in infos),
    ]
    connection = SimpleNamespace(get_current_nodes=lambda include_failed=True: nodes, instances=[])
    sent = []

    def connect(host, port, **kwargs):
        replies = infos.get(port, [{}])
        client = fake_client(replies=lambda *args: sent.append(port))
        client.info.side_effect = lambda section: replies.pop(0) if len(replies) > 1 else replies[0]
        return client_context(client)

    for seed in range(3):
        sent.clear()
        started = clock.monotonic()
        with (
            patch(f"{LOCAL}failover.valkey_client", side_effect=connect),
            patch(f"{LOCAL}failover._responds_to_ping", return_value=primary_answers),
            patch(f"{LOCAL}failover.wait_until_settled", return_value=True) as settled,
        ):
            if promoted is None and primary_answers:
                # No replica synced within the harness's wait: no failover was sent, so Valkey is not judged.
                with pytest.raises(LocalDeploymentError, match="synced within 5s"):
                    failover_shard(connection, 1, force=True, timeout=5.0, rng=random.Random(seed))
            else:
                # A primary gone before a replica synced is left to Valkey's own failover, which must settle.
                assert failover_shard(connection, 1, force=True, timeout=5.0, rng=random.Random(seed)) is True
                assert primary_answers or settled.call_args.kwargs["old_primary"] == "p"
        assert sent == ([promoted] if promoted else []) and (primary_answers or clock.monotonic() - started < 1.0)


def test_failovers_and_validations_end_their_waits_at_the_run_deadline_or_a_cancellation(monkeypatch):
    # A wait cut short never saw what it waited for: it raises (the runner records ERROR) rather than report that
    # Valkey did not settle, and it returns at once, so the deployment's cleanup is not stuck behind it.
    clock = VirtualClock().install(monkeypatch, failover, queries, check_module)
    deadline = SimpleNamespace(deadline=clock.monotonic() + 2.0, sleep=lambda seconds: clock.sleep(seconds) or True)
    cancelled = SimpleNamespace(deadline=None, sleep=lambda seconds: False)
    rows = [node_row("p", 7000), node_row("r", 7001, "replica", primary_id="p")]
    connection = Mock(instances=[], **{"get_current_nodes.return_value": rows})
    with (
        patch(f"{LOCAL}failover.replica_sync_problem", return_value="link down"),
        patch(f"{LOCAL}failover._responds_to_ping", return_value=True),
    ):
        with pytest.raises(WaitInterruptedError, match="deadline"):
            failover_shard(connection, 0, timeout=30.0, rng=random.Random(0), context=deadline)
        with pytest.raises(WaitInterruptedError, match="cancelled"):
            failover_shard(connection, 0, timeout=30.0, rng=random.Random(0), context=cancelled)
    assert clock.elapsed < 2.0
    with patch(f"{LOCAL}failover._shard_settled", return_value=False), pytest.raises(WaitInterruptedError):
        wait_until_settled(connection, 30.0, shard_id=0, old_primary="p", context=cancelled)

    check = DeploymentStateCheck(
        Mock(spec=DeploymentConnection), [], {}, _only(stabilization_wait=5.0), rng=random.Random(0), context=cancelled
    )
    with pytest.raises(WaitInterruptedError, match="cancelled"):
        check.validate()
    assert clock.elapsed < 2.0


def test_a_failover_that_cannot_be_observed_raises_instead_of_reporting_that_valkey_did_not_fail_over(monkeypatch):
    VirtualClock().install(monkeypatch, failover, queries)
    primary, replica = node_row("p", 7000), node_row("r", 7001, "replica", primary_id="p")

    def connection(*rows, live=None):
        nodes = Mock(instances=[])
        nodes.get_current_nodes.side_effect = lambda include_failed=True: list(rows if include_failed else live or rows)
        return nodes

    # No node answered, or the shard has nothing to promote: the failover was never tried.
    with pytest.raises(LocalDeploymentError, match="no cluster node answered"):
        failover_shard(connection(), 0, timeout=30.0, rng=random.Random(0))
    with pytest.raises(LocalDeploymentError, match="no replica to promote"):
        failover_shard(connection(primary), 0, timeout=30.0, rng=random.Random(0))
    # The replica dropped the connection of CLUSTER FAILOVER.
    refused = Mock(**{"execute_command.side_effect": ConnectionError("reset")})
    with (
        patch(f"{LOCAL}failover.replica_sync_problem", return_value=None),
        patch(f"{LOCAL}failover.valkey_client", return_value=client_context(refused)),
        pytest.raises(ConnectionError),
    ):
        failover_shard(connection(primary, replica), 0, timeout=30.0, rng=random.Random(0))
    # Whether the shard settled was unread at the end of the wait.
    with (
        patch(f"{LOCAL}failover.valkey_client", side_effect=ConnectionError("refused")),
        pytest.raises(LocalDeploymentError, match="could not be read"),
    ):
        promoted = [
            replace(replica, role="primary", primary_id=None),
            replace(primary, role="replica", primary_id="r"),
        ]
        wait_until_settled(connection(primary, replica, live=promoted), 1.0, shard_id=0, old_primary="p")


def test_a_primary_that_failed_unstopped_is_left_to_valkeys_own_failover():
    rows = [node_row("p", 7000, status="failed"), node_row("r", 7001, "replica", primary_id="p")]
    connection = Mock(instances=[], **{"get_current_nodes.return_value": rows})
    with patch(f"{LOCAL}failover.wait_until_settled", return_value=True) as settled:
        assert failover_shard(connection, 0, timeout=30.0, rng=random.Random(0)) is True
    assert settled.call_args.kwargs["old_primary"] == "p"
    # With no live replica of it, no failover can be tested: the harness reports it, and liveness judges the deaths.
    connection.get_current_nodes.return_value = [rows[0], replace(rows[1], failed=True, marked_fail=True)]
    with pytest.raises(LocalDeploymentError, match="no replica of it is live"):
        failover_shard(connection, 0, timeout=30.0, rng=random.Random(0))


def test_a_primary_only_one_node_suspects_still_gets_the_failover():
    # The viewer flags the primary fail? (PFAIL): no quorum of primaries agreed, so Valkey fails nothing over by
    # itself, and the replica has to be sent CLUSTER FAILOVER.
    topology = parse_cluster_nodes(
        "p 127.0.0.1:7000@17000 master,fail? - 0 0 1 disconnected 0-16383\n"
        "r 127.0.0.1:7001@17001 myself,slave p 0 0 1 connected"
    )
    connection = DeploymentConnection(
        [instance("valkey-0", port=7000, slot_start=0, slot_end=16383), instance("valkey-1", "replica", port=7001)],
        "d",
    )
    replica = Mock()
    with (
        patch(f"{LOCAL}topology.query_cluster_nodes", return_value=topology),
        patch(f"{LOCAL}failover.replica_sync_problem", return_value=None),
        patch(f"{LOCAL}failover.valkey_client", return_value=client_context(replica)),
        patch(f"{LOCAL}failover.wait_until_settled", return_value=True) as settled,
    ):
        assert failover_shard(connection, 0, timeout=30.0, rng=random.Random(0)) is True
    replica.execute_command.assert_called_once_with("CLUSTER", "FAILOVER")
    assert settled.call_args.kwargs["old_primary"] == "p"


def test_valkey_gets_the_whole_timeout_however_long_the_harness_waited_for_a_synced_replica(monkeypatch):
    clock = VirtualClock().install(monkeypatch, failover, queries)
    rows = [node_row("p", 7000), node_row("r", 7001, "replica", primary_id="p")]
    connection = Mock(instances=[], **{"get_current_nodes.return_value": rows})
    # The replica syncs 9.5 s into the 10 s wait, e.g. after a restart in the same wave.
    with (
        patch(
            f"{LOCAL}failover.replica_sync_problem",
            side_effect=lambda *nodes: None if clock.elapsed >= 9.5 else "sync in progress",
        ),
        patch(f"{LOCAL}failover._responds_to_ping", return_value=True),
        patch(f"{LOCAL}failover.valkey_client", return_value=client_context(Mock())),
        patch(f"{LOCAL}failover.wait_until_settled", return_value=True) as settled,
    ):
        assert failover_shard(connection, 0, timeout=10.0, rng=random.Random(0)) is True
    assert settled.call_args.args[1] == 10.0


def test_a_failover_tries_only_the_replicas_that_follow_the_shards_primary_in_one_topology_read():
    rows = [
        node_row("id-0", 7000),
        node_row("id-1", 7001, shard_id=1),
        node_row("id-3", 7003, "replica", shard_id=1, primary_id="id-1"),
        node_row("id-4", 7004, "replica", shard_id=0, primary_id="id-0"),
    ]
    connection = Mock(instances=[], **{"get_current_nodes.return_value": rows})
    with (
        patch(f"{LOCAL}failover.replica_sync_problem", return_value="unreachable") as sync,
        pytest.raises(LocalDeploymentError, match="synced within"),
    ):
        failover_shard(connection, 1, timeout=0.0, rng=random.Random(0))

    assert [call.args[0].port for call in sync.call_args_list] == [7003]
    connection.get_current_nodes.assert_called_once_with(include_failed=True)


@pytest.mark.parametrize(
    ("stopped", "replica_status", "expected"),
    [
        (("valkey-0", "valkey-1"), "failed", None),
        # A killed replica the cluster has not flagged FAIL yet takes no shard over either.
        (("valkey-0", "valkey-1"), "pfail", None),
        # A primary that died by itself with no live replica leaves no failover to test.
        (("valkey-1",), "failed", LocalDeploymentError),
    ],
)
def test_a_stopped_shard_has_nothing_to_fail_over_only_when_every_member_was_stopped(stopped, replica_status, expected):
    rows = [
        node_row("id-0", 7000, status="failed"),
        node_row("id-1", 7001, "replica", status=replica_status, primary_id="id-0"),
        node_row("id-2", 7002, shard_id=1),
    ]
    connection = Mock(instances=[SimpleNamespace(instance_id=f"valkey-{i}", port=7000 + i) for i in range(3)])
    connection.get_current_nodes.side_effect = lambda include_failed=True: rows if include_failed else rows[2:]

    if expected is LocalDeploymentError:
        with pytest.raises(LocalDeploymentError, match="no replica of it is live"):
            failover_shard(connection, 0, timeout=30.0, stopped_instances=stopped, rng=random.Random(0))
        return
    assert failover_shard(connection, 0, timeout=30.0, stopped_instances=stopped, rng=random.Random(0)) is expected


def test_shards_and_failovers_follow_the_live_topology_not_the_plan():
    topology = parse_cluster_nodes(MIGRATED_TOPOLOGY)
    with patch(f"{LOCAL}topology.query_cluster_nodes", return_value=topology):
        rows = migrated_connection().get_current_nodes(include_failed=True)
    # Only slot owners vote in Valkey's quorum; slotless dead primaries and their replicas hold no shard's data.
    assert [row.port for row in rows if row.slots] == [7003, 7004, 7006, 7011]
    assert [row.shard_id for row in rows] == [None, None, 0, 1, 0, 1, 2, 2, 2, None, 3, 3]
    # Each row names its instance, by cluster node ID once formation recorded it, else by address.
    assert [row.instance_id for row in rows] == [f"valkey-{index}" for index in range(12)]
    connection = migrated_connection()
    connection.instances[4].cluster_node_id = "n2"
    with patch(f"{LOCAL}topology.query_cluster_nodes", return_value=topology):
        assert connection.get_current_nodes()[2].instance_id == "valkey-4"

    sent = []
    connection = migrated_connection()
    with (
        patch(f"{LOCAL}topology.query_cluster_nodes", return_value=topology),
        patch(f"{LOCAL}failover.replica_sync_problem", return_value=None),
        patch(
            f"{LOCAL}failover.valkey_client",
            side_effect=lambda host, port, **kw: sent.append(port) or client_context(Mock()),
        ),
        patch(f"{LOCAL}failover.wait_until_settled", return_value=True),
    ):
        assert all(failover_shard(connection, shard, timeout=30.0, rng=random.Random(1)) for shard in (0, 1, 3))
    # Shard 0's slots are on valkey-4 now, with valkey-2 its only replica; valkey-2 is no longer in shard 1.
    assert sent == [7002, 7005, 7010]


class FakeDeployment:
    """Servers on ports 7000.. in shards of a primary and a replica (three in a cluster, one standalone) that answer
    what the checks and failovers ask: INFO, CLUSTER INFO/NODES/FAILOVER, READONLY, GET, SET, and pipelines."""

    RANGES = ((0, 5460), (5461, 10922), (10923, 16383))

    def __init__(self, mode):
        self.cluster = mode == "cluster"
        self.primary = {shard: 7000 + 2 * shard for shard in range(3 if self.cluster else 1)}
        self.data, self.wrong, self.down, self.dead, self.epoch_seen_by = {}, {}, set(), set(), {}
        self.cluster_client, self.promotes, self.offset, self.refusing = True, True, {}, set()

    def instances(self, logs):
        def info(i):
            slots = dict(zip(("slot_start", "slot_end"), self.RANGES[i // 2])) if self.cluster and i % 2 == 0 else {}
            fields = {"shard_id": i // 2, "cluster_node_id": f"n{i}", "log_file": str(logs / f"valkey-{i}.log")}
            fields.update({"primary_id": f"valkey-{i - 1}"} if i % 2 else {})
            return instance(f"valkey-{i}", "replica" if i % 2 else "primary", 7000 + i, **fields, **slots)

        return [info(i) for i in range(2 * len(self.primary))]

    def info(self, port):
        primary = self.primary[(port - 7000) // 2]
        if port == primary:
            return {"role": "master", "connected_slaves": 1, "master_repl_offset": 100, "cluster_enabled": 1}
        link = "down" if port in self.down else "up"
        return {"role": "slave", "master_host": "127.0.0.1", "master_port": primary, "master_link_status": link}

    def nodes(self, me):
        lines = []
        for port in range(7000, 7000 + 2 * len(self.primary)):
            shard = (port - 7000) // 2
            primary = port == self.primary[shard]
            flags = ("myself," if port == me else "") + ("master" if primary else "slave")
            flags += ",fail - 0 0 {} disconnected" if port in self.dead else " - 0 0 {} connected"
            epoch = shard + 1 + (primary and self.epoch_seen_by.get(me) == shard)
            slots = "{}-{}".format(*self.RANGES[shard]) if primary else ""
            flags = flags.replace("-", "-" if primary else f"n{self.primary[shard] - 7000}", 1).format(epoch)
            lines.append(f"n{port - 7000} 127.0.0.1:{port}@{port + 10000} {flags} {slots}")
        return "\n".join(lines)

    def get(self, port, key):
        return self.wrong.get((port, key), self.data.get(key))

    def set(self, key, value):
        self.data[key] = value
        return True

    def server(self, host=None, port=None, **kwargs):
        if port in self.dead | self.refusing:
            raise ConnectionError(port)

        def command(*args):
            if args[:2] == ("CLUSTER", "FAILOVER") and self.promotes:
                self.primary[(port - 7000) // 2] = port
            replies = {
                ("CLUSTER", "INFO"): f"cluster_state:ok\r\ncluster_known_nodes:{2 * len(self.primary)}\r\n",
                ("CLUSTER", "NODES"): self.nodes(port),
            }
            return replies.get(args[:2], "OK")

        # A list of offsets is read one per INFO, the last one standing.
        offset = self.offset.get(port, 100)
        offset = (offset.pop(0) if len(offset) > 1 else offset[0]) if isinstance(offset, list) else offset
        info = {**self.info(port), "slave_repl_offset": offset}
        server = fake_client(info, command, lambda key: self.get(port, key))
        server.get.side_effect, server.set.side_effect = lambda key: self.get(port, key), self.set
        server.__enter__, server.__exit__ = Mock(return_value=server), Mock(return_value=False)
        return server

    def serving(self):
        def cluster_client(**kwargs):
            if not self.cluster_client:
                raise ConnectionError("no cluster client")
            primary_of = lambda key: self.primary[self.shard_of(key)]  # noqa: E731
            return Mock(set=self.set, get=lambda key: self.get(primary_of(key), key))

        stack = ExitStack()
        stack.enter_context(
            patch.multiple("valkey", Valkey=Mock(side_effect=self.server), ValkeyCluster=cluster_client)
        )
        preload = Mock()
        preload.pipeline.return_value.execute.return_value = []
        stack.enter_context(patch(f"{LOCAL}service.ValkeyCluster", Mock(return_value=preload)))
        stack.enter_context(patch(f"{CHECK}.SYNC_WAIT_S", 0))
        return stack

    def shard_of(self, key):
        slot = key_slot(key.encode())
        return next(shard for shard, (lo, hi) in enumerate(self.RANGES) if lo <= slot <= hi) if self.cluster else 0


@pytest.mark.parametrize(
    ("mode", "fault", "failed"),
    [
        ("cluster", None, []),
        ("standalone", None, []),
        # Keys have one read path: without a cluster client every key is unreachable.
        ("cluster", "no-cluster-client", ["data_consistency"]),
        ("cluster", "stopped", []),  # a replica stopped on purpose is excused, once the cluster logged its failure
        ("cluster", "undetected", ["log_validation"]),
        # The role check judges the crash; the replication check the shard it left without a replica.
        ("cluster", "crashed", ["replication", "roles"]),
        ("cluster", "lost", ["data_consistency"]),
        ("standalone", "lost", ["data_consistency"]),
        ("cluster", "stale", ["data_consistency"]),
        ("cluster", "diverged", ["data_consistency"]),
        ("cluster", "unreadable", ["data_consistency"]),
        # A running node that refuses connections was never seen: the attempt judges nothing.
        ("cluster", "refusing", ["unanswered"]),
        # A running node its peers mark failed (hung, say) was not seen either: liveness judges a hang.
        ("cluster", "hung", ["unanswered"]),
        ("cluster", "down", ["replication"]),
        ("cluster", "lagging", ["replication"]),
        # A replica read before its primary sent a 14-byte replication PING is behind only until it applies it.
        ("cluster", "ping-behind", []),
        # Standalone reads the primary first: a replica that applied a PING since is ahead, not out of sync.
        ("standalone", "ping-ahead", []),
        ("cluster", "epoch", ["view_consistency"]),
    ],
)
def test_every_check_runs_against_the_deployment_and_names_what_broke(tmp_path, monkeypatch, mode, fault, failed):
    # A lagging replica is re-read until the snapshot's query timeout, and a failover polls until it settles.
    VirtualClock().install(monkeypatch, check_module, snapshot, failover, queries)
    fake = FakeDeployment(mode)
    config = DeploymentConfig(mode=DeploymentMode(mode), primaries=len(fake.primary), replicas_per_primary=1)
    instances = fake.instances(tmp_path)
    service = ValkeyDeploymentService(Mock(), LocalDeployment("d", config, instances))
    with fake.serving():
        check = service.state_check({"stabilization_wait": 0, "max_retries": 0}, seed=1)
        keys = check.write_test_keys()
        key = sorted(key for key in keys if fake.shard_of(key) == 0)[-1]  # every key is read, not just the first
        fake.wrong = {
            "lost": {(7000, key): None, (7001, key): None},
            "stale": {(7000, key): "stale"},
            "diverged": {(7001, key): "stale"},
            "unreadable": {(7001, key): ResponseError("MOVED")},
        }.get(fault, {})
        fake.refusing |= {7001} if fault == "refusing" else set()
        fake.down |= {7003} if fault == "down" else set()
        fake.offset.update({7003: 50} if fault == "lagging" else {})
        fake.offset.update({7003: [86, 100]} if fault == "ping-behind" else {})
        fake.offset.update({7001: [114]} if fault == "ping-ahead" else {})
        fake.dead |= {7003} if fault in ("crashed", "stopped", "undetected", "hung") else set()
        if fault in ("crashed", "stopped", "undetected"):
            instances[3].process.poll.return_value = -9
        fake.epoch_seen_by.update({7000: 1} if fault == "epoch" else {})
        fake.cluster_client = fault != "no-cluster-client"
        if fault in ("stopped", "undetected"):
            check.mark_stopped("valkey-3", "replica")
        if fault == "stopped":
            (tmp_path / "valkey-0.log").write_text("1:M 29 Oct 2026 * Marking node n3 as failing (quorum reached).\n")
        summary = check.validate([])

        assert summary["failed_checks"] == failed, summary["error_messages"]
        assert summary["critical"] or fault != "crashed"
        # Keys that could not be read were not checked.
        assert summary["inconclusive"] is (fault in ("refusing", "hung", "no-cluster-client", "unreadable"))
        assert fault != "unreadable" or summary["checks"]["data_consistency"]["unreachable_keys"] == 1
        assert (
            fault in ("refusing", "hung") or summary["checks"]["data_consistency"]["test_keys_checked"] == len(keys) > 0
        )
        if failed:
            assert {row["instance_id"] for row in summary["topology"]} == {i.instance_id for i in instances}
            # A failover whose replica never takes over does not settle.
            fake.promotes = False
            assert fault != "epoch" or not service.failover(0, timeout=0.1, rng=random.Random(1))
        elif mode == "cluster" and fault is None:
            # A failover settles on the promoted replica, and the deployment validates after it.
            assert service.failover(1, timeout=5.0, rng=random.Random(1)) and fake.primary[1] == 7003
            after = {"stabilization_wait": 0, "check_data_consistency": False, "check_logs": False}
            assert service.state_check(after).validate()["overall_success"]
            assert service.seed_slots(1, seed=1)


@pytest.mark.parametrize(
    ("unread", "lost", "success", "inconclusive"), [(0, 0, True, False), (1, 0, False, True), (1, 1, False, False)]
)
def test_a_key_that_could_not_be_read_was_not_checked(unread, lost, success, inconclusive):
    keys = sorted(_KEYS)

    def get(key):
        if key in keys[:unread]:
            raise ConnectionError("reset")
        return None if key in keys[unread : unread + lost] else _KEYS[key]

    snapshot = snapshot_of(node_row("p", 7000), mode=DeploymentMode.STANDALONE)
    config = DataConsistencyValidationConfig()
    with patch(f"{LOCAL}validation.data.valkey_client", return_value=client_context(Mock(get=get))):
        result = check_data(snapshot, _KEYS, config)

    # One unread key in a hundred is not a pass; a key seen missing is data loss whatever else could not be read.
    assert (result.success, result.inconclusive, len(result.unreachable_keys)) == (success, inconclusive, unread)


@pytest.mark.parametrize("other_fails", [False, True])
def test_a_check_that_breaks_judges_nothing_unless_another_saw_valkey_fail(other_fails):
    rows = [node_row("a", 7000), *([node_row("b", 7001, status="failed")] if other_fails else [])]
    connection = Mock(spec=DeploymentConnection, mode=DeploymentMode.CLUSTER, instances=[])
    connection.get_current_nodes.return_value = rows
    config = _only("roles", "slot_coverage", stabilization_wait=0)
    with (
        patch(f"{CHECK}.take_snapshot", return_value=snapshot_of(*rows)),
        patch(f"{CHECK}.check_slot_coverage", side_effect=KeyError("slots")),
    ):
        check = DeploymentStateCheck(connection, [], {"valkey-0": "primary"}, config, rng=random.Random(0))
        result = check._validate_once(None)

    # A KeyError in the harness is no Valkey failure; a node that failed without being stopped is one.
    assert result.failed_checks == (["slot_coverage", "roles"] if other_fails else ["slot_coverage"])
    assert result.inconclusive is not other_fails


def test_one_validation_reads_each_node_once():
    # Every check judges one moment: the topology is read once, and each node's CLUSTER NODES once.
    fake = FakeCluster({port: (f"n{port - 7000}", port - 3 if port > 7002 else None, "") for port in range(7000, 7006)})
    for port, slots in zip(range(7000, 7003), ("0-5460", "5461-10922", "10923-16383")):
        fake.nodes[port] = (fake.nodes[port][0], None, slots)
    infos = [instance(f"valkey-{port - 7000}", "replica" if port > 7002 else "primary", port) for port in fake.nodes]
    planned = {info.instance_id: info.role for info in infos}
    config = StateValidationConfig(stabilization_wait=0, retry_on_transient_failure=False, check_data_consistency=False)
    check = DeploymentStateCheck(DeploymentConnection(infos, "one-read"), infos, planned, config, rng=random.Random(0))

    with patch("valkey.Valkey", side_effect=fake.client):
        summary = check.validate()

    assert summary["overall_success"], summary
    reads = {port: count for (port, what), count in fake.reads.items() if what == "CLUSTER NODES"}
    # The topology's own read is the first node's; every other node is read once.
    assert reads == {7000: 2, 7001: 1, 7002: 1, 7003: 1, 7004: 1, 7005: 1}, reads


@pytest.mark.parametrize(
    ("lost", "message", "critical"),
    [(True, "shard 0 lost its replicas", True), (False, "the words insufficient redundancy, not the fact", False)],
)
def test_a_critical_replication_failure_is_a_flag_not_a_message(lost, message, critical):
    replication = ReplicationValidation(success=False, redundancy_lost=lost, error_message=message)
    result = StateValidationResult(overall_success=False, validation_duration=0, replication=replication)
    assert result.is_critical_failure() is critical


def test_a_wall_clock_jump_cuts_no_wait_short(monkeypatch):
    # Deadlines are on the monotonic clock: a wall clock set forward mid-wait changes nothing.
    wall = iter(range(0, 10**9, 10**8))
    clock = VirtualClock().install(monkeypatch, failover, queries, check_module)
    monkeypatch.setattr(clock, "time", lambda: next(wall))
    with (
        patch("time.time", side_effect=lambda: next(wall)),
        patch(f"{LOCAL}failover._shard_settled", side_effect=[False, False, True]),
    ):
        assert wait_until_settled(Mock(instances=[]), 30.0, shard_id=0, old_primary="o")
        result = _validate_once(_only(stabilization_wait=0), Mock(spec=DeploymentConnection))
    assert result.overall_success, result.error_messages


@pytest.mark.parametrize(
    ("running", "inconclusive", "attempts", "message"),
    [
        # The replica's process runs but its INFO did not come back in time: nothing was seen, so the attempt is
        # retried and, still unanswered, inconclusive (the oracles report ERROR).
        (True, True, 3, "running instance(s) did not answer: 127.0.0.1:7001"),
        # Its process is gone and nobody stopped it: a real failure, judged once retries cannot change it.
        (False, False, 3, "Unexpected node failures"),
    ],
    ids=["did-not-answer", "process-not-running"],
)
def test_an_instance_that_did_not_answer_is_retried_and_inconclusive_not_a_failure(
    monkeypatch, running, inconclusive, attempts, message
):
    from valkey_fuzzer.deployment.local.queries import Replication
    from valkey_fuzzer.deployment.local.validation.check import validation_summary

    primary = instance("valkey-0", "primary", 7000)
    replica = instance("valkey-1", "replica", 7001, primary_id="valkey-0")
    if not running:
        replica.process.poll.return_value = -9
    connection = DeploymentConnection([primary, replica], "d", mode=DeploymentMode.STANDALONE)
    timeouts = []

    def query(host, port, timeout, query):
        timeouts.append(timeout)
        return None if port == 7001 else Replication(role="primary", offset=10, connected_replicas=1)

    monkeypatch.setattr(f"{LOCAL}topology.query_or_none", query)
    monkeypatch.setattr(f"{LOCAL}validation.check.syncing_replicas", lambda connection: [])
    config = StateValidationConfig(
        check_data_consistency=False, check_logs=False, check_cluster_status=False, check_slot_coverage=False,
        check_view_consistency=False, stabilization_wait=0, retry_delay=0, max_retries=2,
    )  # fmt: skip
    planned = {"valkey-0": "primary", "valkey-1": "replica"}
    check = DeploymentStateCheck(connection, [primary, replica], planned, config, rng=random.Random(0))
    summary = validation_summary(check._validate_with_retry(None))
    assert (summary["overall_success"], summary["inconclusive"]) == (False, inconclusive)
    assert summary["critical"] is not inconclusive
    assert any(message in item for item in summary["error_messages"]), summary["error_messages"]
    # Each read waited the configured check timeout, not a fixed 2 s.
    assert set(timeouts) == {config.replication_config.timeout}
    assert len(timeouts) == attempts * (2 if running else 1)
