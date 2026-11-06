"""valkey.action.cluster-chaos, its kill safety, valkey.oracle.deployment-state, and the cluster-chaos experiment,
against a fake deployment."""

import random
import signal
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.support import VirtualClock
from tests.valkey_experiments_support import (
    chaos_operation,
    evidence_values,
    resolved,
    validation_result,
    valkey_experiment,
)
from valkey_fuzzer.components import component_registrations, processes
from valkey_fuzzer.components.cluster_chaos import ClusterChaosAction
from valkey_fuzzer.components.oracles import DeploymentStateOracle, LivenessOracle
from valkey_fuzzer.components.wave_targets import Member, TargetSelector
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus, Evidence
from valkey_fuzzer.testing import FakeContext, run_experiment

PASSED = validation_result()


def failed(*checks, critical=False, inconclusive=False):
    return validation_result(*checks, critical=critical, inconclusive=inconclusive)


class FakeCluster:
    """Shards of a primary and ``replicas`` replicas, and its own state check: kills stop nodes, failovers return
    ``failovers[shard]`` (default True), and validations the next of ``results`` (default a pass)."""

    def __init__(self, shards=3, replicas=1, *, failovers=None, results=(), kill_error=None, log_dir=""):
        roles = ["primary"] + ["replica"] * replicas
        self.instances = [
            SimpleNamespace(instance_id=f"valkey-{shard * len(roles) + index}", role=role, shard_id=shard)
            for shard in range(shards)
            for index, role in enumerate(roles)
        ]
        self.alive = {node.instance_id: True for node in self.instances}
        self.failovers, self.results, self.kill_error, self.log_dir = (
            failovers or {},
            list(results),
            kill_error,
            log_dir,
        )
        self.seed_error = None
        # Replicas whose replication link is not up: one that just migrated has not synced yet.
        self.unsynced = set()
        # Replicas whose link is down with their data kept: their primary was killed.
        self.link_down = set()
        self.mode = SimpleNamespace(value="cluster")
        self.calls, self.draws, self.validations, self.stopped = [], [], [], []
        self.loaded = None
        self.lock = threading.Lock()

    def live_instances(self):
        return [dict(vars(node)) for node in self.instances if self.alive[node.instance_id]]

    def hide(self, predicate):
        """Drop the nodes ``predicate`` picks from the live view."""
        live = self.live_instances
        self.live_instances = lambda: [node for node in live() if not predicate(node)]
        return self

    def is_alive(self, node_id):
        return self.alive[node_id]

    def wait_exit(self, node_id, timeout=10.0):
        return not self.alive[node_id]

    def send_signal(self, node_id, signum):
        if self.kill_error is not None:
            raise self.kill_error
        if not self.alive[node_id]:
            raise ProcessLookupError(node_id)
        with self.lock:
            self.alive[node_id] = False
            self.calls.append(("kill", node_id, signum))

    def failover(self, shard, *, force=False, timeout=60.0, stopped_instances=(), rng=None):
        with self.lock:
            self.calls.append(("failover", shard, tuple(stopped_instances)))
            self.draws.append((shard, rng.random()))
        # None: nothing to fail over (the shard, or every replica of its primary, stopped on purpose).
        outcome = self.failovers.get(shard, True)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def seed_slots(self, keys_per_slot, *, seed=None):
        return True

    def state_check(self, validation=None, *, seed=None):
        validation = dict(validation or {})
        self.config = SimpleNamespace(
            blocking_on_failure=validation.get("blocking_on_failure", False),
            check_data_consistency=validation.get("check_data_consistency", True),
        )
        return self

    def write_test_keys(self):
        if self.seed_error:
            raise RuntimeError(self.seed_error)
        return {"fuzzer:test:0": "value"}

    def load_test_keys(self, keys):
        self.loaded = dict(keys)

    def mark_stopped(self, node_id, role, shard=None):
        # shard: the live shard the node served when it stopped; None keeps its planned one.
        self.stopped.append((node_id, role, shard))

    def validate(self, operations=None, *, retry=True):
        self.validations.append({"operations": operations, "stopped": list(self.stopped)})
        return self.results.pop(0) if self.results else dict(PASSED)

    def new_client(self, node_id, timeout=5.0):
        client = MagicMock()
        client.__enter__.return_value = client
        # INFO's offset is 1 for a replica with neither a primary nor a cached one: one that just migrated.
        offset = 1 if node_id in self.unsynced else 4242
        link = "down" if node_id in self.unsynced or node_id in self.link_down else "up"
        client.info.return_value = {"loading": 0, "master_link_status": link, "slave_repl_offset": offset}
        return client

    def log_file(self, node_id):
        return f"{self.log_dir}/{node_id}.log"

    def process_output_file(self, node_id):
        return None


def run_chaos(handle, seed=7, **config):
    config.setdefault("waves", [[failover(0)]])
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.cluster-chaos", id="chaos", config=config)
    )
    context = FakeContext(spec, services={"valkey.deployment": handle}, deployment_mode="cluster", seed=seed)
    return ClusterChaosAction().run(context, spec), context


def failover(shard, **fields):
    return {"op": "failover", "shard": shard, **fields}


def kill(target, **fields):
    return {"op": "kill", "target": target, **fields}


def evidence(result, evidence_id):
    [value] = evidence_values(result.evidence, evidence_id)
    return value


# ---------------------------------------------------------------------------
# Kill safety: never a shard's last live member, never a primary the quorum needs
# ---------------------------------------------------------------------------


def shards(count, members=2):
    """``count`` shards of a primary and ``members - 1`` replicas, numbered valkey-0 up."""
    return [
        Member(f"valkey-{shard * members + index}", "replica" if index else "primary", shard)
        for shard in range(count)
        for index in range(members)
    ]


# The seed-38 soak view: valkey-4, planned in shard 1, owns shard 0's slots and is its last live member.
_MIGRATED = [Member("valkey-3", "primary", 1), Member("valkey-4", "primary", 0), Member("valkey-5", "replica", 1),
             *shards(4, 3)[6:9], Member("valkey-10", "replica", 3), Member("valkey-11", "primary", 3)]  # fmt: skip


@pytest.mark.parametrize(
    ("planned", "live", "picks"),
    [
        (shards(1), None, [("any", "*"), ("any", "no shard-safe target")]),
        (shards(1, 3), shards(1, 3)[:1], [("any", "no shard-safe target")]),
        (shards(3), None, [("primary", "primary"), ("primary", "no quorum-safe target"), ("any", "replica")]),
        (shards(1, 3), None, [("replica", "replica"), (["valkey-9"], "no live valkey-9 to kill"),
                              (["valkey-0"], "no quorum-safe target"), (["valkey-0", "valkey-1", "valkey-2"], "replica")]),
        (shards(1, 1), None, [("replica", "no live replica to kill")]),
        (shards(3), None, [(["valkey-1"], "valkey-1"), (["valkey-0"], "no shard-safe target"), (["valkey-2"], "valkey-2"),
                           (["valkey-4"], "no quorum-safe target")]),
        (shards(4, 3), _MIGRATED, [(["valkey-4"], "no shard-safe target"), (["valkey-3"], "valkey-3")]),
        # valkey-6 is a live primary without slots: no quorum voter, and it empties no shard.
        (shards(3), [*shards(3)[:4], Member("valkey-6", "primary", None)],
         [(["valkey-0"], "no quorum-safe target"), (["valkey-6"], "valkey-6")]),
    ],
    ids=["last-shard-member", "offline-members-are-no-survivors", "quorum-of-planned-shards", "named-targets",
         "no-live-role", "named-targets-shard-and-quorum", "shard-of-a-migrated-replica", "slotless-primary"],
)  # fmt: skip
def test_kills_never_empty_a_live_shard_or_break_the_primary_quorum(planned, live, picks):
    selector = TargetSelector(planned, quorum=True)
    if live is not None:
        selector.begin_wave(live)
    for target, expected in picks:
        member, refusal = selector.select("kill", target, random.Random(1))
        reason = refusal and refusal.reason
        if expected == "*":
            assert member is not None, (target, reason)
        elif expected in ("primary", "replica"):
            assert member.role == expected, (target, reason)
        elif expected.startswith("valkey-"):
            assert member.instance_id == expected, (target, reason)
        else:
            assert (member, reason) == (None, expected), target


# ---------------------------------------------------------------------------
# The chaos action
# ---------------------------------------------------------------------------


def test_kills_then_failovers_validate_each_wave_with_the_stopped_nodes():
    handle = FakeCluster(replicas=2)
    waves = [
        [
            kill("replica", signal="SIGTERM"),
            failover(0, delay_s=0.05),
            kill("replica", signal="SIGTERM"),
            failover(1, delay_s=0.05),
        ],
        [kill("replica", signal="SIGTERM"), failover(0, delay_s=0.05)],
    ]
    result, context = run_chaos(handle, waves=waves)
    assert result.status is ComponentStatus.PASS, result.summary
    kills = [call for call in handle.calls if call[0] == "kill"]
    assert len(kills) == 3 and {call[2] for call in kills} == {signal.SIGTERM}
    # Each failover sees the nodes stopped before it.
    assert all(call[2] for call in handle.calls if call[0] == "failover")
    assert [len(item["stopped"]) for item in handle.validations] == [2, 3]
    schedule = evidence(result, "valkey.chaos.schedule")
    assert [(wave["wave"], wave["kills"]) for wave in schedule["waves"]] == [(1, 2), (2, 1)]
    # Validation knows the live shard each killed node served.
    assert {(node, shard) for node, _, shard in handle.validations[-1]["stopped"]} == {
        (kill["instance_id"], kill["shard"]) for kill in schedule["all_kills"]
    }
    assert sorted(evidence(result, "valkey.instances.expected-down")["instances"]) == sorted(call[1] for call in kills)
    assert evidence(result, "valkey.chaos.test-keys") == {"keys": {"fuzzer:test:0": "value"}}
    assert len(context.event_bus.committed_history("chaos.instance-killed")) == 3
    assert len(context.event_bus.committed_history("chaos.wave-validated")) == 2

    handle = FakeCluster(replicas=2)
    assert run_chaos(handle, waves=[[failover(0), kill("any", after=0)]])[0].status is ComponentStatus.PASS
    assert [call[0] for call in handle.calls] == ["failover", "kill"]


def test_a_failover_failure_is_excused_only_by_interfering_chaos():
    # Shard 0's own replica, killed before its failover finished.
    waves = [[kill("valkey-1"), failover(0, delay_s=0.05)]]
    result, _ = run_chaos(FakeCluster(replicas=2, failovers={0: False}), waves=waves)
    assert result.status is ComponentStatus.PASS, result.summary
    assert chaos_operation(result, "1.1")["excused_by"] == [
        {"instance_id": "valkey-1", "operation": "1.0", "reason": "same shard"}
    ]
    # No kill, a kill after the failover finished, and a replica of another shard.
    for wave in (
        [failover(0)],
        [failover(0), kill("valkey-1", after=0)],
        [kill("valkey-4"), failover(0, delay_s=0.05)],
    ):
        result, _ = run_chaos(FakeCluster(replicas=2, failovers={0: False}), waves=[wave])
        assert result.status is ComponentStatus.FAIL and "failed without an interfering operation" in result.summary
        records = evidence(result, "valkey.chaos.schedule")["operations"]
        assert next(op for op in records if op["op"] == "failover")["excused_by"] == []


class _GatedCluster(FakeCluster):
    """Shard 0's failovers finish only once valkey-2, shard 1's primary, is dead."""

    def failover(self, shard, **kwargs):
        deadline = time.monotonic() + 10
        while shard == 0 and self.alive["valkey-2"] and time.monotonic() < deadline:
            time.sleep(0.01)
        return super().failover(shard, **kwargs)


def test_a_primary_kill_excuses_failovers_only_in_its_own_wave():
    result, _ = run_chaos(_GatedCluster(failovers={0: False}), waves=[[kill("valkey-2"), failover(0)], [failover(0)]])
    excuse = {"instance_id": "valkey-2", "operation": "1.0", "reason": "primary in the same wave"}
    assert (
        chaos_operation(result, "1.1")["excused_by"] == [excuse] and chaos_operation(result, "2.0")["excused_by"] == []
    )
    assert result.status is ComponentStatus.FAIL
    assert "wave 2 failover 2.0 failed without an interfering operation" in result.summary


def _exited_by_itself():
    handle = FakeCluster(replicas=2)
    handle.alive["valkey-1"] = False  # exited on its own, but still in the live view
    handle.live_instances = lambda: [dict(vars(node)) for node in handle.instances]
    return handle


def _unseedable():
    handle = FakeCluster()
    handle.seed_error = "no keys"
    return handle


def _primaries_drop_out():
    """By kill time every primary has dropped out of the live view: each replica is its shard's last member."""
    handle, calls = FakeCluster(), []
    live = handle.live_instances
    handle.live_instances = lambda: calls.append(1) or [n for n in live() if len(calls) == 1 or n["role"] == "replica"]
    return handle


_TWICE = [[kill("valkey-1"), failover(1)], [kill("valkey-1"), failover(1)]]


@pytest.mark.parametrize(
    ("handle", "waves", "status", "summary", "records"),
    [
        (lambda: FakeCluster(replicas=2, kill_error=PermissionError("denied")), [[kill("replica"), failover(0)]],
         "ERROR", "kill 1.0 was not injected: PermissionError: denied", {}),
        (lambda: FakeCluster(results=[failed("replication")]), None, "FAIL", "wave 1 validation failed (replication)", {}),
        # The harness did not see what Valkey did with the failover.
        (lambda: FakeCluster(failovers={0: RuntimeError("boom")}), None, "ERROR",
         "failover 1.0 could not be observed: RuntimeError: boom", {"1.0": ("error", "RuntimeError: boom")}),
        (_exited_by_itself, [[kill("valkey-1"), failover(0)]], "FAIL", "valkey-1 exited before it was killed", {}),
        (_unseedable, None, "ERROR", "could not start: RuntimeError:", {}),
        # Kills that never landed must not PASS: a target gone, a role with no live member, a kill the safety
        # rules blocked, and a kill that became unsafe by the time it was due.
        (FakeCluster, _TWICE, "ERROR", "wave 2 kill 2.0 had no target", {"2.0": ("missing", "no live valkey-1 to kill")}),
        (lambda: FakeCluster().hide(lambda node: node["role"] == "replica"), [[kill("replica"), failover(0)]],
         "ERROR", "had no target", {"1.0": ("missing", "no live replica to kill")}),
        (lambda: FakeCluster(results=[PASSED, failed("replication")]), _TWICE, "FAIL",
         "wave 2 validation failed (replication)", {"2.0": ("missing", "no live valkey-1 to kill")}),
        (lambda: FakeCluster().hide(lambda node: node["instance_id"] == "valkey-1"), [[kill("valkey-0"), failover(1)]],
         "SKIP", "skipped by the safety rules", {"1.0": ("skipped", "no shard-safe target")}),
        (_primaries_drop_out, [[failover(0), kill("replica", after=0)]], "SKIP", "",
         {"1.1": ("skipped", "no longer shard-safe")}),
        # A wave whose kill the safety rules blocked is no chaos wave, and the summary says so.
        (FakeCluster, [[kill(["valkey-0", "valkey-1"]), failover(1)]] * 2, "PASS",
         "1 planned operation(s) skipped by the safety rules (['2.0'])", {"2.0": ("skipped", "no shard-safe target")}),
        # A later wave the safety rules refused whole leaves the run tested by its earlier waves.
        (FakeCluster, [[kill(["valkey-0", "valkey-1"]), failover(1)], [kill(["valkey-0", "valkey-1"])]], "PASS",
         "1 planned operation(s) skipped by the safety rules (['2.0'])", {"2.0": ("skipped", "no shard-safe target")}),
    ],
    ids=["kill-not-sent", "wave-validation-failed", "failover-raised", "exited-by-itself", "seeding-failed",
         "target-gone", "role-not-live", "validation-fails-despite-missing-target", "kill-blocked",
         "kill-became-unsafe", "blocked-wave-not-counted", "blocked-wave-not-an-error"],
)  # fmt: skip
def test_chaos_runs_that_did_not_establish_their_faults_do_not_pass(handle, waves, status, summary, records):
    result, _ = run_chaos(handle(), **({"waves": waves} if waves else {}))
    assert result.status.value == status and summary in result.summary, result.summary
    for index, (field, value) in records.items():
        assert chaos_operation(result, index)[field] == value


def test_a_failover_with_nothing_to_fail_over_is_skipped_not_failed():
    result, _ = run_chaos(FakeCluster(failovers={0: None}), waves=[[failover(0)]])
    assert result.status is ComponentStatus.SKIP and "no planned fault was injected" in result.summary, result.summary
    record = chaos_operation(result, "1.0")
    assert record["success"] is False and "stopped whole on purpose" in record["skipped"]


def test_a_failover_that_raised_after_a_kill_in_its_shard_is_excused():
    handle = FakeCluster(replicas=2, failovers={0: RuntimeError("boom")})
    result, _ = run_chaos(handle, waves=[[kill("valkey-1"), failover(0, after=0)]])
    assert result.status is ComponentStatus.PASS, result.summary
    assert chaos_operation(result, "1.1")["excused_by"] == [
        {"instance_id": "valkey-1", "operation": "1.0", "reason": "same shard"}
    ]


@pytest.mark.parametrize(("signal_name", "status"), [("SIGKILL", "ERROR"), ("SIGTERM", "FAIL")])
def test_a_kill_target_that_never_exits_fails_only_when_valkey_could_have_refused(monkeypatch, signal_name, status):
    # Valkey drops a SIGTERM it cannot act on (its shutdown save failed); nothing refuses a SIGKILL.
    clock = VirtualClock().install(monkeypatch, processes)

    class Unkillable(FakeCluster):
        def send_signal(self, node_id, signum):
            self.calls.append(("kill", node_id, signum))

        def wait_exit(self, node_id, timeout=10.0):
            clock.sleep(timeout)
            return False

    result, _ = run_chaos(Unkillable(replicas=2), waves=[[kill("valkey-1", signal=signal_name)]])
    assert result.status.value == status, result.summary
    assert f"valkey-1 did not exit within 30s of {signal_name}" in result.summary


@pytest.mark.parametrize(("validation", "results"), [({}, [failed("slot_coverage", critical=True)]),
                                                    ({"blocking_on_failure": True}, [failed("replication")])])  # fmt: skip
def test_critical_or_blocking_failures_halt_after_the_wave(validation, results):
    handle = FakeCluster(replicas=2, results=results)
    result, _ = run_chaos(handle, waves=[[failover(0)], [failover(0)]], validation=validation)
    schedule = evidence(result, "valkey.chaos.schedule")
    assert result.status is ComponentStatus.FAIL
    assert (schedule["halted_after_wave"], schedule["not_run"], len(handle.validations)) == (1, 1, 1)


class _SlowPromotion(FakeCluster):
    """A failover's promotion becomes visible only when it settles (gossip lags the election)."""

    def failover(self, shard, *, force=False, timeout=60.0, stopped_instances=(), rng=None):
        time.sleep(0.3)
        with self.lock:
            for node in self.instances:
                if node.shard_id == shard and self.alive[node.instance_id]:
                    node.role = "replica" if node.role == "primary" else "primary"
            self.calls.append(("settled", shard))
        return True


def test_a_kill_waits_for_its_shards_failover_and_is_judged_by_the_roles_it_left():
    # Soak s100540: a kill planned for a replica took the primary its shard's failover had just promoted.
    handle = _SlowPromotion()
    result, _ = run_chaos(handle, waves=[[kill("valkey-4"), failover(1, force=True), kill("valkey-3", delay_s=0.1)]])
    assert ("kill", "valkey-3") not in [call[:2] for call in handle.calls]
    assert chaos_operation(result, "1.2")["skipped"] == "no longer quorum-safe"

    handle = _SlowPromotion()
    result, _ = run_chaos(handle, waves=[[failover(1, force=True), kill("valkey-3", delay_s=0.1)]])
    calls = [call[:2] for call in handle.calls]
    assert calls.index(("settled", 1)) < calls.index(("kill", "valkey-3"))
    assert chaos_operation(result, "1.1")["success"] and "1.0" in chaos_operation(result, "1.1")["waited_for"]
    assert evidence(result, "valkey.chaos.schedule")["all_kills"][0]["role"] == "primary"


class _MidSwitch(_SlowPromotion):
    """While the failover runs, the live view shows the replica it promotes in no shard (its slots are moving)."""

    def failover(self, shard, *, force=False, timeout=60.0, stopped_instances=(), rng=None):
        promoted = next(n for n in self.instances if n.shard_id == shard and n.role == "replica")
        promoted.shard_id = None
        time.sleep(0.3)
        promoted.shard_id = shard
        return super().failover(shard, force=force, timeout=timeout, stopped_instances=stopped_instances, rng=rng)


def test_a_kill_waits_for_the_failover_of_the_shard_it_was_planned_in_when_the_live_view_shows_none():
    # Soak s1975728775: valkey-8 won its forced election in the millisecond it was asked; the kill planned for it
    # read no shard for it, did not wait for the failover, and killed a second primary of three.
    handle = _MidSwitch()
    result, _ = run_chaos(handle, waves=[[kill("valkey-4"), failover(1, force=True), kill("valkey-3", delay_s=0.1)]])
    assert ("kill", "valkey-3") not in [call[:2] for call in handle.calls]
    assert chaos_operation(result, "1.2")["skipped"] == "no longer quorum-safe"


# ---------------------------------------------------------------------------
# Final validation, liveness, and the experiment through the real compiler
# ---------------------------------------------------------------------------


def _evidence(evidence_id, value):
    return Evidence(evidence_id=evidence_id, source="chaos", phase="action.completed", observed_at=1.0, value=value)


def test_final_validation_uses_the_recorded_kills_keys_and_failovers_and_fails_with_the_failed_checks():
    def validate(handle, evidence=(), **config):
        spec = ComponentSpec(
            role=ComponentRole.ORACLE, use="valkey.oracle.deployment-state", id="cluster", config=config
        )
        context = FakeContext(services={"valkey.deployment": handle}, deployment_mode="cluster")
        return DeploymentStateOracle().evaluate(context, resolved(spec), evidence)

    schedule = {
        "operations": [{"index": "1.0", "op": "failover", "shard": 1, "success": True},
                       {"index": "1.1", "op": "failover", "shard": 2, "success": False},
                       {"index": "1.2", "op": "kill", "shard": None, "success": True}],
        "kills": [{"instance_id": "valkey-3", "role": "replica", "shard": 2, "success": True},
                  {"instance_id": "valkey-5", "role": "x", "shard": 2, "success": False}],
    }  # fmt: skip
    handle = FakeCluster()
    observed = (
        _evidence("valkey.chaos.schedule", schedule),
        _evidence("valkey.chaos.test-keys", {"keys": {"k": "v"}}),
    )
    result = validate(handle, observed, chaos="chaos")
    assert (result.status, result.summary) == (ComponentStatus.PASS, "cluster state validated (0 check(s))")
    assert result.evidence[0].evidence_id == "valkey.deployment.validated"
    # A kill records the live shard its node served (replica migration moves nodes away from the plan).
    assert handle.loaded == {"k": "v"} and handle.stopped == [("valkey-3", "replica", 2)]
    assert [op["index"] for op in handle.validations[0]["operations"]] == ["1.0"]

    # No chaos action ran: no kills, no data check, and no log check (no operations to look for).
    handle = FakeCluster(results=[failed("roles")])
    result = validate(handle)
    assert (result.status, result.summary) == (ComponentStatus.FAIL, "roles broke")
    assert handle.config.check_data_consistency is False and handle.validations[0]["operations"] is None
    assert list(result.diagnostics["notes"]) == ["data consistency not checked: no test keys were recorded"]
    result = validate(FakeCluster(results=[{**failed("slot_coverage"), "error_messages": []}]))
    assert (result.status, result.summary) == (ComponentStatus.FAIL, "failed checks: slot_coverage")

    # An instance a fault stopped on purpose is no unexpected failure, as for liveness; one still running is.
    handle = FakeCluster()
    handle.alive["valkey-3"] = False
    down = (_evidence("valkey.instances.expected-down", {"instances": ["valkey-3", "valkey-1"]}),)
    assert validate(handle, down).status is ComponentStatus.PASS
    assert handle.stopped == [("valkey-3", "replica", None)]

    # A validation config the deployment rejects leaves the run unjudged.
    handle = FakeCluster()
    handle.state_check = lambda validation, seed=None: (_ for _ in ()).throw(ValueError("no such check"))
    result = validate(handle, validation={"check_logs": False})
    assert (result.status, result.summary) == (ComponentStatus.ERROR, "invalid validation config: no such check")

    # A validation that never saw the deployment (a node did not answer, its budget ran out) judges nothing.
    result = validate(FakeCluster(results=[failed("unanswered", inconclusive=True)]))
    assert (result.status, result.summary) == (ComponentStatus.ERROR, "unanswered broke")


def test_liveness_accepts_expected_down_nodes_but_still_scans_their_logs(tmp_path):
    handle = FakeCluster(shards=1, log_dir=str(tmp_path))
    handle.alive["valkey-1"] = False
    spec = resolved(ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.liveness", id="liveness"))
    context = FakeContext(services={"valkey.deployment": handle}, deployment_mode="cluster")
    down = (_evidence("valkey.instances.expected-down", {"instances": ["valkey-1"]}),)

    assert LivenessOracle().evaluate(context, spec, ()).status is ComponentStatus.FAIL
    result = LivenessOracle().evaluate(context, spec, down)
    assert result.status is ComponentStatus.PASS and "1 stopped on purpose" in result.summary
    (tmp_path / "valkey-1.log").write_text("=== VALKEY BUG REPORT START: Cut & paste starting from here ===\n")
    result = LivenessOracle().evaluate(context, spec, down)
    assert result.status is ComponentStatus.FAIL and "crash report" in result.summary


@pytest.mark.parametrize("wave_fails", [False, True])
def test_the_cluster_chaos_experiment_passes_or_fails_with_its_waves_and_always_validates(tmp_path, wave_fails):
    handle = FakeCluster(log_dir=str(tmp_path), results=[failed("view_consistency")] if wave_fails else ())
    outcome = run_experiment(
        valkey_experiment("valkey.cluster-chaos"),
        component_registrations(),
        inputs={"waves": [[kill("replica"), failover(0, delay_s=0.05)]]},
        deployment={"primaries": 3, "replicas_per_primary": 1},
        services={"valkey.deployment": handle},
    )
    results = {result.component_id: result for result in outcome.component_results}
    assert {name: result.status.value for name, result in results.items()} == {
        "chaos": "FAIL" if wave_fails else "PASS", "cluster": "PASS", "liveness": "PASS"
    }  # fmt: skip
    assert outcome.status.value == ("FAIL" if wave_fails else "PASS"), outcome.summary
    assert "stopped on purpose" in results["liveness"].summary


def test_a_replica_that_has_not_synced_is_no_survivor_of_its_shard():
    # The cluster-chaos soak (seed 620078145): a replica migrated to an orphaned primary, which was killed before
    # the replica's first sync; the replica, counted as the shard's survivor, won its election with offset 0 and
    # the shard's data was lost.
    handle = FakeCluster()
    handle.unsynced.add("valkey-1")
    result, _ = run_chaos(handle, waves=[[kill("valkey-0"), failover(1)]])
    assert chaos_operation(result, "1.0")["skipped"] == "no shard-safe target", result.summary
    assert ("kill", "valkey-0", signal.SIGKILL) not in handle.calls
    # Killing the unsynced replica itself takes nothing of the shard's data.
    handle = FakeCluster()
    handle.unsynced.add("valkey-1")
    result, _ = run_chaos(handle, waves=[[kill("valkey-1"), failover(1)]])
    assert chaos_operation(result, "1.0").get("skipped") is None, result.summary


def test_a_replica_whose_primary_was_killed_still_holds_its_shard():
    # The soak after 015af13: a replica whose primary a wave killed has its link down but all of the shard's data;
    # counted as unsynced, the next kill took it too, and the shard's slots stayed on stopped nodes.
    handle = FakeCluster()
    handle.link_down.add("valkey-1")
    handle.alive["valkey-0"] = False
    result, _ = run_chaos(handle, waves=[[kill("valkey-1"), failover(1)]])
    assert chaos_operation(result, "1.0")["skipped"] == "no shard-safe target", result.summary
