"""Durability checks: how the key-value workload records each write, the ledger model of the values a correct
server may hold, the loss windows faults open, and the verdicts of ``valkey.oracle.ledger``."""

import functools
import os
import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
import valkey

from tests.support import VirtualClock
from tests.valkey_experiments_support import fake_context, registration_for, valkey_service
from valkey_fuzzer.components import instances, polling, processes
from valkey_fuzzer.components.faults import FAULT_INJECTED
from valkey_fuzzer.components.files import write_jsonl
from valkey_fuzzer.components.ledger import (
    EVERYSEC_LOSS_WINDOW_S,
    LOSS_WINDOW,
    LedgerModel,
    LedgerOracle,
    _keys_owned_by_primary_of,
    fault_records,
    load_ledger,
)
from valkey_fuzzer.components.workloads import LEDGER_EVIDENCE, KeyValueWorkload, _Target
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import Evidence
from valkey_fuzzer.testing import resolved_spec

# Slots: m:2 hashes into 0-8191 (shard 0), m:0 into 8192-16383 (shard 1).
SHARD0_KEY = "vf:writes:m:2"
SHARD1_KEY = "vf:writes:m:0"
KEY = SHARD1_KEY


def _op(op_id, op, key, outcome="ok", value=None, reply=None, t_end=None, instance_id=None, wait=None):
    t_end = float(op_id) if t_end is None else t_end
    entry = {"id": op_id, "op": op, "key": key, "outcome": outcome, "t_start": t_end - 0.001, "t_end": t_end}
    for name, item in (("value", value), ("instance_id", instance_id), ("wait", wait)):
        if item is not None:
            entry[name] = item
    if op == "get":
        entry["reply"] = reply
    return entry


# The ledger model


def _two_writes(wait=None, key="k"):
    """Two acknowledged writes by valkey-0, the second inside the 10-12 s window."""

    return [
        _op(1, "set", key, "ok", "v1", t_end=5.0, instance_id="valkey-0"),
        _op(2, "set", key, "ok", "v2", t_end=11.0, instance_id="valkey-0", wait=wait),
    ]


_SCOPED = [(10.0, 12.0, frozenset({"valkey-0"}))]


@pytest.mark.parametrize(
    ("preload", "ledger", "windows", "options", "key", "possible", "violations"),
    [
        pytest.param({"k": "v0"}, [_op(1, "set", "k", "ok", "v1"), _op(2, "set", "k", "ambiguous", "v2"), _op(3, "set", "k", "error", "v3")], [], {}, "k", {"v1", "v2"}, [], id="acknowledged-ambiguous-rejected"),
        pytest.param({}, [_op(1, "set", "j", "ok", "j1"), _op(2, "del", "j")], [], {}, "j", {None}, [], id="delete"),
        pytest.param({}, [_op(1, "set", "k", "ok", "v1", t_end=10), _op(2, "set", "k", "ok", "v2", t_end=19)], [], {}, "k", {"v2"}, [], id="no-window"),
        pytest.param({}, [_op(1, "set", "k", "ok", "v1", t_end=10), _op(2, "set", "k", "ok", "v2", t_end=19)], [(18.0, 20.0)], {}, "k", {"v1", "v2"}, [], id="window-makes-an-ack-optional"),
        # A window that names instances lets only their acknowledgements be lost.
        pytest.param({}, [_op(1, "set", "a", "ok", "a1", t_end=5.0), _op(2, "set", "a", "ok", "a2", t_end=11.0, instance_id="valkey-0")], _SCOPED, {}, "a", {"a1", "a2"}, [], id="scoped-window-old-primary"),
        pytest.param({}, [_op(1, "set", "b", "ok", "b0", t_end=5.0), _op(2, "set", "b", "ok", "b1", t_end=11.0, instance_id="valkey-1")], _SCOPED, {}, "b", {"b1"}, [], id="scoped-window-new-primary"),
        # An acknowledgement nobody could attribute may be the old primary's.
        pytest.param({}, [_op(1, "set", "c", "ok", "c0", t_end=5.0), _op(2, "set", "c", "ok", "c1", t_end=11.0)], _SCOPED, {}, "c", {"c0", "c1"}, [], id="scoped-window-unknown-acknowledger"),
        pytest.param({"k": "v0"}, [_op(1, "set", "k", "ok", "v1"), _op(2, "get", "k", reply="v0"), _op(3, "get", "k", reply="v1")], [], {}, "k", {"v1"}, [2], id="impossible-read"),
        pytest.param({"k": "old"}, [_op(1, "set", "k", "ambiguous", "new"), _op(2, "get", "k", reply="new")], [], {}, "k", {"new"}, [], id="read-proves-an-ambiguous-write"),
        pytest.param({"k": "old"}, [_op(1, "set", "k", "ambiguous", "new"), _op(2, "get", "k", reply="new"), _op(3, "get", "k", reply="old")], [], {}, "k", {"new"}, [3], id="non-monotonic-reads"),
        pytest.param({"k": "old"}, [_op(1, "set", "k", "ambiguous", "new"), _op(2, "get", "k", reply="old")], [], {}, "k", {"old", "new"}, [], id="ambiguous-write-may-still-apply"),
        # The loss of a write a read saw may still materialize later.
        pytest.param({"k": "old"}, [_op(1, "set", "k", "ok", "new", t_end=99.0), _op(2, "get", "k", reply="new", t_end=101.0)], [(98.0, 100.0)], {}, "k", {"old", "new"}, [], id="read-inside-window"),
        pytest.param({"k": "old"}, [_op(1, "set", "k", "ambiguous", "new", t_end=97.0), _op(2, "get", "k", reply="new", t_end=97.5)], [(98.0, 100.0)], {}, "k", {"old", "new"}, [], id="ambiguous-before-crash"),
        pytest.param({"k": "old"}, [_op(1, "set", "k", "ok", "lossy", t_end=99.0), _op(2, "set", "k", "ok", "durable", t_end=120.0), _op(3, "set", "k", "ambiguous", "maybe", t_end=121.0), _op(4, "get", "k", reply="maybe", t_end=122.0)], [(98.0, 100.0)], {}, "k", {"maybe"}, [], id="reads-narrow-after-durable-write"),
        # A write WAIT confirmed on every replica survives a loss window; a partial confirmation does not.
        pytest.param({}, _two_writes(wait=2), _SCOPED, {"wait_confirmed": 2}, "k", {"v2"}, [], id="wait-confirmed"),
        pytest.param({}, _two_writes(wait=1), _SCOPED, {"wait_confirmed": 2}, "k", {"v1", "v2"}, [], id="wait-partial"),
        pytest.param({}, _two_writes(wait=2), _SCOPED, {}, "k", {"v1", "v2"}, [], id="wait-without-policy"),
        # The preload is acknowledged writes too.
        pytest.param({"k": "p"}, [], [(98.0, 101.0, frozenset({"valkey-0"}))], {"preload_acks": {"k": (100.0, "valkey-0")}}, "k", {"p", None}, [], id="preload-in-window"),
        pytest.param({"k": "p"}, [], [(98.0, 101.0)], {"preload_acks": {"k": (90.0, "valkey-0")}}, "k", {"p"}, [], id="preload-before-window"),
        pytest.param({"k": "p"}, [], [(98.0, 101.0, frozenset({"valkey-0"}))], {"preload_acks": {"k": (100.0, "valkey-1")}}, "k", {"p"}, [], id="preload-elsewhere"),
        # appendfsync everysec: a write acknowledged before the window has been written.
        pytest.param({"k": "preload"}, [_op(1, "set", "k", "ok", "old", t_end=97.0), _op(2, "set", "k", "ok", "new", t_end=99.0)], [(100.0 - EVERYSEC_LOSS_WINDOW_S, 100.0)], {}, "k", {"old", "new"}, [], id="everysec-window"),
    ],
)  # fmt: skip
def test_the_ledger_model_keeps_only_values_a_correct_server_may_hold(
    preload, ledger, windows, options, key, possible, violations
):
    model = LedgerModel(preload, ledger, windows, **options)
    assert model.possible[key] == possible
    assert [item["id"] for item in model.read_violations] == violations


def _evidence(evidence_id, source, **value):
    return Evidence(evidence_id, source, "action.completed", 1.0, value)


@pytest.mark.parametrize(
    ("evidence", "padding", "faults", "after_s", "windows"),
    [
        ([_evidence(FAULT_INJECTED, "kill", applied_at=100.0), _evidence(FAULT_INJECTED, "other", applied_at=50.0)], 2.0, ["kill"], 0.0, [(98.0, 100.0, [])]),
        ([_evidence(LOSS_WINDOW, "chaos", start=10.0, end=12.0, instance_id="valkey-0"), _evidence(LOSS_WINDOW, "chaos", start=20.0, end=21.0)], 0.5, None, 1.0, [(9.5, 13.0, ["valkey-0"]), (19.5, 22.0, [])]),
        ([_evidence(LOSS_WINDOW, "chaos", start=10.0, end=12.0), _evidence(LOSS_WINDOW, "other", start=30.0, end=31.0)], 0.0, ["chaos"], 0.0, [(10.0, 12.0, [])]),
        # Windows off: no write may be lost.
        ([_evidence(LOSS_WINDOW, "chaos", start=10.0, end=12.0)], 0.0, [], 0.0, []),
        # A malformed window permits no loss.
        ([_evidence(LOSS_WINDOW, "chaos", end=12.0), _evidence(LOSS_WINDOW, "chaos", start="10", end=12.0)], 0.0, None, 0.0, []),
        # A fault window runs until the fault's effect was seen to end, if that came after the heal plus after_s.
        ([_evidence(FAULT_INJECTED, "split", applied_at=100.0, healed_at=105.0), Evidence("t.demoted", "settled", "action.completed", 120.0, {})], 0.0, ["split"], 5.0, [(100.0, 120.0, [])]),
        ([_evidence(FAULT_INJECTED, "split", applied_at=100.0, healed_at=105.0), Evidence("t.demoted", "settled", "action.completed", 107.0, {})], 0.0, ["split"], 5.0, [(100.0, 110.0, [])]),
    ],
    ids=["fault-injection-time", "loss-window-evidence", "by-source", "windows-off", "malformed", "until-settled",
         "settled-early"],
)  # fmt: skip
def test_faults_and_loss_window_evidence_open_loss_windows(evidence, padding, faults, after_s, windows):
    found = fault_records(evidence, padding, faults, after_s, until=["settled"])
    assert [(start, end, list(targets)) for start, end, targets in found] == windows


# valkey.oracle.ledger


class _Pipe:
    def __init__(self, values):
        self.values, self.replies = values, []

    def execute_command(self, *args):
        self.replies.append("OK")

    def get(self, key):
        self.replies.append(self.values.get(key))

    def execute(self):
        return self.replies


class _Handle:
    """Primaries holding ``values``, two per shard, and ``replicas`` replicas; in a cluster shard 0 owns slots
    0-8191. Without ``primaries`` none answers as a primary."""

    def __init__(self, values, cluster=False, primaries=True, replicas=0):
        self.mode = SimpleNamespace(value="cluster" if cluster else "standalone")
        self.values = values
        count = 4 if cluster else 2
        slots = ((0, 8191), (8192, 16383)) if cluster else ((None, None),) * 2
        role = ("primary",) * count + ("replica",) * replicas
        self.instances = tuple(
            SimpleNamespace(instance_id=f"valkey-{i}", role=role[i], host="127.0.0.1", port=7000 + i, shard_id=i % 2, slot_start=slots[i % 2][0], slot_end=slots[i % 2][1])
            for i in range(count + replicas)
        )  # fmt: skip
        self.alive = primaries

    def roles(self):
        return {instance.instance_id: instance.role for instance in self.instances} if self.alive else {}

    def primaries(self):
        return [instance_id for instance_id, role in self.roles().items() if role == "primary"]

    def replicas(self):
        return []

    def instance(self, instance_id):
        return next(instance for instance in self.instances if instance.instance_id == instance_id)

    def new_client(self, instance_id, timeout=5.0):
        return SimpleNamespace(pipeline=lambda transaction=False: _Pipe(self.values), close=lambda: None)

    def cluster_client(self, timeout=5.0):
        return self.new_client(None)


@functools.cache
def _experiment_oracle(experiment, deployment, inputs):
    """The ``data`` oracle of ``experiment`` compiled for ``deployment`` with ``inputs`` (both as item tuples)."""

    plan = valkey_service().compile(
        {
            "scenario_id": "ledger",
            "seed": 1,
            "experiment": {"id": experiment, "inputs": dict(inputs)},
            "deployment": dict(deployment),
        }
    )
    return next(component for component in plan.components if component.id == "data")


def _component(use, component_id, role, config):
    spec = ComponentSpec(role=role, use=use, id=component_id, config=config)
    return resolved_spec(registration_for(use), spec)


def _oracle(**config):
    config = {"workload": "writes", "check_replicas": False, **config}
    return _component("valkey.oracle.ledger", "data", ComponentRole.ORACLE, config)


def _crash(**value):
    return Evidence(FAULT_INJECTED, "kill-during-save", "fault.recovered", 1.0, {"applied_at": 100.0, **value})


_PARTITION = Evidence(
    FAULT_INJECTED,
    "partition",
    "fault.recovered",
    1.0,
    {"applied_at": 100.0, "healed_at": 110.0, "targets": ["valkey-0"]},
)


def _demoted(at):
    return Evidence("valkey.poll.satisfied", "old-primary-demoted", "action.completed", at, {"valkey-0": "slave"})


def _failover(force):
    value = {"force": force, "success": True, "started_at": 100.0, "completed_at": 102.0, "primary_before": "valkey-0"}
    return Evidence("valkey.failover.result", "failover", "action.completed", 1.0, value)


def _window(instance_id="valkey-0"):
    value = {"start": 10.0, "end": 12.0, "instance_id": instance_id}
    return Evidence(LOSS_WINDOW, "chaos", "action.completed", 1.0, value)


_CLUSTER = (("mode", "cluster"), ("replicas_per_primary", 1))
# A cluster run whose primary is killed: only the killed shard's writes may be lost, within the window.
_CLUSTER_CRASH = {
    "loss_window_s": 2.5,
    "loss_window_faults": ["kill-during-save"],
    "loss_window_scope": "fault_targets",
}
_PARTITIONED = ("valkey.cluster-partition", (*_CLUSTER, ("addressing", "per_instance_loopback")), ())
_MIXED_FORCED = ("valkey.mixed-version-cluster", _CLUSTER, (("force_failover", True),))
_MIXED_GRACEFUL = ("valkey.mixed-version-cluster", _CLUSTER, ())
_WAIT = {"loss_window_scope": "fault_targets"}


@pytest.mark.parametrize(
    ("oracle", "handle", "preload", "ledger", "evidence", "summary", "status"),
    [
        # A WAIT reply that claimed both replicas had a write the promotion lost is a lie the oracle catches.
        pytest.param(_WAIT, {KEY: "v1"}, {}, _two_writes(2, KEY), [_window()], {"wait_replicas": 2}, "FAIL", id="fake-wait-reply"),
        pytest.param(_WAIT, {KEY: "v1"}, {}, _two_writes(1, KEY), [_window()], {"wait_replicas": 2}, "PASS", id="honest-wait-reply"),
        pytest.param({}, {KEY: "v1"}, {}, _two_writes(key=KEY), [_window()], {}, "PASS", id="window-allows-the-loss"),
        pytest.param({"loss_window_faults": []}, {KEY: "v1"}, {}, _two_writes(key=KEY), [_window()], {}, "FAIL", id="windows-off"),
        pytest.param({}, {KEY: "corrupt"}, {KEY: "v0"}, [], [], {}, "FAIL", id="value-nobody-wrote"),
        pytest.param({}, {}, {KEY: "v0"}, [_op(1, "set", KEY, "ok", "v1")], [], {}, "FAIL", id="acknowledged-write-lost"),
        pytest.param({"lost_writes_budget": 1}, {}, {KEY: "v0"}, [_op(1, "set", KEY, "ok", "v1")], [], {}, "PASS", id="lost-write-within-budget"),
        pytest.param({}, {KEY: "old"}, {KEY: "old"}, [_op(1, "set", KEY, "ambiguous", "new"), _op(2, "get", KEY, reply="new")], [], {}, "FAIL", id="read-value-later-lost"),
        # No primary to read from: the harness cannot tell (liveness and deployment-state judge the deployment).
        pytest.param({"converge_timeout_s": 0.2}, None, {KEY: "v"}, [_op(1, "set", KEY, "ok", "v")], [], {}, "ERROR", id="no-live-primary"),
        # Only the crashed primary's writes may be lost.
        pytest.param((_CLUSTER_CRASH,), {SHARD1_KEY: "v0"}, {SHARD1_KEY: "v0"}, [_op(1, "set", SHARD1_KEY, "ok", "v1", t_end=99.5, instance_id="valkey-1")], [_crash(target="valkey-0")], {}, "FAIL", id="crash-other-shard-lost"),
        pytest.param((_CLUSTER_CRASH,), {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=99.5, instance_id="valkey-0")], [_crash(target="valkey-0")], {}, "PASS", id="crash-own-shard-lost"),
        # Only the isolated primary's writes may be lost, and reads outside its shard stay strict.
        pytest.param(_PARTITIONED, {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=105.0, instance_id="valkey-1")], [_PARTITION], {}, "FAIL", id="partition-majority-lost"),
        pytest.param(_PARTITIONED, {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=105.0, instance_id="valkey-0")], [_PARTITION], {}, "PASS", id="partition-isolated-lost"),
        pytest.param(_PARTITIONED, {SHARD1_KEY: "v1"}, {SHARD1_KEY: "v0"}, [_op(1, "set", SHARD1_KEY, "ok", "v1", t_end=99.0), _op(2, "get", SHARD1_KEY, reply="v0", t_end=105.0)], [_PARTITION], {}, "FAIL", id="partition-stale-read-elsewhere"),
        # The isolated primary stepped down 10 s after the heal (a slow host): what it acknowledged until then may
        # be lost, past the fixed 5 s after the heal. Once it was seen demoted, the window is closed.
        pytest.param(_PARTITIONED, {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=118.0, instance_id="valkey-0")], [_PARTITION, _demoted(120.0)], {}, "PASS", id="partition-late-demotion"),
        pytest.param(_PARTITIONED, {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=118.0, instance_id="valkey-0")], [_PARTITION, _demoted(112.0)], {}, "FAIL", id="partition-loss-after-demotion"),
        # A forced failover may lose only the old primary's writes; a graceful one loses none.
        pytest.param(_MIXED_FORCED, {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=101.0, instance_id="valkey-0")], [_failover(True)], {}, "PASS", id="forced-failover-old-primary"),
        pytest.param(_MIXED_FORCED, {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=101.0, instance_id="valkey-1")], [_failover(True)], {}, "FAIL", id="forced-failover-new-primary"),
        pytest.param(_MIXED_GRACEFUL, {SHARD0_KEY: "v0"}, {SHARD0_KEY: "v0"}, [_op(1, "set", SHARD0_KEY, "ok", "v1", t_end=101.0, instance_id="valkey-0")], [_failover(False)], {}, "FAIL", id="graceful-failover"),
    ],
)  # fmt: skip
def test_the_ledger_oracle_fails_on_any_value_a_correct_server_could_not_hold(
    tmp_path, oracle, handle, preload, ledger, evidence, summary, status
):
    cluster = isinstance(oracle, tuple)
    if not cluster:
        spec = _oracle(**oracle)
    elif len(oracle) == 1:  # a cluster run's ledger config, given directly
        spec = _oracle(**oracle[0])
    else:
        spec = _experiment_oracle(*oracle)
    paths = {name: os.path.join(tmp_path, f"{name}.jsonl") for name in ("preload", "ledger")}
    write_jsonl(paths["preload"], ({"key": key, "value": value} for key, value in preload.items()))
    write_jsonl(paths["ledger"], ledger)
    ledger_summary = {"preload_path": paths["preload"], "ledger_path": paths["ledger"], **summary}
    items = [Evidence(LEDGER_EVIDENCE, "writes", "workload.quiesced", 1.0, ledger_summary), *evidence]
    replicas = summary.get("wait_replicas", 0)
    deployment = _Handle(handle or {}, cluster=cluster, primaries=handle is not None, replicas=replicas)

    result = LedgerOracle().evaluate(fake_context(deployment), spec, items)

    assert result.status.value == status, result.summary


class _FlakyHandle(_Handle):
    """A primary holding ``KEY: held`` whose first ``failures`` reads raise ``error``; and, unless ``replica`` is
    None, one replica, valkey-2, that holds ``replica`` or raises it."""

    def __init__(self, held="v1", error=None, failures=0, replica=None):
        super().__init__({KEY: held}, replicas=int(replica is not None))
        self.error, self.failures, self.replica = error, failures, replica

    def replicas(self):
        return [] if self.replica is None else ["valkey-2"]

    def new_client(self, instance_id, timeout=5.0):
        if instance_id == "valkey-2":
            value = self.replica
        elif self.failures > 0:
            self.failures, value = self.failures - 1, self.error
        else:
            return super().new_client(instance_id, timeout)
        pipe = (lambda: _FailingPipe(value)) if isinstance(value, Exception) else (lambda: _Pipe({KEY: value}))
        return SimpleNamespace(pipeline=lambda transaction=False: pipe(), close=lambda: None)


class _FailingPipe(_Pipe):
    def __init__(self, error):
        super().__init__({})
        self.error = error

    def execute(self):
        raise self.error


_ALWAYS = 10_000


@pytest.mark.parametrize(
    ("handle", "waits", "status", "summary"),
    [
        ({"error": valkey.exceptions.TimeoutError("Timeout reading from socket"), "failures": 1}, False, "PASS", "1 keys match the ledger"),
        ({"held": "corrupt", "error": valkey.exceptions.BusyLoadingError("Valkey is loading the dataset in memory"), "failures": 1}, False, "FAIL", "1 key(s) hold a value outside"),
        ({"error": valkey.exceptions.ConnectionError("Error 49 connecting to 127.0.0.1:7000. Can't assign requested address."), "failures": _ALWAYS}, True, "ERROR", "cannot read the dataset within 0.5s: ConnectionError"),
        ({"replica": "v1"}, False, "PASS", "1 keys match the ledger"),
        ({"replica": "v0"}, True, "FAIL", "replicas did not converge: valkey-2"),
        ({"replica": valkey.exceptions.ClusterDownError("CLUSTERDOWN The cluster is down")}, True, "ERROR", "cannot read replica(s) valkey-2: ClusterDownError"),
    ],
    ids=["retried-then-right", "retried-then-wrong", "never-read", "replica-converged", "replica-stale", "replica-never-read"],
)  # fmt: skip
def test_the_ledger_oracle_retries_a_failed_read_and_never_fails_valkey_for_one(
    tmp_path, monkeypatch, handle, waits, status, summary
):
    """A read error (of the primary or a replica) says the harness could not read: retried within
    converge_timeout_s, then ERROR. What it reads once it can still decides."""

    clock = VirtualClock().install(monkeypatch, instances, polling, processes)
    paths = {name: os.path.join(tmp_path, f"{name}.jsonl") for name in ("preload", "ledger")}
    write_jsonl(paths["preload"], [{"key": KEY, "value": "v0"}])
    write_jsonl(paths["ledger"], [_op(1, "set", KEY, "ok", "v1")])
    items = [Evidence(LEDGER_EVIDENCE, "writes", "workload.quiesced", 1.0, {"preload_path": paths["preload"], "ledger_path": paths["ledger"]})]  # fmt: skip
    spec = _oracle(converge_timeout_s=0.5, check_replicas="replica" in handle)
    deployment = _FlakyHandle(**handle)
    result = LedgerOracle().evaluate(clock.drive(fake_context(deployment)), spec, items)
    assert (result.status.value, result.summary.startswith(summary)) == (status, True), result.summary
    # A failed read was retried until it worked; one that never did, or a stale replica, for the whole timeout.
    assert deployment.failures == 0 or _ALWAYS - deployment.failures > 1
    assert (clock.elapsed >= 0.5) is waits


# valkey.workload.key-value: what the ledger records


@pytest.mark.parametrize(
    ("error", "outcome"),
    [
        (valkey.exceptions.OutOfMemoryError("command not allowed when used memory > 'maxmemory'."), "error"),
        (valkey.exceptions.ReadOnlyError("You can't write against a read only replica."), "error"),
        (valkey.exceptions.ResponseError("MISCONF Errors writing to the AOF file"), "error"),
        (valkey.exceptions.ClusterDownError("CLUSTERDOWN The cluster is down"), "error"),
        (valkey.exceptions.TimeoutError("Timeout reading from socket"), "ambiguous"),
        (valkey.exceptions.ConnectionError(), "ambiguous"),
        (None, "ok"),
    ],
    ids=["OOM", "READONLY", "MISCONF", "CLUSTERDOWN", "timeout", "connection", "acknowledged"],
)
def test_a_server_rejection_is_not_applied_and_a_lost_reply_is_ambiguous(error, outcome):
    def write(key, value):
        if error is not None:
            raise error
        return True

    node = SimpleNamespace(host="127.0.0.1", port=7001)
    client = SimpleNamespace(
        cluster_error_retry_attempts=3, set=write, get_node_from_key=lambda key, replica=False: node, close=lambda: None
    )
    handle = _Handle({}, cluster=True)
    handle.cluster_client = lambda timeout: client
    workload = KeyValueWorkload()
    workload._target = _Target(handle, 1.0)

    assert workload._execute(1, "set", "k", "v") == outcome
    # Only a client that makes one attempt per command reports a rejection of that attempt.
    assert client.cluster_error_retry_attempts == 0
    if outcome == "ok":
        assert workload._ledger[0]["instance_id"] == "valkey-1", "the node that acknowledged the write"
    model = LedgerModel({"k": "v0"}, workload._ledger, [])
    assert ("v" in model.possible["k"]) is (outcome != "error")


class _StoreClient:
    """A primary that stores what it is told and answers WAIT with ``wait_reply``, slowly."""

    def __init__(self, wait_reply=1):
        self.store, self.wait_reply, self.commands, self.waits_sent_at = {}, wait_reply, [], []

    def set(self, key, value):
        self.store[key] = value
        return True

    def delete(self, key):
        return int(self.store.pop(key, None) is not None)

    def get(self, key):
        return self.store.get(key)

    def pipeline(self, transaction=False):
        return SimpleNamespace(set=self.set, execute=lambda: [True] * len(self.store))

    def execute_command(self, *args):
        self.commands.append(args)
        self.waits_sent_at.append(time.time())
        time.sleep(0.01)
        return self.wait_reply

    def close(self):
        pass


def _standalone(client, primaries=("valkey-0",)):
    instances = (SimpleNamespace(instance_id="valkey-0", host="127.0.0.1", port=7000),)
    mode = SimpleNamespace(value="standalone")
    return SimpleNamespace(
        mode=mode, instances=instances, primaries=lambda: list(primaries), new_client=lambda *a, **k: client
    )


def test_a_harness_bug_in_the_workload_is_an_error_not_an_ambiguous_write(tmp_path, monkeypatch):
    from valkey_fuzzer.components import workloads

    def broken(self, key):
        raise TypeError("served_by bug")

    monkeypatch.setattr(workloads._Target, "served_by", broken)
    spec = _component(
        "valkey.workload.key-value",
        "writes",
        ComponentRole.WORKLOAD,
        {"mutable_keys": 2, "preload_mutable": False, "rate": 500},
    )
    context = fake_context(_standalone(_StoreClient()), spec, artifact_dir=str(tmp_path))
    workload = KeyValueWorkload()
    workload.start(context, spec)
    deadline = time.monotonic() + 5
    while workload._thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)
    result = workload.stop(context, spec)
    assert result.status.value == "ERROR" and "TypeError: served_by bug" in result.summary, result.summary


@pytest.mark.parametrize(("max_ops", "cancel", "status"), [(40, False, "PASS"), (10**9, True, "ERROR")])
def test_the_workload_ledger_explains_what_a_correct_server_holds(tmp_path, max_ops, cancel, status):
    client = _StoreClient()
    config = {
        "stable_keys": 3,
        "mutable_keys": 2,
        "rate": 500,
        "max_ops": max_ops,
        "drain_on_stop": True,
        "wait_replicas": 1,
    }
    spec = _component("valkey.workload.key-value", "writes", ComponentRole.WORKLOAD, config)
    context = fake_context(_standalone(client), spec, artifact_dir=str(tmp_path))
    workload = KeyValueWorkload()
    assert workload.start(context, spec).status.value == "PASS"
    if cancel:
        # A drain that cannot complete stops waiting when the run is cancelled.
        threading.Timer(0.2, context.cancel).start()
    started = time.monotonic()
    result = workload.stop(context, spec)
    assert result.status.value == status, result.summary
    assert time.monotonic() - started < 5
    preload, ledger, acks, summary = load_ledger(result.evidence, "writes")
    # Every preloaded key is an acknowledged write, and the WAIT reply of every write is recorded.
    assert set(acks) == set(preload) and len(preload) == 5
    assert all(("wait" in entry) is (entry["op"] != "get") for entry in ledger if entry["outcome"] == "ok")
    # The write's reply ends it, before its WAIT is sent.
    waited = [entry for entry in ledger if "wait" in entry]
    assert waited and all(entry["t_end"] <= sent for entry, sent in zip(waited, client.waits_sent_at))
    model = LedgerModel(preload, ledger, [], preload_acks=acks)
    assert all(client.store.get(key) in model.possible[key] for key in model.possible)
    assert summary["drained"] is not cancel and (cancel or summary["operations"] == max_ops)


def test_a_workload_whose_worker_did_not_stop_leaves_its_state_to_it(tmp_path):
    spec = _component("valkey.workload.key-value", "writes", ComponentRole.WORKLOAD, {"mutable_keys": 2, "rate": 500})
    context = fake_context(_standalone(_StoreClient()), spec, artifact_dir=str(tmp_path))
    workload = KeyValueWorkload()
    workload.start(context, spec)
    thread = workload._thread
    workload._thread = SimpleNamespace(
        stop=lambda timeout: "thread writes did not stop within 30s", is_alive=lambda: True
    )
    try:
        result = workload.stop(context, spec)
    finally:
        thread.stop()
    assert result.status.value == "ERROR" and "did not stop within 30s" in result.summary
    # A worker still running owns its ledger: none was written from it.
    assert result.evidence == () and not os.listdir(tmp_path)


def test_a_write_that_reached_no_server_was_not_applied():
    # Mid-failover there may be no reachable primary: nothing was sent, so the write is rejected, not ambiguous.
    workload = KeyValueWorkload()
    workload._target = _Target(_standalone(_StoreClient(), primaries=()), 1.0)
    assert workload._execute(1, "set", "k", "v") == "error"
    assert "no reachable primary" in workload._ledger[0]["error"]


def test_a_replica_is_checked_for_the_keys_its_view_gives_its_primary():
    nodes = (
        "p0 127.0.0.1:7000@17000 master - 0 0 1 connected 0-8191\n"
        "p1 127.0.0.1:7002@17002 master - 0 0 2 connected 8192-16383 [9000->-p0]\n"
        "r1 127.0.0.1:7003@17003 slave p1 0 0 2 connected\n"
    )

    def keys_of(view):
        client = SimpleNamespace(execute_command=lambda *args: view)
        handle = SimpleNamespace(new_client=lambda instance_id, timeout: nullcontext(client))
        return _keys_owned_by_primary_of(handle, "valkey-3", [SHARD0_KEY, SHARD1_KEY])

    assert keys_of(nodes.replace("slave p1", "myself,slave p1")) == [SHARD1_KEY]
    assert keys_of(nodes.replace("7000@17000 master", "7000@17000 myself,master")) == [SHARD0_KEY], "a primary's own"
    assert keys_of(nodes) == [], "a view without myself names no primary"
