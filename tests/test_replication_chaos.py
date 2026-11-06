"""valkey.action.replication-chaos and the wave engine on a fake standalone deployment: promotions and their loss
windows, conflict keys, undo obligations through a run, and valkey.action.replicaof."""

import contextvars
import signal
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import valkey

from tests.support import VirtualClock
from tests.test_valkey_components_faults import Iptables
from tests.valkey_experiments_support import (
    chaos_operation,
    chaos_schedule,
    evidence_values,
    fake_context,
    resolved,
    validation_result,
)
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.components import actions, instances, network, polling, processes, replication_chaos
from valkey_fuzzer.components.actions import ReplicaofAction
from valkey_fuzzer.components.ledger import LOSS_WINDOW
from valkey_fuzzer.components.replication_chaos import ReplicationChaosAction, StandaloneWaves
from valkey_fuzzer.plan import ComponentSpec, ExperimentSpec, RunMode
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import ComponentStatus
from valkey_fuzzer.testing import run_experiment


class FakeStandalone:
    """A primary (valkey-0) and ``replicas`` replicas whose replication the fake clients report.

    ``record_dead=False`` is a deployment that forgets the new role of a stopped instance; ``fail`` maps a
    signal or command (``"SIGCONT"``, ``"restart"``, ``"CONFIG SET repl-compression no"``) to how many times it
    fails before it works.
    """

    def __init__(self, replicas=2, *, failover_works=True, record_dead=True, fail=None):
        self.mode = SimpleNamespace(value="standalone")
        self.instances = tuple(
            SimpleNamespace(instance_id=f"valkey-{n}", host="127.0.0.1", port=7000 + n, shard_id=0,
                            role="replica" if n else "primary")
            for n in range(replicas + 1)
        )  # fmt: skip
        ids = [item.instance_id for item in self.instances]
        self.alive, self.stopped = dict.fromkeys(ids, True), dict.fromkeys(ids, False)
        # Live upstream; ``recorded`` is what a restart starts the instance as.
        self.upstream = {item: None if item == "valkey-0" else "valkey-0" for item in ids}
        self.recorded = dict(self.upstream)
        self.offset, self.replid = dict.fromkeys(ids, 1000), dict.fromkeys(ids, "a" * 40)
        self.config = {item: {"repl-compression": "no", "repl-backlog-size": "1048576"} for item in ids}
        self.failover_works, self.record_dead, self.fail = failover_works, record_dead, dict(fail or {})
        self.commands, self.exit_on_its_own = [], None
        self.lock = threading.RLock()

    def _refuse(self, what):
        if self.fail.get(what):
            self.fail[what] -= 1
            raise OSError(f"{what} refused")

    def reachable(self, instance_id):
        return self.alive[instance_id] and not self.stopped[instance_id]

    def roles(self):
        with self.lock:
            return {
                item: "replica" if self.upstream[item] else "primary" for item in self.alive if self.reachable(item)
            }

    refresh_roles = roles

    def primaries(self):
        return [item for item, role in self.roles().items() if role == "primary"]

    def replicas(self):
        return [item for item, role in self.roles().items() if role == "replica"]

    def instance(self, instance_id):
        return next(item for item in self.instances if item.instance_id == instance_id)

    def is_alive(self, instance_id):
        if self.exit_on_its_own == instance_id:
            self.alive[instance_id] = False
        return self.alive[instance_id]

    def send_signal(self, instance_id, signum):
        self._refuse(signal.Signals(signum).name)
        with self.lock:
            if not self.alive[instance_id]:
                raise ProcessLookupError(instance_id)
            if signum in (signal.SIGKILL, signal.SIGTERM):
                self.alive[instance_id] = False
            elif signum in (signal.SIGSTOP, signal.SIGCONT):
                self.stopped[instance_id] = signum == signal.SIGSTOP

    def wait_exit(self, instance_id, timeout=10.0):
        return not self.alive[instance_id]

    def _follow(self, instance_id, primary):
        self.upstream[instance_id] = self.recorded[instance_id] = primary
        if primary is not None:
            self.offset[instance_id], self.replid[instance_id] = self.offset[primary], self.replid[primary]

    def replicate(self, instance_id, primary_id, timeout=5.0):
        with self.lock:
            if not self.alive[instance_id]:
                if self.record_dead:
                    self.recorded[instance_id] = primary_id
                return
            if self.stopped[instance_id]:
                raise TimeoutError(f"{instance_id} is stopped")
            self._follow(instance_id, primary_id)
            if primary_id is None:
                self.replid[instance_id] = "b" * 40

    def restart(self, instance_id, wait_ready=True, ready_timeout=30.0):
        if instance_id == "valkey-2":
            self._refuse("restart")
        with self.lock:
            self.alive[instance_id], self.stopped[instance_id] = True, False
            self._follow(instance_id, self.recorded[instance_id])
            if self.recorded[instance_id] is None:
                self.offset[instance_id] = 0  # an empty restarted primary

    def new_client(self, instance_id, timeout=5.0):
        client = Mock()
        client.__enter__ = Mock(return_value=client)
        client.__exit__ = Mock(return_value=False)
        client.info.side_effect = lambda section=None: self.info(instance_id)
        client.execute_command.side_effect = lambda *args: self.execute(instance_id, args)
        return client

    def info(self, instance_id):
        with self.lock:
            if not self.reachable(instance_id):
                raise ConnectionError(f"{instance_id} is unreachable")
            upstream = self.upstream[instance_id]
            if upstream is None:
                return {"role": "master", "master_replid": self.replid[instance_id],
                        "master_repl_offset": self.offset[instance_id]}  # fmt: skip
            linked = self.reachable(upstream) and self.upstream[upstream] is None
            return {"role": "slave", "master_host": "127.0.0.1", "master_port": self.instance(upstream).port,
                    "master_link_status": "up" if linked else "down", "master_replid": self.replid[instance_id],
                    "slave_repl_offset": self.offset[instance_id]}  # fmt: skip

    def execute(self, instance_id, args):
        name = " ".join(str(arg) for arg in args).upper()
        if name == "CONFIG SET REPL-COMPRESSION NO":
            self._refuse("CONFIG SET repl-compression no")
        with self.lock:
            if not self.reachable(instance_id):
                raise ConnectionError(f"{instance_id} is unreachable")
            self.commands.append((instance_id, *args))
            if name.startswith("FAILOVER TO") and self.failover_works:
                target = f"valkey-{int(args[3]) - 7000}"
                self.upstream[target], self.replid[target], self.upstream[instance_id] = None, "c" * 40, target
            elif name.startswith("CONFIG GET"):
                return [args[2], self.config[instance_id].get(args[2], "")]
            elif name.startswith("CONFIG SET"):
                self.config[instance_id][args[2]] = str(args[3])
            return "OK"

    # The state check: one primary, every other instance linked to it.
    config_check = SimpleNamespace(blocking_on_failure=False, check_data_consistency=True)

    def state_check(self, validation=None, *, seed=None):
        return SimpleNamespace(config=self.config_check, write_test_keys=lambda: {"k": "v"}, validate=self._validate)

    def _validate(self, operations=None, *, retry=True):
        primaries = self.primaries()
        if len(primaries) != 1 or any(self.upstream[item] != primaries[0] for item in self.replicas()):
            return validation_result("roles")
        return validation_result()


def run_chaos(handle, waves, seed=7, clock=None):
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.ACTION, use="valkey.action.replication-chaos", id="chaos", config={"waves": waves}
        )
    )
    context = fake_context(handle, spec, seed=seed, deployment={"replicas_per_primary": len(handle.instances) - 1})
    if clock is not None:
        clock.drive(context)
    return ReplicationChaosAction().run(context, spec), context


# ---------------------------------------------------------------------------
# Promotions, kills, held faults, and the loss windows they open
# ---------------------------------------------------------------------------


def test_promotions_move_the_primary_and_only_forced_ones_open_a_loss_window_until_their_demotion():
    handle = FakeStandalone()
    result, _ = run_chaos(handle, [[{"op": "promote", "target": "valkey-1", "mode": "graceful"}]])
    assert result.status is ComponentStatus.PASS, result.summary
    assert handle.upstream == {"valkey-0": "valkey-1", "valkey-1": None, "valkey-2": "valkey-1"}
    assert evidence_values(result.evidence, LOSS_WINDOW) == []
    assert ("valkey-0", "FAILOVER", "TO", "127.0.0.1", 7001, "TIMEOUT", 10000) in handle.commands
    # Validation ran with writes paused.
    assert ("valkey-1", "CLIENT", "PAUSE", 120000, "WRITE") in handle.commands

    handle = FakeStandalone()
    waves = [[{"op": "promote", "target": target, "mode": "force"}] for target in ("valkey-1", "valkey-0")]
    result, _ = run_chaos(handle, [*waves, [{"op": "promote", "target": "valkey-2", "mode": "force", "split_s": 0.2}]])
    assert result.status is ComponentStatus.PASS, result.summary
    first, _, third = windows = evidence_values(result.evidence, LOSS_WINDOW)
    assert [window["instance_id"] for window in windows] == ["valkey-0", "valkey-1", "valkey-0"]
    # valkey-0 was demoted again in wave 3; its wave-1 window still ends in wave 1.
    assert first["end"] < chaos_operation(result, "2.0")["promoted_at"]
    # From the last sample valkey-2 had reached until the old primary was repointed after the split.
    promoted_at = chaos_operation(result, "3.0")["promoted_at"]
    assert third["start"] <= promoted_at and third["end"] >= promoted_at + 0.2


def test_a_killed_primary_is_replaced_by_the_most_advanced_replica_and_restarts_as_its_replica():
    handle = FakeStandalone()
    handle.offset["valkey-2"] = 900  # behind valkey-1
    result, _ = run_chaos(handle, [[{"op": "kill", "target": "primary"}]])
    assert result.status is ComponentStatus.PASS, result.summary
    [promotion] = chaos_schedule(result)["heal_promotions"]
    assert (promotion["from"], promotion["to"]) == ("valkey-0", "valkey-1")
    assert handle.alive["valkey-0"] and handle.upstream["valkey-0"] == "valkey-1"
    [window] = evidence_values(result.evidence, LOSS_WINDOW)
    assert window["instance_id"] == "valkey-0" and window["end"] == chaos_schedule(result)["all_kills"][0]["exited_at"]
    # Every instance is up again, so none is expected down.
    assert chaos_schedule(result)["kills"] == [] and evidence_values(
        result.evidence, "valkey.instances.expected-down"
    ) == [{"instances": []}]


def test_a_heal_promotion_whose_replicaof_timed_out_after_it_ran_counts():
    class TimesOutAfterPromoting(FakeStandalone):
        def replicate(self, instance_id, primary_id, timeout=5.0):
            super().replicate(instance_id, primary_id, timeout)
            if primary_id is None:
                raise TimeoutError("Timeout reading from socket")

    handle = TimesOutAfterPromoting()
    result, _ = run_chaos(handle, [[{"op": "kill", "target": "primary"}]])
    assert result.status is ComponentStatus.PASS, result.summary
    assert [promotion["to"] for promotion in chaos_schedule(result)["heal_promotions"]] == ["valkey-1"]


def test_a_forced_promotion_skips_a_replica_restarted_in_its_wave_until_it_resynced():
    class RestartsUnsynced(FakeStandalone):
        """valkey-1's first sync after a restart takes half a second."""

        synced_at = 0.0

        def restart(self, instance_id, **options):
            super().restart(instance_id, **options)
            self.synced_at = time.monotonic() + 0.5

        def info(self, instance_id):
            reply = super().info(instance_id)
            syncing = instance_id == "valkey-1" and time.monotonic() < self.synced_at
            return {**reply, "master_link_status": "down"} if syncing else reply

    waves = [[{"op": "kill", "target": "valkey-1"}, {"op": "restart", "target": "killed", "delay_s": 0.05},
              {"op": "promote", "target": "valkey-1", "mode": "force", "delay_s": 0.1}]]  # fmt: skip
    result, _ = run_chaos(RestartsUnsynced(), waves)
    promote = chaos_operation(result, "1.2")
    assert promote["refused"] == "valkey-1 is restarted in this wave and not resynced yet" and not promote["success"]
    # The restart that left it unsynced excuses the refusal.
    assert promote["excused_by"] and evidence_values(result.evidence, LOSS_WINDOW) == []


def test_held_faults_and_a_restart_are_undone_within_the_wave():
    handle = FakeStandalone()
    waves = [
        [{"op": "stall", "target": "valkey-1", "duration_s": 0.05},
         {"op": "compression", "target": "all", "value": "lz4", "duration_s": 0.05},
         {"op": "backlog_pressure", "duration_s": 0.05}, {"op": "repoint", "target": "valkey-2", "to": "valkey-1"}],
        [{"op": "kill", "target": "valkey-2"}, {"op": "restart", "target": "killed", "delay_s": 0.1}],
        # A promotion of an instance the wave killed is refused and excused.
        [{"op": "kill", "target": "valkey-1"}, {"op": "promote", "target": "valkey-1", "delay_s": 0.1},
         {"op": "stall", "target": "valkey-1", "delay_s": 0.1}],
    ]  # fmt: skip
    result, _ = run_chaos(handle, waves)
    assert result.status is ComponentStatus.PASS, result.summary
    assert not any(handle.stopped.values())
    assert {config["repl-compression"] for config in handle.config.values()} == {"no"}
    assert handle.config["valkey-0"]["repl-backlog-size"] == "1048576"
    # The heal pointed the chained replica back at the primary.
    assert handle.upstream["valkey-2"] == "valkey-0"
    assert chaos_operation(result, "2.1")["success"] and chaos_operation(result, "2.1")["target"] == "valkey-2"
    assert chaos_operation(result, "3.1")["refused"] == "valkey-1 is killed"
    assert chaos_operation(result, "3.1")["excused_by"] == [
        {"operation": "3.0", "op": "kill", "instances": ["valkey-1"]}
    ]
    assert chaos_operation(result, "3.2")["skipped"] == "valkey-1 was killed in this wave"


def test_a_waves_operations_run_in_the_calling_context_so_a_cancelled_call_stops_them():
    # The runner finds the component call, its deadline, and its cancellation through the calling context.
    # A wave's operations run inside it: on pool threads outside it, a cancelled run would sleep through them.
    call = contextvars.ContextVar("call", default=None)
    handle, seen = FakeStandalone(), []
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.replication-chaos", id="chaos",
                      config={"waves": [[{"op": "stall", "target": "valkey-1", "delay_s": 0.01, "duration_s": 0.01},
                                         {"op": "stall", "target": "valkey-2", "delay_s": 0.01, "duration_s": 0.01}]]})
    )  # fmt: skip
    context = fake_context(handle, spec, deployment={"replicas_per_primary": 2})
    sleep = context.sleep
    context.sleep = lambda seconds: seen.append(call.get()) or sleep(seconds)
    call.set("the chaos action's call")

    assert ReplicationChaosAction().run(context, spec).status is ComponentStatus.PASS
    assert seen and set(seen) == {"the chaos action's call"}


def test_replicaof_moves_an_instance_and_fails_when_the_link_never_comes_up(monkeypatch):
    handle, clock = FakeStandalone(replicas=1), VirtualClock().install(monkeypatch, instances, polling, processes)

    def replicaof(**config):
        spec = resolved(ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.replicaof", id="r", config=config))
        return ReplicaofAction().run(clock.drive(fake_context(handle, spec)), spec)

    assert replicaof(instance="valkey-1", primary=None, timeout_s=1).status is ComponentStatus.PASS
    assert sorted(handle.primaries()) == ["valkey-0", "valkey-1"]
    result = replicaof(instance="valkey-1", primary="valkey-0", timeout_s=1)
    assert result.status is ComponentStatus.PASS and handle.upstream["valkey-1"] == "valkey-0"
    value = to_plain_data(result.evidence[0].value)
    assert (value["instance_id"], value["primary_id"]) == ("valkey-1", "valkey-0") and value["took_effect_at"]
    handle.stopped["valkey-0"] = True
    waited = clock.elapsed
    assert replicaof(instance="valkey-1", primary="valkey-0", timeout_s=0.3).status is ComponentStatus.FAIL
    assert clock.elapsed - waited >= 0.3, "it waited out timeout_s for the link"
    # A link to the primary's port on another address is not a link to the primary.
    handle = FakeStandalone(replicas=1)
    handle.instances[0].host = "127.0.0.2"
    result = replicaof(instance="valkey-1", primary="valkey-0", timeout_s=0.3)
    assert result.status is ComponentStatus.FAIL and "not linked to valkey-0" in result.summary


def test_replicaof_that_cannot_read_the_instance_is_an_error_not_a_failure(monkeypatch):
    class Unreadable(FakeStandalone):
        def info(self, instance_id):
            if instance_id == "valkey-1":
                raise valkey.exceptions.ConnectionError("Connection reset by peer")
            return super().info(instance_id)

    handle, clock = Unreadable(replicas=1), VirtualClock().install(monkeypatch, instances, polling, processes)
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.replicaof", id="r",
                      config={"instance": "valkey-1", "primary": None, "timeout_s": 0.3})
    )  # fmt: skip
    result = ReplicaofAction().run(clock.drive(fake_context(handle, spec)), spec)
    assert result.status is ComponentStatus.ERROR, result.summary
    assert "cannot tell whether valkey-1 was a primary" in result.summary and clock.elapsed >= 0.3


def _exits_by_itself():
    handle = FakeStandalone()
    handle.exit_on_its_own = "valkey-2"
    return handle


@pytest.mark.parametrize(
    ("handle", "waves", "status", "summary"),
    [
        # Known-bad: a deployment that does not record a stopped instance's new role brings it back as a primary.
        (lambda: FakeStandalone(record_dead=False), [[{"op": "kill", "target": "primary"}]], "FAIL",
         "validation failed (roles)"),
        (lambda: FakeStandalone(replicas=1),
         [[{"op": "kill", "target": "valkey-1"}, {"op": "kill", "target": "valkey-1", "delay_s": 0.1}]], "ERROR",
         "had no target"),
        (lambda: FakeStandalone(replicas=1),
         [[{"op": "promote", "target": "valkey-1"}], [{"op": "promote", "target": "valkey-1"}]], "ERROR",
         "wave 2 promote 2.0 had no target: no live valkey-1 to promote; wave 2 ran no operation"),
        (_exits_by_itself, [[{"op": "stall", "target": "valkey-1", "duration_s": 0.05}]], "FAIL",
         "valkey-2 exited without being killed"),
        (lambda: FakeStandalone(replicas=1), [[{"op": "partition", "target": "replica"}]], "ERROR",
         "partition operations need Linux, root, and iptables (network partitions require root)"),
    ],
    ids=["second-primary", "every-target-killed", "wave-ran-nothing", "exited-by-itself", "no-iptables"],
)  # fmt: skip
def test_replication_chaos_that_breaks_or_runs_nothing_does_not_pass(monkeypatch, handle, waves, status, summary):
    monkeypatch.setattr(replication_chaos, "iptables_supported", lambda: (False, "network partitions require root"))
    result, _ = run_chaos(handle(), waves)
    assert result.status.value == status and summary in result.summary, result.summary


# ---------------------------------------------------------------------------
# Conflict keys: overlapping faults on one state are serialized
# ---------------------------------------------------------------------------

# The second flip starts while the first holds and would end after it.
_FLIPS = [
    {"op": "compression", "target": "valkey-0", "value": "lz4", "duration_s": 0.1},
    {"op": "compression", "target": "valkey-0", "value": "yes", "duration_s": 0.1, "delay_s": 0.05},
]


def test_overlapping_faults_on_one_instance_are_serialized(monkeypatch):
    handle = FakeStandalone(replicas=1)
    result, _ = run_chaos(handle, [_FLIPS])
    assert result.status is ComponentStatus.PASS, result.summary
    first, second = chaos_operation(result, "1.0"), chaos_operation(result, "1.1")
    # The second waited for the first's undo, so it saved the original value.
    assert second["waited_for"] == ["1.0"] and second["applied_at"] >= first["undone_at"]
    assert handle.config["valkey-0"]["repl-compression"] == "no"

    # A kill waits for a stall to be undone, then kills a running process that the heal restarts.
    handle = FakeStandalone()
    waves = [[{"op": "stall", "target": "valkey-1", "duration_s": 0.15},
              {"op": "kill", "target": "valkey-1", "delay_s": 0.05}]]  # fmt: skip
    result, _ = run_chaos(handle, waves)
    assert result.status is ComponentStatus.PASS, result.summary
    assert not any(handle.stopped.values()) and handle.alive["valkey-1"]
    assert (
        chaos_operation(result, "1.1")["waited_for"] == ["1.0"]
        and chaos_operation(result, "1.1")["undone_by"] == "heal"
    )

    # Known-bad: without conflict keys the second flip saved the first's lz4 and restored it last.
    monkeypatch.setattr(StandaloneWaves, "claim", lambda self, record, owns: True)
    handle = FakeStandalone(replicas=1)
    run_chaos(handle, [_FLIPS])
    assert handle.config["valkey-0"]["repl-compression"] == "lz4"


# ---------------------------------------------------------------------------
# Undo obligations: an undo the harness could not complete is ERROR, whatever later waves show
# ---------------------------------------------------------------------------

_RESTORE = "CONFIG SET repl-compression no"


@pytest.mark.parametrize(
    ("refusals", "undone_by", "summary"),
    [
        (1, "heal", "wave 1 compression 1.0 could not be undone (OSError: CONFIG SET repl-compression no refused)"),
        (2, "cleanup", "wave 1: could not undo the server.config on valkey-0"),
        (3, None, "the changes of operation(s) ['1.0'] are still in place after the final cleanup"),
    ],
)
def test_an_undo_that_failed_keeps_the_run_an_error(refusals, undone_by, summary):
    handle = FakeStandalone(replicas=1, fail={_RESTORE: refusals})
    result, _ = run_chaos(handle, [[_FLIPS[0]], [{"op": "stall", "target": "replica", "duration_s": 0.05}]])
    assert result.status is ComponentStatus.ERROR and summary in result.summary, result.summary
    value = chaos_schedule(result)
    assert chaos_operation(result, "1.0").get("undone_by") == undone_by
    assert value["still_active"] == ([] if undone_by else ["1.0"])
    if undone_by == "heal":
        # The later wave validated, and the run is still an error.
        assert [wave["validation"]["overall_success"] for wave in value["waves"]] == [True, True]
    else:
        assert value["halted_after_wave"] == 1 and value["waves"][0]["still_active"] == ["1.0"]


_STALL = [[{"op": "stall", "target": "valkey-1", "duration_s": 0.05}]]


@pytest.mark.parametrize(
    ("fail", "waves", "active"),
    [
        ({}, _STALL, []),
        ({"SIGCONT": 2}, _STALL, []),
        ({"SIGCONT": 9}, _STALL, ["1.0"]),
        ({"SIGCONT": 9}, [[{"op": "stall", "target": "valkey-1", "duration_s": 0.05},
                           {"op": "compression", "target": "valkey-0", "value": "lz4", "duration_s": 0.05}]], ["1.0"]),
        ({"restart": 99}, [[{"op": "kill", "target": "valkey-2"}]], ["1.0"]),
        ({}, [[{"op": "kill", "target": "valkey-2"}]], []),
    ],
    ids=["healed", "healed-by-the-heal", "stall-stuck", "stall-stuck-config-healed", "restart-stuck", "kill-healed"],
)  # fmt: skip
def test_the_still_active_operations_are_exactly_the_open_obligations(fail, waves, active):
    result, context = run_chaos(FakeStandalone(fail=fail), waves)
    value = chaos_schedule(result)
    records = {record["index"]: record for record in value["operations"]}
    still_open = context.obligations.report()["still_active"]
    assert value["still_active"] == active and all(item["owner"] == "chaos" for item in still_open)
    assert sorted(item["target"] for item in still_open) == sorted(records[index]["target"] for index in active)
    assert set(value["waves"][-1]["still_active"]) >= set(active)
    # Only a change its operation undid on the first try passes.
    assert (result.status is ComponentStatus.PASS) is all("undo_error" not in record for record in records.values())


# ---------------------------------------------------------------------------
# Obligations a chaos run left open, through the runner
# ---------------------------------------------------------------------------


def run_planned(handle, waves=_STALL, **deployment):
    experiment = ExperimentSpec(
        experiment_id="tests.chaos",
        mode=RunMode.SMOKE,
        deployment_modes=("standalone",),
        components=(ComponentSpec(use="valkey.action.replication-chaos", id="chaos", config={"waves": waves}),),
    )
    return run_experiment(
        experiment,
        replication_chaos.REGISTRATIONS,
        deployment={"replicas_per_primary": len(handle.instances) - 1, **deployment},
        services={"valkey.deployment": handle},
    )


def test_a_stall_whose_undo_and_heal_fail_leaves_its_obligation_to_the_runner_and_errors():
    # The stall's own undo, the heal, the final cleanup, and both runner sweeps all fail.
    handle = FakeStandalone(fail={"SIGCONT": 10})
    outcome = run_planned(handle)
    assert outcome.status.value == "ERROR" and handle.stopped["valkey-1"]
    assert outcome.summary.startswith("component 'chaos' did not undo its process.paused on valkey-1")
    [left] = to_plain_data(outcome.diagnostics["obligations"])["still_active"]
    assert (left["owner"], left["kind"], left["target"]) == ("chaos", "process.paused", "valkey-1")
    assert [error["by"] for error in left["errors"]] == ["owner", "owner", "owner", "runner", "runner"]


def test_a_partition_whose_heal_fails_is_an_error_though_the_final_cleanup_removed_it(monkeypatch):
    iptables = Iptables(fail={("-D", 1): "resource busy", ("-D", 2): "resource busy"})
    monkeypatch.setattr(network, "run_iptables", iptables)
    monkeypatch.setattr(replication_chaos, "iptables_supported", lambda: (True, ""))
    waves = [[{"op": "partition", "target": "valkey-1", "duration_s": 0.05}]]
    outcome = run_planned(FakeStandalone(), waves, addressing="per_instance_loopback")
    assert outcome.status.value == "ERROR" and not iptables.chains and not iptables.jumps
    # Another process's leaked chains are the sweep's to find and report, not this run's to delete.
    assert ("-S",) not in iptables.calls
    [left] = to_plain_data(outcome.diagnostics["open_obligations"])
    assert (left["owner"], left["kind"], left["undone_by"]) == ("chaos", "network.partition", "owner")
    assert [error["by"] for error in left["errors"]] == ["owner", "owner"]
    [value] = evidence_values(outcome.all_evidence, "valkey.chaos.schedule")
    assert value["waves"][0]["still_active"] == ["1.0"] and value["still_active"] == []
    assert value["operations"][0]["undone_by"] == "cleanup"


# ---------------------------------------------------------------------------
# valkey.action.rolling-upgrade's standalone failover
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("failover_works", "error"), [(True, None), (False, "did not complete within 1")])
def test_a_rolling_upgrade_fails_over_within_its_step_timeout_and_aborts_after(monkeypatch, failover_works, error):
    from valkey_fuzzer.components.actions import RollingUpgradeAction

    handle, clock = (
        FakeStandalone(replicas=2, failover_works=failover_works),
        VirtualClock().install(monkeypatch, instances, polling, processes),
    )
    config = {"to_binary": "/new/valkey-server", "step_timeout_s": 1}
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.rolling-upgrade", id="u", config=config)
    )
    steps = []

    def failover():
        context = clock.drive(fake_context(handle, spec))
        RollingUpgradeAction._failover(
            context, spec, handle, "valkey-0", ["valkey-1", "valkey-2"], False, spec.config, steps
        )

    if error is not None:
        with pytest.raises(RuntimeError, match=error):
            failover()
        assert handle.commands[-1] == ("valkey-0", "FAILOVER", "ABORT") and clock.elapsed >= 1
        return
    failover()
    assert ("valkey-0", "FAILOVER", "TO", "127.0.0.1", 7001, "TIMEOUT", 1000) in handle.commands
    # The other replica follows the new primary, and the deployment recorded it.
    assert handle.upstream == {"valkey-0": "valkey-1", "valkey-1": None, "valkey-2": "valkey-1"}
    assert handle.recorded["valkey-2"] == "valkey-1" and steps[0]["kind"] == "failover"


class _FailoverToRaises(FakeStandalone):
    """``FAILOVER TO`` raises ``error`` and changes nothing."""

    def __init__(self, error, **options):
        super().__init__(failover_works=False, **options)
        self.error = error

    def execute(self, instance_id, args):
        if args[:2] == ("FAILOVER", "TO"):
            raise self.error
        return super().execute(instance_id, args)


@pytest.mark.parametrize(
    ("error", "unknown"),
    [(valkey.exceptions.ConnectionError("Connection reset by peer"), True),
     (valkey.exceptions.TimeoutError("Timeout reading from socket"), True),
     (valkey.exceptions.ResponseError("ERR FAILOVER target replica is not online"), False)],
    ids=["no-reply", "reply-timed-out", "valkey-refused"],
)  # fmt: skip
def test_a_graceful_failover_without_a_reply_is_unknown_and_one_valkey_refused_failed(monkeypatch, error, unknown):
    # A FAILOVER TO that got no reply may never have reached Valkey: roles that did not swap say nothing then.
    handle = _FailoverToRaises(error, replicas=1)
    clock = VirtualClock().install(monkeypatch, instances, polling, processes)
    context = clock.drive(fake_context(handle))
    done, reported = instances.graceful_failover(context, handle, "valkey-0", "valkey-1", 0.3)
    assert done is (None if unknown else False) and reported == f"{type(error).__name__}: {error}"

    waves = [[{"op": "promote", "target": "valkey-1", "timeout_s": 0.3}]]
    result, _ = run_chaos(_FailoverToRaises(error), waves, clock=clock)
    expected = "could not be observed" if unknown else "failed without an interfering operation"
    assert result.status is (ComponentStatus.ERROR if unknown else ComponentStatus.FAIL), result.summary
    assert f"promote 1.0 {expected}: {type(error).__name__}" in result.summary


def test_a_graceful_failover_whose_roles_cannot_be_read_at_the_end_is_unknown(monkeypatch):
    class PrimaryGoesQuiet(FakeStandalone):
        def execute(self, instance_id, args):
            reply = super().execute(instance_id, args)
            if args[:2] == ("FAILOVER", "TO"):
                self.stopped["valkey-0"] = True
            return reply

    handle = PrimaryGoesQuiet(replicas=1, failover_works=False)
    clock = VirtualClock().install(monkeypatch, instances, polling, processes)
    done, error = instances.graceful_failover(clock.drive(fake_context(handle)), handle, "valkey-0", "valkey-1", 0.3)
    assert (done, error) == (None, None)


@pytest.mark.parametrize(
    ("handle", "harness"),
    [(lambda: _FailoverToRaises(valkey.exceptions.ConnectionError("Connection reset by peer")), True),
     (lambda: FakeStandalone(failover_works=False), False)],
    ids=["no-reply", "valkey-did-not-fail-over"],
)  # fmt: skip
def test_a_rolling_upgrade_failover_the_harness_could_not_see_is_no_finding(monkeypatch, handle, harness):
    handle, clock = handle(), VirtualClock().install(monkeypatch, instances, polling, processes)
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.rolling-upgrade", id="u",
                      config={"to_binary": "/new/valkey-server", "step_timeout_s": 1})
    )  # fmt: skip
    with pytest.raises(RuntimeError, match="did not complete within 1") as raised:
        actions.RollingUpgradeAction._failover(
            clock.drive(fake_context(handle, spec)), spec, handle, "valkey-0", ["valkey-1", "valkey-2"], False,
            spec.config, [],
        )  # fmt: skip
    # A plain RuntimeError is the harness's (ERROR); an _UpgradeFailedError is Valkey's (FAIL).
    assert isinstance(raised.value, actions._UpgradeFailedError) is not harness


@pytest.mark.parametrize(
    ("roles", "raised"),
    [({"valkey-1": "primary", "valkey-0": "replica"}, None),
     ({"valkey-1": None, "valkey-0": None}, RuntimeError),
     ({"valkey-1": "primary", "valkey-0": None}, RuntimeError),
     # One that answered with another role decides it, whatever the other's read.
     ({"valkey-1": "replica-syncing", "valkey-0": None}, actions._UpgradeFailedError)],
    ids=["took-effect", "never-read", "half-read", "seen-wrong"],
)  # fmt: skip
def test_a_rolling_upgrade_wait_fails_only_on_roles_valkey_reported(monkeypatch, roles, raised):
    clock = VirtualClock().install(monkeypatch, polling)
    monkeypatch.setattr(actions, "instance_role", lambda handle, instance_id: roles[instance_id])
    context = clock.drive(fake_context(FakeStandalone()))
    wait = lambda: actions._wait(context, None, {"valkey-1": "primary", "valkey-0": "replica"}, 1, "no failover")  # noqa: E731
    if raised is None:
        wait()
        return
    with pytest.raises(raised, match="no failover") as error:
        wait()
    assert type(error.value) is raised and clock.elapsed == pytest.approx(1, abs=0.2)


def test_a_rolling_upgrade_restart_on_another_build_is_the_harnesss_error(monkeypatch):
    # The harness set the binary and restarted the instance: a process on another build is no finding.
    handle = DropsOneSigterm()
    monkeypatch.setattr(actions, "instance_identity", lambda handle, instance_id: {"version": "8.0.0"})
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.rolling-upgrade", id="u",
                      config={"to_binary": "/new/valkey-server", "settle_s": 0})
    )  # fmt: skip
    config = {**spec.config, "target": {"version": "9.0.0"}}
    with pytest.raises(RuntimeError, match=r"restarted on 8\.0\.0, expected 9\.0\.0") as raised:
        actions.RollingUpgradeAction._upgrade(fake_context(handle, spec), spec, handle, "valkey-1", config, [])
    assert type(raised.value) is RuntimeError


class DropsOneSigterm(FakeStandalone):
    """Valkey drops the first SIGTERM, as it does one it cannot act on yet."""

    dropped = 0

    def send_signal(self, instance_id, signum):
        if signum == signal.SIGTERM and not self.dropped:
            self.dropped += 1
            return
        super().send_signal(instance_id, signum)

    def binary(self, instance_id):
        return "/old/valkey-server"

    def set_binary(self, instance_id, binary):
        pass


def test_a_wave_kill_sends_sigterm_again_when_valkey_drops_it():
    # The same Valkey behavior the rolling upgrade works around is no finding in a wave either.
    handle = DropsOneSigterm()
    result, _ = run_chaos(handle, [[{"op": "kill", "target": "valkey-1", "signal": "SIGTERM"}]])
    assert result.status is ComponentStatus.PASS, result.summary
    assert handle.dropped == 1 and chaos_operation(result, "1.0")["success"] is True


def test_a_rolling_upgrade_sends_sigterm_again_when_valkey_drops_it():
    # Valkey drops a SIGTERM while it writes its initial AOF ("Writing initial AOF, can't exit"), as a replica
    # does right after its full sync. The step sends it again rather than wait step_timeout_s for an exit that
    # never comes and leave the stop to the runner, an ERROR.
    from valkey_fuzzer.components.actions import RollingUpgradeAction

    handle = DropsOneSigterm()
    config = {"to_binary": "/new/valkey-server", "step_timeout_s": 5, "settle_s": 0}
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.rolling-upgrade", id="u", config=config)
    )
    context, steps = fake_context(handle, spec), []

    RollingUpgradeAction._upgrade(context, spec, handle, "valkey-1", {**spec.config, "target": {}}, steps)

    assert handle.dropped == 1 and handle.alive["valkey-1"] and [step["kind"] for step in steps] == ["restart"]
    assert context.obligations.report()["still_active"] == []


class _DiesAfterTakingOver(FakeStandalone):
    """FAILOVER TO completes, and the promoted valkey-1 dies at once."""

    def execute(self, instance_id, args):
        reply = super().execute(instance_id, args)
        if " ".join(map(str, args)).upper().startswith("FAILOVER TO"):
            self.alive["valkey-1"] = False
        return reply

    def info(self, instance_id):
        return {**super().info(instance_id), "master_failover_state": "no-failover"}


@pytest.mark.parametrize(
    ("handle", "success"),
    [(lambda: FakeStandalone(replicas=1, failover_works=False), False), (_DiesAfterTakingOver, True)],
    ids=["never-taken-over", "new-primary-died-at-once"],
)
def test_a_graceful_promotion_counts_once_the_old_primary_follows_its_target(monkeypatch, handle, success):
    # FAILOVER TO completed in Valkey a few milliseconds before the promoted replica died, so the swap was never
    # seen. Recorded as failed, the promotion would leave the old primary, a replica of the dead one by then,
    # the tracked primary, and the restart of a killed replica would be refused ("configured primary ... is
    # unavailable"), an ERROR. The old primary following the target with its failover state cleared shows the
    # target took over. A failover that never happened fails once its time is up.
    clock = VirtualClock().install(monkeypatch, instances, polling, processes)
    result, _ = run_chaos(handle(), [[{"op": "promote", "target": "valkey-1", "timeout_s": 0.3}]], clock=clock)

    promoted = chaos_operation(result, "1.0")
    assert promoted["success"] is success and promoted.get("error") == (
        None if success else "FAILOVER TO valkey-1 did not complete within 0.3s"
    ), promoted
    if not success:
        assert result.status is ComponentStatus.FAIL and "failed without an interfering operation" in result.summary
        assert clock.elapsed >= 0.3


class _RefusesBacklogLimit(FakeStandalone):
    """The primary's first CONFIG SET of the replica output buffer limit fails without a reply."""

    refused = False

    def execute(self, instance_id, args):
        if args[:3] == ("CONFIG", "SET", "client-output-buffer-limit") and not self.refused:
            self.refused = True
            raise ConnectionError("Connection reset by peer")
        return super().execute(instance_id, args)


def test_a_held_fault_whose_injection_raised_is_undone_and_frees_its_state_for_the_wave():
    # backlog_pressure set repl-backlog-size, then its second CONFIG SET raised: it still owned the first, kept
    # its run-state claim, and the stall of the primary waited on it forever.
    handle = _RefusesBacklogLimit()
    waves = [[{"op": "backlog_pressure", "duration_s": 0.05}, {"op": "stall", "target": "primary", "duration_s": 0.05}]]
    done = []
    worker = threading.Thread(target=lambda: done.append(run_chaos(handle, waves)[0]), daemon=True)
    worker.start()
    worker.join(10)
    assert done, "the wave hung"
    [result] = done
    assert result.status is ComponentStatus.ERROR, result.summary
    assert "backlog_pressure 1.0 was not injected: ConnectionError: Connection reset by peer" in result.summary
    assert chaos_operation(result, "1.0")["undone_by"] == "operation" and chaos_operation(result, "1.1")["success"]
    assert handle.config["valkey-0"]["repl-backlog-size"] == "1048576"


def test_a_claim_ends_when_the_call_is_cancelled():
    # A claim held by an operation that never returns would otherwise keep every waiter of the wave.
    spec = resolved(
        ComponentSpec(role=ComponentRole.ACTION, use="valkey.action.replication-chaos", id="chaos",
                      config={"waves": [[{"op": "stall", "target": "valkey-1"}]]})
    )  # fmt: skip
    handle = FakeStandalone()
    context = fake_context(handle, spec, deployment={"replicas_per_primary": 2})
    run = StandaloneWaves(context, spec, handle, handle.state_check())
    holder, waiter = {"index": "1.0", "op": "stall"}, {"index": "1.1", "op": "stall"}
    assert run.claim(holder, ["valkey-1"])
    threading.Timer(0.2, context.cancel).start()
    claimed = []
    worker = threading.Thread(target=lambda: claimed.append(run.claim(waiter, ["valkey-1"])), daemon=True)
    worker.start()
    worker.join(10)
    assert claimed == [False]
    assert waiter["error"] == "cancelled" and waiter["injection_error"]


def test_a_topology_change_that_raised_is_an_error_unless_an_interfering_operation_excuses_it():
    class ReplicaofTimesOut(FakeStandalone):
        def replicate(self, instance_id, primary_id, timeout=5.0):
            if instance_id == "valkey-2" and primary_id == "valkey-1":
                raise TimeoutError("Timeout reading from socket")
            super().replicate(instance_id, primary_id, timeout)

    result, _ = run_chaos(ReplicaofTimesOut(), [[{"op": "repoint", "target": "valkey-2", "to": "valkey-1"}]])
    assert result.status is ComponentStatus.ERROR, result.summary
    assert "repoint 1.0 could not be observed: TimeoutError: Timeout reading from socket" in result.summary

    # An unreachable target is no refusal Valkey made either.
    class Unreachable(FakeStandalone):
        def info(self, instance_id):
            if instance_id == "valkey-1" and threading.current_thread() is not threading.main_thread():
                raise ConnectionError("gone")
            return super().info(instance_id)

    result, _ = run_chaos(Unreachable(), [[{"op": "promote", "target": "valkey-1", "mode": "force"}]])
    assert result.status is ComponentStatus.ERROR, result.summary
    assert "promote 1.0 was not injected: valkey-1 did not answer INFO" in result.summary


def test_a_replica_behind_every_sample_may_lack_what_came_before_the_term():
    # The soak (seed 1477792882): promoted with offset 0 just after the workload began seeding, the replica held
    # none of the preload, which the window, opening at the term's start, did not cover.
    from valkey_fuzzer.components.replication_chaos import _Tenure

    tenure = _Tenure("valkey-0", 100.0, [(100.1, "r", 50), (100.5, "r", 900)])
    assert tenure.synced_at("r", 0) == 0.0
    assert tenure.synced_at("r", 60) == 100.1
