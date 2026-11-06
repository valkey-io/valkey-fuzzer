"""The wave engine of ``valkey.action.cluster-chaos`` and ``valkey.action.replication-chaos``.

Both actions run ``waves``: each wave is a list of operations that start
together (each after its ``delay_s``, or after another operation of the wave
finished), then a heal, then validation (:meth:`WaveRun.run_waves`). The
building blocks are the :data:`.wave_operations.OPERATIONS`, each run by a
``_<name>`` method; the policies are per mode, in a :class:`WaveRun` subclass
(``cluster_chaos.ClusterWaves``, ``replication_chaos.StandaloneWaves``): the live
members targets resolve against, the heal, the validation, and which faults
excuse a failed topology change. Every operation draws from its own random
stream, ``wave-<n>-op-<i>``: its target before the wave, then whatever it
picks while it runs. Every change the engine will undo is an undo obligation
of the run (:meth:`WaveRun.own`), the one record of what is still in place.
See docs/architecture.md, "The wave engine".

This module is the run loop, its conflict claims, and its undo ownership;
:mod:`.wave_operations` holds the operations and the ``waves`` config,
:mod:`.wave_targets` target selection and kill safety, and
:mod:`.wave_evidence` the declarations and evidence both actions share.
"""

import contextvars
import threading
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from typing import Any, Callable, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.validation.config import STATE_VALIDATION_SCHEMA
from valkey_fuzzer.results import ComponentStatus, make_result
from valkey_fuzzer.schema import ConfigField, plain_config

from .instances import deployment
from .processes import KILL_EXIT_TIMEOUT_S, wait_killed
from .registration import component_registration
from .wave_evidence import wave_evidence
from .wave_operations import OPERATIONS, SIGNALS, check_waves, waves_field
from .wave_targets import Member, TargetSelector

# The order a heal or the final cleanup undoes changes in: resume stalled processes first (a config restore on
# a stopped one would hang), restart killed instances last.
UNDO_ORDER = {
    "process.paused": 0,
    "network.partition": 1,
    "server.config": 2,
    "server.client-pause": 3,
    "process.stopped": 4,
}


class WaveRun:
    """One action's waves: plan, run, heal, validate, and judge each; the rest of the contract is shared.

    A mode's subclass sets ``mode``, ``quorum`` (kills also keep a majority of
    the planned shards' primaries live: cluster mode; a standalone deployment
    is one shard, so one instance always stays up), and ``halt_on_failure``
    (stop after any failed wave, when the next wave's heal builds on this one),
    and implements ``prepare()`` (what the run needs before its test keys),
    ``outcome(records)`` (its own summary text and schedule fields),
    ``members()`` (the live view targets resolve against),
    ``kill_view()`` (a fresh view to confirm a kill against, or None),
    ``before_kill(instance_id)`` (what to record before an instance goes),
    ``role_of(instance_id, members)`` (the role a kill records), ``heal``,
    ``validate``, ``excusing(record, results)`` (the faults that excuse a
    failed topology change), ``wave_summary`` (its own wave fields), and an
    ``_<name>`` method per operation of its mode.
    """

    mode: str
    quorum: bool
    halt_on_failure: bool

    def __init__(self, context: Any, spec: Any, handle: Any, state: Any):
        self.context = context
        self.spec = spec
        self.handle = handle
        self.state = state
        self.ids = [instance.instance_id for instance in handle.instances]
        self.lock = threading.RLock()
        planned = [Member(node.instance_id, node.role, node.shard_id) for node in handle.instances]
        self.selector = TargetSelector(planned, quorum=self.quorum)
        # Every operation's random stream, by record index.
        self.rngs: dict[str, Any] = {}
        # Each record's completion, for operations that start ``after`` another.
        self.done: dict[str, threading.Event] = {}
        # Every kill sent (or tried), in order, and the killed instances that exited, until restarted.
        self.kills: list[dict[str, Any]] = []
        self.killed: dict[str, dict[str, Any]] = {}
        # Every change the run applied, as the undo obligation it opened, with the record of the operation
        # that owns it (None: the wave's own, such as the validation's CLIENT PAUSE). The one record of what
        # is still in place: a change is active until its obligation is discharged.
        self.owned: list[tuple[Any, Optional[dict[str, Any]]]] = []
        # Conflict key -> the record of the held fault that claimed it.
        self.claims: dict[str, dict[str, Any]] = {}
        self.claimed = threading.Condition(self.lock)
        self.failures: list[str] = []
        self.errors: list[str] = []

    @classmethod
    def execute(cls, context: Any, spec: Any) -> Any:
        """Run the action ``spec``: start the run, run every wave, and finish.

        A validation config the deployment rejects, a host that cannot run
        the waves (:meth:`host_ready`), or a run that cannot start (no test
        keys, no single primary) is an ``ERROR``.
        """

        config = spec.config
        # Resolved and checked when the scenario compiled.
        waves = plain_config(config["waves"])
        handle = deployment(context)
        reason = cls.host_ready(waves)
        if reason is not None:
            return make_result(spec, ComponentStatus.ERROR, reason)
        try:
            state = handle.state_check(config["validation"], seed=context.plan.seed)
        except ValueError as exc:
            return make_result(spec, ComponentStatus.ERROR, f"invalid validation config: {exc}")
        try:
            run = cls(context, spec, handle, state)
            run.prepare()
            test_keys = state.write_test_keys()
        except Exception as exc:
            return make_result(spec, ComponentStatus.ERROR, f"could not start: {type(exc).__name__}: {exc}")
        records, summaries, halted_after = run.run_waves(waves)
        extra_summary, schedule = run.outcome(records)
        return run.finish(records, summaries, halted_after, len(waves), test_keys, extra_summary, **schedule)

    @staticmethod
    def host_ready(waves: Sequence[list[dict[str, Any]]]) -> Optional[str]:
        """Why this host cannot run ``waves``, and what to do about it; None when it can."""
        return None

    def prepare(self) -> None:
        """What the run needs in place before its test keys are written."""

    def wave_running(self, number: int) -> Any:
        """A context held while a wave's operations run."""
        return nullcontext()

    def kill_view(self) -> Optional[list[Member]]:
        return None

    # -- the loop ----------------------------------------------------------------

    def run_waves(self, waves: Sequence[list[dict[str, Any]]]) -> tuple[list, list, Optional[int]]:
        """Plan, run, heal, and validate every wave until one halts the run: ``(records, summaries, halted_after)``."""

        records: list[dict[str, Any]] = []
        summaries: list[dict[str, Any]] = []
        try:
            for number, wave in enumerate(waves, 1):
                if self._run_wave(number, wave, records, summaries):
                    return records, summaries, number
        except BaseException:
            # A cancelled or crashed run leaves no stalled process, partition, or setting behind; killed
            # instances are left to the runner, whose restarts may outlast the cancellation.
            self.release("cleanup")
            raise
        return records, summaries, None

    def _run_wave(self, number: int, wave: list[dict[str, Any]], records: list, summaries: list) -> bool:
        """Run one wave; whether it halts the run."""

        planned = self.plan(number, wave)
        with self.wave_running(number), ThreadPoolExecutor(max_workers=len(planned)) as pool:
            # Each operation runs in a copy of this call's context, so the runner's cancellation of the call
            # reaches it: its delay_s and duration_s waits end at once.
            futures = [pool.submit(contextvars.copy_context().run, self.run_operation, record) for record in planned]
            results = [future.result() for future in futures]
        records.extend(results)
        summary, validated, halt = self.settle(number, results)
        summaries.append(summary)
        self.context.event_bus.publish(
            f"{self.spec.id}.wave-validated", source=self.spec.id, payload={"wave": number, "success": validated}
        )
        return halt

    def plan(self, number: int, wave: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Each operation's record, its target resolved against the live members before the wave starts.

        Targets are picked in operation order, except that operations that
        need a role (promotions, repoints) pick last, so they can avoid what
        the wave disrupts. A restart picks its target when it runs.
        """

        self.selector.begin_wave(self.members())
        records: list[Any] = [None] * len(wave)
        order = sorted(range(len(wave)), key=lambda index: OPERATIONS[wave[index]["op"]].role is not None)
        for index in order:
            operation = wave[index]
            kind, target = operation["op"], operation.get("target")
            rng = self.rngs[f"{number}.{index}"] = self.context.rng(self.spec.id, f"wave-{number}-op-{index}")
            reason, shard = None, operation.get("shard")
            if kind == "compression" and target == "all":
                target = self.selector.select_all()
            elif kind != "restart" and OPERATIONS[kind].targets:
                member, reason = self.selector.select(kind, target, rng)
                target, shard = (member.instance_id, member.shard) if member else (None, None)
            record: dict[str, Any] = {
                "index": f"{number}.{index}",
                "wave": number,
                "op": kind,
                "shard": shard,
                "config": {key: value for key, value in operation.items() if key != "op"},
                "target": target,
                "success": False,
            }
            if reason is None and kind == "repoint":
                record["to"] = "primary"  # resolved when it runs
                if operation["to"] != "primary":
                    record["to"], reason = self.selector.select_other(operation["to"], target, rng)
            if reason is not None:
                record[reason.kind] = reason.reason
            self.done[record["index"]] = threading.Event()
            records[index] = record
        return records

    def run_operation(self, record: dict[str, Any]) -> dict[str, Any]:
        """Run one planned operation when it is due, unless it has no target or was skipped;
        ``finished_at`` is when it returned, unless the operation set it.

        An operation that raised is an ``injection_error``: the harness could
        not apply it or see what Valkey did with it. A held fault that raised
        is undone at once, so it frees the state its claims hold for the wave.
        """

        config = record["config"]
        try:
            if "missing" in record or "skipped" in record:
                return record
            if config["after"] is not None:
                self.done[f"{record['wave']}.{config['after']}"].wait()
            if not self.context.sleep(config["delay_s"]):
                record.update(error="cancelled", injection_error=True)
                return record
            try:
                getattr(self, f"_{record['op']}")(record, config)
            except Exception as exc:
                record.update(error=f"{type(exc).__name__}: {exc}", injection_error=True)
                if OPERATIONS[record["op"]].held:
                    self.undo(record, "operation")
            finally:
                # A held fault keeps its claims until its undo succeeds.
                if not (OPERATIONS[record["op"]].held and self.still_owned(record)):
                    self.unclaim(record)
            return record
        finally:
            record.setdefault("finished_at", time.time())
            self.done[record["index"]].set()

    def landed(self, record: dict[str, Any], affected: Sequence[str]) -> None:
        """``record``'s fault is in place and acts on ``affected``."""
        record["applied_at"] = time.time()
        record["affected"] = sorted(set(affected))

    # -- heal, validation, and verdicts ---------------------------------------------

    def settle(self, number: int, results: list[dict[str, Any]]) -> tuple[dict[str, Any], bool, bool]:
        """Heal, validate, and judge one wave: ``(summary, validated, halt)``.

        The heal first undoes every held fault still in place and checks that
        every instance not killed is alive; an undo, heal step, or validation
        that cannot complete is an error, and the wave is not validated.
        """

        heal_errors = self.release("heal")
        heal_failures = [
            f"{instance_id} exited without being killed"
            for instance_id in self.ids
            if instance_id not in self.down() and not self.handle.is_alive(instance_id)
        ]
        if not heal_failures and not heal_errors:
            try:
                self.heal(number)
            except Exception as exc:
                heal_errors.append(f"heal after wave {number} failed: {type(exc).__name__}: {exc}")
        validation = None
        if not heal_failures and not heal_errors:
            try:
                validation = self.validate(number, results)
            except Exception as exc:
                heal_errors.append(f"could not validate: {type(exc).__name__}: {exc}")
        failures, errors = self.judge(number, results, validation)
        failures[:0] = [f"wave {number}: {problem}" for problem in heal_failures]
        errors[:0] = [f"wave {number}: {problem}" for problem in heal_errors]
        self.failures += failures
        self.errors += errors
        summary = {
            "wave": number,
            "operations": [record["index"] for record in results],
            "ran": [record["index"] for record in results if record.get("applied_at")],
            "heal_problems": heal_failures + heal_errors,
            "validation": validation,
            **self.operation_states(results),
            **self.wave_summary(number, results),
        }
        validated = bool(validation and validation["overall_success"]) and not heal_failures and not heal_errors
        critical = (
            validation is not None
            and not validation["overall_success"]
            and (validation["critical"] or self.state.config.blocking_on_failure)
        )
        halt = bool(heal_failures or heal_errors or critical or (failures and self.halt_on_failure))
        return summary, validated, halt

    def judge(
        self, number: int, results: list[dict[str, Any]], validation: Optional[Mapping[str, Any]]
    ) -> tuple[list[str], list[str]]:
        """The failures (FAIL) and the problems that leave the wave untested (ERROR) of one wave's operations."""

        failures = []
        errors = []
        if validation is not None and not validation["overall_success"]:
            checks = ", ".join(validation["failed_checks"]) or "unknown"
            detail = "; ".join(validation.get("error_messages") or ())
            # An inconclusive validation never saw the deployment: the wave is untested, not failed.
            (errors if validation.get("inconclusive") else failures).append(
                f"wave {number} validation {'could not judge' if validation.get('inconclusive') else 'failed'} "
                f"({checks})" + (f": {detail}" if detail else "")
            )
        for record in results:
            label = f"wave {number} {record['op']} {record['index']}"
            operation = OPERATIONS[record["op"]]
            if "undo_error" in record:
                # Even when the heal undid it later: the fault was not undone when it should have been.
                then = f"undone by the {record['undone_by']}" if record.get("undone_at") else "still in place"
                errors.append(f"{label} could not be undone ({record['undo_error']}); {then}")
            failed_change = (
                operation.topology and not record["success"] and "skipped" not in record and "missing" not in record
            )
            if failed_change:
                # An operation of the wave that disrupted it explains whatever happened, seen or not.
                record["excused_by"] = self.excusing(record, results)
                if record["excused_by"]:
                    continue
            if record.get("unexpected_exit") or record.get("stuck"):
                failures.append(f"{label}: {record.get('unexpected_exit') or record['stuck']}")
            elif record.get("injection_error"):
                # Applied, but the harness did not see what Valkey did with it.
                what = "could not be observed" if record.get("applied_at") else "was not injected"
                errors.append(f"{label} {what}: {record['error']}")
            elif "missing" in record:
                errors.append(f"{label} had no target: {record['missing']}")
            elif failed_change:
                why = record.get("refused") or record.get("error") or "did not complete"
                failures.append(f"{label} failed without an interfering operation: {why}")
        # A wave the safety rules refused whole is only skipped: the earlier waves left nothing safe to do.
        if not any(record.get("applied_at") for record in results) and not all("skipped" in r for r in results):
            errors.append(f"wave {number} ran no operation")
        return failures, errors

    def finish(
        self,
        records: list[dict[str, Any]],
        summaries: list[dict[str, Any]],
        halted_after: Optional[int],
        planned_waves: int,
        test_keys: Mapping[str, Any],
        extra_summary: str = "",
        **schedule: Any,
    ) -> Any:
        """The action's result after the final cleanup: the schedule, its evidence, and the verdict.

        ``FAIL`` on a failure; ``ERROR`` on a problem that leaves the run
        untested or the deployment unrestored (still-active faults
        included); ``SKIP`` when faults were planned and none landed.
        """

        cleanup_problems = self.release("cleanup", restarts=True)
        still_active = self.operation_states(records)["still_active"]
        errors = [*self.errors, *(f"final cleanup: {problem}" for problem in cleanup_problems)]
        if still_active:
            errors.append(f"the changes of operation(s) {still_active} are still in place after the final cleanup")
        landed = [kill for kill in self.kills if kill["success"]]
        still_down = sorted(self.down())
        windows = schedule.pop("loss_windows", [])
        value = {
            "operations": records,
            # The kills whose instance is still down (the others were restarted), and every kill tried.
            "kills": [kill for kill in landed if kill["instance_id"] in still_down],
            "all_kills": self.kills,
            "waves": summaries,
            "halted_after_wave": halted_after,
            "not_run": planned_waves - len(summaries),
            # Operations whose changes are still in place (their obligations are open), and why the final
            # cleanup could not undo them.
            "still_active": still_active,
            "cleanup_problems": cleanup_problems,
            "loss_windows": windows,
            "test_keys": len(test_keys),
            **schedule,
        }
        faults = [record for record in records if OPERATIONS[record["op"]].removes or OPERATIONS[record["op"]].held]
        ran = sum(1 for record in records if record.get("applied_at"))
        untested = not ran or (faults and not any(record.get("applied_at") for record in faults))
        skipped = [record["index"] for record in records if "skipped" in record]
        if skipped:
            extra_summary += f"; {len(skipped)} planned operation(s) skipped by the safety rules ({skipped})"
        summary = (
            f"{ran} of {len(records)} operation(s) in {len(summaries)} wave(s), {len(landed)} kill(s){extra_summary}"
        )
        evidence = wave_evidence(self.spec, value, still_down, test_keys, windows)
        if self.failures or errors:
            return make_result(
                self.spec,
                ComponentStatus.FAIL if self.failures else ComponentStatus.ERROR,
                "; ".join([*self.failures, *errors]),
                evidence=evidence,
                diagnostics={"summary": summary},
            )
        if untested:
            return make_result(
                self.spec, ComponentStatus.SKIP, f"no planned fault was injected: {summary}", evidence=evidence
            )
        return make_result(self.spec, ComponentStatus.PASS, f"{summary}; every wave validated", evidence=evidence)

    # -- building blocks -----------------------------------------------------------

    def _kill(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        """Signal the target unless that is no longer safe, and wait for it to exit.

        It first waits for any held fault on the target to be undone (see
        ``Operation.mutates``), so it never meets a stalled process (one
        takes ``SIGTERM`` only once it runs again) or one a config change
        still needs. A target that already exited was not killed:
        ``unexpected_exit``, which fails the run; one that never exits is
        ``stuck`` (Valkey dropped every ``SIGTERM``), which fails it too, or
        after ``SIGKILL``, which Valkey cannot refuse, an injection error.
        """

        target = record["target"]
        claimed, members = self.claim_kill(record, target)
        if not claimed:
            self.selector.release(target)
            return
        reason = self.selector.confirm(target, members)
        if reason is not None:
            self.selector.release(target)
            record[reason.kind] = reason.reason
            return
        self.before_kill(target)
        kill: dict[str, Any] = {
            "instance_id": target,
            "role": self.role_of(target, members),
            "shard": record["shard"],
            "signal": config["signal"],
            "operation": record["index"],
            "wave": record["wave"],
            "at": time.time(),
            "success": True,
        }
        with self.lock:
            self.kills.append(kill)
        try:
            self.handle.send_signal(target, SIGNALS[config["signal"]])
        except ProcessLookupError:
            # Never picked twice, so the process exited by itself.
            kill.update(success=False, unexpected_exit=True)
            record["unexpected_exit"] = f"{target} exited before it was killed"
            return
        except Exception as exc:
            kill["success"] = False
            self.selector.release(target)
            record.update(error=f"{type(exc).__name__}: {exc}", injection_error=True)
            return
        undo = self.kill_undo(target)
        if undo is not None:
            self.own(record, "process.stopped", target, undo)
        self.selector.land(target)
        self.landed(record, [target])
        # Valkey drops a SIGTERM it cannot act on yet; that is no finding, so it is sent again until the exit.
        exited = wait_killed(self.handle, target, SIGNALS[config["signal"]])
        kill["exited_at"] = exited.get("exited_at")
        with self.lock:
            self.killed[target] = kill
        record["success"] = exited["exited"]
        if not exited["exited"]:
            why = f"{target} did not exit within {KILL_EXIT_TIMEOUT_S:g}s of {config['signal']}, sent every second"
            if config["signal"] == "SIGKILL":
                # Valkey cannot refuse a SIGKILL: the host did not deliver it.
                record.update(error=why, injection_error=True)
            else:
                record["stuck"] = why
        self.context.event_bus.publish(f"{self.spec.id}.instance-killed", source=self.spec.id, payload=kill)

    def claim_kill(self, record: dict[str, Any], target: str) -> tuple[bool, Optional[list[Member]]]:
        """Claim a kill's target and the live shard it serves: whether the claim was granted, and the fresh view
        to confirm the kill against (None: the mode has none).

        The claim waits for a failover of that shard to settle (see
        ``Operation.mutates``); the view is read again once the claim is held,
        after whatever the wave did meanwhile.
        """

        members = self.kill_view()
        live = next((member.shard for member in members or () if member.instance_id == target), None)
        # And the shard it was planned in: a replica a failover is promoting may show in none mid-switch.
        shards = sorted({shard for shard in (live, record["shard"]) if shard is not None})
        if not self.claim(record, [target], shards=shards):
            return False, None
        return True, self.kill_view()

    def before_kill(self, instance_id: str) -> None:
        """What the mode records about ``instance_id`` right before it is killed."""

    def kill_undo(self, instance_id: str) -> Optional[Callable[[], None]]:
        """The undo of a kill whose instance the mode restarts (the kill then owns it), or None: it stays down."""
        return None

    def down(self) -> set[str]:
        """Instances killed and not restarted."""
        return set(self.selector.down)

    def reachable(self, record: dict[str, Any], targets: Sequence[str]) -> list[str]:
        """``targets`` that are neither killed nor stalled; the operation is skipped when none is."""

        with self.lock:
            down, stalled = self.down(), self.held_on("process.paused")
            usable = [target for target in targets if target not in down and target not in stalled]
        if not usable:
            record["skipped"] = f"{', '.join(targets)} killed or stalled in this wave"
        return usable

    def claim(self, record: dict[str, Any], owns: Sequence[str], shards: Sequence[int] = ()) -> bool:
        """Claim the state ``record``'s operation changes on ``owns``, and on ``shards`` if it changes a
        shard (see ``Operation.mutates``).

        Waits while another operation of the wave holds any of it, and claims
        it all at once. False, with ``record["skipped"]``, when the holder's
        undo failed: that state is not back to what this operation would save;
        False, with an ``injection_error``, when the call is cancelled.
        """

        mutates = OPERATIONS[record["op"]].mutates
        keys = {f"{kind}:{item}" for kind in mutates if kind != "shard" for item in owns}
        keys |= {f"shard:{shard}" for shard in shards if "shard" in mutates}
        with self.claimed:
            while True:
                holders = {holder["index"]: holder for key, holder in self.claims.items() if key in keys}
                if not holders:
                    self.claims.update(dict.fromkeys(keys, record))
                    return True
                stuck = sorted(index for index, holder in holders.items() if "undo_error" in holder)
                if stuck:
                    record["skipped"] = f"{', '.join(sorted(keys))} still changed by {stuck[0]}, whose undo failed"
                    return False
                if self.context.cancelled:
                    record.update(error="cancelled", injection_error=True)
                    return False
                record["waited_for"] = sorted(set(record.get("waited_for", [])) | set(holders))
                self.claimed.wait(0.5)

    def unclaim(self, record: dict[str, Any]) -> None:
        with self.claimed:
            for key in [key for key, holder in self.claims.items() if holder is record]:
                del self.claims[key]
            self.claimed.notify_all()

    def hold(self, record: dict[str, Any], affected: Sequence[str], duration_s: float) -> None:
        """A held fault is in place: keep it ``duration_s``, then undo it (unless the heal already did).

        The fault owns its changes (and its claims) until the undo succeeds:
        what fails stays owned for the heal and the final cleanup to retry,
        and records ``undo_error``.
        """

        self.landed(record, affected)
        record["success"] = True
        self.context.sleep(duration_s)
        self.undo(record, "operation")

    # -- ownership ---------------------------------------------------------------

    def own(self, record: Optional[dict[str, Any]], kind: str, target: str, undo: Callable[[], None]) -> Any:
        """Open the obligation for a change of ``kind`` to ``target`` just applied, owned by ``record``'s
        operation (None: by the wave), and undone by ``undo``, which must be idempotent."""

        obligation = self.context.obligations.open(self.spec, kind, target, undo)
        with self.lock:
            self.owned.append((obligation, record))
        return obligation

    def still_owned(self, record: Optional[dict[str, Any]]) -> list[Any]:
        """The open obligations ``record``'s operation owns."""

        with self.lock:
            return [obligation for obligation, owner in self.owned if owner is record and obligation.open]

    def held_on(self, kind: str) -> set[str]:
        """The targets of the operations whose change of ``kind`` is still in place (stalled, partitioned)."""

        with self.lock:
            return {
                owner["target"]
                for obligation, owner in self.owned
                if owner is not None and obligation.open and obligation.kind == kind
            }

    def discharge(self, obligation: Any, record: Optional[dict[str, Any]], stage: str) -> Optional[str]:
        """Undo one owned change as ``stage`` (the operation, a restart, the heal, the validation, or the final
        cleanup); why it failed, or None.

        A failed undo leaves the obligation open, and the operation's claims
        held, for the next try; it records the operation's first
        ``undo_error``. Once every change an operation owns is undone, it
        records ``undone_at`` and ``undone_by`` and releases its claims.
        """

        try:
            obligation.discharge()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if record is not None:
                with self.claimed:
                    record.setdefault("undo_error", error)
                    self.claimed.notify_all()
            return f"could not undo the {obligation.kind} on {obligation.target}: {error}"
        if record is not None and not self.still_owned(record):
            with self.lock:
                undone_at = max(item.undone_at for item, owner in self.owned if owner is record)
            record.update(undone_at=undone_at, undone_by=stage)
            self.unclaim(record)
        return None

    def undo(self, record: dict[str, Any], stage: str) -> list[str]:
        """Undo every change ``record``'s operation still owns; the problems."""

        problems = [self.discharge(obligation, record, stage) for obligation in self.still_owned(record)]
        return [problem for problem in problems if problem]

    def release(self, stage: str, *, restarts: bool = False) -> list[str]:
        """Undo every change still in place (``stage``: who does, the heal or the final cleanup); with
        ``restarts``, restart the killed instances too (the heal restarts them in its own order).

        Returns the undos that failed; those stay owned, for the next try.
        """

        with self.lock:
            pending = [
                (obligation, record)
                for obligation, record in self.owned
                if obligation.open and (restarts or obligation.kind != "process.stopped")
            ]
        pending.sort(key=lambda item: UNDO_ORDER.get(item[0].kind, len(UNDO_ORDER)))
        problems = [self.discharge(obligation, record, stage) for obligation, record in pending]
        return [problem for problem in problems if problem]

    def operation_states(self, results: Sequence[Mapping[str, Any]]) -> dict[str, list]:
        """A wave's operations by what happened to them: ``planned`` (every one), ``applied`` (landed),
        ``healed`` (landed, and every change it owned is undone), ``still_active`` (a change it owns is still
        in place), and ``skipped``.

        Read from the obligations: only a successful undo counts as healed.
        """

        with self.lock:
            active: dict[str, bool] = {}
            for obligation, owner in self.owned:
                if owner is not None:
                    active[owner["index"]] = active.get(owner["index"], False) or obligation.open
        return {
            "planned": [record["index"] for record in results],
            "applied": [record["index"] for record in results if record.get("applied_at")],
            "healed": [
                record["index"]
                for record in results
                if record.get("applied_at") and record["index"] in active and not active[record["index"]]
            ],
            "still_active": [record["index"] for record in results if active.get(record["index"])],
            # Refused by the safety rules (or by an earlier fault's state): this wave did not inject them.
            "skipped": [record["index"] for record in results if "skipped" in record],
        }


class WaveAction:
    """A chaos action: its ``run_cls`` runs its waves (:meth:`WaveRun.execute`)."""

    run_cls: type

    def run(self, context: Any, spec: Any) -> Any:
        return self.run_cls.execute(context, spec)


def wave_registration(
    component_id: str, action: type, fields: Mapping[str, ConfigField], declares: Declarations
) -> Any:
    """The registration of chaos action ``action``: its mode's ``waves``, checked against the plan's
    deployment when the scenario compiles, the deployment's state ``validation`` config, and ``fields``."""

    mode = action.run_cls.mode
    return component_registration(
        component_id,
        action,
        {
            "waves": waves_field(mode),
            # The deployment's state validation config (``handle.state_check``), checked when the scenario compiles.
            "validation": ConfigField(dict, default=dict, schema=STATE_VALIDATION_SCHEMA),
            **fields,
        },
        check=lambda config, spec, plan: check_waves(config["waves"], mode, plain_config(plan.deployment)),
        deployment_modes=(mode,),
        declares=declares,
    )
