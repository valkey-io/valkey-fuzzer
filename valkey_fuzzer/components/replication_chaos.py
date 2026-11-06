"""Replication chaos: seeded waves of faults on a standalone primary and its replicas.

``valkey.action.replication-chaos`` is the standalone mode of the wave engine
(:mod:`.waves`) that also runs ``valkey.action.cluster-chaos``. It runs
``waves``, each a list of operations that start together, each after its own
``delay_s``:

- ``kill {target, signal}``: ``SIGKILL`` or ``SIGTERM`` one instance;
- ``restart {target}``: restart an instance killed earlier in the wave as a
  replica of the current primary;
- ``promote {target, mode, split_s, timeout_s}``: ``graceful`` sends
  ``FAILOVER TO`` to the primary; ``force`` sends ``REPLICAOF NO ONE`` to the
  replica, lets the old primary keep serving for ``split_s`` (a split brain),
  then points the old primary and every other replica at the new primary;
- ``repoint {target, to}``: point a replica at another replica (a chain) or
  at the primary;
- ``partition {target, duration_s}``: drop the traffic between one instance
  and every other (Linux, root, iptables, per-instance loopback addresses);
- ``stall {target, duration_s}``: ``SIGSTOP``, then ``SIGCONT``;
- ``backlog_pressure {repl_backlog_size, output_limit, duration_s}``: shrink
  the primary's replication backlog and replica output buffer limit;
- ``compression {target, value, duration_s}``: ``CONFIG SET repl-compression``.

Targets are ``primary``, ``replica``, ``any``, an instance ID, ``all`` (for
``compression``) or ``killed`` (for ``restart``); roles are the ones at the
start of the wave. Kill safety (the standalone policy of the engine's
``TargetSelector``): a kill never takes the last instance that is not down,
so one copy of the latest synced data stays live. A promotion needs a live replica that is not stalled
or partitioned (and, for ``graceful``, linked); otherwise it is skipped. A
promotion, repoint, or restart that is skipped or fails is tolerated only
when another operation of the same wave disrupted an instance it involves
(:func:`excusing_operations`); otherwise it fails the action.

After every wave the action heals in order: undo what is still in place
(configs, partitions, stalls), promote the most advanced live replica if the
primary is down, restart the killed instances as replicas of the primary
(``replicate`` then ``restart``), and point every other instance at it. Then,
with writes briefly paused (``CLIENT PAUSE WRITE``), it waits for every
replica's offset to reach the primary's and validates the deployment: one
primary, every live replica linked to it, offsets equal, test keys equal.
Every stall, partition, setting, kill, and pause the action will undo is an
undo obligation from the moment it is applied; one whose undo fails stays
open for the next heal, the final cleanup, and the runner, and the run is
``ERROR``.

Durability: a graceful promotion loses nothing. A forced one (and the
promotion that heals a dead primary) may lose writes the old primary
acknowledged after the new primary's last sync point with it until the old
primary stopped acknowledging (exited, or was pointed at the new primary):
each such window is published as ``valkey.data.loss-window`` evidence
``{start, end, instance_id}``, which ``valkey.oracle.ledger`` reads. A sync
point is the latest time ``t`` at which the old primary's offset, sampled
right after ``t``, was at most the new primary's offset, read before it was
promoted. Writes ``WAIT`` confirmed on every replica are never lost (see the
ledger).

Verdict: FAIL on a failed wave validation, an instance that exited without
being killed, or an unexcused promotion, repoint, or restart that Valkey
refused or did not complete; ERROR when an operation had no target at all,
a fault could not be injected, an operation raised (the harness did not see
what Valkey did), a heal step failed, or a wave ran no operation. It publishes
``<id>.instance-killed``, ``<id>.promoted`` and ``<id>.wave-validated``, and
the ``valkey.chaos.schedule``, ``valkey.instances.expected-down``, and
``valkey.chaos.test-keys`` evidence ``valkey.oracle.deployment-state``
reads.
"""

import functools
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Optional

from valkey_fuzzer.deployment.local.queries import parse_replication
from valkey_fuzzer.hoststate import chain_name
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.schema import ConfigField, ConfigSchema

from .capabilities import iptables_supported
from .compression import unsettled_links
from .instances import (
    close_quietly,
    follows,
    graceful_failover,
    is_primary,
    replication_info,
)
from .network import create_chain, drop_between, remove_partition
from .polling import poll_until
from .processes import pause_owned, restart_once
from .schemas import LIST, TEXT
from .wave_evidence import wave_declares
from .wave_operations import STANDALONE
from .wave_targets import Member
from .waves import WaveAction, WaveRun, wave_registration

# How often the primary's replication offset is sampled during a wave.
_SAMPLE_INTERVAL_S = 0.1
_INFO_TIMEOUT_S = 1.0
# How long a repl-compression restore waits for every link to renegotiate (one link per second per primary).
CODEC_SETTLE_S = 15.0


@dataclass
class _Tenure:
    """One primary's tenure, with samples ``(time, replid, offset)`` of its replication offset."""

    instance_id: str
    started_at: float
    samples: list = field(default_factory=list)

    def synced_at(self, replid: Optional[str], offset: Optional[int]) -> float:
        """The latest sample time whose offset ``offset`` reached; the start of time if none. A replica behind
        every sample may lack anything its primary acknowledged, from before this term too: one that was
        promoted 0.3 s after the workload began seeding held none of it."""

        reached = [
            at
            for at, sample_replid, value in self.samples
            if sample_replid == replid and offset is not None and value <= offset
        ]
        return max(reached, default=0.0)


def _replication_offset(info: Mapping[str, Any]) -> Optional[int]:
    """How far a replica applied its primary's stream, or a primary's own offset."""

    link = parse_replication(info)
    return link.offset if link.replica_offset is None else link.replica_offset


def excusing_operations(record: Mapping[str, Any], records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The operations of ``record``'s wave that could have made it fail, and why.

    An operation excuses another when it landed in the same wave before the
    other finished and acted on an instance the other involves: a kill,
    stall, or partition of that instance; a promotion, which changes the
    primary and repoints every replica; a repoint or restart of it; or a
    backlog or compression change on it, which drops replica links (on the
    primary, every replica's link).
    """

    involved = set(record.get("involved", ()))
    excused = []
    for other in records:
        if (
            other is record
            or other["wave"] != record["wave"]
            or not other.get("applied_at")
            or other["applied_at"] >= record.get("finished_at", float("inf"))
        ):
            continue
        affected = set(other.get("affected", ()))
        if involved & affected:
            excused.append({"operation": other["index"], "op": other["op"], "instances": sorted(involved & affected)})
    return excused


class StandaloneWaves(WaveRun):
    """The standalone mode of the wave engine: one primary and its replicas, healed by the action.

    Kill safety keeps one instance up (the deployment is one shard). Members
    are the roles at the start of the wave. After the wave, :meth:`heal`
    restores one primary that every instance follows, and :meth:`validate`
    checks it with writes paused.
    """

    mode = STANDALONE
    quorum = False
    # The next wave's heal builds on this one's result.
    halt_on_failure = True

    def __init__(self, context: Any, spec: ComponentSpec, handle: Any, state: Any):
        super().__init__(context, spec, handle, state)
        self.heal_timeout_s = spec.config["heal_timeout_s"]
        self.instance = {instance.instance_id: instance for instance in handle.instances}
        # Promotions, repoints, and restarts change the topology one at a time.
        self.topology = threading.Lock()
        roles = handle.roles()
        primaries = [item for item in self.ids if roles.get(item) == "primary"]
        if len(primaries) != 1:
            raise RuntimeError(f"replication chaos needs exactly one live primary, found {primaries}")
        self.primary = primaries[0]
        self.term = _Tenure(self.primary, time.time())
        self.windows: list[dict[str, Any]] = []
        # When each instance was told to follow another, every time.
        self.demoted_at: dict[str, list[float]] = {item: [] for item in self.ids}
        # What each instance was last told to follow (None: be a primary).
        self.follows: dict[str, Optional[str]] = {
            item: (None if item == self.primary else self.primary) for item in self.ids
        }
        self.heal_promotions: list[dict[str, Any]] = []

    @staticmethod
    def host_ready(waves: Sequence[list[dict[str, Any]]]) -> Optional[str]:
        if not any(operation["op"] == "partition" for wave in waves for operation in wave):
            return None
        supported, reason = iptables_supported()
        if not supported:
            return f"partition operations need Linux, root, and iptables ({reason}); remove them or run elsewhere"
        return None

    def outcome(self, records: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        windows = self.close_windows()
        promotions = sum(record["op"] == "promote" and record["success"] for record in records)
        return (
            f", {promotions} promotion(s), {len(self.heal_promotions)} heal promotion(s), {len(windows)} loss window(s)",
            {"heal_promotions": self.heal_promotions, "loss_windows": windows},
        )

    def members(self) -> list[Member]:
        # One shard; an instance that does not answer has no role.
        roles = self.handle.roles()
        return [Member(item, roles.get(item), 0) for item in self.ids]

    def before_kill(self, instance_id: str) -> None:
        if instance_id == self.primary:
            # The latest offset sample before the primary goes: a loss window's start.
            self.sample()

    def role_of(self, instance_id: str, members: Optional[Sequence[Member]]) -> str:
        info = self.info(instance_id)
        return "unknown" if info is None else "primary" if is_primary(info) else "replica"

    # -- Valkey access -------------------------------------------------------

    def info(self, instance_id: str, timeout: float = _INFO_TIMEOUT_S) -> Optional[dict]:
        return replication_info(self.handle, instance_id, timeout)

    def command(self, instance_id: str, *args: Any, timeout: float = 5.0) -> Any:
        client = self.handle.new_client(instance_id, timeout=timeout)
        try:
            return client.execute_command(*args)
        finally:
            close_quietly(client)

    def replicate(self, instance_id: str, primary_id: Optional[str]) -> None:
        """``handle.replicate``, recording what the instance was told to follow and
        when a former primary stopped acknowledging writes."""

        self.handle.replicate(instance_id, primary_id)
        with self.lock:
            self.follows[instance_id] = primary_id
            if primary_id is not None and self.handle.is_alive(instance_id):
                self.demoted_at[instance_id].append(time.time())

    # -- Offsets and loss windows -------------------------------------------

    def sample(self) -> None:
        with self.lock:
            term = self.term
        at = time.time()
        info = self.info(term.instance_id, timeout=0.5)
        if is_primary(info):
            offset = _replication_offset(info)
            if offset is not None:
                with self.lock:
                    term.samples.append((at, str(parse_replication(info).replication_id), offset))

    @contextmanager
    def wave_running(self, number: int):
        """Sample the primary's replication offset while the wave's operations run."""

        stop = threading.Event()

        def sampler() -> None:
            while not stop.is_set():
                self.sample()
                stop.wait(_SAMPLE_INTERVAL_S)

        thread = threading.Thread(target=sampler, name=f"{self.spec.id}-sampler", daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join()

    def succeed(self, new_primary: str, at: float, *, forced: bool, sync: Optional[dict], reason: str) -> None:
        """Record that ``new_primary`` replaced the current primary; a forced succession opens a loss window."""

        with self.lock:
            old = self.term
            if forced:
                start = old.synced_at(sync.get("replid"), sync.get("offset")) if sync else old.started_at
                self.windows.append(
                    {
                        "instance_id": old.instance_id,
                        "start": start,
                        "opened_at": at,
                        # The old primary's tenure: only an exit during it ends the window (start may be earlier).
                        "tenure_started_at": old.started_at,
                        "reason": reason,
                        # What the start was derived from, for the report.
                        "sync": sync,
                        "last_sample": old.samples[-1] if old.samples else None,
                    }
                )
            self.primary = new_primary
            self.term = _Tenure(new_primary, at)

    def close_windows(self) -> list[dict[str, Any]]:
        """End every open window when its old primary stopped acknowledging writes: exited or demoted."""

        now = time.time()
        with self.lock:
            for window in self.windows:
                if "end" in window:
                    continue
                instance = window["instance_id"]
                ends = [at for at in self.demoted_at[instance] if at >= window["opened_at"]]
                ends += [
                    kill["exited_at"]
                    for kill in self.kills
                    if kill["instance_id"] == instance
                    and kill.get("exited_at")
                    and kill["at"] >= window["tenure_started_at"]
                ]
                window["end"] = min(ends) if ends else now
                # A kill before the promotion ends the window at the exit, which may precede the promotion.
                window["end"] = max(window["end"], window["start"])
            return list(self.windows)

    # -- Operations ------------------------------------------------------------

    def _restart(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        with self.topology:
            with self.lock:
                down = sorted(self.killed)
                primary = self.primary
            target = record["target"]
            if target == "killed":
                if not down:
                    record["skipped"] = "no instance killed in this wave is down"
                    return
                target = self.rngs[record["index"]].choice(down)
            elif target not in down:
                record["missing"] = f"{target} is not down"
                return
            record["target"] = target
            record["involved"] = [target, primary]
            if primary in down:
                record["skipped"] = f"the primary {primary} is down; {target} restarts when the wave heals"
                return
            self.landed(record, [target, primary])
            self.restart_killed(target, "restart")
        record["success"] = True

    def kill_undo(self, instance_id: str):
        """A killed instance restarts as a replica of the primary (as it was, if it still is the primary)."""

        restart = restart_once(self.handle, instance_id, wait_ready=True, ready_timeout=60.0)

        def undo() -> None:
            with self.lock:
                primary = self.primary
            if primary != instance_id:
                self.replicate(instance_id, primary)
            restart()
            self.selector.restore(instance_id)
            with self.lock:
                self.killed.pop(instance_id, None)

        return undo

    def restart_killed(self, instance_id: str, stage: str) -> None:
        """Restart a killed instance: discharge its kill's obligation as ``stage``; raise if that fails."""

        with self.lock:
            obligation, record = next(
                (obligation, record)
                for obligation, record in self.owned
                if obligation.open and obligation.kind == "process.stopped" and obligation.target == instance_id
            )
        problem = self.discharge(obligation, record, stage)
        if problem:
            raise RuntimeError(problem)

    def _promote(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        target = record["target"]
        with self.topology:
            with self.lock:
                old = self.primary
                refused = (
                    "killed"
                    if target in self.killed
                    else "stalled"
                    if target in self.held_on("process.paused")
                    else "partitioned"
                    if target in self.held_on("network.partition")
                    else None
                )
            record["involved"] = [target, old]
            record["primary_before"] = old
            info = self.info(target)
            if refused is None and not info:
                # No refusal of Valkey's: the harness could not read the target.
                record.update(error=f"{target} did not answer INFO", injection_error=True)
                return
            if refused is None and is_primary(info):
                refused = "already a primary"
            graceful = config["mode"] == "graceful"
            # The same read: one that failed above is the harness's, never a refusal.
            if refused is None and graceful and not follows(info, self.instance[old]):
                refused = f"not linked to the primary {old}"
            if refused is None and not graceful and self.unsynced_restart(target, record["wave"], info):
                # It holds only what it loaded from disk: a forced promotion could lose writes from before the
                # current primary's term, and writes WAIT confirmed on it, which no loss window covers.
                refused = "restarted in this wave and not resynced yet"
            if refused is not None:
                record["refused"] = f"{target} is {refused}"
                return
            if graceful:
                self._graceful(record, target, old, config)
            else:
                self._force(record, target, old, info, config)

    def unsynced_restart(self, instance_id: str, wave: int, info: Mapping[str, Any]) -> bool:
        """Whether ``instance_id`` was killed and restarted in ``wave`` and has no up link since."""

        with self.lock:
            restarted = any(
                kill["instance_id"] == instance_id and kill["wave"] == wave and kill["success"] for kill in self.kills
            )
        return restarted and not parse_replication(info).link_up

    def promote_now(self, instance_id: str) -> None:
        """``REPLICAOF NO ONE`` on ``instance_id``; a timeout may follow one that ran, so then its role decides."""

        try:
            self.replicate(instance_id, None)
        except Exception:
            if not is_primary(self.info(instance_id)):
                raise

    def _graceful(self, record: dict[str, Any], target: str, old: str, config: dict[str, Any]) -> None:
        self.landed(record, self.ids)
        done, error = graceful_failover(self.context, self.handle, old, target, config["timeout_s"])
        if done is None:
            # FAILOVER TO got no reply, or the roles could not be read: the harness did not see what Valkey did.
            record.update(
                error=error or f"the roles could not be read after FAILOVER TO {target}", injection_error=True
            )
            return
        if not done:
            record["error"] = error or f"FAILOVER TO {target} did not complete within {config['timeout_s']}s"
            return
        if error:
            record["failover_error"] = error
        promoted_at = time.time()
        self.succeed(target, promoted_at, forced=False, sync=None, reason=f"graceful promotion of {target}")
        self._follow(target, record)
        record.update(success=True, promoted_at=promoted_at)
        self.context.event_bus.publish(
            f"{self.spec.id}.promoted", source=self.spec.id, payload={"from": old, "to": target, "mode": "graceful"}
        )

    def _force(
        self, record: dict[str, Any], target: str, old: str, info: Mapping[str, Any], config: dict[str, Any]
    ) -> None:
        sync = {"replid": str(parse_replication(info).replication_id), "offset": _replication_offset(info)}
        promoted_at = time.time()
        self.landed(record, self.ids)
        self.promote_now(target)
        self.succeed(target, promoted_at, forced=True, sync=sync, reason=f"forced promotion of {target}")
        record.update(promoted_at=promoted_at, sync=sync)
        self.context.event_bus.publish(
            f"{self.spec.id}.promoted", source=self.spec.id, payload={"from": old, "to": target, "mode": "force"}
        )
        # The old primary keeps acknowledging writes until it is pointed at the new one.
        self.context.sleep(config["split_s"])
        self._follow(target, record)
        record["success"] = True

    def _follow(self, primary: str, record: dict[str, Any]) -> None:
        """Point every other instance at ``primary``; failures are left for the heal to fix."""

        errors = []
        for instance_id in self.ids:
            if instance_id == primary:
                continue
            try:
                self.replicate(instance_id, primary)
            except Exception as exc:
                errors.append(f"{instance_id}: {type(exc).__name__}: {exc}")
        if errors:
            record["follow_errors"] = errors

    def _repoint(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        target = record["target"]
        with self.topology:
            with self.lock:
                to = self.primary if record["to"] == "primary" else record["to"]
                down = set(self.killed)
            record.update(to=to, involved=[target, to])
            if target in down or to in down:
                record["refused"] = f"{target if target in down else to} is down"
                return
            if is_primary(self.info(target)) or target == to:
                record["refused"] = f"{target} is no longer a replica"
                return
            self.landed(record, [target, to])
            self.replicate(target, to)
        record["success"] = True

    def _stall(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        target = record["target"]
        if not self.claim(record, [target]):
            return
        with self.lock:
            if target in self.down():
                record["skipped"] = f"{target} was killed in this wave"
                return
            pause_owned(functools.partial(self.own, record), self.handle, target)
        self.hold(record, [target], config["duration_s"])

    def _partition(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        target = record["target"]
        # Unique to this operation, so concurrent runs in one process never share a chain.
        chain = chain_name(f"{self.spec.id}/{record['index']}/{uuid.uuid4().hex}")
        hosts = {instance.instance_id: instance.host for instance in self.handle.instances}
        create_chain(chain)
        self.own(record, "network.partition", chain, lambda: remove_partition(chain))
        drop_between(chain, hosts, [target], [peer for peer in hosts if peer != target])
        self.hold(record, [target], config["duration_s"])

    def _backlog_pressure(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        with self.lock:
            primary = self.primary
        record["target"] = primary
        if not self.claim(record, [primary]) or not self.reachable(record, [primary]):
            return
        limit = config["output_limit"]
        self.set_config(
            record,
            primary,
            {
                "repl-backlog-size": config["repl_backlog_size"],
                "client-output-buffer-limit": f"replica {limit} {limit} 0",
            },
        )
        self.hold(record, self.ids, config["duration_s"])

    def _compression(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        targets = record["target"] if isinstance(record["target"], list) else [record["target"]]
        if not self.claim(record, targets):
            return
        targets = self.reachable(record, targets)
        if not targets:
            return
        for target in targets:
            self.set_config(record, target, {"repl-compression": config["value"]})
        with self.lock:
            primary = self.primary
        # On the primary, a new value renegotiates every replica's link.
        affected = self.ids if primary in targets else [*targets, primary]
        self.hold(record, affected, config["duration_s"])

    def set_config(self, record: dict[str, Any], instance_id: str, settings: Mapping[str, Any]) -> None:
        """``CONFIG SET`` ``settings`` on ``instance_id``, owned (``server.config``) until the values read
        before are back; an instance whose process is gone restarts with its startup config instead.

        Restoring ``repl-compression`` also waits, up to :data:`CODEC_SETTLE_S`,
        until every link renegotiated (:func:`unsettled_links`): Valkey drops
        and reconnects the links only after the ``CONFIG SET``, and a drop
        after the wave would land in the next one (in s100166 it stopped a
        ``FAILOVER TO``). Links still unsettled then are recorded under
        ``codecs_unsettled``, for ``valkey.oracle.repl-compression`` to judge.
        """

        saved = {name: self.command(instance_id, "CONFIG", "GET", name)[1] for name in settings}

        def restore() -> None:
            if self.handle.is_alive(instance_id):
                for name, value in saved.items():
                    self.command(instance_id, "CONFIG", "SET", name, value)
            if "repl-compression" in saved:

                def links() -> tuple[bool, list[str]]:
                    problems = unsettled_links(self.handle)
                    return not problems, problems

                settled, problems = poll_until(links, timeout_s=CODEC_SETTLE_S, interval_s=0.2, context=self.context)
                if not settled:
                    record["codecs_unsettled"] = problems

        self.own(record, "server.config", instance_id, restore)
        for name, value in settings.items():
            self.command(instance_id, "CONFIG", "SET", name, value)

    def heal(self, number: int) -> None:
        """Promote, restart, and repoint until one primary leads every instance."""

        with self.lock:
            down = set(self.killed)
        if self.primary in down:
            self._heal_promote(number, down)
        for instance_id in sorted(down):
            self.restart_killed(instance_id, "heal")
        # Point the instances told to follow someone else (a repoint, or a
        # REPLICAOF that did not land) at the primary. One that was told to
        # follow the primary and does not is left for validation to fail.
        with self.lock:
            stragglers = [item for item in self.ids if item != self.primary and self.follows[item] != self.primary]
        for instance_id in stragglers:
            self.replicate(instance_id, self.primary)
        self.handle.refresh_roles()
        primary = self.primary

        def settled() -> tuple[bool, None]:
            infos = {item: self.info(item) for item in self.ids if item != primary}
            # A second primary never links by itself: stop waiting and let validation report it.
            if any(is_primary(info) for info in infos.values()):
                return True, None
            return all(follows(info, self.instance[primary]) for info in infos.values()), None

        # Validation reports what did not settle.
        poll_until(settled, timeout_s=self.heal_timeout_s, interval_s=0.2, context=self.context)

    def _heal_promote(self, number: int, down: set) -> None:
        offsets = {}
        for instance_id in self.ids:
            if instance_id in down:
                continue
            info = self.info(instance_id)
            if info:
                offsets[instance_id] = (info, _replication_offset(info) or -1)
        if not offsets:
            raise RuntimeError("the primary is down and no other instance is reachable")
        # The most advanced instance in the dead primary's history (offsets of
        # different histories do not compare); ties go to the lowest instance ID.
        with self.lock:
            replid = self.term.samples[-1][1] if self.term.samples else None
        target = max(
            offsets,
            key=lambda item: (
                str(parse_replication(offsets[item][0]).replication_id) == replid,
                offsets[item][1],
                -self.ids.index(item),
            ),
        )
        info = offsets[target][0]
        sync = {"replid": str(parse_replication(info).replication_id), "offset": _replication_offset(info)}
        old = self.primary
        promoted_at = time.time()
        self.promote_now(target)
        # The heal replaces a killed primary: the kill's durability effect.
        self.succeed(target, promoted_at, forced=True, sync=sync, reason=f"heal promoted {target}")
        promotion = {"wave": number, "from": old, "to": target, "promoted_at": promoted_at, "sync": sync}
        # Before anything restarts: a replica still following the dead primary
        # would otherwise sync from it once it is back.
        self._follow(target, promotion)
        self.heal_promotions.append(promotion)
        self.context.event_bus.publish(
            f"{self.spec.id}.promoted", source=self.spec.id, payload={"from": old, "to": target, "mode": "heal"}
        )

    def validate(self, number: int, results: list[dict[str, Any]]) -> dict[str, Any]:
        """Validate with writes paused, once every replica's offset reached the primary's."""

        primary = self.primary
        paused = self.own(None, "server.client-pause", primary, lambda: self.unpause(primary))
        try:
            self.command(primary, "CLIENT", "PAUSE", 120000, "WRITE")

            def converged() -> tuple[bool, None]:
                info = self.info(primary)
                offset = _replication_offset(info) if is_primary(info) else None
                replicas = [self.info(item) for item in self.ids if item != primary]
                # A second primary never converges: validation reports it.
                if any(is_primary(replica) for replica in replicas):
                    return True, None
                return offset is not None and all(
                    replica and _replication_offset(replica) == offset for replica in replicas
                ), None

            poll_until(converged, timeout_s=min(self.heal_timeout_s, 10.0), interval_s=0.05, context=self.context)
            # Every replica holds everything the primary acknowledged so far.
            self.sample()
            return self.state.validate()
        finally:
            # A pause that stays leaves the wave unvalidated, and the pause owned for the final cleanup.
            problem = self.discharge(paused, None, "validation")
            if problem:
                raise RuntimeError(problem)

    def unpause(self, instance_id: str) -> None:
        """The undo of the validation's ``CLIENT PAUSE``; a primary whose process is gone has none."""

        if self.handle.is_alive(instance_id):
            self.command(instance_id, "CLIENT", "UNPAUSE")

    def excusing(self, record: dict[str, Any], results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return excusing_operations(record, results)

    def wave_summary(self, number: int, results: list[dict[str, Any]]) -> dict[str, Any]:
        return {"primary": self.primary}


class ReplicationChaosAction(WaveAction):
    """Waves of replication faults on a standalone deployment, healed and validated after every wave."""

    run_cls = StandaloneWaves


_ACTION_DECLARES = wave_declares(
    {"primary": TEXT},
    {"heal_promotions": LIST},
    events={
        "{id}.promoted": ConfigSchema(
            {"from": TEXT, "to": TEXT, "mode": ConfigField(str, choices=("graceful", "force", "heal"))}
        )
    },
    loss_windows=True,
    changes=("process.stopped", "process.paused", "network.partition", "server.config", "server.client-pause"),
)

REGISTRATIONS = (
    wave_registration(
        "valkey.action.replication-chaos",
        ReplicationChaosAction,
        # How long the heal waits for every replica to link to the primary.
        {"heal_timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=3600)},
        _ACTION_DECLARES,
    ),
)
