"""Wave targets: the live members an operation's target resolves against, and the kill-safety rules."""

import threading
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Optional

from .wave_operations import OPERATIONS


@dataclass(frozen=True)
class Refusal:
    """Why an operation does not run, under the record key it is reported as: ``missing`` when it has no target
    at all (the wave is untested), ``skipped`` when the safety rules refused every candidate."""

    kind: str
    reason: str


@dataclass(frozen=True)
class Member:
    """A live instance: its role (None when it does not answer) and live shard (None: it holds no slots)."""

    instance_id: str
    role: Optional[str]
    shard: Optional[int]


class TargetSelector:
    """Resolves targets against the live members and keeps kills safe.

    A kill never takes a shard's last live member: shards are the live ones
    (a replica belongs to its primary's shard), so a replica that migrated
    counts where it replicates now. With ``quorum`` it also keeps a
    majority of the planned shards' primaries live; a dead primary still
    counts toward the failover majority until a replica replaces it, so the
    majority is of the planned shards, and only slot owners count as live.

    ``begin_wave`` takes the members at the start of a wave and forgets the
    previous wave's picks; ``select`` resolves one operation's target in
    plan order and reserves a kill target, so two kills of a wave cannot
    both take a shard's last member; ``confirm`` checks a kill again right
    before the signal (with a fresh view, when given, because failovers
    change roles in the meantime); ``land`` marks a target down,
    ``release`` forgets a pick whose kill did not happen, and ``restore`` a
    down instance that runs again.
    """

    def __init__(self, members: Sequence[Member], *, quorum: bool):
        shards = {member.shard for member in members}
        self._quorum = len(shards) // 2 + 1 if quorum else None
        self._members = list(members)
        self._reserved: set[str] = set()
        self._down: set[str] = set()
        self._disrupted: set[str] = set()
        self._lock = threading.Lock()

    @property
    def down(self) -> frozenset:
        with self._lock:
            return frozenset(self._down)

    def begin_wave(self, members: Sequence[Member]) -> None:
        with self._lock:
            self._members = list(members)
            self._reserved = set()
            self._disrupted = set()

    def land(self, instance_id: str) -> None:
        with self._lock:
            self._reserved.discard(instance_id)
            self._down.add(instance_id)

    def release(self, instance_id: str) -> None:
        with self._lock:
            self._reserved.discard(instance_id)

    def restore(self, instance_id: str) -> None:
        with self._lock:
            self._down.discard(instance_id)

    def _candidates(self, target: Any) -> list[Member]:
        """Members ``target`` names, not down, in instance ID order: a role, ``any``, or instance IDs."""

        members = sorted(
            (member for member in self._members if member.instance_id not in self._down),
            key=lambda member: member.instance_id,
        )
        if target == "any":
            return [member for member in members if member.role is not None]
        if target in ("primary", "replica"):
            return [member for member in members if member.role == target]
        wanted = {target} if isinstance(target, str) else set(target)
        return [member for member in members if member.instance_id in wanted]

    def _safe(self, candidates: list[Member], stopped: set[str]) -> tuple[list[Member], Optional[str]]:
        alive = [member for member in self._members if member.instance_id not in stopped | self._down]
        shard_safe = [
            candidate
            for candidate in candidates
            if candidate.shard is None
            or any(member.shard == candidate.shard and member.instance_id != candidate.instance_id for member in alive)
        ]
        if not shard_safe:
            return [], "shard-safe"
        if self._quorum is None:
            return shard_safe, None
        voters = {member.instance_id for member in alive if member.role == "primary" and member.shard is not None}
        quorum_safe = [
            candidate
            for candidate in shard_safe
            if candidate.role != "primary" or len(voters - {candidate.instance_id}) >= self._quorum
        ]
        if not quorum_safe:
            return [], "quorum-safe"
        return quorum_safe, None

    @staticmethod
    def _described(target: Any) -> str:
        if target == "any":
            return "instance"
        return target if isinstance(target, str) else " or ".join(target)

    def select(self, kind: str, target: Any, rng: Any) -> tuple[Optional[Member], Optional[Refusal]]:
        """``(member, None)`` for operation ``kind`` on ``target``, or ``(None, refusal)``."""

        operation = OPERATIONS[kind]
        with self._lock:
            candidates = self._candidates(target)
            if operation.role is not None:
                candidates = [member for member in candidates if member.role == operation.role]
            if not candidates:
                return None, Refusal("missing", f"no live {self._described(target)} to {kind}")
            if operation.removes:
                candidates = [member for member in candidates if member.instance_id not in self._reserved]
                if not candidates:
                    return None, Refusal("missing", f"every live {self._described(target)} is already killed")
                candidates, reason = self._safe(candidates, self._reserved)
                if not candidates:
                    return None, Refusal("skipped", f"no {reason} target")
                choice = rng.choice(candidates)
                self._reserved.add(choice.instance_id)
            elif operation.role is not None:
                # Prefer a member this wave leaves alone; one it disrupts is
                # still picked, and the operation is then refused and excused.
                choice = rng.choice(
                    [member for member in candidates if member.instance_id not in self._disrupted] or candidates
                )
            else:
                choice = rng.choice(candidates)
            if operation.disrupts:
                self._disrupted.add(choice.instance_id)
            return choice, None

    def select_all(self) -> list[str]:
        """Every instance that is not down, for an operation on ``all``."""

        with self._lock:
            return sorted(member.instance_id for member in self._members if member.instance_id not in self._down)

    def select_other(self, target: str, other_than: str, rng: Any) -> tuple[Optional[str], Optional[Refusal]]:
        """A live ``target`` (a role or an ID) other than ``other_than``, e.g. where a repoint sends a replica."""

        with self._lock:
            candidates = [member for member in self._candidates(target) if member.instance_id != other_than]
        if not candidates:
            return None, Refusal("missing", f"no live {target} other than {other_than} to repoint it to")
        return rng.choice(candidates).instance_id, None

    def confirm(self, instance_id: str, members: Optional[Sequence[Member]] = None) -> Optional[Refusal]:
        """``None`` if killing a picked target is still safe, else why not (``members``: a fresh view)."""

        with self._lock:
            if members is not None:
                self._members = list(members)
            if instance_id in self._down:
                return Refusal("missing", f"{instance_id} is already down")
            current = next((member for member in self._members if member.instance_id == instance_id), None)
            if current is None:
                # Already out of the live view: killing it takes nothing more away.
                return None
            _, reason = self._safe([current], self._reserved - {instance_id})
            return Refusal("skipped", f"no longer {reason}") if reason else None
