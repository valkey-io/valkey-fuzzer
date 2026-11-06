"""Cluster chaos: failovers and process kills in waves, validated after every wave.

``valkey.action.cluster-chaos`` is the cluster mode of the wave engine
(:mod:`.waves`). Its ``waves`` hold ``failover {shard, force, timeout_s}``
(``CLUSTER FAILOVER``, or ``FORCE``, on a replica of the shard's current
primary, never one killed on purpose) and ``kill {target, signal}``
operations; a wave fails over at most once per shard. A kill never takes a
live shard's last live member (a replica is one only once it holds a
replication offset of the shard) and keeps the live slot-owning primaries at a
majority of the planned shards; an unsafe kill is skipped. The cluster heals
itself (Valkey fails a killed primary's shard over): after every wave the
action validates the deployment with the killed nodes marked as stopped on
purpose, and it stops after a critical failure (or any failure with
``blocking_on_failure``). A failed failover is excused only by a kill that
could have caused it (:func:`excusing_kills`). ``valkey.oracle.deployment-state``
validates the deployment once more at the end, with the action's kills and keys.
"""

import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.schema import ConfigField

from .schemas import FLAG, INTEGER
from .wave_evidence import wave_declares
from .wave_operations import CLUSTER
from .wave_targets import Member
from .waves import WaveAction, WaveRun, wave_registration


def excusing_kills(record: Mapping[str, Any], kills: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The successful kills that could have made ``record``'s failover fail, and why.

    Only a kill that landed before the failover finished counts, and then:

    - a kill in the failover's shard, in any wave: it can take the replica to
      promote, the primary a graceful failover needs, or the shard's last
      replica (a promoted replica whose old primary died has none);
    - a primary killed in the same wave: until a replica replaces it, it is a
      missing election voter and its replicas have no link, and a failover
      waits for every replica link in the cluster. A primary killed in an
      earlier wave was replaced before that wave validated.

    A replica killed in another shard changes neither.
    """

    excused = []
    for kill in kills:
        if not kill["success"] or kill["at"] >= record["finished_at"]:
            continue
        if kill["shard"] == record["shard"]:
            reason = "same shard"
        elif kill["role"] == "primary" and kill["wave"] == record["wave"]:
            reason = "primary in the same wave"
        else:
            continue
        excused.append({"instance_id": kill["instance_id"], "operation": kill["operation"], "reason": reason})
    return excused


class ClusterWaves(WaveRun):
    """The cluster mode of the wave engine: Valkey heals the cluster, and the action validates it.

    Members are the live view (``handle.live_instances()``: live roles, and
    shards from slot ownership). A failover claims its shard until it
    settled, and a kill waits for that claim and then confirms against a
    fresh view, so it is judged by the roles the failover left (a replica
    the failover just promoted is a primary by then).
    """

    mode = CLUSTER
    quorum = True
    # Valkey heals each wave; a non-critical failure leaves the next wave something to test.
    halt_on_failure = False

    def prepare(self) -> None:
        keys = self.spec.config["keys_per_slot"]
        self.slots_seeded = self.handle.seed_slots(keys, seed=self.context.plan.seed) if keys else True

    def outcome(self, records: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        failovers = [record for record in records if record["op"] == "failover"]
        done = sum(record["success"] for record in failovers)
        return f", {done} of {len(failovers)} failover(s)", {"slots_seeded": self.slots_seeded}

    def members(self) -> list[Member]:
        """The live members. A replica belongs to its shard only while it holds the shard's data, a replication
        offset from its primary or a cached one: one that just migrated to an orphaned primary holds another
        shard's data until its first sync (INFO's offset is 1 then), and a kill that left it as the shard's last
        member would lose the shard. A replica whose primary died keeps its offset, and its link is down."""

        return [
            Member(
                node["instance_id"],
                node["role"],
                node["shard_id"] if node["role"] != "replica" or self._synced(node["instance_id"]) else None,
            )
            for node in self.handle.live_instances()
        ]

    def _synced(self, instance_id: str) -> bool:
        try:
            with self.handle.new_client(instance_id, timeout=2.0) as client:
                info = client.info("replication")
        except Exception:
            return False
        return int(info.get("slave_repl_offset") or 0) > 1

    kill_view = members

    def role_of(self, instance_id: str, members: Optional[Sequence[Member]]) -> str:
        # The role right before the kill tells validation what the shard lost.
        return next((member.role for member in members or () if member.instance_id == instance_id), "unknown")

    def _failover(self, record: dict[str, Any], config: dict[str, Any]) -> None:
        # Kills of the shard's members wait until it settled (see ``Operation.mutates``).
        if not self.claim(record, [], shards=(config["shard"],)):
            return
        record["applied_at"] = time.time()
        result = self.handle.failover(
            config["shard"],
            force=config["force"],
            timeout=config["timeout_s"],
            stopped_instances=sorted(self.down()),
            rng=self.rngs[record["index"]],
        )
        if result is None:
            # Nothing to fail over: no failover ran.
            del record["applied_at"]
            record["skipped"] = (
                f"shard {config['shard']} was stopped whole on purpose, or every replica of its primary was"
            )
        record["success"] = bool(result)

    def heal(self, number: int) -> None:
        for kill in self.kills:
            if kill["wave"] == number and kill["success"]:
                self.state.mark_stopped(kill["instance_id"], kill["role"], shard=kill["shard"])

    def validate(self, number: int, results: list[dict[str, Any]]) -> dict[str, Any]:
        done = [record for record in results if record["op"] == "failover" and record["success"]]
        # Log checks need at least one completed failover to look for.
        return self.state.validate(done or None)

    def excusing(self, record: dict[str, Any], results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return excusing_kills(record, [kill for kill in self.kills if kill["success"]])

    def wave_summary(self, number: int, results: list[dict[str, Any]]) -> dict[str, Any]:
        return {"kills": sum(kill["wave"] == number and kill["success"] for kill in self.kills)}


class ClusterChaosAction(WaveAction):
    """Failovers and process kills in waves, validated after every wave."""

    run_cls = ClusterWaves


# The cluster heals itself: a killed node stays down, so the action owns no change.
_CHAOS_DECLARES = wave_declares({"kills": INTEGER}, {"slots_seeded": FLAG})

REGISTRATIONS = (
    wave_registration(
        "valkey.action.cluster-chaos",
        ClusterChaosAction,
        # Keys written into every hash slot before the first wave.
        {"keys_per_slot": ConfigField(int, default=5, minimum=0, maximum=100)},
        _CHAOS_DECLARES,
    ),
)
