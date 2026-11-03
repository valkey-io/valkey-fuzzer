"""The deployment's current topology, read from its instances: :class:`DeploymentConnection` and its rows."""

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from typing import Optional

from .cluster_nodes import parse_slot_tokens
from .models import DeploymentMode, InstanceInfo
from .queries import Replication, query_cluster_nodes, query_or_none, query_replication


@dataclass(frozen=True)
class TopologyNode:
    """One instance in the deployment's current topology.

    ``node_id`` is the cluster node ID in cluster mode and the instance ID in
    standalone mode; ``instance_id`` is the instance it is, None for a node
    the deployment did not start. ``failed`` means its peers mark it failed
    or disconnected (cluster mode), or it did not answer (standalone mode).
    """

    node_id: str
    instance_id: Optional[str]
    host: str
    port: int
    role: str  # "primary" or "replica"
    shard_id: Optional[int]
    failed: bool
    marked_fail: bool = False
    primary_id: Optional[str] = None  # a cluster replica's primary's node ID
    # The hash slots a cluster primary owns; None outside cluster mode.
    slots: Optional[tuple[int, ...]] = None
    # Standalone mode: the INFO replication the role came from, so a state check need not ask again.
    replication: Optional[Replication] = None

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass
class DeploymentConnection:
    """How to reach a formed deployment, and its current view of its instances."""

    instances: list[InstanceInfo]  # the live instance records, shared with the deployment service
    deployment_id: str
    mode: DeploymentMode = DeploymentMode.CLUSTER

    def __post_init__(self):
        self.mode = DeploymentMode(self.mode)
        self.startup_nodes = [{"host": instance.host, "port": instance.port} for instance in self.instances]
        # The plan's slot ranges, fixed now: instance records are updated with
        # live roles later, and a demoted primary must keep naming its shard.
        self._planned_slot_ranges = tuple(
            (planned.shard_id, planned.slot_start, planned.slot_end)
            for planned in self.instances
            if planned.slot_start is not None and planned.slot_end is not None
        )
        self._planned_port_shards = {planned.port: planned.shard_id for planned in self.instances}

    def get_current_nodes(self, include_failed: bool = True, timeout: float = 2.0) -> list[TopologyNode]:
        """The current instances, from the deployment's authoritative source, waiting up to ``timeout`` seconds
        for each query."""
        if self.mode != DeploymentMode.CLUSTER:
            return self._get_current_non_cluster_nodes(include_failed, timeout)

        return self._get_current_cluster_nodes(include_failed, timeout)

    def _planned_shard_of(self, slots: list[int], port: int) -> Optional[int]:
        """The planned shard whose slot range holds most of ``slots``; by planned port when the plan has no ranges.

        Only the plan's slot ranges and the live slot ownership decide it, never
        an instance's current role: a planned primary that was demoted still
        names its shard's range.
        """
        if not self._planned_slot_ranges:
            return self._planned_port_shards.get(port)
        slots = sorted(slots)
        votes: dict[int, int] = {}
        for shard_id, start, end in self._planned_slot_ranges:
            votes[shard_id] = votes.get(shard_id, 0) + bisect_right(slots, end) - bisect_left(slots, start)
        # Most slots wins; a tie goes to the lowest shard.
        return min(votes, key=lambda shard: (-votes[shard], shard))

    def _get_current_cluster_nodes(self, include_failed: bool, timeout: float = 2.0) -> list[TopologyNode]:
        """The current cluster nodes, from the CLUSTER NODES of the first node that answers; none when none does.

        A node's ``shard_id`` comes from the live topology, not the plan: a
        primary belongs to the planned shard whose hash slots it owns, and a
        replica to its primary's shard. Replica migration and failovers move
        instances between shards, so an instance's planned shard can be wrong.
        A primary without slots (e.g. one stopped on purpose whose replica took
        over its slots) and its replicas hold no shard's data: ``shard_id`` None.
        ``instance_id`` is the instance with the node's cluster node ID, else its address.
        """
        by_cluster_id = {info.cluster_node_id: info.instance_id for info in self.instances if info.cluster_node_id}
        by_address = {(info.host, info.port): info.instance_id for info in self.instances}
        for info in self.startup_nodes:
            parsed_nodes = query_cluster_nodes(info, timeout=timeout)
            if not parsed_nodes:
                continue
            slots_of = {node["node_id"]: parse_slot_tokens(node["slots"]) for node in parsed_nodes}
            node_id_to_shard = {
                node["node_id"]: self._planned_shard_of(slots_of[node["node_id"]], node["port"])
                for node in parsed_nodes
                if node["is_primary"] and slots_of[node["node_id"]]
            }
            for node in parsed_nodes:
                if node["is_replica"] and node["primary_id"]:
                    node_id_to_shard[node["node_id"]] = node_id_to_shard.get(node["primary_id"])

            current_instances = []
            for node in parsed_nodes:
                is_failed = node["is_fail"] or node["link_state"] == "disconnected"
                if not include_failed and is_failed:
                    continue

                current_instances.append(
                    TopologyNode(
                        node_id=node["node_id"],
                        instance_id=by_cluster_id.get(node["node_id"]) or by_address.get((node["host"], node["port"])),
                        host=node["host"],
                        port=node["port"],
                        role="primary" if node["is_primary"] else "replica",
                        primary_id=node["primary_id"] if node["is_replica"] else None,
                        shard_id=node_id_to_shard.get(node["node_id"]),
                        failed=is_failed,
                        marked_fail="fail" in node["flags"].split(","),
                        slots=tuple(slots_of[node["node_id"]]),
                    )
                )

            return current_instances
        return []

    def _get_current_non_cluster_nodes(self, include_failed: bool, timeout: float = 2.0) -> list[TopologyNode]:
        """Inspect a standalone deployment's primary and replicas through direct clients.

        An instance that does not answer is ``failed`` here whether its process
        runs or not; :func:`~.validation.snapshot.take_snapshot` tells the two
        apart.
        """
        current_instances = []
        for instance in self.instances:
            replication = (
                query_or_none(instance.host, instance.port, timeout, query_replication) if instance.is_running else None
            )
            is_connected = replication is not None
            if not include_failed and not is_connected:
                continue

            role = (replication and replication.role) or instance.role

            current_instances.append(
                TopologyNode(
                    node_id=instance.instance_id,
                    instance_id=instance.instance_id,
                    host=instance.host,
                    port=instance.port,
                    role=role,
                    shard_id=instance.shard_id,
                    failed=not is_connected,
                    replication=replication,
                )
            )

        return current_instances
