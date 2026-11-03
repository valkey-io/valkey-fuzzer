"""One read of a deployment per validation attempt, which every check then judges.

A :class:`Snapshot` reads the topology once and each node once (INFO
replication, and in cluster mode CLUSTER INFO and CLUSTER NODES), so no two
checks judge different moments of the same failover.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import valkey

from ....protocols import RunContext
from ..cluster_nodes import parse_cluster_nodes, parse_slot_tokens
from ..models import DeploymentMode, InstanceInfo
from ..queries import Replication, pause, query_cluster_info, query_replication, response_text, valkey_client
from ..topology import DeploymentConnection, TopologyNode

logger = logging.getLogger(__name__)


@dataclass
class Snapshot:
    """The deployment as one validation attempt saw it.

    ``rows`` are the topology (:meth:`DeploymentConnection.get_current_nodes`,
    failed nodes included); the replies are keyed by ``host:port``, and a node
    that did not answer has none. ``unanswered`` holds the addresses of the
    instances whose process is running but that did not answer (with why): the
    snapshot did not see them, unlike an instance whose process is not
    running, which the checks judge. That holds for a node its peers mark
    failed too: whether a running node hung is liveness's to judge. ``caught_up`` holds, for a replica, its
    offset as read after its primary's: a primary sends its replicas a
    replication PING (14 bytes) every few seconds, so a replica read before its
    primary can look behind by a PING it applies a moment later.
    """

    mode: DeploymentMode
    instances: list[InstanceInfo]
    rows: list[TopologyNode]
    replication: dict[str, Replication] = field(default_factory=dict)
    cluster_info: dict[str, dict[str, str]] = field(default_factory=dict)
    cluster_nodes: dict[str, list[dict]] = field(default_factory=dict)
    unanswered: dict[str, str] = field(default_factory=dict)
    caught_up: dict[str, int] = field(default_factory=dict)

    @property
    def live_rows(self) -> list[TopologyNode]:
        """The nodes the topology does not report failed."""
        return [row for row in self.rows if not row.failed]

    def views(self) -> dict[str, list[dict]]:
        """Each live node's CLUSTER NODES, by address; a node that did not answer has none."""
        addresses = (row.address for row in self.live_rows)
        return {address: self.cluster_nodes[address] for address in addresses if address in self.cluster_nodes}

    def slot_claims(self) -> dict[int, set[str]]:
        """Every slot some live node's view assigns, and the primaries the views give it.

        Views can disagree for a gossip round: one that has not yet heard of a
        failover still gives the slot to the old, failed primary. That is no
        split brain (see :func:`~..cluster_nodes.split_slots`).
        """
        claims: dict[int, set[str]] = {}
        for view in self.views().values():
            for node in view:
                if node["is_primary"]:
                    for slot in parse_slot_tokens(node["slots"]):
                        claims.setdefault(slot, set()).add(node["node_id"])
        return claims

    def primary_of(self, replica: TopologyNode) -> Optional[str]:
        """The node ID of the primary a replica follows in its own view (its ``myself`` row)."""
        myself = next((node for node in self.cluster_nodes.get(replica.address, ()) if node["is_myself"]), None)
        return myself["primary_id"] if myself and myself["is_replica"] else None


def take_snapshot(connection: DeploymentConnection, timeout: float, context: Optional[RunContext] = None) -> Snapshot:
    """Read the topology, then each of its nodes once, waiting up to ``timeout`` seconds for each.

    Replicas are read before primaries, so a primary's replication offset is
    never older than its replicas'. In standalone mode the topology itself
    holds each instance's INFO replication, so nothing is read twice. A
    running instance that did not answer, or a cluster whose topology no
    node answered, is recorded in ``unanswered``.
    """
    rows = connection.get_current_nodes(include_failed=True, timeout=timeout)
    snapshot = Snapshot(mode=connection.mode, instances=list(connection.instances), rows=rows)
    cluster = connection.mode is DeploymentMode.CLUSTER
    running = {f"{instance.host}:{instance.port}" for instance in connection.instances if instance.is_running}
    if cluster and not rows:
        snapshot.unanswered.update(
            dict.fromkeys(sorted(running), f"no node answered CLUSTER NODES within {timeout:g}s")
        )
    # When each node's INFO replication was read: the topology's order in standalone mode, this loop's otherwise.
    read_at = {row.address: index for index, row in enumerate(rows) if row.replication}
    for row in sorted(rows, key=lambda row: row.role != "replica"):
        address = row.address
        read_at.setdefault(address, len(rows) + len(read_at))
        if row.replication is not None:
            snapshot.replication[address] = row.replication
        if not cluster and row.failed:
            if address in running:
                snapshot.unanswered[address] = f"did not answer INFO replication within {timeout:g}s"
            continue
        if address in snapshot.replication:
            continue
        try:
            with valkey_client(row.host, row.port, timeout) as client:
                snapshot.replication[address] = query_replication(client)
                if cluster:
                    snapshot.cluster_info[address] = query_cluster_info(client)
                    reply = response_text(client.execute_command("CLUSTER", "NODES"))
                    snapshot.cluster_nodes[address] = parse_cluster_nodes(reply)
        except (valkey.ValkeyError, OSError) as exc:
            logger.debug("%s did not answer: %s", address, exc)
            if address in running:
                snapshot.unanswered[address] = f"{type(exc).__name__}: {exc}"
    _catch_up(snapshot, read_at, timeout, context)
    return snapshot


def _catch_up(snapshot: Snapshot, read_at: dict[str, int], timeout: float, context: Optional[RunContext]) -> None:
    """Record in ``caught_up`` each replica's offset as read after its primary's, re-reading a replica that was
    behind until it reaches the primary's offset or ``timeout`` passes.

    A replica that reaches its primary's offset was not behind it; one that
    stays below it is, and the replication check judges it so.
    """
    deadline = time.monotonic() + timeout
    by_id = {row.node_id: row for row in snapshot.rows}
    by_address = {row.address: row for row in snapshot.rows}
    for row in snapshot.rows:
        address = row.address
        info = snapshot.replication.get(address)
        if row.role != "replica" or info is None or not info.link_up or info.replica_offset is None:
            continue
        if snapshot.mode is DeploymentMode.CLUSTER:
            primary = by_id.get(snapshot.primary_of(row))
        else:
            primary = by_address.get(f"{info.primary_host}:{info.primary_port}")
        primary_address = primary.address if primary else None
        primary_info = snapshot.replication.get(primary_address) if primary_address else None
        if primary_info is None or primary_info.offset is None:
            continue
        offset = info.replica_offset if read_at.get(address, -1) > read_at.get(primary_address, -1) else None
        while (offset is None or offset < primary_info.offset) and time.monotonic() < deadline:
            try:
                with valkey_client(row.host, row.port, timeout) as client:
                    offset = query_replication(client).replica_offset
            except (valkey.ValkeyError, OSError) as exc:
                logger.debug("%s did not answer a re-read: %s", address, exc)
                break
            if offset is None or offset < primary_info.offset:
                pause(context, 0.1, f"{address} to catch up")
        if offset is not None:
            snapshot.caught_up[address] = offset
