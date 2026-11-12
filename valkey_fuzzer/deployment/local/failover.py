"""Failover: promote a replica of a shard's current primary and wait for the cluster to settle."""

import logging
import random
import time
from collections.abc import Collection
from typing import Optional

import valkey

from ...protocols import RunContext
from .models import LocalDeploymentError
from .queries import query_or_none, query_replication, valkey_client, wait_until
from .topology import DeploymentConnection, TopologyNode

logger = logging.getLogger(__name__)

# How often a failover looks again for a replica that has synced with the shard's primary.
SYNC_POLL_S = 0.2


def _shard_primary(nodes: list[TopologyNode], shard_id: int) -> Optional[TopologyNode]:
    return next((node for node in nodes if node.role == "primary" and node.shard_id == shard_id), None)


def replica_sync_problem(replica: TopologyNode, primary: TopologyNode) -> Optional[str]:
    """Why ``replica`` holds no synced copy of ``primary``'s data, or None when it does.

    Synced means its replication link to that primary is up, no sync is in
    progress, and its offset is past zero. ``CLUSTER FAILOVER FORCE`` checks
    none of this by design, so a replica that just migrated to an orphaned
    primary (offset 0, still loading its first sync) would win and replace the
    shard's data with nothing.
    """
    try:
        with valkey_client(replica.host, replica.port, timeout=2.0) as client:
            info = query_replication(client)
    except (valkey.ValkeyError, OSError) as exc:
        return f"unreachable ({type(exc).__name__})"
    if info.role != "replica":
        return f"role {info.role}"
    if (info.primary_host, info.primary_port) != (primary.host, primary.port):
        return f"replicates {info.primary_host}:{info.primary_port}"
    if not info.link_up:
        return "link down"
    if info.sync_in_progress:
        return "sync in progress"
    if (info.replica_offset or 0) <= 0:
        return "offset 0"
    return None


def _responds_to_ping(node: TopologyNode) -> bool:
    return bool(query_or_none(node.host, node.port, 1.0, lambda client: client.ping()))


def _first_synced(
    replicas: list[TopologyNode],
    primary: TopologyNode,
    rng: random.Random,
    timeout: float,
    stopped_ports: Collection[int] = (),
    context: Optional[RunContext] = None,
) -> tuple[Optional[TopologyNode], dict[int, str]]:
    """The first replica, in a random order, that has synced with ``primary`` (see :func:`replica_sync_problem`),
    waiting up to ``timeout`` seconds for one while the primary answers; ``(None, why each was refused)`` when
    none has, the primary's port among them once it stopped answering.

    The seeded ``rng`` tries them in the same order every run. A replica sent
    ``SIGTERM`` still answers until it exits, and Valkey still accepts
    ``CLUSTER FAILOVER`` on it, so one in ``stopped_ports`` is skipped. Every
    replica is shuffled either way, so skipping one does not change the order
    of the others.
    """
    # Shuffle from port order: ports, unlike cluster node IDs, are the same in every run.
    order = sorted(replicas, key=lambda node: (node.host, node.port))
    rng.shuffle(order)
    candidates = [node for node in order if node.port not in stopped_ports]
    chosen: list[TopologyNode] = []
    refused: dict[int, str] = {}
    deadline = time.monotonic() + timeout

    def decided() -> bool:
        refused.clear()
        for node in candidates:
            problem = replica_sync_problem(node, primary)
            if problem is None:
                chosen.append(node)
                return True
            refused[node.port] = problem
        if candidates and time.monotonic() < deadline and not _responds_to_ping(primary):
            # Nothing syncs from a primary that is gone.
            refused[primary.port] = "the primary does not answer"
            return True
        return not candidates

    wait_until(decided, timeout=timeout, interval=SYNC_POLL_S, what="a synced replica", context=context)
    return (chosen[0], {}) if chosen else (None, dict(refused))


def failover_shard(
    connection: DeploymentConnection,
    shard_id: int,
    *,
    force: bool = False,
    timeout: float,
    stopped_instances: Collection[str] = (),
    rng: random.Random,
    context: Optional[RunContext] = None,
) -> Optional[bool]:
    """Promote a replica of ``shard_id``'s current primary and wait up to ``timeout`` seconds for Valkey to settle
    it: True once it has, False when Valkey did not fail the shard over, None when there was nothing to fail
    over: the shard was stopped whole on purpose, or every replica of its primary was.

    ``timeout`` is Valkey's, counted from when the command is sent, or from
    when a primary that failed (FAIL, not one node's PFAIL) or stopped
    answering is left to Valkey's own failover. Before that the harness
    waits, up to another ``timeout``, for a replica that synced with the
    primary. ``stopped_instances`` are instance IDs stopped on purpose. The
    seeded ``rng`` makes the replica choice replayable. What cannot be
    observed raises instead (no node answered, no replica to promote or none
    synced within the harness's wait, a command that failed, or the run's
    deadline or a cancellation of ``context``'s call during a wait, see
    :func:`~.queries.pause`), so the caller can tell it from Valkey's doing.
    """
    logger.info("Executing failover on shard %s", shard_id)
    stopped_ports = {node.port for node in connection.instances if node.instance_id in set(stopped_instances)}
    # One view of the topology: after a previous failover the shard's primary may be a promoted replica.
    rows = connection.get_current_nodes(include_failed=True)
    if not rows:
        raise LocalDeploymentError(f"no cluster node answered CLUSTER NODES; shard {shard_id} was not failed over")
    # One node's PFAIL or lost link is no failure yet: a live primary only it suspects gets the command, and one
    # that is gone stops answering while the harness waits for a synced replica.
    live = [node for node in rows if not node.marked_fail]
    target = _shard_primary(live, shard_id)

    def settled(old_primary: str) -> bool:
        return wait_until_settled(
            connection,
            timeout,
            shard_id=shard_id,
            old_primary=old_primary,
            stopped_ports=stopped_ports,
            context=context,
        )

    if target is None:
        failed_primary = _shard_primary(rows, shard_id)
        if failed_primary is None:
            raise LocalDeploymentError(f"shard {shard_id} has no primary in the cluster's topology")
        # Valkey's own failover promotes a live replica of a failed primary. With none left, a shard stopped
        # whole on purpose is down, which is not a failed failover.
        if any(
            node.role == "replica" and node.primary_id == failed_primary.node_id and node.port not in stopped_ports
            for node in live
        ):
            return settled(failed_primary.node_id)
        if failed_primary.port in stopped_ports:
            return None
        # Nothing is left to promote, so no failover can be tested; valkey.oracle.liveness judges the dead primary.
        raise LocalDeploymentError(
            f"the primary of shard {shard_id} failed without being stopped, and no replica of it is live"
        )

    replica_nodes = [node for node in rows if node.role == "replica" and node.primary_id == target.node_id]
    if not replica_nodes:
        raise LocalDeploymentError(f"the primary of shard {shard_id} has no replica to promote")

    # A random replica that holds the primary's data, waiting for one to sync.
    replica, refused = _first_synced(replica_nodes, target, rng, timeout, stopped_ports, context)
    if not replica:
        if target.port in refused:
            logger.warning(
                "The primary of shard %s stopped answering before a replica synced: %s; "
                "waiting for Valkey's own failover",
                shard_id,
                refused,
            )
            return settled(target.node_id)
        if all(node.port in stopped_ports for node in replica_nodes):
            return None
        raise LocalDeploymentError(
            f"no replica of the primary of shard {shard_id} synced within {timeout:g}s, so no failover was sent: "
            f"{refused}"
        )

    logger.info(
        "Executing CLUSTER %s from the replica at port %s", "FAILOVER FORCE" if force else "FAILOVER", replica.port
    )
    with valkey_client(replica.host, replica.port, timeout=5.0) as replica_client:
        replica_client.execute_command("CLUSTER", "FAILOVER", *(["FORCE"] if force else []))
    return settled(target.node_id)


def wait_until_settled(
    connection: DeploymentConnection,
    timeout: float,
    *,
    shard_id: int,
    old_primary: Optional[str],
    stopped_ports: Collection[int] = (),
    context: Optional[RunContext] = None,
) -> bool:
    """Wait up to ``timeout`` seconds for the shard's failover to settle; True once it has, False when it did not.

    Settled means the shard has one live primary, not ``old_primary``,
    and every live replica of the shard not stopped on purpose is linked
    to it. Other shards do not count: their link health is the state
    validation's to judge, and another shard's primary killed in the same
    wave would otherwise fail this failover. When the last look could not
    read the shard, whether it settled is unknown, and this raises.
    """
    logger.info("Waiting for the failover of shard %s to settle (timeout: %.2fs)", shard_id, timeout)
    unread: list[str] = []

    def done() -> bool:
        unread.clear()
        try:
            return _shard_settled(connection, shard_id, old_primary, stopped_ports)
        except (valkey.ValkeyError, OSError, LocalDeploymentError) as exc:
            unread.append(f"{type(exc).__name__}: {exc}")
            return False

    if wait_until(done, timeout=timeout, interval=0.5, what=f"shard {shard_id} failover", context=context):
        return True
    if unread:
        raise LocalDeploymentError(f"shard {shard_id}'s failover could not be read at the end of the wait: {unread[0]}")
    logger.warning("Shard %s did not settle within %.2fs", shard_id, timeout)
    return False


def _shard_settled(
    connection: DeploymentConnection, shard_id: int, old_primary: Optional[str], stopped_ports: Collection[int]
) -> bool:
    live = connection.get_current_nodes(include_failed=False)
    if not live:
        raise LocalDeploymentError("no cluster node answered CLUSTER NODES")
    members = [node for node in live if node.shard_id == shard_id and node.port not in stopped_ports]
    primaries = [node for node in members if node.role == "primary"]
    if len(primaries) != 1 or primaries[0].node_id == old_primary:
        return False
    primary = primaries[0]
    for replica in members:
        if replica is primary:
            continue
        with valkey_client(replica.host, replica.port, timeout=2.0) as client:
            info = query_replication(client)
        if info.role != "replica" or not info.link_up or info.primary_port != primary.port:
            return False
    return True
