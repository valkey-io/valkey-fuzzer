"""Replication check: replica links, lag, offsets, upstreams, and per-shard redundancy."""

import logging
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Optional

from ..models import DeploymentMode
from ..queries import Replication
from ..topology import TopologyNode
from .config import ReplicationValidationConfig
from .results import ReplicaLagInfo, ReplicationValidation
from .snapshot import Snapshot

logger = logging.getLogger(__name__)


def check_replication(
    snapshot: Snapshot, config: ReplicationValidationConfig, stopped: Collection[str] = ()
) -> ReplicationValidation:
    """Check every replica's link, lag, and offset, and every shard's live replicas.

    ``stopped`` holds the addresses of the instances stopped on purpose: their
    replicas' links may be down, and they may be missing. Whether an instance
    that does not answer was stopped is the role check's to judge; here it
    only counts against its shard's redundancy. Offsets that could not be
    compared fail the check without judging it (``inconclusive``), unless
    something else failed.
    """
    stopped = set(stopped)
    all_nodes = snapshot.rows
    if not all_nodes:
        logger.error("Unable to fetch any nodes from cluster for replication validation")
        return ReplicationValidation(success=False, error_message="Unable to contact cluster - no nodes reachable")

    replica_nodes = [node for node in all_nodes if node.role == "replica"]
    if not replica_nodes:
        if config.min_replicas_per_shard > 0:
            # Replicas are expected: all of them have disappeared.
            message = (
                f"All replicas missing: Expected at least {config.min_replicas_per_shard} replica(s) per shard "
                f"but found 0 replicas in entire cluster. This indicates complete replica loss."
            )
            logger.error(message)
            return ReplicationValidation(success=False, error_message=message)
        logger.info("No replicas configured in cluster (none expected)")
        return ReplicationValidation(success=True, all_replicas_synced=True, max_lag=0.0)

    primary_nodes = {node.node_id: node for node in all_nodes if node.role == "primary"}
    standalone = snapshot.mode is DeploymentMode.STANDALONE
    errors: list[str] = []
    expected_primary = None
    if standalone:
        try:
            expected_primary = _recorded_primary(snapshot)
        except ValueError as exc:
            return ReplicationValidation(success=False, error_message=str(exc))
        if expected_primary.failed:
            errors.append(f"Recorded primary {expected_primary.address} is not connected")
        if expected_primary.node_id not in primary_nodes:
            errors.append(f"Recorded primary {expected_primary.node_id} is not reported as a primary")

    links = _replica_links(snapshot, config, replica_nodes, primary_nodes, expected_primary)
    errors.extend(links.errors)
    # Each primary's own count of connected replicas: advisory in cluster mode, where ownership moves during a
    # failover, and a failure for a standalone deployment's fixed primary and replicas.
    errors.extend(
        _cross_check_primaries(snapshot, primary_nodes, links.followed, links.disconnected, strict=standalone)
    )

    expected_link_down = _orphaned_replicas(all_nodes, replica_nodes, stopped, links.link_down)
    unexplained_link_down = sorted(links.link_down - expected_link_down)
    if unexplained_link_down:
        errors.append(f"Replicas whose replication link is down while their primary is live: {unexplained_link_down}")

    redundancy_lost = False
    unexpected_down = [replica for replica in links.disconnected if replica not in expected_link_down]
    if config.min_replicas_per_shard > 0:
        redundancy_error, redundancy_lost = _redundancy_problem(
            all_nodes, replica_nodes, unexpected_down, stopped, config.min_replicas_per_shard, links.link_down
        )
        if redundancy_error:
            errors.append(redundancy_error)
    elif len(links.disconnected) == len(replica_nodes) and set(unexpected_down) - stopped - links.link_down:
        redundancy_lost = True
        errors.append("All replicas are disconnected")

    all_replicas_synced = not (links.lagging or links.disconnected or errors or links.unobserved)
    if config.require_all_replicas_synced and (links.lagging or links.disconnected):
        errors.append(f"Not all replicas synced: {len(links.lagging)} lagging, {len(links.disconnected)} disconnected")
    elif links.lagging and config.check_replication_offset:
        max_offset_diff = max(replica.replication_offset_diff for replica in links.lagging)
        errors.append(
            f"{len(links.lagging)} replica(s) behind their primary (max_offset_diff={max_offset_diff}, required=0)"
        )
    elif links.lagging:
        errors.append(
            f"{len(links.lagging)} replica(s) lagging beyond acceptable threshold "
            f"(max_lag={links.max_lag:.2f}s > {config.max_acceptable_lag:.2f}s)"
        )
    if links.disconnected and not errors:
        logger.info("%s replica(s) disconnected, each stopped on purpose or its primary was", len(links.disconnected))

    return ReplicationValidation(
        success=not errors and not links.unobserved,
        all_replicas_synced=all_replicas_synced,
        max_lag=links.max_lag,
        lagging_replicas=links.lagging,
        disconnected_replicas=links.disconnected,
        error_message="; ".join(errors + links.unobserved) or None,
        redundancy_lost=redundancy_lost,
        inconclusive=bool(links.unobserved) and not errors,
    )


@dataclass
class _Links:
    """What each replica reports of its link to its primary."""

    lagging: list[ReplicaLagInfo] = field(default_factory=list)
    # Replicas that did not answer or whose link is down; ``link_down`` are the ones that answer.
    disconnected: list[str] = field(default_factory=list)
    link_down: set[str] = field(default_factory=set)
    followed: dict[str, str] = field(default_factory=dict)  # replica address -> its primary's node ID
    errors: list[str] = field(default_factory=list)
    # What could not be compared: a primary not yet known as one, or a missing reply or offset.
    unobserved: list[str] = field(default_factory=list)
    max_lag: float = 0.0


def _replica_links(
    snapshot: Snapshot,
    config: ReplicationValidationConfig,
    replica_nodes: list[TopologyNode],
    primary_nodes: dict[str, TopologyNode],
    expected_primary: Optional[TopologyNode],
) -> _Links:
    """Each replica's upstream, link, lag, and offset against its primary's (``expected_primary`` in standalone)."""
    links = _Links()
    for replica in replica_nodes:
        replica_address = replica.address
        repl_info = snapshot.replication.get(replica_address)
        if replica.failed or repl_info is None:
            if not replica.failed:
                logger.error("Replica %s did not answer INFO replication", replica_address)
            links.disconnected.append(replica_address)
            continue

        if expected_primary is not None:
            primary_id = expected_primary.node_id
            upstream_error = _upstream_error(replica, repl_info, expected_primary)
            if upstream_error:
                links.errors.append(upstream_error)
                logger.error(upstream_error)
                continue
            primary_node = primary_nodes.get(primary_id, expected_primary)
        else:
            # Cluster mode takes the replica's primary from its own view, which failovers keep current.
            primary_id = snapshot.primary_of(replica)
            primary_node = primary_nodes.get(primary_id) if primary_id else None
        if primary_id:
            links.followed[replica_address] = primary_id
        primary_address = primary_node.address if primary_node else "unknown"

        if not repl_info.link_up:
            links.disconnected.append(replica_address)
            links.link_down.add(replica_address)
            logger.warning("Replica %s disconnected from primary (link_status=down)", replica_address)
            continue

        # Seconds since the primary was last heard from; unknown counts as none.
        lag_seconds = float(max(repl_info.last_io_seconds_ago or 0, 0))
        offset_diff = 0
        if config.check_replication_offset and primary_id:
            primary_info = snapshot.replication.get(primary_address) if primary_node else None
            # A missing offset is unknown, not zero lag, and so is a primary demoted since the topology read, or
            # one a gossip round behind a failover has not made a primary yet. A replica read later at the
            # primary's offset or beyond (Snapshot.caught_up) only applied what the primary sent between the two
            # reads; one read before its primary and ahead of it holds data its primary lost.
            later = snapshot.caught_up.get(replica_address)
            if (
                primary_info is None
                or primary_info.role != "primary"
                or primary_info.offset is None
                or repl_info.replica_offset is None
            ):
                links.unobserved.append(
                    f"could not compare the replication offsets of {replica_address} and its primary {primary_address}"
                )
                logger.warning(links.unobserved[-1])
                continue
            caught_up = later is not None and later >= primary_info.offset
            if not caught_up and primary_info.offset < repl_info.replica_offset:
                links.errors.append(
                    f"replica {replica_address} is ahead of its primary {primary_address}: offset "
                    f"{repl_info.replica_offset} > {primary_info.offset}"
                )
                logger.error(links.errors[-1])
                continue
            offset_diff = 0 if caught_up else primary_info.offset - repl_info.replica_offset

        # A replica behind by bytes is not caught up merely because it heard from the primary recently.
        is_lagging = offset_diff > 0 if config.check_replication_offset else lag_seconds > config.max_acceptable_lag
        links.max_lag = max(links.max_lag, lag_seconds)
        if is_lagging:
            links.lagging.append(
                ReplicaLagInfo(
                    replica_node_id=replica.node_id,
                    replica_address=replica_address,
                    primary_node_id=primary_id or "unknown",
                    primary_address=primary_address,
                    lag_seconds=lag_seconds,
                    replication_offset_diff=offset_diff,
                    link_status="up",
                )
            )
            logger.warning(
                "Replica %s lagging behind primary (lag=%.2fs, offset_diff=%s)",
                replica_address,
                lag_seconds,
                offset_diff,
            )
    return links


def _orphaned_replicas(
    all_nodes: list[TopologyNode], replica_nodes: list[TopologyNode], stopped: set[str], link_down: Collection[str]
) -> set[str]:
    """The replicas whose link is down because their shard's primary was stopped on purpose and not yet replaced.

    A replica that does not answer is not among them, even when its primary was stopped.
    """
    if not stopped:
        return set()
    stopped_primary_shards = {
        node.shard_id
        for node in all_nodes
        if node.role == "primary" and node.shard_id is not None and node.address in stopped
    }
    live_primary_shards = {
        node.shard_id
        for node in all_nodes
        if node.role == "primary" and node.shard_id is not None and node.address not in stopped and not node.failed
    }
    orphaned_shards = stopped_primary_shards - live_primary_shards
    orphaned = {
        replica.address
        for replica in replica_nodes
        if replica.shard_id in orphaned_shards and replica.address in link_down
    }
    if orphaned:
        logger.info("Replicas with expected replication link down (primary stopped on purpose): %s", orphaned)
    return orphaned


def _cross_check_primaries(
    snapshot: Snapshot,
    primary_nodes: dict[str, TopologyNode],
    replica_to_primary: dict[str, str],
    disconnected_replicas: list[str],
    *,
    strict: bool,
) -> list[str]:
    """Compare each primary's count of connected replicas with the replicas that follow it."""
    errors: list[str] = []
    for primary_id, primary in primary_nodes.items():
        address = primary.address
        repl_info = snapshot.replication.get(address)
        if repl_info is None:
            if strict:
                message = f"Unable to query replication INFO from primary {address}"
                errors.append(message)
                logger.error(message)
            continue
        connected_replicas = repl_info.connected_replicas or 0
        expected = sum(
            1
            for replica, followed in replica_to_primary.items()
            if followed == primary_id and replica not in disconnected_replicas
        )
        if connected_replicas != expected:
            message = f"Primary {address} reports {connected_replicas} connected replicas, expected {expected}"
            if strict:
                errors.append(message)
                logger.error(message)
            else:
                logger.debug("%s (may be transient during failover)", message)
    return errors


def _recorded_primary(snapshot: Snapshot) -> TopologyNode:
    """The one primary a standalone deployment's records name, as a topology row."""
    primaries = [instance for instance in snapshot.instances if instance.role == "primary"]
    if len(primaries) > 1:
        raise ValueError(f"A standalone deployment requires exactly one recorded primary, found {len(primaries)}")
    if primaries:
        primary = primaries[0]
        row = next((node for node in snapshot.rows if node.node_id == primary.instance_id), None)
        return TopologyNode(
            node_id=primary.instance_id,
            instance_id=primary.instance_id,
            host=primary.host,
            port=primary.port,
            role="primary",
            shard_id=primary.shard_id,
            failed=row.failed if row else True,
        )
    current = [node for node in snapshot.rows if node.role == "primary"]
    if len(current) != 1:
        raise ValueError(f"A standalone deployment requires exactly one primary, found {len(current)}")
    return current[0]


def _upstream_error(replica: TopologyNode, repl_info: Replication, expected_primary: TopologyNode) -> Optional[str]:
    """Why a standalone replica is not attached to the recorded primary, or None when it is."""
    expected = (str(expected_primary.host), int(expected_primary.port))
    if (str(repl_info.primary_host), repl_info.primary_port) == expected:
        return None
    reported = f"{repl_info.primary_host}:{repl_info.primary_port}" if repl_info.primary_host is not None else "unknown"
    return f"Replica {replica.address} is attached to {reported}, expected {expected[0]}:{expected[1]}"


def _redundancy_problem(
    all_nodes: list[TopologyNode],
    replica_nodes: list[TopologyNode],
    disconnected_replicas: list[str],
    stopped: set[str],
    min_replicas_per_shard: int,
    link_down_replicas: Collection[str],
) -> tuple[Optional[str], bool]:
    """Why a shard has fewer than ``min_replicas_per_shard`` connected replicas (None when none has), and
    whether replicas were lost to failures that were not stops.

    A disconnected replica stopped on purpose is expected. One that answers but
    whose link is down, e.g. during a full resync, has not failed: the
    shortfall fails without counting as lost, so a retry can see the resync
    finish.
    """
    shards = {node.shard_id for node in all_nodes if node.role == "primary" and node.shard_id is not None}
    members: dict[int, dict[str, list[str]]] = {
        shard: {"alive": [], "stopped": [], "lost": [], "link_down": []} for shard in shards
    }
    for replica in replica_nodes:
        if replica.shard_id not in members:
            continue
        address = replica.address
        if address not in disconnected_replicas:
            kind = "alive"
        elif address in stopped:
            kind = "stopped"
        elif address in link_down_replicas:
            kind = "link_down"
        else:
            kind = "lost"
        members[replica.shard_id][kind].append(address)

    lost, link_down = [], []
    for shard, replicas in sorted(members.items()):
        alive = len(replicas["alive"])
        if alive >= min_replicas_per_shard:
            continue
        if replicas["lost"]:
            lost.append(f"shard {shard} ({alive} alive, {len(replicas['lost'])} unexpected failures)")
        elif replicas["link_down"]:
            link_down.append(f"shard {shard} ({', '.join(sorted(replicas['link_down']))})")
        else:
            logger.warning(
                "Shard %s has %s live replica(s): %s stopped on purpose", shard, alive, len(replicas["stopped"])
            )
    if lost:
        return (
            f"replication: CRITICAL: {len(lost)} shard(s) have insufficient redundancy due to UNEXPECTED failures "
            f"(< {min_replicas_per_shard} alive replica per shard). Affected shards: {', '.join(lost)}. "
            f"These replicas failed without being stopped on purpose.",
            True,
        )
    if link_down:
        return (
            f"replication: {len(link_down)} shard(s) have fewer than {min_replicas_per_shard} replica(s) with their "
            f"replication link up; these replicas answer but their link is down: {'; '.join(link_down)}",
            False,
        )
    return None, False
