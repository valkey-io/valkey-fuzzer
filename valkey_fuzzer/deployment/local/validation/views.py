"""Deployment views: the expected primaries and replicas, and agreement between cluster nodes' views."""

import logging
from collections import Counter
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Optional

from ..cluster_nodes import group_slots_into_ranges, parse_slot_tokens, split_slots
from ..topology import TopologyNode
from .config import ViewConsistencyValidationConfig
from .results import RoleMismatch, RoleValidation, ViewConsistencyValidation, ViewDiscrepancy
from .snapshot import Snapshot

logger = logging.getLogger(__name__)


@dataclass
class ExpectedRoles:
    """The planned primaries and replicas."""

    num_primaries: int
    num_replicas: int


def check_roles(
    snapshot: Snapshot,
    expected_roles: ExpectedRoles,
    stopped: Collection[str] = (),
    stopped_roles: Optional[Mapping[str, str]] = None,
    stopped_shards: Collection[int] = (),
) -> RoleValidation:
    """Check the live primary and replica counts against the plan, allowing for the instances stopped on purpose.

    This is the one check that judges whether the failed instances are exactly
    the ones stopped on purpose. ``stopped_roles`` holds each stopped
    instance's role when it stopped, and ``stopped_shards`` the live shards
    whose every member was stopped: no replica is left to replace their primary.
    """
    stopped_roles = stopped_roles or {}
    all_nodes = snapshot.rows
    if not all_nodes:
        return RoleValidation(
            success=False,
            expected_primaries=expected_roles.num_primaries,
            expected_replicas=expected_roles.num_replicas,
            error_message="No nodes reachable in cluster",
        )

    live = [node for node in all_nodes if not node.failed]
    failed = [node for node in all_nodes if node.failed]
    unexpected_failures = [node for node in failed if node.address not in stopped]
    for node in unexpected_failures:
        logger.warning(
            "Unexpected node failure: %s (role: %s, node_id: %s), not stopped on purpose",
            node.address,
            node.role,
            node.node_id,
        )
    stopped_alive = [node for node in live if node.address in stopped]
    # Only live nodes count: the cluster should have promoted a replacement for each stopped primary.
    actual_primaries = sum(node.role == "primary" for node in live)
    actual_replicas = sum(node.role == "replica" for node in live)

    def role_at_stop(node: TopologyNode) -> str:
        return stopped_roles.get(node.address, node.role)

    stopped_failed = [node for node in failed if node.address in stopped]
    stopped_primaries = sum(role_at_stop(node) == "primary" for node in stopped_failed)
    stopped_replicas = sum(role_at_stop(node) == "replica" for node in stopped_failed)
    if expected_roles.num_replicas == 0:
        unreplaced_primaries = stopped_primaries
    else:
        # With replicas a failover keeps the primary count, unless the whole shard was stopped. Shards, not
        # nodes, count: a shard may have promoted several primaries before it was stopped whole.
        unreplaced_primaries = len(
            {
                node.shard_id
                for node in stopped_failed
                if role_at_stop(node) == "primary" and node.shard_id in set(stopped_shards)
            }
        )
    expected_primaries = expected_roles.num_primaries - unreplaced_primaries
    promoted = stopped_primaries - unreplaced_primaries
    expected_replicas = max(0, expected_roles.num_replicas - stopped_replicas - promoted)
    if stopped_primaries + stopped_replicas:
        logger.info(
            "Adjusting expected roles for %s primary(s) and %s replica(s) "
            "stopped on purpose: %s primaries, %s replicas",
            stopped_primaries,
            stopped_replicas,
            expected_primaries,
            expected_replicas,
        )

    role_mismatches = []
    if actual_primaries != expected_primaries:
        role_mismatches.append(
            RoleMismatch("primary_count", "cluster", f"{expected_primaries} primaries", f"{actual_primaries} primaries")
        )
    if actual_replicas != expected_replicas:
        role_mismatches.append(
            RoleMismatch("replica_count", "cluster", f"{expected_replicas} replicas", f"{actual_replicas} replicas")
        )
    role_mismatches.extend(
        RoleMismatch(
            "unexpected_failure",
            node.node_id,
            "node alive",
            f"node failed (role: {node.role}, address: {node.address})",
        )
        for node in unexpected_failures
    )
    role_mismatches.extend(
        RoleMismatch("stopped_alive", node.node_id, "node stopped", f"node live ({node.address})")
        for node in stopped_alive
    )

    # Every mismatch fails: an instance that failed without being stopped is a bug, and a wrong primary or replica
    # count means a cluster-bus regression, a dropped replica, or a missing shard.
    errors = []
    if unexpected_failures:
        errors.append(
            f"Unexpected node failures detected: {len(unexpected_failures)} node(s) failed "
            f"that were not stopped on purpose"
        )
    if stopped_alive:
        errors.append(f"Nodes stopped on purpose are still live: {sorted(node.address for node in stopped_alive)}")
    counts = [m for m in role_mismatches if m.mismatch_type in ("primary_count", "replica_count")]
    if counts:
        details = ", ".join(f"{m.mismatch_type}: expected {m.expected}, got {m.actual}" for m in counts)
        errors.append(f"Role validation failed: {details}")
    error_message = "; ".join(errors) or None
    logger.log(
        logging.ERROR if role_mismatches else logging.INFO,
        error_message
        or f"Role validation passed: {actual_primaries} primaries, {actual_replicas} replicas "
        f"(expected {expected_primaries} primaries, {expected_replicas} replicas)",
    )
    return RoleValidation(
        success=not role_mismatches,
        expected_primaries=expected_roles.num_primaries,
        actual_primaries=actual_primaries,
        expected_replicas=expected_roles.num_replicas,
        actual_replicas=actual_replicas,
        role_mismatches=role_mismatches,
        error_message=error_message,
    )


def _view(parsed_nodes: list[dict]) -> dict[str, dict]:
    """One node's CLUSTER NODES as {node ID: role, state, address, primary, config epoch, slots}."""
    view = {}
    for node in parsed_nodes:
        if node["is_fail"]:
            state = "fail"
        elif node["link_state"] == "disconnected" or "noaddr" in node["flags"]:
            state = "disconnected"
        else:
            state = "connected"
        view[node["node_id"]] = {
            "role": "primary" if node["is_primary"] else "replica" if node["is_replica"] else "unknown",
            "state": state,
            "address": f"{node['host']}:{node['port']}",
            "primary_id": node.get("primary_id"),
            # A replica reports its primary's epoch; only a primary's is its own.
            "config_epoch": node["config_epoch"] if node["is_primary"] else None,
            "slots": parse_slot_tokens(node["slots"]) if node["is_primary"] else [],
        }
    return view


def check_views(
    snapshot: Snapshot, config: ViewConsistencyValidationConfig, stopped: Collection[str] = ()
) -> ViewConsistencyValidation:
    """Check that the live nodes agree on membership, roles, states, addresses, primaries, and epochs."""
    live = snapshot.live_rows
    if not live:
        return ViewConsistencyValidation(success=False, error_message="No nodes reachable in cluster")
    if len(live) == 1:
        logger.info("Single node cluster, view consistency check skipped")
        return ViewConsistencyValidation(
            success=True, nodes_checked=1, consistent_views=True, consensus_percentage=100.0
        )
    node_views = {address: _view(view) for address, view in snapshot.views().items()}
    if not node_views:
        return ViewConsistencyValidation(success=False, error_message="Unable to collect cluster view from any node")

    view_discrepancies = _compare_views(node_views)
    stopped_ids = {
        node_id for view in node_views.values() for node_id, info in view.items() if info["address"] in stopped
    }
    unexpected = []
    for discrepancy in view_discrepancies:
        about_stopped = discrepancy.subject_node in stopped_ids
        # About a stopped node, a view may lack it, see it failed or disconnected ("disconnected" first, "fail"
        # once the node timeout passed), or hold a stale copy of its role, primary, epoch, or slots, which it can
        # no longer gossip: a peer that applied a late packet from a node demoted just before it stopped keeps it
        # a primary. A view that gives its slots to a live primary too is still a split brain. Anything else (an
        # address change, or an extra entry) is a bug.
        if about_stopped and (
            discrepancy.discrepancy_type in ("missing", "role", "primary", "config_epoch", "slots")
            or (discrepancy.discrepancy_type == "state" and discrepancy.actual_value != "connected")
        ):
            continue
        unexpected.append(discrepancy)
        logger.warning(
            "Unexpected %s discrepancy about %snode %s: expected %s, saw %s from %s",
            discrepancy.discrepancy_type,
            "stopped " if about_stopped else "",
            discrepancy.subject_node,
            discrepancy.expected_value,
            discrepancy.actual_value,
            discrepancy.node_reporting,
        )

    # Views that disagree on a slot's owner (one a gossip round behind a failover) are an inconsistency, which a
    # retry sees converge; two primaries serving a slot at once are a split brain.
    split_brain_detected = bool(split_slots(snapshot.views()))
    consensus_percentage = _consensus(node_views)
    consistent_views = not unexpected
    success = True
    error_message = None
    if split_brain_detected:
        success = False
        error_message = "Split-brain scenario detected"
    elif not consistent_views:
        success = False
        error_message = (
            f"View inconsistency detected: {len(unexpected)} UNEXPECTED discrepancy(ies) found (not about "
            f"nodes stopped on purpose). Total discrepancies: {len(view_discrepancies)} (including "
            f"{len(view_discrepancies) - len(unexpected)} expected about stopped nodes)"
        )
    logger.log(
        logging.INFO if success else logging.ERROR,
        "View consistency validation %s: %s nodes checked, consensus=%.1f%%, discrepancies=%s, split_brain=%s",
        "passed" if success else "failed",
        len(node_views),
        consensus_percentage,
        len(view_discrepancies),
        split_brain_detected,
    )
    return ViewConsistencyValidation(
        success=success,
        nodes_checked=len(node_views),
        consistent_views=consistent_views,
        split_brain_detected=split_brain_detected,
        view_discrepancies=view_discrepancies,
        consensus_percentage=consensus_percentage,
        error_message=error_message,
    )


def _compare_views(node_views: dict[str, dict[str, dict]]) -> list[ViewDiscrepancy]:
    """Every way each view differs from the first one."""
    discrepancies = []
    reference_address, reference = next(iter(node_views.items()))
    for node_address, view in node_views.items():
        if node_address == reference_address:
            continue
        for node_id in reference.keys() - view.keys():
            discrepancies.append(ViewDiscrepancy("missing", node_address, node_id, "present in cluster", "not in view"))
        for node_id in view.keys() - reference.keys():
            discrepancies.append(ViewDiscrepancy("extra", node_address, node_id, "not in cluster", "present in view"))
        for node_id in reference.keys() & view.keys():
            for field, kind in (
                ("role", "role"),
                ("state", "state"),
                ("primary_id", "primary"),
                ("config_epoch", "config_epoch"),
                ("address", "address"),
                ("slots", "slots"),
            ):
                expected, actual = reference[node_id].get(field), view[node_id].get(field)
                if expected != actual:
                    shown = group_slots_into_ranges if field == "slots" else str
                    discrepancies.append(ViewDiscrepancy(kind, node_address, node_id, shown(expected), shown(actual)))
    return discrepancies


def _consensus(node_views: dict[str, dict[str, dict]]) -> float:
    """The share of nodes, in percent, whose view of membership and roles is the most common one."""
    if len(node_views) < 2:
        return 100.0
    signatures = Counter(
        frozenset((node_id, info["role"]) for node_id, info in view.items()) for view in node_views.values()
    )
    return signatures.most_common(1)[0][1] / len(node_views) * 100.0
