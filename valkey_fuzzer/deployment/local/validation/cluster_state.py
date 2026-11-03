"""Cluster state checks: CLUSTER INFO state, quorum, and hash-slot coverage."""

import logging
from collections.abc import Collection

from ..cluster_nodes import HASH_SLOTS, group_slots_into_ranges, parse_slot_tokens, split_slots
from .config import ClusterStatusValidationConfig, SlotCoverageValidationConfig
from .results import ClusterStatusValidation, SlotConflict, SlotCoverageValidation
from .snapshot import Snapshot

logger = logging.getLogger(__name__)


def check_cluster_status(
    snapshot: Snapshot, config: ClusterStatusValidationConfig, stopped: Collection[str] = ()
) -> ClusterStatusValidation:
    """Check every node's cluster_state and the slot-owning primaries' quorum.

    ``stopped`` (addresses stopped on purpose) only annotates the quorum's
    unreachable primaries.
    """
    stopped = set(stopped)
    all_nodes = snapshot.rows
    if not all_nodes:
        return ClusterStatusValidation(
            success=False, degraded_reason="No nodes reachable", error_message="Unable to connect to any cluster nodes"
        )

    cluster_states: dict[str, str] = {}
    missing_cluster_state_nodes: list[str] = []
    cluster_info_query_failures: list[str] = []
    reachable_addresses: set[str] = set()
    for node in all_nodes:
        node_address = node.address
        cluster_info = snapshot.cluster_info.get(node_address)
        if cluster_info is None:
            # A node still reported as connected must not drop out of the check because it did not answer.
            if not node.failed:
                cluster_info_query_failures.append(node_address)
            continue
        reachable_addresses.add(node_address)
        raw_cluster_state = cluster_info.get("cluster_state")
        if raw_cluster_state is None or not raw_cluster_state.strip():
            missing_cluster_state_nodes.append(node_address)
            cluster_states[node_address] = "unknown"
        else:
            cluster_states[node_address] = raw_cluster_state.strip().lower()
    nodes_in_fail_state = [node.address for node in all_nodes if node.failed]

    # Do not majority-vote away a disagreement: expose it and judge every report.
    reported_states = set(cluster_states.values())
    state_disagreement = len(reported_states) > 1
    if state_disagreement:
        overall_cluster_state = "mixed"
        logger.warning("Cluster state disagreement detected: %s", cluster_states)
    else:
        overall_cluster_state = next(iter(reported_states), "unknown")

    # Quorum as Valkey counts it: a majority of the primaries that own slots. A primary without slots, such
    # as a stopped primary whose replica took over its slots, counts neither for nor against it.
    primary_nodes = [node for node in all_nodes if node.role == "primary" and node.slots != ()]
    reachable_primaries = [node for node in primary_nodes if node.address in reachable_addresses]
    has_quorum = True
    quorum_error = None
    if primary_nodes:
        quorum_threshold = (len(primary_nodes) // 2) + 1
        has_quorum = len(reachable_primaries) >= quorum_threshold
        if not has_quorum:
            unreachable = [
                address + (" (stopped on purpose)" if address in stopped else "")
                for address in sorted(node.address for node in primary_nodes)
                if address not in reachable_addresses
            ]
            quorum_error = (
                f"Cluster does not have quorum: {len(reachable_primaries)}/{len(primary_nodes)} "
                f"slot-owning primaries reachable (need {quorum_threshold}); "
                f"unreachable: {', '.join(unreachable)}"
            )
            logger.warning(quorum_error)

    validation_errors: list[str] = []
    degraded_reason = None
    explicit_acceptable_states = {
        str(state).strip().lower() for state in config.acceptable_states if str(state).strip()
    }
    effective_acceptable_states = explicit_acceptable_states

    if missing_cluster_state_nodes:
        validation_errors.append(
            "Reachable nodes returned a missing or blank cluster_state from CLUSTER INFO: "
            + ", ".join(sorted(missing_cluster_state_nodes))
        )
    if cluster_info_query_failures:
        validation_errors.append(
            "Connected nodes could not be queried with CLUSTER INFO: " + ", ".join(sorted(cluster_info_query_failures))
        )
    unacceptable_reports = {
        address: state
        for address, state in cluster_states.items()
        if address not in missing_cluster_state_nodes and state not in effective_acceptable_states
    }
    if not cluster_states and "unknown" not in effective_acceptable_states:
        validation_errors.append(f"Cluster state 'unknown' not in acceptable states: {config.acceptable_states}")
    elif unacceptable_reports:
        reports = ", ".join(f"{address}={state}" for address, state in sorted(unacceptable_reports.items()))
        validation_errors.append(
            f"Cluster nodes reported unacceptable states ({reports}); acceptable states: {config.acceptable_states}"
        )

    # A mixed report is allowed only when every reported state is listed explicitly.
    if state_disagreement:
        if not reported_states.issubset(explicit_acceptable_states):
            reports = ", ".join(f"{address}={state}" for address, state in sorted(cluster_states.items()))
            validation_errors.append(f"Cluster state disagreement across reachable nodes: {reports}")
        elif not validation_errors:
            degraded_reason = "Cluster state disagreement explicitly allowed by acceptable_states"

    if nodes_in_fail_state:
        # Whether they are the nodes stopped on purpose is the role check's to judge.
        logger.info("Nodes in fail state: %s", nodes_in_fail_state)
        degraded_reason = degraded_reason or f"{len(nodes_in_fail_state)} node(s) in fail state"

    if quorum_error:
        validation_errors.append(quorum_error)

    # Keep every independent failure rather than letting a later one overwrite the first cause.
    success = not validation_errors
    error_message = "; ".join(validation_errors) if validation_errors else None
    logger.log(
        logging.INFO if success else logging.ERROR,
        "Cluster status validation %s: state=%s, quorum=%s, failed_nodes=%s",
        "passed" if success else "failed",
        overall_cluster_state,
        has_quorum,
        len(nodes_in_fail_state),
    )
    return ClusterStatusValidation(
        success=success,
        cluster_state=overall_cluster_state,
        nodes_in_fail_state=nodes_in_fail_state,
        has_quorum=has_quorum,
        degraded_reason=degraded_reason,
        error_message=error_message,
    )


def check_slot_coverage(
    snapshot: Snapshot,
    config: SlotCoverageValidationConfig,
    stopped: Collection[str] = (),
    stopped_shards: Collection[int] = (),
) -> SlotCoverageValidation:
    """Check that every live node's view assigns every slot, to one primary, and none to a stopped node.

    ``stopped_shards`` are the live shards whose every member was stopped on
    purpose: no node is left to take their slots over.
    """
    stopped = set(stopped)
    all_nodes = snapshot.rows
    if not all_nodes:
        logger.error("Unable to fetch any nodes from cluster for slot coverage validation")
        return SlotCoverageValidation(success=False, error_message="Unable to contact cluster - no nodes reachable")
    primary_nodes = [node for node in all_nodes if node.role == "primary"]
    if not primary_nodes:
        return SlotCoverageValidation(success=False, error_message="No primary nodes available in cluster")

    # Each live node's view: slot -> the primary that owns it.
    node_perspectives = {
        address: {
            slot: node["node_id"] for node in view if node["is_primary"] for slot in parse_slot_tokens(node["slots"])
        }
        for address, view in snapshot.views().items()
    }
    if not node_perspectives:
        return SlotCoverageValidation(success=False, error_message="Unable to query slot assignments from any node")

    # A slot the views give to different primaries has no one owner, but is a conflict only when it is split.
    slot_assignments = {slot: sorted(owners) for slot, owners in snapshot.slot_claims().items()}
    slot_distribution: dict[str, list[int]] = {}
    for slot, owners in sorted(slot_assignments.items()):
        if len(owners) == 1:
            slot_distribution.setdefault(owners[0], []).append(slot)
    unassigned_slots = [slot for slot in range(HASH_SLOTS) if slot not in slot_assignments]
    conflicting_slots = [
        SlotConflict(slot=slot, conflicting_nodes=sorted(owners))
        for slot, owners in sorted(split_slots(snapshot.views()).items())
    ]
    for conflict in conflicting_slots:
        logger.warning("Slot %s has conflicting assignments: %s", conflict.slot, conflict.conflicting_nodes)
    if unassigned_slots:
        logger.warning(
            "Found %s unassigned slots: %s", len(unassigned_slots), group_slots_into_ranges(unassigned_slots)
        )

    success = True
    error_message = None

    # The slots of shards whose every member was stopped, including slots now unassigned that a view still
    # gives one of their nodes.
    stopped_shard_nodes = {node.node_id for node in all_nodes if node.shard_id in set(stopped_shards)}
    slots_on_stopped_shards = {
        slot for view in node_perspectives.values() for slot, owner in view.items() if owner in stopped_shard_nodes
    }
    slots_on_stopped_shards.update(
        slot
        for node in all_nodes
        if node.role == "primary" and node.node_id in stopped_shard_nodes
        for slot in slot_distribution.get(node.node_id, [])
    )
    if slots_on_stopped_shards:
        logger.info(
            "Excluding %s slots of shard(s) %s, whose every member was stopped on purpose: %s",
            len(slots_on_stopped_shards),
            sorted(stopped_shards),
            group_slots_into_ranges(sorted(slots_on_stopped_shards)),
        )

    # Slots still assigned to a stopped node: its failover did not complete.
    stopped_node_ids = {node.node_id for node in all_nodes if node.address in stopped}
    slots_on_stopped_nodes = [slot for node_id in stopped_node_ids for slot in slot_distribution.get(node_id, [])]
    unexpected_on_stopped = [slot for slot in slots_on_stopped_nodes if slot not in slots_on_stopped_shards]
    if unexpected_on_stopped:
        success = False
        error_message = (
            f"CRITICAL: {len(unexpected_on_stopped)} slots still assigned to stopped nodes. "
            f"Stopped nodes: {stopped}. "
            f"This indicates failover did not complete or slots were not reassigned. "
            f"Affected slots: {group_slots_into_ranges(unexpected_on_stopped)}"
        )
        logger.error(error_message)
    elif slots_on_stopped_nodes:
        logger.info("%s slots on stopped nodes belong to fully stopped shards", len(slots_on_stopped_nodes))

    if success:
        unexpected_unassigned = [slot for slot in unassigned_slots if slot not in slots_on_stopped_shards]
        if unexpected_unassigned:
            success = False
            error_message = (
                f"{len(unexpected_unassigned)} slots are unassigned (expected all {HASH_SLOTS} slots to be assigned)"
            )
            logger.error(error_message)

    if success:
        # The union of the views can cover every slot while one node still misses some.
        for node_address, slot_view in sorted(node_perspectives.items()):
            node_missing = [
                slot for slot in range(HASH_SLOTS) if slot not in slot_view and slot not in slots_on_stopped_shards
            ]
            if node_missing:
                success = False
                error_message = (
                    f"Node {node_address} sees {len(node_missing)} unassigned slots "
                    f"(expected all {HASH_SLOTS} slots to be assigned): {group_slots_into_ranges(node_missing)}"
                )
                logger.error(error_message)
                break

    if success and conflicting_slots:
        success = False
        error_message = f"{len(conflicting_slots)} slots are served by two primaries at once"
        logger.error(error_message)

    logger.log(
        logging.INFO if success else logging.ERROR,
        "Slot coverage validation %s: %s/%s slots assigned, %s unassigned, %s conflicts",
        "passed" if success else "failed",
        len(slot_assignments),
        HASH_SLOTS,
        len(unassigned_slots),
        len(conflicting_slots),
    )
    return SlotCoverageValidation(
        success=success,
        total_slots_assigned=len(slot_assignments),
        unassigned_slots=unassigned_slots,
        conflicting_slots=conflicting_slots,
        slot_distribution=slot_distribution,
        error_message=error_message,
    )
