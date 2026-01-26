import logging
from typing import Dict, List, Optional, Set, Tuple, Any
from ..models import TopologyMismatch
from ..utils.cluster_parser import (
    parse_cluster_nodes_raw, 
    parse_cluster_nodes_line,
    format_node_address,
    group_slots_into_ranges,
    compute_cluster_slot,
    fetch_cluster_slot_assignments
)

logger = logging.getLogger()

# Re-export cluster utilities for backward compatibility
__all__ = [
    'parse_cluster_nodes_raw', 
    'parse_cluster_nodes_line', 
    'format_node_address',
    'group_slots_into_ranges',
    'compute_cluster_slot',
    'fetch_cluster_slot_assignments'
]


def validate_killed_vs_failed_nodes(killed_nodes: Set[str], failed_nodes: List[str], context: str = "validation") -> Tuple[bool, Optional[str]]:
    if not killed_nodes:
        if failed_nodes:
            logger.warning(f"{context}: {len(failed_nodes)} node(s) in fail state but no killed nodes tracked")
        return True, None
    
    expected_failed_nodes = killed_nodes
    actual_failed_nodes = set(failed_nodes)
    
    # Check if all killed nodes are in fail state
    killed_but_not_failed = expected_failed_nodes - actual_failed_nodes
    if killed_but_not_failed:
        error_msg = (
            f"{context}: Killed nodes not in fail state: {killed_but_not_failed}. "
            f"These nodes were killed by chaos but are not marked as failed. "
            f"This indicates improper node termination or cluster state tracking bug."
        )
        logger.error(error_msg)
        return False, error_msg
    
    # Check for unexpected failures (nodes failed but not killed)
    failed_but_not_killed = actual_failed_nodes - expected_failed_nodes
    if failed_but_not_killed:
        error_msg = (
            f"{context}: Unexpected nodes in fail state: {failed_but_not_killed}. "
            f"These nodes failed but were not killed by chaos. "
            f"This indicates cascading failures or other clustering bugs. "
            f"Expected failed: {expected_failed_nodes}, Actual failed: {actual_failed_nodes}"
        )
        logger.error(error_msg)
        return False, error_msg
    
    # All checks passed
    logger.debug(f"{context}: Failed nodes match killed nodes as expected: {actual_failed_nodes}")
    return True, None


def check_per_shard_redundancy(
    all_nodes: List[Dict],
    replica_nodes: List[Dict],
    disconnected_replicas: List[str],
    killed_nodes: Set[str],
    min_replicas_per_shard: int,
    context: str = "replication"
) -> Tuple[bool, Optional[str]]:
    if min_replicas_per_shard == 0:
        return True, None
    
    # Identify all shards from primary nodes
    all_shard_ids = set()
    for node in all_nodes:
        if node['role'] == 'primary' and node.get('shard_id') is not None:
            all_shard_ids.add(node['shard_id'])
    
    # Group replicas by shard
    shard_replicas: Dict[int, Dict[str, List]] = {}
    
    # Initialize all shards
    for shard_id in all_shard_ids:
        shard_replicas[shard_id] = {
            'alive': [],
            'dead_expected': [],
            'dead_unexpected': []
        }
    
    # Populate replica information
    for replica in replica_nodes:
        shard_id = replica.get('shard_id')
        if shard_id is None or shard_id not in shard_replicas:
            continue
        
        replica_address = format_node_address(replica)
        if replica_address in disconnected_replicas:
            # Check if this was an expected death (killed by chaos)
            if replica_address in killed_nodes:
                shard_replicas[shard_id]['dead_expected'].append(replica_address)
            else:
                shard_replicas[shard_id]['dead_unexpected'].append(replica_address)
        else:
            shard_replicas[shard_id]['alive'].append(replica_address)
    
    # Check each shard has minimum redundancy
    shards_without_redundancy = []
    for shard_id, replicas in shard_replicas.items():
        alive_count = len(replicas['alive'])
        dead_unexpected_count = len(replicas['dead_unexpected'])
        
        # Only fail if we have UNEXPECTED redundancy loss
        if alive_count < min_replicas_per_shard and dead_unexpected_count > 0:
            shards_without_redundancy.append({
                'shard_id': shard_id,
                'alive_replicas': alive_count,
                'dead_expected': len(replicas['dead_expected']),
                'dead_unexpected': dead_unexpected_count,
                'dead_unexpected_addresses': replicas['dead_unexpected']
            })
            logger.error(
                f"{context}: Shard {shard_id} has insufficient redundancy due to UNEXPECTED failures: "
                f"{alive_count} alive replica(s) < {min_replicas_per_shard} required. "
                f"Dead (expected/chaos): {len(replicas['dead_expected'])}, "
                f"Dead (UNEXPECTED): {dead_unexpected_count} - {replicas['dead_unexpected']}"
            )
        elif alive_count < min_replicas_per_shard:
            # Redundancy loss is due to chaos only - log but don't fail
            logger.warning(
                f"{context}: Shard {shard_id} has reduced redundancy due to chaos: "
                f"{alive_count} alive replica(s) (killed by chaos: {len(replicas['dead_expected'])})"
            )
    
    if shards_without_redundancy:
        shard_details = ", ".join([
            f"shard {s['shard_id']} ({s['alive_replicas']} alive, "
            f"{s['dead_unexpected']} unexpected failures)"
            for s in shards_without_redundancy
        ])
        error_msg = (
            f"{context}: CRITICAL: {len(shards_without_redundancy)} shard(s) have insufficient redundancy "
            f"due to UNEXPECTED failures (< {min_replicas_per_shard} alive replica per shard). "
            f"Affected shards: {shard_details}. "
            f"This indicates unexpected replica failures - not caused by chaos!"
        )
        logger.error(error_msg)
        return False, error_msg
    
    return True, None


def is_node_killed_by_chaos(node: Dict, killed_nodes: Set[str]) -> bool:
    node_id = node.get('node_id')
    node_addr = format_node_address(node)
    # Check both node_id and address since killed_nodes may contain either
    return (node_id and node_id in killed_nodes) or (node_addr in killed_nodes)


def detect_unexpected_failures(all_nodes: List[Dict], killed_nodes: Set[str], context: str = "topology") -> List[Dict]:
    unexpected_failures = []
    
    for node in all_nodes:
        if node.get('status') == 'failed':
            if not is_node_killed_by_chaos(node, killed_nodes):
                unexpected_failures.append(node)
                logger.warning(
                    f"{context}: Unexpected node failure detected: {format_node_address(node)} "
                    f"(role: {node['role']}, node_id: {node.get('node_id', 'unknown')}, "
                    f"not in killed_nodes)"
                )
    
    return unexpected_failures


def create_topology_mismatch_for_unexpected_failure(node: Dict) -> TopologyMismatch:
    """Create a TopologyMismatch object for an unexpected node failure."""
    return TopologyMismatch(
        mismatch_type="unexpected_failure",
        node_id=node.get('node_id', 'unknown'),
        expected="node alive",
        actual=f"node failed (role: {node['role']}, address: {format_node_address(node)})"
    )
