import logging
import time
import random
import string
import math
import valkey
from valkey.cluster import ClusterNode
from collections import Counter
from typing import Dict, List, Optional, Callable, TypeVar, Any
import signal
from contextlib import contextmanager
from ..models import (
    ClusterConnection, ReplicationValidation, ReplicationValidationConfig, ReplicaLagInfo,
    ClusterStatusValidation, ClusterStatusValidationConfig,
    SlotCoverageValidation, SlotCoverageValidationConfig, SlotConflict,
    TopologyValidation, TopologyValidationConfig, TopologyMismatch,
    ViewConsistencyValidation, ViewDiscrepancy,
    DataConsistencyValidation, DataConsistencyValidationConfig, DataInconsistency,
    ExpectedTopology, StateValidationConfig, StateValidationResult
)
from .log_validator import ShardLogValidator
from .state_validator_helpers import (
    format_node_address,
    validate_killed_vs_failed_nodes,
    check_per_shard_redundancy,
    group_slots_into_ranges,
    detect_unexpected_failures,
    create_topology_mismatch_for_unexpected_failure,
    is_node_killed_by_chaos,
    compute_cluster_slot,
    fetch_cluster_slot_assignments
)
from ..utils.valkey_utils import valkey_client, safe_query_node, query_cluster_nodes
from ..utils.cluster_parser import parse_cluster_nodes_raw as parse_cluster_nodes_output

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ValidationTimeoutError(Exception):
    """Raised when validation exceeds the configured timeout"""
    pass

@contextmanager
def validation_timeout(seconds: float):
    def timeout_handler(signum, frame):
        raise ValidationTimeoutError(f"Validation exceeded timeout of {seconds}s")
    
    # Only use signal-based timeout on Unix systems
    if hasattr(signal, 'SIGALRM'):
        # Set the signal handler
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        
        # Use setitimer for sub-second precision instead of alarm (which only supports integer seconds)
        # setitimer(ITIMER_REAL, seconds) sets a real-time timer that delivers SIGALRM
        if hasattr(signal, 'setitimer'):
            signal.setitimer(signal.ITIMER_REAL, seconds)
        else:
            # Fallback to alarm with ceiling for systems without setitimer
            signal.alarm(math.ceil(seconds))
        
        try:
            yield
        finally:
            # Disable the timer and restore old handler
            if hasattr(signal, 'setitimer'):
                signal.setitimer(signal.ITIMER_REAL, 0)
            else:
                signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    else:
        # On Windows or systems without SIGALRM, just yield without timeout
        # (timeout enforcement will be done via elapsed time checks)
        logger.warning("Signal-based timeout not available on this platform")
        yield


def parse_slot_range(slot_info: str) -> List[int]:
    try:
        if '-' in slot_info:
            start, end = slot_info.split('-')
            return list(range(int(start), int(end) + 1))
        return [int(slot_info)]
    except (ValueError, IndexError):
        return []


class ReplicationValidator:
    """Validates replication status between primaries and replicas"""

    def validate(
        self,
        cluster_connection: ClusterConnection,
        config: ReplicationValidationConfig,
        killed_nodes: Optional[set[str]] = None
    ) -> ReplicationValidation:
        """Check replication status for all replica nodes in the cluster."""
        try:
            # Get all nodes from cluster INCLUDING failed ones to detect dead replicas
            all_nodes = cluster_connection.get_current_nodes(include_failed=True)
            
            # Check if we could fetch any nodes at all
            if not all_nodes:
                # Unable to contact cluster - this is a failure
                logger.error("Unable to fetch any nodes from cluster for replication validation")
                return ReplicationValidation(
                    success=False,
                    all_replicas_synced=False,
                    max_lag=-1.0,
                    lagging_replicas=[],
                    disconnected_replicas=[],
                    error_message="Unable to contact cluster - no nodes reachable"
                )
            
            replica_nodes = [node for node in all_nodes if node['role'] == 'replica']
            
            if not replica_nodes:
                # No replicas found in cluster
                # This could be:
                # 1. Cluster genuinely has no replicas configured (OK)
                # 2. All replicas disappeared/failed (BUG!)
                
                # Check if we expect replicas (min_replicas_per_shard > 0)
                if config.min_replicas_per_shard > 0:
                    # We expect replicas but found none - this is a failure!
                    primary_nodes_list = [node for node in all_nodes if node['role'] == 'primary']
                    num_shards = len(set(p.get('shard_id') for p in primary_nodes_list if p.get('shard_id') is not None))
                    
                    logger.error(
                        f"CRITICAL: No replicas found in cluster but min_replicas_per_shard={config.min_replicas_per_shard}. "
                        f"Expected at least {num_shards * config.min_replicas_per_shard} replicas for {num_shards} shards. "
                        f"All replicas have disappeared!"
                    )
                    return ReplicationValidation(
                        success=False,
                        all_replicas_synced=False,
                        max_lag=-1.0,
                        lagging_replicas=[],
                        disconnected_replicas=[],
                        error_message=(
                            f"All replicas missing: Expected at least {config.min_replicas_per_shard} replica(s) per shard "
                            f"but found 0 replicas in entire cluster. This indicates complete replica loss."
                        )
                    )
                else:
                    # No replicas expected, this is OK
                    logger.info("No replicas configured in cluster (none expected)")
                    return ReplicationValidation(
                        success=True,
                        all_replicas_synced=True,
                        max_lag=0.0,
                        lagging_replicas=[],
                        disconnected_replicas=[],
                        error_message=None
                    )

            # Build primary node lookup
            primary_nodes = {node['node_id']: node for node in all_nodes if node['role'] == 'primary'}

            lagging_replicas: List[ReplicaLagInfo] = []
            disconnected_replicas: List[str] = []
            max_lag = 0.0
            all_replicas_synced = True

            # Check each replica
            for replica in replica_nodes:
                replica_address = format_node_address(replica)

                # Check if replica is marked as failed in topology
                if replica.get('status') == 'failed':
                    disconnected_replicas.append(replica_address)
                    all_replicas_synced = False
                    continue

                try:
                    # Connect to replica and get replication info
                    with valkey_client(replica['host'], replica['port'], config.timeout) as client:
                        repl_info = client.info('replication')

                    # Extract replication metrics
                    master_link_status = repl_info.get('master_link_status', 'down')
                    master_last_io_seconds_ago = repl_info.get('master_last_io_seconds_ago', -1)

                    # Get master node ID from cluster nodes output
                    master_node_id = self._get_master_node_id(replica, cluster_connection)

                    if master_node_id and master_node_id in primary_nodes:
                        primary_node = primary_nodes[master_node_id]
                        primary_address = format_node_address(primary_node)
                    else:
                        primary_address = "unknown"

                    # Check if replica is disconnected
                    if master_link_status != 'up':
                        disconnected_replicas.append(replica_address)
                        all_replicas_synced = False
                        logger.warning(
                            f"Replica {replica_address} disconnected from primary "
                            f"(link_status={master_link_status})"
                        )
                        continue

                    # Calculate lag - cast to int first since decode_responses=True returns strings
                    try:
                        lag_seconds = float(master_last_io_seconds_ago) if int(master_last_io_seconds_ago) >= 0 else 0.0
                    except (ValueError, TypeError):
                        # If conversion fails, assume no lag data available
                        lag_seconds = 0.0
                        logger.debug(f"Could not parse master_last_io_seconds_ago: {master_last_io_seconds_ago}")

                    # Get replication offset difference if configured
                    replication_offset_diff = 0
                    if config.check_replication_offset and master_node_id:
                        replication_offset_diff = self._get_offset_difference(
                            replica,
                            primary_nodes.get(master_node_id),
                            config.timeout
                        )

                    # Check if replica is lagging
                    if lag_seconds > config.max_acceptable_lag:
                        lag_info = ReplicaLagInfo(
                            replica_node_id=replica['node_id'],
                            replica_address=replica_address,
                            primary_node_id=master_node_id or "unknown",
                            primary_address=primary_address,
                            lag_seconds=lag_seconds,
                            replication_offset_diff=replication_offset_diff,
                            link_status=master_link_status
                        )
                        lagging_replicas.append(lag_info)
                        all_replicas_synced = False
                        max_lag = max(max_lag, lag_seconds)

                        logger.warning(
                            f"Replica {replica_address} lagging behind primary "
                            f"(lag={lag_seconds:.2f}s, offset_diff={replication_offset_diff})"
                        )
                    else:
                        max_lag = max(max_lag, lag_seconds)
                        logger.debug(
                            f"Replica {replica_address} in sync "
                            f"(lag={lag_seconds:.2f}s, offset_diff={replication_offset_diff})"
                        )
                
                except Exception as e:
                    # Replica unreachable or error querying
                    replica_address = format_node_address(replica)
                    disconnected_replicas.append(replica_address)
                    all_replicas_synced = False
                    logger.error(f"Error checking replica {replica_address}: {e}")

            # Cross-validate: Query primaries for their view of replicas
            self._cross_validate_with_primaries(
                primary_nodes,
                replica_nodes,
                disconnected_replicas,
                config
            )

            # Determine overall success
            success = True
            error_message = None

            # Initialize killed_nodes if not provided
            if killed_nodes is None:
                killed_nodes = set()

            # Validate that disconnected replicas match killed nodes (if tracking)
            if killed_nodes and disconnected_replicas:
                disconnected_set = set(disconnected_replicas)
                
                # Check for unexpected disconnections (replicas that died but weren't killed)
                unexpected_disconnections = disconnected_set - killed_nodes
                if unexpected_disconnections:
                    success = False
                    error_message = (
                        f"Unexpected replica disconnections detected: {unexpected_disconnections}. "
                        f"These replicas failed but were not killed by chaos. "
                        f"Expected killed nodes: {killed_nodes}"
                    )
                    logger.error(error_message)
                
                # Check for unexpected survivals (killed nodes that are still connected)
                # This catches bugs where killed nodes incorrectly rejoin or weren't properly killed
                expected_dead_replicas = killed_nodes & {format_node_address(r) for r in replica_nodes}
                actually_dead_replicas = disconnected_set & expected_dead_replicas
                unexpected_survivals = expected_dead_replicas - actually_dead_replicas
                
                if unexpected_survivals and success:  # Only report if no other errors
                    success = False
                    error_message = (
                        f"Killed replicas unexpectedly still connected: {unexpected_survivals}. "
                        f"These nodes were killed by chaos but are still reachable/synced. "
                        f"This indicates improper node termination or unexpected recovery."
                    )
                    logger.error(error_message)

            # CRITICAL: Check per-shard redundancy using helper function
            if success and config.min_replicas_per_shard > 0:
                redundancy_success, redundancy_error = check_per_shard_redundancy(
                    all_nodes=all_nodes,
                    replica_nodes=replica_nodes,
                    disconnected_replicas=disconnected_replicas,
                    killed_nodes=killed_nodes,
                    min_replicas_per_shard=config.min_replicas_per_shard,
                    context="replication"
                )
                if not redundancy_success:
                    success = False
                    error_message = redundancy_error

            if success and config.require_all_replicas_synced:
                # Strict mode: fail if any replica is not synced
                if not all_replicas_synced:
                    success = False
                    error_message = (
                        f"Not all replicas synced: {len(lagging_replicas)} lagging, "
                        f"{len(disconnected_replicas)} disconnected"
                    )
            elif success:
                # Lenient mode: fail if there are lagging replicas OR all replicas are disconnected
                # This ensures that excessive lag is still detected and reported as a failure
                if lagging_replicas:
                    success = False
                    error_message = (
                        f"{len(lagging_replicas)} replica(s) lagging beyond acceptable threshold "
                        f"(max_lag={max_lag:.2f}s > {config.max_acceptable_lag:.2f}s)"
                    )
                elif len(disconnected_replicas) == len(replica_nodes) and replica_nodes:
                    # Only fail if there are unexpected disconnections (not killed by chaos)
                    disconnected_set = set(disconnected_replicas)
                    unexpected_disconnections = disconnected_set - killed_nodes
                    if unexpected_disconnections:
                        success = False
                        error_message = "All replicas are disconnected"
                    else:
                        # All disconnections are due to chaos - this is expected
                        logger.info(
                            f"All {len(replica_nodes)} replica(s) disconnected but all were killed by chaos (expected)"
                        )
                elif disconnected_replicas:
                    # Some replicas disconnected but not all - check if redundancy is maintained
                    logger.info(
                        f"{len(disconnected_replicas)} replica(s) disconnected but cluster still functional"
                    )

            return ReplicationValidation(
                success=success,
                all_replicas_synced=all_replicas_synced,
                max_lag=max_lag,
                lagging_replicas=lagging_replicas,
                disconnected_replicas=disconnected_replicas,
                error_message=error_message
            )

        except Exception as e:
            logger.error(f"Replication validation failed with error: {e}")
            return ReplicationValidation(
                success=False,
                all_replicas_synced=False,
                max_lag=-1.0,
                lagging_replicas=[],
                disconnected_replicas=[],
                error_message=f"Validation error: {str(e)}"
            )

    def _cross_validate_with_primaries(
        self,
        primary_nodes: Dict[str, Dict],
        replica_nodes: List[Dict],
        disconnected_replicas: List[str],
        config: ReplicationValidationConfig
    ) -> None:
        """Cross-validate replication by querying primaries for their view of replicas."""
        logger.debug("Cross-validating replication from primary nodes' perspective")

        # Build a map of which replicas belong to which primary
        # by querying CLUSTER NODES from a reference node
        replica_to_primary_map = {}
        if replica_nodes:
            # Use first available replica to get cluster topology
            for replica in replica_nodes:
                if format_node_address(replica) in disconnected_replicas:
                    continue
                
                master_id = self._get_master_node_id(replica, None)
                if master_id:
                    replica_addr = format_node_address(replica)
                    replica_to_primary_map[replica_addr] = master_id

        # Now validate each primary
        for primary_node_id, primary in primary_nodes.items():
            def query_primary_replication(client: valkey.Valkey) -> Dict:
                return client.info('replication')
            
            repl_info = safe_query_node(
                primary,
                config.timeout,
                query_primary_replication,
                error_message="Unable to cross-validate with primary"
            )
            
            if repl_info:
                # Get number of connected replicas
                connected_slaves = int(repl_info.get('connected_slaves', 0))

                # Count expected replicas for this primary (excluding disconnected ones)
                expected_replicas = sum(
                    1 for replica_addr, master_id in replica_to_primary_map.items()
                    if master_id == primary_node_id
                    and replica_addr not in disconnected_replicas
                )

                if connected_slaves != expected_replicas:
                    logger.debug(
                        f"Primary {primary['host']}:{primary['port']} reports "
                        f"{connected_slaves} connected replicas, expected {expected_replicas} "
                        f"(may be transient during failover)"
                    )
                else:
                    logger.debug(
                        f"Primary {primary['host']}:{primary['port']} has "
                        f"{connected_slaves} replicas connected (as expected)"
                    )

    def _get_master_node_id(self, replica: Dict, cluster_connection: ClusterConnection) -> Optional[str]:
        """Get the master node ID for a replica by parsing CLUSTER NODES output."""
        nodes = query_cluster_nodes(replica, timeout=2.0)
        
        if nodes:
            for node in nodes:
                if node['is_myself'] and node['is_slave']:
                    return node['master_id']
        
        return None

    def _get_offset_difference(self, replica: Dict, primary: Optional[Dict], timeout: float) -> int:
        """Calculate replication offset difference between primary and replica."""
        if not primary:
            return 0

        try:
            # Get replica offset
            with valkey_client(replica['host'], replica['port'], timeout) as replica_client:
                replica_info = replica_client.info('replication')

                # Cast offsets to int since decode_responses=True returns strings
                replica_offset = int(replica_info.get('master_repl_offset', 0))
                if replica_offset == 0:
                    # Replica might report slave_repl_offset instead
                    replica_offset = int(replica_info.get('slave_repl_offset', 0))

            # Get primary offset
            with valkey_client(primary['host'], primary['port'], timeout) as primary_client:
                primary_info = primary_client.info('replication')

                # Cast to int since decode_responses=True returns strings
                primary_offset = int(primary_info.get('master_repl_offset', 0))

            # Calculate difference
            offset_diff = primary_offset - replica_offset
            return max(0, offset_diff)  # Return 0 if negative (shouldn't happen)

        except Exception as e:
            logger.debug(f"Error calculating offset difference: {e}")
            return 0



class ClusterStatusValidator:
    """Validates overall cluster health status"""

    def validate(self, cluster_connection: ClusterConnection, config: ClusterStatusValidationConfig, killed_nodes: Optional[set[str]] = None) -> ClusterStatusValidation:
        """Check cluster health status from all reachable nodes."""
        try:
            # Get all nodes from cluster (including failed ones for comprehensive check)
            all_nodes = cluster_connection.get_current_nodes(include_failed=True)
            
            if not all_nodes:
                return ClusterStatusValidation(
                    success=False,
                    cluster_state="unknown",
                    nodes_in_fail_state=[],
                    has_quorum=False,
                    degraded_reason="No nodes reachable",
                    error_message="Unable to connect to any cluster nodes"
                )
            
            # Track cluster state from different nodes
            cluster_states = {}
            nodes_in_fail_state = []
            reachable_nodes = []
            
            # Query cluster info from all reachable nodes
            for node in all_nodes:
                try:
                    with valkey_client(node['host'], node['port'], config.timeout) as client:
                        # Get cluster info
                        cluster_info = client.info('cluster')

                        # Extract cluster state
                        cluster_state = cluster_info.get('cluster_state', 'unknown')
                        cluster_states[format_node_address(node)] = cluster_state
                        reachable_nodes.append(node)

                        logger.debug(f"Node {node['host']}:{node['port']} reports cluster_state={cluster_state}")

                except Exception as e:
                    logger.debug(
                        f"Unable to query cluster info from {node['host']}:{node['port']}: {e}"
                    )
                    # Node is unreachable, but we already know about failed nodes from get_current_nodes
                    continue

            # Identify nodes in fail state from cluster topology
            for node in all_nodes:
                if node['status'] == 'failed':
                    node_address = format_node_address(node)
                    nodes_in_fail_state.append(node_address)

            # Determine overall cluster state
            # Use the most common state reported by reachable nodes
            if cluster_states:
                # Count occurrences of each state
                state_counts = {}
                for state in cluster_states.values():
                    state_counts[state] = state_counts.get(state, 0) + 1

                # Get the most common state
                overall_cluster_state = max(state_counts, key=state_counts.get)

                # Log if there's disagreement
                if len(set(cluster_states.values())) > 1:
                    logger.warning(
                        f"Cluster state disagreement detected: {cluster_states}"
                    )
            else:
                overall_cluster_state = "unknown"

            # Check quorum
            # A cluster has quorum if majority of primary nodes are reachable
            primary_nodes = [n for n in all_nodes if n['role'] == 'primary']
            reachable_primaries = [n for n in reachable_nodes if n['role'] == 'primary']

            has_quorum = True
            if config.require_quorum and primary_nodes:
                # Need majority of primaries to be reachable
                quorum_threshold = (len(primary_nodes) // 2) + 1
                has_quorum = len(reachable_primaries) >= quorum_threshold

                if not has_quorum:
                    logger.warning(
                        f"Quorum check failed: {len(reachable_primaries)}/{len(primary_nodes)} "
                        f"primaries reachable (need {quorum_threshold})"
                    )

            # Determine success based on configuration
            success = True
            degraded_reason = None
            error_message = None

            # Initialize killed_nodes if not provided
            if killed_nodes is None:
                killed_nodes = set()

            # Check if cluster state is acceptable
            if overall_cluster_state not in config.acceptable_states:
                # Special handling for "unknown" state when nodes were killed
                if overall_cluster_state == 'unknown' and killed_nodes:
                    # "unknown" state is expected when nodes are killed
                    # Use helper to validate killed vs failed nodes
                    validation_success, validation_error = validate_killed_vs_failed_nodes(
                        killed_nodes=killed_nodes,
                        failed_nodes=nodes_in_fail_state,
                        context="cluster_status"
                    )
                    
                    if not validation_success:
                        success = False
                        error_message = validation_error
                    else:
                        # All killed nodes are in fail state as expected
                        degraded_reason = (
                            f"Cluster in 'unknown' state due to {len(killed_nodes)} "
                            f"killed node(s) - expected behavior"
                        )
                        logger.info(
                            f"Cluster state 'unknown' is expected: killed nodes "
                            f"{killed_nodes} are in fail state"
                        )
                elif config.allow_degraded and overall_cluster_state in ['fail', 'degraded']:
                    # Degraded state is acceptable
                    degraded_reason = f"Cluster in {overall_cluster_state} state"
                    logger.info(f"Cluster in degraded state: {overall_cluster_state}")
                else:
                    success = False
                    error_message = (
                        f"Cluster state '{overall_cluster_state}' not in acceptable states: "
                        f"{config.acceptable_states}"
                    )
                    logger.error(error_message)

            # Check for nodes in fail state
            if nodes_in_fail_state:
                logger.warning(f"Nodes in fail state: {nodes_in_fail_state}")

                # Use helper to validate killed vs failed nodes
                if killed_nodes:
                    validation_success, validation_error = validate_killed_vs_failed_nodes(
                        killed_nodes=killed_nodes,
                        failed_nodes=nodes_in_fail_state,
                        context="cluster_status"
                    )
                    
                    if validation_success:
                        # Failed nodes match killed nodes - expected behavior
                        if not degraded_reason:
                            degraded_reason = (
                                f"{len(nodes_in_fail_state)} node(s) in fail state "
                                f"(killed by chaos) - expected behavior"
                            )
                    elif success:  # Only update if not already failed
                        success = False
                        error_message = validation_error
                else:
                    # No killed nodes tracked, but nodes are in fail state
                    # If cluster state is "ok" but nodes are failed, this is suspicious
                    if success and overall_cluster_state in config.acceptable_states:
                        # Cluster says it's OK but has failed nodes
                        # Only allow if explicitly configured to allow degraded state
                        if not config.allow_degraded:
                            success = False
                            error_message = (
                                f"Spontaneous node failures detected: {len(nodes_in_fail_state)} node(s) in fail state "
                                f"but were not killed by chaos. "
                                f"Failed nodes: {nodes_in_fail_state}. "
                                f"This indicates unexpected node failures or failed failover attempts. "
                                f"Cluster state: {overall_cluster_state}"
                            )
                            logger.error(error_message)
                        else:
                            # Degraded state is explicitly allowed
                            degraded_reason = (
                                f"{len(nodes_in_fail_state)} node(s) in fail state "
                                f"(degraded state allowed by config)"
                            )
                            logger.warning(
                                f"Nodes in fail state but degraded state is allowed: {nodes_in_fail_state}"
                            )
                    else:
                        # Cluster state is already bad or we're being lenient
                        if not degraded_reason:
                            degraded_reason = f"{len(nodes_in_fail_state)} node(s) in fail state"

            # Check quorum
            if not has_quorum:
                success = False
                error_message = (
                    f"Cluster does not have quorum: {len(reachable_primaries)}/{len(primary_nodes)} "
                    f"primaries reachable"
                )
                logger.error(error_message)

            # Log overall result
            if success:
                logger.info(
                    f"Cluster status validation passed: state={overall_cluster_state}, "
                    f"quorum={has_quorum}, failed_nodes={len(nodes_in_fail_state)}"
                )
            else:
                logger.error(
                    f"Cluster status validation failed: state={overall_cluster_state}, "
                    f"quorum={has_quorum}, failed_nodes={len(nodes_in_fail_state)}"
                )

            return ClusterStatusValidation(
                success=success,
                cluster_state=overall_cluster_state,
                nodes_in_fail_state=nodes_in_fail_state,
                has_quorum=has_quorum,
                degraded_reason=degraded_reason,
                error_message=error_message
            )

        except Exception as e:
            logger.error(f"Cluster status validation failed with error: {e}")
            return ClusterStatusValidation(
                success=False,
                cluster_state="unknown",
                nodes_in_fail_state=[],
                has_quorum=False,
                degraded_reason=None,
                error_message=f"Validation error: {str(e)}"
            )



class SlotCoverageValidator:
    """Validates hash slot coverage and assignment"""

    def validate(
        self,
        cluster_connection: ClusterConnection,
        config: SlotCoverageValidationConfig,
        killed_nodes: Optional[set[str]] = None
    ) -> SlotCoverageValidation:
        """Check slot coverage across the cluster from all nodes' perspectives."""
        try:
            # Get all nodes from cluster INCLUDING failed ones
            # We need failed nodes to check if slots are assigned to killed nodes
            all_nodes = cluster_connection.get_current_nodes(include_failed=True)

            if not all_nodes:
                # Unable to contact cluster
                logger.error("Unable to fetch any nodes from cluster for slot coverage validation")
                return SlotCoverageValidation(
                    success=False,
                    total_slots_assigned=0,
                    unassigned_slots=list(range(16384)),
                    conflicting_slots=[],
                    slot_distribution={},
                    error_message="Unable to contact cluster - no nodes reachable"
                )

            primary_nodes = [node for node in all_nodes if node['role'] == 'primary']
            
            if not primary_nodes:
                return SlotCoverageValidation(
                    success=False,
                    total_slots_assigned=0,
                    unassigned_slots=list(range(16384)),
                    conflicting_slots=[],
                    slot_distribution={},
                    error_message="No primary nodes available in cluster"
                )
            
            # Track slot assignments from each node's perspective
            # node_address -> {slot -> node_id}
            node_perspectives: Dict[str, Dict[int, str]] = {}

            # Track slot distribution per node: node_id -> list of slots
            slot_distribution: Dict[str, List[int]] = {}

            # Query live nodes (not failed ones) for their view of slot assignments
            # But keep all_nodes (including failed) for killed node checking later
            live_nodes = [node for node in all_nodes if node.get('status') != 'failed']
            nodes_queried = 0
            for node in live_nodes:
                parsed_nodes = query_cluster_nodes(node, timeout=config.timeout)
                
                if parsed_nodes:
                    node_address = format_node_address(node)
                    # Build slot view from parsed nodes
                    node_slot_view = {}
                    for parsed_node in parsed_nodes:
                        if parsed_node['is_master']:
                            for slot_info in parsed_node['slots']:
                                if slot_info.startswith('['):
                                    continue
                                for slot in parse_slot_range(slot_info):
                                    node_slot_view[slot] = parsed_node['node_id']
                    
                    node_perspectives[node_address] = node_slot_view
                    nodes_queried += 1

                    logger.debug(f"Node {node_address} reports {len(node_slot_view)} slots assigned")

            if nodes_queried == 0:
                return SlotCoverageValidation(
                    success=False,
                    total_slots_assigned=0,
                    unassigned_slots=list(range(16384)),
                    conflicting_slots=[],
                    slot_distribution={},
                    error_message="Unable to query slot assignments from any node"
                )

            # Build consensus view of slot assignments
            # Use majority vote for each slot
            slot_assignments: Dict[int, List[str]] = {}
            for slot in range(16384):
                # Collect which node each perspective thinks owns this slot
                slot_owners = []
                for node_addr, slot_view in node_perspectives.items():
                    if slot in slot_view:
                        slot_owners.append(slot_view[slot])

                if slot_owners:
                    # Use most common owner (consensus)
                    owner_counts = Counter(slot_owners)
                    consensus_owner = owner_counts.most_common(1)[0][0]

                    if slot not in slot_assignments:
                        slot_assignments[slot] = []

                    # Check if there's disagreement
                    if len(set(slot_owners)) > 1:
                        # Multiple nodes claim this slot - record all claimants
                        slot_assignments[slot] = list(set(slot_owners))
                    else:
                        slot_assignments[slot] = [consensus_owner]

            # Build slot distribution from consensus view
            for slot, owners in slot_assignments.items():
                if len(owners) == 1:  # Only count non-conflicting slots
                    owner = owners[0]
                    if owner not in slot_distribution:
                        slot_distribution[owner] = []
                    slot_distribution[owner].append(slot)

            # Analyze slot coverage
            total_slots_assigned = len(slot_assignments)
            unassigned_slots = []
            conflicting_slots = []

            # Check for unassigned slots
            for slot in range(16384):
                if slot not in slot_assignments:
                    unassigned_slots.append(slot)
                elif len(slot_assignments[slot]) > 1:
                    # Slot has multiple assignments (conflict)
                    conflict = SlotConflict(
                        slot=slot,
                        conflicting_nodes=slot_assignments[slot]
                    )
                    conflicting_slots.append(conflict)
                    logger.warning(
                        f"Slot {slot} has conflicting assignments: "
                        f"{slot_assignments[slot]}"
                    )

            # Log unassigned slots summary
            if unassigned_slots:
                # Group consecutive unassigned slots into ranges for cleaner logging
                ranges = group_slots_into_ranges(unassigned_slots)
                logger.warning(f"Found {len(unassigned_slots)} unassigned slots: {ranges}")

            # Determine success based on configuration
            success = True
            error_message = None

            # Initialize killed_nodes if not provided
            if killed_nodes is None:
                killed_nodes = set()

            # CRITICAL: Check if slots are assigned to killed nodes
            if killed_nodes:
                # Build reverse mapping: node_address -> node_id for killed nodes
                killed_node_ids = set()
                for node in all_nodes:
                    node_address = format_node_address(node)
                    if node_address in killed_nodes:
                        killed_node_ids.add(node['node_id'])

                # Check if any slots are assigned to killed nodes
                slots_on_killed_nodes = []
                for node_id in killed_node_ids:
                    if node_id in slot_distribution:
                        slots_on_killed_nodes.extend(slot_distribution[node_id])

                if slots_on_killed_nodes:
                    success = False
                    error_message = (
                        f"CRITICAL: {len(slots_on_killed_nodes)} slots still assigned to killed nodes. "
                        f"Killed nodes: {killed_nodes}. "
                        f"This indicates failover did not complete or slots were not reassigned. "
                        f"Affected slots: {group_slots_into_ranges(slots_on_killed_nodes)}"
                    )
                    logger.error(error_message)

            if success and config.require_full_coverage and unassigned_slots:
                success = False
                error_message = (
                    f"{len(unassigned_slots)} slots are unassigned "
                    f"(expected all 16384 slots to be assigned)"
                )
                logger.error(error_message)

            if success and not config.allow_slot_conflicts and conflicting_slots:
                success = False
                conflict_msg = (
                    f"{len(conflicting_slots)} slots have conflicting assignments"
                )
                if error_message:
                    error_message = f"{error_message}; {conflict_msg}"
                else:
                    error_message = conflict_msg
                logger.error(error_message)

            # Log overall result
            if success:
                logger.info(
                    f"Slot coverage validation passed: {total_slots_assigned}/16384 "
                    f"slots assigned across {len(primary_nodes)} primaries"
                )
            else:
                logger.error(
                    f"Slot coverage validation failed: {total_slots_assigned}/16384 "
                    f"slots assigned, {len(unassigned_slots)} unassigned, "
                    f"{len(conflicting_slots)} conflicts"
                )

            return SlotCoverageValidation(
                success=success,
                total_slots_assigned=total_slots_assigned,
                unassigned_slots=unassigned_slots,
                conflicting_slots=conflicting_slots,
                slot_distribution=slot_distribution,
                error_message=error_message
            )

        except Exception as e:
            logger.error(f"Slot coverage validation failed with error: {e}")
            return SlotCoverageValidation(
                success=False,
                total_slots_assigned=0,
                unassigned_slots=[],
                conflicting_slots=[],
                slot_distribution={},
                error_message=f"Validation error: {str(e)}"
            )

    def _parse_node_slots(self, cluster_nodes_raw: str, target_node_id: str) -> List[int]:
        """Parse CLUSTER NODES output to extract slot assignments for a specific node."""
        slots = []

        try:
            nodes = parse_cluster_nodes_output(cluster_nodes_raw)
            for node in nodes:
                if node['node_id'] == target_node_id and node['is_master']:
                    # Parse slot ranges
                    for slot_info in node['slots']:
                        # Skip importing/migrating slot markers
                        if slot_info.startswith('['):
                            continue
                        slots.extend(parse_slot_range(slot_info))
                    break

        except Exception as e:
            logger.debug(f"Error parsing node slots: {e}")

        return slots

    def _parse_all_slot_assignments(self, cluster_nodes_raw: str) -> Dict[int, str]:
        """Parse CLUSTER NODES output to extract all slot assignments.

        Returns a dict mapping slot number to the node_id that owns it.
        """
        slot_to_owner: Dict[int, str] = {}

        try:
            nodes = parse_cluster_nodes_output(cluster_nodes_raw)
            for node in nodes:
                # Only primaries have slot assignments
                if not node['is_master']:
                    continue

                # Parse slot ranges
                for slot_info in node['slots']:
                    # Skip importing/migrating slot markers
                    if slot_info.startswith('['):
                        continue

                    # Parse and assign slots
                    for slot in parse_slot_range(slot_info):
                        slot_to_owner[slot] = node['node_id']

        except Exception as e:
            logger.debug(f"Error parsing all slot assignments: {e}")

        return slot_to_owner

    # _group_slots_into_ranges moved to state_validator_helpers.py


class TopologyValidator:
    """Validates cluster topology against expectations"""

    def validate(
        self,
        cluster_connection: ClusterConnection,
        expected_topology: Optional['ExpectedTopology'],
        config: 'TopologyValidationConfig',
        killed_nodes: Optional[set] = None,
        killed_node_roles: Optional[dict] = None
    ) -> 'TopologyValidation':
        try:
            if killed_nodes is None:
                killed_nodes = set()
            if killed_node_roles is None:
                killed_node_roles = {}
            
            # Get all nodes including failed ones to detect unexpected failures
            all_nodes = cluster_connection.get_current_nodes(include_failed=True)

            if not all_nodes:
                return TopologyValidation(
                    success=False,
                    expected_primaries=expected_topology.num_primaries if expected_topology else 0,
                    actual_primaries=0,
                    expected_replicas=expected_topology.num_replicas if expected_topology else 0,
                    actual_replicas=0,
                    topology_mismatches=[],
                    error_message="No nodes reachable in cluster"
                )

            # Separate nodes into live and failed
            live_nodes = [n for n in all_nodes if n['status'] != 'failed']
            failed_nodes = [n for n in all_nodes if n['status'] == 'failed']
            
            # Detect unexpected failures using helper function
            unexpected_failures = detect_unexpected_failures(
                all_nodes=all_nodes,
                killed_nodes=killed_nodes,
                context="topology"
            )
            
            # Count nodes: ONLY live nodes
            # Failed nodes (even if killed by chaos) should not be counted as they're not functioning
            # The cluster should have promoted replacements for any killed primaries
            actual_primaries = len([n for n in live_nodes if n['role'] == 'primary'])
            actual_replicas = len([n for n in live_nodes if n['role'] == 'replica'])

            # If no expected topology provided, just report current state
            if not expected_topology:
                logger.info(
                    f"Topology validation (no expectations): "
                    f"{actual_primaries} primaries, {actual_replicas} replicas"
                )
                return TopologyValidation(
                    success=True,
                    expected_primaries=actual_primaries,
                    actual_primaries=actual_primaries,
                    expected_replicas=actual_replicas,
                    actual_replicas=actual_replicas,
                    topology_mismatches=[],
                    error_message=None
                )

            # Adjust expected topology to account for killed nodes
            # Count total killed nodes (both primaries and replicas)
            killed_primaries = 0
            killed_replicas = 0
            
            for node in failed_nodes:
                node_addr = format_node_address(node)
                if is_node_killed_by_chaos(node, killed_nodes):
                    role_at_death = killed_node_roles.get(node_addr, node['role'])
                    if role_at_death == 'primary':
                        killed_primaries += 1
                    elif role_at_death == 'replica':
                        killed_replicas += 1
            
            total_killed_nodes = killed_primaries + killed_replicas
            
            # Adjust expectations:
            # - Primary count: stays the same if replicas can promote, otherwise reduces by killed primaries
            #   * With replicas: failover promotes replicas to replace killed primaries
            #   * Without replicas: killed primaries reduce the primary count
            # - Replica count reduces by ALL killed nodes:
            #   * Each killed replica directly reduces replica count
            #   * Each killed primary causes a replica to promote (also reduces replica count)
            # - Clamp replica count at 0 to handle zero-replica scenarios where killing primaries
            #   would otherwise make the expected count negative
            
            # In zero-replica clusters, killed primaries reduce the primary count
            if expected_topology.num_replicas == 0:
                adjusted_expected_primaries = expected_topology.num_primaries - killed_primaries
            else:
                # With replicas, failover maintains primary count
                adjusted_expected_primaries = expected_topology.num_primaries
            
            adjusted_expected_replicas = max(0, expected_topology.num_replicas - total_killed_nodes)
            
            if total_killed_nodes > 0:
                logger.info(
                    f"Adjusting expected topology for chaos: "
                    f"{killed_primaries} primary(s) killed, {killed_replicas} replica(s) killed. "
                    f"Adjusted expected: {adjusted_expected_primaries} primaries, {adjusted_expected_replicas} replicas"
                )
            
            # Compare against expected topology
            topology_mismatches = []

            # Check primary count
            if actual_primaries != adjusted_expected_primaries:
                mismatch = TopologyMismatch(
                    mismatch_type="primary_count",
                    node_id="cluster",
                    expected=f"{adjusted_expected_primaries} primaries",
                    actual=f"{actual_primaries} primaries"
                )
                topology_mismatches.append(mismatch)
                logger.warning(
                    f"Primary count mismatch: expected {adjusted_expected_primaries}, "
                    f"got {actual_primaries}"
                )

            # Check replica count
            if actual_replicas != adjusted_expected_replicas:
                mismatch = TopologyMismatch(
                    mismatch_type="replica_count",
                    node_id="cluster",
                    expected=f"{adjusted_expected_replicas} replicas",
                    actual=f"{actual_replicas} replicas"
                )
                topology_mismatches.append(mismatch)
                logger.warning(
                    f"Replica count mismatch: expected {adjusted_expected_replicas}, "
                    f"got {actual_replicas}"
                )

            # Report unexpected failures as topology mismatches
            if unexpected_failures:
                for node in unexpected_failures:
                    mismatch = create_topology_mismatch_for_unexpected_failure(node)
                    topology_mismatches.append(mismatch)
            
            # Validate shard structure if provided
            if expected_topology.shard_structure:
                # Use live_nodes for shard structure validation (excludes failed nodes)
                shard_mismatches = self._validate_shard_structure(
                    live_nodes,
                    expected_topology.shard_structure,
                    config
                )
                topology_mismatches.extend(shard_mismatches)

            # Determine success
            success = True
            error_message = None
            
            # Always fail on unexpected failures (spontaneous node loss is a bug)
            if unexpected_failures:
                success = False
                error_message = (
                    f"Unexpected node failures detected: {len(unexpected_failures)} node(s) failed "
                    f"that were not killed by chaos"
                )
                logger.error(error_message)

            if config.strict_mode:
                # In strict mode, any mismatch is a failure
                if topology_mismatches and success:  # Don't override unexpected failure message
                    success = False
                    error_message = (
                        f"Topology validation failed in strict mode: "
                        f"{len(topology_mismatches)} mismatch(es) found"
                    )
            else:
                # In non-strict mode, allow some flexibility
                # But still fail on CRITICAL mismatches that indicate serious bugs
                critical_mismatches = [
                    m for m in topology_mismatches
                    if m.mismatch_type in [
                        'missing_primary',      # Missing primary is critical
                        'wrong_role',           # Wrong role is critical
                        'missing_shard',        # ADDED: Entire shard missing is critical
                        'primary_count',        # ADDED: Wrong primary count is critical
                        'replica_count'         # ADDED: Wrong replica count is critical (could indicate dropped replicas)
                    ]
                ]

                if critical_mismatches:
                    success = False
                    # Provide detailed error message
                    mismatch_details = ", ".join([
                        f"{m.mismatch_type}({m.node_id})" for m in critical_mismatches[:5]
                    ])
                    if len(critical_mismatches) > 5:
                        mismatch_details += f"... and {len(critical_mismatches) - 5} more"
                    
                    error_message = (
                        f"Topology validation failed: "
                        f"{len(critical_mismatches)} critical mismatch(es) found. "
                        f"Details: {mismatch_details}. "
                        f"This indicates cluster-bus regressions, dropped replicas, or missing shards."
                    )
                    logger.error(error_message)
                elif topology_mismatches:
                    # Non-critical mismatches (e.g., extra_replica in strict mode), log but don't fail
                    logger.info(
                        f"Topology validation passed with {len(topology_mismatches)} "
                        f"non-critical mismatch(es)"
                    )

            # Log overall result
            if success:
                logger.info(
                    f"Topology validation passed: {actual_primaries} primaries, "
                    f"{actual_replicas} replicas (expected {adjusted_expected_primaries} "
                    f"primaries, {adjusted_expected_replicas} replicas)"
                )
            else:
                logger.error(
                    f"Topology validation failed: {actual_primaries} primaries, "
                    f"{actual_replicas} replicas (expected {expected_topology.num_primaries} "
                    f"primaries, {expected_topology.num_replicas} replicas), "
                    f"{len(topology_mismatches)} mismatch(es)"
                )

            return TopologyValidation(
                success=success,
                expected_primaries=expected_topology.num_primaries,
                actual_primaries=actual_primaries,
                expected_replicas=expected_topology.num_replicas,
                actual_replicas=actual_replicas,
                topology_mismatches=topology_mismatches,
                error_message=error_message
            )

        except Exception as e:
            logger.error(f"Topology validation failed with error: {e}")
            return TopologyValidation(
                success=False,
                expected_primaries=expected_topology.num_primaries if expected_topology else 0,
                actual_primaries=0,
                expected_replicas=expected_topology.num_replicas if expected_topology else 0,
                actual_replicas=0,
                topology_mismatches=[],
                error_message=f"Validation error: {str(e)}"
            )

    def _validate_shard_structure(
        self,
        all_nodes: List[Dict],
        expected_shards: Dict[int, 'ShardExpectation'],
        config: 'TopologyValidationConfig'
    ) -> List['TopologyMismatch']:
        """Validate shard structure against expectations."""
        mismatches = []

        # Build actual shard structure
        actual_shards: Dict[int, Dict] = {}
        for node in all_nodes:
            shard_id = node.get('shard_id')
            if shard_id is None:
                continue

            if shard_id not in actual_shards:
                actual_shards[shard_id] = {
                    'primary': None,
                    'replicas': []
                }

            if node['role'] == 'primary':
                actual_shards[shard_id]['primary'] = node
            else:
                actual_shards[shard_id]['replicas'].append(node)

        # Check each expected shard
        for shard_id, expected_shard in expected_shards.items():
            if shard_id not in actual_shards:
                # Entire shard is missing
                mismatch = TopologyMismatch(
                    mismatch_type="missing_shard",
                    node_id=f"shard_{shard_id}",
                    expected=f"shard {shard_id} with primary and replicas",
                    actual="shard not found"
                )
                mismatches.append(mismatch)
                logger.warning(f"Expected shard {shard_id} not found in cluster")
                continue

            actual_shard = actual_shards[shard_id]

            # Check primary node
            if expected_shard.primary_node_id:
                if not actual_shard['primary']:
                    mismatch = TopologyMismatch(
                        mismatch_type="missing_primary",
                        node_id=expected_shard.primary_node_id,
                        expected=f"primary for shard {shard_id}",
                        actual="no primary found"
                    )
                    mismatches.append(mismatch)
                    logger.warning(f"Shard {shard_id} has no primary node")
                elif actual_shard['primary']['node_id'] != expected_shard.primary_node_id:
                    # Different node is primary (could be due to failover)
                    if config.strict_mode:
                        mismatch = TopologyMismatch(
                            mismatch_type="wrong_primary",
                            node_id=actual_shard['primary']['node_id'],
                            expected=f"primary: {expected_shard.primary_node_id}",
                            actual=f"primary: {actual_shard['primary']['node_id']}"
                        )
                        mismatches.append(mismatch)
                        logger.warning(
                            f"Shard {shard_id} primary mismatch: "
                            f"expected {expected_shard.primary_node_id}, "
                            f"got {actual_shard['primary']['node_id']}"
                        )

            # Check replica nodes
            actual_replica_ids = {r['node_id'] for r in actual_shard['replicas']}
            expected_replica_ids = set(expected_shard.replica_node_ids)

            # Check for missing replicas
            missing_replicas = expected_replica_ids - actual_replica_ids
            for replica_id in missing_replicas:
                mismatch = TopologyMismatch(
                    mismatch_type="missing_replica",
                    node_id=replica_id,
                    expected=f"replica for shard {shard_id}",
                    actual="replica not found"
                )
                mismatches.append(mismatch)
                logger.warning(
                    f"Expected replica {replica_id} not found in shard {shard_id}"
                )

            # Check for extra replicas (only in strict mode)
            if config.strict_mode:
                extra_replicas = actual_replica_ids - expected_replica_ids
                for replica_id in extra_replicas:
                    mismatch = TopologyMismatch(
                        mismatch_type="extra_replica",
                        node_id=replica_id,
                        expected="no replica",
                        actual=f"replica in shard {shard_id}"
                    )
                    mismatches.append(mismatch)
                    logger.warning(
                        f"Unexpected replica {replica_id} found in shard {shard_id}"
                    )

        # Check for extra shards (only in strict mode)
        if config.strict_mode:
            extra_shard_ids = set(actual_shards.keys()) - set(expected_shards.keys())
            for shard_id in extra_shard_ids:
                mismatch = TopologyMismatch(
                    mismatch_type="extra_shard",
                    node_id=f"shard_{shard_id}",
                    expected="no shard",
                    actual=f"shard {shard_id} exists"
                )
                mismatches.append(mismatch)
                logger.warning(f"Unexpected shard {shard_id} found in cluster")

        return mismatches


class ViewConsistencyValidator:
    """Validates consistency of cluster view across all nodes"""

    def validate(
        self,
        cluster_connection: ClusterConnection,
        config: 'ViewConsistencyValidationConfig',
        killed_nodes: Optional[set[str]] = None
    ) -> 'ViewConsistencyValidation':
        """Check cluster view consistency across all reachable nodes."""
        try:
            # Get all nodes from cluster
            all_nodes = cluster_connection.get_current_nodes(include_failed=False)

            if not all_nodes:
                return ViewConsistencyValidation(
                    success=False,
                    nodes_checked=0,
                    consistent_views=False,
                    split_brain_detected=False,
                    view_discrepancies=[],
                    consensus_percentage=0.0,
                    error_message="No nodes reachable in cluster"
                )

            if len(all_nodes) == 1:
                # Single node cluster, no view consistency to check
                logger.info("Single node cluster, view consistency check skipped")
                return ViewConsistencyValidation(
                    success=True,
                    nodes_checked=1,
                    consistent_views=True,
                    split_brain_detected=False,
                    view_discrepancies=[],
                    consensus_percentage=100.0,
                    error_message=None
                )

            # Collect CLUSTER NODES output from all reachable nodes
            node_views = {}
            nodes_checked = 0

            for node in all_nodes:
                parsed_nodes = query_cluster_nodes(node, timeout=config.timeout)
                
                if parsed_nodes:
                    # Convert parsed nodes to view format
                    parsed_view = {}
                    for parsed_node in parsed_nodes:
                        node_id = parsed_node['node_id']
                        
                        # Extract role
                        if parsed_node['is_master']:
                            role = 'primary'
                        elif parsed_node['is_slave']:
                            role = 'replica'
                        else:
                            role = 'unknown'

                        # Extract state
                        if parsed_node['is_fail']:
                            state = 'fail'
                        elif parsed_node['link_state'] == 'disconnected' or 'noaddr' in parsed_node['flags']:
                            state = 'disconnected'
                        else:
                            state = 'connected'

                        # Extract slots (for primaries)
                        slots = []
                        if parsed_node['is_master'] and 'slots' in parsed_node:
                            for slot_range in parsed_node['slots']:
                                # Parse slot range (e.g., "0-5460" or "5461")
                                if '-' in slot_range:
                                    start, end = slot_range.split('-')
                                    slots.extend(range(int(start), int(end) + 1))
                                else:
                                    try:
                                        slots.append(int(slot_range))
                                    except ValueError:
                                        # Skip non-numeric slot values
                                        pass

                        parsed_view[node_id] = {
                            'role': role,
                            'state': state,
                            'address': f"{parsed_node['host']}:{parsed_node['port']}",
                            'master_id': parsed_node.get('master_id'),
                            'slots': slots
                        }
                    
                    node_address = format_node_address(node)
                    node_views[node_address] = parsed_view
                    nodes_checked += 1

                    logger.debug(
                        f"Collected cluster view from {node_address}: "
                        f"{len(parsed_view)} nodes in view"
                    )
            
            if nodes_checked == 0:
                return ViewConsistencyValidation(
                    success=False,
                    nodes_checked=0,
                    consistent_views=False,
                    split_brain_detected=False,
                    view_discrepancies=[],
                    consensus_percentage=0.0,
                    error_message="Unable to collect cluster view from any node"
                )

            # Compare views and identify discrepancies
            view_discrepancies = self._compare_views(node_views)

            # Initialize killed_nodes if not provided
            if killed_nodes is None:
                killed_nodes = set()

            # STRENGTHENED: Filter out expected discrepancies about killed nodes
            unexpected_discrepancies = []
            if killed_nodes:
                # Build mapping of killed node addresses to node IDs
                killed_node_ids = set()
                for node_addr, node_view in node_views.items():
                    for node_id, node_info in node_view.items():
                        node_address = node_info.get('address', '').split('@')[0]  # Remove bus port
                        if node_address in killed_nodes:
                            killed_node_ids.add(node_id)

                # Filter discrepancies - only keep those NOT about killed nodes
                # BUT: Be smart about which discrepancies are expected vs unexpected
                for discrepancy in view_discrepancies:
                    subject_node_id = discrepancy.subject_node
                    
                    # Check if this discrepancy is about a killed node
                    is_about_killed_node = False
                    
                    # Check if subject_node is a killed node ID
                    if subject_node_id in killed_node_ids:
                        is_about_killed_node = True
                    
                    # Also check if the discrepancy is about a killed node address
                    if not is_about_killed_node:
                        for node_view in node_views.values():
                            if subject_node_id in node_view:
                                node_address = node_view[subject_node_id].get('address', '').split('@')[0]
                                if node_address in killed_nodes:
                                    is_about_killed_node = True
                                    break
                    
                    # Determine if this is an expected discrepancy
                    is_expected = False
                    if is_about_killed_node:
                        # For killed nodes, only certain discrepancies are expected
                        if discrepancy.discrepancy_type == "membership":
                            # Missing from view is expected
                            if "not in view" in discrepancy.actual_value or "missing" in discrepancy.actual_value.lower():
                                is_expected = True
                                logger.debug(
                                    f"Ignoring expected membership discrepancy: killed node {subject_node_id} missing from view"
                                )
                        elif discrepancy.discrepancy_type == "state":
                            # Both "fail" and "disconnected" are expected for killed nodes
                            # "disconnected" is the initial state, "fail" comes after timeout (~30s)
                            if "fail" in discrepancy.actual_value.lower() or "disconnected" in discrepancy.actual_value.lower():
                                is_expected = True
                                logger.debug(
                                    f"Ignoring expected state discrepancy: killed node {subject_node_id} in state {discrepancy.actual_value}"
                                )
                        # All other discrepancy types about killed nodes are UNEXPECTED (bugs)
                        # - role changes: killed nodes shouldn't change roles
                        # - address changes: killed nodes shouldn't change addresses
                        # - extra in view: killed nodes shouldn't appear as "extra"
                    
                    if not is_expected:
                        unexpected_discrepancies.append(discrepancy)
                        if is_about_killed_node:
                            logger.error(
                                f"UNEXPECTED discrepancy about killed node {subject_node_id}: "
                                f"type={discrepancy.discrepancy_type}, "
                                f"expected={discrepancy.expected_value}, actual={discrepancy.actual_value}"
                            )
                        else:
                            logger.warning(
                                f"UNEXPECTED view discrepancy (not about killed nodes): "
                                f"{discrepancy.discrepancy_type} for node {subject_node_id}"
                            )
            else:
                # No killed nodes tracked, all discrepancies are unexpected
                unexpected_discrepancies = view_discrepancies

            # Check for split-brain scenarios
            split_brain_detected = self._detect_split_brain(node_views)

            if split_brain_detected:
                logger.error("Split-brain scenario detected in cluster!")

            # Calculate consensus percentage
            consensus_percentage = self._calculate_consensus(node_views)

            # Determine if views are consistent (using unexpected discrepancies only)
            consistent_views = len(unexpected_discrepancies) == 0

            # Determine overall success (using unexpected discrepancies only)
            success = True
            error_message = None

            if split_brain_detected:
                success = False
                error_message = "Split-brain scenario detected"
                logger.error(error_message)
            elif config.require_full_consensus and not consistent_views:
                success = False
                error_message = (
                    f"View inconsistency detected: {len(unexpected_discrepancies)} "
                    f"UNEXPECTED discrepancy(ies) found (not about killed nodes). "
                    f"Total discrepancies: {len(view_discrepancies)} "
                    f"(including {len(view_discrepancies) - len(unexpected_discrepancies)} expected about killed nodes)"
                )
                logger.error(error_message)
            elif not consistent_views:
                # Allow transient inconsistency if configured
                if config.allow_transient_inconsistency:
                    logger.warning(
                        f"Transient view inconsistency detected: "
                        f"{len(unexpected_discrepancies)} UNEXPECTED discrepancy(ies), "
                        f"consensus={consensus_percentage:.1f}%"
                    )
                else:
                    success = False
                    error_message = (
                        f"View inconsistency detected: {len(unexpected_discrepancies)} "
                        f"UNEXPECTED discrepancy(ies) found"
                    )
                    logger.error(error_message)

            # Log overall result
            if success:
                logger.info(
                    f"View consistency validation passed: {nodes_checked} nodes checked, "
                    f"consensus={consensus_percentage:.1f}%, "
                    f"discrepancies={len(view_discrepancies)}"
                )
            else:
                logger.error(
                    f"View consistency validation failed: {nodes_checked} nodes checked, "
                    f"consensus={consensus_percentage:.1f}%, "
                    f"discrepancies={len(view_discrepancies)}, "
                    f"split_brain={split_brain_detected}"
                )

            return ViewConsistencyValidation(
                success=success,
                nodes_checked=nodes_checked,
                consistent_views=consistent_views,
                split_brain_detected=split_brain_detected,
                view_discrepancies=view_discrepancies,
                consensus_percentage=consensus_percentage,
                error_message=error_message
            )

        except Exception as e:
            logger.error(f"View consistency validation failed with error: {e}")
            return ViewConsistencyValidation(
                success=False,
                nodes_checked=0,
                consistent_views=False,
                split_brain_detected=False,
                view_discrepancies=[],
                consensus_percentage=0.0,
                error_message=f"Validation error: {str(e)}"
            )
    
    def _parse_cluster_nodes(self, cluster_nodes_raw: str) -> Dict[str, Dict]:
        """Parse CLUSTER NODES output into a structured format."""
        parsed_nodes = {}

        try:
            nodes = parse_cluster_nodes_output(cluster_nodes_raw)
            for node in nodes:
                node_id = node['node_id']
                
                # Extract role
                if node['is_master']:
                    role = 'primary'
                elif node['is_slave']:
                    role = 'replica'
                else:
                    role = 'unknown'

                # Extract state
                if node['is_fail']:
                    state = 'fail'
                elif node['link_state'] == 'disconnected' or 'noaddr' in node['flags']:
                    state = 'disconnected'
                else:
                    state = 'connected'

                # Parse slot assignments (for primaries)
                slots = []
                for slot_info in node['slots']:
                    # Skip importing/migrating slot markers
                    if slot_info.startswith('['):
                        continue

                    # Parse slot range
                    if '-' in slot_info:
                        start, end = slot_info.split('-')
                        slots.extend(range(int(start), int(end) + 1))
                    else:
                        try:
                            slots.append(int(slot_info))
                        except ValueError:
                            # Not a slot number, skip
                            continue

                parsed_nodes[node_id] = {
                    'node_id': node_id,
                    'address': node['address'],
                    'role': role,
                    'state': state,
                    'master_id': node['master_id'],
                    'slots': slots,
                    'flags': node['flags']
                }

        except Exception as e:
            logger.debug(f"Error parsing cluster nodes: {e}")

        return parsed_nodes

    def _compare_views(self, node_views: Dict[str, Dict[str, Dict]]) -> List['ViewDiscrepancy']:
        """Compare cluster views from different nodes and identify discrepancies."""
        discrepancies = []

        if len(node_views) < 2:
            return discrepancies

        # Get a reference view (first node's view)
        reference_address = list(node_views.keys())[0]
        reference_view = node_views[reference_address]

        # Compare each node's view against the reference
        for node_address, node_view in node_views.items():
            if node_address == reference_address:
                continue

            # Check for membership discrepancies
            reference_node_ids = set(reference_view.keys())
            current_node_ids = set(node_view.keys())

            # Nodes in reference but not in current view
            missing_nodes = reference_node_ids - current_node_ids
            for missing_node_id in missing_nodes:
                discrepancy = ViewDiscrepancy(
                    discrepancy_type="membership",
                    node_reporting=node_address,
                    subject_node=missing_node_id,
                    expected_value="present in cluster",
                    actual_value="not in view"
                )
                discrepancies.append(discrepancy)
                logger.warning(
                    f"Node {node_address} missing node {missing_node_id} in its view"
                )

            # Nodes in current view but not in reference
            extra_nodes = current_node_ids - reference_node_ids
            for extra_node_id in extra_nodes:
                discrepancy = ViewDiscrepancy(
                    discrepancy_type="membership",
                    node_reporting=node_address,
                    subject_node=extra_node_id,
                    expected_value="not in cluster",
                    actual_value="present in view"
                )
                discrepancies.append(discrepancy)
                logger.warning(
                    f"Node {node_address} has extra node {extra_node_id} in its view"
                )

            # For common nodes, check for attribute discrepancies
            common_node_ids = reference_node_ids & current_node_ids
            for node_id in common_node_ids:
                ref_node = reference_view[node_id]
                curr_node = node_view[node_id]

                # Check role discrepancy
                if ref_node['role'] != curr_node['role']:
                    discrepancy = ViewDiscrepancy(
                        discrepancy_type="role",
                        node_reporting=node_address,
                        subject_node=node_id,
                        expected_value=ref_node['role'],
                        actual_value=curr_node['role']
                    )
                    discrepancies.append(discrepancy)
                    logger.warning(
                        f"Node {node_address} sees {node_id} as {curr_node['role']}, "
                        f"but reference sees it as {ref_node['role']}"
                    )

                # Check state discrepancy
                if ref_node['state'] != curr_node['state']:
                    discrepancy = ViewDiscrepancy(
                        discrepancy_type="state",
                        node_reporting=node_address,
                        subject_node=node_id,
                        expected_value=ref_node['state'],
                        actual_value=curr_node['state']
                    )
                    discrepancies.append(discrepancy)
                    logger.warning(
                        f"Node {node_address} sees {node_id} in state {curr_node['state']}, "
                        f"but reference sees it in state {ref_node['state']}"
                    )

                # Check address discrepancy (excluding port differences for same host)
                ref_addr = ref_node['address'].split('@')[0]  # Remove cluster bus port
                curr_addr = curr_node['address'].split('@')[0]
                if ref_addr != curr_addr:
                    discrepancy = ViewDiscrepancy(
                        discrepancy_type="address",
                        node_reporting=node_address,
                        subject_node=node_id,
                        expected_value=ref_addr,
                        actual_value=curr_addr
                    )
                    discrepancies.append(discrepancy)
                    logger.warning(
                        f"Node {node_address} sees {node_id} at {curr_addr}, "
                        f"but reference sees it at {ref_addr}"
                    )

        return discrepancies

    def _detect_split_brain(self, node_views: Dict[str, Dict[str, Dict]]) -> bool:
        # Build a map of slots to primary nodes from each node's perspective
        slot_primary_map: Dict[int, set] = {}

        for node_address, node_view in node_views.items():
            for node_id, node_info in node_view.items():
                if node_info['role'] == 'primary' and node_info.get('slots'):
                    for slot in node_info['slots']:
                        if slot not in slot_primary_map:
                            slot_primary_map[slot] = set()
                        slot_primary_map[slot].add(node_id)

        # Check if any slot has multiple primaries claiming it
        for slot, primaries in slot_primary_map.items():
            if len(primaries) > 1:
                logger.error(
                    f"Split-brain detected: slot {slot} claimed by multiple primaries: "
                    f"{primaries}"
                )
                return True

        return False

    def _calculate_consensus(self, node_views: Dict[str, Dict[str, Dict]]) -> float:
        """Calculate consensus percentage based on view agreement."""
        if len(node_views) < 2:
            return 100.0

        # Count how many nodes agree on the cluster membership
        # Use the most common view as the "majority view"

        # Create a signature for each view based on node IDs and their roles
        view_signatures = {}
        for node_address, node_view in node_views.items():
            # Create a frozenset of (node_id, role) tuples as signature
            signature = frozenset(
                (node_id, info['role'])
                for node_id, info in node_view.items()
            )

            if signature not in view_signatures:
                view_signatures[signature] = []
            view_signatures[signature].append(node_address)

        # Find the most common view
        if not view_signatures:
            return 0.0

        max_agreement = max(len(nodes) for nodes in view_signatures.values())
        total_nodes = len(node_views)

        consensus_percentage = (max_agreement / total_nodes) * 100.0

        logger.debug(
            f"Consensus calculation: {max_agreement}/{total_nodes} nodes "
            f"agree on cluster view ({consensus_percentage:.1f}%)"
        )

        return consensus_percentage


class DataConsistencyValidator:
    """Validates data consistency across cluster nodes"""

    def __init__(self):
        """Initialize the data consistency validator."""
        self.test_keys: Dict[str, str] = {}  # key -> expected_value

    def write_test_keys(self, cluster_connection: ClusterConnection, config: DataConsistencyValidationConfig) -> bool:
        try:
            # Get live nodes for cluster client
            live_nodes = cluster_connection.get_live_nodes()
            
            if not live_nodes:
                logger.error("No live nodes found to write test keys")
                return False
            
            # Use cluster-aware client that handles MOVED redirects
            startup_nodes = [
                ClusterNode(host=node['host'], port=node['port'])
                for node in live_nodes[:3]
            ]
            
            cluster_client = valkey.ValkeyCluster(
                startup_nodes=startup_nodes,
                decode_responses=True,
                skip_full_coverage_check=True,
                socket_timeout=config.timeout
            )
            
            # Write test keys
            for i in range(config.num_test_keys):
                key = f"{config.key_prefix}{i}"
                value = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
                
                try:
                    cluster_client.set(key, value)
                    self.test_keys[key] = value
                except Exception as e:
                    logger.warning(f"Failed to write test key {key}: {e}")
                    continue
            
            cluster_client.close()
            
            logger.info(f"Successfully wrote {len(self.test_keys)} test keys to cluster")
            return len(self.test_keys) > 0
            
        except Exception as e:
            logger.error(f"Error writing test keys: {e}")
            return False

    def validate(self, cluster_connection: ClusterConnection, config: DataConsistencyValidationConfig) -> DataConsistencyValidation:
        try:
            if not self.test_keys:
                # No test keys to validate - this is a failure because we couldn't seed data
                logger.warning("No test keys to validate - data consistency check cannot run")
                return DataConsistencyValidation(
                    success=False,
                    test_keys_checked=0,
                    missing_keys=[],
                    inconsistent_keys=[],
                    unreachable_keys=[],
                    error_message="No test keys available - data seeding failed or was skipped"
                )
            
            # Get all live nodes
            live_nodes = cluster_connection.get_live_nodes()
            
            if not live_nodes:
                return DataConsistencyValidation(
                    success=False,
                    test_keys_checked=0,
                    missing_keys=[],
                    inconsistent_keys=[],
                    unreachable_keys=[],
                    error_message="No live nodes available to validate data"
                )
            
            missing_keys = []
            inconsistent_keys = []
            unreachable_keys = []
            
            # Use cluster-aware client to properly route requests to the correct nodes
            startup_nodes = [
                ClusterNode(host=node['host'], port=node['port'])
                for node in live_nodes[:3]  # Use first 3 live nodes as startup nodes
            ]
            
            if not startup_nodes:
                logger.error("No live nodes available for cluster client")
                return DataConsistencyValidation(
                    success=False,
                    test_keys_checked=0,
                    missing_keys=[],
                    inconsistent_keys=[],
                    unreachable_keys=list(self.test_keys.keys()),
                    error_message="No live nodes available"
                )
            
            try:
                # Create cluster-aware client that handles MOVED redirects automatically
                cluster_client = valkey.ValkeyCluster(
                    startup_nodes=startup_nodes,
                    decode_responses=True,
                    skip_full_coverage_check=True,
                    read_from_replicas=False,  # Read from primaries for authoritative values
                    socket_timeout=config.timeout
                )
                
                # Check each test key using cluster-aware routing
                for key, expected_value in self.test_keys.items():
                    try:
                        actual_value = cluster_client.get(key)
                        
                        if actual_value is None:
                            missing_keys.append(key)
                            logger.warning(f"Test key {key} is missing from cluster")
                        elif actual_value != expected_value:
                            # Determine which slot this key belongs to for logging
                            slot = compute_cluster_slot(key)
                            inconsistency = DataInconsistency(
                                key=key,
                                inconsistency_type="value_mismatch",
                                expected_value=expected_value,
                                actual_values={f"slot_{slot}": actual_value}
                            )
                            inconsistent_keys.append(inconsistency)
                            logger.warning(
                                f"Test key {key} (slot {slot}) has wrong value: "
                                f"expected {expected_value[:16]}, got {actual_value[:16] if actual_value else 'None'}"
                            )
                        else:
                            logger.debug(f"Test key {key} validated successfully")
                        
                    except Exception as e:
                        logger.warning(f"Error validating key {key}: {e}")
                        unreachable_keys.append(key)
                
                cluster_client.close()
                
            except Exception as e:
                logger.error(f"Failed to create cluster client: {e}")
                logger.info("Falling back to manual slot routing")
                
                # Fetch slot assignments from cluster
                slot_map = fetch_cluster_slot_assignments(live_nodes, config.timeout)
                
                if not slot_map:
                    logger.error("Failed to fetch slot assignments - cannot validate keys")
                    return DataConsistencyValidation(
                        success=False,
                        test_keys_checked=0,
                        missing_keys=[],
                        inconsistent_keys=[],
                        unreachable_keys=list(self.test_keys.keys()),
                        error_message="Failed to fetch slot assignments for fallback routing"
                    )
                
                # Fall back to checking what we can with individual node connections
                for key, expected_value in self.test_keys.items():
                    try:
                        # Compute slot and find the owning node
                        slot = compute_cluster_slot(key)
                        owning_node_addr = slot_map.get(slot)
                        
                        if not owning_node_addr:
                            logger.debug(f"No owner found for slot {slot} (key {key})")
                            unreachable_keys.append(key)
                            continue
                        
                        # Find the node info from live_nodes
                        owning_node = None
                        for node in live_nodes:
                            if format_node_address(node) == owning_node_addr:
                                owning_node = node
                                break
                        
                        if not owning_node:
                            logger.debug(f"Owning node {owning_node_addr} not in live nodes")
                            unreachable_keys.append(key)
                            continue
                        
                        with valkey_client(owning_node['host'], owning_node['port'], config.timeout) as client:
                            actual_value = client.get(key)
                            
                            if actual_value is None:
                                missing_keys.append(key)
                                logger.warning(f"Test key {key} is missing from cluster")
                            elif actual_value != expected_value:
                                inconsistency = DataInconsistency(
                                    key=key,
                                    inconsistency_type="value_mismatch",
                                    expected_value=expected_value,
                                    actual_values={format_node_address(owning_node): actual_value}
                                )
                                inconsistent_keys.append(inconsistency)
                                logger.warning(
                                    f"Test key {key} has wrong value: "
                                    f"expected {expected_value[:16]}, got {actual_value[:16] if actual_value else 'None'}"
                                )
                            else:
                                logger.debug(f"Test key {key} validated successfully")
                        
                    except Exception as e:
                        logger.warning(f"Error validating key {key}: {e}")
                        unreachable_keys.append(key)
            
            # Optionally check cross-replica consistency
            if config.check_cross_replica_consistency:
                self._check_cross_replica_consistency(
                    cluster_connection,
                    config,
                    inconsistent_keys
                )
            
            # Determine success
            success = True
            error_message = None
            
            if missing_keys:
                success = False
                error_message = (
                    f"Data loss detected: {len(missing_keys)} test key(s) missing from cluster. "
                    f"Keys: {missing_keys[:10]}{'...' if len(missing_keys) > 10 else ''}"
                )
                logger.error(error_message)
            
            if inconsistent_keys:
                success = False
                inconsistency_msg = (
                    f"Data inconsistency detected: {len(inconsistent_keys)} key(s) have wrong values"
                )
                if error_message:
                    error_message = f"{error_message}; {inconsistency_msg}"
                else:
                    error_message = inconsistency_msg
                logger.error(error_message)
            
            # Fail if too many keys are unreachable (indicates routing/cluster issues)
            unreachable_threshold = max(1, len(self.test_keys) * 0.1)  # Allow up to 10% unreachable
            if len(unreachable_keys) >= unreachable_threshold:
                success = False
                unreachable_msg = (
                    f"{len(unreachable_keys)} test key(s) unreachable "
                    f"(threshold: {int(unreachable_threshold)})"
                )
                if error_message:
                    error_message = f"{error_message}; {unreachable_msg}"
                else:
                    error_message = unreachable_msg
                logger.error(error_message)
            
            logger.info(
                f"Data consistency validation: {len(self.test_keys)} keys checked, "
                f"{len(missing_keys)} missing, {len(inconsistent_keys)} inconsistent, "
                f"{len(unreachable_keys)} unreachable"
            )
            
            return DataConsistencyValidation(
                success=success,
                test_keys_checked=len(self.test_keys),
                missing_keys=missing_keys,
                inconsistent_keys=inconsistent_keys,
                unreachable_keys=unreachable_keys,
                error_message=error_message
            )
            
        except Exception as e:
            logger.error(f"Data consistency validation failed with error: {e}")
            return DataConsistencyValidation(
                success=False,
                test_keys_checked=len(self.test_keys),
                missing_keys=[],
                inconsistent_keys=[],
                unreachable_keys=[],
                error_message=f"Validation error: {str(e)}"
            )

    def _check_cross_replica_consistency(
        self,
        cluster_connection: ClusterConnection,
        config: DataConsistencyValidationConfig,
        inconsistent_keys: List[DataInconsistency]
    ) -> None:
        try:
            # Get all live nodes (primaries and replicas)
            all_nodes = cluster_connection.get_live_nodes()
            replica_nodes = [n for n in all_nodes if n['role'] == 'replica']
            
            if not replica_nodes:
                logger.debug("No replicas to check for cross-replica consistency")
                return
            
            # Sample a subset of test keys to check (checking all can be slow)
            sample_size = min(20, len(self.test_keys))
            sampled_keys = random.sample(list(self.test_keys.keys()), sample_size)
            
            logger.debug(f"Checking cross-replica consistency for {sample_size} test keys across {len(replica_nodes)} replicas")
            
            # For each sampled key, check if replicas have the same value as expected
            for key in sampled_keys:
                expected_value = self.test_keys[key]
                
                # Check each replica
                for replica in replica_nodes:
                    try:
                        with valkey_client(replica['host'], replica['port'], config.timeout) as client:
                            actual_value = client.get(key)
                            
                            if actual_value is None:
                                # Key missing on replica
                                inconsistency = DataInconsistency(
                                    key=key,
                                    inconsistency_type="missing_on_replica",
                                    expected_value=expected_value,
                                    actual_values={format_node_address(replica): "None"}
                                )
                                inconsistent_keys.append(inconsistency)
                                logger.warning(
                                    f"Key {key} missing on replica {format_node_address(replica)}"
                                )
                            elif actual_value != expected_value:
                                # Value mismatch on replica
                                inconsistency = DataInconsistency(
                                    key=key,
                                    inconsistency_type="replica_divergence",
                                    expected_value=expected_value,
                                    actual_values={format_node_address(replica): actual_value}
                                )
                                inconsistent_keys.append(inconsistency)
                                logger.warning(
                                    f"Key {key} has diverged on replica {format_node_address(replica)}: "
                                    f"expected {expected_value[:16]}, got {actual_value[:16]}"
                                )
                    except Exception as e:
                        logger.debug(f"Error checking replica {format_node_address(replica)} for key {key}: {e}")
                        # Don't fail on individual replica query errors
                        continue
            
            if inconsistent_keys:
                logger.warning(
                    f"Cross-replica consistency check found {len(inconsistent_keys)} inconsistencies"
                )
        
        except Exception as e:
            logger.error(f"Error during cross-replica consistency check: {e}")
            # Don't fail the entire validation on cross-replica check errors
    
    # _compute_slot and _fetch_slot_assignments moved to state_validator_helpers.py


class StateValidator:
    """
    Comprehensive cluster state validation coordinator.
    Executes all validation checks and aggregates results.
    """

    def __init__(self, config: 'StateValidationConfig'):
        """Initialize the state validator with configuration."""
        self.config = config

        # Initialize all sub-validators
        self.replication_validator = ReplicationValidator()
        self.cluster_status_validator = ClusterStatusValidator()
        self.slot_validator = SlotCoverageValidator()
        self.topology_validator = TopologyValidator()
        self.view_consistency_validator = ViewConsistencyValidator()
        self.data_consistency_validator = DataConsistencyValidator()

        # Track killed nodes from chaos injections
        self.killed_nodes: set[str] = set()
        self.shards_with_primary_killed: set[int] = set()
        # Track killed node roles at time of death: node_address -> role
        self.killed_node_roles: dict[str, str] = {}

        logger.debug("StateValidator initialized")

    def register_killed_node(self, node_address: str, node_role: str, shard_id: int) -> None:
        """Register a node that was killed by chaos injection."""
        self.killed_nodes.add(node_address)
        self.killed_node_roles[node_address] = node_role
        if node_role == 'primary':
            self.shards_with_primary_killed.add(shard_id)
        logger.debug(f"Registered killed node: {node_address} (role: {node_role}, shard: {shard_id})")

    def clear_killed_nodes(self) -> None:
        """Clear the list of killed nodes (e.g., after recovery)."""
        self.killed_nodes.clear()
        self.shards_with_primary_killed.clear()
        self.killed_node_roles.clear()
        logger.debug("Cleared killed nodes list")

    def write_test_data(self, cluster_connection: ClusterConnection) -> bool:
        if not self.config.check_data_consistency:
            return True
        
        return self.data_consistency_validator.write_test_keys(
            cluster_connection,
            self.config.data_consistency_config
        )

    def _run_validation_check(
        self,
        check_name: str,
        validator_func: Callable,
        failed_checks: List[str],
        error_messages: List[str],
        display_name: Optional[str] = None
    ) -> Any:
        if display_name is None:
            display_name = check_name.replace('_', ' ').title()
        
        logger.info(f"Running {check_name.replace('_', ' ')} validation")
        
        try:
            result = validator_func()
            
            if not result.success:
                failed_checks.append(check_name)
                if result.error_message:
                    error_messages.append(f"{display_name}: {result.error_message}")
            
            logger.info(
                f"{display_name} validation: {'PASSED' if result.success else 'FAILED'}"
            )
            
            return result
            
        except ValidationTimeoutError:
            # Re-raise timeout exceptions so they propagate to validate_state
            raise
        except Exception as e:
            logger.error(f"{display_name} validation error: {e}")
            failed_checks.append(check_name)
            error_messages.append(f"{display_name} validation error: {str(e)}")
            return None

    def validate_state(
        self,
        cluster_connection: ClusterConnection,
        expected_topology: Optional['ExpectedTopology'] = None,
        operation_context: Optional['OperationContext'] = None
    ) -> 'StateValidationResult':
        """Execute all validation checks after an operation."""
        validation_start = time.time()
        timeout_deadline = validation_start + self.config.validation_timeout

        # Optional stabilization wait before validation
        if self.config.stabilization_wait > 0:
            # Cap stabilization wait at remaining timeout budget
            remaining_time = timeout_deadline - time.time()
            actual_wait = min(self.config.stabilization_wait, remaining_time)
            
            if actual_wait <= 0:
                # No time left for stabilization
                logger.warning(
                    f"Skipping stabilization wait: validation timeout ({self.config.validation_timeout}s) "
                    f"already exceeded or no time remaining"
                )
                return StateValidationResult(
                    overall_success=False,
                    validation_timestamp=validation_start,
                    validation_duration=time.time() - validation_start,
                    replication=None,
                    cluster_status=None,
                    slot_coverage=None,
                    topology=None,
                    view_consistency=None,
                    data_consistency=None,
                    failed_checks=["validation_timeout"],
                    error_messages=[f"Validation exceeded timeout of {self.config.validation_timeout}s before stabilization"]
                )
            
            if actual_wait < self.config.stabilization_wait:
                logger.warning(
                    f"Capping stabilization wait at {actual_wait:.2f}s "
                    f"(requested {self.config.stabilization_wait}s, but only {remaining_time:.2f}s remaining in timeout budget)"
                )
            else:
                logger.info(
                    f"Waiting {self.config.stabilization_wait}s for cluster stabilization "
                    "before validation"
                )
            
            time.sleep(actual_wait)
            
            # Check if we're now at or past the deadline
            if time.time() >= timeout_deadline:
                logger.error(
                    f"Validation timeout ({self.config.validation_timeout}s) exceeded "
                    f"during stabilization wait"
                )
                return StateValidationResult(
                    overall_success=False,
                    validation_timestamp=validation_start,
                    validation_duration=time.time() - validation_start,
                    replication=None,
                    cluster_status=None,
                    slot_coverage=None,
                    topology=None,
                    view_consistency=None,
                    data_consistency=None,
                    failed_checks=["validation_timeout"],
                    error_messages=[f"Validation exceeded timeout of {self.config.validation_timeout}s during stabilization"]
                )

        # Track individual check results
        replication_result = None
        cluster_status_result = None
        slot_coverage_result = None
        topology_result = None
        view_consistency_result = None
        data_consistency_result = None

        failed_checks = []
        error_messages = []

        # Execute enabled validation checks with timeout enforcement
        try:
            # Calculate remaining timeout for validation checks
            remaining_timeout = timeout_deadline - time.time()
            if remaining_timeout <= 0:
                raise ValidationTimeoutError(
                    f"Validation timeout ({self.config.validation_timeout}s) exceeded before checks started"
                )
            
            # Use timeout context manager for validation execution
            with validation_timeout(remaining_timeout):
                # 1. Replication validation
                if self.config.check_replication:
                    # Check timeout before each validation
                    if time.time() >= timeout_deadline:
                        raise ValidationTimeoutError(
                            f"Validation timeout ({self.config.validation_timeout}s) exceeded"
                        )
                    replication_result = self._run_validation_check(
                        check_name="replication",
                        validator_func=lambda: self.replication_validator.validate(
                            cluster_connection,
                            self.config.replication_config,
                            killed_nodes=self.killed_nodes
                        ),
                        failed_checks=failed_checks,
                        error_messages=error_messages
                    )

                # 2. Cluster status validation
                if self.config.check_cluster_status:
                    if time.time() >= timeout_deadline:
                        raise ValidationTimeoutError(
                            f"Validation timeout ({self.config.validation_timeout}s) exceeded"
                        )
                    cluster_status_result = self._run_validation_check(
                        check_name="cluster_status",
                        validator_func=lambda: self.cluster_status_validator.validate(
                            cluster_connection,
                            self.config.cluster_status_config,
                            killed_nodes=self.killed_nodes
                        ),
                        failed_checks=failed_checks,
                        error_messages=error_messages,
                        display_name="Cluster Status"
                    )

                # 3. Slot coverage validation
                if self.config.check_slot_coverage:
                    if time.time() >= timeout_deadline:
                        raise ValidationTimeoutError(
                            f"Validation timeout ({self.config.validation_timeout}s) exceeded"
                        )
                    slot_coverage_result = self._run_validation_check(
                        check_name="slot_coverage",
                        validator_func=lambda: self.slot_validator.validate(
                            cluster_connection,
                            self.config.slot_coverage_config,
                            killed_nodes=self.killed_nodes
                        ),
                        failed_checks=failed_checks,
                        error_messages=error_messages,
                        display_name="Slot Coverage"
                    )

                # 4. Topology validation
                if self.config.check_topology and expected_topology:
                    if time.time() >= timeout_deadline:
                        raise ValidationTimeoutError(f"Validation timeout ({self.config.validation_timeout}s) exceeded")
                    topology_result = self._run_validation_check(
                        check_name="topology",
                        validator_func=lambda: self.topology_validator.validate(
                            cluster_connection,
                            expected_topology,
                            self.config.topology_config,
                            self.killed_nodes,
                            self.killed_node_roles
                        ),
                        failed_checks=failed_checks,
                        error_messages=error_messages
                    )
                elif self.config.check_topology and not expected_topology:
                    logger.info("Topology validation skipped (no expected topology provided)")

                # 5. View consistency validation
                if self.config.check_view_consistency:
                    if time.time() >= timeout_deadline:
                        raise ValidationTimeoutError(f"Validation timeout ({self.config.validation_timeout}s) exceeded")
                    view_consistency_result = self._run_validation_check(
                        check_name="view_consistency",
                        validator_func=lambda: self.view_consistency_validator.validate(
                            cluster_connection,
                            self.config.view_consistency_config,
                            killed_nodes=self.killed_nodes
                        ),
                        failed_checks=failed_checks,
                        error_messages=error_messages,
                        display_name="View Consistency"
                    )

                # 6. Data consistency validation
                if self.config.check_data_consistency:
                    if time.time() >= timeout_deadline:
                        raise ValidationTimeoutError(f"Validation timeout ({self.config.validation_timeout}s) exceeded")
                    data_consistency_result = self._run_validation_check(
                        check_name="data_consistency",
                        validator_func=lambda: self.data_consistency_validator.validate(
                            cluster_connection,
                            self.config.data_consistency_config
                        ),
                        failed_checks=failed_checks,
                        error_messages=error_messages,
                        display_name="Data Consistency"
                    )
                
                # 7. Log validation for affected shards
                log_result = None
                if self.config.check_logs and operation_context:
                    if time.time() >= timeout_deadline:
                        raise ValidationTimeoutError(f"Validation timeout ({self.config.validation_timeout}s) exceeded")
                    
                    log_validator = ShardLogValidator()
                    all_nodes = cluster_connection.get_current_nodes(include_failed=True)
                    
                    # Convert dict nodes to NodeInfo for log validator
                    node_info_list = []
                    for node_dict in all_nodes:
                        # Find matching node from initial_nodes to get log_file path
                        matching_node = next(
                            (n for n in cluster_connection.initial_nodes 
                            if n.cluster_node_id == node_dict.get('node_id')),
                            None
                        )
                        if matching_node:
                            matching_node.role = node_dict.get('role', matching_node.role)
                            node_info_list.append(matching_node)
                    
                    log_result = log_validator.validate_affected_shards(
                        node_info_list,
                        operation_context.operations if hasattr(operation_context, 'operations') else [],
                        self.killed_nodes,
                        self.shards_with_primary_killed
                    )
                    
                    if not log_result.success:
                        failed_checks.append("log_validation")
                        for finding in log_result.findings:
                            if finding.severity == 'error':
                                logger.error(f"Log validation error - Shard {finding.shard_id}, Node {finding.node_id}: {finding.message} | Log: {finding.log_line}")
                            elif finding.severity == 'warning':
                                logger.warning(f"Log validation warning - Shard {finding.shard_id}, Node {finding.node_id}: {finding.message} | Log: {finding.log_line}")
                    
                    logger.info(f"Log validation: {'PASSED' if log_result.success else 'FAILED'}")

        except ValidationTimeoutError as e:
            logger.error(f"Validation timeout: {e}")
            failed_checks.append("validation_timeout")
            error_messages.append(str(e))
        except Exception as e:
            logger.error(f"Unexpected error during validation: {e}")
            failed_checks.append("validation_framework")
            error_messages.append(f"Unexpected validation error: {str(e)}")

        # Calculate validation duration
        validation_duration = time.time() - validation_start

        # Determine overall success
        overall_success = len(failed_checks) == 0

        # Create comprehensive result
        result = StateValidationResult(
            overall_success=overall_success,
            validation_timestamp=validation_start,
            validation_duration=validation_duration,
            replication=replication_result,
            cluster_status=cluster_status_result,
            slot_coverage=slot_coverage_result,
            topology=topology_result,
            view_consistency=view_consistency_result,
            data_consistency=data_consistency_result,
            log_validation=log_result if 'log_result' in locals() else None,
            failed_checks=failed_checks,
            error_messages=error_messages
        )

        # Log overall result
        if overall_success:
            logger.info(
                f"State validation PASSED "
                f"(duration: {validation_duration:.2f}s)"
            )
        else:
            logger.error(
                f"State validation FAILED "
                f"(duration: {validation_duration:.2f}s, "
                f"failed checks: {', '.join(failed_checks)})"
            )

            # Check for critical failures
            if result.is_critical_failure():
                logger.error(
                    "CRITICAL FAILURE DETECTED: "
                    "Slot coverage lost or split-brain scenario"
                )

        return result


    def validate_with_retry(
        self,
        cluster_connection: ClusterConnection,
        expected_topology: Optional['ExpectedTopology'] = None,
        operation_context: Optional['OperationContext'] = None
    ) -> 'StateValidationResult':
        """Execute validation with retry logic for transient failures."""
        attempt = 0
        max_attempts = self.config.max_retries + 1  # Initial attempt + retries
        last_failed_checks = set()

        while attempt < max_attempts:
            attempt += 1

            if attempt > 1:
                logger.info(f"Validation attempt {attempt}/{max_attempts}")

            # Execute validation
            result = self.validate_state(
                cluster_connection,
                expected_topology,
                operation_context
            )

            # If validation passed, return immediately
            if result.overall_success:
                if attempt > 1:
                    logger.info(
                        f"Validation succeeded on attempt {attempt}/{max_attempts}"
                    )
                return result

            # Check if this is a critical failure
            if result.is_critical_failure():
                logger.error(
                    "Critical failure detected - skipping retry. "
                    f"Failed checks: {', '.join(result.failed_checks)}"
                )
                return result

            # Check if the same checks are failing repeatedly (deterministic failure)
            current_failed_checks = set(result.failed_checks)
            if attempt > 1 and current_failed_checks == last_failed_checks:
                logger.error(
                    f"Same checks failing repeatedly (attempt {attempt}): {', '.join(result.failed_checks)}. "
                    "This appears to be a deterministic failure, not a transient issue. Skipping further retries."
                )
                return result
            last_failed_checks = current_failed_checks

            # If we have more attempts and retry is enabled, wait and retry
            if attempt < max_attempts and self.config.retry_on_transient_failure:
                # Calculate backoff delay (exponential backoff)
                backoff_delay = self.config.retry_delay * (2 ** (attempt - 1))

                logger.warning(
                    f"Validation failed (attempt {attempt}/{max_attempts}), "
                    f"retrying in {backoff_delay:.1f}s "
                    f"Failed checks: {', '.join(result.failed_checks)}"
                )

                time.sleep(backoff_delay)
            else:
                # No more retries or retry disabled
                if not self.config.retry_on_transient_failure:
                    logger.error(
                        "Validation failed and retry is disabled. "
                        f"Failed checks: {', '.join(result.failed_checks)}"
                    )
                else:
                    logger.error(
                        f"Validation failed after {max_attempts} attempt(s). "
                        f"Failed checks: {', '.join(result.failed_checks)}"
                    )
                return result

        # This should not be reached, but return the last result just in case
        logger.error("Validation retry loop completed unexpectedly")
        return result
