"""
Operation Orchestrator - Executes cluster operations with timing and state management
"""
import time
import logging
import threading
from typing import Dict, Optional
from ..models import Operation, OperationType, ClusterStatus, ClusterConnection
from ..interfaces import IOperationOrchestrator
from ..cluster_orchestrator.orchestrator import ClusterManager
from ..utils.valkey_utils import valkey_client, query_cluster_nodes
from ..utils.cluster_utils import find_primary_node_by_identifier


class OperationOrchestrator(IOperationOrchestrator):
    """Orchestrates execution of cluster operations"""
    
    def __init__(self, cluster_connection: Optional[ClusterConnection] = None):
        """Initialize operation orchestrator"""
        self.cluster_manager = ClusterManager()
        self.cluster_connection = cluster_connection
        self.active_operations: Dict[str, Operation] = {}
        self.operation_counter = 0
        self._state_lock = threading.Lock()  # Protect shared state in parallel execution
    
    def set_cluster_connection(self, cluster_connection: ClusterConnection):
        """Set or update cluster connection"""
        self.cluster_connection = cluster_connection
    
    def execute_operation(self, operation: Operation, log_buffer=None) -> bool:
        """Execute a single cluster operation"""
        log = log_buffer if log_buffer else logging
        
        if not self.cluster_connection:
            log.error("No cluster connection available")
            return False
        
        # Generate operation ID and store in active operations dict (thread-safe)
        with self._state_lock:
            self.operation_counter += 1
            operation_id = f"op-{self.operation_counter}"
            self.active_operations[operation_id] = operation
        
        try:
            # Wait before operation if specified
            if operation.timing.delay_before > 0:
                log.info(f"Waiting {operation.timing.delay_before:.2f}s before operation")
                time.sleep(operation.timing.delay_before)
            
            # Execute based on operation type
            success = False
            if operation.type == OperationType.FAILOVER:
                success = self._execute_failover(operation, log)
            else:
                log.error(f"Unsupported operation type: {operation.type}")
                return False
            
            # Wait after operation if specified
            if operation.timing.delay_after > 0:
                log.info(f"Waiting {operation.timing.delay_after:.2f}s after operation")
                time.sleep(operation.timing.delay_after)
            
            # Remove from active operations (thread-safe)
            with self._state_lock:
                if operation_id in self.active_operations:
                    del self.active_operations[operation_id]
            
            return success
            
        except Exception as e:
            log.error(f"Operation execution failed: {e}")
            with self._state_lock:
                if operation_id in self.active_operations:
                    del self.active_operations[operation_id]
            return False
    
    def _execute_failover(self, operation: Operation, log=None) -> bool:
        """Execute failover operation"""
        if log is None:
            log = logging
        
        log.info(f"Executing failover on {operation.target_node}")
        
        # Get all cluster nodes (including dead ones) to find target primary
        # Gets actual Node information from Valkey
        current_nodes = self.cluster_connection.get_current_nodes()
        
        # Find target primary node
        target_node = find_primary_node_by_identifier(current_nodes, operation.target_node)
        
        if not target_node:
            log.error(f"Target primary node {operation.target_node} not found in cluster")
            return False
        
        # Get replicas of this primary to execute failover
        # Use cluster_connection to find replicas from any live node (resilient to dead primary)
        target_node_id = target_node['node_id']
        target_shard_id = target_node.get('shard_id')
        
        log.info(f"Finding replicas for primary {operation.target_node} (node_id: {target_node_id})")
        
        try:
            # Get fresh cluster topology from any live node
            current_nodes = self.cluster_connection.get_current_nodes()
            
            if not current_nodes:
                log.error("Cannot get current cluster nodes - all nodes may be down")
                return False
            
            # Find replicas of the target primary by shard_id or by querying a live node
            replica_nodes = []
            
            # Strategy 1: Find replicas by shard_id (if available)
            if target_shard_id is not None:
                for node in current_nodes:
                    if node.get('role') == 'replica' and node.get('shard_id') == target_shard_id:
                        replica_nodes.append({
                            'host': node['host'],
                            'port': node['port'],
                            'node_id': node['node_id']
                        })
                        log.info(f"Found replica by shard_id: {node['node_id']} at port {node['port']}")
            
            # Strategy 2: Query a live node for cluster topology
            # Try the target primary first for determinism, then fall back to other nodes
            if not replica_nodes:
                log.info("Querying live nodes for replica information")
                
                # Build query order: target primary first, then other nodes
                nodes_to_query = []
                
                # Add target primary first (if it's in current_nodes)
                for node in current_nodes:
                    if node.get('node_id') == target_node_id or node.get('port') == target_node.get('port'):
                        nodes_to_query.append(node)
                        break
                
                # Add remaining nodes as fallback
                for node in current_nodes:
                    if node not in nodes_to_query:
                        nodes_to_query.append(node)
                
                # Query nodes in priority order
                for node in nodes_to_query:
                    parsed_nodes = query_cluster_nodes(node, timeout=3.0)
                    
                    if parsed_nodes:
                        # Find replicas of our target primary
                        for parsed_node in parsed_nodes:
                            if parsed_node['is_slave'] and parsed_node['master_id'] == target_node_id:
                                replica_nodes.append({
                                    'host': parsed_node['host'],
                                    'port': parsed_node['port'],
                                    'node_id': parsed_node['node_id']
                                })
                                log.info(f"Found replica via CLUSTER NODES from {node['port']}: port {parsed_node['port']}")
                        
                        # If we found replicas, break out of the loop
                        if replica_nodes:
                            break
            
            if not replica_nodes:
                log.error(f"Cannot execute failover: No replicas found for primary {operation.target_node}. "
                             f"Failover requires at least one replica to promote.")
                return False
            
            # Find a random alive replica to execute failover
            replica = self.cluster_connection.find_alive_node(replica_nodes, randomize=True)
            
            if not replica:
                log.error(f"Cannot execute failover: No alive replicas found for primary {operation.target_node}")
                return False
            
            log.info(f"Selected alive replica at port {replica['port']} for failover")
            log.info(f"Executing CLUSTER FAILOVER from replica at port {replica['port']}")
            
            with valkey_client(replica['host'], replica['port'], timeout=5.0, decode_responses=True) as replica_client:
                # Execute CLUSTER FAILOVER command
                force = operation.parameters.get('force', False)
                if force:
                    replica_client.execute_command('CLUSTER', 'FAILOVER', 'FORCE')
                    log.info("Executed FORCE failover")
                else:
                    replica_client.execute_command('CLUSTER', 'FAILOVER')
                    log.info("Executed graceful failover")
            
            # Wait for failover to complete then validate cluster slots and replication links
            return self.wait_for_operation_completion(operation.timing.timeout, log)
            
        except Exception as e:
            log.error(f"Failover execution failed: {e}")
            return False

    def wait_for_operation_completion(self, timeout: float, log=None) -> bool:
        """Wait for operation to complete and validate cluster state"""
        if log is None:
            log = logging

        if not self.cluster_connection:
            return False
        
        log.info(f"Waiting for operation completion (timeout: {timeout:.2f}s)")
        start_time = time.time()
        deadline = start_time + timeout
        
        stabilization_wait = min(3.0, max(1.0, timeout * 0.5))
        time.sleep(stabilization_wait)

        # Validate replication links with remaining timeout
        max_retries = 3
        for attempt in range(max_retries):
            if time.time() >= deadline:
                log.warning(f"Operation timeout ({timeout:.2f}s) exceeded")
                return False
            
            live_nodes = [n for n in self.cluster_connection.initial_nodes if n.process is None or n.process.poll() is None]
            if self.cluster_manager.check_replication_links(live_nodes):
                elapsed = time.time() - start_time
                log.info(f"Operation completed successfully in {elapsed:.2f}s")
                return True
            
            if attempt < max_retries - 1:
                remaining_time = deadline - time.time()
                retry_delay = min(3.0, remaining_time)
                
                if retry_delay <= 0:
                    log.warning(f"Operation timeout ({timeout:.2f}s) exceeded during retries")
                    return False
                
                log.debug(f"Replication link check attempt {attempt + 1} failed, retrying in {retry_delay:.1f}s")
                time.sleep(retry_delay)

        log.warning("Replication link check failed after all retries")
        return False
    