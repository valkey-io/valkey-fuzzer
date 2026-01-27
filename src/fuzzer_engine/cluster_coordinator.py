"""
Cluster Coordinator - Manages cluster lifecycle and interfaces with Cluster Orchestrator
"""
import logging
from typing import List, Optional
from ..models import ClusterConfig, ClusterInstance, ClusterStatus, NodeInfo
from ..cluster_orchestrator.orchestrator import ConfigurationManager, ClusterManager, PortManager

logger = logging.getLogger()


class ClusterCoordinator:
    """
    Coordinates cluster lifecycle management by interfacing with Cluster Orchestrator.
    Provides high-level cluster operations for the Fuzzer Engine.
    """
    
    def __init__(self):
        self.active_clusters: dict[str, ClusterInstance] = {}
        self.cluster_manager = ClusterManager()
    
    def create_cluster(self, config: ClusterConfig, seed: Optional[int] = None) -> ClusterInstance:
        """Create a new Valkey cluster with the specified configuration."""
        logger.info(f"Creating cluster with {config.num_shards} shards, {config.replicas_per_shard} replicas per shard")
        
        config_manager = None
        nodes = []
        
        try:
            # Create a fresh PortManager for this cluster using config's base_port
            port_manager = PortManager(base_port=config.base_port)
            
            # Initialize configuration manager with seed for log file naming
            config_manager = ConfigurationManager(config, port_manager, seed=seed)
            
            # Setup Valkey binary
            valkey_binary = config_manager.setup_valkey_from_source()
            config.valkey_binary = valkey_binary
            logger.debug(f"Using Valkey binary: {valkey_binary}")
            
            # Plan topology
            node_plans = config_manager.plan_topology()
            
            # Spawn all nodes
            nodes = config_manager.spawn_all_nodes(node_plans)
            
            # Form cluster
            cluster_connection = self.cluster_manager.form_cluster(nodes, config_manager.cluster_id)
            
            if not cluster_connection:
                raise Exception("Failed to form cluster")
            
            # Create cluster instance
            cluster_instance = ClusterInstance(
                cluster_id=cluster_connection.cluster_id,
                config=config,
                nodes=nodes,
                is_ready=True
            )
            
            # Store cluster instance and config manager for later cleanup
            self.active_clusters[cluster_instance.cluster_id] = {
                'instance': cluster_instance,
                'config_manager': config_manager
            }
            
            return cluster_instance
            
        except Exception as e:
            logger.error(f"Failed to create cluster: {e}")
            
            # Clean up spawned nodes and release ports on failure
            if nodes and config_manager:
                logger.info("Cleaning up spawned nodes after cluster formation failure")
                try:
                    config_manager.cleanup_cluster(nodes)
                except Exception as cleanup_error:
                    logger.error(f"Error during cleanup: {cleanup_error}")
            
            raise
    
    def get_cluster_status(self, cluster_id: str) -> Optional[ClusterStatus]:
        """Get current status of a cluster."""
        if cluster_id not in self.active_clusters:
            logger.warning(f"Cluster {cluster_id} not found")
            return None
        
        cluster_data = self.active_clusters[cluster_id]
        cluster_instance = cluster_data['instance']
        
        try:
            # Filter out dead nodes before validation
            # After chaos, some nodes may be dead - only validate live nodes
            live_nodes = []
            for node in cluster_instance.nodes:
                if node.process and node.process.poll() is None:
                    # Process is still running
                    live_nodes.append(node)
            
            # If no live nodes, use all nodes (cluster might have restarted nodes)
            nodes_to_validate = live_nodes if live_nodes else cluster_instance.nodes
            
            # Validate cluster health with live nodes only
            is_healthy = self.cluster_manager.validate_cluster(nodes_to_validate)
            
            # Count assigned slots from live primary nodes only
            total_slots = sum(
                (node.slot_end - node.slot_start + 1) 
                for node in nodes_to_validate 
                if node.role == 'primary' and node.slot_start is not None
            )
            
            status = ClusterStatus(
                cluster_id=cluster_id,
                nodes=cluster_instance.nodes,
                total_slots_assigned=total_slots,
                is_healthy=is_healthy,
                formation_complete=cluster_instance.is_ready
            )
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get cluster status for {cluster_id}: {e}")
            return None
    
    def validate_cluster_readiness(self, cluster_id: str) -> bool:
        """Validate that cluster is ready for testing."""
        status = self.get_cluster_status(cluster_id)
        
        if not status:
            return False
        
        # Check all required conditions for readiness
        is_ready = (
            status.formation_complete and
            status.is_healthy and
            status.total_slots_assigned == 16384 and
            len(status.nodes) > 0
        )
        
        if is_ready:
            logger.info(f"Cluster {cluster_id} is ready for testing")
        else:
            logger.warning(f"Cluster {cluster_id} is not ready: "
                         f"formation={status.formation_complete}, "
                         f"healthy={status.is_healthy}, "
                         f"slots={status.total_slots_assigned}")
        
        return is_ready
    
    def get_node_info(self, cluster_id: str, node_id: str) -> Optional[NodeInfo]:
        """Get information about a specific node in the cluster."""
        if cluster_id not in self.active_clusters:
            return None
        
        cluster_instance = self.active_clusters[cluster_id]['instance']
        
        for node in cluster_instance.nodes:
            if node.node_id == node_id:
                return node
        
        return None
    
    def get_all_nodes(self, cluster_id: str) -> List[NodeInfo]:
        """Get all nodes in the cluster."""
        if cluster_id not in self.active_clusters:
            return []
        
        cluster_instance = self.active_clusters[cluster_id]['instance']
        return cluster_instance.nodes
    
    def restart_node(self, cluster_id: str, node_id: str, wait_ready: bool = True, chaos_coordinator=None) -> bool:
        """Restart a specific node in the cluster. Optional ChaosCoordinator to update node registration after restart"""
        if cluster_id not in self.active_clusters:
            logger.error(f"Cluster {cluster_id} not found")
            return False
        
        cluster_data = self.active_clusters[cluster_id]
        config_manager = cluster_data['config_manager']
        
        node = self.get_node_info(cluster_id, node_id)
        if not node:
            logger.error(f"Node {node_id} not found in cluster {cluster_id}")
            return False
        
        try:
            logger.info(f"Restarting node {node_id} in cluster {cluster_id}")
            
            # Restart the node (this updates node.pid with the new process ID)
            config_manager.restart_node(node, wait_ready=wait_ready)
            
            logger.info(f"Node {node_id} restarted successfully with new PID {node.pid}")
            
            # Update chaos engine registration with new PID if chaos_coordinator provided
            if chaos_coordinator:
                chaos_coordinator.update_node_registration(node)
                logger.info(f"Updated chaos registration for restarted node {node_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to restart node {node_id}: {e}")
            return False
    
    def destroy_cluster(self, cluster_id: str) -> bool:
        """Destroy a cluster and clean up all resources."""
        if cluster_id not in self.active_clusters:
            logger.warning(f"Cluster {cluster_id} not found")
            return False
        
        try:
            cluster_data = self.active_clusters[cluster_id]
            cluster_instance = cluster_data['instance']
            config_manager = cluster_data['config_manager']
                        
            self.cluster_manager.close_connections()
            config_manager.cleanup_cluster(cluster_instance.nodes)
            del self.active_clusters[cluster_id]
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to destroy cluster {cluster_id}: {e}")
            return False
