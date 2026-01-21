"""
Chaos Coordinator - Core chaos injection coordination for fuzzer engine
For scenario-based testing with state management, see chaos_engine.coordinator.
"""
import logging
import time
import random
from typing import Optional, List
from copy import deepcopy
from ..models import Operation, ChaosConfig, ChaosResult, NodeInfo, ChaosCoordination, ProcessChaosType, ChaosType, TargetSelection
from ..chaos_engine.base import ProcessChaosEngine

logger = logging.getLogger()


class ChaosCoordinator:
    def __init__(self, seed: Optional[int] = None):
        self.rng = random.Random(seed)  # Dedicated RNG for reproducible chaos selection
        self.chaos_engine = ProcessChaosEngine(self.rng)
        self.active_chaos_scenarios: dict[str, List[ChaosResult]] = {}
        self.chaos_history: List[ChaosResult] = []
        
        if seed is not None:
            logger.debug(f"ChaosCoordinator initialized with seed {seed} for deterministic target selection")

    def register_cluster_nodes(self, cluster_id: str, nodes: List[NodeInfo]) -> None:
        """Register cluster nodes with the chaos engine for chaos injection."""
        logger.info(f"Registering {len(nodes)} nodes for chaos injection in cluster {cluster_id}")
        
        for node in nodes:
            # Register node process with chaos engine
            self.chaos_engine.register_node_process(node.node_id, node.pid)
        
        # Update cluster topology for target selection so that it only contains live nodes
        self.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)
        
        logger.info(f"Successfully registered {len(nodes)} nodes for chaos injection")
    
    def update_node_registration(self, node: NodeInfo) -> None:
        """
        Update the chaos engine registration for a restarted node with its new PID.
        This should be called after a node restart to ensure chaos injections target the correct process.
        """
        logger.info(f"Updating chaos registration for {node.node_id} with new PID {node.pid}")
        self.chaos_engine.register_node_process(node.node_id, node.pid)
        logger.debug(f"Node {node.node_id} chaos registration updated")
    
    def coordinate_chaos_with_operation(
        self, 
        operation: Operation, 
        chaos_config: ChaosConfig,
        cluster_connection,
        cluster_id: str,
        randomize_per_operation: Optional[bool] = None,
        log_buffer=None
    ) -> List[ChaosResult]:
        """Coordinate chaos injection with a cluster operation based on timing configuration."""
        chaos_results = []
        log = log_buffer if log_buffer else logger
        
        log.info(f"Coordinating chaos with operation {operation.type.value} on {operation.target_node}")
        
        try:
            # Determine if randomization should be enabled
            should_randomize = randomize_per_operation if randomize_per_operation is not None else chaos_config.randomize_per_operation

            # Randomize chaos configuration for this operation if enabled
            if should_randomize:
                chaos_config = self._randomize_chaos_config(chaos_config, log)

            # Refresh topology from live cluster connection
            live_nodes_dict = cluster_connection.get_live_nodes()
            if live_nodes_dict:
                # Convert dict nodes to NodeInfo objects for the target selector
                live_nodes = self._convert_dict_nodes_to_nodeinfo(live_nodes_dict, cluster_connection.initial_nodes)
                self.chaos_engine.target_selector.update_cluster_topology(cluster_id, live_nodes)
                log.debug(f"Updated topology with {len(live_nodes)} live nodes from cluster")
            else:
                log.warning("No live nodes available from cluster connection")

            # Select target node for chaos using ChaosTargetSelector
            target_node = self.chaos_engine.target_selector.select_target(cluster_id, chaos_config.target_selection, log_buffer=log)
            
            if not target_node:
                log.warning("No suitable chaos target found")
                return chaos_results
            
            # Execute chaos based on coordination configuration
            coordination = chaos_config.coordination
            timing = chaos_config.timing
            
            # Add slight randomization to timing if enabled (±20%)
            if should_randomize:
                delay_before = timing.delay_before_operation * self.rng.uniform(0.8, 1.2)
                delay_after = timing.delay_after_operation * self.rng.uniform(0.8, 1.2)
            else:
                delay_before = timing.delay_before_operation
                delay_after = timing.delay_after_operation

            # Chaos before operation
            if coordination.chaos_before_operation:
                log.info(f"Injecting chaos before operation (delay: {delay_before:.2f}s)")
                time.sleep(delay_before)
                
                result = self._inject_chaos(target_node, chaos_config, should_randomize, log, cluster_connection)
                chaos_results.append(result)
                
                if result.success:
                    log.debug(f"Pre-operation chaos injected on {target_node.node_id}")
            
            # Chaos during operation (most common for failover testing)
            if coordination.chaos_during_operation:
                log.info("Injecting chaos during operation execution")
                
                result = self._inject_chaos(target_node, chaos_config, should_randomize, log, cluster_connection)
                chaos_results.append(result)
                
                if result.success:
                    log.debug(f"During-operation chaos injected on {target_node.node_id}")
            
            # Chaos after operation - return info for later injection
            if coordination.chaos_after_operation:
                log.info(f"Chaos scheduled after operation (delay: {delay_after:.2f}s)")
                # Don't inject now - return a placeholder that indicates chaos should happen after operation
                chaos_results.append({
                    'deferred': True,
                    'target_node': target_node,
                    'delay': delay_after,
                    'chaos_config': chaos_config,
                    'should_randomize': should_randomize,
                    'cluster_connection': cluster_connection
                })
            
            # Store only actual chaos results (not deferred placeholders) for this scenario
            actual_results = [r for r in chaos_results if isinstance(r, ChaosResult)]
            self.chaos_history.extend(actual_results)
            
        except Exception as e:
            logger.error(f"Failed to coordinate chaos with operation: {e}")
        
        return chaos_results
    
    def _convert_dict_nodes_to_nodeinfo(self, live_nodes_dict: List[dict], initial_nodes: List[NodeInfo]) -> List[NodeInfo]:
        """Convert dictionary node representations to NodeInfo objects with live roles."""
        node_info_list = []
        for node_dict in live_nodes_dict:
            # Find matching initial node to get full info
            # Note: node_dict['node_id'] contains the Valkey cluster ID (40-char hex),
            # which matches NodeInfo.cluster_node_id, not NodeInfo.node_id (logical name)
            matching_node = next(
                (n for n in initial_nodes if n.cluster_node_id == node_dict['node_id']),
                None
            )
            if matching_node:
                matching_node.role = node_dict.get('role', matching_node.role)
                node_info_list.append(matching_node)
            else:
                # Create a basic NodeInfo from the dict if no match found
                # This should rarely happen - it means a node exists in the cluster
                # but wasn't in our initial node list
                logger.warning(
                    f"No matching initial node found for cluster node {node_dict['node_id']} "
                    f"(port {node_dict.get('port')}). Creating fallback NodeInfo without process info."
                )
                node_info_list.append(NodeInfo(
                    node_id=node_dict['node_id'],
                    role=node_dict.get('role', 'unknown'),
                    shard_id=node_dict.get('shard_id', 0),
                    port=node_dict.get('port', 0),
                    bus_port=node_dict.get('bus_port', 0),
                    pid=node_dict.get('pid', 0),
                    process=None,
                    data_dir=f"/tmp/{node_dict['node_id']}",
                    log_file=f"/tmp/{node_dict['node_id']}.log",
                    cluster_node_id=node_dict['node_id']
                ))
        return node_info_list

    def _randomize_chaos_config(self, chaos_config: ChaosConfig, log=None) -> ChaosConfig:
        """Create a randomized copy of the chaos config for this operation."""

        if log is None:
            log = logger
            
        # Create a copy to avoid modifying the original
        randomized_config = deepcopy(chaos_config)

        # Randomize process chaos type (if process kill chaos)
        if randomized_config.chaos_type == ChaosType.PROCESS_KILL:
            # 50/50 chance between SIGKILL and SIGTERM
            randomized_config.process_chaos_type = self.rng.choice([
                ProcessChaosType.SIGKILL,
                ProcessChaosType.SIGTERM
            ])
            log.info(f"Randomized chaos type: {randomized_config.process_chaos_type.value}")

        # Randomize chaos timing coordination - select exactly ONE timing option
        timing_options = ['before', 'during', 'after']
        selected_timing = self.rng.choice(timing_options)

        randomized_config.coordination = ChaosCoordination(
            chaos_before_operation=(selected_timing == 'before'),
            chaos_during_operation=(selected_timing == 'during'),
            chaos_after_operation=(selected_timing == 'after')
        )

        log.info(f"Randomized chaos timing: {selected_timing}")

        # Randomize target selection strategy (if not specific nodes)
        if randomized_config.target_selection.strategy != "specific":
            # Choose randomly between different strategies
            strategies = ["random", "primary_only", "replica_only"]
            new_strategy = self.rng.choice(strategies)

            randomized_config.target_selection = TargetSelection(
                strategy=new_strategy,
                specific_nodes=None
            )
            log.info(f"Randomized target strategy: {new_strategy}")

        return randomized_config

    def _inject_chaos(self, target_node: NodeInfo, chaos_config: ChaosConfig, randomize: bool = False, log=None, cluster_connection=None) -> ChaosResult:
        """Inject chaos on the target node based on configuration."""

        if log is None:
            log = logger
        
        if chaos_config.chaos_type == ChaosType.PROCESS_KILL:
            # Refresh target node role from live cluster immediately before kill
            if cluster_connection:
                live_nodes_dict = cluster_connection.get_live_nodes()
                if live_nodes_dict:
                    for node_dict in live_nodes_dict:
                        if node_dict['node_id'] == target_node.cluster_node_id:
                            target_node.role = node_dict.get('role', target_node.role)
                            log.debug(f"Refreshed target role to {target_node.role} immediately before kill")
                            break
            
            # Use process chaos type from config (may have been randomized)
            if chaos_config.process_chaos_type:
                process_chaos_type = chaos_config.process_chaos_type
            else:
                # Fallback behavior depends on randomization flag
                if randomize:
                    # Randomize when explicitly requested
                    process_chaos_type = self.rng.choice([ProcessChaosType.SIGKILL, ProcessChaosType.SIGTERM])
                    log.info(f"Randomized fallback chaos type: {process_chaos_type.value}")
                else:
                    # Deterministic default for backward compatibility
                    process_chaos_type = ProcessChaosType.SIGKILL
                    log.debug(f"Using default chaos type: {process_chaos_type.value}")
            
            log.info(f"Injecting {process_chaos_type.value} on {target_node.node_id}")
            
            result = self.chaos_engine.inject_process_chaos(target_node, process_chaos_type, log_buffer=log)
            
            # Capture role at time of kill for topology validation
            result.target_role = target_node.role

            return result
        else:
            # Future chaos types (network chaos, etc.)
            logger.warning(f"Unsupported chaos type: {chaos_config.chaos_type}")
            return ChaosResult(
                chaos_id="unsupported",
                chaos_type=chaos_config.chaos_type,
                target_node=target_node.node_id,
                success=False,
                start_time=time.time(),
                end_time=time.time(),
                error_message=f"Unsupported chaos type: {chaos_config.chaos_type}"
            )

    def _select_chaos_target(self, cluster_nodes: List[NodeInfo], target_selection: TargetSelection) -> Optional[NodeInfo]:
        """Select a target node for chaos injection based on selection strategy."""
        
        if not cluster_nodes:
            return None

        strategy = target_selection.strategy
        
        if strategy == "specific":
            # Select from specific nodes
            if target_selection.specific_nodes:
                for node in cluster_nodes:
                    if node.node_id in target_selection.specific_nodes:
                        return node
            return None
        
        elif strategy == "primary_only":
            # Select only from primary nodes
            primary_nodes = [node for node in cluster_nodes if node.role == 'primary']
            if primary_nodes:
                return self.rng.choice(primary_nodes)
            return None

        elif strategy == "replica_only":
            # Select only from replica nodes
            replica_nodes = [node for node in cluster_nodes if node.role == 'replica']
            if replica_nodes:
                return self.rng.choice(replica_nodes)
            return None

        elif strategy == "random":
            # Select randomly from all nodes
            return self.rng.choice(cluster_nodes)
        
        else:
            logger.warning(f"Unknown target selection strategy: {strategy}")
            return None
    
    def get_chaos_history(self) -> List[ChaosResult]:
        """Get the history of all chaos injections."""
        return self.chaos_history.copy()
    
    def cleanup_chaos(self, cluster_id: str) -> bool:
        """Clean up chaos effects for a cluster."""
        logger.debug(f"Cleaning up chaos for cluster {cluster_id}")
        
        try:
            success = self.chaos_engine.cleanup_chaos(cluster_id)
            
            if cluster_id in self.active_chaos_scenarios:
                del self.active_chaos_scenarios[cluster_id]
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to cleanup chaos for cluster {cluster_id}: {e}")
            return False
    
    def get_active_chaos_count(self) -> int:
        """Get the number of active chaos injections."""
        return len(self.chaos_engine.active_chaos)
    
    def stop_all_chaos(self) -> None:
        """Stop all active chaos injections."""
        active_chaos_ids = list(self.chaos_engine.active_chaos.keys())
        
        for chaos_id in active_chaos_ids:
            self.chaos_engine.stop_chaos(chaos_id)
        
        logger.info(f"Stopped {len(active_chaos_ids)} active chaos injections")
