"""Base classes for Chaos Engine components"""
import os
import signal
import time
import uuid
import random
import logging
from abc import ABC
from typing import Dict, List, Optional
from ..interfaces import IChaosEngine
from ..models import NodeInfo, ChaosResult, ChaosType, ProcessChaosType, Operation, TargetSelection


logger = logging.getLogger()

class BaseChaosEngine(IChaosEngine, ABC):
    """Base implementation for chaos injection with common functionality"""
    
    def __init__(self):
        self.active_chaos: Dict[str, ChaosResult] = {}
        self.chaos_history: List[ChaosResult] = []
        self.node_processes: Dict[str, int] = {}  # node_id -> process_id mapping
    
    def inject_process_chaos(self, target_node: NodeInfo, chaos_type: ProcessChaosType, log_buffer=None) -> ChaosResult:
        """Inject process-level chaos on target node"""
        log = log_buffer if log_buffer else logger
        chaos_id = str(uuid.uuid4())
        start_time = time.time()
        
        chaos_result = ChaosResult(
            chaos_id=chaos_id,
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=target_node.node_id,
            success=False,
            start_time=start_time
        )
        
        try:
            # Validate target node
            if not self._validate_chaos_target(target_node, log_buffer):
                chaos_result.error_message = f"Invalid chaos target: {target_node.node_id}"
                self.chaos_history.append(chaos_result)
                return chaos_result
            
            # Get process ID for the target node
            process_id = self._get_node_process_id(target_node)
            if not process_id:
                chaos_result.error_message = f"Could not find process for {target_node.node_id}"
                self.chaos_history.append(chaos_result)
                return chaos_result
            
            # Execute process chaos
            success = self._execute_process_kill(process_id, chaos_type, log_buffer)
            
            chaos_result.success = success
            chaos_result.end_time = time.time()
            
            if success:
                log.info(f"Successfully injected {chaos_type.value} chaos on {target_node.node_id} (PID: {process_id})")
                self.active_chaos[chaos_id] = chaos_result
            else:
                chaos_result.error_message = f"Failed to kill process {process_id} with {chaos_type.value}"
                log.error(chaos_result.error_message)
            
        except Exception as e:
            chaos_result.error_message = f"Exception during chaos injection: {str(e)}"
            chaos_result.end_time = time.time()
            log.error(f"Chaos injection failed: {e}")
        
        self.chaos_history.append(chaos_result)
        return chaos_result
    
    def stop_chaos(self, chaos_id: str) -> bool:
        """Stop active chaos injection"""
        if chaos_id not in self.active_chaos:
            logger.warning(f"Chaos {chaos_id} not found in active chaos")
            return False
        
        chaos_result = self.active_chaos[chaos_id]
        chaos_result.end_time = time.time()
        
        # For process chaos, there's nothing to actively stop since the process is already killed
        # Future chaos types (like network chaos) might need active cleanup
        
        del self.active_chaos[chaos_id]
        logger.debug(f"Stopped chaos with chaos ID: {chaos_id}")
        return True
    
    def cleanup_chaos(self, cluster_id: str) -> bool:
        """Clean up any remaining chaos effects"""
        try:
            # Stop all active chaos
            active_chaos_ids = list(self.active_chaos.keys())
            for chaos_id in active_chaos_ids:
                self.stop_chaos(chaos_id)
            
            # Clear process tracking for this cluster
            self.node_processes.clear()
            
            logger.info(f"Cleaned up chaos effects for cluster {cluster_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cleanup chaos for cluster {cluster_id}: {e}")
            return False
    
    def _validate_chaos_target(self, target_node: NodeInfo, log_buffer=None) -> bool:
        """Validate that chaos can be injected on target node"""
        log = log_buffer if log_buffer else logger
        
        if not target_node:
            return False
        
        # Check if node has a valid process ID
        if target_node.node_id not in self.node_processes:
            log.warning(f"No process ID found for node {target_node.node_id}")
            return False
        
        return True
    
    def _get_node_process_id(self, target_node: NodeInfo) -> Optional[int]:
        """Get the process ID for a target node"""
        return self.node_processes.get(target_node.node_id)
    
    def _execute_process_kill(self, process_id: int, chaos_type: ProcessChaosType, log_buffer=None) -> bool:
        """Execute process termination"""
        log = log_buffer if log_buffer else logger
        
        try:
            if chaos_type == ProcessChaosType.SIGKILL:
                os.kill(process_id, signal.SIGKILL)
            elif chaos_type == ProcessChaosType.SIGTERM:
                os.kill(process_id, signal.SIGTERM)
            else:
                log.error(f"Unsupported process chaos type: {chaos_type}")
                return False
            
            return True
        except ProcessLookupError:
            log.debug(f"Process {process_id} already dead (chaos goal achieved)")
            return True
        except PermissionError:
            log.error(f"Permission denied when trying to kill process {process_id}")
            return False
        except Exception as e:
            log.error(f"Failed to kill process {process_id}: {e}")
            return False
    
    def _select_chaos_target(self, operation: Operation, target_selection: TargetSelection) -> Optional[NodeInfo]:
        """Select target node for chaos injection based on cluster topology"""
        # This is a placeholder implementation
        # In a real implementation, this would query the cluster orchestrator
        # for current cluster topology and select appropriate targets
        
        if target_selection.strategy == "specific" and target_selection.specific_nodes:
            # For specific node selection, we'd need cluster state
            # This is a simplified implementation
            return None
        
        # For now, return None to indicate no target selected
        # This will be properly implemented when cluster orchestrator is available
        return None
    
    def register_node_process(self, node_id: str, process_id: int) -> None:
        """Register a process ID for a node (for testing purposes)"""
        self.node_processes[node_id] = process_id
        logger.debug(f"Registered process {process_id} for node {node_id}")
    
    def unregister_node_process(self, node_id: str) -> None:
        """Unregister a process ID for a node"""
        if node_id in self.node_processes:
            del self.node_processes[node_id]
            logger.debug(f"Unregistered process for node {node_id}")


class ProcessChaosEngine(BaseChaosEngine):
    """Concrete implementation of process chaos injection"""
    
    def __init__(self, rng: Optional[random.Random] = None):
        super().__init__()
        self.target_selector = ChaosTargetSelector(rng)
    
    def _select_chaos_target(self, cluster_id: str, target_selection: TargetSelection) -> Optional[NodeInfo]:
        """Select target node for chaos injection based on cluster topology"""
        return self.target_selector.select_target(cluster_id, target_selection)


class ChaosTargetSelector:
    """Utility class for selecting chaos targets based on cluster topology"""
    
    def __init__(self, rng: Optional[random.Random] = None):
        self.cluster_nodes: Dict[str, List[NodeInfo]] = {}
        self.rng = rng if rng is not None else random.Random()
    
    def update_cluster_topology(self, cluster_id: str, nodes: List[NodeInfo]) -> None:
        """Update cluster topology information"""
        self.cluster_nodes[cluster_id] = nodes
        logger.debug(f"Updated topology for cluster {cluster_id} with {len(nodes)} nodes")
    
    def select_target(self, cluster_id: str, target_selection: TargetSelection, log_buffer=None) -> Optional[NodeInfo]:
        """Select target node based on selection strategy"""
        log = log_buffer if log_buffer else logger

        # Get nodes for this cluster
        if cluster_id not in self.cluster_nodes:
            log.warning(f"No topology information for cluster {cluster_id}")
            return None
        
        nodes = self.cluster_nodes[cluster_id]
        if not nodes:
            log.warning(f"No nodes available in cluster {cluster_id}")
            return None
        
        # Sort nodes by node_id for deterministic ordering
        nodes = sorted(nodes, key=lambda n: n.node_id)
        
        strategy = target_selection.strategy
        
        if target_selection.strategy == "specific" and target_selection.specific_nodes:
            # Find all matching nodes from the specific list
            matching_nodes = []
            for node_id in target_selection.specific_nodes:
                for node in nodes:
                    if node.node_id == node_id:
                        matching_nodes.append(node)
                        break  # Found this node, move to next node_id
            
            if not matching_nodes:
                log.warning(f"None of the specified nodes found: {target_selection.specific_nodes}")
                return None
            
            # Randomly select from matching nodes (consistent with other strategies)
            selected = self.rng.choice(matching_nodes)
            log.info(f"Selected specific node: {selected.node_id} (from {len(matching_nodes)} specified)")
            return selected
        
        elif target_selection.strategy == "random":
            selected = self.rng.choice(nodes)
            log.info(f"Selected random node: shard {selected.shard_id} - {selected.node_id}, role: {selected.role}, port: {selected.port}")
            return selected
        
        elif target_selection.strategy == "primary_only":
            primaries = [n for n in nodes if n.role == 'primary']
            
            if not primaries:
                log.warning("No primary nodes available")
                return None
            
            # Randomly select from primaries
            selected = self.rng.choice(primaries)
            log.info(f"Selected random primary: shard {selected.shard_id} - {selected.node_id} ")
            return selected
        
        elif target_selection.strategy == "replica_only":
            replicas = [n for n in nodes if n.role == 'replica']
            
            if not replicas:
                log.warning("No replica nodes available")
                return None
            
            # Randomly select from replicas
            selected = self.rng.choice(replicas)
            log.info(f"Selected random replica: {selected.node_id} (shard {selected.shard_id})")
            return selected
        
        else:
            log.error(f"Unknown target selection strategy: {strategy}")
            return None
    