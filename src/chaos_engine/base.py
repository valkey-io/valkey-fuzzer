"""Base classes for Chaos Engine components"""
import os
import signal
import time
import uuid
import random
import logging
import threading
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
    """Utility class for selecting chaos targets based on cluster topology.

    Thread-safety: ``select_target`` eagerly records the selected node as
    killed (under ``_lock``) before returning, so concurrent threads in the
    same wave cannot both pick the last surviving member of a shard.  If the
    actual kill later fails, the caller must invoke ``unrecord_kill`` to
    release the reservation.
    """
    
    def __init__(self, rng: Optional[random.Random] = None):
        self.cluster_nodes: Dict[str, List[NodeInfo]] = {}
        self.rng = rng if rng is not None else random.Random()
        self._lock = threading.Lock()
        # Track killed (or reserved-to-kill) node_ids per cluster
        self._killed_node_ids: Dict[str, set] = {}
        # Initial full topology per cluster for shard membership lookup
        self._initial_topology: Dict[str, List[NodeInfo]] = {}

    def reset_cluster_topology(self, cluster_id: str, nodes: List[NodeInfo]) -> None:
        """Replace all selector state for a cluster with a fresh topology snapshot."""
        with self._lock:
            self.cluster_nodes[cluster_id] = list(nodes)
            self._initial_topology[cluster_id] = list(nodes)
            self._killed_node_ids[cluster_id] = set()
        logger.debug(f"Reset selector state for cluster {cluster_id} with {len(nodes)} nodes")

    def clear_cluster_state(self, cluster_id: str) -> None:
        """Drop any cached selector state for a cluster."""
        with self._lock:
            self.cluster_nodes.pop(cluster_id, None)
            self._initial_topology.pop(cluster_id, None)
            self._killed_node_ids.pop(cluster_id, None)
        logger.debug(f"Cleared selector state for cluster {cluster_id}")
    
    def update_cluster_topology(self, cluster_id: str, nodes: List[NodeInfo]) -> None:
        """Update cluster topology information.

        Does NOT reconcile ``_killed_node_ids`` against the live topology
        because ``select_target`` eagerly reserves nodes before the actual
        kill.  A reserved-but-not-yet-killed node would still appear in the
        live list, and clearing it here would re-introduce the TOCTOU race
        this class is designed to prevent.  Failed kills are handled by
        ``unrecord_kill`` instead.
        """
        with self._lock:
            self.cluster_nodes[cluster_id] = nodes
            if cluster_id not in self._initial_topology:
                # First registration — snapshot the full topology
                self._initial_topology[cluster_id] = list(nodes)
                self._killed_node_ids[cluster_id] = set()
        logger.debug(f"Updated topology for cluster {cluster_id} with {len(nodes)} nodes")

    def record_kill(self, cluster_id: str, node_id: str) -> None:
        """Record that a node was killed by chaos in a specific cluster."""
        with self._lock:
            self._killed_node_ids.setdefault(cluster_id, set()).add(node_id)

    def record_recovery(self, cluster_id: str, node_id: str) -> None:
        """Clear killed state after a node is explicitly restarted/re-registered."""
        with self._lock:
            killed = self._killed_node_ids.get(cluster_id)
            if killed:
                killed.discard(node_id)

    def unrecord_kill(self, cluster_id: str, node_id: str) -> None:
        """Remove a kill reservation (e.g. when the actual kill failed)."""
        with self._lock:
            killed = self._killed_node_ids.get(cluster_id)
            if killed:
                killed.discard(node_id)

    def _get_shard_safe_candidates(self, candidates: List[NodeInfo], cluster_id: str, log=None) -> List[NodeInfo]:
        """Filter candidates to avoid killing the last surviving member of any shard.

        Also excludes nodes already reserved/killed (they may still appear in
        the candidate list due to topology refresh lag in concurrent scenarios).

        Caller must hold ``_lock``.
        """
        initial_nodes = self._initial_topology.get(cluster_id, [])
        if not initial_nodes:
            return candidates  # No topology info — can't filter

        killed = self._killed_node_ids.get(cluster_id, set())
        live_node_ids = {
            node.node_id for node in self.cluster_nodes.get(cluster_id, [])
        }

        # Build shard -> set of initial node_ids
        shard_members: Dict[int, set] = {}
        for node in initial_nodes:
            shard_members.setdefault(node.shard_id, set()).add(node.node_id)

        safe = []
        for candidate in candidates:
            # Skip nodes already reserved/killed
            if candidate.node_id in killed:
                continue

            members = shard_members.get(candidate.shard_id, set())
            live_members = members & live_node_ids
            surviving_others = live_members - killed - {candidate.node_id}
            if surviving_others:
                safe.append(candidate)
            elif log is not None:
                log.info(
                    f"Skipping {candidate.node_id} (shard {candidate.shard_id}) — "
                    f"killing it would leave zero live members in the shard"
                )

        return safe

    def is_shard_safety_exhausted(self, cluster_id: str, target_selection: TargetSelection) -> bool:
        """Return True only when shard safety is the reason no target can be selected."""
        with self._lock:
            nodes = self.cluster_nodes.get(cluster_id, [])
            if not nodes:
                return False

            strategy = target_selection.strategy
            if strategy == "random":
                candidates = nodes
            elif strategy == "primary_only":
                candidates = [n for n in nodes if n.role == 'primary']
            elif strategy == "replica_only":
                candidates = [n for n in nodes if n.role == 'replica']
            else:
                return False

            if not candidates:
                return False

            safe_candidates = self._get_shard_safe_candidates(candidates, cluster_id)
            return len(safe_candidates) == 0

    def select_target(self, cluster_id: str, target_selection: TargetSelection, log_buffer=None) -> Optional[NodeInfo]:
        """Select a chaos target and eagerly reserve it as killed.

        The reservation prevents concurrent threads from selecting the last
        member of the same shard.  If the caller later fails to actually kill
        the node, it must call ``unrecord_kill`` to release the reservation.
        """
        log = log_buffer if log_buffer else logger

        with self._lock:
            if cluster_id not in self.cluster_nodes:
                log.warning(f"No topology information for cluster {cluster_id}")
                return None
            
            nodes = self.cluster_nodes[cluster_id]
            if not nodes:
                log.warning(f"No nodes available in cluster {cluster_id}")
                return None
            
            # Sort nodes by node_id for deterministic ordering
            nodes = sorted(nodes, key=lambda n: n.node_id)
            
            selected = self._select_by_strategy(nodes, cluster_id, target_selection, log)

            # Eagerly reserve the selected node so concurrent threads see it
            if selected:
                self._killed_node_ids.setdefault(cluster_id, set()).add(selected.node_id)

        return selected

    def _select_by_strategy(self, nodes: List[NodeInfo], cluster_id: str,
                            target_selection: TargetSelection, log) -> Optional[NodeInfo]:
        """Pick a node according to the strategy.  Caller must hold ``_lock``."""
        strategy = target_selection.strategy

        if strategy == "specific" and target_selection.specific_nodes:
            matching_nodes = []
            for node_id in target_selection.specific_nodes:
                for node in nodes:
                    if node.node_id == node_id:
                        matching_nodes.append(node)
                        break
            if not matching_nodes:
                log.warning(f"None of the specified nodes found: {target_selection.specific_nodes}")
                return None
            selected = self.rng.choice(matching_nodes)
            log.info(f"Selected specific node: {selected.node_id} (from {len(matching_nodes)} specified)")
            return selected

        elif strategy == "random":
            safe_nodes = self._get_shard_safe_candidates(nodes, cluster_id, log)
            if not safe_nodes:
                log.warning("No shard-safe random targets available")
                return None
            selected = self.rng.choice(safe_nodes)
            log.info(f"Selected random node: {selected.node_id} (shard {selected.shard_id})")
            return selected

        elif strategy == "primary_only":
            primaries = [n for n in nodes if n.role == 'primary']
            if not primaries:
                log.warning("No primary nodes available")
                return None
            safe_primaries = self._get_shard_safe_candidates(primaries, cluster_id, log)
            if not safe_primaries:
                log.warning("No shard-safe primary targets available")
                return None
            selected = self.rng.choice(safe_primaries)
            log.info(f"Selected random primary: {selected.node_id} (shard {selected.shard_id})")
            return selected

        elif strategy == "replica_only":
            replicas = [n for n in nodes if n.role == 'replica']
            if not replicas:
                log.warning("No replica nodes available")
                return None
            safe_replicas = self._get_shard_safe_candidates(replicas, cluster_id, log)
            if not safe_replicas:
                log.warning("No shard-safe replica targets available")
                return None
            selected = self.rng.choice(safe_replicas)
            log.info(f"Selected random replica: {selected.node_id} (shard {selected.shard_id})")
            return selected

        else:
            log.error(f"Unknown target selection strategy: {strategy}")
            return None
