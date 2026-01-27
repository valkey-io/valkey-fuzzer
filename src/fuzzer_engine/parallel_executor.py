"""
Parallel Executor - Executes multiple operations concurrently with buffered logging
"""
import logging
import time
from typing import List, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..models import Operation, ChaosConfig, ChaosResult
from .operation_log_buffer import OperationLogBuffer

logger = logging.getLogger()

class ParallelExecutor:
    """Executes operations in parallel by shard with per-operation log buffering"""
    
    def __init__(self, operation_orchestrator, chaos_coordinator, fuzzer_logger):
        self.operation_orchestrator = operation_orchestrator
        self.chaos_coordinator = chaos_coordinator
        self.fuzzer_logger = fuzzer_logger
    
    def execute_operations_parallel(self, operations: List[Operation], chaos_config: ChaosConfig, cluster_connection, cluster_id: str) -> Tuple[int, List[ChaosResult], List]:
        """Execute operations in parallel by shard with buffered logging"""
        
        logger.info(f"Starting parallel execution of {len(operations)} operations")
        
        # Group operations by shard
        shard_groups = defaultdict(list)
        for i, op in enumerate(operations):
            shard_id = op.target_node.rsplit('-', 1)[0] if '-' in op.target_node else op.target_node
            shard_groups[shard_id].append((i, op))
        
        logger.info(f"Grouped {len(operations)} operations into {len(shard_groups)} shard groups")
        
        operations_executed = 0
        all_chaos_events = []
        results = [None] * len(operations)
        
        def _execute_shard_operations(shard_ops: List[Tuple[int, Operation]]):
            """Execute all operations for a shard sequentially"""
            shard_results = []
            for op_index, operation in shard_ops:
                op_id = f"{op_index + 1}: {operation.type.value} on {operation.target_node}"
                buffer = OperationLogBuffer(op_id)
                
                try:
                    buffer.info(f"Starting {operation.type.value}")
                    
                    # Coordinate chaos (may return deferred chaos for after-operation)
                    chaos_results = self.chaos_coordinator.coordinate_chaos_with_operation(
                        operation,
                        chaos_config,
                        cluster_connection,
                        cluster_id,
                        log_buffer=buffer
                    )
                    
                    # Separate immediate and deferred chaos
                    immediate_chaos = [c for c in chaos_results if not isinstance(c, dict) or not c.get('deferred')]
                    deferred_chaos = [c for c in chaos_results if isinstance(c, dict) and c.get('deferred')]
                    
                    # Execute operation with buffered logging
                    success = self.operation_orchestrator.execute_operation(operation, log_buffer=buffer)
                    
                    # Inject deferred chaos after operation completes
                    for deferred in deferred_chaos:
                        buffer.info(f"Injecting deferred chaos after operation (delay: {deferred['delay']:.2f}s)")
                        time.sleep(deferred['delay'])
                        
                        target_node = deferred['target_node']
                        live_nodes_dict = cluster_connection.get_live_nodes()
                        if live_nodes_dict:
                            for node_dict in live_nodes_dict:
                                if node_dict['node_id'] == target_node.cluster_node_id:
                                    target_node.role = node_dict.get('role', target_node.role)
                                    buffer.debug(f"Refreshed target node role to: {target_node.role}")
                                    break
                        
                        result = self.chaos_coordinator._inject_chaos(
                            target_node,
                            deferred['chaos_config'],
                            deferred['should_randomize'],
                            buffer,
                            cluster_connection
                        )
                        immediate_chaos.append(result)
                        
                        if isinstance(result, ChaosResult):
                            self.chaos_coordinator.chaos_history.append(result)
                    
                    chaos_events = immediate_chaos
                    
                    # Log operation result
                    self.fuzzer_logger.log_operation(
                        operation,
                        success,
                        f"Operation {'succeeded' if success else 'failed'}",
                        silent=False
                    )
                    
                    # Log chaos events associated with this operation
                    for chaos_event in chaos_events:
                        if isinstance(chaos_event, ChaosResult):
                            self.fuzzer_logger.log_chaos_event(chaos_event)
                    
                    buffer.info(f"Operation {'succeeded' if success else 'failed'}")
                    
                    shard_results.append((op_index, 1 if success else 0, chaos_events, buffer))
                    
                except Exception as e:
                    buffer.error(f"Operation failed with exception: {e}")
                    shard_results.append((op_index, 0, [], buffer))
            
            return shard_results
        
        # Execute shard groups in parallel
        with ThreadPoolExecutor(max_workers=len(shard_groups)) as executor:
            futures = {
                executor.submit(_execute_shard_operations, shard_ops): shard_id
                for shard_id, shard_ops in shard_groups.items()
            }
            
            # Collect results from all shards
            for future in as_completed(futures):
                shard_id = futures[future]
                try:
                    shard_results = future.result()
                    for op_index, executed, chaos_events, buffer in shard_results:
                        results[op_index] = (executed, chaos_events, buffer)
                except Exception as e:
                    logger.error(f"Shard {shard_id} execution failed: {e}")
                    # Mark all operations in this shard as failed
                    shard_ops = shard_groups[shard_id]
                    for op_index, operation in shard_ops:
                        error_buffer = OperationLogBuffer(f"{op_index + 1}: {operation.type.value} on {operation.target_node}")
                        error_buffer.error(f"Shard execution failed: {e}")
                        results[op_index] = (0, [], error_buffer)
        
        # Flush logs in operation order
        logger.info("")
        logger.info("Operation Logs")
        for result in results:
            if result:
                executed, chaos_events, buffer = result
                operations_executed += executed
                all_chaos_events.extend(chaos_events)
                buffer.flush()
        
        logger.info(f"Parallel execution complete: {operations_executed}/{len(operations)} succeeded")
        
        return operations_executed, all_chaos_events, []
    