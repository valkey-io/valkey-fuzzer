"""
Parallel Executor - Executes multiple operations concurrently with buffered logging
"""
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional, Tuple

from ..models import ChaosConfig, ChaosResult, Operation
from .operation_log_buffer import OperationLogBuffer

logger = logging.getLogger()


class ParallelExecutor:
    """Executes operations in parallel by shard with per-operation log buffering"""

    def __init__(self, operation_orchestrator, chaos_coordinator, fuzzer_logger):
        self.operation_orchestrator = operation_orchestrator
        self.chaos_coordinator = chaos_coordinator
        self.fuzzer_logger = fuzzer_logger

    def execute_operations_parallel(
        self,
        operations: List[Operation],
        chaos_config: ChaosConfig,
        cluster_connection,
        cluster_id: str,
        validation_runner: Optional[
            Callable[[int, List[Tuple[int, int, List[ChaosResult], OperationLogBuffer]]], object]
        ] = None,
        halt_on_validation_failure: bool = False,
    ) -> Tuple[int, List[ChaosResult], List]:
        """Execute operations in parallel by shard, validating after each shard wave."""
        logger.info(f"Starting parallel execution of {len(operations)} operations")

        if not operations:
            return 0, [], []

        shard_groups = defaultdict(list)
        for op_index, operation in enumerate(operations):
            shard_id = operation.target_node.rsplit("-", 1)[0] if "-" in operation.target_node else operation.target_node
            shard_groups[shard_id].append((op_index, operation))

        logger.info(f"Grouped {len(operations)} operations into {len(shard_groups)} shard groups")

        operations_executed = 0
        all_chaos_events = []
        validation_results = []
        ordered_shard_ids = list(shard_groups.keys())
        total_waves = max(len(shard_ops) for shard_ops in shard_groups.values())

        def execute_single_operation(op_index: int, operation: Operation):
            """Execute one operation with chaos coordination and buffered logging."""
            op_id = f"{op_index + 1}: {operation.type.value} on {operation.target_node}"
            buffer = OperationLogBuffer(op_id)

            try:
                buffer.info(f"Starting {operation.type.value}")

                chaos_results = self.chaos_coordinator.coordinate_chaos_with_operation(
                    operation,
                    chaos_config,
                    cluster_connection,
                    cluster_id,
                    log_buffer=buffer,
                )

                immediate_chaos = [
                    chaos for chaos in chaos_results
                    if not isinstance(chaos, dict) or not chaos.get("deferred")
                ]
                deferred_chaos = [
                    chaos for chaos in chaos_results
                    if isinstance(chaos, dict) and chaos.get("deferred")
                ]

                success = self.operation_orchestrator.execute_operation(operation, log_buffer=buffer)

                for deferred in deferred_chaos:
                    buffer.info(f"Injecting deferred chaos after operation (delay: {deferred['delay']:.2f}s)")
                    time.sleep(deferred["delay"])

                    target_node = deferred["target_node"]
                    live_nodes_dict = cluster_connection.get_live_nodes()
                    if live_nodes_dict:
                        for node_dict in live_nodes_dict:
                            if node_dict["node_id"] == target_node.cluster_node_id:
                                target_node.role = node_dict.get("role", target_node.role)
                                buffer.debug(f"Refreshed target node role to: {target_node.role}")
                                break

                    result = self.chaos_coordinator._inject_chaos(
                        target_node,
                        deferred["chaos_config"],
                        deferred["should_randomize"],
                        buffer,
                        cluster_connection,
                    )
                    immediate_chaos.append(result)
                    if isinstance(result, ChaosResult):
                        self.chaos_coordinator.chaos_history.append(result)

                chaos_events = immediate_chaos

                self.fuzzer_logger.log_operation(
                    operation,
                    success,
                    f"Operation {'succeeded' if success else 'failed'}",
                    silent=False,
                )
                for chaos_event in chaos_events:
                    if isinstance(chaos_event, ChaosResult):
                        self.fuzzer_logger.log_chaos_event(chaos_event)

                buffer.info(f"Operation {'succeeded' if success else 'failed'}")
                return op_index, 1 if success else 0, chaos_events, buffer

            except Exception as exc:
                buffer.error(f"Operation failed with exception: {exc}")
                return op_index, 0, [], buffer

        for wave_index in range(total_waves):
            wave_operations = []
            for shard_id in ordered_shard_ids:
                shard_ops = shard_groups[shard_id]
                if wave_index < len(shard_ops):
                    wave_operations.append(shard_ops[wave_index])

            logger.info("")
            logger.info(
                f"Starting operation wave {wave_index + 1}/{total_waves} "
                f"with {len(wave_operations)} operation(s)"
            )

            wave_results = []
            with ThreadPoolExecutor(max_workers=len(wave_operations)) as executor:
                futures = {
                    executor.submit(execute_single_operation, op_index, operation): op_index
                    for op_index, operation in wave_operations
                }

                for future in as_completed(futures):
                    try:
                        wave_results.append(future.result())
                    except Exception as exc:
                        op_index = futures[future]
                        logger.error(f"Operation {op_index + 1} execution failed: {exc}")

            wave_results.sort(key=lambda result: result[0])

            logger.info("")
            logger.info(f"Operation Logs - Wave {wave_index + 1}")
            for _, executed, chaos_events, buffer in wave_results:
                operations_executed += executed
                all_chaos_events.extend(chaos_events)
                buffer.flush()

            if validation_runner and wave_results:
                validation_result = validation_runner(wave_index + 1, wave_results)
                if validation_result is not None:
                    validation_results.append(validation_result)
                    if not validation_result.overall_success and (
                        halt_on_validation_failure or validation_result.is_critical_failure()
                    ):
                        logger.error(
                            f"Stopping execution after validation failure in wave {wave_index + 1}"
                        )
                        break

        logger.info(f"Parallel execution complete: {operations_executed}/{len(operations)} succeeded")
        return operations_executed, all_chaos_events, validation_results
