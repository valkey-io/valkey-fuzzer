"""
Fuzzer Engine - Main orchestrator for test scenario execution
"""
import time
import logging
from copy import deepcopy
from typing import List, Optional
from ..models import Scenario, ExecutionResult, DSLConfig, ClusterConnection, LogValidationContext
from ..interfaces import IFuzzerEngine
from ..valkey_client.load_data import load_all_slots
from .test_case_generator import ScenarioGenerator
from .cluster_coordinator import ClusterCoordinator
from .chaos_coordinator import ChaosCoordinator
from .operation_orchestrator import OperationOrchestrator
from .test_logger import FuzzerLogger
from .error_handler import ErrorHandler, ErrorContext, ErrorCategory, ErrorSeverity, RetryConfig
from .state_validator import StateValidator
from .parallel_executor import ParallelExecutor
from ..models import StateValidationConfig, ExpectedTopology, ChaosType

logger = logging.getLogger()

class FuzzerEngine(IFuzzerEngine):
    """
    Main orchestrator for the Cluster Bus Fuzzer.
    Coordinates all components to execute test scenarios end-to-end.
    """
    
    def __init__(self):
        """Initialize the fuzzer engine with all component coordinators"""
        self.scenario_generator = ScenarioGenerator()
        self.cluster_coordinator = ClusterCoordinator()
        self.chaos_coordinator = ChaosCoordinator()
        self.operation_orchestrator = OperationOrchestrator()
        self.logger = FuzzerLogger()
        self.error_handler = ErrorHandler()
            
    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """Generate a randomized test scenario with optional seed for reproducibility."""
        logger.info(f"Generating random scenario with seed: {seed}")
        return self.scenario_generator.generate_random_scenario(seed)
    
    def execute_dsl_scenario(self, dsl_config: DSLConfig, valkey_binary: Optional[str] = None) -> ExecutionResult:
        """Execute a test scenario from DSL configuration."""
        logger.info("Executing DSL-based scenario")
        
        try:
            # Parse DSL configuration into scenario
            scenario = self.scenario_generator.parse_dsl_config(dsl_config.config_text)

            if valkey_binary:
                scenario.cluster_config.valkey_binary = valkey_binary
            
            # Validate scenario
            self.scenario_generator.validate_scenario(scenario)
            
            # Execute the scenario
            return self.execute_test(scenario)
            
        except ValueError as e:
            logger.error(f"DSL parsing failed: {e}")
            return ExecutionResult(
                scenario_id="dsl-parse-error",
                success=False,
                start_time=time.time(),
                end_time=time.time(),
                operations_executed=0,
                chaos_events=[],
                error_message=f"DSL parsing error: {e}"
            )
        except Exception as e:
            logger.error(f"DSL scenario execution failed: {e}")
            return ExecutionResult(
                scenario_id="dsl-execution-error",
                success=False,
                start_time=time.time(),
                end_time=time.time(),
                operations_executed=0,
                chaos_events=[],
                error_message=f"Execution error: {e}"
            )
    
    def execute_test(self, scenario: Scenario) -> ExecutionResult:
        """
        Execute a complete test scenario end-to-end.
        
        This is the main execution pipeline that:
        1. Creates the cluster
        2. Validates cluster readiness
        3. Executes operations with chaos coordination
        4. Validates cluster state after each operation
        5. Cleans up resources
        """
        start_time = time.time()
        operations_executed = 0
        chaos_events = []
        validation_results = []  # Collect validation results for each operation wave
        final_validation = None
        cluster_instance = None
        cluster_connection = None
        
        logger.info(f"Starting test execution for scenario: {scenario.scenario_id}")
        
        # Reinitialize chaos coordinator with scenario seed for deterministic chaos selection
        self.chaos_coordinator = ChaosCoordinator(seed=scenario.seed)
        
        self.logger.log_test_start(scenario)
        
        try:
            # Step 1: Create cluster
            logger.info("Step 1: Creating cluster")
            logger.info("")
            cluster_instance = self._create_cluster_with_retry(scenario)
            
            if not cluster_instance:
                raise Exception("Failed to create cluster after retries")
            
            # Create cluster connection for operations
            cluster_connection = ClusterConnection(
                initial_nodes=cluster_instance.nodes,
                cluster_id=cluster_instance.cluster_id
            )
            
            # Step 2: Validate cluster readiness
            logger.info("")
            logger.info("Step 2: Validating cluster readiness")
            logger.info("")
            if not self._validate_cluster_readiness_with_retry(cluster_instance.cluster_id):
                raise Exception("Cluster failed readiness validation")
            
            # Step 3: Populate cluster with test data and register nodes for chaos
            logger.info("")
            logger.info("Step 3: Populating cluster with test data")
            logger.info("")
            load_all_slots(cluster_connection, keys_per_slot=5)
            
            self.chaos_coordinator.register_cluster_nodes(cluster_instance.cluster_id, cluster_instance.nodes)
            
            # Set cluster connection for operation orchestrator
            self.operation_orchestrator.set_cluster_connection(cluster_connection)
            
            # Create StateValidator with config from scenario or use defaults
            if hasattr(scenario, 'state_validation_config') and scenario.state_validation_config:
                validation_config = deepcopy(scenario.state_validation_config)
                logger.debug("Using scenario-specific StateValidationConfig")
            else:
                validation_config = StateValidationConfig()
                logger.debug("Using default StateValidationConfig")
            
            # Adjust replication validation config based on cluster topology (for DSL scenarios)
            # If the cluster has no replicas, disable the min_replicas_per_shard check
            expected_replicas_per_shard = scenario.cluster_config.replicas_per_shard
            if expected_replicas_per_shard == 0:
                validation_config.replication_config.min_replicas_per_shard = 0
                logger.info("Cluster has no replicas - disabled min_replicas_per_shard check")
            elif validation_config.replication_config.min_replicas_per_shard > expected_replicas_per_shard:
                # Don't require more replicas than the cluster is configured to have
                validation_config.replication_config.min_replicas_per_shard = expected_replicas_per_shard
                logger.info(f"Adjusted min_replicas_per_shard to {expected_replicas_per_shard} to match cluster configuration")

            state_validator = StateValidator(validation_config)
            
            # Write test data for data consistency validation
            if validation_config.check_data_consistency:
                logger.info("Writing test data for data consistency validation")
                test_data_written = state_validator.write_test_data(cluster_connection)
                if not test_data_written:
                    raise Exception("Failed to write test data required for data consistency validation")
            
            # Step 4: Execute operations in parallel with chaos coordination
            logger.info("")
            logger.info(f"Step 4: Executing {len(scenario.operations)} operations in parallel")
            logger.info("")
            
            parallel_executor = ParallelExecutor(self.operation_orchestrator, self.chaos_coordinator, self.logger)
            expected_topology = self._build_expected_topology(scenario)

            def validate_operation_wave(wave_number, wave_results):
                wave_chaos_events = []
                successful_operations = []
                for _, _, chaos_events, _ in wave_results:
                    wave_chaos_events.extend(chaos_events)
                for op_index, executed, _, _ in wave_results:
                    if executed:
                        successful_operations.append(scenario.operations[op_index])

                self._register_killed_nodes(
                    state_validator,
                    cluster_instance.nodes,
                    wave_chaos_events
                )

                operation_context = None
                if successful_operations:
                    operation_context = LogValidationContext(operations=successful_operations)

                validation_result = state_validator.validate_with_retry(
                    cluster_connection,
                    expected_topology,
                    operation_context
                )
                self.logger.log_state_validation_result(validation_result, f"wave-{wave_number}")
                return validation_result
            
            operations_executed, chaos_events, validation_results = parallel_executor.execute_operations_parallel(
                scenario.operations,
                scenario.chaos_config,
                cluster_connection,
                cluster_instance.cluster_id,
                validation_runner=validate_operation_wave,
                halt_on_validation_failure=validation_config.blocking_on_failure
            )
            
            # Step 5: Final cluster validation
            logger.info("")
            logger.info("Step 5: Final cluster validation")
            logger.info("")
            
            # Execute final validation with retry (consistent with per-operation validation)
            # Create operation context for log validation - only include successful operations
            # Match operations by type and target
            operation_logs = self.logger.test_logs.get(scenario.scenario_id, {}).get('operation_logs', [])
            successful_operations = []
            
            for op in scenario.operations:
                for log in operation_logs:
                    if (log.get('success') and 
                        log.get('operation_type') == op.type.value and 
                        log.get('target_node') == op.target_node):
                        successful_operations.append(op)
                        break
            
            operation_context = LogValidationContext(operations=successful_operations)
            
            final_validation_result = state_validator.validate_with_retry(
                cluster_connection,
                expected_topology,
                operation_context
            )
            
            # Store final validation for API consumers
            final_validation = final_validation_result
            
            # Log final validation result
            self.logger.log_state_validation_result(final_validation_result, "final")
            
            # Determine overall success
            failure_reasons = self._collect_failure_reasons(
                total_operations=len(scenario.operations),
                operations_executed=operations_executed,
                chaos_events=chaos_events,
                validation_results=validation_results,
                final_validation_result=final_validation_result
            )
            success = len(failure_reasons) == 0
            
            end_time = time.time()
            
            # Create execution result with validation results
            result = ExecutionResult(
                scenario_id=scenario.scenario_id,
                success=success,
                start_time=start_time,
                end_time=end_time,
                operations_executed=operations_executed,
                chaos_events=chaos_events,
                error_message="; ".join(failure_reasons) if failure_reasons else None,
                seed=scenario.seed,
                validation_results=validation_results,
                final_validation=final_validation
            )
                        
            # Log test completion
            self.logger.log_test_completion(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Test execution failed: {e}")
            
            end_time = time.time()
            
            # Log error
            self.logger.log_error(f"Test execution failed: {e}")
            
            # Create failure result
            result = ExecutionResult(
                scenario_id=scenario.scenario_id,
                success=False,
                start_time=start_time,
                end_time=end_time,
                operations_executed=operations_executed,
                chaos_events=chaos_events,
                error_message=str(e),
                seed=scenario.seed
            )
            
            # Log test completion
            self.logger.log_test_completion(result)
            
            return result
            
        finally:
            # Step 6: Cleanup
            logger.info("")
            logger.info("Step 6: Cleaning up resources")
            logger.info("")
            self._cleanup_resources(cluster_instance, cluster_connection)
    
    def validate_cluster_state(self, cluster_id: str):
        """Validate current cluster state and return validation result."""
        logger.info(f"Validating cluster state: {cluster_id}")
        
        # Get cluster connection from active clusters
        if cluster_id not in self.cluster_coordinator.active_clusters:
            logger.error(f"Cluster {cluster_id} not found")
            raise ValueError(f"Cluster {cluster_id} not found")
        
        cluster_data = self.cluster_coordinator.active_clusters[cluster_id]
        cluster_instance = cluster_data['instance']
        
        cluster_connection = ClusterConnection(initial_nodes=cluster_instance.nodes, cluster_id=cluster_id)
        
        # Use StateValidator for validation
        # Disable data consistency check for standalone validation (no test data seeded)
        validation_config = StateValidationConfig()
        validation_config.check_data_consistency = False
        
        # Adjust replication validation config based on actual cluster topology
        # This ensures zero-replica clusters can pass validation
        actual_replicas = len([n for n in cluster_instance.nodes if n.role == 'replica'])
        actual_primaries = len([n for n in cluster_instance.nodes if n.role == 'primary'])
        
        # Calculate replicas per shard from actual topology
        if actual_primaries > 0:
            replicas_per_shard = actual_replicas // actual_primaries
        else:
            replicas_per_shard = 0
        
        if replicas_per_shard == 0:
            validation_config.replication_config.min_replicas_per_shard = 0
            logger.info("Cluster has no replicas - disabled min_replicas_per_shard check")
        elif validation_config.replication_config.min_replicas_per_shard > replicas_per_shard:
            # Don't require more replicas than the cluster actually has
            validation_config.replication_config.min_replicas_per_shard = replicas_per_shard
            logger.info(
                f"Adjusted min_replicas_per_shard to {replicas_per_shard} "
                f"to match actual cluster topology"
            )
        
        validator = StateValidator(validation_config)
        
        # Build expected topology
        expected_topology = ExpectedTopology(
            num_primaries=actual_primaries,
            num_replicas=actual_replicas,
            shard_structure={}
        )
        
        logger.info("Running standalone validation (data consistency check disabled - no test data)")
        return validator.validate_state(cluster_connection, expected_topology, None)

    def _build_expected_topology(self, scenario: Scenario) -> ExpectedTopology:
        """Build the expected topology for validation from the scenario."""
        return ExpectedTopology(
            num_primaries=scenario.cluster_config.num_shards,
            num_replicas=scenario.cluster_config.num_shards * scenario.cluster_config.replicas_per_shard,
            shard_structure={}
        )

    def _register_killed_nodes(
        self,
        state_validator: StateValidator,
        cluster_nodes,
        chaos_events: List
    ) -> None:
        """Register chaos-killed nodes so topology and health checks can reason about them."""
        for chaos_event in chaos_events:
            if not chaos_event.success or chaos_event.chaos_type != ChaosType.PROCESS_KILL:
                continue

            target_node_id = chaos_event.target_node
            target_role = chaos_event.target_role
            for node in cluster_nodes:
                if node.node_id == target_node_id:
                    node_address = f"{node.host}:{node.port}"
                    state_validator.register_killed_node(node_address, target_role, node.shard_id)
                    logger.info(
                        f"Registered killed node for validation: {node_address} "
                        f"(role: {target_role}, shard: {node.shard_id})"
                    )
                    break

    def _collect_failure_reasons(
        self,
        total_operations: int,
        operations_executed: int,
        chaos_events: List,
        validation_results: List,
        final_validation_result
    ) -> List[str]:
        """Summarize all execution failures that should make the run fail."""
        failure_reasons = []

        if operations_executed != total_operations:
            failed_operations = total_operations - operations_executed
            failure_reasons.append(f"{failed_operations} operation(s) failed")

        failed_chaos_events = [event for event in chaos_events if not event.success]
        if failed_chaos_events:
            failure_reasons.append(f"{len(failed_chaos_events)} chaos injection(s) failed")

        if validation_results:
            failed_validation_waves = [
                result for result in validation_results
                if not result.overall_success
            ]
            if failed_validation_waves:
                failure_reasons.append(
                    f"{len(failed_validation_waves)} post-operation validation wave(s) failed"
                )
        elif total_operations > 0 and operations_executed == total_operations:
            failure_reasons.append("post-operation validation did not run")

        if not final_validation_result.overall_success:
            failed_checks = ", ".join(final_validation_result.failed_checks) or "unknown checks"
            failure_reasons.append(f"final validation failed ({failed_checks})")

        return failure_reasons
    
    def _create_cluster_with_retry(self, scenario: Scenario, max_retries: int = 3):
        """Create cluster with retry logic for failure recovery."""
        retry_config = RetryConfig(
            max_attempts=max_retries,
            initial_delay=1.0,
            exponential_base=2.0,
            jitter=True
        )
        
        def create_cluster_operation():
            return self.cluster_coordinator.create_cluster(scenario.cluster_config, seed=scenario.seed)
        
        success, cluster_instance = self.error_handler.retry_with_backoff(
            operation=create_cluster_operation,
            config=retry_config,
            error_category=ErrorCategory.CLUSTER_CREATION,
            operation_name="cluster creation"
        )
        
        if success:
            logger.info(f"Cluster created successfully - Cluster ID: {cluster_instance.cluster_id}")
        else:
            self.logger.log_error("Cluster creation failed after all retries", {"attempts": max_retries})
        
        return cluster_instance if success else None
    
    def _validate_cluster_readiness_with_retry(self, cluster_id: str, max_retries: int = 5) -> bool:
        """Validate cluster readiness with retry logic."""
        retry_config = RetryConfig(
            max_attempts=max_retries,
            initial_delay=2.0,
            exponential_base=1.5,
            jitter=False
        )
        
        def validate_readiness_operation():
            if self.cluster_coordinator.validate_cluster_readiness(cluster_id):
                return True
            else:
                raise Exception("Cluster not ready")
        
        success, _ = self.error_handler.retry_with_backoff(
            operation=validate_readiness_operation,
            config=retry_config,
            error_category=ErrorCategory.CLUSTER_VALIDATION,
            operation_name="cluster readiness validation"
        )
        
        if not success:
            self.logger.log_error("Cluster readiness validation failed", {"attempts": max_retries, "cluster_id": cluster_id})
        
        return success
    
    def _cleanup_resources(self, cluster_instance, cluster_connection):
        """Clean up all resources with graceful degradation."""
        try:
            self.error_handler.cleanup_after_failure(
                cluster_instance=cluster_instance,
                cluster_connection=cluster_connection,
                chaos_coordinator=self.chaos_coordinator,
                cluster_coordinator=self.cluster_coordinator
            )
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            # Log error but don't raise - cleanup should always complete
            error_context = ErrorContext(
                category=ErrorCategory.RESOURCE_CLEANUP,
                severity=ErrorSeverity.MEDIUM,
                message=f"Cleanup error: {e}",
                exception=e,
                cluster_id=cluster_instance.cluster_id if cluster_instance else None
            )
            self.error_handler.handle_error(error_context)
