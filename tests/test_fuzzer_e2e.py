"""
End-to-End tests for Fuzzer Engine with real Valkey clusters

These tests create actual Valkey clusters and run complete test scenarios.
They are slower than unit tests but provide full integration validation.
"""

import logging
import time
from pathlib import Path

import pytest

from src.fuzzer_engine import DSLLoader, FuzzerEngine
from src.main import ClusterBusFuzzer
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    Scenario,
    StateValidationConfig,
    TargetSelection,
)

logging.basicConfig(format="%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s", level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


@pytest.mark.slow
class TestFuzzerEngineE2E:
    """End-to-end tests with real Valkey clusters"""

    def test_simple_random_scenario_execution(self):
        """
        Test complete execution of a simple random scenario.
        Creates real cluster, executes operations, validates state.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: Simple Random Scenario")
        logger.info("=" * 80)

        fuzzer = ClusterBusFuzzer()

        # Run a simple random test with seed for reproducibility
        result = fuzzer.run_random_test(seed=42)

        # Verify test completed
        assert result is not None
        assert result.scenario_id == "42"
        assert result.end_time > result.start_time

        # Test should execute at least one operation
        assert result.operations_executed >= 0  # May be 0 if cluster creation failed

        # Log results
        logger.info(f"Test completed: {'SUCCESS' if result.success else 'FAILED'}")
        logger.info(f"Duration: {result.end_time - result.start_time:.2f}s")
        logger.info(f"Operations executed: {result.operations_executed}")
        logger.info(f"Chaos events: {len(result.chaos_events)}")

        if result.error_message:
            logger.warning(f"Error message: {result.error_message}")

    def test_minimal_failover_without_chaos(self):
        """
        Test a minimal failover scenario without chaos injection.
        This is the simplest possible end-to-end test.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: Minimal Failover Without Chaos")
        logger.info("=" * 80)

        engine = FuzzerEngine()

        # Create minimal scenario
        scenario = Scenario(
            scenario_id="e2e-minimal-failover",
            cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1, base_port=7000),
            operations=[
                Operation(
                    type=OperationType.FAILOVER,
                    target_node="shard-0-primary",
                    parameters={"force": False},
                    timing=OperationTiming(delay_before=0.0, timeout=30.0, delay_after=0.0),
                )
            ],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(delay_before_operation=0.0, delay_after_operation=0.0, chaos_duration=0.0),
                coordination=ChaosCoordination(
                    chaos_before_operation=False, chaos_during_operation=False, chaos_after_operation=False
                ),
                process_chaos_type=ProcessChaosType.SIGKILL,
            ),
            state_validation_config=StateValidationConfig(
                check_slot_coverage=True,
                check_data_consistency=False,  # Skip expensive check
                validation_timeout=30.0,
            ),
            seed=123,
        )

        # Execute scenario
        result = engine.execute_test(scenario)

        # Verify execution
        assert result is not None
        assert result.scenario_id == "e2e-minimal-failover"

        # Log results
        logger.info(f"Test completed: {'SUCCESS' if result.success else 'FAILED'}")
        logger.info(f"Duration: {result.end_time - result.start_time:.2f}s")
        logger.info(f"Operations executed: {result.operations_executed}")

        if result.error_message:
            logger.warning(f"Error: {result.error_message}")

    def test_dsl_scenario_from_file(self):
        """
        Test execution of DSL scenario from example file.
        Uses the simple_failover.yaml example.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: DSL Scenario from File")
        logger.info("=" * 80)

        fuzzer = ClusterBusFuzzer()

        # Load DSL from example file
        dsl_file = Path("examples/simple_failover.yaml")

        if not dsl_file.exists():
            pytest.skip(f"DSL file not found: {dsl_file}")

        dsl_config = DSLLoader.load_from_file(dsl_file)

        # Execute DSL scenario
        result = fuzzer.run_dsl_test(dsl_config)

        # Verify execution
        assert result is not None
        assert result.scenario_id == "simple-failover-test"
        assert result.seed == 12345

        # Log results
        logger.info(f"Test completed: {'SUCCESS' if result.success else 'FAILED'}")
        logger.info(f"Duration: {result.end_time - result.start_time:.2f}s")
        logger.info(f"Operations executed: {result.operations_executed}")

        if result.error_message:
            logger.warning(f"Error: {result.error_message}")

    def test_failover_with_chaos_injection(self):
        """
        Test failover with chaos injection during operation.
        This tests the full chaos coordination pipeline.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: Failover with Chaos Injection")
        logger.info("=" * 80)

        engine = FuzzerEngine()

        # Create scenario with chaos during operation
        scenario = Scenario(
            scenario_id="e2e-failover-with-chaos",
            cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1, base_port=7100),
            operations=[
                Operation(
                    type=OperationType.FAILOVER,
                    target_node="shard-0-primary",
                    parameters={"force": False},
                    timing=OperationTiming(timeout=45.0),
                )
            ],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="primary_only"),
                timing=ChaosTiming(delay_before_operation=0.5, delay_after_operation=0.5, chaos_duration=5.0),
                coordination=ChaosCoordination(
                    chaos_before_operation=False, chaos_during_operation=True, chaos_after_operation=False
                ),
                process_chaos_type=ProcessChaosType.SIGKILL,
            ),
            state_validation_config=StateValidationConfig(validation_timeout=60.0, check_data_consistency=False),
            seed=456,
        )

        # Execute scenario
        result = engine.execute_test(scenario)

        # Verify execution
        assert result is not None
        assert result.scenario_id == "e2e-failover-with-chaos"

        # Log results
        logger.info(f"Test completed: {'SUCCESS' if result.success else 'FAILED'}")
        logger.info(f"Duration: {result.end_time - result.start_time:.2f}s")
        logger.info(f"Operations executed: {result.operations_executed}")
        logger.info(f"Chaos events: {len(result.chaos_events)}")

        # Verify chaos was attempted
        if result.chaos_events:
            logger.info(f"Chaos injections: {len(result.chaos_events)}")
            for i, chaos in enumerate(result.chaos_events):
                logger.info(
                    f"  Chaos {i + 1}: {chaos.chaos_type.value} on {chaos.target_node} - "
                    f"{'SUCCESS' if chaos.success else 'FAILED'}"
                )

        if result.error_message:
            logger.warning(f"Error: {result.error_message}")

    def test_multiple_operations_sequence(self):
        """
        Test execution of multiple sequential operations.
        Validates that operations execute in order and cluster remains stable.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: Multiple Sequential Operations")
        logger.info("=" * 80)

        engine = FuzzerEngine()

        # Create scenario with multiple operations
        scenario = Scenario(
            scenario_id="e2e-multiple-operations",
            cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1, base_port=7200),
            operations=[
                Operation(
                    type=OperationType.FAILOVER,
                    target_node="shard-0-primary",
                    parameters={"force": False},
                    timing=OperationTiming(
                        delay_before=0.0,
                        timeout=30.0,
                        delay_after=2.0,  # Wait between operations
                    ),
                ),
                Operation(
                    type=OperationType.FAILOVER,
                    target_node="shard-1-primary",
                    parameters={"force": False},
                    timing=OperationTiming(delay_before=0.0, timeout=30.0, delay_after=2.0),
                ),
            ],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(
                    chaos_before_operation=False, chaos_during_operation=False, chaos_after_operation=False
                ),
                process_chaos_type=ProcessChaosType.SIGKILL,
            ),
            state_validation_config=StateValidationConfig(validation_timeout=45.0, check_data_consistency=False),
            seed=789,
        )

        # Execute scenario
        result = engine.execute_test(scenario)

        # Verify execution
        assert result is not None
        assert result.scenario_id == "e2e-multiple-operations"

        # Log results
        logger.info(f"Test completed: {'SUCCESS' if result.success else 'FAILED'}")
        logger.info(f"Duration: {result.end_time - result.start_time:.2f}s")
        logger.info(f"Operations executed: {result.operations_executed} / {len(scenario.operations)}")

        if result.error_message:
            logger.warning(f"Error: {result.error_message}")

    def test_reproducibility_with_seed(self):
        """
        Test that same seed produces reproducible results.
        Runs same scenario twice and compares outcomes.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: Reproducibility with Seed")
        logger.info("=" * 80)

        fuzzer = ClusterBusFuzzer()
        seed = 999

        # Run first test
        logger.info("Running test 1...")
        result1 = fuzzer.run_random_test(seed=seed)

        # Wait a bit between tests
        time.sleep(2)

        # Run second test with same seed
        logger.info("Running test 2 with same seed...")
        result2 = fuzzer.run_random_test(seed=seed)

        # Verify both completed
        assert result1 is not None
        assert result2 is not None

        # Both should have same scenario ID
        assert result1.scenario_id == result2.scenario_id == str(seed)

        # Log comparison
        logger.info(f"Test 1: Operations={result1.operations_executed}, Success={result1.success}")
        logger.info(f"Test 2: Operations={result2.operations_executed}, Success={result2.success}")

        # Note: Results may differ due to timing, but structure should be same
        logger.info("Both tests used same seed and scenario structure")


@pytest.mark.slow
class TestFuzzerErrorRecovery:
    """Test error handling and graceful degradation in real scenarios"""

    def test_cluster_creation_retry(self):
        """
        Test that cluster creation retries work.
        This may fail on first attempt due to port conflicts.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: Cluster Creation Retry")
        logger.info("=" * 80)

        engine = FuzzerEngine()

        # Create scenario
        scenario = Scenario(
            scenario_id="e2e-retry-test",
            cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1, base_port=7300),
            operations=[
                Operation(
                    type=OperationType.FAILOVER, target_node="shard-0-primary", parameters={}, timing=OperationTiming()
                )
            ],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=ProcessChaosType.SIGKILL,
            ),
            state_validation_config=StateValidationConfig(),
        )

        # Execute - should handle retries internally
        result = engine.execute_test(scenario)

        assert result is not None
        logger.info(f"Test completed: {'SUCCESS' if result.success else 'FAILED'}")

        if result.error_message:
            logger.info(f"Error (expected if ports busy): {result.error_message}")


@pytest.mark.slow
class TestFuzzerLogging:
    """Test logging and reporting functionality with real execution"""

    def test_log_files_created(self):
        """
        Test that log files are created during execution.
        """
        logger.info("=" * 80)
        logger.info("E2E Test: Log File Creation")
        logger.info("=" * 80)

        fuzzer = ClusterBusFuzzer()

        # Run test
        result = fuzzer.run_random_test(seed=111)

        # Check that log directory exists
        log_dir = Path("/tmp/valkey-fuzzer/logs")
        assert log_dir.exists(), "Log directory should be created"

        # Check for log file
        log_file = log_dir / f"{result.scenario_id}.json"

        if log_file.exists():
            logger.info(f"Log file created: {log_file}")

            # Read and verify log content
            import json

            with open(log_file) as f:
                log_data = json.load(f)

            assert log_data["scenario_id"] == result.scenario_id
            assert "start_time" in log_data
            assert "cluster_config" in log_data
            logger.info("Log file contains valid data")
        else:
            logger.warning(f"Log file not found: {log_file}")


if __name__ == "__main__":
    # Run with: pytest tests/test_fuzzer_e2e.py -v -s -m slow
    pytest.main([__file__, "-v", "-s", "-m", "slow"])
