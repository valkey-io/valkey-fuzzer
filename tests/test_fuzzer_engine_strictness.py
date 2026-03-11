"""
Unit tests for strict run success criteria and wave validation behavior.
"""
import time
from unittest.mock import Mock, patch

from src.fuzzer_engine.fuzzer_engine import FuzzerEngine
from src.fuzzer_engine.parallel_executor import ParallelExecutor
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosResult,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    ClusterStatusValidation,
    ExecutionResult,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    Scenario,
    StateValidationResult,
    TargetSelection,
)


def build_scenario() -> Scenario:
    return Scenario(
        scenario_id="strict-test",
        cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1),
        operations=[
            Operation(
                type=OperationType.FAILOVER,
                target_node="shard-0-primary",
                parameters={"force": False},
                timing=OperationTiming(),
            ),
            Operation(
                type=OperationType.FAILOVER,
                target_node="shard-1-primary",
                parameters={"force": False},
                timing=OperationTiming(),
            ),
        ],
        chaos_config=ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(),
            coordination=ChaosCoordination(),
            process_chaos_type=ProcessChaosType.SIGKILL,
        ),
    )


def build_validation_result(
    overall_success: bool,
    failed_checks=None,
    cluster_state: str = "ok",
) -> StateValidationResult:
    failed_checks = failed_checks or []
    error_messages = [f"{check} failed" for check in failed_checks]

    cluster_status = ClusterStatusValidation(
        success=overall_success,
        cluster_state=cluster_state,
        nodes_in_fail_state=[],
        has_quorum=True,
        degraded_reason=None,
        error_message=", ".join(error_messages) if error_messages else None,
    )

    return StateValidationResult(
        overall_success=overall_success,
        validation_timestamp=time.time(),
        validation_duration=0.5,
        replication=None,
        cluster_status=cluster_status,
        slot_coverage=None,
        topology=None,
        view_consistency=None,
        data_consistency=None,
        failed_checks=failed_checks,
        error_messages=error_messages,
    )


def build_cluster_instance():
    node = Mock()
    node.node_id = "node-0"
    node.host = "127.0.0.1"
    node.port = 7000
    return Mock(cluster_id="cluster-1", nodes=[node])


@patch("src.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("src.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("src.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("src.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_not_all_operations_succeed(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection = Mock()
    mock_connection_class.return_value = mock_connection

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        1,
        [],
        [build_validation_result(True)],
    )
    mock_executor_class.return_value = mock_executor

    result = engine.execute_test(scenario)

    assert result.success is False
    assert result.error_message == "1 operation(s) failed"


@patch("src.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("src.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("src.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("src.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_post_operation_validation_fails(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        len(scenario.operations),
        [],
        [build_validation_result(False, failed_checks=["replication"])],
    )
    mock_executor_class.return_value = mock_executor

    result = engine.execute_test(scenario)

    assert result.success is False
    assert "post-operation validation wave(s) failed" in result.error_message


@patch("src.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("src.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("src.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("src.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_chaos_injection_fails(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    failed_chaos = ChaosResult(
        chaos_id="chaos-1",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-0",
        success=False,
        start_time=time.time(),
        end_time=time.time(),
        error_message="kill failed",
    )

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        len(scenario.operations),
        [failed_chaos],
        [build_validation_result(True)],
    )
    mock_executor_class.return_value = mock_executor

    result = engine.execute_test(scenario)

    assert result.success is False
    assert result.error_message == "1 chaos injection(s) failed"


@patch("src.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("src.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("src.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("src.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_fails_when_data_consistency_setup_fails(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = False
    mock_validator_class.return_value = mock_validator

    result = engine.execute_test(scenario)

    assert result.success is False
    assert "Failed to write test data required for data consistency validation" in result.error_message
    mock_executor_class.assert_not_called()


@patch("src.fuzzer_engine.fuzzer_engine.load_all_slots")
@patch("src.fuzzer_engine.fuzzer_engine.StateValidator")
@patch("src.fuzzer_engine.fuzzer_engine.ClusterConnection")
@patch("src.fuzzer_engine.fuzzer_engine.ParallelExecutor")
def test_execute_test_keeps_default_cluster_state_validation_strict(
    mock_executor_class,
    mock_connection_class,
    mock_validator_class,
    mock_load_all_slots,
):
    engine = FuzzerEngine()
    scenario = build_scenario()
    cluster_instance = build_cluster_instance()

    engine._create_cluster_with_retry = Mock(return_value=cluster_instance)
    engine._validate_cluster_readiness_with_retry = Mock(return_value=True)
    engine._cleanup_resources = Mock()

    mock_connection_class.return_value = Mock()

    mock_validator = Mock()
    mock_validator.write_test_data.return_value = True
    mock_validator.validate_with_retry.return_value = build_validation_result(True)
    mock_validator_class.return_value = mock_validator

    mock_executor = Mock()
    mock_executor.execute_operations_parallel.return_value = (
        len(scenario.operations),
        [],
        [build_validation_result(True)],
    )
    mock_executor_class.return_value = mock_executor

    engine.execute_test(scenario)

    config = mock_validator_class.call_args[0][0]
    assert config.cluster_status_config.acceptable_states == ["ok"]


def test_parallel_executor_runs_validation_after_each_wave():
    operation_orchestrator = Mock()
    operation_orchestrator.execute_operation.return_value = True

    chaos_coordinator = Mock()
    chaos_coordinator.coordinate_chaos_with_operation.return_value = []

    fuzzer_logger = Mock()
    executor = ParallelExecutor(operation_orchestrator, chaos_coordinator, fuzzer_logger)

    operations = [
        Operation(
            type=OperationType.FAILOVER,
            target_node="shard-0-primary",
            parameters={},
            timing=OperationTiming(),
        ),
        Operation(
            type=OperationType.FAILOVER,
            target_node="shard-1-primary",
            parameters={},
            timing=OperationTiming(),
        ),
        Operation(
            type=OperationType.FAILOVER,
            target_node="shard-0-primary",
            parameters={},
            timing=OperationTiming(),
        ),
    ]

    validation_runner = Mock(side_effect=[
        build_validation_result(True),
        build_validation_result(True),
    ])

    operations_executed, chaos_events, validation_results = executor.execute_operations_parallel(
        operations,
        build_scenario().chaos_config,
        Mock(get_live_nodes=Mock(return_value=[])),
        "cluster-1",
        validation_runner=validation_runner,
    )

    assert operations_executed == 3
    assert chaos_events == []
    assert len(validation_results) == 2
    assert validation_runner.call_count == 2

    first_wave_results = validation_runner.call_args_list[0].args[1]
    second_wave_results = validation_runner.call_args_list[1].args[1]

    assert [result[0] for result in first_wave_results] == [0, 1]
    assert [result[0] for result in second_wave_results] == [2]
