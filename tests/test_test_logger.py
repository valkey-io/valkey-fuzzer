"""
Tests for Test Logger
"""

import json
import time

import pytest

from src.fuzzer_engine.test_logger import FuzzerLogger
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosResult,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    ClusterStatus,
    ExecutionResult,
    NodeInfo,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    Scenario,
    TargetSelection,
)


@pytest.fixture
def test_logger(tmp_path):
    """Create a test logger with temporary directory"""
    return FuzzerLogger(log_dir=str(tmp_path / "logs"))


@pytest.fixture
def sample_scenario():
    """Create a sample test scenario"""
    return Scenario(
        scenario_id="test-scenario-1",
        cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1, base_port=7000),
        operations=[
            Operation(type=OperationType.FAILOVER, target_node="node-0", parameters={}, timing=OperationTiming())
        ],
        chaos_config=ChaosConfig(
            chaos_type=ChaosType.PROCESS_KILL,
            target_selection=TargetSelection(strategy="random"),
            timing=ChaosTiming(),
            coordination=ChaosCoordination(chaos_during_operation=True),
            process_chaos_type=ProcessChaosType.SIGKILL,
        ),
        seed=12345,
    )


def test_logger_initialization(test_logger):
    """Test logger initialization"""
    assert test_logger.log_dir.exists()
    assert test_logger.current_test_id is None
    assert test_logger.test_logs == {}
    assert test_logger.test_start_times == {}


def test_log_test_start(test_logger, sample_scenario):
    """Test logging test start"""
    test_logger.log_test_start(sample_scenario)

    assert test_logger.current_test_id == "test-scenario-1"
    assert "test-scenario-1" in test_logger.test_logs
    assert "test-scenario-1" in test_logger.test_start_times

    log = test_logger.test_logs["test-scenario-1"]
    assert log["scenario_id"] == "test-scenario-1"
    assert log["seed"] == 12345
    assert log["status"] == "running"
    assert "cluster_config" in log
    assert "operations" in log
    assert "chaos_config" in log

    # Check that log file was created
    log_file = test_logger.log_dir / "test-scenario-1.json"
    assert log_file.exists()


def test_log_operation(test_logger, sample_scenario):
    """Test logging operation execution"""
    test_logger.log_test_start(sample_scenario)

    operation = sample_scenario.operations[0]
    test_logger.log_operation(operation, success=True, details="Failover completed successfully")

    log = test_logger.test_logs["test-scenario-1"]
    assert len(log["operation_logs"]) == 1

    op_log = log["operation_logs"][0]
    assert op_log["operation_type"] == "failover"
    assert op_log["target_node"] == "node-0"
    assert op_log["success"] is True
    assert op_log["details"] == "Failover completed successfully"


def test_log_chaos_event(test_logger, sample_scenario):
    """Test logging chaos event"""
    test_logger.log_test_start(sample_scenario)

    chaos_result = ChaosResult(
        chaos_id="chaos-1",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-0",
        success=True,
        start_time=time.time(),
        end_time=time.time() + 1.0,
    )

    test_logger.log_chaos_event(chaos_result)

    log = test_logger.test_logs["test-scenario-1"]
    assert len(log["chaos_events"]) == 1

    chaos_log = log["chaos_events"][0]
    assert chaos_log["chaos_id"] == "chaos-1"
    assert chaos_log["chaos_type"] == "process_kill"
    assert chaos_log["target_node"] == "node-0"
    assert chaos_log["success"] is True
    assert chaos_log["duration"] is not None


def test_log_cluster_state_snapshot(test_logger, sample_scenario):
    """Test logging cluster state snapshot"""
    test_logger.log_test_start(sample_scenario)

    cluster_status = ClusterStatus(
        cluster_id="test-cluster",
        nodes=[
            NodeInfo(
                node_id="node-0",
                role="primary",
                shard_id=0,
                port=7000,
                bus_port=17000,
                pid=12345,
                process=None,
                data_dir="/tmp/test",
                log_file="/tmp/test.log",
                slot_start=0,
                slot_end=5461,
            )
        ],
        total_slots_assigned=16384,
        is_healthy=True,
        formation_complete=True,
    )

    test_logger.log_cluster_state_snapshot(cluster_status, label="before_operation")

    log = test_logger.test_logs["test-scenario-1"]
    assert len(log["cluster_state_snapshots"]) == 1

    snapshot = log["cluster_state_snapshots"][0]
    assert snapshot["label"] == "before_operation"
    assert snapshot["cluster_id"] == "test-cluster"
    assert snapshot["is_healthy"] is True
    assert len(snapshot["nodes"]) == 1


def test_log_error(test_logger, sample_scenario):
    """Test logging error"""
    test_logger.log_test_start(sample_scenario)

    test_logger.log_error("Test error message", {"error_code": 500})

    log = test_logger.test_logs["test-scenario-1"]
    assert len(log["errors"]) == 1

    error_log = log["errors"][0]
    assert error_log["message"] == "Test error message"
    assert error_log["details"]["error_code"] == 500


def test_log_test_completion(test_logger, sample_scenario):
    """Test logging test completion"""
    test_logger.log_test_start(sample_scenario)

    start_time = test_logger.test_start_times["test-scenario-1"]
    end_time = start_time + 10.0

    execution_result = ExecutionResult(
        scenario_id="test-scenario-1",
        success=True,
        start_time=start_time,
        end_time=end_time,
        operations_executed=1,
        chaos_events=[],
        seed=12345,
    )

    test_logger.log_test_completion(execution_result)

    log = test_logger.test_logs["test-scenario-1"]
    assert log["status"] == "completed"
    assert log["success"] is True
    assert log["operations_executed"] == 1
    assert log["duration"] == 10.0

    # Current test should be cleared
    assert test_logger.current_test_id is None


def test_generate_report(test_logger):
    """Test generating summary report"""
    results = [
        ExecutionResult(
            scenario_id="test-1",
            success=True,
            start_time=0.0,
            end_time=5.0,
            operations_executed=2,
            chaos_events=[],
            seed=111,
        ),
        ExecutionResult(
            scenario_id="test-2",
            success=False,
            start_time=0.0,
            end_time=3.0,
            operations_executed=1,
            chaos_events=[],
            error_message="Test failed",
            seed=222,
        ),
        ExecutionResult(
            scenario_id="test-3",
            success=True,
            start_time=0.0,
            end_time=7.0,
            operations_executed=3,
            chaos_events=[],
            seed=333,
        ),
    ]

    report = test_logger.generate_report(results)

    assert "Total Tests:        3" in report
    assert "Successful:         2" in report
    assert "Failed:             1" in report
    assert "test-1" in report
    assert "test-2" in report
    assert "test-3" in report
    assert "Test failed" in report


def test_generate_report_empty(test_logger):
    """Test generating report with no results"""
    report = test_logger.generate_report([])
    assert report == "No test results to report"


def test_get_test_log(test_logger, sample_scenario):
    """Test getting test log"""
    test_logger.log_test_start(sample_scenario)

    log = test_logger.get_test_log("test-scenario-1")
    assert log is not None
    assert log["scenario_id"] == "test-scenario-1"

    # Non-existent test
    log = test_logger.get_test_log("non-existent")
    assert log is None


def test_get_all_test_logs(test_logger, sample_scenario):
    """Test getting all test logs"""
    test_logger.log_test_start(sample_scenario)

    all_logs = test_logger.get_all_test_logs()
    assert len(all_logs) == 1
    assert "test-scenario-1" in all_logs


def test_log_without_active_test(test_logger):
    """Test logging operations without active test"""
    # These should not crash, just log warnings
    operation = Operation(type=OperationType.FAILOVER, target_node="node-0", parameters={}, timing=OperationTiming())

    test_logger.log_operation(operation, True, "test")
    test_logger.log_chaos_event(
        ChaosResult(
            chaos_id="test", chaos_type=ChaosType.PROCESS_KILL, target_node="node-0", success=True, start_time=0.0
        )
    )
    test_logger.log_error("test error")

    # Should not have created any logs
    assert len(test_logger.test_logs) == 0


def test_log_file_persistence(test_logger, sample_scenario):
    """Test that logs are persisted to disk"""
    test_logger.log_test_start(sample_scenario)

    log_file = test_logger.log_dir / "test-scenario-1.json"
    assert log_file.exists()

    # Read and verify log file
    with open(log_file) as f:
        log_data = json.load(f)

    assert log_data["scenario_id"] == "test-scenario-1"
    assert log_data["seed"] == 12345


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_serialize_state_validation_config_complete(test_logger):
    """Test that _serialize_state_validation_config includes all nested configs"""
    from src.models import (
        ClusterStatusValidationConfig,
        DataConsistencyValidationConfig,
        ReplicationValidationConfig,
        SlotCoverageValidationConfig,
        StateValidationConfig,
        TopologyValidationConfig,
        ViewConsistencyValidationConfig,
    )

    # Create a config with all nested configs customized
    config = StateValidationConfig(
        check_replication=True,
        check_cluster_status=True,
        check_slot_coverage=True,
        check_topology=True,
        check_view_consistency=True,
        check_data_consistency=True,
        stabilization_wait=10.0,
        validation_timeout=60.0,
        replication_config=ReplicationValidationConfig(
            max_acceptable_lag=3.0, require_all_replicas_synced=True, min_replicas_per_shard=2, timeout=15.0
        ),
        cluster_status_config=ClusterStatusValidationConfig(
            acceptable_states=["ok", "degraded"], allow_degraded=True, require_quorum=False, timeout=20.0
        ),
        slot_coverage_config=SlotCoverageValidationConfig(
            require_full_coverage=False, allow_slot_conflicts=True, timeout=25.0
        ),
        topology_config=TopologyValidationConfig(strict_mode=False, allow_failed_nodes=True, timeout=30.0),
        view_consistency_config=ViewConsistencyValidationConfig(
            require_full_consensus=False,
            allow_transient_inconsistency=True,
            max_inconsistency_duration=5.0,
            timeout=35.0,
        ),
        data_consistency_config=DataConsistencyValidationConfig(
            check_test_keys=True,
            check_cross_replica_consistency=True,
            num_test_keys=200,
            key_prefix="custom_test_",
            timeout=40.0,
        ),
    )

    # Serialize the config
    serialized = test_logger._serialize_state_validation_config(config)

    # Verify top-level fields
    assert serialized["check_replication"] is True
    assert serialized["check_cluster_status"] is True
    assert serialized["check_slot_coverage"] is True
    assert serialized["check_topology"] is True
    assert serialized["check_view_consistency"] is True
    assert serialized["check_data_consistency"] is True
    assert serialized["stabilization_wait"] == 10.0
    assert serialized["validation_timeout"] == 60.0

    # Verify replication config
    assert "replication_config" in serialized
    assert serialized["replication_config"]["max_acceptable_lag"] == 3.0
    assert serialized["replication_config"]["require_all_replicas_synced"] is True
    assert serialized["replication_config"]["min_replicas_per_shard"] == 2
    assert serialized["replication_config"]["timeout"] == 15.0

    # Verify cluster status config
    assert "cluster_status_config" in serialized
    assert serialized["cluster_status_config"]["acceptable_states"] == ["ok", "degraded"]
    assert serialized["cluster_status_config"]["allow_degraded"] is True
    assert serialized["cluster_status_config"]["require_quorum"] is False
    assert serialized["cluster_status_config"]["timeout"] == 20.0

    # Verify slot coverage config
    assert "slot_coverage_config" in serialized
    assert serialized["slot_coverage_config"]["require_full_coverage"] is False
    assert serialized["slot_coverage_config"]["allow_slot_conflicts"] is True
    assert serialized["slot_coverage_config"]["timeout"] == 25.0

    # Verify topology config
    assert "topology_config" in serialized
    assert serialized["topology_config"]["strict_mode"] is False
    assert serialized["topology_config"]["allow_failed_nodes"] is True
    assert serialized["topology_config"]["timeout"] == 30.0

    # Verify view consistency config
    assert "view_consistency_config" in serialized
    assert serialized["view_consistency_config"]["require_full_consensus"] is False
    assert serialized["view_consistency_config"]["allow_transient_inconsistency"] is True
    assert serialized["view_consistency_config"]["max_inconsistency_duration"] == 5.0
    assert serialized["view_consistency_config"]["timeout"] == 35.0

    # Verify data consistency config
    assert "data_consistency_config" in serialized
    assert serialized["data_consistency_config"]["check_test_keys"] is True
    assert serialized["data_consistency_config"]["check_cross_replica_consistency"] is True
    assert serialized["data_consistency_config"]["num_test_keys"] == 200
    assert serialized["data_consistency_config"]["key_prefix"] == "custom_test_"
    assert serialized["data_consistency_config"]["timeout"] == 40.0
