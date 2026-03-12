"""
Tests for Chaos Coordinator
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
from src.models import (
    Operation, OperationType, OperationTiming, ChaosConfig, ChaosType,
    ProcessChaosType, TargetSelection, ChaosTiming, ChaosCoordination,
    NodeInfo, ChaosResult
)


@pytest.fixture
def mock_live_process():
    """Create a mock process that appears alive (poll returns None)"""
    mock = Mock()
    mock.poll.return_value = None
    return mock


def test_chaos_coordinator_initialization():
    """Test chaos coordinator initialization"""
    coordinator = ChaosCoordinator()
    
    assert coordinator.chaos_engine is not None
    assert coordinator.active_chaos_scenarios == {}
    assert coordinator.chaos_history == []


def test_register_cluster_nodes():
    """Test registering cluster nodes"""
    coordinator = ChaosCoordinator()
    
    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    coordinator.register_cluster_nodes("test-cluster", nodes)
    
    # Verify nodes are registered in chaos engine
    assert "node-0" in coordinator.chaos_engine.node_processes
    assert "node-1" in coordinator.chaos_engine.node_processes
    assert coordinator.chaos_engine.node_processes["node-0"] == 12345
    assert coordinator.chaos_engine.node_processes["node-1"] == 12346


def test_select_chaos_target_random(mock_live_process):
    """Test selecting chaos target with random strategy"""
    coordinator = ChaosCoordinator()
    cluster_id = "test-cluster"

    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    # Register nodes with the target selector
    coordinator.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)

    target_selection = TargetSelection(strategy="random")
    target = coordinator.chaos_engine.target_selector.select_target(cluster_id, target_selection)
    
    assert target is not None
    assert target.node_id in ["node-0", "node-1"]


def test_select_chaos_target_primary_only(mock_live_process):
    """Test selecting chaos target with primary_only strategy"""
    coordinator = ChaosCoordinator()
    cluster_id = "test-cluster"

    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    coordinator.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)

    target_selection = TargetSelection(strategy="primary_only")
    target = coordinator.chaos_engine.target_selector.select_target(cluster_id, target_selection)
    
    assert target is not None
    assert target.node_id == "node-0"
    assert target.role == "primary"


def test_select_chaos_target_replica_only(mock_live_process):
    """Test selecting chaos target with replica_only strategy"""
    coordinator = ChaosCoordinator()
    cluster_id = "test-cluster"

    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    coordinator.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)

    target_selection = TargetSelection(strategy="replica_only")
    target = coordinator.chaos_engine.target_selector.select_target(cluster_id, target_selection)
    
    assert target is not None
    assert target.node_id == "node-1"
    assert target.role == "replica"


def test_select_chaos_target_specific(mock_live_process):
    """Test selecting chaos target with specific strategy"""
    coordinator = ChaosCoordinator()
    cluster_id = "test-cluster"

    nodes = [
        NodeInfo(
            node_id="node-0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-1",
            role="replica",
            shard_id=0,
            port=7001,
            bus_port=17001,
            pid=12346,
            process=mock_live_process,
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        ),
        NodeInfo(
            node_id="node-2",
            role="replica",
            shard_id=1,
            port=7002,
            bus_port=17002,
            pid=12347,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    coordinator.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)

    # Test single specific node
    target_selection = TargetSelection(strategy="specific", specific_nodes=["node-1"])
    target = coordinator.chaos_engine.target_selector.select_target(cluster_id, target_selection)
    
    assert target is not None
    assert target.node_id == "node-1"

    # Test multiple specific nodes - should randomly select from them
    target_selection_multi = TargetSelection(strategy="specific", specific_nodes=["node-0", "node-2"])
    selected_nodes = set()

    # Run multiple times to verify it can select different nodes
    for _ in range(20):
        target = coordinator.chaos_engine.target_selector.select_target(cluster_id, target_selection_multi)
        assert target is not None
        assert target.node_id in ["node-0", "node-2"], "Should only select from specified nodes"
        selected_nodes.add(target.node_id)

    # With 20 iterations, we should see both nodes (very high probability)
    assert len(selected_nodes) > 1, "Should randomly select from multiple specific nodes"


def test_select_chaos_target_empty_nodes():
    """Test selecting chaos target with empty node list"""
    coordinator = ChaosCoordinator()
    cluster_id = "test-cluster"

    # Register empty node list
    coordinator.chaos_engine.target_selector.update_cluster_topology(cluster_id, [])

    target_selection = TargetSelection(strategy="random")
    target = coordinator.chaos_engine.target_selector.select_target(cluster_id, target_selection)
    
    assert target is None


def test_select_chaos_target_no_primary():
    """Test selecting chaos target when no primary nodes exist"""
    coordinator = ChaosCoordinator()
    cluster_id = "test-cluster"

    nodes = [
        NodeInfo(
            node_id="node-0",
            role="replica",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=12345,
            process=Mock(),
            data_dir="/tmp/test",
            log_file="/tmp/test.log"
        )
    ]
    
    coordinator.chaos_engine.target_selector.update_cluster_topology(cluster_id, nodes)

    target_selection = TargetSelection(strategy="primary_only")
    target = coordinator.chaos_engine.target_selector.select_target(cluster_id, target_selection)
    
    assert target is None


def test_get_chaos_history():
    """Test getting chaos history"""
    coordinator = ChaosCoordinator()
    
    # Add some chaos results to history
    result1 = ChaosResult(
        chaos_id="chaos-1",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-0",
        success=True,
        start_time=0.0,
        end_time=1.0
    )
    result2 = ChaosResult(
        chaos_id="chaos-2",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-1",
        success=True,
        start_time=2.0,
        end_time=3.0
    )
    
    coordinator.chaos_history = [result1, result2]
    
    history = coordinator.get_chaos_history()
    assert len(history) == 2
    assert history[0].chaos_id == "chaos-1"
    assert history[1].chaos_id == "chaos-2"


def test_cleanup_chaos():
    """Test cleaning up chaos for a cluster"""
    coordinator = ChaosCoordinator()
    
    # Add active chaos scenario
    coordinator.active_chaos_scenarios["test-cluster"] = []
    
    result = coordinator.cleanup_chaos("test-cluster")
    
    assert result is True
    assert "test-cluster" not in coordinator.active_chaos_scenarios


def test_get_active_chaos_count():
    """Test getting active chaos count"""
    coordinator = ChaosCoordinator()
    
    # Initially should be 0
    assert coordinator.get_active_chaos_count() == 0
    
    # Add some active chaos
    coordinator.chaos_engine.active_chaos["chaos-1"] = Mock()
    coordinator.chaos_engine.active_chaos["chaos-2"] = Mock()
    
    assert coordinator.get_active_chaos_count() == 2


def test_stop_all_chaos():
    """Test stopping all active chaos"""
    coordinator = ChaosCoordinator()
    
    # Add some active chaos
    coordinator.chaos_engine.active_chaos["chaos-1"] = Mock()
    coordinator.chaos_engine.active_chaos["chaos-2"] = Mock()
    
    coordinator.stop_all_chaos()
    
    assert len(coordinator.chaos_engine.active_chaos) == 0


@patch('src.fuzzer_engine.chaos_coordinator.time.sleep')
def test_coordinate_chaos_with_operation_no_target(mock_sleep):
    """Test coordinating chaos when no suitable target found"""
    coordinator = ChaosCoordinator()
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-0",
        parameters={},
        timing=OperationTiming()
    )
    
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="primary_only"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=ProcessChaosType.SIGKILL
    )
    
    # Mock cluster connection that returns empty node list
    mock_connection = MagicMock()
    mock_connection.get_live_nodes.return_value = []
    mock_connection.initial_nodes = []
    
    # Empty node list - no target will be found
    results = coordinator.coordinate_chaos_with_operation(operation, chaos_config, mock_connection, "test_cluster")
    
    assert len(results) == 1
    assert results[0].success is False
    assert results[0].chaos_type == ChaosType.PROCESS_KILL
    assert results[0].target_node == "node-0"
    assert "No suitable chaos target found" in results[0].error_message
    assert coordinator.chaos_history == results


@patch('src.fuzzer_engine.chaos_coordinator.time.sleep')
def test_coordinate_chaos_with_operation_returns_failure_result_on_exception(mock_sleep):
    """Test that unexpected coordination exceptions become explicit failed chaos results"""
    coordinator = ChaosCoordinator()

    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-0",
        parameters={},
        timing=OperationTiming()
    )

    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=ProcessChaosType.SIGKILL
    )

    mock_connection = MagicMock()
    mock_connection.get_live_nodes.return_value = []
    mock_connection.initial_nodes = []

    with patch.object(
        coordinator.chaos_engine.target_selector,
        'select_target',
        side_effect=RuntimeError("selector exploded")
    ):
        results = coordinator.coordinate_chaos_with_operation(
            operation,
            chaos_config,
            mock_connection,
            "test_cluster"
        )

    assert len(results) == 1
    assert results[0].success is False
    assert results[0].target_node == "node-0"
    assert "selector exploded" in results[0].error_message
    assert coordinator.chaos_history == results


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
