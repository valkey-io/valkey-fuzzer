import pytest
from unittest.mock import Mock, patch
from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
from src.models import (
    Operation, OperationType, OperationTiming, ChaosConfig, ChaosType,
    ProcessChaosType, TargetSelection, ChaosTiming,
    ChaosCoordination, NodeInfo, ChaosResult
)
@pytest.fixture
def chaos_coordinator():
    """Create a chaos coordinator instance."""
    return ChaosCoordinator()

@pytest.fixture
def cluster_nodes():
    """Create a list of test cluster nodes."""
    return [
        NodeInfo(
            node_id="node_0", role="primary", shard_id=0, port=7000, bus_port=17000,
            pid=1000, process=Mock(), data_dir="/tmp/node_0", log_file="/tmp/node_0.log"
        ),
        NodeInfo(
            node_id="node_1", role="primary", shard_id=1, port=7001, bus_port=17001,
            pid=1001, process=Mock(), data_dir="/tmp/node_1", log_file="/tmp/node_1.log"
        ),
        NodeInfo(
            node_id="node_2", role="primary", shard_id=2, port=7002, bus_port=17002,
            pid=1002, process=Mock(), data_dir="/tmp/node_2", log_file="/tmp/node_2.log"
        ),
        NodeInfo(
            node_id="node_3", role="replica", shard_id=0, port=7003, bus_port=17003,
            pid=1003, process=Mock(), data_dir="/tmp/node_3", log_file="/tmp/node_3.log"
        ),
        NodeInfo(
            node_id="node_4", role="replica", shard_id=1, port=7004, bus_port=17004,
            pid=1004, process=Mock(), data_dir="/tmp/node_4", log_file="/tmp/node_4.log"
        ),
        NodeInfo(
            node_id="node_5", role="replica", shard_id=2, port=7005, bus_port=17005,
            pid=1005, process=Mock(), data_dir="/tmp/node_5", log_file="/tmp/node_5.log"
        ),
    ]

@pytest.fixture
def mock_cluster_connection(cluster_nodes):
    """Create a mock cluster connection that returns cluster nodes."""
    mock_conn = Mock()
    # Convert NodeInfo objects to dict format (as ClusterConnection.get_live_nodes() returns)
    live_nodes_dict = [
        {
            'node_id': node.node_id,
            'host': '127.0.0.1',
            'port': node.port,
            'role': node.role,
            'shard_id': node.shard_id,
            'status': 'connected'
        }
        for node in cluster_nodes
    ]
    mock_conn.get_live_nodes.return_value = live_nodes_dict
    mock_conn.initial_nodes = cluster_nodes
    mock_conn.cluster_id = "test_cluster"
    return mock_conn

@pytest.fixture
def base_chaos_config():
    """Create a base chaos configuration with randomization enabled."""
    return ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(
            delay_before_operation=1.0,
            delay_after_operation=1.0
        ),
        coordination=ChaosCoordination(
            chaos_during_operation=True
        ),
        process_chaos_type=None,  # Will be randomized
        randomize_per_operation=True  # Enable randomization
    )

def test_randomize_chaos_config_creates_copy(chaos_coordinator, base_chaos_config):
    """Test that randomization creates a copy and doesn't modify original."""
    original_strategy = base_chaos_config.target_selection.strategy
    original_chaos_type = base_chaos_config.process_chaos_type
    
    randomized = chaos_coordinator._randomize_chaos_config(base_chaos_config)
    
    # Original should be unchanged
    assert base_chaos_config.target_selection.strategy == original_strategy
    assert base_chaos_config.process_chaos_type == original_chaos_type
    
    # Randomized should be different object
    assert randomized is not base_chaos_config


def test_randomize_chaos_type(chaos_coordinator, base_chaos_config):
    """Test that process chaos type is randomized."""
    chaos_types_seen = set()
    
    # Run multiple times to see different chaos types
    for _ in range(20):
        randomized = chaos_coordinator._randomize_chaos_config(base_chaos_config)
        chaos_types_seen.add(randomized.process_chaos_type)
    
    # Should see both SIGKILL and SIGTERM
    assert ProcessChaosType.SIGKILL in chaos_types_seen
    assert ProcessChaosType.SIGTERM in chaos_types_seen


def test_randomize_timing_coordination(chaos_coordinator, base_chaos_config):
    """Test that chaos timing is randomized."""
    timing_combinations = set()
    
    # Run multiple times to see different timing combinations
    for _ in range(30):
        randomized = chaos_coordinator._randomize_chaos_config(base_chaos_config)
        coord = randomized.coordination
        timing_tuple = (coord.chaos_before_operation, coord.chaos_during_operation, coord.chaos_after_operation)
        timing_combinations.add(timing_tuple)
    
    # Should see multiple different timing combinations
    assert len(timing_combinations) > 1
    
    # At least one should be True in each combination
    for combo in timing_combinations:
        assert any(combo), "At least one timing option should be True"


def test_randomize_target_strategy(chaos_coordinator, base_chaos_config):
    """Test that target selection strategy is randomized."""
    strategies_seen = set()
    
    # Run multiple times to see different strategies
    for _ in range(30):
        randomized = chaos_coordinator._randomize_chaos_config(base_chaos_config)
        strategies_seen.add(randomized.target_selection.strategy)
    
    # Should see all three strategies
    assert "random" in strategies_seen
    assert "primary_only" in strategies_seen
    assert "replica_only" in strategies_seen


def test_specific_nodes_not_randomized(chaos_coordinator):
    """Test that specific node selection is not randomized."""
    config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(
            strategy="specific",
            specific_nodes=["node_0", "node_1"]
        ),
        timing=ChaosTiming(delay_before_operation=1.0, delay_after_operation=1.0),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=None
    )
    
    # Run multiple times
    for _ in range(10):
        randomized = chaos_coordinator._randomize_chaos_config(config)
        # Strategy should remain "specific"
        assert randomized.target_selection.strategy == "specific"
        assert randomized.target_selection.specific_nodes == ["node_0", "node_1"]


def test_randomization_distribution(chaos_coordinator, base_chaos_config):
    """Test that randomization has reasonable distribution."""
    # Test chaos type distribution
    sigkill_count = 0
    sigterm_count = 0
    
    for _ in range(100):
        randomized = chaos_coordinator._randomize_chaos_config(base_chaos_config)
        if randomized.process_chaos_type == ProcessChaosType.SIGKILL:
            sigkill_count += 1
        else:
            sigterm_count += 1
    
    # Should be roughly 50/50 (allow 30-70% range for randomness)
    assert 30 <= sigkill_count <= 70
    assert 30 <= sigterm_count <= 70
    
    # Test strategy distribution
    strategy_counts = {"random": 0, "primary_only": 0, "replica_only": 0}
    
    for _ in range(150):
        randomized = chaos_coordinator._randomize_chaos_config(base_chaos_config)
        strategy_counts[randomized.target_selection.strategy] += 1
    
    # Each strategy should appear (allow 30-70% range for randomness)
    for strategy, count in strategy_counts.items():
        assert 30 <= count <= 70, f"Strategy {strategy} has poor distribution: {count}/150"


# ============================================================================
# Unit Tests - Coordination
# ============================================================================

@patch('src.fuzzer_engine.chaos_coordinator.time.sleep')
def test_coordinate_chaos_without_randomization(mock_sleep, chaos_coordinator, mock_cluster_connection):
    """Test that chaos is deterministic when randomize_per_operation=False."""
    chaos_types_used = []
    
    def capture_chaos_type(node, chaos_type, **kwargs):
        chaos_types_used.append(chaos_type)
        return Mock(
            success=True,
            chaos_id=f"test_{len(chaos_types_used)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            start_time=0,
            end_time=1
        )
    
    chaos_coordinator.chaos_engine.inject_process_chaos = capture_chaos_type
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node_0",
        parameters={"test_key": "test_value"},
        timing=OperationTiming()
    )
    
    # Test 1: Explicit chaos type is preserved
    config_explicit = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="primary_only"),
        timing=ChaosTiming(delay_before_operation=1.0, delay_after_operation=1.0),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=ProcessChaosType.SIGTERM  # Explicitly set to SIGTERM
    )
    
    chaos_types_used.clear()
    for _ in range(5):
        chaos_coordinator.coordinate_chaos_with_operation(
            operation, config_explicit, mock_cluster_connection, "test_cluster"
        )
    assert all(ct == ProcessChaosType.SIGTERM for ct in chaos_types_used), "Explicit SIGTERM should be preserved"
    
    # Test 2: Unset defaults to SIGKILL
    config_unset = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="primary_only"),
        timing=ChaosTiming(delay_before_operation=1.0, delay_after_operation=1.0),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=None,  # Unset - should default to SIGKILL
        randomize_per_operation=False  # Deterministic
    )
    
    chaos_types_used.clear()
    for _ in range(5):
        chaos_coordinator.coordinate_chaos_with_operation(
            operation, config_unset, mock_cluster_connection, "test_cluster"
        )
    assert all(ct == ProcessChaosType.SIGKILL for ct in chaos_types_used), "Unset should default to SIGKILL"


@patch('src.fuzzer_engine.chaos_coordinator.time.sleep')
def test_timing_delay_randomization(mock_sleep, chaos_coordinator, mock_cluster_connection):
    """
    Test that timing delays are randomized within ±20% when randomization is enabled.
    """
    chaos_coordinator.chaos_engine.inject_process_chaos = Mock(return_value=Mock(
        success=True,
        chaos_id="test",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node_0",
        start_time=0,
        end_time=1
    ))
    
    # Set coordination to use before operation
    config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(
            delay_before_operation=10.0,  # Base delay
            delay_after_operation=10.0
        ),
        coordination=ChaosCoordination(
            chaos_before_operation=True,
            chaos_during_operation=False,
            chaos_after_operation=False
        ),
        process_chaos_type=ProcessChaosType.SIGKILL,
        randomize_per_operation=True  # Enable randomization for timing delays
    )
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node_0",
        parameters={"test_key": "test_value"},
        timing=OperationTiming()
    )
    
    # Run multiple times to ensure we get at least one sleep call
    # (randomization might change coordination timing)
    sleep_values = []
    for _ in range(20):
        mock_sleep.reset_mock()
        chaos_coordinator.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster"
        )
        
        # If sleep was called, capture the value
        if mock_sleep.called:
            sleep_values.append(mock_sleep.call_args[0][0])
    
    # We should have at least some sleep calls
    assert len(sleep_values) > 0, "Sleep should have been called at least once across 20 operations"
    
    # All sleep values should be in range [8.0, 12.0] (±20% of 10.0)
    for sleep_value in sleep_values:
        assert 8.0 <= sleep_value <= 12.0, f"Sleep value {sleep_value} not in expected range [8.0, 12.0]"

@patch('src.fuzzer_engine.chaos_coordinator.time.sleep')
def test_chaos_randomization_across_operations(mock_sleep, chaos_coordinator, mock_cluster_connection):
    """
    Integration test: Verify that chaos is randomized across multiple operations.
    This test simulates executing multiple operations and verifies that:
    """
    # Track chaos injections
    chaos_injections = []
    
    def mock_inject_chaos(node, chaos_type, **kwargs):
        """Mock chaos injection to track what was injected."""
        chaos_injections.append({
            'node_id': node.node_id,
            'node_role': node.role,
            'chaos_type': chaos_type
        })
        return ChaosResult(
            chaos_id=f"chaos_{len(chaos_injections)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            success=True,
            start_time=0,
            end_time=1
        )
    
    chaos_coordinator.chaos_engine.inject_process_chaos = mock_inject_chaos
    
    # Base chaos config (will be randomized per operation)
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(
            delay_before_operation=0.1,
            delay_after_operation=0.1
        ),
        coordination=ChaosCoordination(
            chaos_during_operation=True
        ),
        process_chaos_type=None,  # Will be randomized
        randomize_per_operation=True  # Enable randomization
    )
    
    # Create multiple operations
    operations = [
        Operation(
            type=OperationType.FAILOVER,
            target_node="node_0",
            parameters={"key": f"key_{i}", "value": f"value_{i}"},
            timing=OperationTiming()
        )
        for i in range(20)
    ]
    
    # Execute operations with chaos
    for operation in operations:
        chaos_coordinator.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster"
        )
    
    # Verify chaos was injected
    assert len(chaos_injections) > 0, "No chaos was injected"
    
    # Verify different chaos types were used
    chaos_types = [inj['chaos_type'] for inj in chaos_injections]
    unique_chaos_types = set(chaos_types)
    assert ProcessChaosType.SIGKILL in unique_chaos_types, "SIGKILL should be used"
    assert ProcessChaosType.SIGTERM in unique_chaos_types, "SIGTERM should be used"
    
    # Verify different nodes were targeted
    target_nodes = [inj['node_id'] for inj in chaos_injections]
    unique_nodes = set(target_nodes)
    assert len(unique_nodes) > 1, f"Only one node was targeted: {unique_nodes}"
    
    # Print summary for visibility
    print(f"\n=== Chaos Randomization Results ===")
    print(f"Total chaos injections: {len(chaos_injections)}")
    print(f"Unique chaos types: {unique_chaos_types}")
    print(f"Unique nodes targeted: {unique_nodes}")
    print(f"\nChaos type distribution:")
    print(f"  SIGKILL: {chaos_types.count(ProcessChaosType.SIGKILL)}")
    print(f"  SIGTERM: {chaos_types.count(ProcessChaosType.SIGTERM)}")
    print(f"\nNode distribution:")
    for node_id in sorted(unique_nodes):
        count = target_nodes.count(node_id)
        role = next(inj['node_role'] for inj in chaos_injections if inj['node_id'] == node_id)
        print(f"  {node_id} ({role}): {count}")


@patch('src.fuzzer_engine.chaos_coordinator.time.sleep')
def test_chaos_without_randomization_is_consistent(mock_sleep, chaos_coordinator, mock_cluster_connection):
    """
    Integration test: Verify that when randomization is disabled, chaos remains consistent.
    """
    # Track chaos injections
    chaos_injections = []
    
    def mock_inject_chaos(node, chaos_type, **kwargs):
        """Mock chaos injection to track what was injected."""
        chaos_injections.append({
            'node_id': node.node_id,
            'chaos_type': chaos_type
        })
        return ChaosResult(
            chaos_id=f"chaos_{len(chaos_injections)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            success=True,
            start_time=0,
            end_time=1
        )
    
    chaos_coordinator.chaos_engine.inject_process_chaos = mock_inject_chaos
    
    # Explicit chaos config (no randomization)
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="primary_only"),
        timing=ChaosTiming(
            delay_before_operation=0.1,
            delay_after_operation=0.1
        ),
        coordination=ChaosCoordination(
            chaos_during_operation=True,
            chaos_before_operation=False,
            chaos_after_operation=False
        ),
        process_chaos_type=ProcessChaosType.SIGKILL  # Explicitly set
    )
    
    # Create multiple operations
    operations = [
        Operation(
            type=OperationType.FAILOVER,
            target_node="node_0",
            parameters={"key": f"key_{i}", "value": f"value_{i}"},
            timing=OperationTiming()
        )
        for i in range(10)
    ]
    
    # Execute operations WITHOUT randomization
    for operation in operations:
        chaos_coordinator.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster"
        )
    
    # Verify chaos was injected
    assert len(chaos_injections) == 10, "Should have 10 chaos injections"
    
    # Verify only SIGKILL was used (no randomization)
    chaos_types = [inj['chaos_type'] for inj in chaos_injections]
    assert all(ct == ProcessChaosType.SIGKILL for ct in chaos_types), "Only SIGKILL should be used"
    
    # Verify only primary nodes were targeted
    target_nodes = [inj['node_id'] for inj in chaos_injections]
    primary_node_ids = {"node_0", "node_1", "node_2"}
    assert all(node_id in primary_node_ids for node_id in target_nodes), "Only primary nodes should be targeted"
    
    # Print summary for visibility
    print(f"\n=== Non-Randomized Chaos Results ===")
    print(f"Total chaos injections: {len(chaos_injections)}")
    print(f"Chaos types: {set(chaos_types)}")
    print(f"Nodes targeted: {set(target_nodes)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
