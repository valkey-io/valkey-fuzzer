"""
Test that chaos selection is deterministic when using a seed
"""

from unittest.mock import Mock, patch

import pytest

from src.fuzzer_engine.chaos_coordinator import ChaosCoordinator
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosTiming,
    ChaosType,
    NodeInfo,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    TargetSelection,
)


@pytest.fixture
def cluster_nodes():
    """Create a list of test cluster nodes."""
    return [
        NodeInfo(
            node_id="node_0",
            role="primary",
            shard_id=0,
            port=7000,
            bus_port=17000,
            pid=1000,
            process=Mock(),
            data_dir="/tmp/node_0",
            log_file="/tmp/node_0.log",
        ),
        NodeInfo(
            node_id="node_1",
            role="primary",
            shard_id=1,
            port=7001,
            bus_port=17001,
            pid=1001,
            process=Mock(),
            data_dir="/tmp/node_1",
            log_file="/tmp/node_1.log",
        ),
        NodeInfo(
            node_id="node_2",
            role="primary",
            shard_id=2,
            port=7002,
            bus_port=17002,
            pid=1002,
            process=Mock(),
            data_dir="/tmp/node_2",
            log_file="/tmp/node_2.log",
        ),
        NodeInfo(
            node_id="node_3",
            role="replica",
            shard_id=0,
            port=7003,
            bus_port=17003,
            pid=1003,
            process=Mock(),
            data_dir="/tmp/node_3",
            log_file="/tmp/node_3.log",
        ),
        NodeInfo(
            node_id="node_4",
            role="replica",
            shard_id=1,
            port=7004,
            bus_port=17004,
            pid=1004,
            process=Mock(),
            data_dir="/tmp/node_4",
            log_file="/tmp/node_4.log",
        ),
        NodeInfo(
            node_id="node_5",
            role="replica",
            shard_id=2,
            port=7005,
            bus_port=17005,
            pid=1005,
            process=Mock(),
            data_dir="/tmp/node_5",
            log_file="/tmp/node_5.log",
        ),
    ]


@pytest.fixture
def mock_cluster_connection(cluster_nodes):
    """Create a mock cluster connection that returns cluster nodes."""
    mock_conn = Mock()
    live_nodes_dict = [
        {
            "node_id": node.node_id,
            "host": "127.0.0.1",
            "port": node.port,
            "role": node.role,
            "shard_id": node.shard_id,
            "status": "connected",
        }
        for node in cluster_nodes
    ]
    mock_conn.get_live_nodes.return_value = live_nodes_dict
    mock_conn.initial_nodes = cluster_nodes
    mock_conn.cluster_id = "test_cluster"
    return mock_conn


@patch("src.fuzzer_engine.chaos_coordinator.time.sleep")
def test_seeded_chaos_coordinator_is_deterministic(mock_sleep, cluster_nodes, mock_cluster_connection):
    """Test that using the same seed produces identical chaos selections"""

    # Create two coordinators with the same seed
    coordinator1 = ChaosCoordinator(seed=42)
    coordinator2 = ChaosCoordinator(seed=42)

    # Track chaos injections
    chaos_injections_1 = []
    chaos_injections_2 = []

    def capture_chaos_1(node, chaos_type, **kwargs):
        chaos_injections_1.append({"node_id": node.node_id, "chaos_type": chaos_type})
        return Mock(
            success=True,
            chaos_id=f"test_{len(chaos_injections_1)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            start_time=0,
            end_time=1,
        )

    def capture_chaos_2(node, chaos_type, **kwargs):
        chaos_injections_2.append({"node_id": node.node_id, "chaos_type": chaos_type})
        return Mock(
            success=True,
            chaos_id=f"test_{len(chaos_injections_2)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            start_time=0,
            end_time=1,
        )

    coordinator1.chaos_engine.inject_process_chaos = capture_chaos_1
    coordinator2.chaos_engine.inject_process_chaos = capture_chaos_2

    # Create chaos config with randomization enabled
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(delay_before_operation=0.1, delay_after_operation=0.1),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=None,  # Will be randomized
        randomize_per_operation=True,
    )

    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node_0",
        parameters={"test_key": "test_value"},
        timing=OperationTiming(),
    )

    # Execute same operations on both coordinators
    for _ in range(10):
        coordinator1.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster",
        )
        coordinator2.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster",
        )

    # Both should have same number of injections
    assert len(chaos_injections_1) == len(chaos_injections_2)

    # Both should have selected the same nodes in the same order
    for i, (inj1, inj2) in enumerate(zip(chaos_injections_1, chaos_injections_2)):
        assert inj1["node_id"] == inj2["node_id"], (
            f"Injection {i}: node_id mismatch ({inj1['node_id']} vs {inj2['node_id']})"
        )
        assert inj1["chaos_type"] == inj2["chaos_type"], (
            f"Injection {i}: chaos_type mismatch ({inj1['chaos_type']} vs {inj2['chaos_type']})"
        )

    print("\n=== Determinism Test Results ===")
    print(f"Total injections: {len(chaos_injections_1)}")
    print(f"Nodes selected (coordinator 1): {[inj['node_id'] for inj in chaos_injections_1]}")
    print(f"Nodes selected (coordinator 2): {[inj['node_id'] for inj in chaos_injections_2]}")
    print(f"Chaos types (coordinator 1): {[inj['chaos_type'] for inj in chaos_injections_1]}")
    print(f"Chaos types (coordinator 2): {[inj['chaos_type'] for inj in chaos_injections_2]}")


@patch("src.fuzzer_engine.chaos_coordinator.time.sleep")
def test_different_seeds_produce_different_chaos(mock_sleep, cluster_nodes, mock_cluster_connection):
    """Test that different seeds produce different chaos selections"""

    # Create two coordinators with different seeds
    coordinator1 = ChaosCoordinator(seed=42)
    coordinator2 = ChaosCoordinator(seed=99)

    # Track chaos injections
    chaos_injections_1 = []
    chaos_injections_2 = []

    def capture_chaos_1(node, chaos_type, **kwargs):
        chaos_injections_1.append({"node_id": node.node_id, "chaos_type": chaos_type})
        return Mock(
            success=True,
            chaos_id=f"test_{len(chaos_injections_1)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            start_time=0,
            end_time=1,
        )

    def capture_chaos_2(node, chaos_type, **kwargs):
        chaos_injections_2.append({"node_id": node.node_id, "chaos_type": chaos_type})
        return Mock(
            success=True,
            chaos_id=f"test_{len(chaos_injections_2)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            start_time=0,
            end_time=1,
        )

    coordinator1.chaos_engine.inject_process_chaos = capture_chaos_1
    coordinator2.chaos_engine.inject_process_chaos = capture_chaos_2

    # Create chaos config with randomization enabled
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(delay_before_operation=0.1, delay_after_operation=0.1),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=None,  # Will be randomized
        randomize_per_operation=True,
    )

    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node_0",
        parameters={"test_key": "test_value"},
        timing=OperationTiming(),
    )

    # Execute same operations on both coordinators
    for _ in range(10):
        coordinator1.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster",
        )
        coordinator2.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster",
        )

    # Both should have injections (may differ in count due to randomized timing)
    assert len(chaos_injections_1) > 0
    assert len(chaos_injections_2) > 0

    # At least some selections should be different (very high probability)
    nodes_1 = [inj["node_id"] for inj in chaos_injections_1]
    nodes_2 = [inj["node_id"] for inj in chaos_injections_2]

    # With different seeds, it's extremely unlikely the first few selections are all identical
    # Compare first N selections where N is the minimum length
    min_len = min(len(nodes_1), len(nodes_2))
    first_nodes_1 = nodes_1[:min_len]
    first_nodes_2 = nodes_2[:min_len]

    assert first_nodes_1 != first_nodes_2, "Different seeds should produce different node selections"

    print("\n=== Different Seeds Test Results ===")
    print(f"Nodes selected (seed 42): {nodes_1}")
    print(f"Nodes selected (seed 99): {nodes_2}")


@patch("src.fuzzer_engine.chaos_coordinator.time.sleep")
def test_target_selector_uses_seeded_rng(mock_sleep, cluster_nodes, mock_cluster_connection):
    """Test that the target selector uses the seeded RNG for node selection"""

    coordinator = ChaosCoordinator(seed=12345)

    # Track selected nodes
    selected_nodes = []

    def capture_chaos(node, chaos_type, **kwargs):
        selected_nodes.append(node.node_id)
        return Mock(
            success=True,
            chaos_id=f"test_{len(selected_nodes)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            start_time=0,
            end_time=1,
        )

    coordinator.chaos_engine.inject_process_chaos = capture_chaos

    # Create chaos config WITHOUT randomization (deterministic target selection only)
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(delay_before_operation=0.0, delay_after_operation=0.0),
        coordination=ChaosCoordination(chaos_during_operation=True),
        process_chaos_type=ProcessChaosType.SIGKILL,
        randomize_per_operation=False,  # No randomization, just target selection
    )

    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node_0",
        parameters={"test_key": "test_value"},
        timing=OperationTiming(),
    )

    # Execute multiple operations
    for _ in range(5):
        coordinator.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster",
        )

    # Create a new coordinator with the same seed
    coordinator2 = ChaosCoordinator(seed=12345)
    selected_nodes_2 = []

    def capture_chaos_2(node, chaos_type, **kwargs):
        selected_nodes_2.append(node.node_id)
        return Mock(
            success=True,
            chaos_id=f"test_{len(selected_nodes_2)}",
            chaos_type=ChaosType.PROCESS_KILL,
            target_node=node.node_id,
            start_time=0,
            end_time=1,
        )

    coordinator2.chaos_engine.inject_process_chaos = capture_chaos_2

    # Execute same operations
    for _ in range(5):
        coordinator2.coordinate_chaos_with_operation(
            operation=operation,
            chaos_config=chaos_config,
            cluster_connection=mock_cluster_connection,
            cluster_id="test_cluster",
        )

    # Should select the same nodes in the same order
    assert selected_nodes == selected_nodes_2, (
        f"Target selection should be deterministic with same seed: {selected_nodes} vs {selected_nodes_2}"
    )

    print("\n=== Target Selector Determinism ===")
    print(f"Selected nodes (run 1): {selected_nodes}")
    print(f"Selected nodes (run 2): {selected_nodes_2}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
