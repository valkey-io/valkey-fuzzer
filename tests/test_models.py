"""
Tests for core data models
"""
import pytest
from src.models import (
    ClusterConfig, NodePlan, NodeInfo, OperationType, ChaosType,
    Scenario, Operation, OperationTiming, ChaosConfig,
    TargetSelection, ChaosTiming, ChaosCoordination
)


def test_cluster_config_creation():
    """Test cluster configuration creation"""
    config = ClusterConfig(num_shards=3, replicas_per_shard=1)
    assert config.num_shards == 3
    assert config.replicas_per_shard == 1
    assert config.base_port == 7000
    assert config.enable_cleanup == True


def test_node_plan_creation():
    """Test node plan creation"""
    # Primary node
    primary = NodePlan("node1", "primary", 0, 7000, 17000, 0, 5461)
    assert primary.node_id == "node1"
    assert primary.role == "primary"
    assert primary.shard_id == 0
    assert primary.slot_start == 0
    assert primary.slot_end == 5461
    
    # Replica node
    replica = NodePlan("node2", "replica", 0, 7001, 17001, master_node_id="node1")
    assert replica.node_id == "node2"
    assert replica.role == "replica"
    assert replica.master_node_id == "node1"


def test_operation_creation():
    """Test operation creation"""
    timing = OperationTiming(delay_before=1.0, timeout=30.0, delay_after=2.0)
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node1",
        parameters={"force": True},
        timing=timing
    )
    
    assert operation.type == OperationType.FAILOVER
    assert operation.target_node == "node1"
    assert operation.parameters["force"] is True
    assert operation.timing.delay_before == 1.0


def test_chaos_config_creation():
    """Test chaos configuration creation"""
    target_selection = TargetSelection(strategy="random")
    timing = ChaosTiming(delay_before_operation=1.0, chaos_duration=10.0)
    coordination = ChaosCoordination(chaos_during_operation=True)
    
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=target_selection,
        timing=timing,
        coordination=coordination
    )
    
    assert chaos_config.chaos_type == ChaosType.PROCESS_KILL
    assert chaos_config.target_selection.strategy == "random"
    assert chaos_config.timing.chaos_duration == 10.0
    assert chaos_config.coordination.chaos_during_operation is True


def test_scenario_creation():
    """Test complete test scenario creation"""
    # Create cluster config
    cluster_config = ClusterConfig(num_shards=3, replicas_per_shard=0)
    
    # Create operation
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node1",
        parameters={},
        timing=OperationTiming()
    )
    
    # Create chaos config
    chaos_config = ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination()
    )
    
    # Create test scenario
    scenario = Scenario(
        scenario_id="test-001",
        cluster_config=cluster_config,
        operations=[operation],
        chaos_config=chaos_config,
        seed=12345
    )
    
    assert scenario.scenario_id == "test-001"
    assert scenario.seed == 12345
    assert len(scenario.operations) == 1
    assert scenario.operations[0].type == OperationType.FAILOVER


if __name__ == "__main__":
    pytest.main([__file__])