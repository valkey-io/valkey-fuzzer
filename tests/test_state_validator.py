"""
Tests for State Validator
"""
import pytest
import time
from src.fuzzer_engine.state_validator import StateValidator
from src.models import (
    StateValidationConfig, ClusterStatus, NodeInfo
)


def test_killed_nodes_tracking():
    """Test that StateValidator tracks killed nodes correctly"""
    config = StateValidationConfig()
    validator = StateValidator(config)
    
    assert len(validator.killed_nodes) == 0
    
    validator.register_killed_node("127.0.0.1:6379", "primary", 0)
    validator.register_killed_node("127.0.0.1:6380", "replica", 1)
    
    assert len(validator.killed_nodes) == 2
    validator.clear_killed_nodes()
    assert len(validator.killed_nodes) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def test_topology_validation_zero_replicas_with_killed_primary():
    """Test that topology validation handles zero-replica clusters correctly when primaries are killed"""
    from src.fuzzer_engine.state_validator import TopologyValidator
    from src.models import TopologyValidationConfig, ExpectedTopology
    from unittest.mock import Mock
    
    config = TopologyValidationConfig()
    validator = TopologyValidator()
    
    # Mock cluster connection with zero replicas
    mock_connection = Mock()
    
    # Simulate a 3-primary, 0-replica cluster where 1 primary was killed
    mock_nodes = [
        {'node_id': 'node1', 'host': '127.0.0.1', 'port': 7000, 'role': 'primary', 'status': 'ok'},
        {'node_id': 'node2', 'host': '127.0.0.1', 'port': 7001, 'role': 'primary', 'status': 'ok'},
        {'node_id': 'node3', 'host': '127.0.0.1', 'port': 7002, 'role': 'primary', 'status': 'failed'},
    ]
    mock_connection.get_current_nodes.return_value = mock_nodes
    
    # Expected topology: 3 primaries, 0 replicas
    expected_topology = ExpectedTopology(
        num_primaries=3,
        num_replicas=0,
        shard_structure=None
    )
    
    # Killed nodes set (node3 was killed)
    killed_nodes = {'127.0.0.1:7002'}
    
    # Validate topology
    result = validator.validate(
        cluster_connection=mock_connection,
        config=config,
        expected_topology=expected_topology,
        killed_nodes=killed_nodes
    )
    
    # The adjusted expected replica count should be clamped at 0, not -1
    # With 0 replicas initially and 1 killed node, adjusted = max(0, 0 - 1) = 0
    assert result.expected_replicas == 0, "Expected replicas should be clamped at 0, not negative"
    assert result.actual_replicas == 0, "Actual replicas should be 0"
    
    # Should succeed since actual matches expected (both 0)
    assert result.success is True, "Topology validation should succeed for zero-replica cluster with killed primary"


def test_topology_validation_zero_replicas_multiple_killed():
    """Test that topology validation handles zero-replica clusters when multiple primaries are killed"""
    from src.fuzzer_engine.state_validator import TopologyValidator
    from src.models import TopologyValidationConfig, ExpectedTopology
    from unittest.mock import Mock
    
    config = TopologyValidationConfig()
    validator = TopologyValidator()
    
    # Mock cluster connection with zero replicas
    mock_connection = Mock()
    
    # Simulate a 3-primary, 0-replica cluster where 2 primaries were killed
    mock_nodes = [
        {'node_id': 'node1', 'host': '127.0.0.1', 'port': 7000, 'role': 'primary', 'status': 'ok'},
        {'node_id': 'node2', 'host': '127.0.0.1', 'port': 7001, 'role': 'primary', 'status': 'failed'},
        {'node_id': 'node3', 'host': '127.0.0.1', 'port': 7002, 'role': 'primary', 'status': 'failed'},
    ]
    mock_connection.get_current_nodes.return_value = mock_nodes
    
    # Expected topology: 3 primaries, 0 replicas
    expected_topology = ExpectedTopology(
        num_primaries=3,
        num_replicas=0,
        shard_structure=None
    )
    
    # Killed nodes set (node2 and node3 were killed)
    killed_nodes = {'127.0.0.1:7001', '127.0.0.1:7002'}
    
    # Validate topology
    result = validator.validate(
        cluster_connection=mock_connection,
        config=config,
        expected_topology=expected_topology,
        killed_nodes=killed_nodes
    )
    
    # The adjusted expected replica count should be clamped at 0, not -2
    # With 0 replicas initially and 2 killed nodes, adjusted = max(0, 0 - 2) = 0
    assert result.expected_replicas == 0, "Expected replicas should be clamped at 0, not negative"
    assert result.actual_replicas == 0, "Actual replicas should be 0"
    
    # Should succeed since actual matches expected (both 0)
    assert result.success is True, "Topology validation should succeed for zero-replica cluster with multiple killed primaries"


def test_validate_cluster_state_zero_replicas():
    """Test that validate_cluster_state correctly handles zero-replica clusters"""
    from src.fuzzer_engine.fuzzer_engine import FuzzerEngine
    from src.models import ClusterConfig
    from unittest.mock import Mock, MagicMock, patch
    
    # Create a fuzzer instance
    fuzzer = FuzzerEngine()
    
    # Mock cluster instance with zero replicas
    mock_cluster_instance = Mock()
    # Create mock nodes with just the role attribute we need
    mock_node1 = Mock(role='primary')
    mock_node2 = Mock(role='primary')
    mock_node3 = Mock(role='primary')
    mock_cluster_instance.nodes = [mock_node1, mock_node2, mock_node3]
    
    # Mock cluster coordinator
    fuzzer.cluster_coordinator = Mock()
    fuzzer.cluster_coordinator.active_clusters = {
        'test-cluster': {
            'instance': mock_cluster_instance
        }
    }
    
    # Mock the ClusterConnection and validator
    with patch('src.fuzzer_engine.fuzzer_engine.ClusterConnection') as mock_conn_class, \
         patch('src.fuzzer_engine.fuzzer_engine.StateValidator') as mock_validator_class:
        
        mock_connection = Mock()
        mock_conn_class.return_value = mock_connection
        
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        
        # Mock validation result
        from src.models import StateValidationResult
        mock_result = StateValidationResult(
            overall_success=True,
            validation_timestamp=time.time(),
            validation_duration=1.0,
            replication=None,
            cluster_status=None,
            slot_coverage=None,
            topology=None,
            view_consistency=None,
            data_consistency=None,
            failed_checks=[],
            error_messages=[]
        )
        mock_validator.validate_state.return_value = mock_result
        
        # Call validate_cluster_state
        result = fuzzer.validate_cluster_state('test-cluster')
        
        # Verify StateValidator was created with correct config
        assert mock_validator_class.called
        config = mock_validator_class.call_args[0][0]
        
        # Verify min_replicas_per_shard was set to 0 for zero-replica cluster
        assert config.replication_config.min_replicas_per_shard == 0, \
            "min_replicas_per_shard should be 0 for zero-replica cluster"
        
        # Verify data consistency check was disabled
        assert config.check_data_consistency is False, \
            "Data consistency check should be disabled for standalone validation"
        
        # Verify validation was called with correct expected topology
        assert mock_validator.validate_state.called
        call_args = mock_validator.validate_state.call_args
        expected_topology = call_args[0][1]
        assert expected_topology.num_primaries == 3
        assert expected_topology.num_replicas == 0


def test_validate_cluster_state_with_replicas():
    """Test that validate_cluster_state correctly handles clusters with replicas"""
    from src.fuzzer_engine.fuzzer_engine import FuzzerEngine
    from src.models import ClusterConfig
    from unittest.mock import Mock, MagicMock, patch
    
    # Create a fuzzer instance
    fuzzer = FuzzerEngine()
    
    # Mock cluster instance with replicas (3 primaries, 3 replicas = 1 replica per shard)
    mock_cluster_instance = Mock()
    # Create mock nodes with just the role attribute we need
    mock_nodes = [
        Mock(role='primary'),
        Mock(role='primary'),
        Mock(role='primary'),
        Mock(role='replica'),
        Mock(role='replica'),
        Mock(role='replica'),
    ]
    mock_cluster_instance.nodes = mock_nodes
    
    # Mock cluster coordinator
    fuzzer.cluster_coordinator = Mock()
    fuzzer.cluster_coordinator.active_clusters = {
        'test-cluster': {
            'instance': mock_cluster_instance
        }
    }
    
    # Mock the ClusterConnection and validator
    with patch('src.fuzzer_engine.fuzzer_engine.ClusterConnection') as mock_conn_class, \
         patch('src.fuzzer_engine.fuzzer_engine.StateValidator') as mock_validator_class:
        
        mock_connection = Mock()
        mock_conn_class.return_value = mock_connection
        
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        
        # Mock validation result
        from src.models import StateValidationResult
        mock_result = StateValidationResult(
            overall_success=True,
            validation_timestamp=time.time(),
            validation_duration=1.0,
            replication=None,
            cluster_status=None,
            slot_coverage=None,
            topology=None,
            view_consistency=None,
            data_consistency=None,
            failed_checks=[],
            error_messages=[]
        )
        mock_validator.validate_state.return_value = mock_result
        
        # Call validate_cluster_state
        result = fuzzer.validate_cluster_state('test-cluster')
        
        # Verify StateValidator was created with correct config
        assert mock_validator_class.called
        config = mock_validator_class.call_args[0][0]
        
        # Verify min_replicas_per_shard was set to 1 (3 replicas / 3 primaries)
        assert config.replication_config.min_replicas_per_shard == 1, \
            "min_replicas_per_shard should be 1 for cluster with 1 replica per shard"
        
        # Verify validation was called with correct expected topology
        assert mock_validator.validate_state.called
        call_args = mock_validator.validate_state.call_args
        expected_topology = call_args[0][1]
        assert expected_topology.num_primaries == 3
        assert expected_topology.num_replicas == 3
