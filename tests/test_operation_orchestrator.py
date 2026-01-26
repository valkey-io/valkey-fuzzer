"""
Tests for Operation Orchestrator
"""
import pytest
from unittest.mock import Mock, patch
from src.fuzzer_engine.operation_orchestrator import OperationOrchestrator
from src.models import (Operation, OperationType, OperationTiming, NodeInfo, ClusterConnection)


def test_operation_orchestrator_initialization():
    """Test operation orchestrator initialization"""
    orchestrator = OperationOrchestrator()
    
    assert orchestrator.cluster_connection is None
    assert orchestrator.active_operations == {}
    assert orchestrator.operation_counter == 0


def test_set_cluster_connection():
    """Test setting cluster connection"""
    orchestrator = OperationOrchestrator()
    
    # Create mock cluster connection
    nodes = [
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
            slot_end=5461
        )
    ]
    connection = ClusterConnection(nodes, "test-cluster")
    
    orchestrator.set_cluster_connection(connection)
    assert orchestrator.cluster_connection == connection


def test_execute_operation_no_cluster_connection():
    """Test executing operation without cluster connection"""
    orchestrator = OperationOrchestrator()
    
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-0",
        parameters={},
        timing=OperationTiming()
    )
    
    result = orchestrator.execute_operation(operation)
    assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])



def test_execute_failover_exact_node_id_match():
    """Test that failover uses exact node_id matching, not substring"""
    orchestrator = OperationOrchestrator()
    
    # Create mock cluster connection with node-1 and node-10
    mock_connection = Mock()
    nodes_list = [
        {
            'node_id': 'node-1',
            'host': '127.0.0.1',
            'port': 7001,
            'role': 'primary',
            'shard_id': 0
        },
        {
            'node_id': 'node-10',
            'host': '127.0.0.1',
            'port': 7010,
            'role': 'primary',
            'shard_id': 1
        },
        {
            'node_id': 'node-11',
            'host': '127.0.0.1',
            'port': 7011,
            'role': 'primary',
            'shard_id': 2
        }
    ]
    mock_connection.get_current_nodes.return_value = nodes_list
    mock_connection.get_live_nodes.return_value = nodes_list
    
    orchestrator.set_cluster_connection(mock_connection)
    
    # Create operation targeting node-1
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-1",
        parameters={},
        timing=OperationTiming()
    )
    
    # Mock the Valkey client
    with patch('src.utils.valkey_utils.valkey.Valkey') as mock_valkey:
        mock_client = Mock()
        mock_valkey.return_value = mock_client
        
        # Mock CLUSTER NODES response with a replica for node-1
        cluster_nodes_response = (
            "node-1 127.0.0.1:7001@17001 master - 0 0 1 connected 0-5460\n"
            "replica-1 127.0.0.1:7002@17002 slave node-1 0 0 2 connected\n"
            "node-10 127.0.0.1:7010@17010 master - 0 0 3 connected 5461-10922\n"
            "node-11 127.0.0.1:7011@17011 master - 0 0 4 connected 10923-16383\n"
        )
        
        # First call: CLUSTER NODES (to find replicas)
        # Second call: CLUSTER FAILOVER (on the replica)
        mock_client.execute_command.side_effect = [
            cluster_nodes_response,  # CLUSTER NODES response
            b'OK'  # CLUSTER FAILOVER response
        ]
        
        # Execute failover
        result = orchestrator._execute_failover(operation)
        
        # Verify it connected to the correct port (7001, not 7010 or 7011)
        # The first call should be to port 7001 (the target primary)
        first_call = mock_valkey.call_args_list[0]
        assert first_call.kwargs['host'] == '127.0.0.1'
        assert first_call.kwargs['port'] == 7001


def test_execute_failover_no_substring_false_positive():
    """Test that substring matching doesn't cause false positives in failover"""
    orchestrator = OperationOrchestrator()
    
    # Create mock cluster connection with only node-10, node-11, node-12
    mock_connection = Mock()
    nodes_list = [
        {
            'node_id': 'node-10',
            'host': '127.0.0.1',
            'port': 7010,
            'role': 'primary',
            'shard_id': 1
        },
        {
            'node_id': 'node-11',
            'host': '127.0.0.1',
            'port': 7011,
            'role': 'primary',
            'shard_id': 2
        },
        {
            'node_id': 'node-12',
            'host': '127.0.0.1',
            'port': 7012,
            'role': 'primary',
            'shard_id': 3
        }
    ]
    mock_connection.get_current_nodes.return_value = nodes_list
    mock_connection.get_live_nodes.return_value = nodes_list
    
    orchestrator.set_cluster_connection(mock_connection)
    
    # Create operation targeting node-1 (which doesn't exist)
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="node-1",
        parameters={},
        timing=OperationTiming()
    )
    
    # Execute failover - should fail because node-1 doesn't exist
    result = orchestrator._execute_failover(operation)
    
    # Should return False (not found)
    assert result is False


def test_execute_failover_shard_based_matching():
    """Test that failover can match by shard-X-primary format"""
    orchestrator = OperationOrchestrator()
    
    # Create mock cluster connection with nodes having shard_id
    mock_connection = Mock()
    nodes_list = [
        {
            'node_id': 'node-0',
            'host': '127.0.0.1',
            'port': 6379,
            'role': 'primary',
            'shard_id': 0
        },
        {
            'node_id': 'node-30',
            'host': '127.0.0.1',
            'port': 6409,
            'role': 'primary',
            'shard_id': 10
        },
        {
            'node_id': 'node-31',
            'host': '127.0.0.1',
            'port': 6410,
            'role': 'replica',
            'shard_id': 10
        }
    ]
    mock_connection.get_current_nodes.return_value = nodes_list
    mock_connection.get_live_nodes.return_value = nodes_list
    # Mock find_alive_node to return the replica
    mock_connection.find_alive_node.return_value = nodes_list[2]  # Return the replica
    
    orchestrator.set_cluster_connection(mock_connection)
    
    # Create operation targeting shard-10-primary
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="shard-10-primary",
        parameters={},
        timing=OperationTiming()
    )
    
    # Mock the Valkey client
    with patch('src.utils.valkey_utils.valkey.Valkey') as mock_valkey:
        mock_client = Mock()
        mock_valkey.return_value = mock_client
        # Mock CLUSTER FAILOVER response
        mock_client.execute_command.return_value = b'OK'
        
        # Execute failover
        result = orchestrator._execute_failover(operation)
        
        # Verify it found the replica by shard_id and connected to it (port 6410)
        # When shard_id is available, we use Strategy 1 (direct shard_id matching)
        # and skip querying nodes, going straight to the replica
        call_args_list = mock_valkey.call_args_list
        # Should connect to the replica (6410) to execute CLUSTER FAILOVER
        assert call_args_list[0].kwargs['port'] == 6410


def test_execute_failover_port_matching():
    """Test that failover can match by exact port number"""
    orchestrator = OperationOrchestrator()
    
    # Create mock cluster connection
    mock_connection = Mock()
    nodes_list = [
        {
            'node_id': 'abc123',
            'host': '127.0.0.1',
            'port': 7001,
            'role': 'primary',
            'shard_id': 0
        },
        {
            'node_id': 'replica123',
            'host': '127.0.0.1',
            'port': 7002,
            'role': 'replica',
            'shard_id': 0
        },
        {
            'node_id': 'def456',
            'host': '127.0.0.1',
            'port': 7010,
            'role': 'primary',
            'shard_id': 1
        }
    ]
    mock_connection.get_current_nodes.return_value = nodes_list
    mock_connection.get_live_nodes.return_value = nodes_list
    # Mock find_alive_node to return the replica
    mock_connection.find_alive_node.return_value = nodes_list[1]  # Return the replica
    
    orchestrator.set_cluster_connection(mock_connection)
    
    # Create operation targeting by port
    operation = Operation(
        type=OperationType.FAILOVER,
        target_node="7001",
        parameters={},
        timing=OperationTiming()
    )
    
    # Mock the Valkey client
    with patch('src.utils.valkey_utils.valkey.Valkey') as mock_valkey:
        mock_client = Mock()
        mock_valkey.return_value = mock_client
        # Mock CLUSTER FAILOVER response
        mock_client.execute_command.return_value = b'OK'
        
        # Execute failover
        result = orchestrator._execute_failover(operation)
        
        # Verify it found the replica by shard_id and connected to it
        # When shard_id is available, we use Strategy 1 (direct shard_id matching)
        call_args_list = mock_valkey.call_args_list
        # Should connect to the replica (7002) to execute CLUSTER FAILOVER
        assert call_args_list[0].kwargs['host'] == '127.0.0.1'
        assert call_args_list[0].kwargs['port'] == 7002
