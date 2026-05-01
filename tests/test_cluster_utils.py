import pytest

from src.utils.cluster_utils import (
    find_node_by_identifier,
    find_primary_node_by_identifier,
    find_replica_nodes_by_primary,
    group_nodes_by_shard,
)


@pytest.fixture
def sample_nodes():
    """Sample cluster nodes for testing"""
    return [
        {"node_id": "abc123", "host": "127.0.0.1", "port": 6379, "role": "primary", "shard_id": 0},
        {
            "node_id": "def456",
            "host": "127.0.0.1",
            "port": 6380,
            "role": "replica",
            "shard_id": 0,
            "master_id": "abc123",
        },
        {
            "node_id": "ghi789",
            "host": "127.0.0.1",
            "port": 6381,
            "role": "replica",
            "shard_id": 0,
            "master_id": "abc123",
        },
        {"node_id": "jkl012", "host": "127.0.0.1", "port": 6382, "role": "primary", "shard_id": 1},
        {
            "node_id": "mno345",
            "host": "127.0.0.1",
            "port": 6383,
            "role": "replica",
            "shard_id": 1,
            "master_id": "jkl012",
        },
    ]


def test_find_node_by_node_id(sample_nodes):
    """Test finding node by exact node_id"""
    node = find_node_by_identifier(sample_nodes, "abc123")
    assert node is not None
    assert node["node_id"] == "abc123"
    assert node["port"] == 6379


def test_find_node_by_port(sample_nodes):
    """Test finding node by port number"""
    node = find_node_by_identifier(sample_nodes, "6380")
    assert node is not None
    assert node["port"] == 6380


def test_find_node_by_shard_pattern_primary(sample_nodes):
    """Test finding node by shard pattern (shard-X-primary)"""
    node = find_node_by_identifier(sample_nodes, "shard-0-primary")
    assert node is not None
    assert node["role"] == "primary"
    assert node["shard_id"] == 0
    assert node["port"] == 6379


def test_find_node_by_shard_pattern_replica(sample_nodes):
    """Test finding node by shard pattern (shard-X-replica)"""
    node = find_node_by_identifier(sample_nodes, "shard-1-replica")
    assert node is not None
    assert node["role"] == "replica"
    assert node["shard_id"] == 1


def test_find_node_by_shard_pattern_master_alias(sample_nodes):
    """Test finding node by shard pattern with 'master' alias"""
    node = find_node_by_identifier(sample_nodes, "shard-1-master")
    assert node is not None
    assert node["role"] == "primary"
    assert node["shard_id"] == 1


def test_find_node_not_found(sample_nodes):
    """Test that None is returned when node is not found"""
    assert find_node_by_identifier(sample_nodes, "nonexistent") is None
    assert find_node_by_identifier(sample_nodes, "shard-99-primary") is None


def test_find_node_with_specific_strategies(sample_nodes):
    """Test finding node with specific match strategies"""
    # Only try port matching
    node = find_node_by_identifier(sample_nodes, "6379", match_strategies=["port"])
    assert node is not None
    assert node["port"] == 6379

    # Only try shard pattern matching
    node = find_node_by_identifier(sample_nodes, "shard-0-primary", match_strategies=["shard_pattern"])
    assert node is not None
    assert node["shard_id"] == 0


def test_find_node_empty_list():
    """Test finding node in empty list"""
    node = find_node_by_identifier([], "anything")
    assert node is None


def test_find_primary_node_success(sample_nodes):
    """Test finding primary node"""
    node = find_primary_node_by_identifier(sample_nodes, "shard-0-primary")
    assert node is not None
    assert node["role"] == "primary"
    assert node["shard_id"] == 0


def test_find_primary_node_rejects_replica(sample_nodes):
    """Test that finding primary node rejects replica nodes"""
    node = find_primary_node_by_identifier(sample_nodes, "def456")
    assert node is None  # def456 is a replica, not a primary


def test_find_primary_node_by_port(sample_nodes):
    """Test finding primary node by port"""
    node = find_primary_node_by_identifier(sample_nodes, "6379")
    assert node is not None
    assert node["role"] == "primary"
    assert node["port"] == 6379


def test_find_replica_nodes_by_shard_id(sample_nodes):
    """Test finding replicas by shard_id"""
    replicas = find_replica_nodes_by_primary(sample_nodes, "abc123", primary_shard_id=0)
    assert len(replicas) == 2
    assert all(r["role"] == "replica" for r in replicas)
    assert all(r["shard_id"] == 0 for r in replicas)


def test_find_replica_nodes_by_master_id(sample_nodes):
    """Test finding replicas by master_id"""
    replicas = find_replica_nodes_by_primary(sample_nodes, "jkl012")
    assert len(replicas) == 1
    assert replicas[0]["role"] == "replica"
    assert replicas[0]["master_id"] == "jkl012"


def test_find_replica_nodes_no_replicas(sample_nodes):
    """Test finding replicas when none exist"""
    replicas = find_replica_nodes_by_primary(sample_nodes, "nonexistent")
    assert len(replicas) == 0


def test_group_nodes_by_shard(sample_nodes):
    """Test grouping nodes by shard"""
    shards = group_nodes_by_shard(sample_nodes)

    assert len(shards) == 2

    # Shard 0
    assert 0 in shards
    assert shards[0]["primary"] is not None
    assert shards[0]["primary"]["node_id"] == "abc123"
    assert len(shards[0]["replicas"]) == 2

    # Shard 1
    assert 1 in shards
    assert shards[1]["primary"] is not None
    assert shards[1]["primary"]["node_id"] == "jkl012"
    assert len(shards[1]["replicas"]) == 1


def test_group_nodes_by_shard_edge_cases():
    """Test grouping with edge cases"""
    assert len(group_nodes_by_shard([])) == 0
    assert len(group_nodes_by_shard([{"node_id": "abc", "role": "primary"}])) == 0


def test_find_node_priority_order(sample_nodes):
    """Test that node_id matching takes priority over port matching"""
    # Add a node where node_id looks like a port
    nodes_with_conflict = [
        *sample_nodes,
        {
            "node_id": "6379",  # node_id that looks like a port
            "host": "127.0.0.1",
            "port": 9999,
            "role": "primary",
            "shard_id": 2,
        },
    ]

    # Should match by node_id first (port 9999), not by port (port 6379)
    node = find_node_by_identifier(nodes_with_conflict, "6379")
    assert node is not None
    assert node["port"] == 9999  # Matched by node_id, not port
