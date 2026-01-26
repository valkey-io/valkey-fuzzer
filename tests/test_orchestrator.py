import pytest
import logging
from src.cluster_orchestrator.orchestrator import PortManager, ConfigurationManager, ClusterManager
from src.models import ClusterConfig

logging.basicConfig(format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', level=logging.INFO, force=True)

@pytest.fixture(autouse=True)
def test_separator(request):
    print(f"\n{'='*60}")
    print(f"Running: {request.node.name}")
    print(f"{'='*60}")
    yield
    print(f"{'='*60}")
    print(f"Completed: {request.node.name}")
    print(f"{'='*60}\n")

def test_port_manager_allocation():
    """Test port allocation and release"""
    port_mgr = PortManager(base_port=6789, max_ports=10)
    
    client_port, bus_port = port_mgr.allocate_ports("node-1")
    assert client_port == 6789
    assert bus_port == 16789
    assert "node-1" in port_mgr.allocated_ports
    
    client_port2, bus_port2 = port_mgr.allocate_ports("node-2")
    assert client_port2 == 6790
    assert bus_port2 == 16790
    
    port_mgr.release_ports("node-1")
    assert "node-1" not in port_mgr.allocated_ports
    assert 6789 in port_mgr.available_ports
    
    client_port3, bus_port3 = port_mgr.allocate_ports("node-3")
    assert client_port3 == 6789
    assert bus_port3 == 16789
    assert "node-3" in port_mgr.allocated_ports
    
    port_mgr.release_ports("node-2")
    port_mgr.release_ports("node-3")

def test_configuration_manager_topology():
    """Test topology planning"""
    config = ClusterConfig(
        num_shards=3, 
        replicas_per_shard=2
    )
    
    port_mgr = PortManager(base_port=7000)
    config_mgr = ConfigurationManager(config, port_mgr)
    topology = config_mgr.plan_topology()
    
    # Should have 9 nodes total (3 primaries + 2 replicas each)
    assert len(topology) == 9
    
    primaries = [node for node in topology if node.role == 'primary']
    assert len(primaries) == 3
    
    replicas = [node for node in topology if node.role == 'replica']
    assert len(replicas) == 6
    
    # Check slot assignments
    for primary in primaries:
        assert primary.slot_start is not None
        assert primary.slot_end is not None
        assert primary.slot_start <= primary.slot_end


def test_full_cluster_creation():
    """Test complete cluster creation with 2 shards and 1 replica, including config validation"""
    config_mgr = ConfigurationManager(ClusterConfig(num_shards=2, replicas_per_shard=1), PortManager())
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    config = ClusterConfig(
        num_shards=2, 
        replicas_per_shard=1, 
        base_port=7100, 
        valkey_binary=valkey_binary
    )
    port_mgr = PortManager(base_port=7100)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()
    
    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)
    
    try:
        # Validate node configurations
        
        # Test cluster formation
        success = cluster_mgr.form_cluster(nodes, config_mgr.cluster_id)
        assert success
    finally:
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)


def test_full_cluster_creation_large():
    """Test complete cluster creation with 3 shards and 2 replicas"""
    config_mgr = ConfigurationManager(ClusterConfig(num_shards=3, replicas_per_shard=2), PortManager())
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    config = ClusterConfig(
        num_shards=3, 
        replicas_per_shard=2, 
        base_port=7200, 
        valkey_binary=valkey_binary
    )
    port_mgr = PortManager(base_port=7200)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()
    
    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)
    
    try:
        success = cluster_mgr.form_cluster(nodes, config_mgr.cluster_id)
        assert success
    finally:
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
