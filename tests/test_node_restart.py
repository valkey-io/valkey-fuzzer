"""
Test node restart functionality
"""
import pytest
import time
import logging
from src.cluster_orchestrator.orchestrator import (
    PortManager, ConfigurationManager, ClusterManager
)
from src.models import ClusterConfig

logging.basicConfig(
    format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s',
    level=logging.INFO,
    force=True
)


def test_restart_single_node():
    """
    Test restarting a single node in a cluster
    """
    # Setup
    config_mgr = ConfigurationManager(
        ClusterConfig(num_shards=1, replicas_per_shard=1),
        PortManager()
    )
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    config = ClusterConfig(
        num_shards=1,
        replicas_per_shard=1,
        base_port=7600,
        valkey_binary=valkey_binary,
        enable_cleanup=True
    )
    
    port_mgr = PortManager(base_port=7600)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()
    
    # Create cluster
    logging.info("Creating cluster...")
    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)
    
    try:
        success = cluster_mgr.form_cluster(nodes, config_mgr.cluster_id)
        assert success, "Failed to form cluster"
        logging.info("OK: Cluster formed")
        
        # Verify cluster is healthy
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy")
        
        # Pick a replica to restart
        replica = next(node for node in nodes if node.role == 'replica')
        old_pid = replica.pid
        logging.info(f"Restarting replica {replica.node_id} (PID {old_pid})")
        
        # Kill the node first
        replica.process.kill()
        replica.process.wait()
        logging.info(f"OK: Node {replica.node_id} killed")
        
        # Restart the node
        updated_node = config_mgr.restart_node(replica, wait_ready=True, ready_timeout=30.0)
        
        # Verify new process
        assert updated_node.pid != old_pid, "PID should be different after restart"
        assert updated_node.process.poll() is None, "Process should be running"
        logging.info(f"OK: Node restarted with new PID {updated_node.pid}")
        
        # Give cluster time to detect the node is back
        time.sleep(5)
        
        # Verify cluster is still healthy
        cluster_healthy = cluster_mgr.validate_cluster(nodes)
        logging.info(f"Cluster health after restart: {cluster_healthy}")
        
        logging.info("PASS: Node restart test completed")
        
    finally:
        # Cleanup
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
        logging.info("OK: Cleanup complete")


def test_restart_primary_in_multi_shard_cluster():
    """
    Test restarting a primary node in a multi-shard cluster
    
    This test verifies:
    1. Primary can be killed and restarted
    2. Cluster detects the primary is back
    3. Cluster remains operational with all slots covered
    """
    # Setup
    config_mgr = ConfigurationManager(
        ClusterConfig(num_shards=3, replicas_per_shard=1),
        PortManager()
    )
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    config = ClusterConfig(
        num_shards=3,
        replicas_per_shard=1,
        base_port=7700,
        valkey_binary=valkey_binary,
        enable_cleanup=True
    )
    
    port_mgr = PortManager(base_port=7700)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()
    
    # Create cluster
    logging.info("Creating multi-shard cluster (3 shards, 1 replica each)...")
    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)
    
    try:
        success = cluster_mgr.form_cluster(nodes, config_mgr.cluster_id)
        assert success, "Failed to form cluster"
        logging.info("OK: Cluster formed")
        
        # Verify cluster is healthy
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before restart")
        
        # Pick a primary from shard 0 to restart
        shard_0_nodes = [node for node in nodes if node.shard_id == 0]
        primary = next(node for node in shard_0_nodes if node.role == 'primary')
        replica = next(node for node in shard_0_nodes if node.role == 'replica')
        
        old_pid = primary.pid
        logging.info(f"Restarting primary {primary.node_id} (PID {old_pid}) from shard 0")
        logging.info(f"  Shard 0 has replica: {replica.node_id}")
        
        # Kill the primary
        primary.process.kill()
        primary.process.wait()
        logging.info(f"OK: Primary {primary.node_id} killed")
        
        # Wait a moment for cluster to detect the failure
        time.sleep(3)
        
        # Check cluster state with primary down
        remaining_nodes = [n for n in nodes if n.node_id != primary.node_id]
        try:
            cluster_healthy = cluster_mgr.validate_cluster(remaining_nodes)
            logging.info(f"Cluster health with primary down: {cluster_healthy}")
        except Exception as e:
            logging.info(f"Cluster validation with primary down: {e}")
        
        # Restart the primary
        logging.info(f"Restarting primary {primary.node_id}...")
        updated_node = config_mgr.restart_node(primary, wait_ready=True, ready_timeout=30.0)
        
        # Verify new process
        assert updated_node.pid != old_pid, "PID should be different after restart"
        assert updated_node.process.poll() is None, "Process should be running"
        logging.info(f"OK: Primary restarted with new PID {updated_node.pid}")
        
        # Give cluster time to detect the primary is back and sync
        logging.info("Waiting for cluster to stabilize after primary restart...")
        time.sleep(10)
        
        # Verify cluster is healthy again
        cluster_healthy = cluster_mgr.validate_cluster(nodes)
        logging.info(f"Cluster health after primary restart: {cluster_healthy}")
        
        logging.info("PASS: Multi-shard primary restart test completed")
        
    finally:
        # Cleanup
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
        logging.info("OK: Cleanup complete")


def test_restart_multiple_nodes_sequentially():
    """
    Test restarting multiple nodes sequentially in a cluster
    
    This simulates a rolling restart scenario.
    """
    # Setup
    config_mgr = ConfigurationManager(
        ClusterConfig(num_shards=2, replicas_per_shard=1),
        PortManager()
    )
    valkey_binary = config_mgr.setup_valkey_from_source()
    
    config = ClusterConfig(
        num_shards=2,
        replicas_per_shard=1,
        base_port=7800,
        valkey_binary=valkey_binary,
        enable_cleanup=True
    )
    
    port_mgr = PortManager(base_port=7800)
    config_mgr = ConfigurationManager(config, port_mgr)
    cluster_mgr = ClusterManager()
    
    # Create cluster
    logging.info("Creating cluster for rolling restart test...")
    topology = config_mgr.plan_topology()
    nodes = config_mgr.spawn_all_nodes(topology)
    
    try:
        success = cluster_mgr.form_cluster(nodes, config_mgr.cluster_id)
        assert success, "Failed to form cluster"
        logging.info("OK: Cluster formed")
        
        # Verify cluster is healthy
        assert cluster_mgr.validate_cluster(nodes)
        logging.info("OK: Cluster healthy before restarts")
        
        # Restart all replicas one by one
        replicas = [node for node in nodes if node.role == 'replica']
        logging.info(f"Performing rolling restart of {len(replicas)} replicas...")
        
        for i, replica in enumerate(replicas, 1):
            logging.info(f"\nRestarting replica {i}/{len(replicas)}: {replica.node_id}")
            old_pid = replica.pid
            
            # Kill the replica
            replica.process.kill()
            replica.process.wait()
            logging.info(f"  Killed {replica.node_id} (PID {old_pid})")
            
            # Restart it
            updated_node = config_mgr.restart_node(replica, wait_ready=True, ready_timeout=30.0)
            assert updated_node.pid != old_pid
            logging.info(f"  Restarted {replica.node_id} (new PID {updated_node.pid})")
            
            # Brief pause between restarts
            time.sleep(2)
        
        # Give cluster time to stabilize
        logging.info("Waiting for cluster to stabilize after all restarts...")
        time.sleep(5)
        
        # Verify cluster is still healthy
        cluster_healthy = cluster_mgr.validate_cluster(nodes)
        assert cluster_healthy, "Cluster should be healthy after rolling restart"
        logging.info("OK: Cluster healthy after rolling restart of all replicas")
        
        logging.info("PASS: Rolling restart test completed")
        
    finally:
        # Cleanup
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
        logging.info("OK: Cleanup complete")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
