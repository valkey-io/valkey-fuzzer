from src.cluster_orchestrator.orchestrator import ClusterManager, ConfigurationManager, PortManager
from src.models import ClusterConfig
from src.valkey_client.load_data import load_all_slots


def test_load_data():
    """Test loading data into all cluster slots"""
    cluster_config = ClusterConfig(num_shards=3, replicas_per_shard=1, base_port=7000)

    port_mgr = PortManager(base_port=7000)
    config_mgr = ConfigurationManager(cluster_config, port_mgr)
    cluster_mgr = ClusterManager()

    try:
        valkey_binary = config_mgr.setup_valkey_from_source()
        cluster_config.valkey_binary = valkey_binary

        topology = config_mgr.plan_topology()
        nodes = config_mgr.spawn_all_nodes(topology)
        cluster_connection = cluster_mgr.form_cluster(nodes, config_mgr.cluster_id)
        assert cluster_connection is not None

        # Test live discovery
        live_nodes = cluster_connection.get_current_nodes()
        primary_nodes = cluster_connection.get_primary_nodes()
        replica_nodes = cluster_connection.get_replica_nodes()

        assert len(live_nodes) == 6
        assert len(primary_nodes) == 3
        assert len(replica_nodes) == 3

        success = load_all_slots(cluster_connection, keys_per_slot=10)
        assert success

    finally:
        cluster_mgr.close_connections()
        config_mgr.cleanup_cluster(nodes)
