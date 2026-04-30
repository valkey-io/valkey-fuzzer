"""
Simple unit tests for zero-replica cluster validation configuration
"""

from src.models import ClusterConfig, StateValidationConfig


def test_default_replication_config_has_min_replicas():
    """Test that default config has min_replicas_per_shard = 1"""
    config = StateValidationConfig()
    assert config.replication_config.min_replicas_per_shard == 1


def test_zero_replica_cluster_config():
    """Test that ClusterConfig can have zero replicas"""
    cluster_config = ClusterConfig(num_shards=3, replicas_per_shard=0)
    assert cluster_config.replicas_per_shard == 0


def test_validation_config_can_be_adjusted_for_zero_replicas():
    """Test that validation config can be adjusted for zero-replica clusters"""
    config = StateValidationConfig()

    # Initially has min_replicas_per_shard = 1
    assert config.replication_config.min_replicas_per_shard == 1

    # Can be adjusted to 0 for zero-replica clusters
    config.replication_config.min_replicas_per_shard = 0
    assert config.replication_config.min_replicas_per_shard == 0


def test_validation_config_adjustment_logic():
    """
    Test the logic that should be applied in fuzzer_engine.py
    to adjust min_replicas_per_shard based on cluster config
    """
    # Scenario 1: Zero-replica cluster
    cluster_config = ClusterConfig(num_shards=3, replicas_per_shard=0)
    validation_config = StateValidationConfig()

    # Apply the adjustment logic
    expected_replicas_per_shard = cluster_config.replicas_per_shard
    if expected_replicas_per_shard == 0:
        validation_config.replication_config.min_replicas_per_shard = 0
    elif validation_config.replication_config.min_replicas_per_shard > expected_replicas_per_shard:
        validation_config.replication_config.min_replicas_per_shard = expected_replicas_per_shard

    # Should be adjusted to 0
    assert validation_config.replication_config.min_replicas_per_shard == 0

    # Scenario 2: Single-replica cluster
    cluster_config = ClusterConfig(num_shards=2, replicas_per_shard=1)
    validation_config = StateValidationConfig()

    expected_replicas_per_shard = cluster_config.replicas_per_shard
    if expected_replicas_per_shard == 0:
        validation_config.replication_config.min_replicas_per_shard = 0
    elif validation_config.replication_config.min_replicas_per_shard > expected_replicas_per_shard:
        validation_config.replication_config.min_replicas_per_shard = expected_replicas_per_shard

    # Should remain 1
    assert validation_config.replication_config.min_replicas_per_shard == 1

    # Scenario 3: Multi-replica cluster (more than default)
    cluster_config = ClusterConfig(num_shards=2, replicas_per_shard=3)
    validation_config = StateValidationConfig()

    expected_replicas_per_shard = cluster_config.replicas_per_shard
    if expected_replicas_per_shard == 0:
        validation_config.replication_config.min_replicas_per_shard = 0
    elif validation_config.replication_config.min_replicas_per_shard > expected_replicas_per_shard:
        validation_config.replication_config.min_replicas_per_shard = expected_replicas_per_shard

    # Should remain 1 (not adjusted up, only down)
    assert validation_config.replication_config.min_replicas_per_shard == 1
