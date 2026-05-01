"""
Tests for Scenario Generator
"""

import pytest

from src.fuzzer_engine.test_case_generator import ScenarioGenerator
from src.models import ChaosType, OperationType, ProcessChaosType


def test_generate_random_scenario_with_seed():
    """Test generating random scenario with seed produces consistent results"""
    generator = ScenarioGenerator()

    # Generate two scenarios with same seed
    scenario1 = generator.generate_random_scenario(seed=12345)
    scenario2 = generator.generate_random_scenario(seed=12345)

    # Should have same configuration
    assert scenario1.cluster_config.num_shards == scenario2.cluster_config.num_shards
    assert scenario1.cluster_config.replicas_per_shard == scenario2.cluster_config.replicas_per_shard
    assert len(scenario1.operations) == len(scenario2.operations)
    assert scenario1.seed == scenario2.seed == 12345


def test_generate_random_scenario_without_seed():
    """Test generating random scenario without seed"""
    generator = ScenarioGenerator()

    scenario = generator.generate_random_scenario()

    # Should have valid configuration
    assert 3 <= scenario.cluster_config.num_shards <= 16
    # Ensure at least 1 replica for failover operations
    assert 1 <= scenario.cluster_config.replicas_per_shard <= 5
    assert len(scenario.operations) >= 1
    assert scenario.seed is not None


def test_random_cluster_config_ranges():
    """Test random cluster configuration stays within valid ranges"""
    generator = ScenarioGenerator(random_seed=42)

    for _ in range(10):
        scenario = generator.generate_random_scenario()
        assert 3 <= scenario.cluster_config.num_shards <= 16
        # Ensure at least 1 replica for failover operations
        assert 1 <= scenario.cluster_config.replicas_per_shard <= 5


def test_random_operations_are_failover():
    """Test random operations are failover type"""
    generator = ScenarioGenerator(random_seed=42)
    scenario = generator.generate_random_scenario()

    for operation in scenario.operations:
        assert operation.type == OperationType.FAILOVER
        assert operation.target_node is not None
        assert operation.timing.timeout > 0


def test_random_chaos_config():
    """Test random chaos configuration is valid"""
    generator = ScenarioGenerator(random_seed=42)
    scenario = generator.generate_random_scenario()

    assert scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL
    assert scenario.chaos_config.process_chaos_type in [ProcessChaosType.SIGKILL, ProcessChaosType.SIGTERM]
    assert scenario.chaos_config.target_selection.strategy in ["random", "primary_only", "replica_only"]


def test_parse_dsl_config_valid():
    """Test parsing valid DSL configuration"""
    dsl_text = """
scenario_id: test-scenario-001
seed: 12345
cluster:
  num_shards: 3
  replicas_per_shard: 1
  base_port: 7000
operations:
  - type: failover
    target_node: shard-0-primary
    parameters:
      force: true
    timing:
      delay_before: 1.0
      timeout: 30.0
      delay_after: 2.0
chaos:
  type: process_kill
  process_chaos_type: sigkill
  target_selection:
    strategy: random
  timing:
    delay_before_operation: 0.5
    chaos_duration: 10.0
  coordination:
    chaos_during_operation: true
state_validation:
  check_slot_coverage: true
  validation_timeout: 60.0
"""

    generator = ScenarioGenerator()
    scenario = generator.parse_dsl_config(dsl_text)

    assert scenario.scenario_id == "test-scenario-001"
    assert scenario.seed == 12345
    assert scenario.cluster_config.num_shards == 3
    assert scenario.cluster_config.replicas_per_shard == 1
    assert len(scenario.operations) == 1
    assert scenario.operations[0].type == OperationType.FAILOVER
    assert scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL


def test_parse_dsl_config_minimal():
    """Test parsing minimal DSL configuration"""
    dsl_text = """
scenario_id: minimal-test
cluster:
  num_shards: 3
  replicas_per_shard: 0
operations:
  - type: failover
    target_node: shard-0-primary
"""

    generator = ScenarioGenerator()
    scenario = generator.parse_dsl_config(dsl_text)

    assert scenario.scenario_id == "minimal-test"
    assert scenario.cluster_config.num_shards == 3
    assert len(scenario.operations) == 1


def test_parse_dsl_config_invalid_yaml():
    """Test parsing invalid YAML"""
    dsl_text = """
scenario_id: test
cluster:
  num_shards: 3
  invalid yaml here
"""

    generator = ScenarioGenerator()
    with pytest.raises(ValueError, match="Invalid YAML syntax"):
        generator.parse_dsl_config(dsl_text)


def test_parse_dsl_config_missing_required_field():
    """Test parsing DSL with missing required fields"""
    dsl_text = """
scenario_id: test
cluster:
  num_shards: 3
"""

    generator = ScenarioGenerator()
    with pytest.raises(ValueError, match="Missing required field: operations"):
        generator.parse_dsl_config(dsl_text)


def test_parse_dsl_config_invalid_shard_count():
    """Test parsing DSL with invalid shard count"""
    dsl_text = """
scenario_id: test
cluster:
  num_shards: 20
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
"""

    generator = ScenarioGenerator()
    with pytest.raises(ValueError, match="num_shards must be between 3 and 16"):
        generator.parse_dsl_config(dsl_text)


def test_validate_scenario_valid():
    """Test validating valid scenario"""
    generator = ScenarioGenerator(random_seed=42)
    scenario = generator.generate_random_scenario()

    assert generator.validate_scenario(scenario) is True


def test_validate_scenario_invalid_shards():
    """Test validating scenario with invalid shard count"""
    generator = ScenarioGenerator()
    scenario = generator.generate_random_scenario(seed=42)
    scenario.cluster_config.num_shards = 20

    with pytest.raises(ValueError, match="Invalid num_shards"):
        generator.validate_scenario(scenario)


def test_validate_scenario_no_operations():
    """Test validating scenario with no operations"""
    generator = ScenarioGenerator()
    scenario = generator.generate_random_scenario(seed=42)
    scenario.operations = []

    with pytest.raises(ValueError, match="at least one operation"):
        generator.validate_scenario(scenario)


def test_validate_scenario_process_chaos_type_none_without_randomization():
    """Test that None process_chaos_type is rejected when randomization is disabled"""
    generator = ScenarioGenerator()
    scenario = generator.generate_random_scenario(seed=42)

    # Set process_chaos_type to None without enabling randomization
    scenario.chaos_config.process_chaos_type = None
    scenario.chaos_config.randomize_per_operation = False

    with pytest.raises(ValueError, match="process_chaos_type required"):
        generator.validate_scenario(scenario)


def test_validate_scenario_process_chaos_type_none_with_randomization():
    """Test that None process_chaos_type is allowed when randomization is enabled"""
    generator = ScenarioGenerator()
    scenario = generator.generate_random_scenario(seed=42)

    # Set process_chaos_type to None WITH randomization enabled
    scenario.chaos_config.process_chaos_type = None
    scenario.chaos_config.randomize_per_operation = True

    # Should not raise - randomization will handle it at runtime
    assert generator.validate_scenario(scenario) is True


def test_parse_dsl_with_null_process_chaos_type():
    """Test parsing DSL with null process_chaos_type for randomization"""
    dsl_text = """
scenario_id: test-randomization
cluster:
  num_shards: 3
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
chaos:
  type: process_kill
  process_chaos_type: null
  randomize_per_operation: true
  target_selection:
    strategy: random
  coordination:
    chaos_during_operation: true
"""

    generator = ScenarioGenerator()
    scenario = generator.parse_dsl_config(dsl_text)

    # Should parse successfully with None process_chaos_type
    assert scenario.chaos_config.process_chaos_type is None
    assert scenario.chaos_config.randomize_per_operation is True
    assert scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL


def test_parse_dsl_with_omitted_process_chaos_type():
    """Test parsing DSL with omitted process_chaos_type field (defaults to sigkill)"""
    dsl_text = """
scenario_id: test-default
cluster:
  num_shards: 3
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
chaos:
  type: process_kill
  target_selection:
    strategy: random
  coordination:
    chaos_during_operation: true
"""

    generator = ScenarioGenerator()
    scenario = generator.parse_dsl_config(dsl_text)

    # Should default to SIGKILL when omitted
    assert scenario.chaos_config.process_chaos_type == ProcessChaosType.SIGKILL
    assert scenario.chaos_config.chaos_type == ChaosType.PROCESS_KILL


def test_dsl_roundtrip_with_none_process_chaos_type(tmp_path):
    """Test that scenarios with None process_chaos_type can round-trip through DSL"""
    from src.fuzzer_engine.dsl_utils import DSLLoader

    generator = ScenarioGenerator()

    # Create a scenario with None process_chaos_type and randomization enabled
    scenario = generator.generate_random_scenario(seed=42)
    scenario.chaos_config.process_chaos_type = None
    scenario.chaos_config.randomize_per_operation = True

    # Save to DSL file
    dsl_file = tmp_path / "test_scenario.yaml"
    DSLLoader.save_scenario_as_dsl(scenario, dsl_file)

    # Load back from DSL file
    dsl_config = DSLLoader.load_from_file(dsl_file)
    loaded_scenario = generator.parse_dsl_config(dsl_config.config_text)

    # Verify the loaded scenario has None process_chaos_type
    assert loaded_scenario.chaos_config.process_chaos_type is None
    assert loaded_scenario.chaos_config.randomize_per_operation is True
    assert loaded_scenario.scenario_id == scenario.scenario_id
    assert loaded_scenario.seed == scenario.seed


def test_parse_dsl_with_zero_stabilization_wait():
    """Test that stabilization_wait: 0 is accepted (to skip the wait)"""
    dsl_text = """
scenario_id: test-zero-stabilization
cluster:
  num_shards: 3
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
state_validation:
  stabilization_wait: 0
  validation_timeout: 30.0
"""

    generator = ScenarioGenerator()
    scenario = generator.parse_dsl_config(dsl_text)

    # Should parse successfully with 0 stabilization_wait
    assert scenario.state_validation_config.stabilization_wait == 0
    assert scenario.state_validation_config.validation_timeout == 30.0


def test_parse_dsl_with_negative_stabilization_wait():
    """Test that negative stabilization_wait is rejected"""
    dsl_text = """
scenario_id: test-negative-stabilization
cluster:
  num_shards: 3
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
state_validation:
  stabilization_wait: -1.0
  validation_timeout: 30.0
"""

    generator = ScenarioGenerator()

    # Should raise validation error for negative value
    with pytest.raises(ValueError, match=r"stabilization_wait.*non-negative"):
        generator.parse_dsl_config(dsl_text)


def test_parse_dsl_with_zero_validation_timeout():
    """Test that validation_timeout: 0 is rejected (must be positive)"""
    dsl_text = """
scenario_id: test-zero-timeout
cluster:
  num_shards: 3
  replicas_per_shard: 1
operations:
  - type: failover
    target_node: shard-0-primary
state_validation:
  stabilization_wait: 5.0
  validation_timeout: 0
"""

    generator = ScenarioGenerator()

    # Should raise validation error for zero timeout
    with pytest.raises(ValueError, match=r"validation_timeout.*positive"):
        generator.parse_dsl_config(dsl_text)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
