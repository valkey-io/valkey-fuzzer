"""
Integration tests for Fuzzer Engine end-to-end test execution
"""

import pytest

from src.fuzzer_engine import DSLLoader, FuzzerEngine
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosTiming,
    ChaosType,
    ClusterConfig,
    Operation,
    OperationTiming,
    OperationType,
    ProcessChaosType,
    Scenario,
    TargetSelection,
)


class TestFuzzerEngineIntegration:
    """Integration tests for complete test scenario execution"""

    def test_random_scenario_generation(self):
        """Test that random scenario generation works"""
        engine = FuzzerEngine()

        # Generate scenario with seed for reproducibility
        scenario = engine.generate_random_scenario(seed=42)

        assert scenario is not None
        assert scenario.scenario_id == "42"
        assert scenario.seed == 42
        assert scenario.cluster_config is not None
        assert len(scenario.operations) > 0
        assert scenario.chaos_config is not None

    def test_random_scenario_reproducibility(self):
        """Test that same seed produces same scenario"""
        engine = FuzzerEngine()

        scenario1 = engine.generate_random_scenario(seed=123)
        scenario2 = engine.generate_random_scenario(seed=123)

        # Should have same configuration
        assert scenario1.cluster_config.num_shards == scenario2.cluster_config.num_shards
        assert scenario1.cluster_config.replicas_per_shard == scenario2.cluster_config.replicas_per_shard
        assert len(scenario1.operations) == len(scenario2.operations)

    def test_dsl_scenario_parsing(self):
        """Test DSL configuration parsing"""
        engine = FuzzerEngine()

        dsl_text = """
scenario_id: "test-dsl-parsing"
seed: 999
cluster:
  num_shards: 3
  replicas_per_shard: 1
  base_port: 7000
operations:
  - type: "failover"
    target_node: "shard-0-primary"
    parameters:
      force: false
    timing:
      delay_before: 0.0
      timeout: 30.0
      delay_after: 0.0
chaos:
  type: "process_kill"
  process_chaos_type: "sigkill"
  target_selection:
    strategy: "random"
  timing:
    delay_before_operation: 0.0
    delay_after_operation: 0.0
    chaos_duration: 10.0
  coordination:
    chaos_before_operation: false
    chaos_during_operation: true
    chaos_after_operation: false
state_validation:
  check_slot_coverage: true
  check_cluster_status: true
  check_replication: true
  check_topology: true
  check_view_consistency: true
  check_data_consistency: true
  stabilization_wait: 5.0
  validation_timeout: 60.0
  replication_config:
    max_acceptable_lag: 5.0
"""

        from src.models import DSLConfig

        DSLConfig(config_text=dsl_text)

        # Parse should not raise exception
        scenario = engine.scenario_generator.parse_dsl_config(dsl_text)

        assert scenario.scenario_id == "test-dsl-parsing"
        assert scenario.seed == 999
        assert scenario.cluster_config.num_shards == 3
        assert len(scenario.operations) == 1
        assert scenario.operations[0].type == OperationType.FAILOVER

    def test_dsl_validation_errors(self):
        """Test that invalid DSL configurations are rejected"""
        engine = FuzzerEngine()

        # Missing required field
        invalid_dsl = """
scenario_id: "invalid-test"
cluster:
  num_shards: 3
"""

        with pytest.raises(ValueError, match="Missing required field: operations"):
            engine.scenario_generator.parse_dsl_config(invalid_dsl)

        # Invalid num_shards
        invalid_dsl2 = """
scenario_id: "invalid-test"
cluster:
  num_shards: 20
  replicas_per_shard: 1
operations:
  - type: "failover"
    target_node: "shard-0-primary"
"""

        with pytest.raises(ValueError, match="num_shards must be between 3 and 16"):
            engine.scenario_generator.parse_dsl_config(invalid_dsl2)

    def test_scenario_validation(self):
        """Test scenario validation logic"""
        engine = FuzzerEngine()

        # Create valid scenario
        scenario = Scenario(
            scenario_id="test-validation",
            cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1),
            operations=[
                Operation(
                    type=OperationType.FAILOVER, target_node="shard-0-primary", parameters={}, timing=OperationTiming()
                )
            ],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=ProcessChaosType.SIGKILL,
            ),
        )

        # Should validate successfully
        assert engine.scenario_generator.validate_scenario(scenario) is True

        # Invalid scenario - no operations
        invalid_scenario = Scenario(
            scenario_id="invalid",
            cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1),
            operations=[],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=ProcessChaosType.SIGKILL,
            ),
        )

        with pytest.raises(ValueError, match="Scenario must have at least one operation"):
            engine.scenario_generator.validate_scenario(invalid_scenario)

    def test_dsl_loader_from_string(self):
        """Test DSL loader utility"""
        dsl_text = """
scenario_id: "loader-test"
cluster:
  num_shards: 3
  replicas_per_shard: 1
operations:
  - type: "failover"
    target_node: "shard-0-primary"
"""

        dsl_config = DSLLoader.load_from_string(dsl_text)

        assert dsl_config is not None
        assert dsl_config.config_text == dsl_text

    def test_dsl_loader_invalid_yaml(self):
        """Test DSL loader with invalid YAML"""
        invalid_yaml = """
scenario_id: "test
  invalid: yaml: syntax
"""

        with pytest.raises(ValueError, match="Invalid YAML syntax"):
            DSLLoader.load_from_string(invalid_yaml)


class TestDSLReproducibility:
    """Tests for DSL-based test reproducibility"""

    def test_dsl_produces_consistent_results(self):
        """Test that same DSL produces consistent scenario"""
        engine = FuzzerEngine()

        dsl_text = """
scenario_id: "reproducibility-test"
seed: 42
cluster:
  num_shards: 3
  replicas_per_shard: 1
  base_port: 7000
operations:
  - type: "failover"
    target_node: "shard-0-primary"
    parameters:
      force: false
    timing:
      delay_before: 0.0
      timeout: 30.0
      delay_after: 0.0
"""

        # Parse twice
        scenario1 = engine.scenario_generator.parse_dsl_config(dsl_text)
        scenario2 = engine.scenario_generator.parse_dsl_config(dsl_text)

        # Should be identical
        assert scenario1.scenario_id == scenario2.scenario_id
        assert scenario1.seed == scenario2.seed
        assert scenario1.cluster_config.num_shards == scenario2.cluster_config.num_shards
        assert len(scenario1.operations) == len(scenario2.operations)
        assert scenario1.operations[0].type == scenario2.operations[0].type

    def test_random_to_dsl_conversion(self):
        """Test converting random scenario to DSL and back"""
        engine = FuzzerEngine()

        # Generate random scenario
        original_scenario = engine.generate_random_scenario(seed=777)

        # Convert to DSL
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpdir:
            dsl_file = Path(tmpdir) / "test_scenario.yaml"
            DSLLoader.save_scenario_as_dsl(original_scenario, dsl_file)

            # Load back from DSL
            dsl_config = DSLLoader.load_from_file(dsl_file)
            recreated_scenario = engine.scenario_generator.parse_dsl_config(dsl_config.config_text)

            # Should match original
            assert recreated_scenario.scenario_id == original_scenario.scenario_id
            assert recreated_scenario.seed == original_scenario.seed
            assert recreated_scenario.cluster_config.num_shards == original_scenario.cluster_config.num_shards
            assert len(recreated_scenario.operations) == len(original_scenario.operations)


class TestErrorHandling:
    """Tests for error handling and graceful degradation"""

    def test_invalid_cluster_config_handling(self):
        """Test handling of invalid cluster configuration"""
        engine = FuzzerEngine()

        # Create scenario with invalid config
        scenario = Scenario(
            scenario_id="invalid-cluster",
            cluster_config=ClusterConfig(num_shards=2, replicas_per_shard=1),  # Invalid: < 3 shards
            operations=[
                Operation(
                    type=OperationType.FAILOVER, target_node="shard-0-primary", parameters={}, timing=OperationTiming()
                )
            ],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=ProcessChaosType.SIGKILL,
            ),
        )

        # Should raise validation error
        with pytest.raises(ValueError, match="Invalid num_shards"):
            engine.scenario_generator.validate_scenario(scenario)

    def test_missing_process_chaos_type(self):
        """Test validation of chaos config with missing process_chaos_type"""
        engine = FuzzerEngine()

        scenario = Scenario(
            scenario_id="missing-chaos-type",
            cluster_config=ClusterConfig(num_shards=3, replicas_per_shard=1),
            operations=[
                Operation(
                    type=OperationType.FAILOVER, target_node="shard-0-primary", parameters={}, timing=OperationTiming()
                )
            ],
            chaos_config=ChaosConfig(
                chaos_type=ChaosType.PROCESS_KILL,
                target_selection=TargetSelection(strategy="random"),
                timing=ChaosTiming(),
                coordination=ChaosCoordination(),
                process_chaos_type=None,  # Missing!
            ),
        )

        with pytest.raises(ValueError, match="process_chaos_type required"):
            engine.scenario_generator.validate_scenario(scenario)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
