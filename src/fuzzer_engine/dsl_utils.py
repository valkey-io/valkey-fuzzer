"""
DSL Utilities - Helper functions for DSL configuration handling
"""
import yaml
from pathlib import Path
from typing import Union
from ..models import DSLConfig, Scenario


class DSLLoader:
    """Utility class for loading and validating DSL configurations"""
    
    @staticmethod
    def load_from_file(file_path: Union[str, Path]) -> DSLConfig:
        """Load DSL configuration from a YAML file."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"DSL file not found: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                config_text = f.read()
            
            yaml.safe_load(config_text)
            return DSLConfig(config_text=config_text)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax in {file_path}: {e}")
        except Exception as e:
            raise ValueError(f"Error loading DSL file {file_path}: {e}")
    
    @staticmethod
    def load_from_string(config_text: str) -> DSLConfig:
        """Load DSL configuration from a YAML string."""
        try:
            yaml.safe_load(config_text)
            return DSLConfig(config_text=config_text)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax: {e}")
    
    @staticmethod
    def validate_dsl_config(dsl_config: DSLConfig, scenario_generator) -> bool:
        """Validate a DSL configuration by parsing and validating the scenario."""
        scenario = scenario_generator.parse_dsl_config(dsl_config.config_text)
        scenario_generator.validate_scenario(scenario)
        return True
    
    @staticmethod
    def _serialize_state_validation_config(config) -> dict:
        """Serialize StateValidationConfig and all nested configs to dictionary."""
        result = {
            'check_replication': config.check_replication,
            'check_cluster_status': config.check_cluster_status,
            'check_slot_coverage': config.check_slot_coverage,
            'check_topology': config.check_topology,
            'check_view_consistency': config.check_view_consistency,
            'check_data_consistency': config.check_data_consistency,
            'stabilization_wait': config.stabilization_wait,
            'validation_timeout': config.validation_timeout,
            'blocking_on_failure': config.blocking_on_failure,
            'retry_on_transient_failure': config.retry_on_transient_failure,
            'max_retries': config.max_retries,
            'retry_delay': config.retry_delay
        }
        
        # Serialize replication config
        if config.replication_config:
            result['replication_config'] = {
                'max_acceptable_lag': config.replication_config.max_acceptable_lag,
                'require_all_replicas_synced': config.replication_config.require_all_replicas_synced,
                'check_replication_offset': config.replication_config.check_replication_offset,
                'min_replicas_per_shard': config.replication_config.min_replicas_per_shard,
                'timeout': config.replication_config.timeout
            }
        
        # Serialize cluster status config
        if config.cluster_status_config:
            result['cluster_status_config'] = {
                'acceptable_states': config.cluster_status_config.acceptable_states,
                'allow_degraded': config.cluster_status_config.allow_degraded,
                'require_quorum': config.cluster_status_config.require_quorum,
                'timeout': config.cluster_status_config.timeout
            }
        
        # Serialize slot coverage config
        if config.slot_coverage_config:
            result['slot_coverage_config'] = {
                'require_full_coverage': config.slot_coverage_config.require_full_coverage,
                'allow_slot_conflicts': config.slot_coverage_config.allow_slot_conflicts,
                'timeout': config.slot_coverage_config.timeout
            }
        
        # Serialize topology config
        if config.topology_config:
            result['topology_config'] = {
                'strict_mode': config.topology_config.strict_mode,
                'allow_failed_nodes': config.topology_config.allow_failed_nodes,
                'timeout': config.topology_config.timeout
            }
        
        # Serialize view consistency config
        if config.view_consistency_config:
            result['view_consistency_config'] = {
                'require_full_consensus': config.view_consistency_config.require_full_consensus,
                'allow_transient_inconsistency': config.view_consistency_config.allow_transient_inconsistency,
                'max_inconsistency_duration': config.view_consistency_config.max_inconsistency_duration,
                'timeout': config.view_consistency_config.timeout
            }
        
        # Serialize data consistency config
        if config.data_consistency_config:
            result['data_consistency_config'] = {
                'check_test_keys': config.data_consistency_config.check_test_keys,
                'check_cross_replica_consistency': config.data_consistency_config.check_cross_replica_consistency,
                'num_test_keys': config.data_consistency_config.num_test_keys,
                'key_prefix': config.data_consistency_config.key_prefix,
                'timeout': config.data_consistency_config.timeout
            }
        
        return result
    
    @staticmethod
    def save_scenario_as_dsl(scenario: Scenario, file_path: Union[str, Path]) -> None:
        """Save a scenario as a DSL YAML file for reproducibility."""
        file_path = Path(file_path)
        
        dsl_dict = {
            'scenario_id': scenario.scenario_id,
            'seed': scenario.seed,
            'cluster': {
                'num_shards': scenario.cluster_config.num_shards,
                'replicas_per_shard': scenario.cluster_config.replicas_per_shard,
                'base_port': scenario.cluster_config.base_port,
                'base_data_dir': scenario.cluster_config.base_data_dir,
                'valkey_binary': scenario.cluster_config.valkey_binary,
                'enable_cleanup': scenario.cluster_config.enable_cleanup
            },
            'operations': [
                {
                    'type': op.type.value,
                    'target_node': op.target_node,
                    'parameters': op.parameters,
                    'timing': {
                        'delay_before': op.timing.delay_before,
                        'timeout': op.timing.timeout,
                        'delay_after': op.timing.delay_after
                    }
                }
                for op in scenario.operations
            ],
            'chaos': {
                'type': scenario.chaos_config.chaos_type.value,
                'process_chaos_type': scenario.chaos_config.process_chaos_type.value if scenario.chaos_config.process_chaos_type else None,
                'randomize_per_operation': scenario.chaos_config.randomize_per_operation,
                'target_selection': {
                    'strategy': scenario.chaos_config.target_selection.strategy,
                    'specific_nodes': scenario.chaos_config.target_selection.specific_nodes
                },
                'timing': {
                    'delay_before_operation': scenario.chaos_config.timing.delay_before_operation,
                    'delay_after_operation': scenario.chaos_config.timing.delay_after_operation,
                    'chaos_duration': scenario.chaos_config.timing.chaos_duration
                },
                'coordination': {
                    'chaos_before_operation': scenario.chaos_config.coordination.chaos_before_operation,
                    'chaos_during_operation': scenario.chaos_config.coordination.chaos_during_operation,
                    'chaos_after_operation': scenario.chaos_config.coordination.chaos_after_operation
                }
            },
            'state_validation': DSLLoader._serialize_state_validation_config(scenario.state_validation_config) if scenario.state_validation_config else None
        }
        
        with open(file_path, 'w') as f:
            yaml.dump(dsl_dict, f, default_flow_style=False, sort_keys=False)


class DSLValidator:
    """Validator for DSL configurations with detailed error reporting"""
    
    @staticmethod
    def validate_structure(config_dict: dict) -> list:
        """
        Validate the structure of a DSL configuration dictionary.
        """
        errors = []
        
        # Check required top-level fields
        required_fields = ['scenario_id', 'cluster', 'operations']
        for field in required_fields:
            if field not in config_dict:
                errors.append(f"Missing required field: {field}")
        
        # Validate cluster configuration
        if 'cluster' in config_dict:
            cluster_errors = DSLValidator._validate_cluster(config_dict['cluster'])
            errors.extend(cluster_errors)
        
        # Validate operations
        if 'operations' in config_dict:
            operations_errors = DSLValidator._validate_operations(config_dict['operations'])
            errors.extend(operations_errors)
        
        # Validate chaos configuration (optional)
        if 'chaos' in config_dict:
            chaos_errors = DSLValidator._validate_chaos(config_dict['chaos'])
            errors.extend(chaos_errors)
        
        # Validate state validation configuration (optional)
        if 'state_validation' in config_dict:
            validation_errors = DSLValidator._validate_state_validation(config_dict['state_validation'])
            errors.extend(validation_errors)
        
        return errors
    
    @staticmethod
    def _validate_cluster(cluster_dict: dict) -> list:
        """Validate cluster configuration"""
        errors = []
        
        required_fields = ['num_shards', 'replicas_per_shard']
        for field in required_fields:
            if field not in cluster_dict:
                errors.append(f"cluster: Missing required field '{field}'")
        
        # Validate ranges
        if 'num_shards' in cluster_dict:
            num_shards = cluster_dict['num_shards']
            if not isinstance(num_shards, int) or not (3 <= num_shards <= 16):
                errors.append(f"cluster.num_shards: Must be an integer between 3 and 16, got {num_shards}")
        
        if 'replicas_per_shard' in cluster_dict:
            replicas = cluster_dict['replicas_per_shard']
            if not isinstance(replicas, int) or not (0 <= replicas <= 5):
                errors.append(f"cluster.replicas_per_shard: Must be an integer between 0 and 5, got {replicas}")
        
        return errors
    
    @staticmethod
    def _validate_operations(operations_list: list) -> list:
        """Validate operations list"""
        errors = []
        
        if not isinstance(operations_list, list):
            errors.append("operations: Must be a list")
            return errors
        
        if len(operations_list) == 0:
            errors.append("operations: Must contain at least one operation")
        
        for i, op in enumerate(operations_list):
            if not isinstance(op, dict):
                errors.append(f"operations[{i}]: Must be a dictionary")
                continue
            
            # Check required fields
            if 'type' not in op:
                errors.append(f"operations[{i}]: Missing required field 'type'")
            elif op['type'] not in ['failover']:  # Add more types as they're implemented
                errors.append(f"operations[{i}]: Invalid operation type '{op['type']}'")
            
            if 'target_node' not in op:
                errors.append(f"operations[{i}]: Missing required field 'target_node'")
        
        return errors
    
    @staticmethod
    def _validate_chaos(chaos_dict: dict) -> list:
        """Validate chaos configuration"""
        errors = []
        
        if not isinstance(chaos_dict, dict):
            errors.append("chaos: Must be a dictionary")
            return errors
        
        # Validate chaos type
        if 'type' in chaos_dict:
            if chaos_dict['type'] not in ['process_kill']:  # Add more types as implemented
                errors.append(f"chaos.type: Invalid chaos type '{chaos_dict['type']}'")
        
        # Validate target selection
        if 'target_selection' in chaos_dict:
            target_sel = chaos_dict['target_selection']
            if 'strategy' in target_sel:
                valid_strategies = ['random', 'primary_only', 'replica_only', 'specific']
                if target_sel['strategy'] not in valid_strategies:
                    errors.append(f"chaos.target_selection.strategy: Must be one of {valid_strategies}")
        
        return errors
    
    @staticmethod
    def _validate_state_validation(validation_dict: dict) -> list:
        """Validate state validation configuration"""
        errors = []
        
        if not isinstance(validation_dict, dict):
            errors.append("state_validation: Must be a dictionary")
            return errors
        
        # All fields are optional booleans or floats, just check types if present
        bool_fields = [
            'check_replication', 'check_cluster_status', 'check_slot_coverage',
            'check_topology', 'check_view_consistency', 'check_data_consistency',
            'blocking_on_failure', 'retry_on_transient_failure'
        ]
        
        for field in bool_fields:
            if field in validation_dict and not isinstance(validation_dict[field], bool):
                errors.append(f"state_validation.{field}: Must be a boolean")
        
        # Fields that must be strictly positive (> 0)
        positive_fields = ['validation_timeout']
        for field in positive_fields:
            if field in validation_dict:
                value = validation_dict[field]
                if not isinstance(value, (int, float)) or value <= 0:
                    errors.append(f"state_validation.{field}: Must be a positive number")
        
        # Fields that must be non-negative (>= 0)
        non_negative_fields = ['stabilization_wait', 'retry_delay']
        for field in non_negative_fields:
            if field in validation_dict:
                value = validation_dict[field]
                if not isinstance(value, (int, float)) or value < 0:
                    errors.append(f"state_validation.{field}: Must be a non-negative number")
        
        int_fields = ['max_retries']
        for field in int_fields:
            if field in validation_dict:
                value = validation_dict[field]
                if not isinstance(value, int) or value < 0:
                    errors.append(f"state_validation.{field}: Must be a non-negative integer")
        
        return errors
