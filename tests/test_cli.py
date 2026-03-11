"""
Tests for CLI functionality
"""
import pytest
import json
import yaml
from unittest.mock import Mock, patch

from src.cli import FuzzerCLI, create_parser, main
from src.models import ExecutionResult, ChaosResult


@pytest.fixture
def mock_result():
    """Create a mock ExecutionResult"""
    return ExecutionResult(
        scenario_id="test-scenario-123",
        success=True,
        start_time=1000.0,
        end_time=1015.5,
        operations_executed=3,
        chaos_events=[
            ChaosResult(
                chaos_id="chaos-1",
                chaos_type=Mock(value="process_kill"),
                target_node="node-1",
                success=True,
                start_time=1005.0,
                end_time=1006.0
            )
        ],
        seed=42
    )


class TestFuzzerCLI:
    """Test FuzzerCLI class"""
    
    def test_load_yaml_config(self, tmp_path):
        """Test loading YAML configuration file"""
        config_file = tmp_path / "config.yaml"
        config_data = {
            'cluster': {'num_shards': 3},
            'validation': {'check_slot_coverage': True}
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        cli = FuzzerCLI()
        loaded = cli.load_config_file(str(config_file))
        
        assert loaded == config_data
    
    def test_load_json_config(self, tmp_path):
        """Test loading JSON configuration file"""
        config_file = tmp_path / "config.json"
        config_data = {
            'cluster': {'num_shards': 3},
            'validation': {'check_slot_coverage': True}
        }
        
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        cli = FuzzerCLI()
        loaded = cli.load_config_file(str(config_file))
        
        assert loaded == config_data
    
    def test_load_config_file_not_found(self):
        """Test loading non-existent config file"""
        cli = FuzzerCLI()
        
        with pytest.raises(FileNotFoundError):
            cli.load_config_file("nonexistent.yaml")
    
    def test_load_config_unsupported_format(self, tmp_path):
        """Test loading unsupported config format"""
        config_file = tmp_path / "config.txt"
        config_file.write_text("some text")
        
        cli = FuzzerCLI()
        
        with pytest.raises(ValueError, match="Unsupported config format"):
            cli.load_config_file(str(config_file))
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_success(self, mock_fuzzer_class, mock_result, capsys):
        """Test running random test successfully"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            valkey_binary=None,
            output=None,
            verbose=False,
            export_dsl=None
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        mock_fuzzer.run_random_test.assert_called_once_with(seed=42)
        
        captured = capsys.readouterr()
        assert "PASSED" in captured.out
        assert "test-scenario-123" in captured.out
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_with_config(self, mock_fuzzer_class, mock_result, tmp_path):
        """Test running random test with config file"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("cluster:\n  num_shards: 3\n")
        
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=None,
            iterations=1,
            config=str(config_file),
            valkey_binary=None,
            output=None,
            verbose=False,
            export_dsl=None
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        assert cli.config == {'cluster': {'num_shards': 3}}
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_multiple_iterations(self, mock_fuzzer_class, mock_result):
        """Test running multiple test iterations"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=3,
            config=None,
            valkey_binary=None,
            output=None,
            verbose=False,
            export_dsl=None
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        assert mock_fuzzer.run_random_test.call_count == 3
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_with_output(self, mock_fuzzer_class, mock_result, tmp_path):
        """Test saving test results to file"""
        output_file = tmp_path / "results.json"
        
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            valkey_binary=None,
            output=str(output_file),
            format='json',
            verbose=False,
            export_dsl=None
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        assert output_file.exists()
        
        with open(output_file) as f:
            data = json.load(f)
        
        assert data['total_tests'] == 1
        assert data['passed'] == 1
        assert len(data['results']) == 1
    
    @patch('src.cli.DSLLoader.save_scenario_as_dsl')
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_with_export_dsl(self, mock_fuzzer_class, mock_save_dsl, mock_result, tmp_path):
        """Test exporting random scenario to DSL file"""
        export_file = tmp_path / "scenario.yaml"
        
        mock_scenario = Mock()
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = mock_scenario
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            valkey_binary=None,
            output=None,
            verbose=False,
            export_dsl=str(export_file)
        )
        
        result = cli.run_random_test(args)
        
        assert result == 0
        mock_save_dsl.assert_called_once_with(mock_scenario, export_file)
    
    @patch('src.cli.ClusterBusFuzzer')
    @patch('src.cli.DSLLoader')
    def test_run_dsl_test_success(self, mock_loader, mock_fuzzer_class, mock_result, tmp_path):
        """Test running DSL-based test"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text("scenario:\n  id: test\n")
        
        mock_dsl_config = Mock()
        mock_loader.load_from_file.return_value = mock_dsl_config
        
        mock_fuzzer = Mock()
        mock_fuzzer.run_dsl_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer
        
        cli = FuzzerCLI()
        args = Mock(
            file=str(dsl_file),
            valkey_binary=None,
            output=None,
            verbose=False
        )
        
        result = cli.run_dsl_test(args)
        
        assert result == 0
        mock_loader.load_from_file.assert_called_once_with(str(dsl_file))
        mock_fuzzer.run_dsl_test.assert_called_once_with(mock_dsl_config)

    @patch('src.cli.ClusterBusFuzzer')
    def test_run_random_test_passes_valkey_binary(self, mock_fuzzer_class, mock_result):
        """Test running random test with explicit Valkey binary override"""
        mock_fuzzer = Mock()
        mock_fuzzer.run_random_test.return_value = mock_result
        mock_fuzzer.last_scenario = Mock()
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(
            seed=42,
            iterations=1,
            config=None,
            valkey_binary="/tmp/valkey/src/valkey-server",
            output=None,
            verbose=False,
            export_dsl=None
        )

        result = cli.run_random_test(args)

        assert result == 0
        mock_fuzzer.run_random_test.assert_called_once_with(
            seed=42,
            valkey_binary="/tmp/valkey/src/valkey-server"
        )

    @patch('src.cli.ClusterBusFuzzer')
    @patch('src.cli.DSLLoader')
    def test_run_dsl_test_passes_valkey_binary(self, mock_loader, mock_fuzzer_class, mock_result, tmp_path):
        """Test running DSL test with explicit Valkey binary override"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text("scenario:\n  id: test\n")

        mock_dsl_config = Mock()
        mock_loader.load_from_file.return_value = mock_dsl_config

        mock_fuzzer = Mock()
        mock_fuzzer.run_dsl_test.return_value = mock_result
        mock_fuzzer_class.return_value = mock_fuzzer

        cli = FuzzerCLI()
        args = Mock(
            file=str(dsl_file),
            valkey_binary="/tmp/valkey/src/valkey-server",
            output=None,
            verbose=False
        )

        result = cli.run_dsl_test(args)

        assert result == 0
        mock_fuzzer.run_dsl_test.assert_called_once_with(
            mock_dsl_config,
            valkey_binary="/tmp/valkey/src/valkey-server"
        )
    
    @patch('src.cli.ClusterBusFuzzer')
    def test_run_dsl_test_file_not_found(self, mock_fuzzer_class, capsys):
        """Test DSL test with non-existent file"""
        cli = FuzzerCLI()
        args = Mock(
            file="nonexistent.yaml",
            output=None,
            verbose=False
        )
        
        result = cli.run_dsl_test(args)
        
        assert result == 1
        captured = capsys.readouterr()
        assert "not found" in captured.out
    
    @patch('src.cli.ScenarioGenerator')
    @patch('src.cli.DSLLoader')
    def test_validate_dsl_success(self, mock_loader, mock_generator_class, tmp_path, capsys):
        """Test validating DSL configuration"""
        dsl_file = tmp_path / "test.yaml"
        dsl_file.write_text("scenario_id: test\ncluster:\n  num_shards: 3\n  replicas_per_shard: 1\noperations:\n  - type: failover\n    target_node: node-0\n")
        
        mock_dsl_config = Mock(config_text="test config")
        mock_loader.load_from_file.return_value = mock_dsl_config
        
        mock_scenario = Mock(
            scenario_id="test-123",
            seed=42,
            cluster_config=Mock(num_shards=3, replicas_per_shard=1),
            operations=[Mock()],
            chaos_config=Mock(chaos_type=Mock(value="process_kill"))
        )
        
        mock_generator = Mock()
        mock_generator.parse_dsl_config.return_value = mock_scenario
        mock_generator.validate_scenario.return_value = None
        mock_generator_class.return_value = mock_generator
        
        cli = FuzzerCLI()
        args = Mock(
            file=str(dsl_file),
            verbose=False
        )
        
        result = cli.validate_dsl(args)
        
        assert result == 0
        captured = capsys.readouterr()
        assert "valid" in captured.out.lower()
    
    def test_result_to_dict(self, mock_result):
        """Test converting ExecutionResult to dictionary"""
        cli = FuzzerCLI()
        result_dict = cli._result_to_dict(mock_result)
        
        assert result_dict['scenario_id'] == "test-scenario-123"
        assert result_dict['success'] is True
        assert result_dict['duration'] == 15.5
        assert result_dict['operations_executed'] == 3
        assert result_dict['chaos_events'] == 1
        assert result_dict['seed'] == 42


class TestCLIParser:
    """Test CLI argument parser"""
    
    def test_parse_commands(self):
        """Test parsing various CLI commands"""
        parser = create_parser()
        assert parser.prog == 'valkey-fuzzer'
    
    def test_parse_random_command(self):
        """Test parsing random command"""
        parser = create_parser()
        args = parser.parse_args(['cluster', '--seed', '42'])
        
        assert args.command == 'cluster'
        assert args.seed == 42
    
    def test_parse_random_with_iterations(self):
        """Test parsing random command with iterations"""
        parser = create_parser()
        args = parser.parse_args(['cluster', '--random', '--iterations', '5'])
        
        assert args.command == 'cluster'
        assert args.iterations == 5
    
    def test_parse_random_with_config(self):
        """Test parsing random command with config"""
        parser = create_parser()
        args = parser.parse_args(['cluster', '--random', '--config', 'config.yaml'])
        
        assert args.command == 'cluster'
        assert args.config == 'config.yaml'
    
    def test_parse_dsl_command(self):
        """Test parsing DSL command"""
        parser = create_parser()
        args = parser.parse_args(['cluster', '--dsl', 'test.yaml'])
        
        assert args.command == 'cluster'
        assert args.dsl == 'test.yaml'

    def test_parse_valkey_binary_option(self):
        """Test parsing explicit Valkey binary option"""
        parser = create_parser()
        args = parser.parse_args(['cluster', '--random', '--valkey-binary', '/tmp/valkey-server'])

        assert args.command == 'cluster'
        assert args.valkey_binary == '/tmp/valkey-server'
    
    def test_parse_output_options(self):
        """Test parsing output options"""
        parser = create_parser()
        args = parser.parse_args(['cluster', '--random', '--output', 'results.json', '--format', 'yaml'])
        
        assert args.command == 'cluster'
        assert args.output == 'results.json'
        assert args.format == 'yaml'
    
    def test_parse_export_dsl_option(self):
        """Test parsing export-dsl option"""
        parser = create_parser()
        args = parser.parse_args(['cluster', '--random', '--export-dsl', 'scenario.yaml'])
        
        assert args.command == 'cluster'
        assert args.export_dsl == 'scenario.yaml'


class TestCLIMain:
    """Test main CLI entry point"""
    
    @patch('src.cli.FuzzerCLI')
    def test_main_random_command(self, mock_cli_class):
        """Test main with random command"""
        mock_cli = Mock()
        mock_cli.run_random_test.return_value = 0
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'cluster', '--random', '--seed', '42']):
            result = main()
        
        assert result == 0
        mock_cli.run_random_test.assert_called_once()
    
    @patch('src.cli.FuzzerCLI')
    def test_main_dsl_command(self, mock_cli_class):
        """Test main with DSL command"""
        mock_cli = Mock()
        mock_cli.run_dsl_test.return_value = 0
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'cluster', '--dsl', 'test.yaml']):
            result = main()
        
        assert result == 0
        mock_cli.run_dsl_test.assert_called_once()
        dsl_args = mock_cli.run_dsl_test.call_args.args[0]
        assert dsl_args.valkey_binary is None

    @patch('src.cli.FuzzerCLI')
    def test_main_dsl_command_with_valkey_binary(self, mock_cli_class):
        """Test main with DSL command and explicit Valkey binary"""
        mock_cli = Mock()
        mock_cli.run_dsl_test.return_value = 0
        mock_cli_class.return_value = mock_cli

        with patch('sys.argv', ['cli', 'cluster', '--dsl', 'test.yaml', '--valkey-binary', '/tmp/valkey-server']):
            result = main()

        assert result == 0
        mock_cli.run_dsl_test.assert_called_once()
        dsl_args = mock_cli.run_dsl_test.call_args.args[0]
        assert dsl_args.valkey_binary == '/tmp/valkey-server'
    
    @patch('src.cli.FuzzerCLI')
    def test_main_validate_command(self, mock_cli_class):
        """Test main with validate command"""
        mock_cli = Mock()
        mock_cli.validate_dsl.return_value = 0
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'validate', 'test.yaml']):
            result = main()
        
        assert result == 0
        mock_cli.validate_dsl.assert_called_once()
    
    def test_main_no_command(self):
        """Test main with no command"""
        with patch('sys.argv', ['cli']):
            result = main()
        
        assert result == 1
    
    @patch('src.cli.FuzzerCLI')
    def test_main_keyboard_interrupt(self, mock_cli_class):
        """Test main handles keyboard interrupt"""
        mock_cli = Mock()
        mock_cli.run_random_test.side_effect = KeyboardInterrupt()
        mock_cli_class.return_value = mock_cli
        
        with patch('sys.argv', ['cli', 'cluster', '--random']):
            result = main()
        
        assert result == 130

def test_print_detailed_with_validation():
    """Test that _print_detailed_result displays detailed validation info"""
    from src.models import (
        StateValidationResult, ReplicationValidation, TopologyValidation,
        TopologyMismatch
    )
    import io
    import sys

    cli = FuzzerCLI()

    replication = ReplicationValidation(
        success=False,
        all_replicas_synced=False,
        max_lag=10.5,
        lagging_replicas=[],
        disconnected_replicas=["127.0.0.1:7001"],
        error_message="1 replica disconnected"
    )

    topology = TopologyValidation(
        success=False,
        expected_primaries=3,
        actual_primaries=2,
        expected_replicas=3,
        actual_replicas=3,
        topology_mismatches=[
            TopologyMismatch(
                mismatch_type="primary_count",
                node_id="cluster",
                expected="3 primaries",
                actual="2 primaries"
            )
        ],
        error_message="Primary count mismatch"
    )

    validation = StateValidationResult(
        overall_success=False,
        validation_timestamp=1000.0,
        validation_duration=2.5,
        replication=replication,
        cluster_status=None,
        slot_coverage=None,
        topology=topology,
        view_consistency=None,
        data_consistency=None,
        failed_checks=["replication", "topology"],
        error_messages=["1 replica disconnected", "Primary count mismatch"]
    )

    result = ExecutionResult(
        scenario_id="test-123",
        success=False,
        start_time=1000.0,
        end_time=1015.0,
        operations_executed=3,
        chaos_events=[],
        seed=42,
        final_validation=validation
    )

    captured_output = io.StringIO()
    sys.stdout = captured_output

    try:
        cli._print_detailed_result(result)
        output = captured_output.getvalue()
    finally:
        sys.stdout = sys.__stdout__

    assert "Final Validation Details:" in output
    assert "Replication: FAIL" in output
    assert "Topology: FAIL" in output


def test_result_to_dict_includes_validation():
    """Test that _result_to_dict includes validation data"""
    from src.models import StateValidationResult, ReplicationValidation, TopologyValidation

    cli = FuzzerCLI()

    # Create result with validation
    replication = ReplicationValidation(
        success=False,
        all_replicas_synced=False,
        max_lag=10.5,
        lagging_replicas=[],
        disconnected_replicas=["127.0.0.1:7001"],
        error_message="1 replica disconnected"
    )

    topology = TopologyValidation(
        success=True,
        expected_primaries=3,
        actual_primaries=3,
        expected_replicas=3,
        actual_replicas=3,
        topology_mismatches=[],
        error_message=None
    )

    validation = StateValidationResult(
        overall_success=False,
        validation_timestamp=1000.0,
        validation_duration=2.5,
        replication=replication,
        cluster_status=None,
        slot_coverage=None,
        topology=topology,
        view_consistency=None,
        data_consistency=None,
        failed_checks=["replication"],
        error_messages=["1 replica disconnected"]
    )

    result = ExecutionResult(
        scenario_id="test-123",
        success=False,
        start_time=1000.0,
        end_time=1015.0,
        operations_executed=3,
        chaos_events=[],
        seed=42,
        final_validation=validation
    )

    # Convert to dict
    result_dict = cli._result_to_dict(result)

    # Verify final validation data is included
    assert 'final_validation' in result_dict
    assert result_dict['final_validation']['overall_success'] is False
    assert 'replication' in result_dict['final_validation']['failed_checks']
    assert result_dict['final_validation']['checks']['replication']['success'] is False
    assert result_dict['final_validation']['checks']['replication']['error'] == "1 replica disconnected"
    assert result_dict['final_validation']['checks']['replication']['max_lag'] == 10.5
    assert result_dict['final_validation']['checks']['replication']['disconnected_replicas_count'] == 1

    # Verify topology check is included
    assert result_dict['final_validation']['checks']['topology']['success'] is True
    assert result_dict['final_validation']['checks']['topology']['expected_primaries'] == 3
    assert result_dict['final_validation']['checks']['topology']['actual_primaries'] == 3


def test_validation_serialization_all_checks():
    """Test validation serialization with all check types"""
    from src.models import (
        StateValidationResult, ViewConsistencyValidation, DataConsistencyValidation,
        SlotCoverageValidation, ViewDiscrepancy, DataInconsistency, SlotConflict
    )

    cli = FuzzerCLI()

    validation = StateValidationResult(
        overall_success=False,
        validation_timestamp=1000.0,
        validation_duration=3.5,
        replication=None,
        cluster_status=None,
        slot_coverage=SlotCoverageValidation(
            success=False,
            total_slots_assigned=16380,
            unassigned_slots=[100, 101],
            conflicting_slots=[SlotConflict(slot=200, conflicting_nodes=["n1", "n2"])],
            slot_distribution={},
            error_message="Slot issues"
        ),
        topology=None,
        view_consistency=ViewConsistencyValidation(
            success=False,
            nodes_checked=6,
            consistent_views=False,
            split_brain_detected=True,
            view_discrepancies=[ViewDiscrepancy(
                discrepancy_type="membership",
                node_reporting="127.0.0.1:7000",
                subject_node="127.0.0.1:7001",
                expected_value="primary",
                actual_value="failed"
            )],
            consensus_percentage=66.7,
            error_message="Split brain"
        ),
        data_consistency=DataConsistencyValidation(
            success=False,
            test_keys_checked=100,
            missing_keys=["k1"],
            inconsistent_keys=[],
            unreachable_keys=[],
            error_message="Missing keys"
        ),
        failed_checks=["slot_coverage", "view_consistency", "data_consistency"],
        error_messages=[]
    )

    validation_dict = cli._validation_to_dict(validation)

    # Verify all check types serialize correctly
    assert validation_dict['checks']['slot_coverage']['unassigned_slots_count'] == 2
    assert validation_dict['checks']['view_consistency']['split_brain_detected'] is True
    assert validation_dict['checks']['data_consistency']['missing_keys_count'] == 1
