#!/usr/bin/env python3
"""
Command-line interface for the Cluster Bus Fuzzer
Provides commands for running random tests, DSL-based tests, and validating configurations.
"""
import sys
import argparse
import json
import yaml
import traceback
import time
import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
from .main import ClusterBusFuzzer
from .fuzzer_engine import DSLLoader
from .fuzzer_engine.test_case_generator import ScenarioGenerator
from .models import ExecutionResult, StateValidationResult
from .utils.cluster_utils import cleanup_logs


class FuzzerCLI:
    """Command-line interface for the Cluster Bus Fuzzer"""
    
    def __init__(self):
        self.fuzzer = ClusterBusFuzzer()
        self.config = {}
    
    def load_config_file(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file"""
        path = Path(config_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(path, 'r') as f:
            if path.suffix in ['.yaml', '.yml']:
                return yaml.safe_load(f)
            elif path.suffix == '.json':
                return json.load(f)
            else:
                raise ValueError(f"Unsupported config format: {path.suffix}")
    
    def run_random_test(self, args) -> int:
        """Execute a randomized test scenario"""
        self._print_header("Random Test Scenario")
        
        # Load config if provided
        if args.config:
            try:
                self.config = self.load_config_file(args.config)
                print(f"Loaded configuration from {args.config}")
            except Exception as e:
                print(f"Error: Failed to load config file: {e}")
                print(f"\nConfig file must be YAML (.yaml, .yml) or JSON (.json)")
                print(f"Example: valkey-fuzzer cluster --seed 42 --config config.yaml")
                return 1
        
        seed = args.seed
        if seed:
            print(f"Seed: {seed} (reproducible)")
        else:
            print("Seed: Random")
        
        # Default number of iterations is 1
        if args.iterations:
            print(f"Iterations: {args.iterations}")
        
        if args.export_dsl:
            print(f"DSL Export: {args.export_dsl}")
        
        print()
        
        # Run test(s)
        results = []
        for i in range(args.iterations):
            if args.iterations > 1:
                print(f"\n--- Iteration {i + 1}/{args.iterations} ---")
            
            try:
                result = self.fuzzer.run_random_test(seed=seed)
                results.append(result)
                
                if args.verbose:
                    self._print_detailed_result(result)
                else:
                    self._print_summary_result(result)
                
                # For multiple iterations with same seed, increment seed
                if seed and args.iterations > 1:
                    seed += 1
                    
            except Exception as e:
                print(f"Test failed with exception: {e}")
                if args.verbose:
                    traceback.print_exc()
                return 1
        
        # Print aggregate results for multiple iterations
        if args.iterations > 1:
            self._print_aggregate_results(results)
        
        # Save results if output specified
        if args.output:
            self._save_results(results, args.output, args.format)
        
        # Export scenario to DSL if requested
        if args.export_dsl and self.fuzzer.last_scenario:
            try:
                self._export_scenario_to_dsl(self.fuzzer.last_scenario, args.export_dsl)
            except Exception as e:
                print(f"\nError: Failed to export scenario to DSL: {e}")
                return 1
        
        # Return success if all tests passed
        return 0 if all(r.success for r in results) else 1
    
    def run_dsl_test(self, args) -> int:
        """Execute a DSL-based test scenario"""
        self._print_header(f"DSL Test: {args.file}")
        
        dsl_path = Path(args.file)
        if not dsl_path.exists():
            print(f"Error: DSL file not found: {args.file}")
            print(f"\nMake sure the file path is correct.")
            print(f"Example: valkey-fuzzer cluster --dsl examples/simple_failover.yaml")
            return 1
        
        try:
            # Load DSL configuration
            dsl_config = DSLLoader.load_from_file(str(dsl_path))
            print(f"Loaded DSL from {args.file}")
            print()
            
            # Run test
            result = self.fuzzer.run_dsl_test(dsl_config)
            
            if args.verbose:
                self._print_detailed_result(result)
            else:
                self._print_summary_result(result)
            
            # Save results if output specified
            if args.output:
                self._save_results([result], args.output, args.format)
            
            return 0 if result.success else 1
            
        except Exception as e:
            print(f"Error: DSL test failed: {e}")
            print(f"\nTry validating your DSL file first: valkey-fuzzer validate {args.file}")
            if args.verbose:
                traceback.print_exc()
            return 1
    
    def validate_dsl(self, args) -> int:
        """Validate a DSL configuration file"""
        self._print_header(f"Validating DSL: {args.file}")
        
        dsl_path = Path(args.file)
        if not dsl_path.exists():
            print(f"Error: DSL file not found: {args.file}")
            print(f"\nMake sure the file path is correct.")
            print(f"Example: valkey-fuzzer validate examples/simple_failover.yaml")
            return 1
        
        try:
            # Load DSL
            dsl_config = DSLLoader.load_from_file(str(dsl_path))
            print("DSL file loaded successfully")
            
            # Parse and validate
            generator = ScenarioGenerator()
            
            scenario = generator.parse_dsl_config(dsl_config.config_text)
            print("DSL parsed successfully")
            
            generator.validate_scenario(scenario)
            print("Scenario validated successfully")
            
            print("\n" + "=" * 60)
            print("Scenario Summary")
            print("=" * 60)
            print(f"ID: {scenario.scenario_id}")
            print(f"Seed: {scenario.seed}")
            print(f"Cluster: {scenario.cluster_config.num_shards} shards, "
                  f"{scenario.cluster_config.replicas_per_shard} replicas per shard")
            print(f"Operations: {len(scenario.operations)}")
            print(f"Chaos Type: {scenario.chaos_config.chaos_type.value}")
            
            if args.verbose:
                print("\nOperations:")
                for i, op in enumerate(scenario.operations, 1):
                    print(f"  {i}. {op.type.value} on {op.target_node}")
            
            print("\nDSL configuration is valid!")
            return 0
            
        except Exception as e:
            print(f"\nError: Validation failed: {e}")
            print(f"\nCheck your DSL file syntax. See examples in the examples/ directory.")
            if args.verbose:
                traceback.print_exc()
            return 1
    
    def _print_header(self, title: str):
        """Print formatted header"""
        print()
        print("=" * 80)
        print(title)
        print("=" * 80)
        print()
    
    def _print_summary_result(self, result: ExecutionResult):
        """Print summary of test result"""
        time.sleep(0.1)
        
        status = "PASSED" if result.success else "FAILED"
        duration = result.end_time - result.start_time
        
        print(f"\nScenario: {result.scenario_id}")
        print(f"Status: {status}")
        print(f"Duration: {duration:.2f}s")
        print(f"Operations: {result.operations_executed}")
        print(f"Chaos Events: {len(result.chaos_events)}")
        
        if result.seed:
            print(f"Seed: {result.seed} (use to reproduce)")
        
        # Print final validation summary
        if result.final_validation:
            val = result.final_validation
            val_status = "PASSED" if val.overall_success else "FAILED"
            print(f"\nFinal Validation: {val_status}")

            if not val.overall_success and val.failed_checks:
                print(f"Failed Checks: {', '.join(val.failed_checks)}")

            if val.error_messages:
                print(f"Validation Errors: {len(val.error_messages)}")

        if result.error_message:
            print(f"Error: {result.error_message}")
    
    def _print_detailed_result(self, result: ExecutionResult):
        """Print detailed test result when --verbose flag is specified"""
        self._print_summary_result(result)
        
        # Print chaos events
        if result.chaos_events:
            print("\nChaos Events:")
            for event in result.chaos_events:
                status = "[PASS]" if event.success else "[FAIL]"
                duration = (event.end_time - event.start_time) if event.end_time else 0
                print(f"  {status} {event.chaos_type.value} on {event.target_node} "
                      f"({duration:.2f}s)")
                if event.error_message:
                    print(f"    Error: {event.error_message}")
        
        # Print detailed validation results
        if result.final_validation:
            print("\nFinal Validation Details:")
            val = result.final_validation

            # Print individual check results
            checks = []
            if val.replication:
                status = "PASS" if val.replication.success else "FAIL"
                checks.append(f"  Replication: {status}")
                if not val.replication.success and val.replication.error_message:
                    checks.append(f"    → {val.replication.error_message}")

            if val.cluster_status:
                status = "PASS" if val.cluster_status.success else "FAIL"
                checks.append(f"  Cluster Status: {status}")
                if not val.cluster_status.success and val.cluster_status.error_message:
                    checks.append(f"    → {val.cluster_status.error_message}")

            if val.slot_coverage:
                status = "PASS" if val.slot_coverage.success else "FAIL"
                checks.append(f"  Slot Coverage: {status}")
                if not val.slot_coverage.success and val.slot_coverage.error_message:
                    checks.append(f"    → {val.slot_coverage.error_message}")

            if val.topology:
                status = "PASS" if val.topology.success else "FAIL"
                checks.append(f"  Topology: {status}")
                if not val.topology.success and val.topology.error_message:
                    checks.append(f"    → {val.topology.error_message}")

            if val.view_consistency:
                status = "PASS" if val.view_consistency.success else "FAIL"
                checks.append(f"  View Consistency: {status}")
                if not val.view_consistency.success and val.view_consistency.error_message:
                    checks.append(f"    → {val.view_consistency.error_message}")

            if val.data_consistency:
                status = "PASS" if val.data_consistency.success else "FAIL"
                checks.append(f"  Data Consistency: {status}")
                if not val.data_consistency.success and val.data_consistency.error_message:
                    checks.append(f"    → {val.data_consistency.error_message}")

            if val.log_validation:
                status = "PASS" if val.log_validation.success else "FAIL"
                checks.append(f"  Log Validation: {status}")
                if not val.log_validation.success:
                    error_count = len([f for f in val.log_validation.findings if hasattr(f, 'severity') and f.severity == 'error'])
                    warning_count = len([f for f in val.log_validation.findings if hasattr(f, 'severity') and f.severity == 'warning'])
                    checks.append(f"    → {error_count} errors, {warning_count} warnings in {val.log_validation.shards_checked} shards")

            for check in checks:
                print(check)

            # Print all error messages if any
            if val.error_messages:
                print("\n  Validation Error Messages:")
                for msg in val.error_messages:
                    print(f"    → {msg}")
    
    def _print_aggregate_results(self, results: list):
        """Print aggregate statistics for multiple test runs"""
        print("\n" + "=" * 80)
        print("Aggregate Results")
        print("=" * 80)
        
        total = len(results)
        passed = sum(1 for r in results if r.success)
        failed = total - passed
        
        avg_duration = sum(r.end_time - r.start_time for r in results) / total
        avg_operations = sum(r.operations_executed for r in results) / total
        avg_chaos = sum(len(r.chaos_events) for r in results) / total
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed} ({passed/total*100:.1f}%)")
        print(f"Failed: {failed} ({failed/total*100:.1f}%)")
        print(f"Average Duration: {avg_duration:.2f}s")
        print(f"Average Operations: {avg_operations:.1f}")
        print(f"Average Chaos Events: {avg_chaos:.1f}")
    
    def _export_scenario_to_dsl(self, scenario, export_path: str):
        """Export scenario to DSL YAML file"""
        export_path = Path(export_path)
        export_path.parent.mkdir(parents=True, exist_ok=True)
        
        DSLLoader.save_scenario_as_dsl(scenario, export_path)
        print(f"\nScenario exported to DSL: {export_path}")
    
    def _save_results(self, results: list, output_path: str, format: str):
        """Save test results to file"""
        try:
            output = Path(output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert results to dict
            data = {
                'timestamp': datetime.now().isoformat(),
                'total_tests': len(results),
                'passed': sum(1 for r in results if r.success),
                'failed': sum(1 for r in results if not r.success),
                'results': [self._result_to_dict(r) for r in results]
            }
            
            with open(output, 'w') as f:
                if format == 'json':
                    json.dump(data, f, indent=2)
                elif format == 'yaml':
                    yaml.dump(data, f, default_flow_style=False)
            
            print(f"\nResults saved to {output_path}")
            
        except Exception as e:
            print(f"\nFailed to save results: {e}")
    
    def _result_to_dict(self, result: ExecutionResult) -> Dict[str, Any]:
        """Convert ExecutionResult to dictionary"""
        data = {
            'scenario_id': result.scenario_id,
            'success': result.success,
            'duration': result.end_time - result.start_time,
            'operations_executed': result.operations_executed,
            'chaos_events': len(result.chaos_events),
            'seed': result.seed,
            'error_message': result.error_message
        }

        # Add final validation results if available
        if result.final_validation:
            data['final_validation'] = self._validation_to_dict(result.final_validation)

        # Add per-operation validation results if available
        if result.validation_results:
            data['per_operation_validations'] = [self._validation_to_dict(val) for val in result.validation_results]

        return data

    def _validation_to_dict(self, val: StateValidationResult) -> Dict[str, Any]:
        """Convert StateValidationResult to dictionary"""
        validation_data = {
            'overall_success': val.overall_success,
            'failed_checks': val.failed_checks,
            'error_messages': val.error_messages,
            'duration': val.validation_duration,
            'timestamp': val.validation_timestamp,
            'checks': {}
        }

        # Add individual check results
        if val.replication:
            validation_data['checks']['replication'] = {
                'success': val.replication.success,
                'all_replicas_synced': val.replication.all_replicas_synced,
                'max_lag': val.replication.max_lag,
                'lagging_replicas_count': len(val.replication.lagging_replicas),
                'disconnected_replicas_count': len(val.replication.disconnected_replicas),
                'error': val.replication.error_message
            }

        if val.cluster_status:
            validation_data['checks']['cluster_status'] = {
                'success': val.cluster_status.success,
                'cluster_state': val.cluster_status.cluster_state,
                'has_quorum': val.cluster_status.has_quorum,
                'nodes_in_fail_state_count': len(val.cluster_status.nodes_in_fail_state),
                'error': val.cluster_status.error_message
            }

        if val.slot_coverage:
            validation_data['checks']['slot_coverage'] = {
                'success': val.slot_coverage.success,
                'total_slots_assigned': val.slot_coverage.total_slots_assigned,
                'unassigned_slots_count': len(val.slot_coverage.unassigned_slots),
                'conflicting_slots_count': len(val.slot_coverage.conflicting_slots),
                'error': val.slot_coverage.error_message
            }

        if val.topology:
            validation_data['checks']['topology'] = {
                'success': val.topology.success,
                'expected_primaries': val.topology.expected_primaries,
                'actual_primaries': val.topology.actual_primaries,
                'expected_replicas': val.topology.expected_replicas,
                'actual_replicas': val.topology.actual_replicas,
                'mismatches_count': len(val.topology.topology_mismatches),
                'error': val.topology.error_message
            }

        if val.view_consistency:
            validation_data['checks']['view_consistency'] = {
                'success': val.view_consistency.success,
                'nodes_checked': val.view_consistency.nodes_checked,
                'consistent_views': val.view_consistency.consistent_views,
                'split_brain_detected': val.view_consistency.split_brain_detected,
                'consensus_percentage': val.view_consistency.consensus_percentage,
                'discrepancies_count': len(val.view_consistency.view_discrepancies),
                'error': val.view_consistency.error_message
            }

        if val.data_consistency:
            validation_data['checks']['data_consistency'] = {
                'success': val.data_consistency.success,
                'test_keys_checked': val.data_consistency.test_keys_checked,
                'missing_keys_count': len(val.data_consistency.missing_keys),
                'inconsistent_keys_count': len(val.data_consistency.inconsistent_keys),
                'unreachable_keys_count': len(val.data_consistency.unreachable_keys),
                'error': val.data_consistency.error_message
            }

        if val.log_validation:
            validation_data['checks']['log_validation'] = {
                'success': val.log_validation.success,
                'shards_checked': val.log_validation.shards_checked,
                'findings_count': len(val.log_validation.findings),
            }

        return validation_data


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for CLI"""
    parser = argparse.ArgumentParser(
        prog='valkey-fuzzer',
        description='Valkey Cluster Bus Fuzzer - Test Valkey cluster robustness through chaos engineering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a random cluster test
  valkey-fuzzer cluster --random
  
  # Run with specific seed for reproducibility
  valkey-fuzzer cluster --seed 42
  
  # Run multiple iterations
  valkey-fuzzer cluster --random --iterations 10
  
  # Run with configuration file and output results to another file
  valkey-fuzzer cluster --seed 42 --config config.yaml --output results.json

  # Verbose output
  valkey-fuzzer cluster --seed 42 --verbose
  
  # Run DSL-based test
  valkey-fuzzer cluster --dsl examples/simple_failover.yaml

  # Export scenario to DSL File
  valkey-fuzzer cluster --random --export-dsl dsl_file.yml
  
  # Validate DSL file
  valkey-fuzzer validate examples/simple_failover.yaml
        """
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Valkey Fuzzer 0.1.0'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Cluster test command
    cluster_parser = subparsers.add_parser(
        'cluster',
        help='Run cluster-related fuzzer tests'
    )
    cluster_parser.add_argument(
        '--random',
        action='store_true',
        help='Run with random scenario'
    )
    cluster_parser.add_argument(
        '--seed',
        type=int,
        help='Seed for reproducibility'
    )
    cluster_parser.add_argument(
        '--dsl',
        type=str,
        metavar='FILE',
        help='Path to DSL YAML file'
    )
    cluster_parser.add_argument(
        '--iterations',
        type=int,
        default=1,
        help='Number of test iterations to run (default: 1)'
    )
    cluster_parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file (YAML or JSON)'
    )
    cluster_parser.add_argument(
        '--output',
        type=str,
        help='Path to save test results'
    )
    cluster_parser.add_argument(
        '--format',
        choices=['json', 'yaml'],
        default='json',
        help='Output format for results (default: json)'
    )
    cluster_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    cluster_parser.add_argument(
        '--export-dsl',
        type=str,
        metavar='FILE',
        help='Export the generated scenario to a DSL YAML file'
    )
    
    # Validate command
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate a DSL configuration file'
    )
    validate_parser.add_argument(
        'file',
        help='Path to DSL YAML file'
    )
    validate_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    # Cleanup logs command
    cleanup_parser = subparsers.add_parser(
        'cleanup-logs',
        help='Clean up log files for a specific seed'
    )
    cleanup_parser.add_argument(
        'seed',
        type=int,
        help='Seed value to clean up logs for'
    )
    cleanup_parser.add_argument(
        '--data-dir',
        type=str,
        help='Base data directory (default: /tmp/valkey-fuzzer)'
    )
    cleanup_parser.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation prompt'
    )
    
    return parser


def main():
    """Main entry point for CLI"""

    logging.basicConfig(level=logging.INFO, format='%(levelname)-5s | %(filename)s:%(lineno)-3d | %(message)s', handlers=[logging.StreamHandler()])
    
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        print("Error: No command specified\n")
        parser.print_help()
        print("\nCommon commands:")
        print("  valkey-fuzzer cluster --random          # Run random test")
        print("  valkey-fuzzer cluster --seed 42         # Run with specific seed")
        print("  valkey-fuzzer validate <file.yaml>      # Validate DSL file")
        return 1
    
    cli = FuzzerCLI()
    
    try:
        if args.command == 'cluster':
            # Validate cluster command arguments
            if not args.dsl and args.seed is None and not args.random:
                print("Error: cluster command requires either --dsl, --seed, or --random\n")
                print("Examples:")
                print("  valkey-fuzzer cluster --random")
                print("  valkey-fuzzer cluster --seed 42")
                print("  valkey-fuzzer cluster --dsl test.yaml")
                return 1
            
            if args.dsl:
                dsl_args = type('obj', (object,), {
                    'file': args.dsl,
                    'output': args.output,
                    'format': args.format,
                    'verbose': args.verbose
                })
                return cli.run_dsl_test(dsl_args)
            else:
                return cli.run_random_test(args)
        elif args.command == 'validate':
            return cli.validate_dsl(args)
        elif args.command == 'cleanup-logs':
            return cleanup_logs(args.seed, args.data_dir or "/tmp/valkey-fuzzer", args.force)
    except KeyboardInterrupt:
        print("\n\nValkey Fuzzer process was interrupted by user")
        return 130
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        if hasattr(args, 'verbose') and args.verbose:
            traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
