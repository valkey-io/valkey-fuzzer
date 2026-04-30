#!/usr/bin/env python3
"""
Example script demonstrating how to use the Cluster Bus Fuzzer
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fuzzer_engine import DSLLoader
from src.main import ClusterBusFuzzer


def run_random_test(seed=None):
    """Run a randomized test scenario"""
    print("=" * 80)
    print("Running Random Test Scenario")
    print("=" * 80)

    fuzzer = ClusterBusFuzzer()

    if seed:
        print(f"Using seed: {seed}")
    else:
        print("Using random seed")

    result = fuzzer.run_random_test(seed=seed)

    print("\n" + "=" * 80)
    print("Test Results")
    print("=" * 80)
    print(f"Scenario ID: {result.scenario_id}")
    print(f"Success: {result.success}")
    print(f"Duration: {result.end_time - result.start_time:.2f}s")
    print(f"Operations Executed: {result.operations_executed}")
    print(f"Chaos Events: {len(result.chaos_events)}")

    if result.seed:
        print(f"\nReproduction Seed: {result.seed}")
        print("Use this seed to reproduce this exact test scenario")

    if result.error_message:
        print(f"\nError: {result.error_message}")

    return result


def run_dsl_test(dsl_file):
    """Run a DSL-based test scenario"""
    print("=" * 80)
    print(f"Running DSL Test: {dsl_file}")
    print("=" * 80)

    fuzzer = ClusterBusFuzzer()

    try:
        # Load DSL configuration
        dsl_config = DSLLoader.load_from_file(dsl_file)

        # Run test
        result = fuzzer.run_dsl_test(dsl_config)

        print("\n" + "=" * 80)
        print("Test Results")
        print("=" * 80)
        print(f"Scenario ID: {result.scenario_id}")
        print(f"Success: {result.success}")
        print(f"Duration: {result.end_time - result.start_time:.2f}s")
        print(f"Operations Executed: {result.operations_executed}")
        print(f"Chaos Events: {len(result.chaos_events)}")

        if result.error_message:
            print(f"\nError: {result.error_message}")

        return result

    except Exception as e:
        print(f"\nError loading or executing DSL: {e}")
        return None


def validate_dsl(dsl_file):
    """Validate a DSL configuration file"""
    print("=" * 80)
    print(f"Validating DSL: {dsl_file}")
    print("=" * 80)

    try:
        # Load DSL
        dsl_config = DSLLoader.load_from_file(dsl_file)
        print("DSL file loaded successfully")

        # Parse and validate
        from src.fuzzer_engine import ScenarioGenerator

        generator = ScenarioGenerator()

        scenario = generator.parse_dsl_config(dsl_config.config_text)
        print("DSL parsed successfully")

        generator.validate_scenario(scenario)
        print("Scenario validated successfully")

        print("\nScenario Summary:")
        print(f"  ID: {scenario.scenario_id}")
        print(f"  Seed: {scenario.seed}")
        print(
            f"  Cluster: {scenario.cluster_config.num_shards} shards, "
            f"{scenario.cluster_config.replicas_per_shard} replicas per shard"
        )
        print(f"  Operations: {len(scenario.operations)}")
        print(f"  Chaos Type: {scenario.chaos_config.chaos_type.value}")

        print("\nDSL configuration is valid!")
        return True

    except Exception as e:
        print(f"\nValidation failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Cluster Bus Fuzzer - Test Valkey cluster bus robustness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a random test
  python run_fuzzer.py random

  # Run a random test with specific seed
  python run_fuzzer.py random --seed 42

  # Run a DSL-based test
  python run_fuzzer.py dsl example_scenario.yaml

  # Validate a DSL file
  python run_fuzzer.py validate example_scenario.yaml
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Random test command
    random_parser = subparsers.add_parser("random", help="Run a randomized test")
    random_parser.add_argument("--seed", type=int, help="Seed for reproducibility")

    # DSL test command
    dsl_parser = subparsers.add_parser("dsl", help="Run a DSL-based test")
    dsl_parser.add_argument("file", help="Path to DSL YAML file")

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate a DSL file")
    validate_parser.add_argument("file", help="Path to DSL YAML file")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "random":
        result = run_random_test(seed=args.seed)
        return 0 if result.success else 1

    elif args.command == "dsl":
        result = run_dsl_test(args.file)
        return 0 if result and result.success else 1

    elif args.command == "validate":
        success = validate_dsl(args.file)
        return 0 if success else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
