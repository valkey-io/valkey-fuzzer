# valkey-fuzzer

A comprehensive testing framework designed to validate and test the robustness of Valkey through automated chaos engineering and randomized testing scenarios.

## Overview

valkey-fuzzer helps identify bugs that are difficult to catch through traditional integration testing by simulating real-world failure scenarios and edge cases. The initial focus is on cluster bus communication and cluster operations, with an extensible architecture supporting future expansion to other Valkey components and testing scenarios.

### Key Features

- **Reproducible Testing**: Generate deterministic test scenarios using configurable seeds
- **DSL-Based Configuration**: Create and reproduce specific test scenarios using a domain-specific language
- **Chaos Engineering**: Inject process failures coordinated with cluster operations
- **Comprehensive Validation**: Monitor cluster state, data consistency, and performance metrics
- **Extensible Architecture**: Modular design supporting future expansion of operations and chaos types

## Architecture

The system follows a modular architecture with five core components:

### Components

- **Fuzzer Engine**: Central orchestrator that creates scenarios, coordinates test execution, and manages test scenarios
- **Cluster Orchestrator**: Manages Valkey cluster lifecycle including node spawning, cluster formation, and configuration
- **Chaos Engine**: Injects process failures and coordinates chaos timing with cluster operations
- **State Validator**: Verifies cluster health, slot coverage, replication status, data consistency and log messages across nodes
- **Valkey Client**: Generates realistic workload using valkey-benchmark during testing

## Installation

### Prerequisites

- Python 3.9+

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd valkey-fuzzer

# Install dependencies
pip install -r requirements.txt

# Verify Valkey installation
valkey-server --version
valkey-benchmark --version
```

## Usage

### CLI Help

View all available commands and options:

```bash
# Show all commands and options
valkey-fuzzer --help

# Show cluster command options
valkey-fuzzer cluster --help

# Show validate command options
valkey-fuzzer validate --help
```

### Random Test Execution

Generate and execute randomized test scenarios:

```bash
# Run with random seed
valkey-fuzzer cluster --random

# Run with specific seed for reproducibility
valkey-fuzzer cluster --seed 12345

# Run multiple iterations
valkey-fuzzer cluster --random --iterations 10

# Run with configuration file
valkey-fuzzer cluster --seed 42 --config config.yaml --output results.json

# Verbose output
valkey-fuzzer cluster --seed 42 --verbose
valkey-fuzzer cluster --random --verbose

# Run with iterations 
valkey-fuzzer cluster --seed 42 --iterations 2 --verbose

```

### DSL-Based Test Execution

Execute predefined test scenarios using DSL configuration:

```bash
# Run DSL-based test
valkey-fuzzer cluster --dsl test_scenario.yaml

# Validate DSL configuration
valkey-fuzzer validate test_scenario.yaml
```

### Exporting Random Scenarios

Capture randomly generated test scenarios as DSL files for reproducibility and sharing:

```bash
# Export a random scenario
valkey-fuzzer cluster --random --export-dsl captured_scenario.yaml

# Export with specific seed
valkey-fuzzer cluster --seed 12345 --export-dsl seed_12345_scenario.yaml

# Reproduce the exact scenario
valkey-fuzzer cluster --dsl captured_scenario.yaml
```

### DSL Configuration Examples

#### Basic Failover Test

```yaml
# simple_failover.yaml
scenario_id: "basic_failover_test"
seed: 42
  
cluster:
  num_shards: 3
  replicas_per_shard: 1
  
operations:
  - type: "failover"
    target: "primary"
    shard: 1
    timing: "immediate"
    
chaos:
  - type: "process_kill"
    signal: "SIGKILL"
    target: "primary"
    shard: 2
    coordination: "before_failover"
    delay: 5.0

state_validation:
  check_slot_coverage: true
  check_replication: true
  check_data_consistency: true
  validation_timeout: 30.0
```

#### Complex Multi-Operation Test

```yaml
scenario_id: "complex_chaos_test"
seed: 98765
  
cluster:
  num_shards: 8
  replicas_per_shard: 2
  
operations:
  - type: "failover"
    target: "primary"
    shard: 3
    timing: "delayed"
    delay: 10.0
    
  - type: "failover"
    target: "primary" 
    shard: 7
    timing: "immediate"
    
chaos:
  - type: "process_kill"
    signal: "SIGTERM"
    target: "replica"
    shard: 1
    coordination: "during_operation"
    
  - type: "process_kill"
    signal: "SIGKILL"
    target: "primary"
    shard: 5
    coordination: "after_operation"
    delay: 15.0

workload:
  enabled: true
  pattern: "mixed"
  intensity: "medium"
  duration: 120.0

state_validation:
  check_slot_coverage: true
  check_replication: true
  check_data_consistency: true
  validation_timeout: 45.0
  replication_config:
    max_acceptable_lag: 5.0
```

### Expected Output Formats

#### Test Execution Summary

```
=== valkey-fuzzer Test Results ===
Scenario: simple-failover-test
Status: PASSED
Duration: 30.88s
Operations: 1
Chaos Events: 0
Seed: 12345 (use to reproduce)

Final Validation: PASSED

Final Validation Details:
  Replication: PASS
  Cluster Status: PASS
  Slot Coverage: PASS
  Topology: PASS
  View Consistency: PASS
```

#### Failure Report

```
=== Test Failure Report ===
Scenario: 4226257627
Status: FAILED
Duration: 211.85s
Operations: 2
Chaos Events: 2
Seed: 4226257627 (use to reproduce)

Final Validation: FAILED
Failed Checks: slot_coverage, topology, data_consistency
Validation Errors: 3

Chaos Events:
  [PASS] process_kill on node-0 (8.40s)
  [PASS] process_kill on node-1 (5.43s)

Final Validation Details:
  Replication: PASS
  Cluster Status: PASS
  Slot Coverage: FAIL
    → CRITICAL: 5461 slots still assigned to killed nodes. Killed nodes: {'127.0.0.1:7623', '127.0.0.1:7622'}. This indicates failover did not complete or slots were not reassigned. Affected slots: 0-5460
  Topology: FAIL
    → Topology validation failed in strict mode: 2 mismatch(es) found
  View Consistency: PASS
  Data Consistency: FAIL
    → 35 test key(s) unreachable (threshold: 10)

  Validation Error Messages:
    • Slot Coverage: CRITICAL: 5461 slots still assigned to killed nodes. Killed nodes: {'127.0.0.1:7623', '127.0.0.1:7622'}. This indicates failover did not complete or slots were not reassigned. Affected slots: 0-5460
    • Topology: Topology validation failed in strict mode: 2 mismatch(es) found
    • Data Consistency: 35 test key(s) unreachable (threshold: 10)

Reproduction Command:
valkey-fuzzer cluster --seed 4226257627
```

## Initial Prototype Scope

The current implementation focuses on:

### Supported Operations
- **Failover Operations**: Primary node failover with replica promotion
- **Process Chaos**: Random process termination (SIGKILL, SIGTERM)

### Supported Configurations
- **Cluster Sizes**: 3-16 shards
- **Replication**: 0-2 replicas per shard
- **Deployment**: Single metal instance with multiple processes

### Validation Capabilities
- Slot coverage and assignment validation
- Replica synchronization monitoring
- Data consistency verification
- Convergence time and replication lag tracking
- Actual Valkey node log validation

## Future Extensibility

The architecture is designed to support expansion in multiple dimensions:

### Additional Operations
- `add_replica`: Add new replica nodes to existing shards
- `remove_replica`: Remove replica nodes from shards
- `reshard`: Redistribute slots across shards
- `scale_out`: Add new shards to the cluster
- `scale_in`: Remove shards and redistribute data
- `configuration_change`: Modify cluster configuration parameters

### Enhanced Chaos Engineering
- **Network Chaos**: Packet filtering, latency simulation, selective packet drops
- **Resource Chaos**: Memory pressure, CPU throttling, disk I/O limitations
- **Time Chaos**: Clock skew simulation, network time protocol disruption

### Extended Validation
- **Performance Regression**: Benchmark comparison across test runs
- **Memory Leak Detection**: Long-running test memory profiling
- **Security Validation**: Authentication and authorization testing

### Mixed Clusters
- Test the fuzzer on different versions of Valkey
- Create chaos while the cluster is upgrading from an older branch to unstable

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with comprehensive tests
4. Submit a pull request with detailed description

## Daily Test Runs
- We run a randomly generated scenario every 4 hours and the results are listed here: https://github.com/valkey-io/valkey-fuzzer/actions/workflows/fuzzer-run.yml
- These results can be analyzed to find potential bugs in Valkey

## License

This project is licensed under the BSD 3-Clause License.
