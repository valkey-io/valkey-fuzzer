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

# Run against a specific Valkey binary
valkey-fuzzer cluster --random --valkey-binary /tmp/valkey/src/valkey-server

# Run a DSL scenario against a specific Valkey binary
valkey-fuzzer cluster --dsl examples/simple_failover.yaml --valkey-binary /tmp/valkey/src/valkey-server

```

### PR-Triggered Fuzzer Runs

This repo includes a reusable GitHub Actions workflow at `.github/workflows/valkey-pr-fuzzer.yml` that can:

- build Valkey from a specific ref, including a PR merge ref such as `refs/pull/123/merge`
- fan out a configurable number of random fuzzer runs, defaulting to `10`
- upload per-run artifacts with the console log, node logs, and JSON result payload
- fail the overall workflow if any run fails, while still letting all matrix runs complete
- treat a run as failed if any operation fails, any chaos injection fails, any post-operation validation wave fails, or the final validation fails

#### Manual Trigger

You can manually trigger the fuzzer workflow from the GitHub Actions UI:

1. Navigate to **Actions** -> **Valkey Cluster PR Fuzzer**
2. Click **Run workflow**
3. Fill in the required inputs:
   - `valkey_repository`: Repository containing the Valkey code (e.g., `valkey-io/valkey` or `username/valkey`)
   - `valkey_ref`: Git ref to test (e.g., `refs/pull/123/merge`, `main`, or a commit SHA)
   - `pr_number`: (Optional) PR number for summary display
   - `pr_url`: (Optional) PR URL for summary display
   - `run_count`: Number of random fuzzer runs (default: 10)
   - `fuzzer_repository`: (Optional) Override fuzzer repo to use
   - `fuzzer_ref`: (Optional) Override fuzzer ref to use

Example manual trigger inputs:
```
valkey_repository: valkey-io/valkey
valkey_ref: refs/pull/456/merge
pr_number: 456
pr_url: https://github.com/valkey-io/valkey/pull/456
run_count: 5
```

Each fuzzer run uploads artifacts containing:
- `console.log` - Full fuzzer execution output
- `result.json` - Structured test results
- `metadata.json` - Run metadata (status, seed, duration, failures)
- `node-logs/` - Individual Valkey node logs from `/tmp/valkey-fuzzer/logs`

To access these:
1. Navigate to the workflow run in **Actions**
2. Scroll to the **Artifacts** section at the bottom
3. Download `valkey-pr-fuzzer-run-N` for each run
4. Extract and examine the logs to debug failures

You can also see the full Fuzzer Run and other information in the `Execute Fuzzer Run` job

#### Label-Triggered Workflow

If you want this to run when a label is applied in the Valkey repo, the Valkey repo needs a small caller workflow. Example:

```yaml
name: Label-triggered Valkey Cluster Fuzzer

on:
  pull_request_target:
    types: [labeled]

permissions:
  contents: read

jobs:
  pr-fuzzer:
    permissions:
      contents: read
      issues: write
      pull-requests: write
    if: github.event.label.name == 'run-cluster-fuzzer'
    uses: valkey-io/valkey-fuzzer/.github/workflows/valkey-pr-fuzzer.yml@main
    with:
      fuzzer_repository: valkey-io/valkey-fuzzer
      fuzzer_ref: main
      valkey_repository: ${{ github.repository }}
      valkey_ref: refs/pull/${{ github.event.pull_request.number }}/merge
      pr_number: ${{ github.event.pull_request.number }}
      pr_url: ${{ github.event.pull_request.html_url }}
      label_name: ${{ github.event.label.name }}
      run_count: 10
```

For the simplest setup, point both the reusable workflow ref and `fuzzer_ref` at `main`, along with the matching `fuzzer_repository`, so the caller executes the current upstream workflow and fuzzer code. If you want tighter reproducibility, switch both references to the same tag or commit SHA instead.

This keeps the status visible on the Valkey PR as a normal workflow check, and the reusable workflow updates a sticky PR comment with the latest run summary. If the PR cannot produce a merge ref, for example due to merge conflicts, the workflow will fail during the Valkey checkout/build stage.

You can also add a small cleanup job in the caller workflow to remove `run-cluster-fuzzer` after the run finishes, so maintainers can rerun by simply reapplying the label.

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
- Each scheduled run uploads a `fuzzer-run-artifacts-*` bundle containing the structured `results.json`, the exported scenario DSL, and the collected node/test logs from `/tmp/valkey-fuzzer/logs`
- These results can be analyzed to find potential bugs in Valkey

## License

This project is licensed under the BSD 3-Clause License.
