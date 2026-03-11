"""
Main entry point for the Cluster Bus Fuzzer
"""
from typing import Optional

from .models import DSLConfig, ExecutionResult
from .fuzzer_engine import FuzzerEngine


class ClusterBusFuzzer:
    """Main orchestrator for the Cluster Bus Fuzzer"""

    def __init__(self):
        """Initialize the fuzzer with the main fuzzer engine"""
        self.fuzzer_engine = FuzzerEngine()
        self.last_scenario = None

    def run_random_test(self, seed: int = None, valkey_binary: Optional[str] = None) -> ExecutionResult:
        """Run a randomized test scenario."""
        scenario = self.fuzzer_engine.generate_random_scenario(seed)
        if valkey_binary:
            scenario.cluster_config.valkey_binary = valkey_binary
        result = self.fuzzer_engine.execute_test(scenario)
        self.last_scenario = scenario
        return result

    def run_dsl_test(self, dsl_config: DSLConfig, valkey_binary: Optional[str] = None) -> ExecutionResult:
        """Run a DSL-based test scenario."""
        return self.fuzzer_engine.execute_dsl_scenario(dsl_config, valkey_binary=valkey_binary)

    def validate_cluster(self, cluster_id: str):
        """Validate cluster state."""
        return self.fuzzer_engine.validate_cluster_state(cluster_id)
