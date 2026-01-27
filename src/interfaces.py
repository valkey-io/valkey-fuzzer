"""
Base interfaces and abstract classes for all major components
"""
from abc import ABC, abstractmethod
from typing import List, Optional
from dataclasses import dataclass
from .models import (
    Scenario, ExecutionResult, DSLConfig, StateValidationResult,
    ClusterStatus, ClusterConnection,
    NodeInfo, NodePlan, ChaosResult, ProcessChaosType,
    WorkloadConfig, WorkloadSession, WorkloadMetrics, Operation
)


class IFuzzerEngine(ABC):
    """Interface for the main Fuzzer Engine orchestrator"""
    
    @abstractmethod
    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """Generate a randomized test scenario"""
        pass
    
    @abstractmethod
    def execute_dsl_scenario(self, dsl_config: DSLConfig) -> ExecutionResult:
        """Execute a test scenario from DSL configuration"""
        pass
    
    @abstractmethod
    def execute_test(self, scenario: Scenario) -> ExecutionResult:
        """Execute a complete test scenario"""
        pass
    
    @abstractmethod
    def validate_cluster_state(self, cluster_id: str) -> StateValidationResult:
        """Validate current cluster state"""
        pass


class IClusterOrchestrator(ABC):
    """Interface for cluster lifecycle management"""
    
    @abstractmethod
    def setup_valkey_from_source(self, base_dir: str = "/tmp/valkey-build") -> str:
        """Find or build Valkey binary, return path to valkey-server binary"""
        pass
    
    @abstractmethod
    def spawn_all_nodes(self, node_plans: List[NodePlan]) -> List[NodeInfo]:
        """Spawn all Valkey processes and wait for them to be ready"""
        pass
    
    @abstractmethod
    def form_cluster(self, nodes_in_cluster: List[NodeInfo], cluster_id: str) -> ClusterConnection:
        """Form a complete cluster from spawned nodes"""
        pass
    
    @abstractmethod
    def validate_cluster(self, nodes_in_cluster: List[NodeInfo], timeout: float = 30.0, interval: float = 1.0) -> bool:
        """Validate cluster health and configuration"""
        pass
    
    @abstractmethod
    def cleanup_cluster(self, nodes_in_cluster: List[NodeInfo]) -> None:
        """Clean up cluster by terminating nodes and releasing resources"""
        pass
    
    @abstractmethod
    def restart_node(self, node: NodeInfo, wait_ready: bool = True, ready_timeout: float = 30.0) -> NodeInfo:
        """Restart a Valkey node process"""
        pass

    @abstractmethod
    def close_connections(self) -> None:
        """Close all Valkey client connections"""
        pass


class IChaosEngine(ABC):
    """Interface for chaos injection"""
    
    @abstractmethod
    def inject_process_chaos(self, target_node: NodeInfo, chaos_type: ProcessChaosType, log_buffer=None) -> ChaosResult:
        """Inject process-level chaos on target node"""
        pass
    
    @abstractmethod
    def stop_chaos(self, chaos_id: str) -> bool:
        """Stop active chaos injection"""
        pass
    
    @abstractmethod
    def cleanup_chaos(self, cluster_id: str) -> bool:
        """Clean up any remaining chaos effects"""
        pass


class IValkeyClient(ABC):
    """Interface for workload generation"""
    
    @abstractmethod
    def start_workload(self, cluster_info: ClusterStatus, workload_config: WorkloadConfig) -> WorkloadSession:
        """Start workload generation against cluster"""
        pass
    
    @abstractmethod
    def stop_workload(self, session_id: str) -> WorkloadMetrics:
        """Stop workload and return metrics"""
        pass
    
    @abstractmethod
    def get_workload_metrics(self, session_id: str) -> WorkloadMetrics:
        """Get current workload metrics"""
        pass
    
    @abstractmethod
    def is_workload_active(self, session_id: str) -> bool:
        """Check if workload session is active"""
        pass


class ITestCaseGenerator(ABC):
    """Interface for test case generation"""
    
    @abstractmethod
    def generate_random_scenario(self, seed: Optional[int] = None) -> Scenario:
        """Generate randomized test scenario"""
        pass
    
    @abstractmethod
    def parse_dsl_config(self, dsl_text: str) -> Scenario:
        """Parse DSL configuration into test scenario"""
        pass
    
    @abstractmethod
    def validate_scenario(self, scenario: Scenario) -> bool:
        """Validate test scenario configuration"""
        pass


class IOperationOrchestrator(ABC):
    """Interface for operation execution"""
    
    @abstractmethod
    def execute_operation(self, operation: Operation, log_buffer=None) -> bool:
        """Execute a single cluster operation"""
        pass

    @abstractmethod
    def wait_for_operation_completion(self, timeout: float) -> bool:
        """Wait for operation to complete"""
        pass


class IStateValidator(ABC):
    """Interface for cluster state validation"""
    
    @abstractmethod
    def validate_state(self, cluster_connection: ClusterConnection, expected_topology: Optional = None, operation_context: Optional = None) -> StateValidationResult:
        """Perform comprehensive cluster state validation"""
        pass
    
    @abstractmethod
    def validate_with_retry(self, cluster_connection: ClusterConnection, expected_topology: Optional = None, operation_context: Optional = None) -> StateValidationResult:
        """Perform validation with retry logic for transient failures"""
        pass


class ILogger(ABC):
    """Interface for test logging and reporting"""
    
    @abstractmethod
    def log_test_start(self, scenario: Scenario) -> None:
        """Log test scenario start"""
        pass
    
    @abstractmethod
    def log_operation(self, operation: Operation, success: bool, details: str, silent: bool = False) -> None:
        """Log operation execution"""
        pass
    
    @abstractmethod
    def log_chaos_event(self, chaos_result: ChaosResult) -> None:
        """Log chaos injection event"""
        pass
    

    
    @abstractmethod
    def log_test_completion(self, test_result: ExecutionResult) -> None:
        """Log test completion"""
        pass
    
    @abstractmethod
    def generate_report(self, test_results: List[ExecutionResult]) -> str:
        """Generate summary report"""
        pass