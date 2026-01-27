"""
Core data models for the Cluster Bus Fuzzer
"""
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from enum import Enum
import subprocess
import random
from .utils.valkey_utils import is_node_alive as check_node_alive, query_cluster_nodes


@dataclass
class ClusterConfig:
    """Configuration for a Valkey cluster"""
    num_shards: int
    replicas_per_shard: int
    base_port: int = 7000
    base_data_dir: str = "/tmp/valkey-fuzzer"
    valkey_binary: str = "/usr/local/bin/valkey-server"
    enable_cleanup: bool = True


@dataclass
class NodePlan:
    """Plan for a node before it's spawned"""
    node_id: str
    role: str 
    shard_id: int
    port: int
    bus_port: int
    slot_start: Optional[int] = None 
    slot_end: Optional[int] = None   
    master_node_id: Optional[str] = None


@dataclass
class NodeInfo:
    """Information about a running cluster node"""
    node_id: str
    role: str
    shard_id: int
    port: int
    bus_port: int
    pid: int
    process: subprocess.Popen
    data_dir: str
    log_file: str
    host: str = "127.0.0.1"  # Host address (default localhost, can be container IP)
    slot_start: Optional[int] = None
    slot_end: Optional[int] = None
    master_node_id: Optional[str] = None
    cluster_node_id: Optional[str] = None

class OperationType(Enum):
    """Types of operations supported by the fuzzer"""
    FAILOVER = "failover"
    # Future extensions: ADD_REPLICA, REMOVE_REPLICA, RESHARD, SCALE_OUT, SCALE_IN, CONFIG_CHANGE


class ChaosType(Enum):
    """Types of chaos injection supported"""
    PROCESS_KILL = "process_kill"
    # Future extensions: NETWORK_PARTITION, PACKET_DROP, LATENCY_INJECTION


class ProcessChaosType(Enum):
    """Types of process chaos injection"""
    SIGKILL = "sigkill"
    SIGTERM = "sigterm"


@dataclass
class OperationTiming:
    """Timing configuration for operations"""
    delay_before: float = 0.0  # Seconds to wait before operation
    timeout: float = 30.0  # Operation timeout in seconds
    delay_after: float = 0.0  # Seconds to wait after operation


@dataclass
class Operation:
    """Represents a cluster operation to be executed"""
    type: OperationType
    target_node: str
    parameters: Dict[str, Any]
    timing: OperationTiming


@dataclass
class TargetSelection:
    """Configuration for chaos target selection"""
    strategy: str  # "random", "primary_only", "replica_only", "specific"
    specific_nodes: Optional[List[str]] = None


@dataclass
class ChaosTiming:
    """Timing configuration for chaos injection"""
    delay_before_operation: float = 0.0
    delay_after_operation: float = 0.0
    chaos_duration: float = 10.0


@dataclass
class ChaosCoordination:
    """Configuration for chaos coordination with operations"""
    chaos_before_operation: bool = False
    chaos_during_operation: bool = True
    chaos_after_operation: bool = False


@dataclass
class ChaosConfig:
    """Configuration for chaos injection"""
    chaos_type: ChaosType
    target_selection: TargetSelection
    timing: ChaosTiming
    coordination: ChaosCoordination
    process_chaos_type: Optional[ProcessChaosType] = None  # For process chaos
    randomize_per_operation: bool = False  # Enable randomization of chaos parameters per operation


@dataclass
class Scenario:
    """Complete test scenario configuration"""
    scenario_id: str
    cluster_config: ClusterConfig
    operations: List[Operation]
    chaos_config: ChaosConfig
    seed: Optional[int] = None  # For reproducibility
    state_validation_config: Optional['StateValidationConfig'] = None

@dataclass
class SlotConflict:
    """Represents a slot assignment conflict"""
    slot: int
    conflicting_nodes: List[str]


@dataclass
class ClusterStatus:
    """Overall cluster status information"""
    cluster_id: str
    nodes: List[NodeInfo]
    total_slots_assigned: int
    is_healthy: bool
    formation_complete: bool


@dataclass
class ClusterInstance:
    """Represents a created cluster instance"""
    cluster_id: str
    config: ClusterConfig
    nodes: List[NodeInfo]
    is_ready: bool = False


@dataclass
class WorkloadConfig:
    """Configuration for workload generation"""
    clients: int = 10
    requests_per_client: int = 1000
    data_size: int = 1024
    pipeline: int = 1
    key_pattern: str = "test:key:*"


@dataclass
class WorkloadMetrics:
    """Metrics from workload execution"""
    total_requests: int
    successful_requests: int
    failed_requests: int
    average_latency: float
    max_latency: float
    requests_per_second: float


@dataclass
class WorkloadSession:
    """Active workload session"""
    session_id: str
    config: WorkloadConfig
    start_time: float
    is_active: bool = True


@dataclass
class ChaosResult:
    """Result of chaos injection"""
    chaos_id: str
    chaos_type: ChaosType
    target_node: str
    success: bool
    start_time: float
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    target_role: Optional[str] = None


@dataclass
class ExecutionResult:
    """Complete test execution result"""
    scenario_id: str
    success: bool
    start_time: float
    end_time: float
    operations_executed: int
    chaos_events: List[ChaosResult]
    error_message: Optional[str] = None
    seed: Optional[int] = None
    validation_results: Optional[List['StateValidationResult']] = None  # Per-operation validation results
    final_validation: Optional['StateValidationResult'] = None  # Final validation result


@dataclass
class DSLConfig:
    """DSL-based test configuration"""
    config_text: str
    parsed_scenario: Optional[Scenario] = None


@dataclass
class ClusterConnection:
    """Stores cluster connection information after orchestrator creates cluster"""
    initial_nodes: List[NodeInfo]
    cluster_id: str
    
    def __post_init__(self):
        self.startup_nodes = [{'host': '127.0.0.1', 'port': node.port} for node in self.initial_nodes]
    
    def get_current_nodes(self, include_failed: bool = True) -> List[Dict]:
        """Get current cluster topology via CLUSTER NODES. List of node dictionaries with keys: node_id, host, port, role, shard_id, status"""
        # Build a mapping from port to shard_id using initial_nodes
        port_to_shard = {node.port: node.shard_id for node in self.initial_nodes}
        
        for node_info in self.startup_nodes:
            try:
                parsed_nodes = query_cluster_nodes(node_info, timeout=2.0)
                if not parsed_nodes:
                    continue
                node_id_to_shard = {}
                for node in parsed_nodes:
                    if node['is_master']:
                        port = node['port']
                        initial_shard_id = port_to_shard.get(port)
                        if initial_shard_id is not None:
                            node_id_to_shard[node['node_id']] = initial_shard_id
                
                # Second pass - replicas get shard from their master
                for node in parsed_nodes:
                    if node['is_slave'] and node['master_id']:
                        master_shard = node_id_to_shard.get(node['master_id'])
                        if master_shard is not None:
                            node_id_to_shard[node['node_id']] = master_shard
                        else:
                            # Fallback to port mapping
                            node_id_to_shard[node['node_id']] = port_to_shard.get(node['port'])
                
                # Transform to expected format
                current_nodes = []
                for node in parsed_nodes:
                    # Determine status
                    is_failed = node['is_fail'] or node['link_state'] == 'disconnected'
                    
                    # Skip failed nodes if not requested
                    if not include_failed and is_failed:
                        continue
                    
                    current_nodes.append({
                        'node_id': node['node_id'],
                        'host': node['host'],
                        'port': node['port'],
                        'role': 'primary' if node['is_master'] else 'replica',
                        'shard_id': node_id_to_shard.get(node['node_id']),
                        'status': 'failed' if is_failed else 'connected'
                    })
                
                return current_nodes
            except Exception as e:
                continue
        return []
    
    def get_live_nodes(self) -> List[Dict]:
        """Get only live (connected) nodes from the cluster"""
        return self.get_current_nodes(include_failed=False)
    
    def get_primary_nodes(self) -> List[Dict]:
        """Get current primary nodes"""
        return [node for node in self.get_current_nodes() if node['role'] == 'primary']
    
    def get_replica_nodes(self) -> List[Dict]:
        """Get current replica nodes"""
        return [node for node in self.get_current_nodes() if node['role'] == 'replica']

    def is_node_alive(self, host: str, port: int, timeout: float = 2.0) -> bool:
        return check_node_alive(host, port, timeout)

    def find_alive_node(self, nodes: List[Dict], randomize: bool = True) -> Optional[Dict]:
        if not nodes:
            return None

        # Make a copy to avoid modifying the original list
        node_list = nodes.copy()

        if randomize:
            random.shuffle(node_list)

        for node in node_list:
            if self.is_node_alive(node['host'], node['port']):
                return node

        return None

@dataclass
class ReplicationValidationConfig:
    """Configuration for replication validation"""
    max_acceptable_lag: float = 10.0  # seconds
    require_all_replicas_synced: bool = False  # Allow dead replicas after chaos
    check_replication_offset: bool = True
    min_replicas_per_shard: int = 1  # Minimum live replicas per shard (0 = no check)
    timeout: float = 10.0


@dataclass
class ClusterStatusValidationConfig:
    """Configuration for cluster status validation"""
    acceptable_states: List[str] = None
    allow_degraded: bool = False
    require_quorum: bool = True
    timeout: float = 10.0

    def __post_init__(self):
        if self.acceptable_states is None:
            self.acceptable_states = ['ok']


@dataclass
class SlotCoverageValidationConfig:
    """Configuration for slot coverage validation"""
    require_full_coverage: bool = True
    allow_slot_conflicts: bool = False
    timeout: float = 10.0


@dataclass
class TopologyValidationConfig:
    """Configuration for topology validation"""
    strict_mode: bool = True  # Strict matching of expected topology
    allow_failed_nodes: bool = True  # Allow nodes to be failed after chaos
    timeout: float = 10.0


@dataclass
class ViewConsistencyValidationConfig:
    """Configuration for view consistency validation"""
    require_full_consensus: bool = True
    allow_transient_inconsistency: bool = True
    max_inconsistency_duration: float = 5.0
    timeout: float = 15.0


@dataclass
class DataConsistencyValidationConfig:
    """Configuration for data consistency validation"""
    check_test_keys: bool = True  # Validate test keys written before chaos
    check_cross_replica_consistency: bool = True  # Validate data matches across replicas
    num_test_keys: int = 100  # Number of test keys to write/validate
    key_prefix: str = "fuzzer:test:"  # Prefix for test keys
    timeout: float = 10.0


@dataclass
class StateValidationConfig:
    """Configuration for post-operation validation"""
    # Enable/disable individual checks
    check_replication: bool = True
    check_cluster_status: bool = True
    check_slot_coverage: bool = True
    check_topology: bool = True
    check_view_consistency: bool = True
    check_data_consistency: bool = True
    check_logs: bool = True

    # Timing configuration
    stabilization_wait: float = 5.0  # Wait before validation
    validation_timeout: float = 30.0  # Total validation timeout

    # Behavior configuration
    blocking_on_failure: bool = False  # Halt execution on failure
    retry_on_transient_failure: bool = True
    max_retries: int = 3
    retry_delay: float = 2.0

    # Sub-component configurations
    replication_config: ReplicationValidationConfig = None
    cluster_status_config: ClusterStatusValidationConfig = None
    slot_coverage_config: SlotCoverageValidationConfig = None
    topology_config: TopologyValidationConfig = None
    view_consistency_config: ViewConsistencyValidationConfig = None
    data_consistency_config: DataConsistencyValidationConfig = None

    def __post_init__(self):
        if self.replication_config is None:
            self.replication_config = ReplicationValidationConfig()
        if self.cluster_status_config is None:
            self.cluster_status_config = ClusterStatusValidationConfig()
        if self.slot_coverage_config is None:
            self.slot_coverage_config = SlotCoverageValidationConfig()
        if self.topology_config is None:
            self.topology_config = TopologyValidationConfig()
        if self.view_consistency_config is None:
            self.view_consistency_config = ViewConsistencyValidationConfig()
        if self.data_consistency_config is None:
            self.data_consistency_config = DataConsistencyValidationConfig()


@dataclass
class ReplicaLagInfo:
    """Information about replica lag"""
    replica_node_id: str
    replica_address: str
    primary_node_id: str
    primary_address: str
    lag_seconds: float
    replication_offset_diff: int
    link_status: str


@dataclass
class ReplicationValidation:
    """Replication validation result"""
    success: bool
    all_replicas_synced: bool
    max_lag: float
    lagging_replicas: List[ReplicaLagInfo]
    disconnected_replicas: List[str]
    error_message: Optional[str] = None


@dataclass
class ClusterStatusValidation:
    """Cluster status validation result"""
    success: bool
    cluster_state: str
    nodes_in_fail_state: List[str]
    has_quorum: bool
    degraded_reason: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class SlotCoverageValidation:
    """Slot coverage validation result"""
    success: bool
    total_slots_assigned: int
    unassigned_slots: List[int]
    conflicting_slots: List[SlotConflict]
    slot_distribution: Dict[str, List[int]]  # node_id -> slot ranges
    error_message: Optional[str] = None


@dataclass
class TopologyMismatch:
    """Describes a topology mismatch"""
    mismatch_type: str  # "missing_node", "extra_node", "wrong_role", "wrong_shard"
    node_id: str
    expected: str
    actual: str


@dataclass
class TopologyValidation:
    """Topology validation result"""
    success: bool
    expected_primaries: int
    actual_primaries: int
    expected_replicas: int
    actual_replicas: int
    topology_mismatches: List[TopologyMismatch]
    error_message: Optional[str] = None


@dataclass
class ViewDiscrepancy:
    """Describes a view discrepancy between nodes"""
    discrepancy_type: str  # "membership", "role", "state", "address"
    node_reporting: str
    subject_node: str
    expected_value: str
    actual_value: str


@dataclass
class ViewConsistencyValidation:
    """View consistency validation result"""
    success: bool
    nodes_checked: int
    consistent_views: bool
    split_brain_detected: bool
    view_discrepancies: List[ViewDiscrepancy]
    consensus_percentage: float  # Percentage of nodes with majority view
    error_message: Optional[str] = None


@dataclass
class DataInconsistency:
    """Describes a data inconsistency"""
    key: str
    inconsistency_type: str  # "missing", "value_mismatch", "unreachable"
    expected_value: Optional[str]
    actual_values: Dict[str, str]  # node_address -> value


@dataclass
class DataConsistencyValidation:
    """Data consistency validation result"""
    success: bool
    test_keys_checked: int
    missing_keys: List[str]
    inconsistent_keys: List[DataInconsistency]
    unreachable_keys: List[str]
    error_message: Optional[str] = None


@dataclass
class StateValidationResult:
    """Comprehensive validation result"""
    overall_success: bool
    validation_timestamp: float
    validation_duration: float

    # Individual check results
    replication: Optional[ReplicationValidation]
    cluster_status: Optional[ClusterStatusValidation]
    slot_coverage: Optional[SlotCoverageValidation]
    topology: Optional[TopologyValidation]
    view_consistency: Optional[ViewConsistencyValidation]
    data_consistency: Optional[DataConsistencyValidation]
    log_validation: Optional[Any] = None  # LogValidationResult from validators.log_validator

    # Failure information
    failed_checks: List[str] = None
    error_messages: List[str] = None

    def __post_init__(self):
        if self.failed_checks is None:
            self.failed_checks = []
        if self.error_messages is None:
            self.error_messages = []

    def is_critical_failure(self) -> bool:
        """Determine if failure is critical and should halt execution.
        
        Critical failures indicate the cluster is in a broken state where
        continuing operations could cause data loss or corruption.
        """
        # 1. Slot coverage lost - data is unreachable
        if self.slot_coverage and not self.slot_coverage.success:
            return True
        
        # 2. Split-brain detected - cluster partitioned
        if self.view_consistency and self.view_consistency.split_brain_detected:
            return True
        
        # 3. Cluster status failures - cluster in unhealthy state
        if self.cluster_status and not self.cluster_status.success:
            if self.cluster_status.has_quorum is False:
                return True
                
            # Cluster state is 'fail' - cluster is broken
            if self.cluster_status.cluster_state == 'fail':
                return True
            
            # 'unknown' state is only critical if slots are lost
            # (transient 'unknown' after failover is normal)
            if self.cluster_status.cluster_state == 'unknown':
                if self.slot_coverage and not self.slot_coverage.success:
                    return True
            
            # Nodes in fail state - cluster has failed nodes
            if self.cluster_status.nodes_in_fail_state:
                return True                
        
        # 4. All replicas down for any shard - zero redundancy
        if self.replication and not self.replication.success:
            # Check if error message indicates zero redundancy
            if self.replication.error_message:
                if "insufficient redundancy" in self.replication.error_message.lower():
                    return True
                if "all replicas are disconnected" in self.replication.error_message.lower():
                    return True
        
        # 5. Missing shards or wrong primary count - topology broken
        if self.topology and not self.topology.success:
            # Check for critical topology issues
            for mismatch in self.topology.topology_mismatches:
                if mismatch.mismatch_type in ['missing_shard', 'missing_primary', 'primary_count']:
                    return True
            
            # Unexpected failures ARE critical (spontaneous node crashes)
            for mismatch in self.topology.topology_mismatches:
                if mismatch.mismatch_type == 'unexpected_failure':
                    return True
        
        # 6. Data loss detected
        if self.data_consistency and not self.data_consistency.success:
            if self.data_consistency.missing_keys:
                return True
        
        return False


@dataclass
class ShardExpectation:
    """Expected state of a shard"""
    primary_node_id: Optional[str]
    replica_node_ids: List[str]
    slot_ranges: List[tuple]


@dataclass
class ExpectedTopology:
    """Expected cluster topology after an operation"""
    num_primaries: int
    num_replicas: int
    shard_structure: Dict[int, ShardExpectation]  # shard_id -> expectation


@dataclass
class OperationContext:
    """Context about the operation that was executed"""
    operation_type: OperationType
    target_node: str
    operation_success: bool
    operation_timestamp: float


@dataclass
class LogValidationContext:
    """Context for log validation - contains only successful operations"""
    operations: List[Operation]
