"""What each state check found, and one validation attempt's result."""

from dataclasses import dataclass, field
from typing import Optional

from ..cluster_nodes import HASH_SLOTS
from .logs import LogValidationResult


@dataclass
class SlotConflict:
    """A slot two primaries serve at once"""

    slot: int
    conflicting_nodes: list[str]


@dataclass
class ReplicaLagInfo:
    replica_node_id: str
    replica_address: str
    primary_node_id: str
    primary_address: str
    lag_seconds: float
    replication_offset_diff: int
    link_status: str


# The validation results' defaults describe a check that measured nothing.


@dataclass
class ReplicationValidation:
    success: bool
    all_replicas_synced: bool = False
    max_lag: float = -1.0
    lagging_replicas: list[ReplicaLagInfo] = field(default_factory=list)
    disconnected_replicas: list[str] = field(default_factory=list)
    error_message: Optional[str] = None
    # A shard lost replicas to failures that were not stops: critical, see StateValidationResult.is_critical_failure.
    redundancy_lost: bool = False
    # Offsets could not be compared and nothing else was wrong: nothing was judged.
    inconclusive: bool = False


@dataclass
class ClusterStatusValidation:
    success: bool
    cluster_state: str = "unknown"
    nodes_in_fail_state: list[str] = field(default_factory=list)
    has_quorum: bool = False
    degraded_reason: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class SlotCoverageValidation:
    success: bool
    total_slots_assigned: int = 0
    unassigned_slots: list[int] = field(default_factory=lambda: list(range(HASH_SLOTS)))
    conflicting_slots: list[SlotConflict] = field(default_factory=list)
    slot_distribution: dict[str, list[int]] = field(default_factory=dict)  # node_id -> slot ranges
    error_message: Optional[str] = None


@dataclass
class RoleMismatch:
    mismatch_type: str  # "primary_count", "replica_count", "unexpected_failure", or "stopped_alive"
    node_id: str
    expected: str
    actual: str


@dataclass
class RoleValidation:
    success: bool
    expected_primaries: int = 0
    actual_primaries: int = 0
    expected_replicas: int = 0
    actual_replicas: int = 0
    role_mismatches: list[RoleMismatch] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class ViewDiscrepancy:
    discrepancy_type: str  # "missing", "extra", "role", "state", "address", "primary", "config_epoch", "slots"
    node_reporting: str
    subject_node: str
    expected_value: str
    actual_value: str


@dataclass
class ViewConsistencyValidation:
    success: bool
    nodes_checked: int = 0
    consistent_views: bool = False
    split_brain_detected: bool = False
    view_discrepancies: list[ViewDiscrepancy] = field(default_factory=list)
    consensus_percentage: float = 0.0  # Percentage of nodes with majority view
    error_message: Optional[str] = None


@dataclass
class DataInconsistency:
    key: str
    inconsistency_type: str  # "value_mismatch", "missing_on_replica", "replica_divergence"
    expected_value: Optional[str]
    actual_values: dict[str, str]  # node_address -> value


@dataclass
class DataConsistencyValidation:
    success: bool
    test_keys_checked: int = 0
    missing_keys: list[str] = field(default_factory=list)
    inconsistent_keys: list[DataInconsistency] = field(default_factory=list)
    unreachable_keys: list[str] = field(default_factory=list)
    error_message: Optional[str] = None
    # Keys could not be read and none was seen missing or wrong: nothing was judged.
    inconclusive: bool = False


@dataclass
class StateValidationResult:
    """One validation attempt: each check's result, and what failed."""

    overall_success: bool
    validation_duration: float
    # The attempt could not judge the deployment (a running instance did not answer, the time budget ran out, a
    # check broke or could not read) and no check saw Valkey do something wrong. Retried; one still inconclusive
    # is ERROR, never FAIL.
    inconclusive: bool = False

    # Individual check results; None for a check that did not run
    replication: Optional[ReplicationValidation] = None
    cluster_status: Optional[ClusterStatusValidation] = None
    slot_coverage: Optional[SlotCoverageValidation] = None
    roles: Optional[RoleValidation] = None
    view_consistency: Optional[ViewConsistencyValidation] = None
    data_consistency: Optional[DataConsistencyValidation] = None
    log_validation: Optional[LogValidationResult] = None

    failed_checks: list[str] = field(default_factory=list)
    error_messages: list[str] = field(default_factory=list)

    def is_critical_failure(self) -> bool:
        """Determine if failure is critical and should halt execution.

        Critical failures indicate the cluster is in a broken state where
        continuing operations could cause data loss or corruption. An
        inconclusive attempt judged nothing, so it is never critical.
        """
        if self.inconclusive:
            return False
        # 1. Slot coverage lost - data is unreachable
        if self.slot_coverage and not self.slot_coverage.success:
            return True

        # 2. Split-brain detected - cluster partitioned
        if self.view_consistency and self.view_consistency.split_brain_detected:
            return True

        # 3. Cluster lost quorum, or reports itself failed
        if self.cluster_status and not self.cluster_status.success:
            if self.cluster_status.has_quorum is False or self.cluster_status.cluster_state == "fail":
                return True

        # 4. Replicas lost to failures that were not stops - redundancy broken
        if self.replication and not self.replication.success and self.replication.redundancy_lost:
            return True

        # 5. Wrong primary count, or unexpected failures (spontaneous node crashes) - roles broken
        if self.roles and not self.roles.success:
            if any(m.mismatch_type in ("primary_count", "unexpected_failure") for m in self.roles.role_mismatches):
                return True

        # 6. Data loss detected
        return bool(self.data_consistency and not self.data_consistency.success and self.data_consistency.missing_keys)
