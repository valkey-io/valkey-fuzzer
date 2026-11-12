"""Data models of the local deployment: its configuration and its planned and running instances."""

import subprocess
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class LocalDeploymentError(RuntimeError):
    """Base class for local deployment lifecycle failures."""


class LocalDeploymentValidationError(ValueError):
    """A local deployment document or startup overlay is invalid."""


DEFAULT_BASE_PORT = 7000
# Each instance's cluster bus port is this far above its client port.
CLUSTER_BUS_PORT_OFFSET = 10000
DEFAULT_BASE_DATA_DIR = "/tmp/valkey-fuzzer"


class DeploymentMode(str, Enum):
    """Valkey's modes: standalone (one primary, optional replicas) or cluster."""

    STANDALONE = "standalone"
    CLUSTER = "cluster"


# The instance counts each mode has when a deployment leaves them out: a single instance, or three
# primaries with a replica each.
DEFAULT_DIMENSIONS = {
    DeploymentMode.STANDALONE: {"primaries": 1, "replicas_per_primary": 0},
    DeploymentMode.CLUSTER: {"primaries": 3, "replicas_per_primary": 1},
}


@dataclass
class DeploymentConfig:
    """Configuration for one local Valkey deployment, as the document describes it (see :mod:`.config`).

    ``primaries`` is the number of shards in cluster mode and one in
    standalone mode; each primary has ``replicas_per_primary`` replicas. Left
    out, they take the mode's :data:`DEFAULT_DIMENSIONS`.
    """

    mode: DeploymentMode = DeploymentMode.CLUSTER
    primaries: Optional[int] = None
    replicas_per_primary: Optional[int] = None
    base_port: int = DEFAULT_BASE_PORT
    base_data_dir: str = DEFAULT_BASE_DATA_DIR
    valkey_binary: str = ""
    enable_cleanup: bool = True
    server_config: dict[str, Any] = field(default_factory=dict)
    instance_configs: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Per-instance overrides keyed by role ("primary"/"replica") or exact instance ID.
    # An exact instance ID wins over a role, which wins over ``valkey_binary``.
    instance_binaries: dict[str, str] = field(default_factory=dict)
    # Roots that hold instance data (RDB, AOF, nodes.conf) instead of
    # ``base_data_dir``, e.g. a size-limited filesystem. Logs stay put.
    instance_data_roots: dict[str, str] = field(default_factory=dict)
    # "shared_loopback": every instance binds 127.0.0.1. "per_instance_loopback":
    # valkey-N binds 127.0.B.(N+2), B being ``loopback_block``, so network faults can target instance pairs.
    addressing: str = "shared_loopback"
    # The /24 of 127.0.0.0/16 per-instance addresses come from: a batch gives each worker its own, so a
    # partition (which matches addresses) in one worker never cuts another's instances.
    loopback_block: int = 0

    def __post_init__(self):
        defaults = DEFAULT_DIMENSIONS[self.mode]
        if self.primaries is None:
            self.primaries = defaults["primaries"]
        if self.replicas_per_primary is None:
            self.replicas_per_primary = defaults["replicas_per_primary"]


@dataclass
class InstancePlan:
    """Plan for one instance before it is spawned."""

    instance_id: str
    role: str
    shard_id: int
    port: int
    bus_port: int
    slot_start: Optional[int] = None
    slot_end: Optional[int] = None
    primary_id: Optional[str] = None
    server_config: dict[str, Any] = field(default_factory=dict)
    valkey_binary: Optional[str] = None
    data_root: Optional[str] = None
    host: str = "127.0.0.1"


@dataclass
class InstanceInfo(InstancePlan):
    """A started instance: its plan, process, and files.

    The record is live and mutable: ``role`` and ``primary_id`` follow
    recorded role changes, and ``process``, ``pid`` and ``started_at`` each restart.
    ``valkey_binary`` is the binary its next (re)start uses; None means the
    deployment default.
    """

    pid: int = 0
    process: Optional[subprocess.Popen] = None
    # Wall-clock time the current process was spawned: from then on it, not its
    # predecessor, may be the one acting, even before it is ready.
    started_at: float = 0.0
    data_dir: str = ""
    log_file: str = ""
    cluster_node_id: Optional[str] = None

    @property
    def is_running(self) -> bool:
        """Whether its current server process is running."""
        return self.process is not None and self.process.poll() is None


@dataclass
class LocalDeployment:
    """A formed local deployment and its running instances."""

    deployment_id: str
    config: DeploymentConfig
    instances: list[InstanceInfo]
    valkey_sha: Optional[str] = None  # Git SHA the deployment's valkey-server was built from
