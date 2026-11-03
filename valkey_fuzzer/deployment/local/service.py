"""The component-facing service of one local deployment: its instances, processes, clients, and checks."""

import logging
import random
import threading
import time
import weakref
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
from enum import Enum
from typing import Any, Optional

import valkey
from valkey.backoff import NoBackoff
from valkey.cluster import ClusterNode, ValkeyCluster
from valkey.retry import Retry

from ..._data import to_plain_data
from ...protocols import RunContext
from .cluster_nodes import slot_hash_tags
from .failover import failover_shard
from .models import (
    DeploymentMode,
    InstanceInfo,
    LocalDeployment,
    LocalDeploymentError,
)
from .processes import ProcessManager, process_output_path, resolve_binary_path
from .queries import Replication, query_or_none, query_replication, valkey_client, wait_until
from .topology import DeploymentConnection
from .validation.check import DeploymentStateCheck, adapt_validation_config, scenario_rng
from .validation.config import validation_config_from_document

logger = logging.getLogger(__name__)

VALKEY_DEPLOYMENT_SERVICE = "valkey.deployment"

# How long cleanup waits for operations still running, such as a call the runner cancelled, before it stops the
# instances under them: their waits end at the cancellation, but a query in flight still takes its own timeout.
CLOSE_WAIT_S = 30.0


# How many SETs one preload pipeline sends.
_PRELOAD_BATCH = 1000


def load_all_slots(connection: DeploymentConnection, keys_per_slot: int, rng: random.Random) -> bool:
    """Write ``keys_per_slot`` distinct keys into every hash slot, in pipelines; True when every write succeeded.

    Key ``j`` of a slot is ``key:{tag}:j`` with the slot's hash tag, and its
    value is drawn from ``rng``. Outside cluster mode it writes nothing and
    returns True.
    """
    if connection.mode != DeploymentMode.CLUSTER:
        logger.info("Skipping slot preload for %s deployment", connection.mode.value)
        return True

    client = ValkeyCluster(
        startup_nodes=[ClusterNode(host=node["host"], port=node["port"]) for node in connection.startup_nodes],
        decode_responses=True,
        skip_full_coverage_check=True,
    )
    keys = [f"key:{{{tag}}}:{index}" for index in range(keys_per_slot) for tag in slot_hash_tags()]
    failures = []
    try:
        logger.info("Loading %s keys (%s per slot) into cluster", len(keys), keys_per_slot)
        for start in range(0, len(keys), _PRELOAD_BATCH):
            batch = keys[start : start + _PRELOAD_BATCH]
            pipeline = client.pipeline()
            for key in batch:
                pipeline.set(key, f"{rng.getrandbits(64):016x}")
            replies = pipeline.execute(raise_on_error=False)
            failures.extend(
                (key, reply) for key, reply in zip(batch, replies) if isinstance(reply, Exception) or not reply
            )
    finally:
        client.close()
    if failures:
        key, reply = failures[0]
        logger.warning("Failed to load %s of %s keys; the first, %s: %s", len(failures), len(keys), key, reply)
        return False
    logger.info("Successfully loaded %s keys", len(keys))
    return True


@dataclass(frozen=True)
class LocalInstance:
    """Immutable, process-free description of one locally provisioned Valkey instance."""

    instance_id: str
    role: str
    shard_id: int
    host: str
    port: int
    bus_port: int
    slot_start: Optional[int] = None
    slot_end: Optional[int] = None
    primary_id: Optional[str] = None
    cluster_node_id: Optional[str] = None

    @classmethod
    def from_instance_info(cls, info: InstanceInfo) -> "LocalInstance":
        return cls(**{field.name: getattr(info, field.name) for field in fields(cls)})


class _ServiceState(str, Enum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


class ValkeyDeploymentService:
    """Narrow component-facing service for one locally provisioned deployment.

    Its waits (failovers, state checks, restarts) end at ``context``'s run
    deadline or when the runner cancels the calling component, raising
    :class:`~.queries.WaitInterruptedError`.
    """

    def __init__(self, processes: ProcessManager, deployment: LocalDeployment, context: Optional[RunContext] = None):
        # Clients handed to components; whatever is still open is closed when
        # the deployment is cleaned up.
        self._clients: weakref.WeakSet[Any] = weakref.WeakSet()
        # One client per instance for execute_command, replaced when the instance restarts.
        self._command_clients: dict[str, valkey.Valkey] = {}
        instance_ids = [info.instance_id for info in deployment.instances]
        self._processes = processes
        self._context = context
        self._deployment = deployment
        self._infos = tuple(deployment.instances)
        self._infos_by_id = {info.instance_id: info for info in self._infos}
        self._instances = tuple(LocalInstance.from_instance_info(info) for info in self._infos)
        self._instances_by_id = {instance.instance_id: instance for instance in self._instances}
        self._connection = DeploymentConnection(
            list(deployment.instances),
            deployment.deployment_id,
            mode=deployment.config.mode,
        )
        self._state = _ServiceState.OPEN
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._active_operations = 0
        self._operation_owners: dict[int, int] = {}
        # Two concurrent restarts of one instance would each start a server: one
        # would be orphaned, and the other's readiness check could stop the wrong one.
        self._restart_locks = {instance_id: threading.Lock() for instance_id in instance_ids}

    @property
    def deployment_id(self) -> str:
        return self._deployment.deployment_id

    @property
    def mode(self) -> DeploymentMode:
        return DeploymentMode(self._deployment.config.mode)

    @property
    def instances(self) -> tuple[LocalInstance, ...]:
        """The planned instances, in planning order (``valkey-0``, ``valkey-1``, ...)."""
        with self._lock:
            self._ensure_open()
            return self._instances

    @property
    def valkey_sha(self) -> Optional[str]:
        return self._deployment.valkey_sha

    def _ensure_open(self) -> None:
        if self._state is _ServiceState.CLOSING:
            raise LocalDeploymentError(f"deployment {self.deployment_id!r} is being cleaned up")
        if self._state is _ServiceState.CLOSED:
            raise LocalDeploymentError(f"deployment {self.deployment_id!r} has already been cleaned up")

    @contextmanager
    def _operation(self):
        owner = threading.get_ident()
        with self._condition:
            self._ensure_open()
            self._active_operations += 1
            self._operation_owners[owner] = self._operation_owners.get(owner, 0) + 1
        try:
            yield
        finally:
            with self._condition:
                self._active_operations -= 1
                remaining = self._operation_owners[owner] - 1
                if remaining:
                    self._operation_owners[owner] = remaining
                else:
                    del self._operation_owners[owner]
                if self._active_operations == 0:
                    self._condition.notify_all()

    def close(self, release: Callable[[list[InstanceInfo]], None]) -> None:
        """Refuse new operations, wait up to :data:`CLOSE_WAIT_S` for the running ones, ``release`` the instances,
        and close every client handed out. A ``release`` that raises leaves the deployment open for a retry."""
        self._begin_close()
        try:
            release(list(self._infos))
        except BaseException:
            self._abort_close()
            raise
        self._mark_closed()

    def _begin_close(self) -> None:
        owner = threading.get_ident()
        with self._condition:
            self._ensure_open()
            if self._operation_owners.get(owner, 0):
                raise LocalDeploymentError("cannot clean up a local deployment reentrantly from an active operation")
            self._state = _ServiceState.CLOSING
            deadline = time.monotonic() + CLOSE_WAIT_S
            while self._active_operations:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        "Cleaning up deployment %s while %d operation(s) still run",
                        self.deployment_id,
                        self._active_operations,
                    )
                    break
                self._condition.wait(remaining)

    def _abort_close(self) -> None:
        with self._condition:
            if self._state is _ServiceState.CLOSING:
                self._state = _ServiceState.OPEN
                self._condition.notify_all()

    def _mark_closed(self) -> None:
        with self._condition:
            if self._state is not _ServiceState.CLOSING:
                raise LocalDeploymentError("local deployment handle was not closing")
            self._state = _ServiceState.CLOSED
            self._condition.notify_all()
        self._close_clients()

    def _close_clients(self) -> None:
        for client in [*self._clients, *self._command_clients.values()]:
            try:
                client.close()
            except Exception:  # pragma: no cover - best effort
                pass
        self._clients.clear()
        self._command_clients.clear()

    def _info(self, instance_id: str) -> InstanceInfo:
        try:
            return self._infos_by_id[instance_id]
        except KeyError as exc:
            raise KeyError(f"unknown instance ID {instance_id!r}") from exc

    def instance(self, instance_id: str) -> LocalInstance:
        """Return one initially planned instance by its stable ID."""
        with self._lock:
            self._ensure_open()
            try:
                return self._instances_by_id[instance_id]
            except KeyError as exc:
                raise KeyError(f"unknown instance ID {instance_id!r}") from exc

    def execute_command(self, instance_id: str, *args: Any, **kwargs: Any) -> Any:
        """Execute one command without exporting a reconnectable raw client."""

        with self._operation():
            info = self._info(instance_id)
            with self._lock:
                client = self._command_clients.get(instance_id)
                if client is None:
                    client = self._command_clients[instance_id] = valkey.Valkey(
                        host=info.host, port=info.port, socket_timeout=5, decode_responses=True
                    )
            return client.execute_command(*args, **kwargs)

    def restart(self, instance_id: str, wait_ready: bool = True, ready_timeout: float = 30.0) -> LocalInstance:
        """Restart one instance while preserving its resolved startup config.

        Live replication roles are recorded first, so an instance whose
        replica was promoted restarts in its current role rather than its
        planned one. With ``wait_ready``, the restart fails if the instance is
        not ready within ``ready_timeout`` seconds.
        """
        with self._operation():
            if instance_id not in self._infos_by_id:
                raise KeyError(f"unknown instance ID {instance_id!r}")
            with self._restart_locks[instance_id]:
                self._refresh_roles()
                try:
                    self._processes.restart_instance(
                        self._infos_by_id[instance_id],
                        wait_ready=wait_ready,
                        ready_timeout=ready_timeout,
                        context=self._context,
                    )
                except Exception as exc:
                    logger.error("Failed to restart instance %s: %s", instance_id, exc)
                    raise LocalDeploymentError(
                        f"failed to restart instance {instance_id!r} in deployment {self.deployment_id!r}"
                    ) from exc
                with self._lock:
                    cleaned_up = self._state is not _ServiceState.OPEN
                if cleaned_up:
                    # Cleanup stopped waiting for this restart (CLOSE_WAIT_S) and may have released the instance
                    # before its new server started, which would then outlive the deployment.
                    self._processes.terminate_instance(self._infos_by_id[instance_id])
                    raise LocalDeploymentError(
                        f"deployment {self.deployment_id!r} was cleaned up during the restart of {instance_id!r}"
                    )
                # The cached client is bound to the old process's socket.
                with self._lock:
                    stale = self._command_clients.pop(instance_id, None)
                if stale is not None:
                    try:
                        stale.close()
                    except Exception:  # pragma: no cover - best effort
                        pass
                return self._instances_by_id[instance_id]

    # -- Process control ---------------------------------------------------

    def pid(self, instance_id: str) -> int:
        """Return the process ID of the instance's current server process."""
        with self._operation():
            return self._info(instance_id).pid

    def started_at(self, instance_id: str) -> float:
        """Return when the instance's current server process was spawned (``time.time()``), before it was ready."""
        with self._operation():
            return self._info(instance_id).started_at

    def is_alive(self, instance_id: str) -> bool:
        """Return whether the instance's current server process is running."""
        with self._operation():
            return self._info(instance_id).is_running

    def send_signal(self, instance_id: str, signum: int) -> None:
        """Deliver a POSIX signal to the instance's current server process."""
        with self._operation():
            info = self._info(instance_id)
            if not info.is_running:
                raise ProcessLookupError(f"instance {instance_id!r} is not running")
            info.process.send_signal(signum)

    def wait_exit(self, instance_id: str, timeout: float = 10.0) -> bool:
        """Wait for (and reap) the instance's current process; False on timeout."""
        with self._operation():
            process = self._info(instance_id).process
            if process is None:
                return True
            return wait_until(
                lambda: process.poll() is not None,
                timeout=timeout,
                interval=0.05,
                what=f"{instance_id} to exit",
                context=self._context,
            )

    # -- Files and binaries -------------------------------------------------

    def log_file(self, instance_id: str) -> str:
        with self._operation():
            return self._info(instance_id).log_file

    def process_output_file(self, instance_id: str) -> Optional[str]:
        """Return the file capturing the instance's stdout/stderr (sanitizer reports land here)."""
        with self._operation():
            return process_output_path(self._info(instance_id).log_file)

    def data_dir(self, instance_id: str) -> str:
        with self._operation():
            return self._info(instance_id).data_dir

    def binary(self, instance_id: str) -> str:
        """Return the valkey-server binary the instance's next (re)start uses."""
        with self._operation():
            return self._info(instance_id).valkey_binary or self._deployment.config.valkey_binary

    def set_binary(self, instance_id: str, binary: str) -> str:
        """Use ``binary`` from the instance's next restart on; return its resolved path."""
        with self._operation():
            info = self._info(instance_id)
            resolved = resolve_binary_path(binary, f"binary for {instance_id}")
            info.valkey_binary = resolved
            return resolved

    # -- Clients and roles ----------------------------------------------------

    def new_client(self, instance_id: str, timeout: float = 5.0) -> Any:
        """Return a fresh client for one instance; it is closed at deployment cleanup.

        It sends each command once: a re-sent write after a timeout would hide
        whether the first attempt was applied, which the workloads record.
        """
        with self._operation():
            info = self._info(instance_id)
            client = valkey.Valkey(
                host=info.host,
                port=info.port,
                socket_timeout=timeout,
                socket_connect_timeout=timeout,
                decode_responses=True,
                retry=Retry(NoBackoff(), 0),
            )
            self._clients.add(client)
            return client

    def cluster_client(self, timeout: float = 5.0) -> Any:
        """Return a fresh slot-routing client; only in cluster mode."""
        with self._operation():
            if self.mode is not DeploymentMode.CLUSTER:
                raise LocalDeploymentError("cluster_client is only available in cluster mode")
            client = ValkeyCluster(
                startup_nodes=[ClusterNode(info.host, info.port) for info in self._infos],
                socket_timeout=timeout,
                socket_connect_timeout=timeout,
                decode_responses=True,
            )
            self._clients.add(client)
            return client

    def roles(self) -> dict[str, str]:
        """Return each reachable instance's live replication role: ``primary`` or ``replica``."""
        with self._operation():
            return {instance_id: replication.role for instance_id, replication in self._live_replication().items()}

    def primaries(self) -> list[str]:
        """Return live primary instance IDs, falling back to the planned layout."""
        roles = self.roles()
        if roles:
            return [item.instance_id for item in self._instances if roles.get(item.instance_id) == "primary"]
        return [item.instance_id for item in self._instances if item.role == "primary"]

    def replicas(self) -> list[str]:
        """Return live replica instance IDs, falling back to the planned layout."""
        roles = self.roles()
        if roles:
            return [item.instance_id for item in self._instances if roles.get(item.instance_id) == "replica"]
        return [item.instance_id for item in self._instances if item.role == "replica"]

    def refresh_roles(self) -> dict[str, str]:
        """Record live roles so later restarts and validators use them."""
        with self._operation():
            return self._refresh_roles()

    def replicate(self, instance_id: str, primary_id: Optional[str], timeout: float = 5.0) -> None:
        """Make one instance a replica of ``primary_id``, or a primary with ``None`` (standalone only).

        The role and primary are recorded first, whether or not the instance is
        running, so a later ``restart`` brings a stopped instance back in that
        role (a replica starts with ``--replicaof``) and the validators expect
        it there. A running instance is then sent ``REPLICAOF host port`` or
        ``REPLICAOF NO ONE``; an error or timeout from it is raised, with the
        record kept.
        """
        with self._operation():
            if self.mode is not DeploymentMode.STANDALONE:
                raise LocalDeploymentError("replicate is only available in standalone mode")
            info = self._info(instance_id)
            primary = None
            if primary_id is not None:
                if primary_id == instance_id:
                    raise ValueError(f"{instance_id} cannot replicate itself")
                primary = self._info(primary_id)
            info.role = "primary" if primary is None else "replica"
            info.primary_id = primary_id
            if not info.is_running:
                return
            command = ("REPLICAOF", "NO", "ONE") if primary is None else ("REPLICAOF", primary.host, primary.port)
            with valkey_client(info.host, info.port, timeout=timeout) as client:
                client.execute_command(*command)

    def _live_replication(self) -> dict[str, Replication]:
        """Each reachable instance's INFO replication; an instance that reports neither role counts as a replica."""
        live = {}
        for info in self._infos:
            replication = query_or_none(info.host, info.port, 2.0, query_replication)
            if replication is not None:
                live[info.instance_id] = replication if replication.role else replace(replication, role="replica")
        return live

    def _refresh_roles(self) -> dict[str, str]:
        by_address = {(info.host, info.port): info for info in self._infos}
        roles = {}
        for instance_id, replication in self._live_replication().items():
            info = self._infos_by_id[instance_id]
            info.role = replication.role
            if replication.role == "primary":
                info.primary_id = None
            else:
                primary = by_address.get((replication.primary_host, replication.primary_port))
                if primary is not None:
                    info.primary_id = primary.instance_id
            roles[instance_id] = replication.role
        return roles

    # -- Deployment operations and checks ---------------------------------------

    def live_instances(self) -> list[dict[str, Any]]:
        """Reachable instances as ``{instance_id, role, shard_id}``, all live.

        In cluster mode ``shard_id`` is the shard whose slots the node (or its
        primary) owns now, which replica migration and failovers move away
        from the plan; ``None`` for a primary without slots and its replicas.
        """
        with self._operation():
            return [
                {"instance_id": entry.instance_id, "role": entry.role, "shard_id": entry.shard_id}
                for entry in self._connection.get_current_nodes(include_failed=False)
                if entry.instance_id is not None
            ]

    def failover(
        self,
        shard_id: int,
        *,
        force: bool = False,
        timeout: float,
        stopped_instances: Sequence[str] = (),
        rng: random.Random,
    ) -> Optional[bool]:
        """Promote a replica of ``shard_id``'s current primary; False when Valkey did not fail the shard over,
        None when there was nothing to fail over.

        ``stopped_instances`` are instance IDs stopped on purpose: a stopped primary
        whose replicas take over is not a failed failover, and a shard stopped
        whole, or whose every replica was stopped, has nothing to fail over. ``rng``
        (a seeded ``random.Random``, e.g. ``context.rng``) picks the replica, so
        a seed replays the same choice. ``timeout`` is Valkey's to settle the
        shard; what could not be observed raises (see :func:`~.failover.failover_shard`).
        """
        with self._operation():
            if self.mode is not DeploymentMode.CLUSTER:
                raise LocalDeploymentError("failover is only available in cluster mode")
            return failover_shard(
                self._connection,
                shard_id,
                force=force,
                timeout=timeout,
                stopped_instances=stopped_instances,
                rng=rng,
                context=self._context,
            )

    def seed_slots(self, keys_per_slot: int, *, seed: Optional[int] = None) -> bool:
        """Write ``keys_per_slot`` keys into every hash slot; True when every write succeeded."""
        with self._operation():
            return load_all_slots(self._connection, keys_per_slot=keys_per_slot, rng=scenario_rng(seed, "slot-preload"))

    def state_check(
        self, validation: Optional[Mapping[str, Any]] = None, *, seed: Optional[int] = None
    ) -> DeploymentStateCheck:
        """A state validator for this deployment; ``validation`` is a mapping of
        :data:`~.validation.config.STATE_VALIDATION_SCHEMA` (``check_*`` flags, timeouts, retries)."""
        with self._operation():
            config = validation_config_from_document(to_plain_data(validation) if validation else None)
            planned = {instance.instance_id: instance.role for instance in self._instances}
            primaries = sum(role == "primary" for role in planned.values())
            replicas = sum(role == "replica" for role in planned.values())
            # Outside cluster mode this turns the cluster checks off and the role check on, whatever was asked:
            # the role check is that mode's liveness check (docs/components.md, valkey.oracle.deployment-state).
            adapt_validation_config(config, self.mode, replicas // primaries if primaries else 0)
            return DeploymentStateCheck(
                self._connection,
                self._infos,
                planned,
                config,
                rng=scenario_rng(seed, "data-validation"),
                guard=self._operation,
                context=self._context,
            )
