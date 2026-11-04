"""Processes: valkey-server binaries, spawning, restarting, identity checks, and cleanup.

Every deployment directory holds an owner tag and every server carries one in
its process title (:mod:`valkey_fuzzer.hoststate`), so ``valkey-fuzzer sweep``
can remove them if this process dies without cleaning up. A deployment kept
with ``enable_cleanup: false`` loses its directory tags at cleanup.
"""

import fcntl
import logging
import os
import re
import shutil
import signal
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict
from typing import Any, Optional

import valkey

from ...hoststate import (
    open_shared_lock,
    run_bounded,
    server_tag_arguments,
    shared_directory,
    tag_directory,
    untag_directory,
)
from ...protocols import RunContext
from .cluster_nodes import parse_cluster_nodes
from .models import (
    DEFAULT_BASE_DATA_DIR,
    DeploymentConfig,
    DeploymentMode,
    InstanceInfo,
    InstancePlan,
    LocalDeploymentError,
)
from .ports import ensure_ports_free
from .queries import query_cluster_info, query_replication, response_text, valkey_client, wait_until
from .server_config import build_server_config_arguments, effective_server_config

logger = logging.getLogger(__name__)


# How long a spawned instance has to answer PING.
READY_TIMEOUT_S = 30.0
# How long a source build (clone and make) may take, at most.
BUILD_TIMEOUT_S = 1800.0
# Where valkey-server is built when no binary is configured or on PATH, and the release it builds.
SOURCE_BUILD_DIR = "/tmp/valkey-build"
VALKEY_REPOSITORY = "https://github.com/valkey-io/valkey.git"
VALKEY_SOURCE_REF = "9.1.3"

# Matches the `sha=<hex>:<dirty>` field from `valkey-server --version`.
# Example output: "Valkey server v=8.1.0 sha=abc1234:0 malloc=jemalloc-5.3.0 ..."
_VERSION_SHA_RE = re.compile(r"sha=([0-9a-f]{7,40}):", re.IGNORECASE)


class InstanceIdentityError(RuntimeError):
    """An instance port answered, but not from the Valkey process this deployment started."""


def verify_instance_process_identity(instance: InstanceInfo, client: valkey.Valkey) -> None:
    """Raise InstanceIdentityError unless the server behind ``client`` is ``instance``'s own process."""
    expected_pid = instance.pid
    if instance.process is not None:
        expected_pid = instance.process.pid
        if instance.process.poll() is not None:
            raise InstanceIdentityError(
                f"{instance.instance_id}: port {instance.port} answered, but the process this deployment started "
                f"(pid {expected_pid}) has exited"
            )

    server_info = client.info("server")
    reported_pid = server_info.get("process_id") if isinstance(server_info, Mapping) else None
    try:
        reported_pid = int(reported_pid)
    except (TypeError, ValueError):
        raise InstanceIdentityError(
            f"{instance.instance_id}: server on port {instance.port} did not report a usable process_id ({reported_pid!r})"
        ) from None

    if reported_pid != expected_pid:
        raise InstanceIdentityError(
            f"{instance.instance_id}: port {instance.port} is served by pid {reported_pid}, not by the process this "
            f"deployment started (pid {expected_pid}); refusing to use a foreign server"
        )


def kill_server(process: subprocess.Popen) -> None:
    """SIGKILL a server and the children it forked (an AOF rewrite, an RDB save), which would outlive it.

    The server is stopped first, so it cannot reap a child (freeing its PID for reuse) before the child is killed.
    """
    process.send_signal(signal.SIGSTOP)
    if process.poll() is None:
        listing = run_bounded(["ps", "-A", "-o", "pid=", "-o", "ppid="])
        for line in listing.stdout.splitlines():
            fields = line.split()
            if len(fields) == 2 and fields[1] == str(process.pid):
                try:
                    os.kill(int(fields[0]), signal.SIGKILL)
                except ProcessLookupError:
                    pass
    process.kill()


def _stop_process(process: subprocess.Popen) -> None:
    """Send SIGTERM, then SIGKILL (to it and its children) if the process has not exited after 5 seconds.

    A server refuses a SIGTERM while it writes its first AOF (a replica after a full sync does), and its
    rewrite child would outlive a SIGKILL of the server alone.
    """
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        kill_server(process)
        process.wait()


def verify_cluster_node_identity(instance: InstanceInfo, client: valkey.Valkey) -> str:
    """Return CLUSTER MYID, raising InstanceIdentityError if it differs from the saved instance ID."""
    cluster_node_id = response_text(client.execute_command("CLUSTER", "MYID"))
    if instance.cluster_node_id and cluster_node_id != instance.cluster_node_id:
        raise InstanceIdentityError(
            f"{instance.instance_id}: restarted server reports cluster node ID {cluster_node_id}, "
            f"expected {instance.cluster_node_id}"
        )
    return cluster_node_id


def detect_valkey_sha(binary: str) -> Optional[str]:
    """Return the git SHA the binary was built from, or None if unavailable.

    Runs ``<binary> --version`` and parses the ``sha=<hex>:<dirty>`` field.
    Release builds without git context emit ``sha=00000000:0`` — those return
    None so callers don't try to clone an all-zero commit.
    """
    try:
        out = subprocess.run(
            [binary, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        logger.debug("Could not run %s --version: %s", binary, exc)
        return None
    m = _VERSION_SHA_RE.search(out)
    if not m:
        return None
    sha = m.group(1).lower()
    if set(sha) == {"0"}:
        return None
    return sha


def resolve_binary_path(configured_binary: str, label: str = "valkey_binary") -> str:
    """Return an absolute, executable path for a configured binary path or command name."""
    # For absolute/relative paths, check existence and permissions directly
    if os.path.sep in configured_binary or configured_binary.startswith("."):
        if not os.path.isfile(configured_binary):
            raise FileNotFoundError(f"Configured {label} '{configured_binary}' does not exist.")
        if not os.access(configured_binary, os.X_OK):
            raise PermissionError(f"Configured {label} '{configured_binary}' exists but is not executable.")
        return os.path.abspath(configured_binary)
    # Bare command name — search PATH
    resolved_binary = shutil.which(configured_binary)
    if resolved_binary:
        return resolved_binary
    raise FileNotFoundError(f"Configured {label} '{configured_binary}' could not be found on PATH.")


def _built_binary(base_dir: str) -> str:
    return os.path.join(base_dir, "valkey", "src", "valkey-server")


def installed_valkey_binary(base_dir: str = SOURCE_BUILD_DIR) -> Optional[str]:
    """The valkey-server a deployment without a configured binary runs, without building one: ``valkey-server``
    on PATH, else the one :func:`build_valkey` built under ``base_dir``; None when neither exists."""
    built = _built_binary(base_dir)
    return shutil.which("valkey-server") or (built if os.path.exists(built) else None)


def build_valkey(base_dir: str = SOURCE_BUILD_DIR, timeout: float = BUILD_TIMEOUT_S) -> str:
    """``base_dir/valkey/src/valkey-server``, building Valkey :data:`VALKEY_SOURCE_REF` there first if it is not,
    within ``timeout`` seconds (``subprocess.TimeoutExpired`` past it).

    One build at a time: concurrent batch workers wait on a lock in
    ``base_dir``. The clone is built aside and renamed into place only once
    ``make`` succeeded, so nobody ever runs a half-built tree. Every user's
    runs share the default ``base_dir`` (a shared directory) and its lock.
    """
    target = os.path.join(base_dir, "valkey")
    binary = _built_binary(base_dir)
    if base_dir == SOURCE_BUILD_DIR:
        shared_directory(base_dir)
    else:
        os.makedirs(base_dir, exist_ok=True)
    with open(open_shared_lock(os.path.join(base_dir, ".lock"))) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            if not os.path.exists(binary):
                work = tempfile.mkdtemp(prefix="valkey-", dir=base_dir)
                deadline = time.monotonic() + timeout
                try:
                    clone = ["git", "clone", "--depth", "1", "--branch", VALKEY_SOURCE_REF, VALKEY_REPOSITORY, work]
                    subprocess.run(clone, check=True, timeout=timeout)
                    make = ["make", f"-j{os.cpu_count() or 1}"]
                    subprocess.run(make, cwd=work, check=True, timeout=max(0.0, deadline - time.monotonic()))
                    os.rename(work, target)
                except BaseException:
                    shutil.rmtree(work, ignore_errors=True)
                    raise
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
    if not os.path.exists(binary):
        raise LocalDeploymentError(f"the Valkey build finished without {binary}; build it by hand or pass a binary")
    return binary


def process_output_path(log_file: str) -> Optional[str]:
    """Per-instance file that captures valkey-server stdout/stderr."""
    if not log_file:
        return None
    root, _ = os.path.splitext(log_file)
    return f"{root}.stdio.log"


class ProcessManager:
    """Starts, restarts, and cleans up the valkey-server processes of one deployment."""

    def __init__(self, config: DeploymentConfig, seed: Optional[int] = None):
        self.config = config
        self.deployment_id = str(uuid.uuid4())[:8]
        self.seed = seed
        self._spawned_instances: dict[str, InstanceInfo] = {}
        # Deployment directories created under per-instance data roots; removed
        # with the main deployment directory during cleanup.
        self._extra_deployment_dirs: set[str] = set()
        # Deployment directories that hold this process's owner tag.
        self._tagged_dirs: set[str] = set()

    @property
    def spawned_instances(self) -> list[InstanceInfo]:
        """Return instances currently owned by this manager."""
        return list(self._spawned_instances.values())

    @staticmethod
    def resolve_instance_binaries(plans: Iterable[InstancePlan]) -> None:
        """Resolve each plan's own binary (from ``instance_binaries``) to an executable path, in place."""
        for plan in plans:
            if plan.valkey_binary:
                plan.valkey_binary = resolve_binary_path(
                    plan.valkey_binary, f"instance_binaries for {plan.instance_id}"
                )

    def resolve_valkey_binary(self, base_dir: str = SOURCE_BUILD_DIR, build_timeout: float = BUILD_TIMEOUT_S) -> str:
        """The deployment's valkey-server: the configured one, else ``valkey-server`` on PATH, else a build
        under ``base_dir`` within ``build_timeout`` seconds (see :func:`build_valkey`) as a last resort."""
        configured_binary = self.config.valkey_binary
        if configured_binary:
            return resolve_binary_path(configured_binary)
        return installed_valkey_binary(base_dir) or build_valkey(base_dir, build_timeout)

    def create_instance_directories(self, instance_id: str, data_root: Optional[str] = None) -> tuple[str, str]:
        """Create an instance's directories and return its data directory and log file.

        ``data_root`` places the instance's data (RDB, AOF, nodes.conf) on another
        filesystem, e.g. a size-limited resource; logs stay under base_data_dir.
        """
        deployment_dir = os.path.join(data_root or self.config.base_data_dir, f"deployment-{self.deployment_id}")
        if data_root:
            self._extra_deployment_dirs.add(deployment_dir)
        instance_data_dir = os.path.join(deployment_dir, instance_id, "data")
        log_dir = os.path.join(self.config.base_data_dir, "logs")
        if os.path.normpath(self.config.base_data_dir) == DEFAULT_BASE_DATA_DIR:
            # Every user's runs share the default base_data_dir and its logs; a deployment directory is its own.
            shared_directory(self.config.base_data_dir)
            shared_directory(log_dir)

        if deployment_dir not in self._tagged_dirs:
            # Tagged while still empty, before anything is made in it: a kill in between leaves no untagged tree.
            os.makedirs(deployment_dir, exist_ok=True)
            tag_directory(deployment_dir, "deployment", f"deployment {self.deployment_id} seed {self.seed}")
            self._tagged_dirs.add(deployment_dir)
        os.makedirs(instance_data_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)

        # One file per deployment: concurrent runs of one seed must not share a log.
        log_file = os.path.join(log_dir, f"{self.seed}-{self.deployment_id}-{instance_id}.log")
        return instance_data_dir, log_file

    def build_instance_command(
        self,
        port: int,
        data_dir: str,
        log_file: str,
        server_config: Optional[Mapping[str, Any]] = None,
        valkey_binary: Optional[str] = None,
        host: str = "127.0.0.1",
    ) -> list[str]:
        """Build a shell-safe valkey-server command for the configured deployment mode."""
        mode = self.config.mode
        cluster_enabled = mode == DeploymentMode.CLUSTER
        command = [
            valkey_binary or self.config.valkey_binary,
            "--port",
            str(port),
            "--bind",
            host,
        ]
        if host != "127.0.0.1":
            # Replication and cluster-bus connections must originate from the
            # instance's own address so network faults can match instance pairs.
            command.extend(["--bind-source-addr", host])
        command += [
            "--protected-mode",
            "no",
            "--cluster-enabled",
            "yes" if cluster_enabled else "no",
        ]

        if cluster_enabled:
            command.extend(["--cluster-config-file", os.path.join(data_dir, "nodes.conf")])

        command.extend(["--dir", data_dir, "--logfile", log_file, *server_tag_arguments()])
        # The provider's defaults under the instance's own config: what the plan records.
        command.extend(build_server_config_arguments(effective_server_config(cluster_enabled, server_config)))
        return command

    def spawn_all(self, instance_plans: list[InstancePlan], context: Optional[RunContext] = None) -> list[InstanceInfo]:
        """Spawn all Valkey processes and wait for them to be ready; the wait ends at ``context``'s deadline."""
        logger.info("Spawning %s instances", len(instance_plans))
        infos = []

        self.ensure_ports_available(instance_plans)

        try:
            for plan in instance_plans:
                logger.debug("Spawning %s on port %s", plan.instance_id, plan.port)
                instance_data_dir, log_file = self.create_instance_directories(plan.instance_id, plan.data_root)
                # Build command using the configuration resolved while planning.
                cmd = self.build_instance_command(
                    plan.port,
                    instance_data_dir,
                    log_file,
                    server_config=plan.server_config,
                    valkey_binary=plan.valkey_binary,
                    host=plan.host,
                )
                process = None
                try:
                    process = self._start_process(cmd, log_file)

                    info = InstanceInfo(
                        **asdict(plan),
                        pid=process.pid,
                        process=process,
                        started_at=time.time(),
                        data_dir=instance_data_dir,
                        log_file=log_file,
                    )

                    self._spawned_instances[info.instance_id] = info
                    infos.append(info)
                except BaseException:
                    if process is not None and plan.instance_id not in self._spawned_instances:
                        self._terminate_untracked_process(process, plan.instance_id)
                    raise
                logger.debug("Spawned %s with PID %s", plan.instance_id, process.pid)

            logger.info("Waiting for all instances to be ready")
            for instance in infos:
                # Ready once it answers as the process we started; an exit ends the wait at once.
                answered = wait_until(
                    lambda instance=instance: instance.process.poll() is not None or self._answers(instance),
                    timeout=READY_TIMEOUT_S,
                    interval=0.5,
                    what=f"{instance.instance_id} start",
                    retry=(valkey.ConnectionError, valkey.TimeoutError),
                    context=context,
                )
                if not answered or instance.process.poll() is not None:
                    raise LocalDeploymentError(
                        f"Instance {instance.instance_id} failed to start within {READY_TIMEOUT_S:g}s"
                        f"{self._process_output_tail(instance.log_file)}"
                    )

        except BaseException as exc:
            logger.error("Failed while spawning or waiting for instances: %s", exc)
            self._cleanup_failed_spawn(infos)
            raise

        logger.info("All %s instances are active", len(infos))
        return infos

    def ensure_ports_available(self, instances: Sequence[InstancePlan]) -> None:
        """Refuse to start ``instances`` while any of their client (or cluster bus) ports is in use."""
        names = ", ".join(instance.instance_id for instance in instances)
        ensure_ports_free(instances, cluster=self.config.mode is DeploymentMode.CLUSTER, action=f"start {names}")

    def _start_process(self, cmd: list[str], log_file: str) -> subprocess.Popen:
        """Start valkey-server with stdout/stderr appended to a per-instance file.

        Output is never left in an unread pipe, which could block the server
        once the pipe buffer fills.
        """
        output_path = process_output_path(log_file)
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "ab") as output:
            return subprocess.Popen(cmd, stdout=output, stderr=subprocess.STDOUT)

    def _process_output_tail(self, log_file: str, lines: int = 20) -> str:
        """Return a short suffix with the last process output lines, for error messages."""
        output_path = process_output_path(log_file)
        try:
            with open(output_path, encoding="utf-8", errors="replace") as output:
                tail = output.read().splitlines()[-lines:]
        except OSError:
            return ""
        if not tail:
            return ""
        return f" (process output from {output_path}: " + " | ".join(line.strip() for line in tail) + ")"

    @staticmethod
    def _terminate_untracked_process(process: subprocess.Popen, instance_id: str) -> None:
        """Best-effort termination before a process has an InstanceInfo owner."""
        try:
            if process.poll() is None:
                _stop_process(process)
        except BaseException as exc:
            logger.error("Failed to terminate untracked process for %s: %s", instance_id, exc)

    def _cleanup_failed_spawn(self, spawned_instances: list[InstanceInfo]) -> None:
        """Best-effort cleanup when spawning fails before ownership is returned.

        This method deliberately reports cleanup failures instead of raising:
        its caller is already handling a spawn/readiness exception or
        interruption, which must remain the exception observed by the caller.
        """
        instances_by_id = dict(self._spawned_instances)
        instances_by_id.update({instance.instance_id: instance for instance in spawned_instances})
        self._cleanup_resources(
            instances_by_id.values(),
            context="failed spawn",
            raise_on_failure=False,
        )

    def terminate_instance(self, instance: InstanceInfo) -> None:
        """Terminate a Valkey instance process"""
        if not instance.is_running:
            return

        logger.debug("Terminating %s (PID %s)", instance.instance_id, instance.pid)
        _stop_process(instance.process)
        logger.debug("%s terminated", instance.instance_id)

    def restart_instance(
        self,
        instance: InstanceInfo,
        wait_ready: bool = True,
        ready_timeout: float = 30.0,
        context: Optional[RunContext] = None,
    ) -> InstanceInfo:
        """Restart a Valkey instance process; the wait for it to be ready ends at ``context``'s deadline."""
        logger.info("Restarting %s on port %s", instance.instance_id, instance.port)

        # Build command using centralized configuration. The instance keeps its
        # address, binary, and resolved startup config across restarts.
        cmd = self.build_instance_command(
            instance.port,
            instance.data_dir,
            instance.log_file,
            server_config=instance.server_config,
            valkey_binary=instance.valkey_binary,
            host=instance.host,
        )
        mode = self.config.mode
        if mode is DeploymentMode.STANDALONE and instance.role == "replica":
            if not instance.primary_id:
                raise ValueError(f"Replica {instance.instance_id} has no configured primary")

            primary = self._spawned_instances.get(instance.primary_id)
            if primary is None or primary.role != "primary":
                raise ValueError(
                    f"Cannot restart replica {instance.instance_id}: configured primary {instance.primary_id!r} is unavailable"
                )

            # Replication configured at runtime is not persisted by the process
            # command assembled above. Inject the deployment-owned startup
            # directive so even a non-blocking restart cannot briefly become a
            # writable primary.
            cmd.extend(["--replicaof", primary.host, str(primary.port)])

        # A restart replaces the instance's process. Stop a still-running one first
        # so two servers never compete for the same port and data directory.
        if instance.is_running:
            logger.info("Stopping running %s (PID %s) before restart", instance.instance_id, instance.pid)
            self.terminate_instance(instance)
        self.ensure_ports_available([instance])

        process = None
        try:
            # Start new process
            process = self._start_process(cmd, instance.log_file)

            # Update instance info with new process
            instance.process = process
            instance.pid = process.pid
            instance.started_at = time.time()
            self._spawned_instances[instance.instance_id] = instance

            logger.info("Restarted %s with new PID %s", instance.instance_id, process.pid)

            # Wait for instance to be ready if requested
            if wait_ready and not wait_until(
                lambda: self._ready_after_restart(instance, mode),
                timeout=ready_timeout,
                interval=0.5,
                what=f"{instance.instance_id} restart",
                context=context,
            ):
                self.terminate_instance(instance)
                process = None
                raise LocalDeploymentError(
                    f"Instance {instance.instance_id} failed to become ready within {ready_timeout:.2f}s"
                    f"{self._process_output_tail(instance.log_file)}"
                )
            logger.info("Instance %s is ready after restart", instance.instance_id)

            return instance

        except BaseException as e:
            logger.error("Failed to restart %s: %s", instance.instance_id, e)
            if process is not None and process.poll() is None:
                try:
                    self.terminate_instance(instance)
                except BaseException as cleanup_error:
                    logger.error(
                        "Failed to terminate %s after restart failure: %s", instance.instance_id, cleanup_error
                    )
            raise

    @staticmethod
    def _answers(instance: InstanceInfo) -> bool:
        """Whether the instance answers PING as the process we started (a foreign server raises)."""
        with valkey_client(instance.host, instance.port, timeout=3.0) as client:
            if not client.ping():
                return False
            verify_instance_process_identity(instance, client)
            return True

    def _ready_after_restart(self, instance: InstanceInfo, mode: DeploymentMode) -> bool:
        """One readiness check of a restarted instance; its exit or a foreign server ends the wait with an error."""
        if instance.process.poll() is not None:
            raise LocalDeploymentError(f"Instance {instance.instance_id} process died during restart")
        try:
            with valkey_client(instance.host, instance.port, timeout=0.5) as client:
                return self._restart_is_ready(instance, client, mode)
        except (valkey.ValkeyError, OSError) as exc:
            logger.debug("Instance %s is not ready after restart: %s", instance.instance_id, exc)
            return False

    def _restart_is_ready(
        self,
        instance: InstanceInfo,
        client: valkey.Valkey,
        mode: DeploymentMode,
    ) -> bool:
        """Return whether a restarted instance has rejoined its deployment in its expected role.

        In standalone mode a replica is ready once it replicates its primary
        with the link up, or, while that primary's process is not running,
        once it is configured to replicate it: it links when the primary is
        back, so waiting for the link would only wait out the timeout. A
        primary is ready once every replica whose process is running is
        connected.

        Raises InstanceIdentityError if the answering server is not the restarted
        process or, for cluster nodes, does not carry the instance's saved ID.
        """
        if not client.ping():
            return False
        verify_instance_process_identity(instance, client)

        if mode == DeploymentMode.CLUSTER:
            server_info = client.info("cluster")
            cluster_enabled = response_text(server_info.get("cluster_enabled", 0)).lower()
            cluster_info = query_cluster_info(client)
            cluster_state = response_text(cluster_info.get("cluster_state", "")).lower()
            try:
                known_nodes = int(cluster_info.get("cluster_known_nodes", 0))
            except (TypeError, ValueError):
                return False

            expected_nodes = max(len(self._spawned_instances), 1)
            if cluster_enabled not in {"1", "yes", "true"} or cluster_state != "ok" or known_nodes < expected_nodes:
                return False

            cluster_node_id = verify_cluster_node_identity(instance, client)
            unhealthy_flags = {"fail", "fail?", "handshake", "noaddr"}
            healthy_members = [
                node
                for node in parse_cluster_nodes(response_text(client.execute_command("CLUSTER", "NODES")))
                if unhealthy_flags.isdisjoint(node["flags"].split(",")) and node["link_state"] == "connected"
            ]
            self_is_healthy = any(node["is_myself"] and node["node_id"] == cluster_node_id for node in healthy_members)

            # Itself and at least one peer: cluster mode has at least three primaries.
            return self_is_healthy and len(healthy_members) >= 2

        replication = query_replication(client)

        if instance.role == "replica":
            primary = self._spawned_instances.get(instance.primary_id or "")
            if replication.role != "replica" or primary is None:
                return False
            if ((replication.primary_host or "").lower(), replication.primary_port) != (
                primary.host.lower(),
                primary.port,
            ):
                return False
            return replication.link_up or not primary.is_running

        if replication.role != "primary":
            return False
        expected_replicas = sum(
            other.role == "replica" and other.primary_id in {None, instance.instance_id} and other.is_running
            for other in self._spawned_instances.values()
        )
        return (replication.connected_replicas or 0) >= expected_replicas

    def cleanup_deployment(self, instances: list[InstanceInfo]) -> None:
        """Clean up every instance and resource, then report any failures."""
        logger.info("Cleaning up deployment %s", self.deployment_id)
        self._cleanup_resources(
            instances,
            context=f"deployment {self.deployment_id}",
            raise_on_failure=True,
        )
        logger.debug("Deployment %s cleaned up", self.deployment_id)

    def _cleanup_resources(
        self,
        instances: Iterable[InstanceInfo],
        *,
        context: str,
        raise_on_failure: bool,
    ) -> None:
        """Attempt all lifecycle cleanup actions regardless of individual failures."""
        failures: list[tuple[str, BaseException]] = []
        # A server whose termination failed may still be running: keep tracking
        # it and its data, so that a retry stops it.
        still_running: set[str] = set()

        for instance in instances:
            try:
                self.terminate_instance(instance)
            except BaseException as exc:
                failures.append((f"terminate {instance.instance_id}", exc))
                still_running.add(instance.instance_id)
                self._spawned_instances.setdefault(instance.instance_id, instance)
            else:
                self._spawned_instances.pop(instance.instance_id, None)

        deployment_dirs = [os.path.join(self.config.base_data_dir, f"deployment-{self.deployment_id}")]
        deployment_dirs.extend(sorted(self._extra_deployment_dirs))
        if not self.config.enable_cleanup and not still_running:
            # Kept on purpose: the sweep must leave them alone.
            for deployment_dir in deployment_dirs:
                try:
                    untag_directory(deployment_dir)
                except BaseException as exc:
                    failures.append((f"untag {deployment_dir}", exc))
        if self.config.enable_cleanup and not still_running:
            for deployment_dir in deployment_dirs:
                try:
                    deployment_dir_exists = os.path.exists(deployment_dir)
                except BaseException as exc:
                    failures.append((f"inspect {deployment_dir}", exc))
                    deployment_dir_exists = False

                if deployment_dir_exists:
                    try:
                        shutil.rmtree(deployment_dir)
                        logger.info("Deleted data directory")
                    except BaseException as exc:
                        failures.append((f"delete {deployment_dir}", exc))

        for action, exc in failures:
            logger.error("Cleanup action failed during %s (%s): %s", context, action, exc)

        if failures and raise_on_failure:
            interrupt = next((exc for _, exc in failures if not isinstance(exc, Exception)), None)
            if interrupt is not None:
                raise interrupt

            actions = ", ".join(action for action, _ in failures)
            raise RuntimeError(f"Cleanup for {context} completed with failures: {actions}") from failures[0][1]
