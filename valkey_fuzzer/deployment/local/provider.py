"""The local deployment provider: Valkey deployments as processes on this host.

``validate`` plans a deployment without side effects. ``provision`` resolves
the binaries, plans the instances, applies the plan's startup overlays,
spawns them, and forms the deployment; ``rollback`` and ``cleanup`` stop the
processes and remove their data.
"""

import logging
import os
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from ..._data import exception_type, file_sha256, to_plain_data
from ...plan import ExecutionPlan, StartupOverlay
from ...protocols import ProvisionedDeployment, RunContext
from ...registrations import DeploymentProviderRegistration
from ...schema import plain_config
from . import rerun
from .config import deployment_config_from_document, normalize_deployment_document
from .formation import Formation
from .models import (
    DeploymentConfig,
    DeploymentMode,
    InstancePlan,
    LocalDeployment,
    LocalDeploymentError,
    LocalDeploymentValidationError,
)
from .planner import plan_deployment
from .ports import PortUnavailableError, claim_loopback_block, ensure_ports_free, ephemeral_overlap
from .processes import BUILD_TIMEOUT_S, ProcessManager, detect_valkey_sha
from .server_config import effective_server_config
from .service import VALKEY_DEPLOYMENT_SERVICE, ValkeyDeploymentService

logger = logging.getLogger(__name__)

LOCAL_DEPLOYMENT_PROVIDER_ID = "valkey-fuzzer.deployment.local"


def deployment_metadata(deployment: LocalDeployment) -> dict[str, Any]:
    """Report-safe provenance: instance layout, binaries, and processes."""
    default_binary = deployment.config.valkey_binary or ""
    binaries = [info.valkey_binary or default_binary for info in deployment.instances]
    digests = {binary: file_sha256(binary) for binary in set(binaries) if binary}
    instances = []
    for info, binary in zip(deployment.instances, binaries):
        instances.append(
            {
                "instance_id": info.instance_id,
                "role": info.role,
                "shard_id": info.shard_id,
                "host": info.host,
                "port": info.port,
                "pid": info.pid,
                "binary": binary,
                "binary_sha256": digests.get(binary),
            }
        )
    return {
        "deployment_id": deployment.deployment_id,
        "mode": DeploymentMode(deployment.config.mode).value,
        "addressing": deployment.config.addressing,
        "valkey_sha": deployment.valkey_sha,
        "instances": instances,
    }


def _release(processes: ProcessManager, instances: list, what: str) -> None:
    """Stop and remove every process; a failure raises :class:`LocalDeploymentError`, an interruption re-raises.

    The error omits the underlying exception text, which may echo resolved
    run-time values.
    """
    try:
        processes.cleanup_deployment(instances)
    except Exception as exc:
        logger.error("Failed to %s: stop the instances and remove their data: %s", what, exception_type(exc))
        raise LocalDeploymentError(f"failed to {what}") from exc


class LocalDeploymentProvider:
    """Provision standalone and cluster deployments locally."""

    def __init__(self):
        # The processes of the provisioned deployment, and of an attempt not yet provisioned or rolled back.
        self._active: Optional[ProcessManager] = None
        self._active_deployment: Optional[ProvisionedDeployment] = None
        self._pending: Optional[ProcessManager] = None
        # The descriptor holding this deployment's loopback block (per-instance addressing).
        self._block: Optional[int] = None

    @staticmethod
    def _prepare(plan: ExecutionPlan) -> tuple[DeploymentConfig, tuple[InstancePlan, ...]]:
        deployment = to_plain_data(plan.deployment)
        try:
            config = deployment_config_from_document(deployment)
            if config.mode.value != plan.deployment_mode:
                raise LocalDeploymentValidationError(
                    f"deployment document selects {config.mode.value!r}, but the plan selects {plan.deployment_mode!r}"
                )

            # Planning only mutates these throwaway in-memory objects. It does
            # not resolve/build a binary, create directories, spawn processes,
            # or open sockets.
            dry_plans = plan_deployment(config, plan.startup_overlays)
        except LocalDeploymentValidationError:
            raise
        except (TypeError, ValueError) as exc:
            raise LocalDeploymentValidationError(str(exc)) from exc
        return config, tuple(dry_plans)

    def validate(self, plan: ExecutionPlan) -> None:
        """Perform a complete, externally side-effect-free preflight."""
        self._prepare(plan)

    def _launch(
        self, config: DeploymentConfig, instance_plans: Sequence[InstancePlan], context: RunContext
    ) -> LocalDeployment:
        """Resolve the binaries, then spawn the planned instances and form them, by the run's deadline; partial
        state stays in ``self._pending``."""
        mode = config.mode
        logger.info(
            "Creating a %s deployment with %s primary(ies) and %s replica(s) per primary",
            mode.value,
            config.primaries,
            config.replicas_per_primary,
        )
        processes = ProcessManager(config, context.plan.seed)
        self._pending = processes

        build_timeout = BUILD_TIMEOUT_S
        if context.deadline is not None:
            build_timeout = min(build_timeout, max(0.0, context.deadline - time.monotonic()))
        config.valkey_binary = processes.resolve_valkey_binary(build_timeout=build_timeout)
        processes.resolve_instance_binaries(instance_plans)
        logger.debug("Using Valkey binary: %s", config.valkey_binary)
        valkey_sha = detect_valkey_sha(config.valkey_binary)
        if valkey_sha:
            logger.info("Valkey binary built from commit: %s", valkey_sha)
        else:
            logger.debug("Could not determine valkey-server build SHA")

        instances = processes.spawn_all(list(instance_plans), context)
        Formation(context).form(instances, mode=mode)
        return LocalDeployment(
            deployment_id=processes.deployment_id,
            config=config,
            instances=instances,
            valkey_sha=valkey_sha,
        )

    def _rollback_pending(self) -> None:
        processes = self._pending
        if processes is not None:
            _release(processes, processes.spawned_instances, "roll back the local deployment")
            self._pending = None
        self._release_block()

    def _release_block(self) -> None:
        """Release the loopback block, once nothing of the deployment runs on it."""
        if self._block is not None:
            os.close(self._block)
            self._block = None

    def provision(self, context: RunContext) -> ProvisionedDeployment:
        """Provision a preflighted deployment; a failed attempt is cleaned up at once or by ``rollback``."""
        if self._active_deployment is not None:
            raise LocalDeploymentError("this provider already owns a provisioned deployment")
        if self._pending is not None:
            raise LocalDeploymentError("this provider still owns a partial deployment; rollback it before provisioning")

        plan = context.plan
        config, instance_plans = self._prepare(plan)
        # Fail before resolving binaries or starting anything when a planned port is
        # taken, or could be taken by an outgoing connection before Valkey binds it;
        # ports are part of the plan, so they are never moved silently.
        cluster = config.mode is DeploymentMode.CLUSTER
        ensure_ports_free(instance_plans, cluster=cluster, action="provision the local deployment")
        overlap = ephemeral_overlap(config.base_port, len(instance_plans), cluster=cluster, field="base_port")
        if overlap:
            raise PortUnavailableError(f"Cannot provision the local deployment: {overlap}.")
        if config.addressing == "per_instance_loopback":
            self._block = claim_loopback_block(config.loopback_block)
        try:
            launched = self._launch(config, instance_plans, context)
            processes = self._pending
            handle = ValkeyDeploymentService(processes, launched, context)
            deployment = ProvisionedDeployment(
                handle=handle,
                services={VALKEY_DEPLOYMENT_SERVICE: handle},
                valkey_sha=launched.valkey_sha,
                metadata=deployment_metadata(launched),
            )
        except BaseException:
            # Stop whatever started now; a failed rollback leaves the attempt
            # pending, so the runner's rollback retries it.
            try:
                self._rollback_pending()
            except BaseException as cleanup_error:
                logger.error("Cleanup after a failed provision failed: %s", exception_type(cleanup_error))
            raise

        self._active = processes
        self._active_deployment = deployment
        self._pending = None
        return deployment

    def rollback(self, context: RunContext) -> None:
        """Best-effort, idempotent cleanup of a failed provision attempt."""
        del context
        self._rollback_pending()

    def cleanup(self, context: RunContext, deployment: ProvisionedDeployment) -> None:
        """Clean up the exact deployment returned by this provider."""
        if deployment is not self._active_deployment or self._active is None:
            raise LocalDeploymentError("cleanup received a deployment not owned by this provider")

        handle = deployment.handle
        processes = self._active
        handle.close(lambda instances: _release(processes, instances, f"clean up deployment {handle.deployment_id!r}"))
        self._active_deployment = None
        self._active = None
        self._release_block()


def plan_instances(document: Mapping[str, Any], overlays: Sequence[StartupOverlay]) -> dict[str, dict[str, Any]]:
    """Each planned instance's role, shard, and effective server config (pure).

    The effective config is the provider's defaults (``DEFAULT_SERVER_CONFIG``)
    under the deployment's ``server_config``, its ``instance_configs``, and
    the startup overlays, exactly as the instance is started.
    """
    config = deployment_config_from_document(plain_config(document))
    plans = plan_deployment(config, overlays)
    cluster = config.mode is DeploymentMode.CLUSTER
    return {
        plan.instance_id: {
            "role": plan.role,
            "shard": plan.shard_id,
            "server_config": effective_server_config(cluster, plan.server_config),
        }
        for plan in plans
    }


def local_deployment_provider_registrations() -> dict[str, DeploymentProviderRegistration]:
    """Return replay-identifiable registrations for local Valkey deployments."""

    return {
        mode.value: DeploymentProviderRegistration(
            deployment_mode=mode.value,
            provider_id=LOCAL_DEPLOYMENT_PROVIDER_ID,
            factory=LocalDeploymentProvider,
            normalizer=normalize_deployment_document,
            plan_instances=plan_instances,
            binary_override=rerun.binary_override,
            relocate_ports=rerun.relocate_ports,
            recorded_instances=rerun.deployment_instances,
            binary_drift=rerun.binary_drift,
            reductions=rerun.deployment_reductions,
            host_fields=rerun.HOST_FIELDS,
        )
        for mode in DeploymentMode
    }
