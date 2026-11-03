"""Running a scenario again, on this host or smaller: the local provider's run-service hooks.

A generated scenario's ports are not part of what it tests, so when they are
taken it moves to free ones. A replay must run the binaries the recorded run
ran, which the local deployment records per instance (``binary`` and
``binary_sha256`` in its metadata). The minimizer shrinks a deployment by its
instance counts and drops its server config. See
:class:`~valkey_fuzzer.registrations.DeploymentProviderRegistration` for the
hooks these implement.
"""

import copy
from collections.abc import Mapping
from typing import Any, Optional

from ..._data import file_sha256, to_plain_data
from ...minimize import smaller
from ...plan import ExecutionPlan
from . import config, ports
from .models import DeploymentMode
from .planner import instance_value
from .processes import installed_valkey_binary, resolve_binary_path

# How many other base ports a generated scenario may move to when its planned ports are taken.
_BASE_PORT_TRIES = 64
# Where the deployment runs on this host, not what it tests.
HOST_FIELDS = ("base_port", "base_data_dir", "instance_data_roots", "loopback_block", "enable_cleanup")


def binary_override(path: str) -> dict[str, Any]:
    """The deployment fields that run the Valkey binary ``path`` on every instance without its own."""

    return {"valkey_binary": path}


def relocate_ports(plan: ExecutionPlan) -> tuple[dict[str, Any], Optional[str]]:
    """A ``base_port`` whose planned client and bus ports are free now, when ``plan``'s are not, and a note.

    Only ``deployment.base_port`` changes, so a seed's draws stay the same,
    and the scenario records the port it ran on. Ports in the host's
    ephemeral range count as taken: an outgoing connection can take one
    before Valkey binds it. A deployment whose ports cannot be planned
    before its environment exists (one naming an artifact or resource) is
    left as it is.
    """

    document = to_plain_data(plan.deployment)
    try:
        resolved = config.deployment_config_from_document(document)
        busy = ports.busy_planned_ports(document)
    except (TypeError, ValueError):
        return {}, None
    original, count = resolved.base_port, len(plan.instances)
    highest = ports.highest_base_port(count, cluster=resolved.mode is DeploymentMode.CLUSTER)
    if busy:
        shown = ", ".join(str(item) for item in busy[:5]) + (", ..." if len(busy) > 5 else "")
        problem = f"{shown} in use"
    elif original > highest:
        problem = f"its ports reach this host's ephemeral port range (base_port at most {highest})"
    else:
        return {}, None
    # Free blocks above the original first, then below it, never past ``highest``.
    bases = [*range(original + count, highest + 1, count), *range(min(original - count, highest), 1023, -count)]
    for base in bases[:_BASE_PORT_TRIES]:
        if not ports.busy_planned_ports({**document, "base_port": base}):
            return {"base_port": base}, (
                f"seed {plan.seed} plans base_port {original}, but {problem}; "
                f"using base_port {base}, which the scenario records"
            )
    return {}, f"seed {plan.seed} plans base_port {original}, but {problem}"


def deployment_reductions(
    deployment: Mapping[str, Any], requirements: Mapping[str, Any]
) -> list[tuple[str, dict[str, Any]]]:
    """Smaller deployment sections, most aggressive first: fewer instances, then less server config.

    The experiment's ``requirements`` bound the instance counts; a size the
    deployment refuses fails to compile, which costs no run.
    """

    reductions: list[tuple[str, dict[str, Any]]] = []
    for name in ("replicas_per_primary", "primaries"):
        minimum = (requirements.get(name) or {}).get("minimum", 0)
        for value in smaller(deployment.get(name), minimum):
            reductions.append((f"deployment.{name}={value}", {**copy.deepcopy(dict(deployment)), name: value}))
    for field, label in (
        ("server_config", "drop deployment.server_config.{}"),
        ("instance_configs", "drop deployment.instance_configs.{}"),
    ):
        for key in sorted(deployment.get(field) or {}):
            reduced = copy.deepcopy(dict(deployment))
            del reduced[field][key]
            reductions.append((label.format(key), reduced))
    return reductions


def deployment_instances(outcome: Any) -> list[Mapping[str, Any]]:
    """The per-instance binary provenance the local deployment left in an outcome's (plain) diagnostics."""

    diagnostics = outcome.get("diagnostics") if isinstance(outcome, Mapping) else None
    deployment = diagnostics.get("deployment") if isinstance(diagnostics, Mapping) else None
    instances = deployment.get("instances") if isinstance(deployment, Mapping) else None
    if not isinstance(instances, (list, tuple)):
        return []
    return [
        instance
        for instance in instances
        if isinstance(instance, Mapping) and isinstance(instance.get("instance_id"), str)
    ]


def _instance_binary(plan: ExecutionPlan, instance: Mapping[str, Any]) -> tuple[Any, bool]:
    """The binary setting ``instance`` starts with (a path or an environment reference), and whether it is the
    deployment default."""

    override = instance_value(plan.deployment.get("instance_binaries"), instance["instance_id"], instance.get("role"))
    if override is not None:
        return override, False
    return plan.deployment.get("valkey_binary") or "", True


def _resolve_binary(setting: str) -> Optional[str]:
    """The binary ``setting`` names, as the deployment resolves it; the default one only if it exists already."""
    if not setting:
        return installed_valkey_binary()
    try:
        return resolve_binary_path(setting)
    except OSError:
        return None


def binary_drift(
    plan: ExecutionPlan,
    recorded: list[Mapping[str, Any]],
    *,
    override: Optional[str] = None,
    replayed: Optional[list[Mapping[str, Any]]] = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Compare each ``recorded`` instance's binary digest with the replay's, and say what could not be.

    Before the run the binary an instance would start is resolved and hashed;
    given the ``replayed`` instances, the replay's own deployment metadata is used. Instances
    on the deployment default are skipped under an explicit ``override``.
    """

    replayed_by_id = None if replayed is None else {instance["instance_id"]: instance for instance in replayed}
    digests: dict[str, Optional[str]] = {}
    mismatched: dict[tuple[str, str], list[str]] = {}
    unchecked: dict[str, list[str]] = {}
    for instance in recorded:
        instance_id, recorded_digest = instance["instance_id"], instance.get("binary_sha256")
        setting, default = _instance_binary(plan, instance)
        if default and override:
            continue
        if not recorded_digest:
            unchecked.setdefault("the recorded run has no digest for it", []).append(instance_id)
            continue
        if replayed_by_id is not None:
            replay_instance = replayed_by_id.get(instance_id) or {}
            path, digest = replay_instance.get("binary"), replay_instance.get("binary_sha256")
        elif not isinstance(setting, str):
            unchecked.setdefault("it comes from an environment provider at run time", []).append(instance_id)
            continue
        else:
            path = _resolve_binary(setting)
            if path is not None and path not in digests:
                digests[path] = file_sha256(path)
            digest = None if path is None else digests[path]
        if not digest:
            # A reference names its path (artifacts.<id>.<output>).
            unchecked.setdefault(f"the replay's binary ({setting or 'valkey-server'}) cannot be read", []).append(
                instance_id
            )
        elif digest != recorded_digest:
            before = f"{instance.get('binary')} (sha256 {recorded_digest})"
            mismatched.setdefault((before, f"{path} (sha256 {digest})"), []).append(instance_id)
    drift = [
        {"item": f"Valkey binary of {', '.join(instances)}", "recorded": before, "replay": after}
        for (before, after), instances in mismatched.items()
    ]
    return drift, [f"Valkey binary of {', '.join(instances)}: {reason}" for reason, instances in unchecked.items()]
