"""A run's coverage tuple: the semantic combination its plan exercises.

Every run report carries the tuple and its key (``coverage``), and the batch
summary counts the distinct keys each experiment covered. A tuple is built only
from the plan's generic structure: experiment, deployment provider, the
planned instances (their roles, shards, and server config), the rest of the
deployment document but for where it places the deployment on the host,
inputs, the workload/action/fault/observer/oracle composition, and the
artifact, resource, and policy declarations with their config. Nothing here
interprets experiment or component IDs, or a provider's deployment document.
"""

import hashlib
import json
from collections import Counter
from collections.abc import Collection
from typing import Any

from ._data import to_plain_data
from .plan import ExecutionPlan
from .protocols import ComponentRole
from .results import RunOutcome


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def static_tuple(plan: ExecutionPlan, host_fields: Collection[str] = ()) -> dict[str, Any]:
    """The semantic tuple a plan targets, independent of its result.

    ``host_fields`` are the deployment fields that only place the deployment
    on a host (its provider's ``host_fields``): a run on other ports is the
    same combination.
    """

    instances = to_plain_data(plan.instances)
    components = to_plain_data(plan.components)

    def of_role(role: ComponentRole) -> list:
        return sorted({component["use"] for component in components if component["role"] == role.value})

    config_mix = {
        "instances": sorted((str(item.get("role")), _digest(item["server_config"])) for item in instances.values()),
        "overlays": [to_plain_data(overlay) for overlay in plan.startup_overlays],
    }
    # Different builds are different combinations, whether a scenario names a
    # binary in its deployment or through an artifact declaration.
    document = {
        name: value
        for name, value in to_plain_data(plan.deployment).items()
        if name not in host_fields and name != "mode"
    }
    data = {
        "experiment": plan.experiment_id,
        # The same deployment shape on another deployment provider is another
        # combination.
        "deployment": {
            "mode": plan.deployment_mode,
            "provider": plan.deployment_provider.provider_id,
            "roles": dict(sorted(Counter(str(item.get("role")) for item in instances.values()).items())),
            "shards": len({item.get("shard") for item in instances.values()}),
        },
        "config_mix": _digest(config_mix),
        "deployment_mix": _digest(document),
        "inputs": to_plain_data(plan.inputs),
        "artifact_mix": _digest(
            sorted(
                (artifact.id, artifact.provider_id, _digest(to_plain_data(artifact.config)))
                for artifact in plan.artifacts
            )
        ),
        "resources": sorted(resource.provider_id for resource in plan.resources),
        "policies": sorted(policy.policy_type for policy in plan.policies),
        "workloads": of_role(ComponentRole.WORKLOAD),
        "actions": of_role(ComponentRole.ACTION),
        "faults": of_role(ComponentRole.FAULT),
        "observers": of_role(ComponentRole.OBSERVER),
        "oracles": of_role(ComponentRole.ORACLE),
        # A different tmpfs size or policy threshold is a different combination.
        "resource_mix": _digest(
            sorted((resource.provider_id, _digest(to_plain_data(resource.config))) for resource in plan.resources)
        ),
        "policy_mix": _digest(
            sorted((policy.policy_type, _digest(to_plain_data(policy.config))) for policy in plan.policies)
        ),
    }
    return data


def coverage_tuple(outcome: RunOutcome, host_fields: Collection[str] = ()) -> dict[str, Any]:
    """A run's coverage: its plan's tuple (:func:`static_tuple`) and the tuple's key, with the run's status and
    evidence IDs."""

    static = static_tuple(outcome.plan, host_fields)
    return {
        "key": _digest(static),
        "tuple": static,
        "status": outcome.status.value,
        "evidence": sorted({item.evidence_id for item in outcome.all_evidence}),
    }
