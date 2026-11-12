"""Bind a plan to its acquired environment: find its environment references and resolve them from a lease."""

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ..references import collect_environment_references
from .provisioner import EnvironmentLease

if TYPE_CHECKING:
    # The plan module imports this package for the plan's declarations.
    from ..plan import ExecutionPlan


def _has_references(*values: Any) -> bool:
    return any(collect_environment_references(value) for value in values)


def plan_has_references(plan: "ExecutionPlan") -> bool:
    return _has_references(
        plan.inputs,
        plan.deployment,
        plan.instances,
        *(component.config for component in plan.components),
        *(policy.config for policy in plan.policies),
        *(overlay.server_config for overlay in plan.startup_overlays),
    )


def deployment_has_references(plan: "ExecutionPlan") -> bool:
    return _has_references(plan.deployment, *(overlay.server_config for overlay in plan.startup_overlays))


def resolve_plan(plan: "ExecutionPlan", lease: EnvironmentLease) -> "ExecutionPlan":
    """The executable plan: ``plan`` with every environment reference resolved."""

    if not plan_has_references(plan):
        return plan
    return replace(
        plan,
        inputs=lease.resolve(plan.inputs),
        deployment=lease.resolve(plan.deployment),
        instances=lease.resolve(plan.instances),
        components=tuple(replace(item, config=lease.resolve(item.config)) for item in plan.components),
        policies=tuple(replace(item, config=lease.resolve(item.config)) for item in plan.policies),
        startup_overlays=tuple(
            replace(item, server_config=lease.resolve(item.server_config)) for item in plan.startup_overlays
        ),
    )
