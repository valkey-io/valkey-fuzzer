"""Artifacts, resources, and policies a run acquires before its deployment starts.

:mod:`.specs` holds the declarations, :mod:`.providers` the provider
contracts, registrations, and dependency graph, :mod:`.provisioner`
acquires and releases a plan's declarations as one unit, and :mod:`.binding`
resolves a plan's references from what was acquired.
"""

from .binding import deployment_has_references, plan_has_references, resolve_plan
from .providers import (
    EnvironmentProvider,
    EnvironmentProviderContext,
    EnvironmentProviderRegistration,
    EnvironmentProviderRegistry,
    EnvironmentResult,
    ProviderError,
    check_references,
    resolve_environment_references,
    stable_environment_order,
)
from .provisioner import (
    EnvironmentCleanupError,
    EnvironmentLease,
    EnvironmentLifecycleError,
    EnvironmentProvisioner,
    EnvironmentReport,
    UnsupportedEnvironmentError,
)
from .specs import EnvironmentSpec, PolicySpec, SupportDecision

__all__ = [
    "EnvironmentCleanupError",
    "EnvironmentLease",
    "EnvironmentLifecycleError",
    "EnvironmentProvider",
    "EnvironmentProviderContext",
    "EnvironmentProviderRegistration",
    "EnvironmentProviderRegistry",
    "EnvironmentProvisioner",
    "EnvironmentReport",
    "EnvironmentResult",
    "EnvironmentSpec",
    "PolicySpec",
    "ProviderError",
    "SupportDecision",
    "UnsupportedEnvironmentError",
    "check_references",
    "deployment_has_references",
    "plan_has_references",
    "resolve_environment_references",
    "resolve_plan",
    "stable_environment_order",
]
