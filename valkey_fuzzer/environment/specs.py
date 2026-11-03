"""Environment declarations acquired before the deployment: artifacts, resources, and policies.

An artifact (a binary, module, dataset) or a resource (a filesystem, store,
proxy) is acquired by a registered provider before the deployment
starts. Both are an :class:`EnvironmentSpec`; its ``kind`` is the scenario
section it was declared in. Scenarios reference a provider output exactly as
``{$ref: artifacts.<id>.<output>}`` or ``{$ref: resources.<id>.<output>}``.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Optional

from .._data import freeze_mapping, to_plain_data
from ..fields import as_tuple, require_qualified_id, require_segment, require_text
from ..references import EnvironmentDependency, EnvironmentKind, parse_environment_dependency

# ---------------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnvironmentSpec:
    """An artifact or resource acquired before deployment setup. Config must not hold credentials."""

    kind: EnvironmentKind
    id: str
    provider_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        require_segment(self.id, "id")
        require_qualified_id(self.provider_id, "provider_id")
        object.__setattr__(self, "config", freeze_mapping(self.config, f"{self.kind.value} config", references=True))
        dependencies = tuple(parse_environment_dependency(item) for item in as_tuple(self.depends_on, "depends_on"))
        if len(set(dependencies)) != len(dependencies):
            raise ValueError("depends_on contains duplicate dependencies")
        if self.dependency in dependencies:
            raise ValueError(f"{self.dependency.path} cannot depend on itself")
        object.__setattr__(self, "depends_on", tuple(item.path for item in dependencies))

    @property
    def dependency(self) -> EnvironmentDependency:
        return EnvironmentDependency(self.kind, self.id)

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)


@dataclass(frozen=True)
class PolicySpec:
    """Opaque policy intent carried to providers and verdict components."""

    policy_id: str
    policy_type: str
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_segment(self.policy_id, "policy_id")
        require_qualified_id(self.policy_type, "policy_type")
        object.__setattr__(self, "config", freeze_mapping(self.config, "policy config", references=True))

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)


@dataclass(frozen=True)
class SupportDecision:
    """A provider's report-safe answer, before any mutation, to "can you do this here?"."""

    supported: bool
    reason: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.supported, bool):
            raise TypeError("supported must be a boolean")
        if self.supported and self.reason is not None:
            raise ValueError("a supported decision cannot include a rejection reason")
        if not self.supported:
            require_text(self.reason, "unsupported decision reason")
        object.__setattr__(self, "details", freeze_mapping(self.details, "support decision details"))

    @classmethod
    def allow(cls, *, details: Optional[Mapping[str, Any]] = None) -> "SupportDecision":
        return cls(supported=True, details=details or {})

    @classmethod
    def deny(cls, reason: str, *, details: Optional[Mapping[str, Any]] = None) -> "SupportDecision":
        return cls(supported=False, reason=reason, details=details or {})

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)
