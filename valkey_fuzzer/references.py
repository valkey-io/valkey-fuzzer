"""Typed environment references (``artifacts.<id>.<output>``, ``resources.<id>.<output>``) and their
serialized ``{"$valkey_fuzzer_ref": path}`` form.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .fields import require_segment

REFERENCE_DATA_KEY = "$valkey_fuzzer_ref"


class EnvironmentKind(str, Enum):
    """Addressable namespaces in the environment acquired before the deployment."""

    ARTIFACT = "artifacts"
    RESOURCE = "resources"


class EnvironmentReferenceError(ValueError):
    """An environment reference is malformed, unknown, undeclared, or unresolved."""


@dataclass(frozen=True)
class EnvironmentDependency:
    """One artifact or resource declaration: ``artifacts.<id>`` or ``resources.<id>``."""

    kind: EnvironmentKind
    id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        require_segment(self.id, "environment dependency id")

    @property
    def path(self) -> str:
        return f"{self.kind.value}.{self.id}"

    def __str__(self) -> str:
        return self.path


@dataclass(frozen=True)
class EnvironmentReference:
    """An exact reference to one declared provider output."""

    kind: EnvironmentKind
    id: str
    output: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        require_segment(self.id, "environment reference id")
        require_segment(self.output, "environment reference output")

    @property
    def dependency(self) -> EnvironmentDependency:
        return EnvironmentDependency(self.kind, self.id)

    @property
    def path(self) -> str:
        return f"{self.kind.value}.{self.id}.{self.output}"

    def __str__(self) -> str:
        return self.path


def collect_environment_references(value: Any) -> tuple[EnvironmentReference, ...]:
    """Distinct references in ``value``, in first-observed order."""

    found: dict[EnvironmentReference, None] = {}

    def walk(item: Any) -> None:
        if isinstance(item, EnvironmentReference):
            found.setdefault(item)
        elif isinstance(item, Mapping):
            for nested in item.values():
                walk(nested)
        elif isinstance(item, (list, tuple)):
            for nested in item:
                walk(nested)

    walk(value)
    return tuple(found)


def _parse_path(value: Any, parts: int, expected: str) -> list[str]:
    if not isinstance(value, str) or value != value.strip():
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: expected {expected}")
    segments = value.split(".")
    if len(segments) != parts or segments[0] not in {kind.value for kind in EnvironmentKind}:
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: expected {expected}")
    return segments


def parse_environment_dependency(value: Any) -> EnvironmentDependency:
    """Parse exactly ``artifacts.<id>`` or ``resources.<id>``."""

    kind, declaration_id = _parse_path(value, 2, "'artifacts.<id>' or 'resources.<id>'")
    try:
        return EnvironmentDependency(EnvironmentKind(kind), declaration_id)
    except ValueError as exc:
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: {exc}") from exc


def parse_environment_reference(value: Any) -> EnvironmentReference:
    """Parse exactly ``artifacts.<id>.<output>`` or ``resources.<id>.<output>``."""

    kind, declaration_id, output = _parse_path(value, 3, "'artifacts.<id>.<output>' or 'resources.<id>.<output>'")
    try:
        return EnvironmentReference(EnvironmentKind(kind), declaration_id, output)
    except ValueError as exc:
        raise EnvironmentReferenceError(f"invalid environment reference {value!r}: {exc}") from exc


def references_from_data(value: Any) -> Any:
    """Decode serialized ``{"$valkey_fuzzer_ref": path}`` markers into references."""

    if isinstance(value, Mapping):
        if REFERENCE_DATA_KEY in value:
            if set(value) != {REFERENCE_DATA_KEY}:
                raise EnvironmentReferenceError(
                    f"a serialized reference marker must contain only the {REFERENCE_DATA_KEY!r} key"
                )
            return parse_environment_reference(value[REFERENCE_DATA_KEY])
        return {key: references_from_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [references_from_data(item) for item in value]
    return value
