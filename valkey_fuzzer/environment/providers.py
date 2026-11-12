"""Environment provider contracts, results, registrations, and the dependency graph.

Providers acquire artifacts and resources; the graph functions
validate a plan's declarations against the registered providers and order
them so every declaration is acquired after what it depends on.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol, runtime_checkable

from .._data import freeze, freeze_mapping, to_plain_data
from ..fields import as_tuple, require_qualified_id, require_segment
from ..ordering import stable_order
from ..references import (
    EnvironmentDependency,
    EnvironmentKind,
    EnvironmentReference,
    EnvironmentReferenceError,
    collect_environment_references,
    parse_environment_dependency,
)
from ..registry import BaseRegistry, UnknownRegistrationError
from ..schema import ConfigSchema, plain_config
from .specs import EnvironmentSpec, PolicySpec, SupportDecision


class ProviderError(RuntimeError):
    """A provider broke its contract or returned an invalid result."""

    def __init__(self, provider_id: str, reason: str):
        self.provider_id = provider_id
        self.reason = reason
        super().__init__(f"provider {provider_id!r}: {reason}")


# ---------------------------------------------------------------------------
# Provider results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnvironmentResult:
    """What a provider acquired: runtime outputs plus report-safe provenance; only provenance is reported."""

    outputs: Mapping[str, Any] = field(repr=False, compare=False, metadata={"serialize": False})
    provenance: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        outputs = freeze_mapping(self.outputs, "environment outputs")
        for name in outputs:
            require_segment(name, "output name")
        object.__setattr__(self, "outputs", outputs)
        object.__setattr__(self, "provenance", freeze_mapping(self.provenance, "environment provenance"))

    @property
    def output_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.outputs))

    def to_data(self) -> Mapping[str, Any]:
        """A report-safe record: output names and provenance, never output values."""

        return {"output_names": list(self.output_names), "provenance": to_plain_data(self.provenance)}


# ---------------------------------------------------------------------------
# Provider contracts and registrations
# ---------------------------------------------------------------------------


@runtime_checkable
class EnvironmentProviderContext(Protocol):
    """Services available to a provider for one declaration.

    ``deadline`` is an absolute monotonic time or ``None``; ``artifact_dir`` is
    a directory private to this declaration.
    """

    @property
    def run_id(self) -> str: ...

    @property
    def seed(self) -> int: ...

    @property
    def deadline(self) -> Optional[float]: ...

    @property
    def artifact_dir(self) -> str: ...


@runtime_checkable
class EnvironmentProvider(Protocol):
    """Acquires an artifact or a resource. ``check_support`` must not mutate anything;
    ``rollback`` follows a failed ``acquire`` and ``cleanup`` a successful one."""

    def check_support(
        self, context: EnvironmentProviderContext, spec: EnvironmentSpec, resolved_config: Mapping[str, Any]
    ) -> SupportDecision: ...

    def acquire(
        self, context: EnvironmentProviderContext, spec: EnvironmentSpec, resolved_config: Mapping[str, Any]
    ) -> EnvironmentResult: ...

    def rollback(self, context: EnvironmentProviderContext, spec: EnvironmentSpec) -> None: ...

    def cleanup(
        self, context: EnvironmentProviderContext, spec: EnvironmentSpec, result: EnvironmentResult
    ) -> None: ...


@dataclass(frozen=True)
class EnvironmentProviderRegistration:
    """A provider factory for one declaration kind, its declared outputs, and the schema of its config.

    :meth:`resolve_config` checks a declaration's config through ``config``
    and fills in its defaults; without ``config`` the config is taken as
    written. The compiler records the result in the plan, and the provisioner
    resolves the concrete config (references replaced by their outputs) the
    same way and passes it to the provider. ``check(spec, config)`` adds the
    provider's own pure checks of the resolved config; at compile time,
    reference fields may still hold references.
    """

    kind: EnvironmentKind
    provider_id: str
    factory: Callable[[], EnvironmentProvider]
    outputs: tuple[str, ...] = ()
    config: Optional[ConfigSchema] = None
    check: Optional[Callable[[EnvironmentSpec, Mapping[str, Any]], None]] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", EnvironmentKind(self.kind))
        require_qualified_id(self.provider_id, "provider_id")
        outputs = as_tuple(self.outputs, "outputs")
        for output in outputs:
            require_segment(output, "declared output")
        if len(set(outputs)) != len(outputs):
            raise ValueError("outputs contains duplicate declarations")
        object.__setattr__(self, "outputs", outputs)

    def resolve_config(self, spec: EnvironmentSpec, config: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
        """``config``, by default the declaration's own, checked and with its defaults filled in.

        Raises :class:`~valkey_fuzzer.schema.ConfigError` naming the field
        (under ``<kind>.<id>.with``), or whatever ``check`` raises.
        """

        value = spec.config if config is None else config
        location = f"{spec.dependency.path}.with"
        resolved = plain_config(value) if self.config is None else self.config.resolve(value, location)
        if self.check is not None:
            self.check(spec, resolved)
        return resolved

    def validate_result(self, result: Any) -> None:
        if not isinstance(result, EnvironmentResult):
            raise ProviderError(self.provider_id, "expected an EnvironmentResult")
        missing = sorted(set(self.outputs) - set(result.outputs))
        unexpected = sorted(set(result.outputs) - set(self.outputs))
        if missing or unexpected:
            raise ProviderError(
                self.provider_id,
                f"returned outputs that do not match its declaration (missing {missing}, extra {unexpected})",
            )


class EnvironmentProviderRegistry(BaseRegistry[EnvironmentProviderRegistration]):
    """Artifact and resource providers, keyed by ``(kind, provider_id)`` with the kind's value."""

    label = "environment provider"
    entry_type = EnvironmentProviderRegistration

    def key(self, entry: EnvironmentProviderRegistration) -> tuple[str, str]:
        return (entry.kind.value, entry.provider_id)

    def registration_for(self, spec: EnvironmentSpec) -> EnvironmentProviderRegistration:
        """The provider registered for ``spec``'s kind and provider ID."""

        registration = self.get((spec.kind.value, spec.provider_id))
        if registration is None:
            raise UnknownRegistrationError(f"{spec.dependency.path} provider", spec.provider_id)
        return registration


# ---------------------------------------------------------------------------
# References and the dependency graph
# ---------------------------------------------------------------------------


def resolve_environment_references(value: Any, results: Mapping[EnvironmentDependency, EnvironmentResult]) -> Any:
    """Replace every reference in ``value`` with the output it names in ``results`` (frozen)."""

    def output(reference: EnvironmentReference) -> Any:
        record = results.get(reference.dependency)
        if record is None or reference.output not in record.outputs:
            raise EnvironmentReferenceError(f"cannot resolve {reference.path!r}: it has not been acquired")
        return record.outputs[reference.output]

    def resolve(item: Any) -> Any:
        if isinstance(item, EnvironmentReference):
            return output(item)
        if isinstance(item, Mapping):
            return {key: resolve(nested) for key, nested in item.items()}
        if isinstance(item, (list, tuple)):
            return [resolve(nested) for nested in item]
        return item

    return freeze(resolve(value), "resolved value")


def check_references(
    source: str,
    value: Any,
    declarations: Mapping[EnvironmentDependency, EnvironmentSpec],
    registry: EnvironmentProviderRegistry,
) -> None:
    """Every reference in ``value`` must name a declaration and a declared output."""

    for reference in collect_environment_references(value):
        target = declarations.get(reference.dependency)
        if target is None:
            raise EnvironmentReferenceError(
                f"{source} references unknown environment target {reference.dependency.path!r}"
            )
        registration = registry.registration_for(target)
        if reference.output not in registration.outputs:
            raise EnvironmentReferenceError(
                f"{source} references undeclared output {reference.output!r} on {reference.dependency.path!r}; "
                f"provider {registration.provider_id!r} declares {list(registration.outputs) or 'none'}"
            )


def stable_environment_order(
    artifacts: Sequence[EnvironmentSpec],
    resources: Sequence[EnvironmentSpec],
    *,
    registry: EnvironmentProviderRegistry,
    policies: Sequence[PolicySpec] = (),
) -> tuple[EnvironmentSpec, ...]:
    """Validate the declarations against their providers and return them in dependency order.

    Providers must be registered and accept their declaration, references must
    name declared outputs, and explicit ``depends_on`` plus references must form
    an acyclic graph. Among ready declarations, declaration order (artifacts,
    then resources) is preserved. The plan already holds well-typed, unique
    declarations and policy IDs.
    """

    specs: tuple[EnvironmentSpec, ...] = (*artifacts, *resources)
    declarations = {spec.dependency: spec for spec in specs}

    edges: dict[EnvironmentDependency, tuple[EnvironmentDependency, ...]] = {}
    for spec in specs:
        registry.registration_for(spec).resolve_config(spec)
        check_references(spec.dependency.path, spec.config, declarations, registry)
        targets = [parse_environment_dependency(item) for item in spec.depends_on]
        targets += [reference.dependency for reference in collect_environment_references(spec.config)]
        for target in targets:
            if target not in declarations:
                raise EnvironmentReferenceError(
                    f"{spec.dependency.path} references unknown environment target {target.path!r}"
                )
        edges[spec.dependency] = tuple(dict.fromkeys(targets))
    for policy in policies:
        check_references(f"policies.{policy.policy_id}", policy.config, declarations, registry)

    return tuple(declarations[node] for node in stable_order(tuple(declarations), edges, "environment"))
