"""Experiments and execution plans: the models the compiler builds and the runner executes.

What an experiment tests lives entirely in registered components and
providers; nothing here knows an individual experiment, evidence ID, or policy
type.
"""

import hashlib
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from typing import Any, Optional

from ._data import canonical_json, freeze_mapping, to_plain_data
from .environment import EnvironmentSpec, PolicySpec
from .fields import (
    as_tuple,
    model_from_data,
    models_from_data,
    require_name,
    require_number,
    require_qualified_id,
    require_text,
    require_type,
    unique_tuple,
)
from .ordering import stable_order
from .protocols import ROLES, ComponentRole, Stage
from .references import EnvironmentKind, references_from_data
from .schema import ConfigSchema


class RunMode(str, Enum):
    """``experiment`` runs make coverage claims; ``smoke`` runs only exercise the deployment lifecycle."""

    EXPERIMENT = "experiment"
    SMOKE = "smoke"


@dataclass(frozen=True)
class ComponentSpec:
    """One configured use of a registered component in an experiment.

    ``use`` names the registration and ``id`` this use of it, unique within
    the experiment, as in experiment YAML. ``role`` comes from the
    registration: an experiment may leave it out, the compiler fills it in,
    and every component of an execution plan has one.
    """

    use: str
    id: str
    role: Optional[ComponentRole] = None
    config: Mapping[str, Any] = field(default_factory=dict)
    required: bool = True
    depends_on: tuple[str, ...] = ()
    timeout_s: Optional[float] = None

    def __post_init__(self) -> None:
        if self.role is not None:
            object.__setattr__(self, "role", ComponentRole(self.role))
        require_qualified_id(self.use, "use")
        require_name(self.id, "id")
        object.__setattr__(self, "config", freeze_mapping(self.config, "component config", references=True))
        depends_on = unique_tuple(self.depends_on, f"component {self.id!r} depends_on")
        if self.id in depends_on:
            raise ValueError(f"component {self.id!r} cannot depend on itself")
        object.__setattr__(self, "depends_on", depends_on)
        if self.timeout_s is not None:
            require_number(self.timeout_s, "timeout_s", positive=True)
        require_type(self.required, bool, "required")


# The selector words, as (roles, first): "primary" and "replica" pick one instance.
_SELECTOR_WORDS = {
    "all": ((), False),
    "primaries": (("primary",), False),
    "primary": (("primary",), True),
    "replicas": (("replica",), False),
    "replica": (("replica",), True),
}


@dataclass(frozen=True)
class InstanceSelector:
    """Instances of a deployment, by role, by instance ID, or both.

    Empty criteria select every instance. Values within a field are
    alternatives; non-empty fields combine with AND. ``first`` keeps only the
    first selected instance. :meth:`parse` reads the forms scenarios and
    component configs use, and :meth:`resolve` selects from a deployment.
    """

    roles: tuple[str, ...] = ()
    instance_ids: tuple[str, ...] = ()
    first: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "roles", unique_tuple(self.roles, "selector roles"))
        object.__setattr__(self, "instance_ids", unique_tuple(self.instance_ids, "selector instance_ids"))
        require_type(self.first, bool, "selector first")

    @classmethod
    def parse(cls, value: Any) -> "InstanceSelector":
        """``all``, ``primaries``, ``primary``, ``replicas``, ``replica``, an instance ID,
        a non-empty list of instance IDs, or ``{roles, instance_ids, first}``."""

        if isinstance(value, InstanceSelector):
            return value
        if isinstance(value, str):
            if value in _SELECTOR_WORDS:
                roles, first = _SELECTOR_WORDS[value]
                return cls(roles=roles, first=first)
            return cls(instance_ids=(value,))
        if isinstance(value, Mapping):
            return model_from_data(cls, value, "instance selector")
        if isinstance(value, (list, tuple)) and value and all(isinstance(item, str) for item in value):
            return cls(instance_ids=tuple(value))
        raise ValueError(
            f"an instance selector is one of {list(_SELECTOR_WORDS)}, an instance ID, a non-empty list of "
            f"instance IDs, or a mapping of roles and instance_ids; got {value!r}"
        )

    def resolve(self, instance_ids: Sequence[str], roles: Mapping[str, str]) -> tuple[str, ...]:
        """The selected instances among ``instance_ids``; ``roles`` maps an instance ID to its role.

        Instances come in ``instance_ids`` order, except that IDs selected by
        ID alone come in the order the selector lists them. An unknown
        instance ID is an error; a role no instance has selects nothing.
        """

        known = set(instance_ids)
        unknown = [item for item in self.instance_ids if item not in known]
        if unknown:
            raise ValueError(f"unknown instance IDs {unknown}")
        if self.instance_ids and not self.roles:
            selected = self.instance_ids
        else:
            selected = tuple(
                item
                for item in instance_ids
                if (not self.roles or roles.get(item) in self.roles)
                and (not self.instance_ids or item in self.instance_ids)
            )
        return selected[:1] if self.first else selected


class ConflictPolicy(str, Enum):
    """What a startup overlay does with a setting the deployment or an earlier overlay already set."""

    # Replace the earlier value.
    OVERRIDE = "override"
    # Reject a different value.
    ERROR = "error"


@dataclass(frozen=True)
class StartupOverlay:
    """Pre-start server configuration for selected instances."""

    overlay_id: str
    selector: InstanceSelector = field(default_factory=InstanceSelector)
    server_config: Mapping[str, Any] = field(default_factory=dict)
    conflict_policy: ConflictPolicy = ConflictPolicy.OVERRIDE

    def __post_init__(self) -> None:
        require_name(self.overlay_id, "overlay_id")
        require_type(self.selector, InstanceSelector, "selector")
        server_config = freeze_mapping(self.server_config, "startup overlay server_config", references=True)
        if not server_config:
            raise ValueError("a startup overlay must provide server_config")
        object.__setattr__(self, "server_config", server_config)
        object.__setattr__(self, "conflict_policy", ConflictPolicy(self.conflict_policy))


@dataclass(frozen=True)
class EvidenceRequirement:
    """Minimum trusted evidence an experiment verdict needs."""

    evidence_id: str
    source: Optional[str] = None
    min_count: int = 1

    def __post_init__(self) -> None:
        require_qualified_id(self.evidence_id, "evidence_id")
        if self.source is not None:
            require_name(self.source, "source")
        if require_number(self.min_count, "min_count", integer=True) < 1:
            raise ValueError("min_count must be an integer greater than or equal to one")


def _validate_composition(spec: Any) -> tuple[ComponentSpec, ...]:
    """Checks shared by an experiment and the plan compiled from it; returns its components in dependency order."""

    object.__setattr__(spec, "mode", RunMode(spec.mode))
    components = as_tuple(spec.components, "components")
    for component in components:
        require_type(component, ComponentSpec, "component")
    by_id = {component.id: component for component in components}
    if len(by_id) != len(components):
        raise ValueError("component IDs must be unique")
    for component in components:
        unknown = [dependency for dependency in component.depends_on if dependency not in by_id]
        if unknown:
            raise ValueError(f"component {component.id!r} has unknown dependencies: {unknown}")
    # Dependency order, keeping the declaration order wherever the dependencies allow it.
    order = stable_order(tuple(by_id), {component.id: component.depends_on for component in components}, "component")
    ordered = tuple(by_id[component_id] for component_id in order)
    object.__setattr__(spec, "components", components)

    overlays = as_tuple(spec.startup_overlays, "startup_overlays")
    for overlay in overlays:
        require_type(overlay, StartupOverlay, "startup overlay")
    unique_tuple([overlay.overlay_id for overlay in overlays], "startup overlay_id")
    object.__setattr__(spec, "startup_overlays", overlays)
    object.__setattr__(spec, "required_events", unique_tuple(spec.required_events, "required_events"))

    requirements = as_tuple(spec.required_evidence, "required_evidence")
    keys = set()
    for requirement in requirements:
        require_type(requirement, EvidenceRequirement, "evidence requirement")
        key = (requirement.evidence_id, requirement.source)
        if key in keys:
            raise ValueError(
                f"duplicate evidence requirement for {requirement.evidence_id!r} from {requirement.source!r}"
            )
        keys.add(key)
        if requirement.source is not None and requirement.source not in by_id:
            raise ValueError(
                f"evidence requirement {requirement.evidence_id!r} references unknown source component "
                f"{requirement.source!r}"
            )
    object.__setattr__(spec, "required_evidence", requirements)
    object.__setattr__(spec, "metadata", freeze_mapping(spec.metadata, "metadata"))
    return ordered


def _check_stages(components: Sequence[ComponentSpec]) -> None:
    """Reject a component that depends on one the runner calls at a later stage, which it could never wait for."""

    roles = {component.id: component.role for component in components}
    for component in components:
        stage = ROLES[component.role].stage
        for dependency_id in component.depends_on:
            if ROLES[roles[dependency_id]].stage <= stage:
                continue
            if stage is Stage.PROBE:
                raise ValueError(
                    f"capability {component.id!r} cannot depend on post-provision component {dependency_id!r}"
                )
            raise ValueError(f"component {component.id!r} cannot depend on post-execution oracle {dependency_id!r}")


@dataclass(frozen=True)
class ExperimentSpec:
    """Declarative description of an experiment.

    ``deployment_modes`` are the deployment modes it supports, and ``inputs``
    is the schema of the inputs a scenario passes it.
    ``deployment_requirements`` maps deployment fields to rules (``type``,
    ``enum``, ``minimum``, ``maximum``, as in ``inputs``) that the deployment
    document, with its provider's defaults filled in, must satisfy; for
    example ``{"replicas_per_primary": {"minimum": 1}}``. In ``experiment`` mode
    the compiled plan must have a required oracle and require some evidence:
    listed in ``required_evidence`` or derived from components whose
    registration declares the evidence their configuration marks as required.
    """

    experiment_id: str
    deployment_modes: tuple[str, ...]
    components: tuple[ComponentSpec, ...]
    mode: RunMode = RunMode.EXPERIMENT
    inputs: Mapping[str, Any] = field(default_factory=dict)
    deployment_requirements: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    startup_overlays: tuple[StartupOverlay, ...] = ()
    required_events: tuple[str, ...] = ()
    required_evidence: tuple[EvidenceRequirement, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_qualified_id(self.experiment_id, "experiment_id")
        modes = unique_tuple(self.deployment_modes, "deployment_modes")
        if not modes:
            raise ValueError("deployment_modes must not be empty")
        object.__setattr__(self, "deployment_modes", modes)
        requirements = freeze_mapping(self.deployment_requirements, "deployment_requirements")
        for name in requirements:
            if name in ("mode", "modes", "provider"):
                raise ValueError(f"deployment requirement {name!r} must name a deployment field")
        object.__setattr__(self, "deployment_requirements", requirements)
        object.__setattr__(self, "inputs", freeze_mapping(self.inputs, "inputs"))
        # Not fields: built once here, so a malformed schema is an error before any scenario uses it.
        object.__setattr__(self, "_input_schema", ConfigSchema.from_document(self.inputs, "inputs"))
        object.__setattr__(self, "_deployment_schema", ConfigSchema.from_rules(requirements, "deployment"))
        _validate_composition(self)

    @property
    def input_schema(self) -> ConfigSchema:
        """The schema ``inputs`` declares, which a scenario's inputs must meet."""

        return self._input_schema

    @property
    def deployment_schema(self) -> ConfigSchema:
        """The schema ``deployment_requirements`` declares, which the normalized deployment must meet."""

        return self._deployment_schema


@dataclass(frozen=True)
class DeploymentProviderIdentity:
    """Replay lock for one deployment-provider implementation.

    ``implementation_digest``, when the provider declares one, locks the plan
    to that build of it; the runner refuses a plan whose identity differs.
    """

    provider_id: str
    implementation_digest: Optional[str] = None

    def __post_init__(self) -> None:
        require_qualified_id(self.provider_id, "provider_id")
        if self.implementation_digest is not None:
            require_text(self.implementation_digest, "implementation_digest")


def derive_component_seed(seed: int, component_id: str, stream: str = "default") -> int:
    """A stable seed that does not depend on Python's process-randomized hash."""

    require_number(seed, "seed", integer=True)
    require_name(component_id, "component_id")
    require_name(stream, "stream")
    material = f"{seed}\0{component_id}\0{stream}".encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:16], "big")


@dataclass(frozen=True)
class ExecutionPlan:
    """A fully compiled, replayable run.

    The compiled plan keeps typed environment references; the runner executes
    a copy in which every reference is replaced by the acquired output. Only
    the compiled plan is ever reported or persisted.

    ``instances`` maps each instance the deployment provider plans to its
    ``role``, ``shard``, and effective ``server_config``: the provider's
    defaults under the deployment's own config and the startup overlays.
    Compile-time checks read it rather than the deployment document, so a
    default counts the same as a setting written out; the runner refuses a
    plan whose provider no longer plans it that way.
    """

    scenario_id: str
    experiment_id: str
    seed: int
    deployment_mode: str
    components: tuple[ComponentSpec, ...]
    deployment_provider: DeploymentProviderIdentity
    mode: RunMode = RunMode.EXPERIMENT
    inputs: Mapping[str, Any] = field(default_factory=dict)
    artifacts: tuple[EnvironmentSpec, ...] = ()
    resources: tuple[EnvironmentSpec, ...] = ()
    policies: tuple[PolicySpec, ...] = ()
    startup_overlays: tuple[StartupOverlay, ...] = ()
    required_events: tuple[str, ...] = ()
    required_evidence: tuple[EvidenceRequirement, ...] = ()
    deployment: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    instances: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_name(self.scenario_id, "scenario_id")
        require_qualified_id(self.experiment_id, "experiment_id")
        require_number(self.seed, "seed", integer=True)
        require_name(self.deployment_mode, "deployment_mode")
        require_type(self.deployment_provider, DeploymentProviderIdentity, "deployment_provider")
        ordered = _validate_composition(self)
        unassigned = [component.id for component in self.components if component.role is None]
        if unassigned:
            raise ValueError(f"execution plan components need a role: {unassigned}")
        _check_stages(self.components)
        # Not a field: never serialized or compared, and rebuilt whenever the plan is.
        object.__setattr__(self, "_ordered_components", ordered)
        if self.mode is RunMode.EXPERIMENT and not any(
            component.role is ComponentRole.ORACLE and component.required for component in self.components
        ):
            raise ValueError("experiment mode requires at least one required oracle")
        if self.mode is RunMode.EXPERIMENT and not self.required_evidence:
            raise ValueError("experiment mode requires at least one required_evidence requirement")
        object.__setattr__(self, "inputs", freeze_mapping(self.inputs, "inputs", references=True))
        for name, spec_type, id_field in (
            ("artifacts", EnvironmentSpec, "id"),
            ("resources", EnvironmentSpec, "id"),
            ("policies", PolicySpec, "policy_id"),
        ):
            specs = as_tuple(getattr(self, name), name)
            for spec in specs:
                require_type(spec, spec_type, f"{name} entry")
                if spec_type is EnvironmentSpec and spec.kind is not EnvironmentKind(name):
                    raise ValueError(f"{name} entry {spec.id!r} is a {spec.kind.value} declaration")
            unique_tuple([getattr(spec, id_field) for spec in specs], f"{name} {id_field}")
            object.__setattr__(self, name, specs)
        if not isinstance(self.deployment, Mapping):
            raise TypeError("deployment must be a mapping")
        document = freeze_mapping(self.deployment or {"mode": self.deployment_mode}, "deployment", references=True)
        if document.get("mode") != self.deployment_mode:
            raise ValueError("deployment.mode must match deployment_mode")
        object.__setattr__(self, "deployment", document)
        instances = freeze_mapping(self.instances, "instances", references=True)
        for instance_id, instance in instances.items():
            require_name(instance_id, "instance ID")
            if not isinstance(instance, Mapping) or not isinstance(instance.get("server_config"), Mapping):
                raise ValueError(f"instances.{instance_id} must be a mapping with a server_config mapping")
        object.__setattr__(self, "instances", instances)

    @classmethod
    def from_experiment(
        cls,
        spec: ExperimentSpec,
        *,
        scenario_id: str,
        seed: int,
        deployment_mode: str,
        deployment_provider: DeploymentProviderIdentity,
        inputs: Optional[Mapping[str, Any]] = None,
        deployment: Optional[Mapping[str, Any]] = None,
        artifacts: Sequence[EnvironmentSpec] = (),
        resources: Sequence[EnvironmentSpec] = (),
        policies: Sequence[PolicySpec] = (),
    ) -> "ExecutionPlan":
        require_type(spec, ExperimentSpec, "spec")
        return cls(
            scenario_id=scenario_id,
            experiment_id=spec.experiment_id,
            seed=seed,
            deployment_mode=deployment_mode,
            components=spec.components,
            deployment_provider=deployment_provider,
            mode=spec.mode,
            inputs={} if inputs is None else inputs,
            artifacts=tuple(artifacts),
            resources=tuple(resources),
            policies=tuple(policies),
            startup_overlays=spec.startup_overlays,
            required_events=spec.required_events,
            required_evidence=spec.required_evidence,
            deployment={"mode": deployment_mode} if deployment is None else deployment,
            metadata=spec.metadata,
        )

    @property
    def ordered_components(self) -> tuple[ComponentSpec, ...]:
        """The components in the order the runner starts them: after their dependencies, else as declared."""

        return self._ordered_components

    def dependencies(self, component_id: str) -> set[str]:
        """Every component ``component_id`` depends on, directly or through others."""

        by_id = {component.id: component for component in self.components}
        found: set[str] = set()
        pending = list(by_id[component_id].depends_on)
        while pending:
            current = pending.pop()
            if current not in found:
                found.add(current)
                pending.extend(by_id[current].depends_on)
        return found

    def rng(self, component_id: str, stream: str = "default") -> random.Random:
        """A new generator at the start of one component stream.

        Components should draw from ``RunContext.rng``, which keeps one
        continuing generator per stream for the whole run.
        """

        if component_id not in {component.id for component in self.components}:
            raise ValueError(f"unknown component id: {component_id!r}")
        return random.Random(derive_component_seed(self.seed, component_id, stream))

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "ExecutionPlan":
        """Rebuild a validated plan from its canonical data form."""

        return model_from_data(
            cls,
            references_from_data(value),
            "execution plan",
            components=partial(models_from_data, ComponentSpec, location="components"),
            deployment_provider=partial(model_from_data, DeploymentProviderIdentity, location="deployment_provider"),
            artifacts=partial(models_from_data, EnvironmentSpec, location="artifacts"),
            resources=partial(models_from_data, EnvironmentSpec, location="resources"),
            policies=partial(models_from_data, PolicySpec, location="policies"),
            startup_overlays=partial(
                models_from_data, StartupOverlay, location="startup_overlays", selector=InstanceSelector.parse
            ),
            required_evidence=partial(models_from_data, EvidenceRequirement, location="required_evidence"),
        )
