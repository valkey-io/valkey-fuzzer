"""What the catalog registers: components, generator hooks, and deployment providers."""

import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, Optional, TypeVar

from .declarations import Declarations
from .fields import require_name, require_qualified_id, require_type, unique_tuple
from .plan import ComponentSpec, DeploymentProviderIdentity, ExecutionPlan, StartupOverlay
from .protocols import ComponentRole, DeploymentProvider
from .schema import ConfigError, ConfigSchema, plain_config

T = TypeVar("T")


@dataclass(frozen=True)
class ComponentRegistration(Generic[T]):
    """A component factory plus the schema of its config.

    Factories only build in-memory state; external work belongs in lifecycle
    methods. :meth:`resolve_config` checks a component's config, and the
    compiler records its result, defaults included, in the plan: ``config``
    checks the fields and fills in defaults (without it the config is taken
    as written), ``deployment_modes`` limits the deployment modes the
    component supports, and ``check(config, spec, plan)`` makes the
    cross-field checks that need the resolved config or the plan. ``check``
    must be pure: it runs at compile time, when reference fields may still
    hold unresolved environment references, and again on the executable plan.
    """

    component_id: str
    role: ComponentRole
    factory: Callable[[], T]
    config: Optional[ConfigSchema] = None
    check: Optional[Callable[[Mapping[str, Any], ComponentSpec, ExecutionPlan], None]] = None
    deployment_modes: Optional[tuple[str, ...]] = None
    # Evidence IDs a component's config marks as required; the compiler adds
    # them to the plan's required_evidence with that component as their source.
    required_evidence: Optional[Callable[[ComponentSpec], Sequence[str]]] = None
    # The ``(evidence_id, source)`` pairs a component's config names (source None: any component); each must be
    # declared by its source, or by some component, as required evidence must, so a typo is a compile error.
    references_evidence: Optional[Callable[[ComponentSpec], Sequence[tuple[str, Optional[str]]]]] = None
    # What it publishes, emits, and changes; required (``Declarations()`` for nothing).
    declares: Optional[Declarations] = None

    def __post_init__(self) -> None:
        require_qualified_id(self.component_id, "component_id")
        object.__setattr__(self, "role", ComponentRole(self.role))
        if self.declares is None:
            raise ValueError(
                f"component {self.component_id!r} must declare the events it publishes, the evidence it "
                "emits, and the state it changes (declares=Declarations(...); Declarations() for none)"
            )
        require_type(self.declares, Declarations, "declares")
        if self.config is not None:
            require_type(self.config, ConfigSchema, "component config")
        if self.deployment_modes is not None:
            object.__setattr__(self, "deployment_modes", unique_tuple(self.deployment_modes, "deployment_modes"))

    def resolve_config(self, spec: ComponentSpec, plan: ExecutionPlan) -> dict[str, Any]:
        """``spec``'s config as plain data, checked, with its defaults filled in.

        Raises :class:`~valkey_fuzzer.schema.ConfigError` naming the field, or
        whatever ``check`` raises.
        """

        location = f"components.{spec.id}.config"
        config = plain_config(spec.config) if self.config is None else self.config.resolve(spec.config, location)
        if self.deployment_modes is not None and plan.deployment_mode not in self.deployment_modes:
            raise ConfigError(
                f"{self.component_id} supports deployment modes {sorted(self.deployment_modes)}, "
                f"not {plan.deployment_mode!r}"
            )
        if self.check is not None:
            self.check(config, spec, plan)
        return config


@dataclass(frozen=True)
class GeneratorRegistration:
    """A generator hook, named ``<namespace>.generator.<name>``.

    ``hook(rng, scenario)`` draws part of a random scenario: an input hook
    returns the input's value, a scenario hook a partial scenario (see
    :mod:`valkey_fuzzer.generator`). Hooks run while a scenario is drawn,
    never in a plan.
    """

    generator_id: str
    hook: Callable[[random.Random, Mapping[str, Any]], Any]

    def __post_init__(self) -> None:
        require_qualified_id(self.generator_id, "generator_id")


@dataclass(frozen=True)
class DeploymentProviderRegistration:
    """A fresh-provider factory implementing one deployment mode.

    ``deployment_mode`` is the mode experiments list in ``deployment_modes``;
    ``provider_id`` names this implementation of it. Providers own mutable
    lifecycle state, so the runner builds a new one for every validation or
    execution attempt. ``normalizer``, when given, is a pure function from a
    scenario's deployment document to the same document with the provider's
    defaults filled in; it raises ``ValueError`` for a document the provider
    cannot run. The compiler records its result in the plan and checks the
    experiment's ``deployment_requirements`` against it. ``plan_instances``,
    when given, is a pure function from the normalized document and the
    plan's startup overlays to the instances the provider will start, each
    ``{role, shard, server_config}`` with its effective server config: the
    provider's own defaults under the document's config and the overlays.
    The compiler records it as the plan's ``instances``.

    The run service knows nothing of a provider's document; what it needs to
    know comes from optional hooks, and without one it does without:

    - ``binary_override(path)``: the deployment fields that make the
      deployment run the Valkey binary ``path`` (``--valkey-binary``).
    - ``relocate_ports(plan)``: the deployment fields that move a generated
      scenario's ports off ones taken on this host (empty when they are
      free), and a note saying what was done, or what could not be.
    - ``recorded_instances(outcome)``: the per-instance provenance (each
      with its ``instance_id``) the provider left in a run's outcome, as
      plain data.
    - ``binary_drift(plan, recorded, *, override, replayed=None)``: how the
      Valkey binaries a replay of ``plan`` runs differ from the ``recorded``
      instances (``override`` is a ``--valkey-binary``; ``replayed`` the
      replay's own instances once it ran), and what could not be compared.
    - ``reductions(deployment, requirements)``: single-step smaller versions
      of a scenario's deployment section, each with a label, most aggressive
      first, within the experiment's ``deployment_requirements``; the
      minimizer tries them.
    - ``host_fields``: the deployment fields that only place the deployment
      on a host (ports, folders), which a run's coverage leaves out.
    """

    deployment_mode: str
    provider_id: str
    factory: Callable[[], DeploymentProvider]
    implementation_digest: Optional[str] = None
    normalizer: Optional[Callable[[Mapping[str, Any]], Mapping[str, Any]]] = None
    plan_instances: Optional[Callable[[Mapping[str, Any], Sequence[StartupOverlay]], Mapping[str, Any]]] = None
    binary_override: Optional[Callable[[str], Mapping[str, Any]]] = None
    relocate_ports: Optional[Callable[[ExecutionPlan], tuple[Mapping[str, Any], Optional[str]]]] = None
    recorded_instances: Optional[Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]] = None
    binary_drift: Optional[Callable[..., tuple[list[dict[str, Any]], list[str]]]] = None
    reductions: Optional[Callable[[Mapping[str, Any], Mapping[str, Any]], Sequence[tuple[str, Mapping[str, Any]]]]] = (
        None
    )
    host_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_name(self.deployment_mode, "deployment_mode")
        self.identity  # noqa: B018 - validates the identity fields
        object.__setattr__(self, "host_fields", unique_tuple(self.host_fields, "host_fields"))

    @property
    def identity(self) -> DeploymentProviderIdentity:
        return DeploymentProviderIdentity(
            provider_id=self.provider_id, implementation_digest=self.implementation_digest
        )
