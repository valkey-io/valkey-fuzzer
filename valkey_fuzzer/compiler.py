"""The scenario compiler: one strict scenario plus an installed experiment become an execution plan.

The plan is the immutable :class:`~valkey_fuzzer.plan.ExecutionPlan` the
runner executes. Each component's role comes from its registration. The
deployment document is normalized by its provider's registration (which fills
in the provider's defaults) and must meet the experiment's deployment
requirements. The experiment's inputs, every component's config, and every
artifact and resource config are resolved through their
:class:`~valkey_fuzzer.schema.ConfigSchema`, and the plan records them with
their defaults, so a report shows exactly what ran.
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, Optional, Union

from ._data import to_plain_data
from .catalog import Catalog, ComponentRegistry
from .documents import ABSENT, get_path, merge
from .environment import (
    EnvironmentProviderRegistry,
    EnvironmentSpec,
    PolicySpec,
    check_references,
    stable_environment_order,
)
from .evidence import check_requirements
from .fields import carry_path, document_text, errors_at, located, model_from_data, strict_fields
from .plan import ComponentSpec, EvidenceRequirement, ExecutionPlan, ExperimentSpec
from .protocols import ComponentRole
from .references import (
    REFERENCE_DATA_KEY,
    EnvironmentKind,
    EnvironmentReference,
    EnvironmentReferenceError,
    parse_environment_reference,
)
from .registrations import DeploymentProviderRegistration
from .schema import (
    ENVIRONMENT_NAMESPACES,
    INPUT_SEGMENT,
    ConfigError,
    ExperimentDocumentError,
    ExperimentError,
    ScenarioCompileError,
    document_mapping,
)

_INPUT_REF = re.compile(rf"^inputs\.({INPUT_SEGMENT}(?:\.{INPUT_SEGMENT})*)$")
_LITERAL_MARKER_KEY = "$valkey_fuzzer_literal"


class UnresolvedReferenceError(ScenarioCompileError):
    """A ``$ref`` could not be resolved exactly."""


def _unwrap_literal(value: Mapping[str, Any], location: str) -> Any:
    if set(value) != {_LITERAL_MARKER_KEY}:
        raise located(
            UnresolvedReferenceError(f"{location} literal marker must contain only the {_LITERAL_MARKER_KEY!r} key"),
            location,
        )
    return value[_LITERAL_MARKER_KEY]


def _environment_reference(value: Any, location: str, hint: str = "") -> EnvironmentReference:
    try:
        return parse_environment_reference(value)
    except EnvironmentReferenceError as exc:
        raise located(UnresolvedReferenceError(f"{location}: {exc}{hint}"), location) from exc


def _prepare_inputs(value: Any, location: str = "experiment.inputs") -> Any:
    """Parse ``{$ref: artifacts|resources...}`` bindings in authored inputs.

    Inputs may reference nothing else: a ``$ref`` to anything else is a typo,
    or data that belongs in the literal marker.
    """

    if isinstance(value, Mapping):
        if _LITERAL_MARKER_KEY in value:
            return _unwrap_literal(value, location)
        if REFERENCE_DATA_KEY in value:
            raise located(
                ScenarioCompileError(f"{location} uses the reserved {REFERENCE_DATA_KEY!r} serialization marker"),
                location,
            )
        if "$ref" in value:
            reference = value["$ref"]
            if not isinstance(reference, str) or reference.split(".", 1)[0] not in ENVIRONMENT_NAMESPACES:
                raise located(
                    UnresolvedReferenceError(
                        f"{location} uses $ref {reference!r}; scenario inputs may reference only artifacts or "
                        f"resources (wrap data that holds $ref in {{{_LITERAL_MARKER_KEY}: ...}})"
                    ),
                    location,
                )
            if len(value) != 1:
                raise located(
                    ScenarioCompileError(f"{location} uses an environment $ref with sibling fields"), location
                )
            return _environment_reference(reference, location)
        return {key: _prepare_inputs(item, f"{location}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_prepare_inputs(item, f"{location}[{index}]") for index, item in enumerate(value)]
    return value


def _lookup_input(reference: str, inputs: Mapping[str, Any], location: str) -> Any:
    value = get_path(inputs, _INPUT_REF.fullmatch(reference).group(1))
    if value is ABSENT:
        raise located(UnresolvedReferenceError(f"{location} references missing input path {reference!r}"), location)
    return value


def _resolve_template(value: Any, inputs: Mapping[str, Any], location: str) -> Any:
    """Replace ``{$ref: inputs.<path>}`` with the input and parse environment references."""

    if isinstance(value, EnvironmentReference):
        return value
    if isinstance(value, Mapping):
        if _LITERAL_MARKER_KEY in value:
            return _unwrap_literal(value, location)
        if "$ref" in value:
            if len(value) != 1:
                raise located(
                    UnresolvedReferenceError(f"{location} uses $ref with sibling fields; references must be exact"),
                    location,
                )
            reference = value["$ref"]
            if isinstance(reference, str) and _INPUT_REF.fullmatch(reference):
                return _lookup_input(reference, inputs, location)
            return _environment_reference(
                reference, f"{location}.$ref", "; a $ref must name inputs, artifacts, or resources"
            )
        return {key: _resolve_template(item, inputs, f"{location}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_resolve_template(item, inputs, f"{location}[{index}]") for index, item in enumerate(value)]
    return value


def _component_role(component: ComponentSpec, components: ComponentRegistry) -> ComponentRole:
    """The role ``component``'s registration has; an explicit role must agree with it."""

    registration = components.get(component.use)
    if registration is None:
        raise located(
            ScenarioCompileError(f"component {component.id!r} uses {component.use!r}, which is not registered"),
            f"components.{component.id}.use",
        )
    actual = registration.role
    if component.role is not None and component.role is not actual:
        raise located(
            ScenarioCompileError(
                f"component {component.use!r} is registered as {actual.value!r}, "
                f"but the experiment declares {component.role.value!r}"
            ),
            f"components.{component.id}.role",
        )
    return actual


def _resolve_experiment(spec: ExperimentSpec, inputs: Mapping[str, Any], registry: ComponentRegistry) -> ExperimentSpec:
    """``spec`` with its inputs substituted, its components' roles, and the evidence they require."""

    components = []
    for component in spec.components:
        location = f"components.{component.id}"
        role = _component_role(component, registry)
        config = _resolve_template(component.config, inputs, f"{location}.config")
        with errors_at(location, ExperimentDocumentError):
            components.append(replace(component, role=role, config=config))
    overlays = []
    for overlay in spec.startup_overlays:
        location = f"startup_overlays.{overlay.overlay_id}"
        server_config = _resolve_template(overlay.server_config, inputs, f"{location}.server_config")
        with errors_at(location, ExperimentDocumentError):
            overlays.append(replace(overlay, server_config=server_config))
    metadata = _resolve_template(spec.metadata, inputs, "metadata")
    with errors_at("experiment", ExperimentDocumentError):
        resolved = replace(spec, components=tuple(components), startup_overlays=tuple(overlays), metadata=metadata)
    return replace(resolved, required_evidence=derive_required_evidence(resolved, registry))


def _parse_environment(
    value: Any, section: str, model: type, fields: tuple[str, str], extra: Sequence[str], **fixed: Any
) -> tuple[Any, ...]:
    id_field, provider_field = fields
    result = []
    for declaration_id, declaration in document_mapping(value, section).items():
        location = f"{section}.{declaration_id}"
        mapping = dict(
            strict_fields(
                declaration, location, allowed=("use", *extra), required=("use",), error=ExperimentDocumentError
            )
        )
        mapping[provider_field] = mapping.pop("use")
        if "with" in mapping:
            mapping["config"] = mapping.pop("with")
        result.append(
            model_from_data(
                model, {**fixed, id_field: declaration_id, **mapping}, location, error=ExperimentDocumentError
            )
        )
    return tuple(result)


def _resolve_inputs(spec: ExperimentSpec, supplied: Mapping[str, Any]) -> dict[str, Any]:
    """The scenario's inputs checked against the experiment's ``inputs:`` schema, with its defaults."""

    try:
        return spec.input_schema.resolve(supplied, "experiment.inputs")
    except ConfigError as exc:
        raise carry_path(ScenarioCompileError(str(exc)), exc) from None


def _check_deployment_requirements(spec: ExperimentSpec, deployment: Mapping[str, Any]) -> None:
    """Fail unless ``deployment`` (with its provider's defaults) meets the experiment's field rules."""

    try:
        spec.deployment_schema.resolve(deployment, "deployment")
    except ConfigError as exc:
        raise carry_path(
            ScenarioCompileError(f"experiment {spec.experiment_id!r} does not support this deployment: {exc}"), exc
        ) from None


def _resolve_declarations(
    specs: Sequence[EnvironmentSpec], registry: EnvironmentProviderRegistry
) -> tuple[EnvironmentSpec, ...]:
    """Each declaration with its config resolved by its provider; an unregistered provider is reported later."""

    resolved = []
    for spec in specs:
        registration = registry.get((spec.kind.value, spec.provider_id))
        if registration is not None:
            try:
                spec = replace(spec, config=registration.resolve_config(spec))
            except (TypeError, ValueError) as exc:
                error = ScenarioCompileError(f"provider {spec.provider_id!r} rejected {spec.dependency.path}: {exc}")
                raise located(carry_path(error, exc), f"{spec.dependency.path}.with") from exc
        resolved.append(spec)
    return tuple(resolved)


def _normalized_deployment(
    registration: DeploymentProviderRegistration, document: Mapping[str, Any], deployment_mode: str
) -> Mapping[str, Any]:
    """The deployment document with the provider's defaults filled in by its normalizer, if it has one."""

    if registration.normalizer is None:
        return document
    try:
        normalized = registration.normalizer(document)
    except (TypeError, ValueError) as exc:
        raise located(carry_path(ScenarioCompileError(f"deployment: {exc}"), exc), "deployment") from exc
    if not isinstance(normalized, Mapping) or normalized.get("mode") != deployment_mode:
        raise ScenarioCompileError(
            f"deployment provider {registration.provider_id!r} normalized the deployment into an invalid document"
        )
    return dict(normalized)


# ---------------------------------------------------------------------------
# Validation shared with the runner
# ---------------------------------------------------------------------------


def derive_required_evidence(
    spec: Union[ExperimentSpec, ExecutionPlan], components: ComponentRegistry
) -> tuple[EvidenceRequirement, ...]:
    """``spec``'s required evidence plus what each component's config, defaults included, marks as required."""

    requirements = list(spec.required_evidence)
    known = {(item.evidence_id, item.source) for item in requirements}
    for component in spec.components:
        registration = components.get(component.use)
        if registration is None or registration.required_evidence is None:
            continue
        location = f"components.{component.id}.config"
        try:
            if registration.config is not None:
                component = replace(component, config=registration.config.resolve(component.config, location))
            evidence_ids = tuple(registration.required_evidence(component))
        except Exception as exc:
            rejected = ScenarioCompileError(f"component {component.use!r} rejected {component.id!r}: {exc}")
            raise located(carry_path(rejected, exc), location) from exc
        for evidence_id in evidence_ids:
            if (evidence_id, component.id) not in known:
                known.add((evidence_id, component.id))
                with errors_at(f"components.{component.id}", ScenarioCompileError):
                    requirements.append(EvidenceRequirement(evidence_id, source=component.id))
    return tuple(requirements)


def resolve_components(plan: ExecutionPlan, components: ComponentRegistry) -> ExecutionPlan:
    """``plan`` with every component's config resolved by its registration, defaults included.

    Every component must be registered for its role and accepted by
    :meth:`~valkey_fuzzer.registrations.ComponentRegistration.resolve_config`.
    """

    resolved = []
    for component in plan.components:
        registration = components.get(component.use)
        if registration is None:
            raise located(
                ScenarioCompileError(f"component {component.use!r} is not registered"), f"components.{component.id}.use"
            )
        if registration.role is not component.role:
            raise located(
                ScenarioCompileError(
                    f"component {component.use!r} is registered as {registration.role.value!r}, "
                    f"but the plan declares {component.role.value!r}"
                ),
                f"components.{component.id}.role",
            )
        try:
            config = registration.resolve_config(component, plan)
        except Exception as exc:
            rejected = ScenarioCompileError(f"component {component.use!r} rejected {component.id!r}: {exc}")
            raise located(carry_path(rejected, exc), f"components.{component.id}.config") from exc
        resolved.append(replace(component, config=config))
    # A compiled plan already holds its resolved configs and is returned as is.
    return plan if tuple(resolved) == plan.components else replace(plan, components=tuple(resolved))


def with_planned_instances(plan: ExecutionPlan, registration: DeploymentProviderRegistration) -> ExecutionPlan:
    """``plan`` with the instances its provider plans; an error if it records others.

    A plan that records no instances (one built without the compiler) gets
    them; one that records different instances was planned by another
    version of the provider, and is refused.
    """

    if registration.plan_instances is None:
        return plan
    try:
        planned = to_plain_data(registration.plan_instances(plan.deployment, plan.startup_overlays))
    except (TypeError, ValueError) as exc:
        raise located(ScenarioCompileError(f"deployment: {exc}"), "deployment") from exc
    if plan.instances and to_plain_data(plan.instances) != planned:
        raise ScenarioCompileError(
            f"the plan records instances that deployment provider {registration.provider_id!r} "
            "no longer plans for its deployment (its defaults or planning changed); recompile the scenario"
        )
    return replace(plan, instances=planned)


def validate_plan_environment(plan: ExecutionPlan, catalog: Catalog) -> None:
    """Validate the plan's declarations and every reference that consumes them."""

    registry = catalog.environment_providers
    ordered = stable_environment_order(plan.artifacts, plan.resources, registry=registry, policies=plan.policies)
    declarations = {spec.dependency: spec for spec in ordered}
    consumers = [("experiment.inputs", plan.inputs), ("deployment", plan.deployment)]
    consumers += [(f"components.{item.id}.config", item.config) for item in plan.components]
    consumers += [
        (f"startup_overlays.{item.overlay_id}.server_config", item.server_config) for item in plan.startup_overlays
    ]
    for source, value in consumers:
        check_references(source, value, declarations, registry)


def deployment_registration(plan: ExecutionPlan, catalog: Catalog) -> DeploymentProviderRegistration:
    """The registration of the deployment provider ``plan`` is locked to; an error if it is gone or changed."""

    registration = catalog.deployment_providers.of_plan(plan)
    if registration is None:
        raise ScenarioCompileError(
            f"deployment provider {plan.deployment_provider.provider_id!r} is not registered for "
            f"{plan.deployment_mode!r}"
        )
    if plan.deployment_provider != registration.identity:
        raise ScenarioCompileError(
            f"deployment provider identity for {plan.deployment_mode!r} does not match the replay-locked plan; "
            "recompile the scenario with this provider"
        )
    return registration


def check_plan(plan: ExecutionPlan, catalog: Catalog) -> ExecutionPlan:
    """``plan`` checked against ``catalog``, with resolved component configs and the evidence those configs mark
    as required.

    The compiler runs this on every plan it makes and the runner on every plan
    it is given, so a plan built or edited outside the compiler meets the same
    rules: it is locked to a registered provider, every component accepts its
    config, every required event and evidence can come from the plan, and
    every environment reference names a declared output. Callers first give
    the plan the instances its provider plans (:func:`with_planned_instances`);
    the runner does so only once the deployment holds no environment
    references. Errors are :class:`~valkey_fuzzer.schema.ScenarioCompileError`,
    except environment reference errors.
    """

    deployment_registration(plan, catalog)
    plan = resolve_components(plan, catalog.components)
    required = derive_required_evidence(plan, catalog.components)
    if required != plan.required_evidence:
        plan = replace(plan, required_evidence=required)
    try:
        check_requirements(plan, catalog.components)
    except ValueError as exc:
        raise located(ScenarioCompileError(str(exc)), "experiment") from exc
    validate_plan_environment(plan, catalog)
    return plan


def scenario_provider(
    catalog: Catalog, deployment_mode: str, provider: Optional[str]
) -> DeploymentProviderRegistration:
    """The provider a scenario's ``deployment.provider`` names, or the only one registered for ``deployment_mode``."""

    registry = catalog.deployment_providers
    registered = registry.provider_ids(deployment_mode)
    if provider is None:
        if not registered:
            raise located(
                ScenarioCompileError(f"deployment mode {deployment_mode!r} has no registered deployment provider"),
                "deployment.mode",
            )
        if len(registered) > 1:
            raise located(
                ScenarioCompileError(
                    f"deployment mode {deployment_mode!r} has multiple registered providers {list(registered)!r}; "
                    "select one with deployment.provider"
                ),
                "deployment.provider",
            )
        provider = registered[0]
    elif provider not in registered:
        raise located(
            ScenarioCompileError(
                f"deployment provider {provider!r} is not registered for deployment mode {deployment_mode!r}; "
                f"registered providers: {list(registered)!r}"
            ),
            "deployment.provider",
        )
    return registry.resolve((deployment_mode, provider))


def binary_fields(registration: DeploymentProviderRegistration, path: str) -> dict[str, Any]:
    """The deployment fields that make ``registration``'s deployment run the Valkey binary ``path``."""

    if registration.binary_override is None:
        raise located(
            ScenarioCompileError(f"deployment provider {registration.provider_id!r} cannot run another Valkey binary"),
            "deployment",
        )
    return dict(registration.binary_override(path))


def with_valkey_binary(
    catalog: Catalog, scenario: Mapping[str, Any], path: Optional[str], *, override: bool = True
) -> dict[str, Any]:
    """A copy of ``scenario`` whose deployment runs the Valkey binary ``path``, when there is one.

    The deployment's provider says which fields that sets (its
    ``binary_override`` hook); one without the hook cannot run another
    binary. With ``override`` (``--valkey-binary``) they replace the
    scenario's own; without it (a batch's binary for scenarios that name
    none) a scenario that sets them, such as a zoo run on its build, keeps them.
    """

    if not path:
        return merge(scenario, {})
    deployment = document_mapping(scenario.get("deployment", {}), "deployment")
    mode = document_text(deployment.get("mode"), "deployment.mode", ExperimentDocumentError)
    fields = binary_fields(scenario_provider(catalog, mode, deployment.get("provider")), path)
    if not override:
        fields = {name: value for name, value in fields.items() if name not in deployment}
    return merge(scenario, {"deployment": fields})


class ScenarioCompiler:
    """Compile strict scenarios against the experiments, components, and providers of a catalog."""

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def compile(self, value: Mapping[str, Any]) -> ExecutionPlan:
        envelope = strict_fields(
            value,
            "scenario",
            allowed=("scenario_id", "seed", "experiment", "deployment", "artifacts", "resources", "policies"),
            required=("scenario_id", "seed", "experiment", "deployment"),
            error=ExperimentDocumentError,
        )
        seed = envelope["seed"]
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise located(ScenarioCompileError("seed must be an integer"), "seed")
        experiment = strict_fields(
            envelope["experiment"],
            "experiment",
            allowed=("id", "inputs"),
            required=("id",),
            error=ExperimentDocumentError,
        )
        experiment_id = document_text(experiment["id"], "experiment.id", ExperimentDocumentError)
        supplied = _prepare_inputs(document_mapping(experiment.get("inputs", {}), "experiment.inputs"))

        components = self.catalog.components
        try:
            spec = self.catalog.experiments.resolve(experiment_id)
        except ExperimentError as exc:
            located(exc, "experiment.id")
            raise
        inputs = _resolve_inputs(spec, supplied)
        resolved_spec = _resolve_experiment(spec, inputs, components)
        document = document_mapping(
            _resolve_template(document_mapping(envelope["deployment"], "deployment"), inputs, "deployment"),
            "deployment",
        )
        if "mode" not in document:
            raise located(ExperimentDocumentError("deployment is missing required field 'mode'"), "deployment.mode")
        deployment_mode = document_text(document["mode"], "deployment.mode", ExperimentDocumentError)
        if deployment_mode not in spec.deployment_modes:
            raise located(
                ScenarioCompileError(
                    f"deployment mode {deployment_mode!r} is not supported by experiment {experiment_id!r}"
                ),
                "deployment.mode",
            )
        # ``provider`` selects the implementation of the deployment mode. The plan
        # records it as the provider identity, so providers never see the field.
        provider = (
            None
            if document.get("provider") is None
            else document_text(document["provider"], "deployment.provider", ExperimentDocumentError)
        )
        document = {name: item for name, item in document.items() if name != "provider"}
        registration = scenario_provider(self.catalog, deployment_mode, provider)
        document = _normalized_deployment(registration, document, deployment_mode)
        _check_deployment_requirements(spec, document)

        def section(
            name: str, model: type, fields: tuple[str, str], extra: Sequence[str], **fixed: Any
        ) -> tuple[Any, ...]:
            return _parse_environment(
                _resolve_template(envelope.get(name, {}), inputs, name), name, model, fields, extra, **fixed
            )

        def declarations(kind: EnvironmentKind) -> tuple[EnvironmentSpec, ...]:
            specs = section(kind.value, EnvironmentSpec, ("id", "provider_id"), ("with", "depends_on"), kind=kind)
            return _resolve_declarations(specs, self.catalog.environment_providers)

        try:
            plan = ExecutionPlan.from_experiment(
                resolved_spec,
                scenario_id=envelope["scenario_id"],
                seed=seed,
                deployment_mode=deployment_mode,
                deployment_provider=registration.identity,
                inputs=inputs,
                deployment=document,
                artifacts=declarations(EnvironmentKind.ARTIFACT),
                resources=declarations(EnvironmentKind.RESOURCE),
                policies=section("policies", PolicySpec, ("policy_id", "policy_type"), ("with",)),
            )
        except ExperimentError:
            raise
        except (TypeError, ValueError) as exc:
            raise ScenarioCompileError(str(exc)) from exc
        return check_plan(with_planned_instances(plan, registration), self.catalog)
