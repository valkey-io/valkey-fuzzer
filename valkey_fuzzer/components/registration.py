"""How a component registers: its config, the selector and trigger checks it gets, and spec-bundle pinning."""

import functools
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema
from valkey_fuzzer.spec.bundle import SpecBundleError, load_spec_mapping

from .instances import validate_instance_selector
from .predicates import validate_payload_filter

Check = Callable[[Mapping[str, Any], ComponentSpec, ExecutionPlan], None]


# Event-triggered components name their trigger in their own config; the
# framework has no scheduling language.  ``occurrence`` fires on the nth
# matching event, ``when_payload`` filters events by payload fields, and
# ``delay_s`` waits inside the trigger's dispatch before firing.
TRIGGER_FIELDS = {
    "when_event": ConfigField(str),
    "occurrence": ConfigField(int, default=1, minimum=1, maximum=1_000_000),
    "when_payload": ConfigField(dict, default=dict),
    "delay_s": ConfigField(float, default=0.0, minimum=0, maximum=600),
}


def component_registration(
    component_id: str,
    factory: Callable[[], Any],
    fields: Mapping[str, ConfigField],
    *,
    check: Optional[Check] = None,
    selectors: Optional[Mapping[str, Sequence[str]]] = None,
    deployment_modes: Optional[Iterable[str]] = None,
    required_evidence: Optional[Callable[[ComponentSpec], Any]] = None,
    references_evidence: Optional[Callable[[ComponentSpec], Any]] = None,
    triggered: bool = False,
    pins_spec: bool = False,
    declares: Declarations,
) -> ComponentRegistration:
    """The registration of one component whose config has ``fields``.

    Its role is the second segment of ``component_id`` (``valkey.<role>.<name>``).
    ``selectors`` names the fields that select instances, each with the
    values it also accepts (``instance: event``); they are checked when the
    scenario compiles, before ``check``, so no component repeats it.
    ``triggered`` components take the trigger fields too: they name their
    trigger in their own config (``when_event``), optionally the nth matching
    occurrence, a payload filter, which is checked before ``check`` runs, and a
    delay; the framework has no scheduling language. ``pins_spec`` components
    read a spec bundle's mapping.yaml (``spec``, ``spec_sha256``) and pin it
    when the scenario compiles (:class:`SpecSchema`). ``declares`` names the
    events, evidence, and state changes the runner holds the component to.
    """

    if selectors:
        check = functools.partial(_check_selectors, dict(selectors), check)
    if triggered:
        fields = {**TRIGGER_FIELDS, **fields}
        check = functools.partial(_check_trigger, check)
    return ComponentRegistration(
        component_id=component_id,
        role=ComponentRole(component_id.split(".")[1]),
        factory=factory,
        config=(SpecSchema if pins_spec else ConfigSchema)(fields),
        check=check,
        deployment_modes=None if deployment_modes is None else tuple(deployment_modes),
        required_evidence=required_evidence,
        references_evidence=references_evidence,
        declares=declares,
    )


def _check_selectors(
    selectors: Mapping[str, Sequence[str]],
    check: Optional[Check],
    config: Mapping[str, Any],
    spec: ComponentSpec,
    plan: ExecutionPlan,
) -> None:
    """Every selector field that is set names local instances (or one of its other values), then ``check``."""

    for name, accepted in selectors.items():
        if config[name] is not None and config[name] not in accepted:
            validate_instance_selector(config[name], name)
    if check is not None:
        check(config, spec, plan)


def _check_trigger(check: Optional[Check], config: Mapping[str, Any], spec: ComponentSpec, plan: ExecutionPlan) -> None:
    """A triggered component's ``when_payload`` filter, then its own ``check``."""

    validate_payload_filter(config["when_payload"])
    if check is not None:
        check(config, spec, plan)


def validate_command(command: Any, name: str = "command") -> None:
    if not command or not all(isinstance(item, (str, int, float)) and not isinstance(item, bool) for item in command):
        raise ConfigError(f"{name} must be a non-empty list of strings or numbers")


def spec_mapping(name: str) -> Any:
    """The ``mapping.yaml`` of spec bundle ``name``; a bundle that cannot be read is a config error."""

    try:
        return load_spec_mapping(name)
    except SpecBundleError as exc:
        raise ConfigError(f"spec {name!r}: {exc}") from exc


# The digest of the mapping.yaml that ``spec`` names, filled in when the scenario compiles (SpecSchema).
SPEC_SHA256_FIELD = ConfigField(str, allow_none=True, default=None)


class SpecSchema(ConfigSchema):
    """The config of a component that reads its ``spec`` bundle's mapping.yaml when it runs.

    Resolving fills in ``spec_sha256``, so the plan, and its digest, name
    the mapping it was compiled against: a pattern or event name the
    mapping changes then changes the plan, and replay sees it.
    """

    def resolve(self, config: Mapping[str, Any], location: str) -> dict[str, Any]:
        resolved = super().resolve(config, location)
        if resolved["spec"] and resolved["spec_sha256"] is None:
            resolved["spec_sha256"] = spec_mapping(resolved["spec"]).sha256
        return resolved


def pinned_spec_mapping(config: Mapping[str, Any]) -> Any:
    """The mapping ``config["spec"]`` names, which must be the one the plan was compiled against."""

    mapping = spec_mapping(config["spec"])
    pinned = config["spec_sha256"]
    if pinned is not None and mapping.sha256 != pinned:
        raise ConfigError(
            f"spec {config['spec']!r}: {mapping.path} has sha256 {mapping.sha256}, but the plan was compiled "
            f"against {pinned}; compile the scenario again, or restore that mapping"
        )
    return mapping
