"""What a component declares it publishes, emits, and changes, by names that may hold ``{id}`` and
``{config.<path>}`` placeholders; the evidence contract holds it to them.
"""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from .events import LIFECYCLE_EVENTS, TEARDOWN_EVENTS
from .fields import require_name, require_qualified_id, require_type, unique_tuple
from .plan import ComponentSpec
from .schema import ConfigField, ConfigSchema, plain_config

# A placeholder in a declared event name or evidence ID: ``{id}`` is the
# component use's ``id``; ``{config.<path>}`` is every string the path names
# in its resolved config, where ``<segment>[]`` steps into each list item.
_PLACEHOLDER = re.compile(r"\{(id|config(?:\.[A-Za-z0-9_-]+(?:\[\])?)+)\}")


def _expand(template: str, spec: ComponentSpec) -> tuple[str, ...]:
    """The names ``template`` declares for ``spec``; none when a config path names nothing."""

    names = [template]
    for match in _PLACEHOLDER.finditer(template):
        token = match.group(0)
        if match.group(1) == "id":
            values = [spec.id]
        else:
            values = [plain_config(spec.config)]
            for segment in match.group(1).split(".")[1:]:
                key = segment[:-2] if segment.endswith("[]") else segment
                values = [value[key] for value in values if isinstance(value, Mapping) and key in value]
                if segment.endswith("[]"):
                    values = [item for value in values if isinstance(value, (list, tuple)) for item in value]
            values = [value for value in values if isinstance(value, str)]
        names = [name.replace(token, value, 1) for name in names for value in values]
    return tuple(dict.fromkeys(names))


def _check_template(template: Any, what: str, check: Callable[[Any, str], Any]) -> None:
    """``template`` with every placeholder filled in must pass ``check``."""

    if not isinstance(template, str):
        raise TypeError(f"declared {what} names must be strings")
    leftover = _PLACEHOLDER.sub("x", template)
    if "{" in leftover or "}" in leftover:
        raise ValueError(f"declared {what} {template!r} has a placeholder other than {{id}} or {{config.<path>}}")
    check(_PLACEHOLDER.sub("x.x", template), f"declared {what}")


@dataclass(frozen=True)
class EvidenceDeclaration:
    """The schema of one evidence ID's value; ``many`` allows more than one item per component use.

    Without ``many`` the evidence is a single-valued quantity: a component
    use that emits it twice in one run has contradicted itself.
    """

    value: ConfigField
    many: bool = False

    def __post_init__(self) -> None:
        require_type(self.value, ConfigField, "evidence value schema")
        require_type(self.many, bool, "many")


@dataclass(frozen=True)
class Declarations:
    """What a component publishes, emits, and changes; the runner holds it to them.

    ``events`` maps each event name the component publishes (besides the
    runner's lifecycle events) to the schema of its payload; ``evidence``
    maps each evidence ID it emits to the schema of its value; ``changes``
    names the kinds of host or server state it changes, each owned by an
    undo obligation while it lasts. Names may hold placeholders (``{id}``,
    ``{config.<path>}``).
    """

    events: Mapping[str, ConfigSchema] = field(default_factory=dict)
    evidence: Mapping[str, EvidenceDeclaration] = field(default_factory=dict)
    changes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name, schema in self.events.items():
            _check_template(name, "event", require_name)
            require_type(schema, ConfigSchema, f"payload schema of event {name!r}")
            if name.startswith("{id}.") and name[len("{id}.") :] in (*LIFECYCLE_EVENTS, *TEARDOWN_EVENTS):
                raise ValueError(f"event {name!r} is published by the runner")
        for evidence_id, declaration in self.evidence.items():
            _check_template(evidence_id, "evidence", require_qualified_id)
            require_type(declaration, EvidenceDeclaration, f"declaration of evidence {evidence_id!r}")
        object.__setattr__(self, "events", MappingProxyType(dict(self.events)))
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))
        changes = unique_tuple(self.changes, "changes")
        for kind in changes:
            require_name(kind, "change kind")
        object.__setattr__(self, "changes", changes)

    def events_for(self, spec: ComponentSpec) -> dict[str, ConfigSchema]:
        """The events ``spec`` may publish, by name, with their payload schemas."""

        return {name: schema for template, schema in self.events.items() for name in _expand(template, spec)}

    def evidence_for(self, spec: ComponentSpec) -> dict[str, EvidenceDeclaration]:
        """The evidence ``spec`` may emit, by ID, with its declaration."""

        return {name: item for template, item in self.evidence.items() for name in _expand(template, spec)}

    def json_schema(self) -> dict[str, Any]:
        """The declarations as the catalog shows them: payload and value schemas, as written."""

        return {
            "events": {name: schema.json_schema() for name, schema in sorted(self.events.items())},
            "evidence": {
                name: {"value": item.value.json_schema(), "many": item.many}
                for name, item in sorted(self.evidence.items())
            },
            "changes": list(self.changes),
        }
