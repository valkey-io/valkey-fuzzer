"""The evidence contract: the runner holds every component to what its registration declares.

A registration's :class:`~valkey_fuzzer.declarations.Declarations` name the
events a component publishes and the evidence it emits, each with a schema.
:func:`check_requirements` makes sure, before anything runs, that an
experiment's required events and evidence can come from some component of its
plan. :class:`EvidenceContract` checks, as it happens, every event a component
publishes and every evidence item it returns; a violation is recorded, the
offending item never reaches a subscriber or an oracle, and the run is
``ERROR``.

Besides its declarations, every component is held to the rules that need
none: an event's source is a component of the plan, a component publishes
nothing once its last call returned, evidence references only committed
events, and a component never publishes the runner's lifecycle events.
"""

import threading
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from ._data import to_plain_data
from .catalog import ComponentRegistry
from .declarations import Declarations
from .events import LIFECYCLE_EVENTS, RUNNER_EVENTS, TEARDOWN_EVENTS
from .plan import ComponentSpec, ExecutionPlan
from .protocols import ROLES
from .results import Evidence
from .schema import ConfigError


class ContractViolationError(RuntimeError):
    """A component published or emitted something its declarations do not allow."""


def lifecycle_events(spec: ComponentSpec) -> tuple[str, ...]:
    """The events the runner publishes for one component use."""

    suffixes = LIFECYCLE_EVENTS + (TEARDOWN_EVENTS if ROLES[spec.role].teardown else ())
    return tuple(f"{spec.id}.{suffix}" for suffix in suffixes)


def _declared(plan: ExecutionPlan, components: ComponentRegistry) -> dict[str, Declarations]:
    """Each component's declarations, their names expanded for its config."""

    declared = {}
    for spec in plan.components:
        declares = components.resolve(spec.use).declares
        declared[spec.id] = Declarations(declares.events_for(spec), declares.evidence_for(spec), declares.changes)
    return declared


def _check_declared(declared: Mapping[str, Declarations], evidence_id: str, source: Optional[str], what: str) -> None:
    """Raise ``ValueError`` unless ``source`` (any component when None) declares ``evidence_id``."""

    if source is not None and source not in declared:
        raise ValueError(f"{what} {evidence_id!r} names source {source!r}, which is not a component of this plan")
    sources = [source] if source is not None else list(declared)
    if not any(evidence_id in declared[name].evidence for name in sources):
        where = f"component {source!r}" if source else "any component"
        raise ValueError(f"{what} {evidence_id!r} is not declared by {where}")


def check_evidence_reference(
    plan: ExecutionPlan, components: ComponentRegistry, evidence_id: str, source: Optional[str], what: str
) -> None:
    """Raise ``ValueError`` unless ``evidence_id`` can come from ``source`` (any component when None) of ``plan``.

    ``what`` names the reference in the message, such as ``compare rule evidence``.
    """

    _check_declared(_declared(plan, components), evidence_id, source, what)


def check_requirements(plan: ExecutionPlan, components: ComponentRegistry) -> None:
    """Raise ``ValueError`` unless every required event and evidence can come from the plan.

    A required event is one the runner publishes, or one a component of the
    plan declares; required evidence, and the evidence a component's config
    names (its registration's ``references_evidence``), must be declared by
    its ``source`` or, without one, by some component. A misspelt ID would
    otherwise be judged at run time, as missing evidence or as a rule that
    can never match.
    """

    declared = _declared(plan, components)
    known = set(RUNNER_EVENTS)
    for spec in plan.components:
        known.update(lifecycle_events(spec))
        known.update(declared[spec.id].events)
    for name in plan.required_events:
        if name not in known:
            raise ValueError(f"required event {name!r} is not published by the runner or declared by any component")
    for requirement in plan.required_evidence:
        _check_declared(declared, requirement.evidence_id, requirement.source, "required evidence")
        if requirement.source is not None and requirement.min_count > 1:
            if not declared[requirement.source].evidence[requirement.evidence_id].many:
                raise ValueError(
                    f"required evidence {requirement.evidence_id!r} needs {requirement.min_count} items from "
                    f"{requirement.source!r}, which declares it as a single value"
                )
    for spec in plan.components:
        references = components.resolve(spec.use).references_evidence
        for evidence_id, source in references(spec) if references is not None else ():
            _check_declared(declared, evidence_id, source, f"component {spec.id!r} names evidence")


class EvidenceContract:
    """Holds the components of one executable plan to their declarations for one run.

    :meth:`check_event` is the event bus's check: it raises
    :class:`ContractViolationError` for an event a component may not publish, so
    it is never dispatched. :meth:`check_evidence` checks a component's
    result before the runner records it. Every violation is kept in
    :attr:`violations` for the verdict.
    """

    def __init__(self, components: ComponentRegistry):
        self._components = components
        self._specs: dict[str, ComponentSpec] = {}
        self._declared: dict[str, Declarations] = {}
        self._lifecycle: set[str] = set()
        self._finished: set[str] = set()
        self._emitted: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()
        self.violations: list[Mapping[str, Any]] = []

    def bind(self, plan: ExecutionPlan) -> None:
        """Hold the components of ``plan`` (the executable plan, once its references are resolved) to account."""

        declared = _declared(plan, self._components)
        with self._lock:
            self._specs = {spec.id: spec for spec in plan.components}
            self._declared = declared
            self._lifecycle = {name for spec in plan.components for name in lifecycle_events(spec)}

    def _violate(self, component: str, kind: str, name: str, message: str) -> ContractViolationError:
        message = f"component {component!r} {message}"
        with self._lock:
            self.violations.append({"component": component, "kind": kind, "name": name, "message": message})
        return ContractViolationError(message)

    def planned_changes(self) -> dict[str, tuple[str, ...]]:
        """The kinds of state each component of the plan may change."""

        with self._lock:
            return {component: item.changes for component, item in self._declared.items()}

    def check_change(self, owner: str, kind: str) -> None:
        """Raise unless component ``owner`` may open an obligation for a change of ``kind`` now."""

        with self._lock:
            known, declared, finished = owner in self._specs, self._declared.get(owner), owner in self._finished
        if not known:
            raise self._violate(owner, "change", kind, f"changed {kind!r}, but is not a component of the plan")
        if finished:
            raise self._violate(owner, "change", kind, f"changed {kind!r} after its last call returned")
        if kind not in declared.changes:
            raise self._violate(owner, "change", kind, f"changed undeclared state {kind!r}")

    def finished(self, component_id: str) -> None:
        """``component_id``'s last call returned: it may publish nothing more."""

        with self._lock:
            self._finished.add(component_id)

    def check_event(self, name: str, source: str, payload: Optional[Mapping[str, Any]]) -> None:
        """Raise unless component ``source`` may publish event ``name`` with ``payload`` now."""

        with self._lock:
            known, declared, finished = source in self._specs, self._declared.get(source), source in self._finished
        if not known:
            raise self._violate(source, "event", name, f"published event {name!r}, but is not a component of the plan")
        if name in RUNNER_EVENTS or name in self._lifecycle:
            raise self._violate(source, "event", name, f"published {name!r}, which only the runner publishes")
        if finished:
            raise self._violate(source, "event", name, f"published {name!r} after its last call returned")
        schema = declared.events.get(name)
        if schema is None:
            raise self._violate(source, "event", name, f"published undeclared event {name!r}")
        try:
            schema.resolve(payload or {}, f"payload of {name!r}")
        except ConfigError as exc:
            raise self._violate(source, "event", name, f"published a malformed {name!r}: {exc}") from None

    def check_evidence(self, spec: ComponentSpec, items: Sequence[Evidence], committed: set[int]) -> None:
        """Raise unless ``spec`` may emit ``items``; count them only if all pass."""

        declared = self._declared.get(spec.id)
        counts: dict[tuple[str, str], int] = {}
        for item in items:
            evidence_id = item.evidence_id
            if item.source != spec.id:
                raise self._violate(spec.id, "evidence", evidence_id, f"emitted evidence owned by {item.source!r}")
            if item.event_sequence is not None and item.event_sequence not in committed:
                raise self._violate(
                    spec.id,
                    "evidence",
                    evidence_id,
                    f"emitted {evidence_id!r} referencing uncommitted event sequence {item.event_sequence}",
                )
            declaration = declared.evidence.get(evidence_id)
            if declaration is None:
                raise self._violate(spec.id, "evidence", evidence_id, f"emitted undeclared evidence {evidence_id!r}")
            try:
                declaration.value.resolve(to_plain_data(item.value), f"value of {evidence_id!r}")
            except ConfigError as exc:
                raise self._violate(
                    spec.id, "evidence", evidence_id, f"emitted malformed {evidence_id!r}: {exc}"
                ) from None
            key = (spec.id, evidence_id)
            counts[key] = counts.get(key, 0) + 1
            with self._lock:
                total = self._emitted.get(key, 0) + counts[key]
            if total > 1 and not declaration.many:
                raise self._violate(
                    spec.id,
                    "evidence",
                    evidence_id,
                    f"emitted {evidence_id!r} {total} times, but declares it as a single value",
                )
        with self._lock:
            for key, count in counts.items():
                self._emitted[key] = self._emitted.get(key, 0) + count
