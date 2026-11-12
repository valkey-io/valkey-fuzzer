"""What a run produces: evidence, component results, and the run outcome, and the helpers components build
results and evidence with.
"""

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from typing import Any, Optional

from ._data import canonical_json, freeze, freeze_mapping, jsonable, to_plain_data
from .fields import (
    as_tuple,
    model_from_data,
    models_from_data,
    require_name,
    require_number,
    require_qualified_id,
    require_text,
    require_type,
)
from .plan import ComponentSpec, ExecutionPlan


class ComponentStatus(str, Enum):
    """Terminal result of one component invocation."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"
    ERROR = "ERROR"


class OutcomeStatus(str, Enum):
    """Terminal, trustworthy classification for a complete run."""

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP_UNSUPPORTED = "SKIP_UNSUPPORTED"
    ERROR = "ERROR"


def _check_times(started_at: Any, ended_at: Any) -> None:
    for value, field_name in ((started_at, "started_at"), (ended_at, "ended_at")):
        if value is not None:
            require_number(value, field_name)
    if started_at is not None and ended_at is not None and ended_at < started_at:
        raise ValueError("ended_at cannot be earlier than started_at")


@dataclass(frozen=True)
class Evidence:
    """Immutable path evidence or diagnostic observation."""

    evidence_id: str
    source: str
    phase: str
    observed_at: float
    value: Any
    event_sequence: Optional[int] = None
    artifacts: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_qualified_id(self.evidence_id, "evidence_id")
        require_name(self.source, "source")
        require_name(self.phase, "phase")
        require_number(self.observed_at, "observed_at")
        if self.event_sequence is not None and require_number(self.event_sequence, "event_sequence", integer=True) < 1:
            raise ValueError("event_sequence must be a positive integer")
        artifacts = as_tuple(self.artifacts, "artifacts")
        for artifact in artifacts:
            require_text(artifact, "artifact")
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "value", freeze(self.value, "evidence value"))
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "evidence metadata"))


@dataclass(frozen=True)
class ComponentResult:
    """Terminal result emitted by one component of a plan.

    ``crash`` marks a FAIL that reports a server process that crashed or
    stopped answering without the fuzzer stopping it. Such a FAIL stands even
    when the teardown before the oracles failed, since the crash may be why it
    failed.
    """

    component_id: str
    status: ComponentStatus
    summary: str
    evidence: tuple[Evidence, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    crash: bool = False

    def __post_init__(self) -> None:
        require_name(self.component_id, "component_id")
        object.__setattr__(self, "status", ComponentStatus(self.status))
        require_type(self.summary, str, "summary")
        evidence = as_tuple(self.evidence, "evidence")
        for item in evidence:
            require_type(item, Evidence, "evidence entry")
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(self, "diagnostics", freeze_mapping(self.diagnostics, "diagnostics"))
        _check_times(self.started_at, self.ended_at)
        require_type(self.crash, bool, "crash")
        if self.crash and self.status is not ComponentStatus.FAIL:
            raise ValueError("only a FAIL can report a crash")


@dataclass(frozen=True)
class RunOutcome:
    """Terminal classification and result bundle for a complete run."""

    plan: ExecutionPlan
    status: OutcomeStatus
    summary: str
    component_results: tuple[ComponentResult, ...] = ()
    evidence: tuple[Evidence, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    # Git SHA of the valkey-server under test, when the deployment provider knows it.
    valkey_sha: Optional[str] = None

    def __post_init__(self) -> None:
        require_type(self.plan, ExecutionPlan, "plan")
        object.__setattr__(self, "status", OutcomeStatus(self.status))
        require_type(self.summary, str, "summary")
        if self.valkey_sha is not None:
            require_type(self.valkey_sha, str, "valkey_sha")
        for name, item_type in (("component_results", ComponentResult), ("evidence", Evidence)):
            items = as_tuple(getattr(self, name), name)
            for item in items:
                require_type(item, item_type, f"{name} entry")
            object.__setattr__(self, name, items)
        object.__setattr__(self, "diagnostics", freeze_mapping(self.diagnostics, "diagnostics"))
        _check_times(self.started_at, self.ended_at)

    @property
    def all_evidence(self) -> tuple[Evidence, ...]:
        return self.evidence + tuple(item for result in self.component_results for item in result.evidence)

    def to_data(self) -> Mapping[str, Any]:
        return to_plain_data(self)

    def to_json(self) -> str:
        return canonical_json(self)

    @classmethod
    def from_data(cls, value: Mapping[str, Any]) -> "RunOutcome":
        """Rebuild a validated outcome from its canonical data form."""

        return model_from_data(
            cls,
            value,
            "run outcome",
            plan=ExecutionPlan.from_data,
            component_results=partial(
                models_from_data,
                ComponentResult,
                location="component_results",
                evidence=partial(models_from_data, Evidence, location="evidence"),
            ),
            evidence=partial(models_from_data, Evidence, location="evidence"),
        )


def make_evidence(
    spec: ComponentSpec,
    evidence_id: str,
    phase: str,
    value: Any,
    *,
    event_sequence: Optional[int] = None,
    artifacts: Sequence[str] = (),
    metadata: Optional[Mapping[str, Any]] = None,
) -> Evidence:
    """Evidence owned by component ``spec``, observed now; ``value`` is made JSON-safe."""

    return Evidence(
        evidence_id=evidence_id,
        source=spec.id,
        phase=phase,
        observed_at=time.time(),
        value=jsonable(value),
        event_sequence=event_sequence,
        artifacts=tuple(artifacts),
        metadata=jsonable(metadata or {}),
    )


def make_result(
    spec: ComponentSpec,
    status: ComponentStatus,
    summary: str,
    *,
    evidence: Sequence[Evidence] = (),
    diagnostics: Optional[Mapping[str, Any]] = None,
    crash: bool = False,
) -> ComponentResult:
    """A result for component ``spec``; ``diagnostics`` are made JSON-safe, and ``crash`` marks a FAIL that
    reports a crashed or unresponsive server (:class:`ComponentResult`)."""

    return ComponentResult(
        component_id=spec.id,
        status=status,
        summary=summary,
        evidence=tuple(evidence),
        diagnostics=jsonable(diagnostics or {}),
        crash=crash,
    )


def evidence_of(evidence: Sequence[Evidence], evidence_id: str, source: Optional[str] = None) -> list[Evidence]:
    """The items of ``evidence`` with ``evidence_id`` (and ``source``, if given)."""

    return [item for item in evidence if item.evidence_id == evidence_id and source in (None, item.source)]
