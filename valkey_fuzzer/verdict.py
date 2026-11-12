"""Outcome calculation: how a run's results, events, evidence, and errors become its verdict.

Only an oracle or action FAIL judges the system under test, and only a trusted one makes the run FAIL. A FAIL
is not trusted when it depends on a component that does not judge and did not do its part (it returned ERROR
or FAIL: a fault not injected, an observer that could not read the server), nor, for an oracle, when a
teardown before the oracles failed or left a change for the runner to undo, unless it reports a crash
(:attr:`~valkey_fuzzer.results.ComponentResult.crash`). Without a trusted FAIL, anything else that went
wrong makes the run ERROR: a required component that failed or was skipped, a missing required event or
evidence item (the run did not reach what it tests), or a failed teardown or cleanup. :func:`decide`
applies the precedence that picks which of several problems names the outcome, and
:func:`environment_status` the one environment teardown adds on top.
"""

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from ._data import to_plain_data
from .plan import ExecutionPlan
from .protocols import ROLES, ComponentRole
from .results import ComponentResult, ComponentStatus, Evidence, OutcomeStatus, evidence_of


def required_components(plan: ExecutionPlan) -> frozenset[str]:
    """Required components, evidence sources, and everything they depend on."""

    by_id = {component.id: component for component in plan.components}
    required = {component.id for component in plan.components if component.required}
    required.update(item.source for item in plan.required_evidence if item.source is not None)
    pending = list(required)
    while pending:
        for dependency in by_id[pending.pop()].depends_on:
            if dependency not in required:
                required.add(dependency)
                pending.append(dependency)
    return frozenset(required)


def missing_evidence(plan: ExecutionPlan, evidence: Sequence[Evidence]) -> tuple[Mapping[str, Any], ...]:
    """Each requirement the observed ``evidence`` does not meet, with its observed count."""

    missing = []
    for requirement in plan.required_evidence:
        count = len(evidence_of(evidence, requirement.evidence_id, requirement.source))
        if count < requirement.min_count:
            missing.append({**to_plain_data(requirement), "observed_count": count})
    return tuple(missing)


def _named(items: Sequence[Mapping[str, Any]]) -> str:
    named = [f"{item['evidence_id']} from {item.get('source') or 'any component'}" for item in items]
    more = f", and {len(named) - 5} more" if len(named) > 5 else ""
    return ", ".join(named[:5]) + more


@dataclass(frozen=True)
class Verdict:
    """A run's status and summary, the diagnostics behind them, and whether a
    trustworthy failure of the system under test was established."""

    status: OutcomeStatus
    summary: str
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
    trusted: bool = False


@dataclass(frozen=True)
class RunState:
    """What the runner recorded of one run, which :func:`decide` judges.

    ``results`` maps a component ID to its result, ``invoked`` names the
    components the runner called, and ``evidence`` is every item they
    recorded; ``committed(name)`` returns the committed events with that name;
    ``terminal`` is a status and summary that ended the run early;
    ``runner_failure`` says in which phase and why the runner itself failed,
    if it did; ``teardown_failed`` reports a component's ``stop`` or
    ``recover`` that failed before the oracles, and ``cleanup_failed`` a
    cleanup that failed after them (the deployment's, or a deferred
    teardown); ``violations`` are the evidence contract's, each of which
    makes the run ERROR: what a component emitted against its declarations
    proves nothing; ``open_obligations`` are changes their owner did not undo
    on its first try: like a failed teardown, they leave the oracles looking
    at a deployment that may not be in the state the plan describes.
    """

    plan: ExecutionPlan
    results: Mapping[str, ComponentResult]
    invoked: Collection[str]
    evidence: Sequence[Evidence]
    committed: Callable[[str], Sequence[Any]]
    terminal: Optional[tuple[OutcomeStatus, str]] = None
    runner_failure: Optional[str] = None
    teardown_failed: bool = False
    cleanup_failed: bool = False
    violations: Sequence[Mapping[str, Any]] = ()
    open_obligations: Sequence[Mapping[str, Any]] = ()


def decide(run: RunState) -> Verdict:
    """The verdict for one run, judged over the plan's components in dependency order."""

    plan, results, invoked, evidence, committed = run.plan, run.results, run.invoked, run.evidence, run.committed
    terminal, runner_failure, violations, open_obligations = (
        run.terminal, run.runner_failure, run.violations, run.open_obligations
    )  # fmt: skip
    ordered, required = plan.ordered_components, required_components(plan)

    required_failures = tuple(
        spec.id
        for spec in ordered
        if spec.id in required
        and spec.id in results
        and results[spec.id].status in (ComponentStatus.FAIL, ComponentStatus.SKIP)
    )
    # Root causes are required components that ran and did not pass; blocked
    # components are their consequences. Only a verdict-role FAIL is
    # evidence about the system under test.
    invoked_failures = [spec for spec in ordered if spec.id in required_failures and spec.id in invoked]
    # A component that does not judge and did not pass (a fault not injected,
    # a workload that stopped, an observer that could not read the server)
    # did not do its part: a finding that depends on it may be caused by that.
    # A finding that does not depend on it, such as a crash before a fault
    # fired, stands.
    unestablished_setup = {
        spec.id
        for spec in ordered
        if spec.id in results
        and not ROLES[spec.role].judges
        and results[spec.id].status in (ComponentStatus.ERROR, ComponentStatus.FAIL)
    }
    # Oracles judge the deployment once it is torn down. A failed teardown, or
    # a change the runner had to undo, may leave it in a state the plan does
    # not describe, so an oracle's FAIL is not trusted then, unless it reports
    # a crash, which may be why the teardown failed. An action's FAIL was
    # found before the teardown and stands.
    unclean = run.teardown_failed or bool(open_obligations)

    def trusted(spec: Any) -> bool:
        result = results[spec.id]
        if not ROLES[spec.role].judges or result.status is not ComponentStatus.FAIL:
            return False
        if plan.dependencies(spec.id) & unestablished_setup:
            return False
        return not (unclean and spec.role is ComponentRole.ORACLE and not result.crash)

    findings = tuple(spec.id for spec in invoked_failures if trusted(spec))
    untrusted = tuple(spec.id for spec in invoked_failures if spec.id not in findings)
    # Required components that did not do their part, besides those that failed.
    errored = tuple(
        spec.id
        for spec in ordered
        if spec.id in required and spec.id in unestablished_setup and spec.id not in required_failures
    )
    missing_events = tuple(name for name in plan.required_events if not committed(name))
    missing = missing_evidence(plan, evidence)
    found = bool(findings) and terminal is None and runner_failure is None

    diagnostics: dict[str, Any] = {}
    if violations:
        diagnostics["contract_violations"] = tuple(violations)
    if terminal is None:
        for key, value in (
            ("missing_required_events", missing_events),
            ("missing_required_evidence", missing),
            ("required_component_failures", required_failures),
            ("untrusted_component_failures", untrusted),
        ):
            if value:
                diagnostics[key] = value

    if violations:
        more = f" (and {len(violations) - 1} more)" if len(violations) > 1 else ""
        status, summary = OutcomeStatus.ERROR, f"evidence contract violated: {violations[0]['message']}{more}"
        found = False
    elif open_obligations and not found:
        first = open_obligations[0]
        how = "is still active" if "undone_at" not in first else f"was undone by the {first['undone_by']}"
        if first.get("errors") and "undone_at" in first:
            how += " after a failed undo"
        more = f" (and {len(open_obligations) - 1} more)" if len(open_obligations) > 1 else ""
        status = OutcomeStatus.ERROR
        summary = f"component {first['owner']!r} did not undo its {first['kind']} on {first['target']}: it {how}{more}"
    elif runner_failure is not None:
        status, summary = OutcomeStatus.ERROR, f"runner execution failed during {runner_failure}"
    elif terminal is not None and terminal[0] is OutcomeStatus.ERROR:
        status, summary = OutcomeStatus.ERROR, terminal[1]
    elif found:
        status, summary = OutcomeStatus.FAIL, f"required components did not pass: {', '.join(required_failures)}"
    elif terminal is None and (untrusted or errored):
        status = OutcomeStatus.ERROR
        summary = (
            "no trustworthy verdict is possible; required components did not do their part outside an oracle or "
            "action, depend on one that did not, judged a deployment whose teardown failed, or were skipped: "
            f"{', '.join(errored + untrusted)}"
        )
    elif run.teardown_failed or run.cleanup_failed:
        status, summary = OutcomeStatus.ERROR, "cleanup failed before a trustworthy failure was established"
    elif terminal is not None:
        status, summary = terminal
    elif missing_events or missing:
        # The run never reached what it tests, and nothing found the server wanting.
        reasons = []
        if missing_events:
            reasons.append(f"required events were not observed: {', '.join(missing_events)}")
        if missing:
            reasons.append(f"required evidence was not observed: {_named(missing)}")
        status = OutcomeStatus.ERROR
        summary = f"{'; '.join(reasons)}; the run did not reach what it tests, so no trustworthy verdict is possible"
    else:
        status, summary = OutcomeStatus.PASS, "experiment passed"
    return Verdict(status, summary, diagnostics, found)


def environment_status(
    status: OutcomeStatus, summary: str, *, trusted: bool, cleaned: bool
) -> tuple[OutcomeStatus, str]:
    """The status once the environment is released; a failed release can only make it worse.

    A PASS or SKIP_UNSUPPORTED becomes ERROR, and so does a FAIL that was not
    yet a trustworthy failure of the system under test.
    """

    if not cleaned:
        if status in (OutcomeStatus.PASS, OutcomeStatus.SKIP_UNSUPPORTED):
            return OutcomeStatus.ERROR, "environment cleanup failed after execution"
        if status is OutcomeStatus.FAIL and not trusted:
            return (
                OutcomeStatus.ERROR,
                "environment cleanup failed before a trustworthy failure was established",
            )
    return status, summary
