"""The process exit codes of every command, and how a run's outcome prints (the commands and the batch worker)."""

from typing import Any, Union

import yaml

from ._data import to_plain_data
from .results import ComponentStatus, OutcomeStatus, RunOutcome

# The process exit codes of every command (docs/cli.md#exit-codes).
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_HARNESS_ERROR = 2
EXIT_UNSUPPORTED = 3
EXIT_INTERRUPTED = 130

EXIT_CODES = {
    OutcomeStatus.PASS: EXIT_SUCCESS,
    OutcomeStatus.FAIL: EXIT_FAILURE,
    OutcomeStatus.ERROR: EXIT_HARNESS_ERROR,
    OutcomeStatus.SKIP_UNSUPPORTED: EXIT_UNSUPPORTED,
}


def exit_code(status: Union[OutcomeStatus, str], *, allow_unsupported: bool = False) -> int:
    """The documented exit code for an outcome status; ``allow_unsupported`` turns SKIP_UNSUPPORTED into success."""

    status = OutcomeStatus(status)
    if status is OutcomeStatus.SKIP_UNSUPPORTED and allow_unsupported:
        return EXIT_SUCCESS
    return EXIT_CODES[status]


def _duration(outcome: RunOutcome) -> float:
    if outcome.started_at is None or outcome.ended_at is None:
        return 0.0
    return max(0.0, outcome.ended_at - outcome.started_at)


def _print_summary(outcome: RunOutcome) -> None:
    counts = {status: 0 for status in ComponentStatus}
    for result in outcome.component_results:
        counts[result.status] += 1
    print(f"\nScenario: {outcome.plan.scenario_id}")
    print(f"Experiment: {outcome.plan.experiment_id}")
    print(f"Status: {outcome.status.value}")
    print(f"Mode: {outcome.plan.mode.value}")
    print(f"Deployment: {outcome.plan.deployment_mode}")
    print(f"Duration: {_duration(outcome):.2f}s")
    print(
        "Components: "
        f"{counts[ComponentStatus.PASS]} passed, "
        f"{counts[ComponentStatus.FAIL]} failed, "
        f"{counts[ComponentStatus.SKIP]} skipped, "
        f"{counts[ComponentStatus.ERROR]} errors"
    )
    print(f"Evidence: {len(outcome.all_evidence)}")
    if outcome.valkey_sha:
        print(f"Valkey SHA: {outcome.valkey_sha}")
    print(f"Summary: {outcome.summary}")


def _yaml_lines(value: Any) -> list[str]:
    return yaml.safe_dump(to_plain_data(value), default_flow_style=False, sort_keys=False).rstrip().splitlines()


def print_outcome(outcome: RunOutcome, *, verbose: bool = False) -> None:
    """A run's summary; ``verbose`` adds every component's result and the run diagnostics."""

    _print_summary(outcome)
    if not verbose:
        return
    results_by_id = {result.component_id: result for result in outcome.component_results}

    print("\nComponent Results:")
    for component in outcome.plan.components:
        result = results_by_id.get(component.id)
        if result is None:
            print(f"  [NOT_RUN] {component.id} ({component.role.value}: {component.use})")
            continue

        duration = 0.0
        if result.started_at is not None and result.ended_at is not None:
            duration = max(0.0, result.ended_at - result.started_at)
        print(f"  [{result.status.value}] {component.id} ({component.role.value}: {component.use}, {duration:.2f}s)")
        print(f"    Summary: {result.summary}")
        print(f"    Evidence: {len(result.evidence)}")
        if result.diagnostics:
            print("    Diagnostics:")
            for line in _yaml_lines(result.diagnostics):
                print(f"      {line}")

    if outcome.diagnostics:
        print("\nRun Diagnostics:")
        for line in _yaml_lines(outcome.diagnostics):
            print(f"  {line}")
