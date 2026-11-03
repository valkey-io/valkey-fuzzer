"""Run reports and report folders: the files a run leaves, writing and reading them, and the report it gets.

Every executed plan, whichever command runs it, leaves one report folder::

    <folder>/plan.json                      the execution plan, written before the run starts
    <folder>/report.json                    the run report (see :func:`run_report`)
    <folder>/components/<component id>/    files the run's components wrote

``run`` puts it at ``<root>/runs/<name>`` and ``replay`` at
``<root>/replays/<name>``. A differential or minimization gets a session
folder ``<root>/differential/<name>`` or ``<root>/minimize/<name>`` holding
one report folder per variant (``<variant>``, next to ``differential.json``)
or candidate (``attempt-<n>``); a batch puts each job's at ``runs/<job id>``
of its batch folder. The root is the artifact directory, or ``<tmp>/valkey-fuzzer``. Every ``<name>`` is
``<UTC time to the millisecond>-<scenario or document ID>-<random suffix>``
(:func:`~valkey_fuzzer.hoststate.new_run_folder`, which also names a run executed
without a report folder), created exclusively, so names sort by start time and
never collide, even between processes.
"""

import json
import os
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

import yaml

from ._data import to_plain_data
from .catalog import Catalog
from .coverage import coverage_tuple
from .hoststate import new_run_folder
from .plan import ExecutionPlan
from .provenance import reproducer as provenance_reproducer
from .results import RunOutcome
from .schema import load_yaml, plain_config

# Every run report has these keys (see :func:`run_report`).
REPORT_KEYS = ("generated_at", "status", "summary", "scenario", "outcome", "coverage", "reproducer", "provenance")
# What a report's reproducer records; replay compares each against this host.
REPRODUCER_KEYS = (
    "python",
    "platform",
    "experiment",
    "fuzzer_digest",
    "components",
    "deployment_provider",
    "plan_digest",
    "valkey_sha",
)

PLAN_FILE = "plan.json"
REPORT_FILE = "report.json"
# A generated scenario, written next to its report so it runs again without the generator.
SCENARIO_FILE = "scenario.yaml"


class RunServiceError(RuntimeError):
    """The service cannot do what was asked; the command-line interface reports a harness error."""


class InputError(RunServiceError):
    """A scenario, report, plan, or document file is missing, unreadable, or of the wrong kind."""


class ReportWriteError(RunServiceError):
    """A report, output, or exported scenario file cannot be written."""


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------


def _write_text(path: str, text: str, what: str) -> None:
    """Write ``text`` to ``path`` atomically: to a temporary file beside it, renamed over it once synced."""

    output = Path(path)
    temporary_path = None
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(text)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, output)
    except BaseException as exc:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
        if isinstance(exc, OSError):
            raise ReportWriteError(f"could not write {what} to {path}: {exc}") from exc
        raise


def write_document(path: str, data: Any, output_format: str = "json") -> None:
    """Atomically write ``data`` to ``path`` as JSON or YAML."""

    if output_format not in {"json", "yaml"}:
        raise ValueError(f"unsupported output format: {output_format}")
    try:
        if output_format == "json":
            text = json.dumps(data, indent=2, allow_nan=False, default=str) + "\n"
        else:
            text = yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
    except (ValueError, yaml.YAMLError) as exc:
        raise ReportWriteError(f"could not write the report to {path}: {exc}") from exc
    _write_text(path, text, "the report")


def read_scenario(path: str) -> str:
    source = Path(path)
    if not source.is_file():
        raise InputError(f"scenario file not found: {path}")
    try:
        return source.read_text(encoding="utf-8")
    except OSError as exc:
        raise InputError(f"unable to read scenario file {path}: {exc}") from exc


def load_document(path: str) -> Any:
    source = Path(path)
    if not source.is_file():
        raise InputError(f"file not found: {path}")
    try:
        text = source.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise InputError(f"unable to read {path}: {exc}") from exc
    if source.suffix != ".json":
        return load_yaml(text, path)
    # Not as YAML: a YAML 1.1 loader reads a JSON float such as 5e-05 as a string.
    try:
        return json.loads(text, object_pairs_hook=_unique_keys)
    except ValueError as exc:
        raise InputError(f"{path} is not valid JSON: {exc}") from exc


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError(f"duplicate key {key!r}")
        document[key] = value
    return document


def make_report_folder(path: str) -> str:
    """Create ``path``, which must not exist yet."""

    try:
        os.makedirs(path)
    except OSError as exc:
        raise ReportWriteError(f"could not create the report folder {path}: {exc}") from exc
    return path


def new_report_folder(parent: str, name: str) -> str:
    """Create and return ``<parent>/<UTC time>-<name>-<suffix>`` (:func:`~valkey_fuzzer.hoststate.new_run_folder`)."""

    try:
        return new_run_folder(parent, name)
    except OSError as exc:
        raise ReportWriteError(f"could not create a report folder under {parent}: {exc}") from exc


def write_plan(report_dir: str, plan: ExecutionPlan) -> None:
    """Write ``plan.json``, before the run starts."""

    write_document(os.path.join(report_dir, PLAN_FILE), plan.to_data())


def export_scenario(scenario: Mapping[str, Any], path: str, *, generated: Optional[Mapping[str, Any]] = None) -> None:
    """Write a scenario so that ``run`` runs it again; a generated one's header says how ``generated`` drew it."""

    target = Path(path)
    header = f"# Reproduce with: valkey-fuzzer run {target.name}\n"
    if generated is not None:
        header = (
            f"# valkey-fuzzer generated scenario: {generated['experiment']}, seed {generated['seed']} "
            f"(attempt {generated['attempt']}).\n{header}"
        )
    _write_text(path, header + yaml.safe_dump(dict(scenario), sort_keys=False), "the scenario")


# ---------------------------------------------------------------------------
# Run reports
# ---------------------------------------------------------------------------


def run_report(
    catalog: Catalog,
    outcome: RunOutcome,
    *,
    scenario: Optional[Mapping[str, Any]] = None,
    provenance: Optional[Mapping[str, Any]] = None,
    generated: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """One self-contained, replayable report for a single run.

    ``outcome`` holds the plan (``outcome.plan``) with the component
    results, evidence, and diagnostics; ``coverage`` is the run's coverage
    tuple; ``reproducer`` is the environment it ran in, with
    ``generated`` (seed, experiment, attempt, exclusions, boundary)
    for a generated scenario; ``provenance`` is how a replay
    compared with the run it replayed (None for other runs).
    """

    reproducer = provenance_reproducer(catalog, outcome.plan, outcome)
    provider = catalog.deployment_providers.of_plan(outcome.plan)
    host_fields = () if provider is None else provider.host_fields
    if generated is not None:
        reproducer["generated"] = to_plain_data(generated)
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": outcome.status.value,
        "summary": outcome.summary,
        "scenario": plain_config(scenario) if scenario is not None else None,
        "outcome": outcome.to_data(),
        "coverage": to_plain_data(coverage_tuple(outcome, host_fields)),
        "reproducer": reproducer,
        "provenance": to_plain_data(provenance) if provenance is not None else None,
    }


def _missing(data: Mapping[str, Any], keys: Sequence[str]) -> list[str]:
    return [key for key in keys if key not in data]


def as_run_report(data: Any, path: str) -> Optional[Mapping[str, Any]]:
    """The run report ``data`` is, or None for anything else.

    A document with ``outcome`` or ``reproducer`` is a run report and must have
    all its keys. Anything else is left to the caller, which reads it as a bare
    plan.
    """

    if not isinstance(data, Mapping) or ("outcome" not in data and "reproducer" not in data):
        return None
    missing = _missing(data, REPORT_KEYS)
    for key, required in (("reproducer", REPRODUCER_KEYS), ("outcome", ("plan",))):
        if missing:
            break
        if not isinstance(data[key], Mapping):
            missing = [key]
        else:
            missing = [f"{key}.{name}" for name in _missing(data[key], required)]
    if missing:
        raise InputError(f"{path} is not a complete run report: it has no {', '.join(missing)}")
    return data


def recorded_run(path: str) -> tuple[ExecutionPlan, Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    """The plan to replay, its scenario, and the recorded reproducer and outcome (None for a bare plan)."""

    data = load_document(path)
    report = as_run_report(data, path)
    if report is None:
        return ExecutionPlan.from_data(data), None, None
    outcome = report["outcome"]
    recorded = {"reproducer": report["reproducer"], "outcome": outcome}
    return ExecutionPlan.from_data(outcome["plan"]), report["scenario"], recorded
