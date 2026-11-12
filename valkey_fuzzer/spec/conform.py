"""``spec conform``: rebuild a model's states from real runs and check the claims on them.

For each run, the mapping turns the run's records (fuzzer events, including
log lines published as events, and state samples) into a sequence of
observed states over the model's variables. TLC then evaluates every claimed
property marked ``conform: true`` and every target on each observed state,
through ``INSTANCE model WITH <var> <- Trace[k].<var>``, so the properties are
the model's own definitions.

This is an observed-state check plus action coverage, not refinement: it does
not check that consecutive observed states are related by the model's Next
(with stuttering and hidden steps). Observations are partial and coarser than
model steps; the report says so.

A run's records are its committed events, or a trace a component wrote: a
JSONL file :data:`TRACE_FILE` in the component's artifact folder
(``components/<id>/``) or anywhere under a directory given. The spec bridge
owns this format: the first record is the header, ``{"kind": "header",
"nodes": {<node>: {"node_id": <ID>, "config": {...}}}, "scenario_id", "seed",
"experiment_id", "inputs"}``, and every later record is one the mapping's
evidence matches. A ``node_id`` field of the mapping resolves to its node
through ``nodes``, and a constant mapped from a ``config`` reads ``nodes.<node>.config``.
"""

import copy
import glob
import json
import os
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from ..documents import ABSENT, get_path
from .bundle import MODEL_FILE, Bundle, Evidence, SpecMapping, StateVar, field_ref, where_matches
from .tla import ModelValue, TlaError, TlaFunction, TlaSet, TlaTools, print_values, render, tlc

CHECKS = "observed-state check and action coverage"
REFINEMENT = (
    "not checked: consecutive observed states are not checked against the model's Next (with stuttering and "
    "hidden steps); a pass means every observed state satisfies the checked properties, not that the run "
    "is a behavior of the model"
)
_TAG = "spec-conform"
TRACE_FILE = "trace.jsonl"


class _SkipError(Exception):
    """A record's evidence cannot be applied (a field is missing or names an unknown node)."""


class _OutsideProcessError(_SkipError):
    """An update keys on a node that is not in the process set it ranges over."""


@dataclass
class RunEvidence:
    """What one run left behind: its records in order, the trace header, and the run's identity.

    ``problem`` says why a run folder could not be read; the run is then an
    error rather than left out, so a conformance pass covers every run.
    """

    source: str
    header: dict[str, Any]
    records: list[dict[str, Any]]
    meta: dict[str, Any] = field(default_factory=dict)
    problem: Optional[str] = None


# ---------------------------------------------------------------------------
# Reading runs
# ---------------------------------------------------------------------------


def _jsonl(path: str) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _trace_run(path: str, meta: Optional[dict[str, Any]] = None) -> RunEvidence:
    records = _jsonl(path)
    if not records or records[0].get("kind") != "header":
        raise ValueError(f"{path} does not start with a trace header")
    header = records[0]
    meta = dict(meta or {})
    for key in ("scenario_id", "seed", "experiment_id", "inputs"):
        meta.setdefault(key, header.get(key))
    return RunEvidence(source=path, header=header, records=records[1:], meta=meta)


def _run_report(path: str) -> Optional[dict[str, Any]]:
    """The content of a report.json that is a run report, else None."""

    try:
        with open(path, encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, ValueError):
        return None
    if isinstance(report, dict) and isinstance(report.get("outcome"), dict) and "plan" in report["outcome"]:
        return report
    return None


def _report_run(path: str, report: Mapping[str, Any]) -> RunEvidence:
    outcome = report["outcome"]
    plan = outcome["plan"]
    meta = {
        "scenario_id": plan.get("scenario_id"),
        "seed": plan.get("seed"),
        "experiment_id": plan.get("experiment_id"),
        "inputs": plan.get("inputs"),
        "run_status": report.get("status"),
        "report": path,
    }
    folder = os.path.dirname(path)
    traces = sorted(glob.glob(os.path.join(folder, "components", "*", TRACE_FILE)))
    if traces:
        return _trace_run(traces[0], meta)
    events = sorted(
        (item for item in (outcome.get("diagnostics") or {}).get("events") or [] if item.get("committed")),
        key=lambda item: item.get("sequence", 0),
    )
    records = [{"kind": "event", "t": item.get("timestamp"), **item} for item in events]
    return RunEvidence(source=path, header={"nodes": {}}, records=records, meta=meta)


def _folder_run(folder: str) -> RunEvidence:
    """The run in a run folder: its report, else (a run killed before it wrote one) its trace."""

    report_path = os.path.join(folder, "report.json")
    if os.path.exists(report_path):
        report = _run_report(report_path)
        if report is not None:
            return _report_run(report_path, report)
        problem = f"{report_path}: report.json is not a readable run report"
    else:
        traces = sorted(glob.glob(os.path.join(folder, "components", "*", TRACE_FILE)))
        if not traces:
            problem = f"{folder}: the run left no report.json and no trace"
        else:
            try:
                return _trace_run(traces[0])
            except (OSError, ValueError) as exc:
                problem = f"{traces[0]}: {exc}"
    return RunEvidence(source=folder, header={}, records=[], problem=problem)


def _directory_runs(path: str) -> list[RunEvidence]:
    """Every run folder under ``path`` (one with plan.json or report.json), then traces outside them."""

    folders = sorted(
        {
            os.path.dirname(item)
            for name in ("plan.json", "report.json")
            for item in glob.glob(os.path.join(path, "**", name), recursive=True)
        }
    )
    runs = [_folder_run(folder) for folder in folders]
    for trace in sorted(glob.glob(os.path.join(path, "**", TRACE_FILE), recursive=True)):
        if not any(trace.startswith(folder + os.sep) for folder in folders):
            try:
                runs.append(_trace_run(trace))
            except (OSError, ValueError) as exc:
                runs.append(RunEvidence(source=trace, header={}, records=[], problem=f"{trace}: {exc}"))
    return runs


def collect_runs(paths: Iterable[str]) -> list[RunEvidence]:
    """Runs from report.json files, run folders, directories of them, or trace files.

    In a directory, every run folder counts: one whose report cannot be
    read, or that left neither a report nor a trace, is collected with its
    ``problem``, and a run killed before its report counts by its trace.
    """

    runs = []
    for path in paths:
        if os.path.isdir(path):
            runs.extend(_directory_runs(path))
        elif path.endswith(".jsonl"):
            runs.append(_trace_run(path))
        else:
            report = _run_report(path)
            if report is None:
                raise ValueError(f"{path} is not a run report.json, a run folder, or a trace.jsonl file")
            runs.append(_report_run(path, report))
    return runs


# ---------------------------------------------------------------------------
# Observed states
# ---------------------------------------------------------------------------


def _identity(prefix: str, node: str) -> ModelValue:
    return ModelValue(prefix + "_" + re.sub(r"[^A-Za-z0-9]", "_", node))


class ObservedRun:
    """One run's records replayed through the mapping into observed states."""

    def __init__(self, mapping: SpecMapping, run: RunEvidence):
        self.mapping = mapping
        self.run = run
        nodes = run.header.get("nodes") or {}
        self.by_node_id = {info["node_id"]: node for node, info in nodes.items() if info.get("node_id")}
        self.notes: list[str] = []
        # Mapped records (or their updates) that could not be applied: the states may miss what they show.
        self.unapplied: list[str] = []
        self.actions: Counter = Counter()
        self.members = self._members()
        self.states: list[dict[str, Any]] = []
        self.steps: list[dict[str, Any]] = []

    # -- fields and values --------------------------------------------------

    def _fields(self, evidence: Evidence, record: Mapping[str, Any]) -> dict[str, Any]:
        body = evidence.body(record)
        values = {}
        for spec in evidence.fields.values():
            value = get_path(body, spec.path)
            if value is ABSENT or value is None:
                if not spec.optional:
                    raise _SkipError(f"{evidence.name}: no {spec.path}")
                value = spec.default
            values[spec.name] = None if value is None else self._typed(evidence, spec.name, spec.type, value)
        return values

    def _typed(self, evidence: Evidence, name: str, kind: str, value: Any) -> Any:
        try:
            if kind == "int":
                return int(value)
            if kind == "bool":
                # bool("false") is True: take a JSON boolean, or the word true or false.
                if isinstance(value, bool):
                    return value
                if isinstance(value, str) and value.lower() in ("true", "false"):
                    return value.lower() == "true"
                raise ValueError(value)
            if kind == "node_id":
                if value not in self.by_node_id:
                    raise _SkipError(f"{evidence.name}: {name} {value} is no node of the trace header")
                return self.by_node_id[value]
            return str(value)
        except (TypeError, ValueError) as exc:
            raise _SkipError(f"{evidence.name}: {name} = {value!r} is not {kind}") from exc

    def _members(self) -> dict[str, list[str]]:
        members: dict[str, list[str]] = {name: [] for name in self.mapping.processes}
        for record in self.run.records:
            for name, process in self.mapping.processes.items():
                for rule in process.members:
                    evidence = self.mapping.evidence[rule.evidence]
                    if not evidence.matches(record):
                        continue
                    body = evidence.body(record)
                    if not where_matches(body, rule.where):
                        continue
                    if not all(get_path(body, key) not in (ABSENT, None, [], {}, "") for key in rule.require):
                        continue
                    try:
                        node = self._fields(evidence, record)[rule.field]
                    except _SkipError:
                        continue
                    if node not in members[name]:
                        members[name].append(node)
        return {name: sorted(nodes) for name, nodes in members.items()}

    def identities(self, process: str) -> list[ModelValue]:
        prefix = self.mapping.processes[process].prefix
        return [_identity(prefix, node) for node in self.members[process]]

    def _identity_of(self, node: str, process: str) -> ModelValue:
        if node not in self.members[process]:
            raise _OutsideProcessError(f"{node} is not one of {process}")
        return _identity(self.mapping.processes[process].prefix, node)

    def _value(self, spec: Any, fields: Mapping[str, Any]) -> Any:
        if isinstance(spec, str):
            ref = field_ref(spec)
            if not ref:
                return spec
            value = fields[ref[0]]
            return self._identity_of(value, ref[1]) if ref[1] else value
        if isinstance(spec, list):
            return tuple(self._value(item, fields) for item in spec)
        if isinstance(spec, Mapping):
            if set(spec) == {"set"}:
                values = TlaSet()
                for item in spec["set"]:
                    value = self._value(item, fields)
                    if value not in values:
                        values.append(value)
                return values
            return {key: self._value(item, fields) for key, item in spec.items()}
        return spec

    def _keys(self, var: StateVar, spec: Any, fields: Mapping[str, Any]) -> list[Any]:
        if spec is None:
            raise _SkipError(f"{var.name} is a function; the update needs a key")
        ref = field_ref(str(spec))
        if ref and not ref[1]:
            # A node in every process set the variable ranges over.
            node = fields[ref[0]]
            keys = [self._identity_of(node, process) for process in var.over if node in self.members[process]]
            if not keys:
                raise _OutsideProcessError(f"{node} is in none of {list(var.over)}")
            return keys
        return [self._value(spec, fields)]

    # -- replay ---------------------------------------------------------------

    def initial(self) -> dict[str, Any]:
        state: dict[str, Any] = {}
        for var in self.mapping.state.values():
            if var.hidden is not None:
                continue
            if var.over:
                state[var.name] = TlaFunction(
                    (key, self._value(var.init, {})) for process in var.over for key in self.identities(process)
                )
            else:
                state[var.name] = self._value(var.init, {})
        return state

    def replay(self) -> list[dict[str, Any]]:
        state = self.initial()
        self.states = [copy.deepcopy(state)]
        self.steps = [{"state": 1, "why": "initial state"}]
        applied: dict[str, list[Mapping[str, Any]]] = {name: [] for name in self.mapping.evidence}
        # Records waiting for the record they follow: trace order is log time to the millisecond, and two
        # nodes' lines of one millisecond can be read in either order.
        waiting: list[tuple[Evidence, Mapping[str, Any], Mapping[str, Any]]] = []
        for record in self.run.records:
            for evidence in self.mapping.evidence.values():
                if not evidence.matches(record):
                    continue
                try:
                    fields = self._fields(evidence, record)
                except _SkipError as exc:
                    self.unapplied.append(str(exc))
                    continue
                if not self._follows(evidence, fields, applied):
                    waiting.append((evidence, record, fields))
                    continue
                self._step(state, evidence, record, fields)
                applied[evidence.name].append(fields)
                ready = True
                while ready:
                    ready = next((item for item in waiting if self._follows(item[0], item[2], applied)), None)
                    if ready:
                        waiting.remove(ready)
                        self._step(state, *ready)
                        applied[ready[0].name].append(ready[2])
        for evidence, _, fields in waiting:
            after = evidence.after
            wanted = ", ".join(f"{name} {fields[spec[1:]]}" for name, spec in after.match.items())
            self.unapplied.append(f"{evidence.name}: no {after.evidence} record ({wanted}) before or after it")
        return self.states

    def _follows(
        self, evidence: Evidence, fields: Mapping[str, Any], applied: Mapping[str, list[Mapping[str, Any]]]
    ) -> bool:
        """Whether the record ``evidence`` follows (its ``after``) has been applied, or it follows none."""

        if evidence.after is None:
            return True
        wanted = {name: fields[spec[1:]] for name, spec in evidence.after.match.items()}
        return any(
            all(prior[name] == value for name, value in wanted.items()) for prior in applied[evidence.after.evidence]
        )

    def _step(
        self, state: dict[str, Any], evidence: Evidence, record: Mapping[str, Any], fields: Mapping[str, Any]
    ) -> None:
        before = copy.deepcopy(state)
        for update in evidence.updates:
            # Each update stands alone: a node outside the process set one update ranges over still
            # gets the updates whose variables it is in.
            try:
                self._apply(state, update, fields)
            except _OutsideProcessError as exc:
                self._note(f"{evidence.name}: {update.op} {update.var} skipped: {exc}")
            except _SkipError as exc:
                self.unapplied.append(f"{evidence.name}: {update.op} {update.var}: {exc}")
        if evidence.action:
            self.actions[evidence.action] += 1
        if state != before:
            self.states.append(copy.deepcopy(state))
            self.steps.append(
                {
                    "state": len(self.states),
                    "why": evidence.action or f"{evidence.name} sample",
                    "evidence": evidence.name,
                    "t": record.get("t"),
                    "fields": {key: value for key, value in fields.items()},
                }
            )

    def _note(self, text: str) -> None:
        if text not in self.notes and len(self.notes) < 100:
            self.notes.append(text)

    def _apply(self, state: dict[str, Any], update: Any, fields: Mapping[str, Any]) -> None:
        var = self.mapping.state[update.var]
        if var.hidden is not None:
            raise _SkipError(f"{var.name} is hidden; it cannot be updated")
        targets = self._keys(var, update.key, fields) if var.over else [None]
        value = None if update.value is None else self._value(update.value, fields)
        for key in targets:
            current = state[var.name] if key is None else state[var.name][key]
            if update.op == "set":
                new = value
            elif update.op == "max":
                new = max(current, value)
            elif update.op == "incr":
                new = current + 1
            elif update.op == "clear":
                new = self._value(var.init, {})
            elif update.op == "add":
                new = TlaSet(current)
                if value not in new:
                    new.append(value)
            else:  # remove
                new = TlaSet(item for item in current if item != value)
            if key is None:
                state[var.name] = new
            else:
                state[var.name][key] = new

    # -- constants ------------------------------------------------------------

    def constants(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for name in self.mapping.processes:
            values[name] = render(TlaSet(self.identities(name)))
        for name, spec in self.mapping.constants.items():
            if "value" in spec:
                values[name] = str(spec["value"])
            elif "config" in spec:
                seen = [
                    info.get("config", {}).get(spec["config"])
                    for info in (self.run.header.get("nodes") or {}).values()
                    if spec["config"] in (info.get("config") or {})
                ]
                if not seen:
                    raise TlaError(f"constant {name} needs {spec['config']!r} from the trace header's node configs")
                values[name] = render(all(item == spec["equals"] for item in seen))
            elif "input" in spec:
                # The experiment sets the configuration from this input; the run records its value.
                inputs = self.run.meta.get("inputs") or {}
                if spec["input"] not in inputs:
                    raise TlaError(f"constant {name} needs input {spec['input']!r}, which the run does not record")
                values[name] = render(inputs[spec["input"]] == spec["equals"])
            else:
                numbers = [spec.get("at_least", 0)]
                for state in self.states:
                    for var in spec["max_of"]:
                        value = state[var]
                        numbers.extend(value.values() if isinstance(value, dict) else [value])
                values[name] = str(max(item for item in numbers if isinstance(item, int)))
        return values


# ---------------------------------------------------------------------------
# Checking observed states with TLC
# ---------------------------------------------------------------------------


def trace_module(
    bundle: Bundle, observed: ObservedRun, constants: Mapping[str, str], names: Sequence[str], targets: Sequence[str]
) -> tuple[str, str]:
    """``ObservedTrace.tla`` and its config: every observed state, and PrintT of where each check fails or holds."""

    mapping = bundle.mapping
    variables = list(mapping.state)
    states = []
    for state in observed.states:
        fields = [
            f"{var} |-> {mapping.state[var].hidden if mapping.state[var].hidden is not None else render(state[var])}"
            for var in variables
        ]
        states.append("[" + ", ".join(fields) + "]")
    model_values = sorted({str(item) for process in mapping.processes for item in observed.identities(process)})
    substitutions = ",\n    ".join(f"{var} <- Trace[k].{var}" for var in variables)
    prints = [
        f'ASSUME PrintT(<<"{_TAG}", "violated", "{name}", {{k \\in DOMAIN Trace : ~M(k)!{name}}}>>)' for name in names
    ] + [f'ASSUME PrintT(<<"{_TAG}", "reached", "{name}", {{k \\in DOMAIN Trace : M(k)!{name}}}>>)' for name in targets]
    declared = list(constants) + model_values
    module = "\n".join(
        [
            "---- MODULE ObservedTrace ----",
            f"\\* States observed in {observed.run.source}, rebuilt through {os.path.basename(mapping.path)}.",
            "EXTENDS Naturals, Sequences, TLC",
            f"CONSTANTS {', '.join(declared)}" if declared else "",
            "VARIABLE checked",
            "Trace == <<",
            "    " + ",\n    ".join(states),
            ">>",
            "M(k) == INSTANCE model WITH",
            "    " + substitutions,
            *prints,
            "Init == checked = TRUE",
            "Next == UNCHANGED checked",
            "====",
            "",
        ]
    )
    cfg = "\n".join(
        ["CONSTANTS"]
        + [f"    {name} = {value}" for name, value in constants.items()]
        + [f"    {name} = {name}" for name in model_values]
        + ["INIT Init", "NEXT Next", ""]
    )
    return module, cfg


def check_run(bundle: Bundle, run: RunEvidence, tools: TlaTools) -> dict[str, Any]:
    """Rebuild ``run``'s observed states and check the claims on them."""

    claims = bundle.claims
    names = [item.name for item in claims.properties if item.conform]
    targets = [item.name for item in claims.targets]
    result: dict[str, Any] = {
        "source": run.source,
        **{key: run.meta.get(key) for key in ("scenario_id", "seed", "inputs", "run_status", "report")},
        "excluded": {item.name: item.conform_note for item in claims.properties if not item.conform},
    }
    if run.problem is not None:
        return {**result, "states": 0, "status": "error", "error": run.problem}
    recorded = (run.header.get("spec") or {}).get("mapping_sha256")
    observed = ObservedRun(bundle.mapping, run)
    observed.replay()
    if recorded and recorded != bundle.mapping.sha256:
        observed.notes.append("the run recorded a different mapping.yaml (sha256 differs): check for mapping drift")
    result.update(
        {
            "processes": {
                name: dict(zip(observed.members[name], map(str, observed.identities(name))))
                for name in bundle.mapping.processes
            },
            "states": len(observed.states),
            "actions": dict(sorted(observed.actions.items())),
            "notes": observed.notes,
        }
    )
    if observed.unapplied:
        result["status"] = "error"
        shown = "; ".join(observed.unapplied[:5]) + ("; ..." if len(observed.unapplied) > 5 else "")
        result["error"] = (
            f"{len(observed.unapplied)} mapped record(s) could not be applied, so the observed states may miss "
            f"what they show: {shown}"
        )
        return result
    if len(observed.states) < 2 or not observed.actions:
        result["status"] = "error"
        result["error"] = "no mapped evidence in this run: nothing to check"
        return result
    try:
        result["constants"] = observed.constants()
        module, cfg = trace_module(bundle, observed, result["constants"], names, targets)
        _, run_tlc = tlc(
            tools,
            "ObservedTrace.tla",
            "ObservedTrace.cfg",
            {MODEL_FILE: bundle.model_text, "ObservedTrace.tla": module, "ObservedTrace.cfg": cfg},
        )
        if run_tlc.timed_out:
            raise TlaError(run_tlc.timeout_error)
        printed = {(item[1], item[2]): sorted(item[3]) for item in print_values(run_tlc.output, _TAG)}
    except TlaError as exc:
        result["status"] = "error"
        result["error"] = str(exc)
        return result
    missing = [name for name in names if ("violated", name) not in printed] + [
        name for name in targets if ("reached", name) not in printed
    ]
    if missing:
        result["status"] = "error"
        result["error"] = f"TLC did not evaluate {missing}"
        result["tlc_output"] = run_tlc.tail()
        return result
    properties = {}
    for name in names:
        failing = printed[("violated", name)]
        claim = bundle.property(name)
        claimed = claim.holds_under(result["constants"])
        entry: dict[str, Any] = {"status": "violated" if failing else "holds", "claimed": claimed}
        if not claimed:
            entry["outside_claim"] = {key: result["constants"].get(key) for key in claim.holds_when}
        if failing:
            entry["states"] = failing
            entry["first"] = observed.steps[failing[0] - 1]
            entry["previous"] = observed.steps[max(failing[0] - 2, 0)]
        properties[name] = entry
    result["properties"] = properties
    result["targets"] = {}
    for name in targets:
        reached = printed[("reached", name)]
        result["targets"][name] = {"reached": bool(reached), "states": reached}
        if reached:
            result["targets"][name]["first"] = observed.steps[reached[0] - 1]
    result["status"] = (
        "fail" if any(entry["status"] == "violated" and entry["claimed"] for entry in properties.values()) else "pass"
    )
    return result


def conform_runs(
    bundle: Bundle, runs: Sequence[RunEvidence], tools: TlaTools, *, group_by: Sequence[str] = ()
) -> dict[str, Any]:
    """Check every run and summarize: per-run results, live action coverage, targets reached, and groups."""

    results = [check_run(bundle, run, tools) for run in runs]
    observed_in: Counter = Counter()
    for result in results:
        for action in result.get("actions", {}):
            observed_in[action] += 1
    mapped = sorted(bundle.mapping.actions())
    targets = {
        item.name: sum(bool(result.get("targets", {}).get(item.name, {}).get("reached")) for result in results)
        for item in bundle.claims.targets
    }
    statuses = [result["status"] for result in results]
    report: dict[str, Any] = {
        "kind": "spec-conformance",
        "bundle": bundle.protocol,
        "mapping_sha256": bundle.mapping.sha256,
        "checks": CHECKS,
        "refinement": REFINEMENT,
        "properties_checked": [item.name for item in bundle.claims.properties if item.conform],
        "runs": results,
        "action_coverage": {
            "runs": len(results),
            "observed": {action: observed_in.get(action, 0) for action in mapped},
            "never_observed": [action for action in mapped if not observed_in.get(action)],
            "unobservable": bundle.mapping.unobservable,
        },
        "targets_reached": targets,
        "status": "fail" if "fail" in statuses else ("error" if "error" in statuses or not results else "pass"),
    }
    if group_by:
        report["groups"] = _groups(results, group_by, [item.name for item in bundle.claims.targets])
    return report


def _groups(results: Sequence[Mapping[str, Any]], keys: Sequence[str], targets: Sequence[str]) -> list[dict[str, Any]]:
    groups: dict[tuple, list[Mapping[str, Any]]] = {}
    for result in results:
        inputs = result.get("inputs") or {}
        groups.setdefault(tuple(inputs.get(key) for key in keys), []).append(result)
    rows = []
    for group, members in sorted(groups.items(), key=lambda item: tuple(map(str, item[0]))):
        rows.append(
            {
                **dict(zip(keys, group)),
                "runs": len(members),
                "run_passed": sum(result.get("run_status") == "PASS" for result in members),
                "conform_violations": sum(result["status"] == "fail" for result in members),
                "conform_errors": sum(result["status"] == "error" for result in members),
                **{
                    f"reached {name}": sum(
                        bool(result.get("targets", {}).get(name, {}).get("reached")) for result in members
                    )
                    for name in targets
                },
            }
        )
    return rows


__all__ = [
    "CHECKS",
    "REFINEMENT",
    "ObservedRun",
    "RunEvidence",
    "check_run",
    "collect_runs",
    "conform_runs",
    "trace_module",
]
