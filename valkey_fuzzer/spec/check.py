"""``spec check``: the model-checking gates of a bundle, as a machine-readable assurance report.

Gates, in order (each records ``status`` pass, fail, or error and what it saw):

``bundle``            claims.yaml and mapping.yaml read and agree with the model text
``parsed``            every module uses only the standard modules and the bundle's own, and
                      SANY accepts model.tla and every mutated negative-control module
``claims_checked``    every property is checked by a safe config whose constants meet its
                      ``holds_when``, and every declared bound is what the safe configs use
``model_checked``     TLC finds no error in every expected-safe config
``model_coverage``    which actions fired in the safe configs (TLC's coverage); an error when
                      TLC printed none, as ``mapping_complete`` then is
``negative_controls`` TLC finds the named violation in every negative control; a control
                      that passes means the property cannot fail as configured (vacuous)
``experiment``        the counterexamples of the negative controls translate into scenarios
                      of the mapped experiment (the bundle's own ``experiments/`` or an
                      installed one) that compile, with every mapped fault a fault component;
                      a bundle with its own experiments must be the one its protocol names
``mapping_complete``  every action TLC explored is mapped to evidence or declared unobservable,
                      and every mapped action exists
``approved_claim``    the claims' approval status; a failing gate only with ``require_approved``

With a time limit (:meth:`TlaTools.within`), TLC and SANY are stopped (with
their process group) when it runs out. A run cut short, or a tool that never
started (a corrupt jar), says nothing about the model: that run's gate is an
``error`` and the check ends there. So is a bundle reference that names no
bundle. A failing gate outranks an error, as a FAIL does in a batch.

The report scopes itself: a pass means the model satisfies its claimed
properties within the stated bounds, and says nothing about Valkey's code.
"""

import posixpath
import time
from collections.abc import Mapping, Sequence
from typing import Any, Callable, Optional

from .bundle import MODEL_FILE, SPEC_PATH_VARIABLE, Bundle, NegativeControl, SpecBundleError, find_bundle, load_bundle
from .tla import STANDARD_MODULES, TlaTools, TlcResult, ToolRun, module_references, normalize, parse_config, sany, tlc
from .translate import NoFaultStepError, TranslateError, bundle_service, translate_trace

SCOPE = (
    "Model-checking evidence for the bundle's model within its declared bounds and assumptions. "
    "It does not show that Valkey implements the model; see `spec conform` for runs."
)

Progress = Callable[[str], None]


class _StopError(Exception):
    """A tool run timed out or never started; the gate it belongs to is already recorded as an error."""


def _stop(gate: dict[str, Any], entry: dict[str, Any], error: str) -> None:
    entry.update(status="error", error=error)
    gate["status"] = "error"
    raise _StopError(error)


def _flat(name: str) -> str:
    """A bundle file's name in the one directory a tool runs in (``negative-controls/x.cfg`` -> ``x.cfg``)."""

    return posixpath.basename(name)


def _files(bundle: Bundle, *names: Optional[str]) -> dict[str, str]:
    """The model plus the given bundle files, flattened into one directory."""

    files = {MODEL_FILE: bundle.model_text}
    for name in names:
        if name:
            files[_flat(name)] = bundle.files[name]
    return files


def _violation_matches(result: TlcResult, control: NegativeControl, kind: str, config_checks: tuple[str, ...]) -> bool:
    if kind == "invariant":
        return result.violation == "invariant" and result.violated == control.violates
    if result.violation == "action-property":
        return result.violated == control.violates
    # TLC does not name a violated temporal property; the control must check only that one.
    return result.violation == "temporal" and config_checks == (control.violates,)


def run_control(bundle: Bundle, control: NegativeControl, tools: TlaTools) -> tuple[TlcResult, ToolRun]:
    """Model-check one negative control (its config, against its mutated module or model.tla)."""

    module = _flat(control.module) if control.module else MODEL_FILE
    return tlc(tools, module, _flat(control.config), _files(bundle, control.config, control.module))


def _tlc_summary(result: TlcResult, run: ToolRun) -> dict[str, Any]:
    return {
        "exit_code": result.exit_code,
        "violation": result.violation,
        "violated": result.violated,
        "distinct_states": result.distinct_states,
        "generated_states": result.generated_states,
        "depth": result.depth,
        "seconds": run.seconds,
        **({"timed_out": True} if run.timed_out else {}),
    }


def check_bundle(
    reference: str,
    tools: TlaTools,
    *,
    require_approved: bool = False,
    experiment_dirs: Sequence[str] = (),
    progress: Optional[Progress] = None,
) -> dict[str, Any]:
    """Run every gate on the bundle at ``reference`` and return the assurance report.

    The time limit of ``tools`` bounds the whole check; ``experiment_dirs`` say
    which experiments, besides the Valkey ones and the bundle's own, the
    translated scenarios compile against.
    """

    say = progress or (lambda message: None)
    started = time.time()
    report: dict[str, Any] = {
        "kind": "spec-assurance",
        "bundle": reference,
        "scope": SCOPE,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "tools": tools.identity(),
        "timeout_s": tools.timeout_s,
        "gates": {},
    }
    try:
        find_bundle(reference)
    except SpecBundleError as exc:
        report["gates"]["bundle"] = {"status": "error", "error": str(exc)}
        return _finish(report, started)
    try:
        bundle = load_bundle(reference)
    except SpecBundleError as exc:
        report["gates"]["bundle"] = {"status": "fail", "error": str(exc)}
        return _finish(report, started)
    claims = bundle.claims
    report.update(
        {
            "bundle": bundle.protocol,
            "summary": claims.summary,
            "claims": {
                "status": claims.status,
                "approved_by": claims.approved_by,
                "scope": list(claims.scope),
                "properties": [
                    {
                        "name": item.name,
                        "kind": item.kind,
                        "statement": item.statement,
                        "holds_when": item.holds_when,
                        "negative_controls": list(item.negative_controls),
                    }
                    for item in claims.properties
                ],
                "assumptions": list(claims.assumptions),
                "exclusions": list(claims.exclusions),
                "bounds": claims.bounds,
            },
            "mapping_sha256": bundle.mapping.sha256,
        }
    )
    report["gates"]["bundle"] = {"status": "pass"}
    try:
        _model_gates(bundle, tools, report, say)
        traces = _negative_controls(bundle, tools, report, say)
    except _StopError as exc:
        report["error"] = f"spec check stopped: {exc}"
        return _finish(report, started)
    gates = report["gates"]
    if gates["parsed"]["status"] == "fail":
        return _finish(report, started)
    say("Translating the counterexamples into scenarios")
    gates["experiment"] = _experiment(bundle, traces, experiment_dirs)

    # mapping_complete
    mapping = bundle.mapping
    mapped = set(mapping.actions()) | set(mapping.unobservable)
    explored = set(gates["model_coverage"]["actions"])
    gates["mapping_complete"] = {
        "status": "pass" if mapped == explored else "fail",
        "observed_by": mapping.actions(),
        "unobservable": mapping.unobservable,
        "unmapped_actions": sorted(explored - mapped),
        "unknown_actions": sorted(mapped - explored),
    }
    if not explored:
        gates["mapping_complete"].update(
            status="error", error="TLC reported no action coverage to compare the mapping with"
        )

    # approved_claim
    approved = claims.status == "approved"
    gates["approved_claim"] = {
        "status": "pass" if approved or not require_approved else "fail",
        "claims_status": claims.status,
        "approved_by": claims.approved_by,
        "required": require_approved,
    }
    return _finish(report, started)


def _model_gates(bundle: Bundle, tools: TlaTools, report: dict[str, Any], say: Progress) -> None:
    """``parsed``, then (when it passes) ``claims_checked``, ``model_checked``, and ``model_coverage``."""

    gates = report["gates"]
    claims = bundle.claims
    say("SANY: model.tla")
    own = {"model"} | {_flat(control.module)[: -len(".tla")] for control in bundle.controls if control.module}
    parsed: dict[str, Any] = {}
    gates["parsed"] = {"status": "fail", "modules": parsed}
    for module in [MODEL_FILE] + [control.module for control in bundle.controls if control.module]:
        text = bundle.files[module]
        foreign = sorted(module_references(text) - STANDARD_MODULES - own)
        if foreign:
            parsed[module] = {
                "status": "fail",
                "error": f"uses modules {', '.join(foreign)}; a bundle's modules may use only the standard modules "
                f"({', '.join(sorted(STANDARD_MODULES))}) and the bundle's own",
            }
            continue
        ok, run = sany(tools, _flat(module), _files(bundle, module if module != MODEL_FILE else None))
        parsed[module] = {"status": "pass" if ok else "fail", **({} if ok else {"output": run.tail()})}
        if run.harness_error:
            _stop(gates["parsed"], parsed[module], run.harness_error)
    gates["parsed"]["status"] = _all(parsed.values())
    if gates["parsed"]["status"] == "fail":
        return

    # claims_checked
    configs = {name: parse_config(bundle.files[name]) for name in bundle.safe_configs}
    problems = []
    for item in claims.properties:
        checking = [
            name
            for name, config in configs.items()
            if item.name in config.checked and item.holds_under(config.constants)
        ]
        if not checking:
            problems.append(f"{item.name}: no safe config checks it with {item.holds_when or 'any constants'}")
    for constant, value in claims.bounds.items():
        for name, config in configs.items():
            if normalize(config.constants.get(constant, "")) != normalize(value):
                problems.append(f"bound {constant} = {value}, but {name} sets {config.constants.get(constant)!r}")
    gates["claims_checked"] = {"status": "fail" if problems else "pass", "problems": problems}

    # model_checked + model_coverage
    checked: dict[str, Any] = {}
    coverage: dict[str, dict[str, Any]] = {}
    gates["model_checked"] = {"status": "fail", "configs": checked}
    for name in bundle.safe_configs:
        say(f"TLC: {name}")
        result, run = tlc(tools, MODEL_FILE, name, _files(bundle, name), coverage=True)
        entry = {"status": "pass" if result.no_error else "fail", "checks": list(configs[name].checked)}
        entry.update(_tlc_summary(result, run))
        if not result.no_error:
            entry["errors"] = result.errors
            entry["output"] = run.tail()
        checked[name] = entry
        if run.harness_error:
            _stop(gates["model_checked"], entry, run.harness_error)
        report["tools"].setdefault("tlc_version", result.version)
        for action, counts in result.coverage.items():
            total = coverage.setdefault(action, {"distinct": 0, "generated": 0, "configs": []})
            total["distinct"] += counts["distinct"]
            total["generated"] += counts["generated"]
            if counts["generated"]:
                total["configs"].append(name)
    gates["model_checked"]["status"] = _all(checked.values())
    # TLC printing no coverage block is output the check cannot read, not a defect in the bundle.
    gates["model_coverage"] = {
        "status": "pass" if coverage else "error",
        "actions": coverage,
        "never_fired": sorted(action for action, counts in coverage.items() if not counts["generated"]),
        **({} if coverage else {"error": "TLC reported no action coverage"}),
    }


def _negative_controls(bundle: Bundle, tools: TlaTools, report: dict[str, Any], say: Progress) -> dict[str, TlcResult]:
    """The ``negative_controls`` gate; returns each control's TLC result, for translating its counterexample."""

    if report["gates"]["parsed"]["status"] == "fail":
        return {}
    controls: dict[str, Any] = {}
    results: dict[str, TlcResult] = {}
    report["gates"]["negative_controls"] = gate = {"status": "fail", "controls": controls}
    for control in bundle.controls:
        say(f"TLC: negative control {control.name} (must violate {control.violates})")
        result, run = run_control(bundle, control, tools)
        config_checks = parse_config(bundle.files[control.config]).checked
        found = not run.timed_out and _violation_matches(
            result, control, bundle.property(control.violates).kind, config_checks
        )
        entry = {
            "status": "pass" if found else "fail",
            "violates": control.violates,
            "config": control.config,
            "module": control.module or MODEL_FILE,
            "trace_length": len(result.trace),
            **_tlc_summary(result, run),
        }
        controls[control.name] = entry
        if run.harness_error:
            _stop(gate, entry, run.harness_error)
        if not found:
            entry["error"] = (
                f"expected TLC to report {control.violates} violated; "
                + (
                    f"it reported {result.violation} {result.violated or ''}".strip()
                    if result.violation
                    else "it found no error"
                )
                + ": the claim cannot fail as configured (vacuous) or the control is broken"
            )
            entry["output"] = run.tail()
            continue
        results[control.name] = result
    gate["status"] = _all(controls.values())
    return results


def _experiment(bundle: Bundle, traces: Mapping[str, TlcResult], experiment_dirs: Sequence[str]) -> dict[str, Any]:
    """The ``experiment`` gate: every control's counterexample becomes scenarios that compile, or has no fault."""

    scenario = bundle.mapping.scenario
    if not scenario:
        return {
            "status": "pass",
            "note": "mapping.yaml has no scenario section, so counterexamples do not become scenarios",
        }
    gate: dict[str, Any] = {"status": "fail", "experiment": scenario["experiment"], "controls": {}}
    if bundle.experiment_directory is not None:
        # The bundle's own experiments name it by protocol (spec: <protocol>): that name must be this bundle,
        # or they would compile against another copy's mapping.
        try:
            named = str(find_bundle(bundle.protocol))
        except SpecBundleError as exc:
            named = str(exc)
        if named != bundle.path:
            gate["error"] = (
                f"the protocol name {bundle.protocol!r} does not resolve to this bundle ({named}); put the "
                f"directory holding it on ${SPEC_PATH_VARIABLE}"
            )
            return gate
    try:
        service = bundle_service(bundle, experiment_dirs)
    except Exception as exc:
        gate["error"] = f"the experiments do not load: {exc}"
        return gate
    for control in bundle.controls:
        result = traces.get(control.name)
        if result is None:
            gate["controls"][control.name] = {"status": "fail", "error": "the control found no counterexample"}
            continue
        try:
            translation = translate_trace(
                bundle,
                result.trace,
                bundle.files[control.config],
                name=control.name,
                violated=result.violated or result.violation,
                compile_scenario=service.compile,
            )
        except NoFaultStepError as exc:
            # A counterexample that needs no fault has nothing for the experiment to inject.
            gate["controls"][control.name] = {"status": "no-fault", "reason": str(exc)}
            continue
        except (TranslateError, SpecBundleError) as exc:
            gate["controls"][control.name] = {"status": "fail", "error": str(exc)}
            continue
        gate["controls"][control.name] = {
            "status": "pass",
            "scenarios": [item["scenario_id"] for item in translation["scenarios"]],
            "faults": {item["fault"]: item["component"] for item in translation["alignment"]},
        }
    entries = gate["controls"].values()
    translated = any(entry["status"] == "pass" for entry in entries)
    if not translated:
        gate["error"] = "no negative control's counterexample has a mapped fault: the experiment is never exercised"
    gate["status"] = "pass" if translated and all(entry["status"] != "fail" for entry in entries) else "fail"
    return gate


def _all(entries: Any) -> str:
    return "pass" if all(entry["status"] == "pass" for entry in entries) else "fail"


def _finish(report: dict[str, Any], started: float) -> dict[str, Any]:
    statuses = {gate["status"] for gate in report["gates"].values()}
    if "fail" in statuses:
        report["status"] = "fail"
    elif "error" in statuses or "error" in report:
        report["status"] = "error"
    else:
        report["status"] = "pass"
    report["seconds"] = round(time.time() - started, 1)
    return report


__all__ = ["SCOPE", "check_bundle", "run_control"]
