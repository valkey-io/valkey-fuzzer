"""``spec``: check a TLA+ bundle, translate its counterexamples into scenarios, and conform real runs to it."""

import argparse
import json
import os
import sys
from typing import Any

import yaml

from ..outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, EXIT_SUCCESS
from ..reports import write_document
from ..spec.bundle import SPEC_PATH_VARIABLE, Bundle, SpecBundleError, load_bundle
from ..spec.check import check_bundle, run_control
from ..spec.conform import collect_runs, conform_runs
from ..spec.tla import TlaError, TlaTools, read_tlc_output
from ..spec.translate import TranslateError, bundle_service, translate_trace
from ._common import add_common_options, positive_seconds

# How long one spec command may run SANY and TLC, unless --timeout says otherwise.
DEFAULT_TIMEOUT_S = 3600.0


def register(subparsers: Any) -> None:
    spec = subparsers.add_parser(
        "spec",
        help="Check a TLA+ spec bundle, translate its counterexamples, and conform runs to it",
        description=f"A spec bundle is a directory, or a protocol name looked up in ${SPEC_PATH_VARIABLE} and then "
        "the bundles shipped in formal/; see docs/spec-bridge.md.",
    )
    tools = spec.add_subparsers(dest="spec_command", required=True)

    check = tools.add_parser("check", help="SANY, TLC on the safe configs and negative controls, model coverage")
    add_common_options(check)
    _bundle_options(check)
    check.add_argument("--require-approved", action="store_true", help="Fail unless the claims are approved")
    check.set_defaults(handler=check_command)

    translate = tools.add_parser("translate", help="Turn a TLC counterexample into fuzzer scenarios")
    add_common_options(translate)
    _bundle_options(translate)
    source = translate.add_mutually_exclusive_group(required=True)
    source.add_argument("--control", help="Run TLC on this negative control and translate its counterexample")
    source.add_argument("--trace", help="TLC text output holding a counterexample (needs --config)")
    translate.add_argument("--config", help="The .cfg TLC ran with --trace")
    translate.add_argument("--variants", type=int, default=4, help="Nearby timing variants to add (default 4)")
    translate.add_argument("--seed", type=int, default=1, help="Seed of the first scenario (default 1)")
    translate.add_argument("--out", required=True, help="Directory to write the scenarios and translation.json to")
    translate.set_defaults(handler=translate_command)

    conform = tools.add_parser("conform", help="Check the claims on states observed in real runs")
    _bundle_options(conform)
    conform.add_argument(
        "runs", nargs="+", help="Run report folders, report.json files, directories, or trace.jsonl files"
    )
    conform.add_argument("--group-by", default="", help="Comma-separated scenario inputs to tabulate runs by")
    conform.set_defaults(handler=conform_command)


def _bundle_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("bundle", help=f"Bundle directory, or a protocol name (${SPEC_PATH_VARIABLE}, then formal/)")
    parser.add_argument("--tla2tools", help="Path to tla2tools.jar (default: $TLA2TOOLS_JAR)")
    parser.add_argument("--workers", default="auto", help="TLC workers (default auto)")
    parser.add_argument(
        "--timeout",
        type=positive_seconds,
        default=DEFAULT_TIMEOUT_S,
        metavar="SECONDS",
        help="Stop SANY and TLC (their process group) once the command has run this long; a run cut short "
        f"is an error, exit 2 (default {DEFAULT_TIMEOUT_S:g})",
    )
    parser.add_argument("--output", help="Write the JSON report to this file")
    parser.add_argument("--verbose", action="store_true")


def _tools(args: argparse.Namespace) -> TlaTools:
    return TlaTools.locate(args.tla2tools, workers=args.workers).within(args.timeout)


def _relative(bundle: Bundle, path: str) -> str:
    """``path`` relative to the bundle when it is inside it, else its file name: reports never hold absolute paths."""

    relative = os.path.relpath(os.path.abspath(path), bundle.path)
    return relative if not relative.startswith(os.pardir) else os.path.basename(path)


def _write(args: argparse.Namespace, report: dict[str, Any]) -> None:
    if args.output:
        write_document(args.output, report)
        print(f"Report: {args.output}")


def check_command(args: argparse.Namespace) -> int:
    try:
        tools = _tools(args)
    except TlaError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    report = check_bundle(
        args.bundle,
        tools,
        require_approved=args.require_approved,
        experiment_dirs=args.experiment_dirs,
        progress=print,
    )
    for name, gate in report["gates"].items():
        print(f"[{gate['status'].upper():<4}] {name}{': ' + gate['error'] if gate.get('error') else ''}")
        for item, entry in (gate.get("configs") or gate.get("controls") or gate.get("modules") or {}).items():
            if isinstance(entry, dict) and "status" in entry:
                detail = entry.get("error") or entry.get("reason") or ""
                if not detail and entry.get("distinct_states") is not None:
                    detail = f"{entry['distinct_states']} distinct states, depth {entry.get('depth')}"
                if not detail and "scenarios" in entry:
                    detail = f"{len(entry['scenarios'])} scenario(s) compile"
                print(f"         {entry['status']:<4} {item} {detail}".rstrip())
        for problem in gate.get("problems") or []:
            print(f"         {problem}")
    coverage = report["gates"].get("model_coverage", {})
    if coverage.get("never_fired"):
        print(f"Actions that never fired in a safe config: {', '.join(coverage['never_fired'])}")
    if report.get("error"):
        print(f"\n{report['error']}")
    claims = f"Claims {report['claims']['status']}; " if "claims" in report else ""
    print(f"\n{claims}spec check {report['status'].upper()} in {report['seconds']}s")
    _write(args, report)
    return {"pass": EXIT_SUCCESS, "fail": EXIT_FAILURE}.get(report["status"], EXIT_HARNESS_ERROR)


def translate_command(args: argparse.Namespace) -> int:
    bundle = load_bundle(args.bundle)
    if args.control:
        control = bundle.control(args.control)
        print(f"TLC: negative control {control.name} (must violate {control.violates})")
        result, run = run_control(bundle, control, _tools(args))
        if run.timed_out:
            print(f"Error: {run.timeout_error}", file=sys.stderr)
            return EXIT_HARNESS_ERROR
        config_text = bundle.files[control.config]
        config = control.config
        name = control.name
    else:
        if not args.config:
            print("Error: --trace needs --config, the .cfg TLC ran with", file=sys.stderr)
            return EXIT_HARNESS_ERROR
        with open(args.trace, encoding="utf-8") as handle:
            result = read_tlc_output(handle.read())
        with open(args.config, encoding="utf-8") as handle:
            config_text = handle.read()
        config = _relative(bundle, args.config)
        name = os.path.splitext(os.path.basename(args.trace))[0]
        name = name[len(bundle.protocol) + 1 :] if name.startswith(f"{bundle.protocol}-") else name
    if not result.trace:
        print("Error: TLC produced no counterexample to translate", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    try:
        service = bundle_service(bundle, args.experiment_dirs)
    except Exception as exc:
        print(f"Error: the experiments do not load: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    try:
        translation = translate_trace(
            bundle,
            result.trace,
            config_text,
            name=name,
            violated=result.violated or result.violation,
            config=config,
            variants=args.variants,
            seed=args.seed,
            compile_scenario=service.compile,
        )
    except (TranslateError, SpecBundleError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    os.makedirs(args.out, exist_ok=True)
    for scenario in translation["scenarios"]:
        path = os.path.join(args.out, f"{scenario['scenario_id']}.yaml")
        header = (
            f"# Translated by `valkey-fuzzer spec translate` from the {name} counterexample of the\n"
            f"# {bundle.protocol} bundle ({config}: {translation['counterexample']['violated']}, "
            f"{translation['counterexample']['length']} states).\n"
        )
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(header + yaml.safe_dump(scenario, sort_keys=False))
        scenario_inputs = json.dumps(scenario["experiment"]["inputs"], sort_keys=True)
        print(f"  {path}: {scenario_inputs}")
    for item in translation["alignment"]:
        between = ", ".join(item["between"]) or "nothing"
        print(
            f"{item['fault']} ({item['component']}) at state {item['fault_state']} follows "
            f"{item['trigger']} #{item['occurrence']} "
            f"({item['trigger_event']}, state {item['trigger_state']}); model steps in between: {between}"
        )
    write_document(os.path.join(args.out, "translation.json"), translation)
    _write(args, translation)
    return EXIT_SUCCESS


def conform_command(args: argparse.Namespace) -> int:
    bundle = load_bundle(args.bundle)
    try:
        tools = _tools(args)
    except TlaError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    runs = collect_runs(args.runs)
    if not runs:
        print("Error: no runs found", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    group_by = [key for key in args.group_by.split(",") if key]
    report = conform_runs(bundle, runs, tools, group_by=group_by)
    for run in report["runs"]:
        checks = ", ".join(
            f"{name} {entry['status']}" + ("" if entry["claimed"] else " (outside the claim)")
            for name, entry in (run.get("properties") or {}).items()
        )
        reached = [name for name, entry in (run.get("targets") or {}).items() if entry["reached"]]
        print(
            f"[{run['status'].upper():<5}] {run.get('scenario_id') or run['source']}: {run['states']} observed states; "
            + (run.get("error") or checks)
            + (f"; reached {', '.join(reached)}" if reached else "")
        )
    coverage = report["action_coverage"]
    observed = ", ".join(f"{action} {count}/{coverage['runs']}" for action, count in coverage["observed"].items())
    print(f"\nLive action coverage (runs observing each action): {observed}")
    if coverage["unobservable"]:
        print(f"Unobservable: {', '.join(coverage['unobservable'])}")
    for row in report.get("groups", []):
        print("  " + ", ".join(f"{key}={value}" for key, value in row.items()))
    print(f"\nChecked: {report['checks']}. Refinement {report['refinement']}.")
    print(f"spec conform {report['status'].upper()}")
    _write(args, report)
    return {"pass": EXIT_SUCCESS, "fail": EXIT_FAILURE}.get(report["status"], EXIT_HARNESS_ERROR)
