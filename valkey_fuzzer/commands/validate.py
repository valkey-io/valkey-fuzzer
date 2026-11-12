"""``validate``: compile and statically check scenarios without provisioning anything.

Every file is validated with one catalog, so a batch of drafts loads the
experiments once. ``--format json`` prints, per file, ``ok`` and the errors that stop it
from compiling, each with the document path it is about (see
:func:`valkey_fuzzer.fields.located`) and the message; the exit code is
non-zero when any file fails.
"""

import argparse
import json
import sys
import traceback
from typing import Any, Optional

from ..outcomes import EXIT_HARNESS_ERROR, EXIT_SUCCESS
from ..plan import ExecutionPlan
from ..reports import InputError
from ..runner import RunnerError
from ..service import RunService
from ._common import add_common_options, print_header, service_for


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser("validate", help="Validate scenarios")
    parser.add_argument("files", nargs="+", metavar="FILE", help="Scenario YAML files")
    add_common_options(parser, valkey_binary=True)
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--verbose", action="store_true", help="Show the resolved component plan")
    parser.set_defaults(handler=validate_command)


def error_location(error: BaseException) -> str:
    """The dotted document path an error names; ``""`` for the whole document.

    Paths are relative to the scenario (``experiment.inputs.rate``,
    ``deployment.mode``); ``components.<id>...`` and
    ``startup_overlays.<id>...`` name the experiment's component or overlay
    whose resolved config was rejected.
    """

    path = getattr(error, "path", None) or ""
    return "" if path == "scenario" else path.removeprefix("scenario.")


# What an invalid scenario raises: it cannot be read (InputError), the compiler rejects it (every compile
# error is a ValueError), or the runner's static checks do (RunnerError, or a deployment provider's
# ValueError). Anything else is a bug, which cli.main reports.
INVALID_SCENARIO_ERRORS = (InputError, RunnerError, ValueError)


def _validate_file(
    service: RunService, path: str, binary: Optional[str]
) -> tuple[dict[str, Any], Optional[ExecutionPlan], Optional[Exception]]:
    try:
        plan = service.validate(path, valkey_binary=binary)
    except INVALID_SCENARIO_ERRORS as exc:
        row = {"file": path, "ok": False, "errors": [{"path": error_location(exc), "message": str(exc)}]}
        return row, None, exc
    row = {"file": path, "ok": True, "errors": [], "experiment": plan.experiment_id, "scenario_id": plan.scenario_id}
    return row, plan, None


def validate_command(args: argparse.Namespace) -> int:
    """Validate every file; print text or one JSON document, and fail if any file fails."""

    service = service_for(args)
    binary = args.valkey_binary
    json_output = args.format == "json"
    verbose = args.verbose
    rows = []
    for path in args.files:
        row, plan, error = _validate_file(service, path, binary)
        rows.append(row)
        if json_output:
            continue
        print_header(f"Validating scenario: {path}")
        if error is not None:
            print(f"Error: scenario validation failed: {error}", file=sys.stderr)
            if verbose:
                traceback.print_exception(type(error), error, error.__traceback__)
            continue
        print(
            "Scenario is valid without provisioning (what needs acquired artifacts or a server is checked at run time)"
        )
        _print_plan(plan, verbose=verbose)
    ok = all(row["ok"] for row in rows)
    if json_output:
        print(json.dumps({"ok": ok, "files": rows}, indent=2, sort_keys=True))
    elif len(rows) > 1:
        failed = [row["file"] for row in rows if not row["ok"]]
        print(
            f"\n{len(rows) - len(failed)}/{len(rows)} files passed"
            + (f"; failed: {', '.join(failed)}" if failed else "")
        )
    return EXIT_SUCCESS if ok else EXIT_HARNESS_ERROR


def _print_plan(plan: ExecutionPlan, *, verbose: bool = False) -> None:
    print("\n" + "=" * 60)
    print("Execution Plan")
    print("=" * 60)
    print(f"ID: {plan.scenario_id}")
    print(f"Experiment: {plan.experiment_id}")
    print(f"Mode: {plan.mode.value}")
    print(f"Deployment: {plan.deployment_mode}")
    print(f"Seed: {plan.seed}")
    print(f"Components: {len(plan.components)}")
    print(f"Startup Overlays: {len(plan.startup_overlays)}")
    print(f"Required Evidence: {len(plan.required_evidence)}")

    if verbose and plan.components:
        print("\nComponents:")
        for component in plan.components:
            dependencies = ", ".join(component.depends_on) if component.depends_on else "none"
            print(
                f"  - {component.id}: {component.role.value} "
                f"{component.use} (required={component.required}, depends_on={dependencies})"
            )
