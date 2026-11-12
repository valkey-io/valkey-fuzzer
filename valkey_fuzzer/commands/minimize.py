"""``minimize``: shrink a failing run's scenario while its failure signature holds."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml

from ..outcomes import EXIT_HARNESS_ERROR, EXIT_SUCCESS
from ..service import ReplayDriftError
from ._common import add_common_options, describe_drift, positive_count, service_for


def register(subparsers: Any) -> None:
    minimize = subparsers.add_parser("minimize", help="Shrink a failing run while preserving the failure")
    minimize.add_argument("report", help="Run report (report.json or a --output copy) of the failing run")
    minimize.add_argument("--output", help="Write the minimized scenario YAML here")
    minimize.add_argument("--max-attempts", type=positive_count, default=30, help="Candidates to run (default 30)")
    minimize.add_argument(
        "--allow-drift",
        action="store_true",
        help="Minimize even if the fuzzer, experiment, or Valkey binary differ from the recorded run",
    )
    minimize.add_argument("--verbose", action="store_true")
    add_common_options(minimize, valkey_binary=True, artifact_dir=True, timeout=True)
    minimize.set_defaults(handler=minimize_command)


def minimize_command(args: argparse.Namespace) -> int:
    """Shrink a failing run's scenario while the same failure persists, and print or write the result."""

    service = service_for(args)
    try:
        prepared = service.prepare_minimization(
            args.report, valkey_binary=args.valkey_binary, allow_drift=args.allow_drift
        )
    except ReplayDriftError as exc:
        print("Error: refusing to minimize because this environment differs from the recorded run:", file=sys.stderr)
        for entry in exc.drift:
            print(f"  - {describe_drift(entry)}", file=sys.stderr)
        print(
            "Pass --allow-drift to minimize anyway, or --valkey-binary to choose a binary on purpose.", file=sys.stderr
        )
        return EXIT_HARNESS_ERROR
    if not prepared.needed:
        print(f"Nothing to minimize: the run's status is {prepared.reference['status']}")
        return EXIT_SUCCESS
    print(f"Minimizing {prepared.plan.scenario_id}: signature {json.dumps(prepared.reference)}")
    result = service.minimize(prepared, timeout=args.timeout, max_attempts=args.max_attempts, log=print)
    print(f"Accepted reductions: {', '.join(result.accepted) or 'none'}")
    if result.report_dir:
        print(f"Reports: {result.report_dir}")
    text = yaml.safe_dump(result.scenario, sort_keys=False)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
        print(f"Minimized scenario written to {args.output}")
    else:
        print(text)
    return EXIT_SUCCESS
