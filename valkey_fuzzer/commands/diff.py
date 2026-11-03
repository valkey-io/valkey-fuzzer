"""``diff``: one seeded scenario under several variants, with their evidence compared."""

import argparse
import json
import sys
from typing import Any

from ..outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, exit_code
from ..reports import ReportWriteError, write_document
from ..results import RunOutcome
from ._common import add_common_options, service_for


def register(subparsers: Any) -> None:
    diff = subparsers.add_parser("diff", help="Run one seeded scenario under several variants and compare evidence")
    diff.add_argument("file", help="Path to a differential YAML document")
    diff.add_argument("--seed", type=int, help="Override the base scenario seed")
    diff.add_argument("--output", help="Path to save the differential report")
    diff.add_argument("--verbose", action="store_true")
    add_common_options(diff, valkey_binary=True, artifact_dir=True, timeout=True, allow_unsupported=True)
    diff.set_defaults(handler=diff_command)


def _short(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= 20 else f"{text[:16]}..."


def diff_command(args: argparse.Namespace) -> int:
    """Run a differential document and print each comparison."""

    def on_variant(name: str, outcome: RunOutcome) -> None:
        print(f"[{outcome.status.value:<16}] variant {name}: {outcome.summary}")

    result = service_for(args).differential(
        args.file,
        seed=args.seed,
        valkey_binary=args.valkey_binary,
        timeout=args.timeout,
        on_variant=on_variant,
    )
    report = result.report
    print(f"\nDifferential {report['differential_id']} (seed {report['seed']}): {report['status']}")
    print(f"Summary: {report['summary']}")
    for comparison in report["comparisons"]:
        # A missing value is a comparison the harness could not make, not a finding.
        marker = {"ok": "ok  ", "missing": "ERR "}.get(comparison["result"], "FAIL")
        values = ", ".join(f"{name}={_short(value)}" for name, value in comparison["values"].items())
        print(f"  [{marker}] {comparison['kind']:<6} {comparison['rule']}: {values}")
    print(f"Reports: {result.report_dir}")
    code = exit_code(report["status"], allow_unsupported=args.allow_unsupported)
    if args.output:
        try:
            write_document(args.output, report)
        except ReportWriteError as exc:
            # A copy that cannot be written is an ERROR, unless the differential already FAILed.
            print(f"Error: {exc}", file=sys.stderr)
            return code if code == EXIT_FAILURE else EXIT_HARNESS_ERROR
    return code
