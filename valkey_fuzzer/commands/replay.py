"""``replay``: re-execute a recorded run, refusing (or recording) drift from its provenance."""

import argparse
import sys
from typing import Any

from ..outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, exit_code
from ..reports import ReportWriteError, write_document
from ..service import ReplayDriftError
from ._common import add_common_options, describe_drift, service_for


def register(subparsers: Any) -> None:
    replay = subparsers.add_parser("replay", help="Re-execute a recorded run (report.json or plan.json)")
    replay.add_argument("plan", help="Path to a run report (report.json or a --output copy) or a plan.json")
    replay.add_argument("--output", help="Path to save the replay report")
    replay.add_argument(
        "--allow-drift",
        action="store_true",
        help="Replay even if the fuzzer, experiment, components, or Valkey binary differ from the "
        "recorded run; the differences are printed and recorded in the replay report",
    )
    replay.add_argument("--verbose", action="store_true")
    add_common_options(replay, valkey_binary=True, artifact_dir=True, timeout=True, allow_unsupported=True)
    replay.set_defaults(handler=replay_command)


def replay_command(args: argparse.Namespace) -> int:
    """Check a recorded run against this environment, then run its plan again."""

    service = service_for(args)
    try:
        prepared = service.prepare_replay(args.plan, valkey_binary=args.valkey_binary, allow_drift=args.allow_drift)
    except ReplayDriftError as exc:
        print("Error: refusing to replay because this environment differs from the recorded run:", file=sys.stderr)
        for entry in exc.drift:
            print(f"  - {describe_drift(entry)}", file=sys.stderr)
        print("Pass --allow-drift to replay anyway, or --valkey-binary to choose a binary on purpose.", file=sys.stderr)
        return EXIT_HARNESS_ERROR
    if not prepared.verified:
        print(
            f"Warning: {args.plan} records no provenance, so the replay cannot be verified against the original run",
            file=sys.stderr,
        )
    for entry in prepared.drift:
        print(f"Warning: replaying despite drift: {describe_drift(entry)}", file=sys.stderr)
    for note in prepared.not_verified:
        print(f"Note: not verified: {note}")
    if args.valkey_binary:
        # An intentional override: recorded in the report, not treated as drift.
        print(f"Using --valkey-binary {args.valkey_binary} instead of the recorded default binary")
    plan = prepared.plan
    print(f"Replaying {plan.scenario_id} ({plan.experiment_id}, seed {plan.seed})")

    result = service.replay(prepared, timeout=args.timeout)
    record = result.record
    print(f"Status: {record.outcome.status.value}")
    print(f"Summary: {record.outcome.summary}")
    print(f"Report: {record.report_path}")
    code = exit_code(record.outcome.status, allow_unsupported=args.allow_unsupported)
    if result.late_drift and not args.allow_drift:
        print(
            "Error: the replay ran a different Valkey binary or artifact than the recorded run, "
            "so it does not reproduce it:",
            file=sys.stderr,
        )
        for entry in result.late_drift:
            print(f"  - {describe_drift(entry)}", file=sys.stderr)
        print("Pass --allow-drift to accept such a replay.", file=sys.stderr)
        code = EXIT_HARNESS_ERROR
    if args.output:
        try:
            write_document(args.output, record.report)
        except ReportWriteError as exc:
            # A copy that cannot be written is an ERROR, unless the replay already FAILed.
            print(f"Error: {exc}", file=sys.stderr)
            return code if code == EXIT_FAILURE else EXIT_HARNESS_ERROR
    return code
