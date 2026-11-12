#!/usr/bin/env python3
"""Command-line interface: build the parser from the command modules and dispatch to a handler."""

import argparse
import logging
import sys
import traceback
from typing import Any

from . import __version__
from .commands import batch, diff, listing, minimize, replay, run, spec, sweep, validate
from .hoststate import interrupt_on_sigterm
from .outcomes import EXIT_HARNESS_ERROR, EXIT_INTERRUPTED

COMMAND_MODULES = (run, batch, validate, listing, replay, diff, minimize, sweep, spec)


def register_commands(subparsers: Any) -> None:
    """Add every command's subparser to the main parser's ``subparsers``."""

    for module in COMMAND_MODULES:
        module.register(subparsers)


def create_parser() -> argparse.ArgumentParser:
    """Create the public command-line parser."""

    parser = argparse.ArgumentParser(
        prog="valkey-fuzzer",
        description="Generic, experiment-driven fuzzing for standalone and cluster Valkey deployments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples (paths are relative to a checkout of the repository):
  # Experiments
  valkey-fuzzer list experiments
  valkey-fuzzer validate examples/valkey/aof_durability.yaml
  valkey-fuzzer run examples/valkey/aof_durability.yaml --output report.json
  valkey-fuzzer run examples/valkey/cluster_chaos.yaml --timeout 300 --verbose
  valkey-fuzzer replay report.json
  valkey-fuzzer minimize report.json --output minimized.yaml
  valkey-fuzzer diff examples/valkey/differential_baseline_vs_candidate.yaml

  # One random scenario of an experiment that declares metadata.generator
  valkey-fuzzer run --random
  valkey-fuzzer run --random --experiment valkey.cluster-chaos
  valkey-fuzzer run --seed 42 --experiment valkey.aof-durability --profile pr-quick

  # Many runs: random seeds, a matrix, scenario files, the bug zoo, earlier failures
  valkey-fuzzer batch --random all --seeds 5 --profile pr-quick --workers 4
  valkey-fuzzer batch examples/valkey/cluster_election_batch.yaml --workers 2
  valkey-fuzzer batch --zoo BUILDS --workers 2
  valkey-fuzzer batch --resume BATCH_FOLDER

  # Data-only experiments from a directory
  valkey-fuzzer run examples/standalone_scenario.yaml --experiment-dir examples/experiments

  # Remove a seed's instance logs
  valkey-fuzzer cleanup-logs 42 --force

  # Remove what killed runs left behind (servers, iptables chains, mounts, data directories)
  valkey-fuzzer sweep --dry-run

Exit codes: 0 PASS, 1 FAIL, 2 ERROR, 3 SKIP_UNSUPPORTED, 130 interrupted.
Documentation: README.md and docs/cli.md.
        """,
    )
    parser.add_argument("--version", action="version", version=f"Valkey Fuzzer {__version__}")
    # Commands without --verbose still answer args.verbose.
    parser.set_defaults(verbose=False)
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    register_commands(subparsers)
    return parser


def main() -> int:
    """CLI entry point: every command's errors are reported here, as harness errors (exit 2)."""

    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()])
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        print("Error: name a command, as below\n", file=sys.stderr)
        parser.print_help(sys.stderr)
        return EXIT_HARNESS_ERROR

    try:
        # SIGTERM takes Ctrl-C's path: the run cleans up, and the command exits 130.
        with interrupt_on_sigterm():
            return args.handler(args)
    except KeyboardInterrupt:
        print("\n\nValkey Fuzzer process was interrupted by user")
        return EXIT_INTERRUPTED
    except Exception as exc:
        print(f"Error: {_message(exc)}", file=sys.stderr)
        if args.verbose:
            traceback.print_exc()
        return EXIT_HARNESS_ERROR


def _message(error: Exception) -> str:
    """What the error says, with its type when that says nothing (no message, or a KeyError's bare key)."""

    message = str(error)
    if not message:
        return type(error).__name__
    if type(error).__str__ is KeyError.__str__:
        return f"{type(error).__name__}: {message}"
    return message


if __name__ == "__main__":
    sys.exit(main())
