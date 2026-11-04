"""``run`` and ``cleanup-logs``.

``run`` executes one scenario file, or with ``--random``/``--seed`` one
scenario generated for any experiment that declares ``metadata.generator``
(see :mod:`valkey_fuzzer.generator`), ``valkey.cluster-chaos`` included; the PR
fuzzer workflow runs it. Many runs go through ``batch``. ``cleanup-logs``
removes the local deployment's logs for a seed. The run leaves a report folder
under ``runs/``, and ``--output`` saves a copy of its run report.
"""

import argparse
import glob
import os
import shlex
import sys
from typing import Any

from ..deployment.local.models import DEFAULT_BASE_DATA_DIR
from ..outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, EXIT_SUCCESS, exit_code, print_outcome
from ..reports import SCENARIO_FILE, ReportWriteError, export_scenario, write_document
from ..service import PreparedRun
from ._common import add_common_options, artifact_paths, exclusion, print_header, service_for


def register(subparsers: Any) -> None:
    run_parser = subparsers.add_parser("run", help="Compile and run a scenario, or generated ones")
    run_parser.add_argument("file", nargs="?", help="Path to the scenario YAML file (omit with --random or --seed)")
    add_common_options(
        run_parser, valkey_binary=True, artifact=True, artifact_dir=True, timeout=True, allow_unsupported=True
    )
    generated = run_parser.add_argument_group("generated scenarios")
    generated.add_argument("--random", action="store_true", help="Run a scenario generated from a random seed")
    generated.add_argument("--seed", type=int, help="Run the scenario generated from this seed")
    generated.add_argument(
        "--experiment", metavar="ID", help="Experiment to generate for (default: drawn from those that opt in)"
    )
    generated.add_argument(
        "--exclude",
        dest="excludes",
        action="append",
        default=[],
        type=exclusion,
        metavar="PATH=VALUE",
        help="Never generate VALUE at PATH (inputs.<name>, deployment.<field>, server_config.<key>, or "
        "experiment); may be repeated",
    )
    generated.add_argument(
        "--base", metavar="FILE", help="Partial scenario whose fields override the draws (a second build, say)"
    )
    generated.add_argument(
        "--profile", metavar="NAME", help="Search profile that samples the experiment's legal space (pr-quick, ...)"
    )
    generated.add_argument(
        "--export-scenario", metavar="FILE", help="Write the generated scenario to FILE; `run FILE` reruns it"
    )
    run_parser.add_argument("--output", help="Path to save a copy of the run report")
    run_parser.add_argument(
        "--output-format", choices=["json", "yaml"], help="Format of the --output copy (default: json)"
    )
    run_parser.add_argument("--verbose", action="store_true", help="Show component-level details")
    run_parser.set_defaults(handler=run_command)

    cleanup_parser = subparsers.add_parser("cleanup-logs", help="Clean up log files for a seed")
    cleanup_parser.add_argument("seed", type=int, help="Seed whose logs should be removed")
    cleanup_parser.add_argument(
        "--data-dir",
        default=DEFAULT_BASE_DATA_DIR,
        help="Base data directory (default: %(default)s)",
    )
    cleanup_parser.add_argument("--force", action="store_true", help="Skip confirmation")
    cleanup_parser.set_defaults(handler=cleanup_logs_command)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def run_command(args: argparse.Namespace) -> int:
    """Compile and execute one scenario (``valkey-fuzzer run``), or a generated one with ``--random``/``--seed``."""

    generating = args.random or args.seed is not None
    misplaced = [
        flag
        for flag, value in (
            ("--experiment", args.experiment),
            ("--exclude", args.excludes),
            ("--base", args.base),
            ("--profile", args.profile),
            ("--export-scenario", args.export_scenario),
        )
        if value
    ]
    if args.output_format and not args.output:
        reason = "--output-format needs --output"
    elif generating and args.file:
        reason = "give a scenario file or --random/--seed, not both"
    elif not generating and misplaced:
        reason = f"{', '.join(misplaced)} need --random or --seed"
    elif generating and args.artifacts:
        reason = "--artifact needs a scenario file"
    elif not generating and not args.file:
        reason = "run needs a scenario file"
    else:
        reason = None
    if reason is not None:
        print(f"Error: {reason}\n", file=sys.stderr)
        print("Examples:", file=sys.stderr)
        print("  valkey-fuzzer run scenario.yaml", file=sys.stderr)
        print("  valkey-fuzzer run --random", file=sys.stderr)
        print("  valkey-fuzzer run --seed 42 --experiment valkey.aof-durability", file=sys.stderr)
        return EXIT_HARNESS_ERROR

    service = service_for(args)
    if generating:
        base = service.read_base(args.base) if args.base else None
        prepared = service.prepare_random(
            seed=args.seed,
            experiment=args.experiment,
            excludes=args.excludes,
            base=base,
            valkey_binary=args.valkey_binary,
            profile=args.profile,
        )
        generated = prepared.generated
        print_header(
            f"Generated scenario: {generated['experiment']}, seed {generated['seed']} (attempt {generated['attempt']})"
        )
        for note in prepared.notes:
            print(f"Note: {note}")
    else:
        print_header(f"Scenario: {args.file}")
        # The report records the scenario as run, including a --valkey-binary
        # or --artifact override, so it replays and minimizes on its own.
        prepared = service.prepare_run(args.file, valkey_binary=args.valkey_binary, artifacts=artifact_paths(args))
    record = service.execute(
        prepared.plan, scenario=prepared.scenario, timeout=args.timeout, generated=prepared.generated
    )
    print_outcome(record.outcome, verbose=args.verbose)
    print(f"Report: {record.report_path}")
    if generating:
        print(f"Reproduce: {_reproduce_command(prepared, args)}")
        print(f"  or, without the generator: valkey-fuzzer run {record.report_dir}/{SCENARIO_FILE}")
    code = exit_code(record.outcome.status, allow_unsupported=args.allow_unsupported)
    try:
        if args.output:
            write_document(args.output, record.report, args.output_format or "json")
            print(f"\nResults saved to {args.output}")
        if args.export_scenario:
            export_scenario(prepared.scenario, args.export_scenario, generated=prepared.generated)
            print(f"\nScenario exported to {args.export_scenario}")
    except ReportWriteError as exc:
        # A file that cannot be written is an ERROR, unless the run already FAILed.
        print(f"\nError: {exc}", file=sys.stderr)
        return code if code == EXIT_FAILURE else EXIT_HARNESS_ERROR
    return code


def _reproduce_command(prepared: PreparedRun, args: argparse.Namespace) -> str:
    """A shell command that draws the same scenario: every option a draw depends on, quoted."""

    generated = prepared.generated
    command = ["valkey-fuzzer", "run", "--seed", str(generated["seed"]), "--experiment", generated["experiment"]]
    for item in generated.get("excludes", ()):
        command += ["--exclude", str(item)]
    if generated.get("profile"):
        command += ["--profile", generated["profile"]]
    if args.base:
        command += ["--base", args.base]
    # Hooks see the binary (rolling-upgrade names the second build after it), and the catalog decides what exists.
    if args.valkey_binary:
        command += ["--valkey-binary", args.valkey_binary]
    for directory in args.experiment_dirs:
        command += ["--experiment-dir", directory]
    return shlex.join(command)


# ---------------------------------------------------------------------------
# cleanup-logs
# ---------------------------------------------------------------------------


def cleanup_logs(seed: int, base_data_dir: str, force: bool = False) -> int:
    """Delete ``<seed>-*.log`` under ``base_data_dir/logs``, asking first unless ``force``.

    A cancelled prompt or a file that could not be deleted is a harness error
    (exit 2): a script that cleans up after a seed must not take either for
    done.
    """

    log_files = sorted(glob.glob(os.path.join(base_data_dir, "logs", f"{seed}-*.log")))
    if not log_files:
        print(f"No log files found for seed {seed}")
        return EXIT_SUCCESS
    for log_file in log_files:
        print(f"  {os.path.basename(log_file)}")
    if not force and input(f"\nDelete these {len(log_files)} log file(s)? [y/N]: ").lower() != "y":
        print("Cancelled; nothing deleted")
        return EXIT_HARNESS_ERROR

    failed = 0
    for log_file in log_files:
        try:
            os.remove(log_file)
        except OSError as exc:
            failed += 1
            print(f"Error: could not delete {log_file}: {exc}", file=sys.stderr)
    print(f"Deleted {len(log_files) - failed} log file(s) for seed {seed}")
    return EXIT_HARNESS_ERROR if failed else EXIT_SUCCESS


def cleanup_logs_command(args: argparse.Namespace) -> int:
    """Remove the local deployment's log files for a seed (``valkey-fuzzer cleanup-logs``)."""

    return cleanup_logs(args.seed, args.data_dir, args.force)
