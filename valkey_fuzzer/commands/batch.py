"""``batch``: run many scenarios on parallel, isolated workers (see :mod:`valkey_fuzzer.batch`)."""

import argparse
import functools
import os
from typing import Any

from ..batch import BatchError, BatchOptions, BatchRunner, expand, load_spec, normalize_source
from ..outcomes import EXIT_SUCCESS, exit_code
from ..reports import new_report_folder
from ._common import add_common_options, exclusion, positive_count, service_for


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "batch",
        help="Run many scenarios (random seeds, a matrix, files, the bug zoo, earlier failures) on parallel workers",
    )
    parser.add_argument("spec", nargs="?", help="Batch spec YAML (sources and options); flags add to it")
    sources = parser.add_argument_group("sources (each adds to the spec's)")
    sources.add_argument(
        "--random",
        dest="experiments",
        action="append",
        default=[],
        metavar="EXPERIMENT",
        help="Random seeds of this experiment, or 'all' that declare metadata.generator; may be repeated",
    )
    sources.add_argument("--seeds", type=positive_count, metavar="N", help="Seeds per experiment (default: profile's)")
    sources.add_argument("--first-seed", type=int, metavar="N", help="First seed (default: a fresh one)")
    sources.add_argument(
        "--exclude",
        dest="excludes",
        action="append",
        default=[],
        type=exclusion,
        metavar="PATH=VALUE",
        help="Never generate VALUE at PATH in a random job; may be repeated",
    )
    sources.add_argument("--base", metavar="FILE", help="Partial scenario merged over every random draw")
    sources.add_argument(
        "--scenarios", action="append", default=[], metavar="PATH", help="A scenario file or directory; may be repeated"
    )
    sources.add_argument("--zoo", metavar="BUILDS", help="The bug zoo's cases, on the builds zoo/build.sh made here")
    sources.add_argument(
        "--zoo-case",
        dest="zoo_cases",
        action="append",
        default=[],
        metavar="ID",
        help="Only this zoo case; may be repeated",
    )
    sources.add_argument("--failures", metavar="BATCH", help="The failed runs of an earlier batch folder")
    options = parser.add_argument_group("options (each overrides the spec's)")
    options.add_argument("--profile", help="Search profile: sampling and budgets (pr-quick, nightly-soak, ...)")
    options.add_argument("--workers", type=positive_count, help="Parallel workers (default: 1)")
    options.add_argument("--port-base", type=int, metavar="PORT", help="First client port of worker 0 (default 11000)")
    options.add_argument(
        "--serial-reruns", type=int, metavar="N", help="Re-run N runs of each failure signature serially"
    )
    options.add_argument(
        "--prune-pass",
        action="store_true",
        default=None,
        help="Keep only report.json and scenario.yaml of a passing run",
    )
    options.add_argument("--valkey-binary", help="valkey-server for every scenario that names none")
    options.add_argument("--batch-id", help="Name of the batch folder (default: batch)")
    folder = parser.add_argument_group("folder")
    folder.add_argument(
        "--out", metavar="DIR", help="Batch folder (default: <artifact dir>/batches/<time>-<id>-<suffix>)"
    )
    folder.add_argument(
        "--resume",
        metavar="DIR",
        help="Finish an interrupted batch (only --workers and --allow-unsupported may be given)",
    )
    folder.add_argument("--dry-run", action="store_true", help="List the jobs without running anything")
    add_common_options(parser, artifact_dir=True, timeout=True, allow_unsupported=True)
    parser.set_defaults(handler=functools.partial(batch_command, parser=parser))


def _cli_sources(args: argparse.Namespace) -> list[dict[str, Any]]:
    cwd = os.getcwd()
    sources = []
    if args.experiments:
        experiments = "all" if "all" in args.experiments else args.experiments
        random_source = {
            "experiments": experiments,
            "seeds": args.seeds,
            "first_seed": args.first_seed,
            "excludes": [str(item) for item in args.excludes],
        }
        if args.base:
            random_source["base"] = service_for(args).read_base(args.base)
        sources.append({"random": random_source})
    elif args.seeds or args.first_seed is not None or args.excludes or args.base:
        raise BatchError("--seeds, --first-seed, --exclude, and --base need --random")
    if args.scenarios:
        sources.append({"scenarios": args.scenarios})
    if args.zoo:
        sources.append({"zoo": {"builds": args.zoo, "cases": args.zoo_cases}})
    elif args.zoo_cases:
        raise BatchError("--zoo-case needs --zoo")
    if args.failures:
        sources.append({"failures": args.failures})
    return [normalize_source(source, cwd) for source in sources]


def _print_summary(summary: dict[str, Any]) -> None:
    print(f"\nBatch {summary['batch_id']}: {summary['finished']}/{summary['jobs']} jobs, {summary['counts']}")
    for group in summary["signatures"]:
        reruns = group.get("serial_reruns")
        rerun = f"; serial re-runs {reruns['reproduced']}/{len(reruns['runs'])} {reruns['verdict']}" if reruns else ""
        if reruns and reruns["verdict"] == "failed differently":
            other = {run["signature_label"] for run in reruns["runs"] if run["status"] != "PASS"}
            rerun += f": {' / '.join(sorted(other))}"
        print(f"  [{group['count']:>4}] {group['label']}{rerun}")
        print(f"         e.g. runs/{group['runs'][0]}")
    zoo = summary["zoo"]
    if zoo is not None:
        for case in zoo["cases"]:
            if case.get("pending"):
                print(f"  zoo {case['case']}: not finished")
                continue
            rates = ", ".join(f"{file} {item['detection_rate']:.0%}" for file, item in case["scenarios"].items())
            controls = "/".join(row["status"] for row in case["controls"])
            verdict = case["undecided"] or ("OK" if case["holds"] else "FAIL")
            print(f"  zoo {verdict:<5} {case['case']}: control {controls}; detected {rates}")
        for item in zoo["skipped"]:
            print(f"  zoo SKIP {item['case']}: {item['reason']}")
    if summary["leaks"]:
        print(f"  Leaks: {len(summary['leaks'])}")
        for leak in summary["leaks"][:20]:
            print(f"    {leak['run']}: {leak['kind']} {leak['target']} ({leak['action']})")
    if summary["invalid"]:
        print(f"  Matrix cases that do not compile (not run): {len(summary['invalid'])}")
    print(f"Status: {summary['status']}{'' if summary['complete'] else ' (interrupted: resume to finish)'}")
    print(f"Batch: {summary['folder']}")


# What a resume may set: the batch folder holds every other option.
RESUME_OPTIONS = ("resume", "workers", "allow_unsupported")


def _not_resumable(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[str]:
    """Every option but :data:`RESUME_OPTIONS` that ``args`` sets, as the command line names it."""

    return [
        action.option_strings[0] if action.option_strings else action.dest.upper()
        for action in parser._actions
        if action.dest not in RESUME_OPTIONS and getattr(args, action.dest, action.default) != action.default
    ]


def batch_command(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Create or resume a batch, run it, print its summary, and exit with its worst result."""

    if args.resume:
        given = _not_resumable(args, parser)
        if given:
            raise BatchError(
                f"--resume takes only --workers and --allow-unsupported, not {', '.join(given)}: "
                "the batch folder holds its sources and options"
            )
        runner = BatchRunner(args.resume)
        runner.check_draws()
        if args.workers:
            runner.options.workers = args.workers
            runner.options.check()
    else:
        service = service_for(args)
        settings, sources = load_spec(args.spec) if args.spec else ({}, [])
        sources += _cli_sources(args)
        if not sources:
            raise BatchError("name at least one source: a spec, --random, --scenarios, --zoo, or --failures")
        for name in ("profile", "workers", "port_base", "serial_reruns", "prune_pass", "batch_id"):
            if getattr(args, name) is not None:
                settings[name] = getattr(args, name)
        if args.timeout is not None:
            settings["timeout_s"] = args.timeout
        if args.valkey_binary:
            settings["valkey_binary"] = os.path.abspath(args.valkey_binary)
        options = BatchOptions(
            **settings,
            experiment_directories=tuple(os.path.abspath(path) for path in args.experiment_dirs),
        )
        options.check()
        expansion = expand(sources, service, options)
        if args.dry_run:
            for job in expansion.jobs:
                print(f"{job['id']}: {job.get('label', '')}")
            for case in expansion.invalid:
                print(f"invalid {case['case']}: {case['error']}")
            for item in (expansion.zoo or {}).get("skipped", ()):
                print(f"skipped zoo case {item['case']}: {item['reason']}")
            print(f"{len(expansion.jobs)} job(s)")
            return EXIT_SUCCESS
        folder = args.out or new_report_folder(os.path.join(service.report_root, "batches"), options.batch_id)
        runner = BatchRunner.create(folder, options, sources, expansion)
        print(f"Batch: {runner.folder} ({len(runner.jobs)} jobs, {options.workers} worker(s))")
    summary = runner.run()
    _print_summary(summary)
    if not summary["complete"]:
        return summary["exit_code"]
    return exit_code(summary["status"], allow_unsupported=args.allow_unsupported)
