"""``sweep``: remove the servers, chains, mounts, and directories of fuzzer processes that are gone.

See :mod:`valkey_fuzzer.hoststate` for the tags it finds them by. The exit code
is 2, a harness error, when anything found could not be removed (a chain or mount without
root, say).
"""

import argparse
import json
from typing import Any

from ..hoststate import default_roots, sweep
from ..outcomes import EXIT_HARNESS_ERROR, EXIT_SUCCESS
from ._common import positive_seconds


def register(subparsers: Any) -> None:
    parser = subparsers.add_parser(
        "sweep", help="Remove what killed fuzzer runs left on this host (servers, chains, mounts, data directories)"
    )
    parser.add_argument("--dry-run", action="store_true", help="List the leftovers without removing them")
    parser.add_argument(
        "--older-than",
        type=positive_seconds,
        metavar="SECONDS",
        help="Only leftovers at least this old; one whose age is unknown (an iptables chain) is left alone",
    )
    parser.add_argument(
        "--root",
        dest="roots",
        action="append",
        metavar="DIR",
        help="Look for tagged directories in DIR instead of the defaults "
        f"({', '.join(default_roots())}); may be repeated",
    )
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.set_defaults(handler=sweep_command)


def sweep_command(args: argparse.Namespace) -> int:
    report = sweep(args.roots, older_than=args.older_than, dry_run=args.dry_run)
    if args.format == "json":
        print(json.dumps(report.to_data(), indent=2, sort_keys=True))
    else:
        for item in report.leftovers:
            age = "age unknown" if item.age_s is None else f"{item.age_s:.0f}s old"
            reason = f": {item.reason}" if item.reason else ""
            print(f"{item.action:<12} {item.kind:<9} {item.target} (owner {item.owner}, {age}){reason}")
        for note in report.unchecked:
            print(f"not checked  {note}")
        if not report.leftovers:
            print("No leftovers of fuzzer processes that are gone")
    return EXIT_SUCCESS if report.ok else EXIT_HARNESS_ERROR
