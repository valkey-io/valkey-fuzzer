"""``list``: the experiments, components, and generator hooks the run service's catalog holds.

``--format json`` prints the machine-readable catalog of
:mod:`valkey_fuzzer.describe`, with sorted keys so the output is stable.
"""

import argparse
import json
from typing import Any

from ..describe import describe_components, describe_experiments, describe_generators
from ..outcomes import EXIT_SUCCESS
from ._common import add_common_options, service_for


def register(subparsers: Any) -> None:
    list_parser = subparsers.add_parser("list", help="List installed experiments, components, or generators")
    list_parser.add_argument("what", choices=["experiments", "components", "generators"])
    list_parser.add_argument("--format", choices=["text", "json"], default="text")
    add_common_options(list_parser)
    list_parser.set_defaults(handler=list_command)


def list_command(args: argparse.Namespace) -> int:
    """Print the catalog's experiments, components, or generators as text or JSON."""

    catalog = service_for(args).catalog
    if args.what == "experiments":
        rows = describe_experiments(catalog)
        lines = [f"{row['id']} [{', '.join(row['deployment_modes'])}] {row['description']}" for row in rows]
    elif args.what == "components":
        rows = describe_components(catalog)
        lines = [f"{row['role']:<10} {row['component_id']:<28} {row['implementation']}" for row in rows]
    else:
        rows = describe_generators(catalog)
        lines = [f"{row['generator_id']:<42} {row['implementation']}" for row in rows]
    if args.format == "json":
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        for line in lines:
            print(line)
    return EXIT_SUCCESS
