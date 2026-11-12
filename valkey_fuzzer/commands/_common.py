"""What every command shares: the common options, the run service, and terminal output."""

import argparse
import json
import math
import re
from collections.abc import Mapping
from typing import Any

from ..generator import Exclusion, GeneratorError
from ..reports import InputError
from ..service import RunService

# ---------------------------------------------------------------------------
# Common options and the service
# ---------------------------------------------------------------------------


def positive_seconds(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number greater than zero") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return seconds


def positive_count(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a whole number of at least 1") from exc
    if count < 1:
        raise argparse.ArgumentTypeError("must be a whole number of at least 1")
    return count


def exclusion(text: str) -> Exclusion:
    """A ``--exclude PATH=VALUE`` argument."""

    try:
        return Exclusion.parse(text)
    except GeneratorError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def artifact_path(text: str) -> tuple[str, str]:
    """A ``--artifact NAME=PATH`` argument."""

    name, separator, path = text.partition("=")
    if not separator or not name or not path:
        raise argparse.ArgumentTypeError("must be NAME=PATH")
    return name, path


def artifact_paths(args: argparse.Namespace) -> dict[str, str]:
    """The ``--artifact`` overrides by artifact name; a name given twice is an error."""

    paths: dict[str, str] = {}
    for name, path in args.artifacts:
        if name in paths:
            raise InputError(f"--artifact {name} is given twice")
        paths[name] = path
    return paths


def add_common_options(
    parser: argparse.ArgumentParser,
    *,
    valkey_binary: bool = False,
    artifact: bool = False,
    artifact_dir: bool = False,
    timeout: bool = False,
    allow_unsupported: bool = False,
) -> None:
    """Register ``--experiment-dir``, and the other shared options a command uses."""

    if valkey_binary:
        parser.add_argument("--valkey-binary", help="Override the Valkey binary used by the deployment")
    if artifact:
        parser.add_argument(
            "--artifact",
            dest="artifacts",
            action="append",
            default=[],
            type=artifact_path,
            metavar="NAME=PATH",
            help="Resolve the valkey.artifact.local-binary artifact NAME at PATH instead of the recorded path; "
            "may be repeated",
        )
    parser.add_argument(
        "--experiment-dir",
        dest="experiment_dirs",
        action="append",
        default=[],
        metavar="DIR",
        help="Directory containing data-only experiment documents; may be repeated",
    )
    if artifact_dir:
        parser.add_argument(
            "--artifact-dir",
            default="",
            metavar="DIR",
            help="Root for report folders, which hold each run's component artifacts, and provider directories "
            "(default: <tmp>/valkey-fuzzer)",
        )
    if timeout:
        parser.add_argument(
            "--timeout",
            type=positive_seconds,
            metavar="SECONDS",
            help="Cooperative run deadline in seconds",
        )
    if allow_unsupported:
        parser.add_argument(
            "--allow-unsupported",
            action="store_true",
            help="Return success when a required capability reports unsupported",
        )


def service_for(args: argparse.Namespace) -> RunService:
    """A run service for the options in ``args``; its catalog is built on first use."""

    return RunService(
        experiment_directories=tuple(args.experiment_dirs),
        # Commands without --artifact-dir use the default root.
        artifact_dir=getattr(args, "artifact_dir", ""),
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_header(title: str) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)
    print()


def describe_drift(entry: Mapping[str, Any]) -> str:
    """One terminal line for a replay drift entry."""

    def shown(value: Any) -> str:
        text = "none" if value is None else value if isinstance(value, str) else json.dumps(value, sort_keys=True)
        # Reports keep full digests; a prefix is enough on the terminal.
        return re.sub(r"\b([0-9a-f]{12})[0-9a-f]{52}\b", r"\1...", text)

    return f"{entry['item']}: recorded {shown(entry['recorded'])}, replay uses {shown(entry['replay'])}"
