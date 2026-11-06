"""The Valkey components, providers, and experiments.

:func:`valkey_fuzzer.catalog.register_valkey_components` registers what this
package lists. Its experiments are the data-only YAML documents under
``experiments/``; they extend the fragments under ``fragments/``
(``valkey.writes``, ``valkey.base``, the procedures they share, such as
``valkey.psync``), and every experiment extends ``valkey.liveness``, which adds
the ``valkey.oracle.liveness`` oracle. Data-only experiments elsewhere can
``extends`` the same fragments.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from valkey_fuzzer.catalog import load_experiment_yaml
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.schema import load_yaml_mapping

from . import (
    actions,
    capabilities,
    cluster_chaos,
    cluster_election,
    cluster_trace,
    command_table,
    compression,
    election_safety,
    faults,
    history,
    ledger,
    network,
    observers,
    oracles,
    replication_chaos,
    snapshots,
    storage,
    tools,
    workloads,
)
from .generators import GENERATORS
from .providers import ENVIRONMENT_PROVIDERS

EXPERIMENT_DIRECTORY = Path(__file__).resolve().parent / "experiments"
FRAGMENT_DIRECTORY = Path(__file__).resolve().parent / "fragments"
# The component modules, in registration order.
_MODULES = (
    capabilities,
    storage,
    workloads,
    command_table,
    actions,
    tools,
    cluster_chaos,
    faults,
    network,
    observers,
    oracles,
    ledger,
    history,
    snapshots,
    compression,
    cluster_election,
    cluster_trace,
    election_safety,
    replication_chaos,
)


def component_registrations() -> tuple[ComponentRegistration[Any], ...]:
    """Every module's ``REGISTRATIONS``, in module order."""

    return tuple(registration for module in _MODULES for registration in module.REGISTRATIONS)


def experiment_fragments() -> dict[str, Any]:
    """``{name: fragment}`` for every ``fragments/<name>.yaml``."""

    return {
        path.stem: load_yaml_mapping(path.read_text(encoding="utf-8"), source_name=str(path))
        for path in sorted(FRAGMENT_DIRECTORY.glob("*.yaml"))
    }


def experiments(fragments: Optional[Mapping[str, Any]] = None) -> tuple[Any, ...]:
    """Every ``experiments/<id>.yaml``, extending ``fragments`` (default: :func:`experiment_fragments`)."""

    fragments = experiment_fragments() if fragments is None else fragments
    return tuple(
        load_experiment_yaml(
            path.read_text(encoding="utf-8"),
            source_name=str(path),
            fragments=fragments,
            default_extends=("valkey.liveness",),
        )
        for path in sorted(EXPERIMENT_DIRECTORY.glob("*.yaml"))
    )


__all__ = [
    "ENVIRONMENT_PROVIDERS",
    "EXPERIMENT_DIRECTORY",
    "FRAGMENT_DIRECTORY",
    "GENERATORS",
    "component_registrations",
    "experiment_fragments",
    "experiments",
]
