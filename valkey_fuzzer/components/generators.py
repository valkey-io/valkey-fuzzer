"""Generator hooks for random scenarios (``metadata.generator`` in the experiments).

Each hook is a :class:`~valkey_fuzzer.registrations.GeneratorRegistration`
``hook(rng, scenario)``; ``scenario`` is a copy of what has been drawn so far.

``valkey.generator.waves``
    The ``waves`` of ``valkey.cluster-chaos`` and ``valkey.replication-chaos``
    (:func:`waves`), drawn from the operations of the deployment's mode.
``valkey.generator.tmpfs-data``
    Puts the primary's data on a size-limited tmpfs and passes its path as the
    ``disk`` input (``valkey.bgsave-disk-full``).
``valkey.generator.upgrade-target``
    Names the deployment's own build as the second binary
    ``valkey.rolling-upgrade`` needs (``to_binary``), unless the scenario
    already sets it. The experiment's probes then make the run ERROR until a
    base scenario names a second build.
``valkey.generator.wait-replicas``
    ``wait_replicas`` from 0 (no WAIT) to the drawn ``replicas_per_primary``.
``valkey.generator.aof-loss-window``
    The least crash loss window the drawn ``appendfsync`` allows
    (:func:`aof_loss_window`).
"""

import random
from collections.abc import Mapping
from typing import Any

from valkey_fuzzer.registrations import GeneratorRegistration
from valkey_fuzzer.schema import plain_config

from .ledger import EVERYSEC_LOSS_WINDOW_S


def _round(value: float) -> float:
    return round(value, 2)


def _seconds(rng: random.Random, low: float, high: float) -> float:
    """A duration drawn uniformly from ``low`` to ``high``, to the hundredth of a second."""

    return _round(rng.uniform(low, high))


def _drawn(document: Mapping[str, Any], name: str, default: int) -> int:
    value = document.get(name)
    return default if value is None else int(value)


def waves(rng: random.Random, scenario: Mapping[str, Any]) -> list[list[dict[str, Any]]]:
    """Seeded ``waves`` for both chaos experiments (generator hook ``valkey.generator.waves``).

    One to four waves of one to three operations, each after its own
    ``delay_s``, drawn from the operations of the partial scenario's
    deployment mode, with kills that follow that mode's kill safety so each
    has a target. Standalone: at most ``replicas`` kills per wave (one
    instance always survives), only replicas after the first, a ``restart``
    after half of them, partitions only with per-instance loopback addresses,
    and repoints to another replica only with two replicas or more. Cluster:
    one to three failovers per wave, of distinct shards, four in five with a
    kill before, with, or after (once it finished) the failover, and fewer
    kills in the run than there are replicas (Valkey does not restart them).
    """

    document = plain_config(scenario.get("deployment") or {})
    # An explicit 0 stays 0; only a value not drawn yet takes the default.
    replicas = _drawn(document, "replicas_per_primary", 1)
    if document.get("mode") == "cluster":
        return _cluster_waves(rng, _drawn(document, "primaries", 3), replicas)
    kinds = ["kill"] * 3 + ["promote"] * 3 + ["stall"] * 2 + ["backlog_pressure"]
    if replicas >= 2:
        kinds.append("repoint")
    if document.get("addressing") == "per_instance_loopback":
        kinds.append("partition")
    drawn = []
    for _ in range(rng.randint(1, 4)):
        wave: list[dict[str, Any]] = []
        for _ in range(rng.randint(1, 3)):
            kills = [operation["target"] for operation in wave if operation["op"] == "kill"]
            # At most ``replicas`` kills: one instance always survives.
            allowed = [kind for kind in kinds if kind != "kill" or len(kills) < replicas]
            kind = rng.choice(allowed)
            operation: dict[str, Any] = {"op": kind, "delay_s": _seconds(rng, 0, 1.5)}
            if kind == "kill":
                target = rng.choice(("primary", "replica", "any")) if not kills else "replica"
                operation.update(target=target, signal=rng.choice(("SIGKILL", "SIGTERM")))
            elif kind == "promote":
                operation.update(target="replica", mode=rng.choice(("graceful", "force")))
                if operation["mode"] == "force":
                    operation["split_s"] = rng.choice((0.0, 0.2, 1.0))
            elif kind == "repoint":
                operation.update(target="replica", to=rng.choice(("replica", "primary")))
            elif kind in ("stall", "partition"):
                operation.update(target=rng.choice(("primary", "replica")), duration_s=_seconds(rng, 0.5, 3))
            elif kind == "backlog_pressure":
                operation.update(
                    repl_backlog_size=rng.choice((16384, 65536, 1048576)),
                    output_limit=rng.choice((65536, 262144)),
                    duration_s=_seconds(rng, 1, 3),
                )
            wave.append(operation)
            if kind == "kill" and rng.random() < 0.5:
                wave.append(
                    {"op": "restart", "target": "killed", "delay_s": _round(operation["delay_s"] + rng.uniform(0.5, 2))}
                )
        drawn.append(wave)
    return drawn


def _cluster_waves(rng: random.Random, primaries: int, replicas: int) -> list[list[dict[str, Any]]]:
    kills_left = primaries * replicas - 1
    drawn = []
    for _ in range(rng.randint(1, 4)):
        shards = list(range(primaries))
        rng.shuffle(shards)
        wave: list[dict[str, Any]] = []
        for _ in range(rng.randint(1, min(3, primaries))):
            failover = {
                "op": "failover",
                "shard": shards.pop(),
                "force": rng.choice((True, False)),
                "timeout_s": _seconds(rng, 20, 60),
                "delay_s": _seconds(rng, 0, 1.5),
            }
            if not kills_left or rng.random() >= 0.8:
                wave.append(failover)
                continue
            kills_left -= 1
            kill = {
                "op": "kill",
                "target": rng.choice(("primary", "replica", "any")),
                "signal": rng.choice(("SIGKILL", "SIGTERM")),
            }
            when = rng.choice(("before", "with", "after"))
            if when == "after":
                # Once the failover finished: its new primary, the old one, or a voter.
                kill.update(after=len(wave), delay_s=_seconds(rng, 0, 1))
                wave += [failover, kill]
            else:
                lead = _seconds(rng, 0, 1) if when == "before" else 0.0
                kill["delay_s"] = failover["delay_s"]
                failover["delay_s"] = _round(failover["delay_s"] + lead)
                wave += [kill, failover]
        drawn.append(wave)
    return drawn


def tmpfs_data(rng: random.Random, scenario: Mapping[str, Any]) -> dict[str, Any]:
    """A tmpfs of 16-64 MB holding the primary's data, passed as the ``disk`` input."""

    path = {"$ref": "resources.disk.path"}
    return {
        "resources": {"disk": {"use": "valkey.resource.tmpfs", "with": {"size_mb": rng.choice([16, 32, 64])}}},
        "experiment": {"inputs": {"disk": dict(path)}},
        "deployment": {"instance_data_roots": {"primary": dict(path)}},
    }


def _deployment_build(scenario: Mapping[str, Any], name: str, artifact: str, **inputs: Any) -> dict[str, Any]:
    """Input ``name`` as artifact ``artifact``, the deployment's own binary, unless the scenario sets it."""

    if name in scenario["experiment"].get("inputs", {}):
        return {}
    binary = scenario["deployment"].get("valkey_binary")
    return {
        "artifacts": {
            artifact: {
                "use": "valkey.artifact.local-binary",
                "with": {"path": binary if isinstance(binary, str) and binary else "valkey-server"},
            }
        },
        "experiment": {"inputs": {name: {"$ref": f"artifacts.{artifact}.binary"}, **inputs}},
    }


def upgrade_target(rng: random.Random, scenario: Mapping[str, Any]) -> dict[str, Any]:
    """The ``to_binary`` artifact: the deployment's binary (one build, so the run is skipped) unless set."""

    return _deployment_build(scenario, "to_binary", "upgrade-target")


def wait_replicas(rng: random.Random, scenario: Mapping[str, Any]) -> int:
    """How many replicas every write WAITs for: 0 to the deployment's replicas per primary."""

    return rng.randint(0, int(scenario["deployment"].get("replicas_per_primary") or 0))


def aof_loss_window(rng: random.Random, scenario: Mapping[str, Any]) -> float:
    """``valkey.generator.aof-loss-window``: the least crash loss window the drawn ``appendfsync`` allows.

    0 for ``always`` (fsynced before the reply) and ``no`` (written before the
    reply), :data:`~.ledger.EVERYSEC_LOSS_WINDOW_S` for ``everysec``.
    """

    everysec = scenario["experiment"]["inputs"].get("appendfsync") == "everysec"
    return EVERYSEC_LOSS_WINDOW_S if everysec else 0.0


GENERATORS = (
    GeneratorRegistration("valkey.generator.waves", waves),
    GeneratorRegistration("valkey.generator.tmpfs-data", tmpfs_data),
    GeneratorRegistration("valkey.generator.upgrade-target", upgrade_target),
    GeneratorRegistration("valkey.generator.wait-replicas", wait_replicas),
    GeneratorRegistration("valkey.generator.aof-loss-window", aof_loss_window),
)
