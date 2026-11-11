"""Oracles that judge only what a server reports about itself in INFO, so they need no model of the feature
under test and every experiment inherits them: error counters that rose with nothing in the scenario to explain
the rise, and a quiet end.
"""

import fnmatch
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.cluster_nodes import parse_cluster_nodes
from valkey_fuzzer.deployment.local.queries import parse_replication, response_text
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, Evidence, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField

from . import checks
from .command_table import EVIDENCE as COMMAND_TABLE
from .faults import FAULT_INJECTED
from .instances import EXPECTED_DOWN, deployment, instance_at, is_cluster
from .ledger import FAILOVER_RESULT
from .polling import poll_each, remaining
from .registration import component_registration
from .schemas import MAPPING, declared, mapping_of

# How long an oracle retries reading a running instance's INFO; valkey.oracle.liveness judges one that never answers.
_READ_TIMEOUT_S = 30.0


def _running(handle: Any) -> list[str]:
    """The running instances, which is all an INFO oracle can read; valkey.oracle.liveness judges the others."""

    return [instance.instance_id for instance in handle.instances if handle.is_alive(instance.instance_id)]


# ---------------------------------------------------------------------------
# valkey.oracle.error-counters — every Valkey experiment includes it
# ---------------------------------------------------------------------------

# What a scenario does that can raise an error counter (see scenario_causes).
LINKS = "links"
STORAGE = "storage"
COMMANDS = "commands"
MEMORY = "memory"

# The changes (undo obligations) that break replication and cluster links for a while, and those that make
# writes to disk fail.
_LINK_CHANGES = ("process.stopped", "process.paused", "network.partition")
_STORAGE_CHANGES = ("disk.filled", "directory.read-only")
# A kill or a role change lasts until the deployment is removed, so no obligation owns it: its evidence says so.
_LINK_EVIDENCE = (EXPECTED_DOWN, "valkey.chaos.schedule", FAILOVER_RESULT, "valkey.replication.replicaof")
# Configured commands that act on connections, the topology, or the server's internals, besides those that set
# replication (checks._sets_replication): CLIENT KILL drops links, DEBUG CHANGE-REPL-ID forces a full resync.
_LINK_COMMANDS = ("CLIENT", "CLUSTER", "DEBUG")

# A counter whose name says something went wrong, in a core section or a module's.
_WENT_WRONG = re.compile(
    r"(?:^|_)(?:err|errors?|fail|failed|failures?|corrupt\w*|lost|dropped|rejected|unexpected|denied)(?:_|$)"
)
# Watched though their names do not say it.
_ALSO_WATCHED = ("client_query_buffer_limit_disconnections", "client_output_buffer_limit_disconnections")
# The sum of the errorstat entries, each of which is judged by its own code.
_SUMS = ("total_error_replies",)

# The causes that explain a rise, by counter name, first match first; none for a counter that stays at zero
# in every run.
_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    # An error reply on a replication link or while loading the AOF: the replica or the file diverged.
    ("unexpected_error_replies", ()),
    # No component sends a query near the limit (1 GB), or opens connections near maxclients.
    ("client_query_buffer_limit_disconnections", ()),
    ("rejected_*connections", ()),
    # A save, a rewrite, or an AOF write failed. A cancelled save and a killed server leave the status ok.
    ("*_status", (STORAGE,)),
    ("aof_rewrites_consecutive_failures", (STORAGE,)),
    # A replica that came back was refused a partial resync, or fell too far behind its primary's buffer.
    ("sync_partial_err", (LINKS,)),
    ("client_output_buffer_limit_disconnections", (LINKS,)),
    # Nodes reported failed (FAIL messages, failed nodes and slots).
    ("cluster_*", (LINKS,)),
    # A client was denied: only a command the scenario sent can be.
    ("acl_access_denied_*", (COMMANDS,)),
    ("errorstat_OOM", (MEMORY, COMMANDS, LINKS, STORAGE)),
    # An error reply (errorstat_<code>), or a counter of a module's or a newer build's: it needs a reason. Either
    # Valkey refused one of the scenario's own commands, or the scenario disturbed the deployment, so clients
    # met READONLY, LOADING, CLUSTERDOWN, MISCONF, and the like.
    ("*", (COMMANDS, LINKS, STORAGE)),
)


def risen(info: Mapping[str, Any]) -> dict[str, Any]:
    """The watched counters of one INFO reply that are above zero, and the status fields that read ``err``.

    A process starts with every counter at zero and every status ok, and the deployment started every process
    during the run, so each of these rose during the run, in its process's lifetime; what an instance counted
    before a restart is gone with that process.
    """

    found: dict[str, Any] = {}
    for name, value in info.items():
        if not isinstance(name, str) or name in _SUMS:
            continue
        if name.startswith("errorstat_") and isinstance(value, Mapping):
            value = value.get("count", 0)
        elif name.endswith("_status"):
            if value == "err":
                found[name] = value
            continue
        elif not (_WENT_WRONG.search(name) or name in _ALSO_WATCHED):
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
            found[name] = value
    return found


def _configured_commands(plan: ExecutionPlan) -> list[dict[str, Any]]:
    """The commands the plan's components are configured to send (valkey.action.command, valkey.fault.command),
    as the command checks judge them."""

    return [
        {"command": spec.config["command"]}
        for spec in plan.components
        if isinstance(spec.config.get("command"), (list, tuple)) and spec.config["command"]
    ]


def _scenario_settings(plan: ExecutionPlan) -> dict[str, Any]:
    """The server settings the scenario itself names: its deployment's ``server_config`` and per-role and
    per-instance configs, and the experiment's startup overlays; never the deployment provider's defaults."""

    settings = dict(plan.deployment.get("server_config") or {})
    for config in (plan.deployment.get("instance_configs") or {}).values():
        settings.update(config)
    for overlay in plan.startup_overlays:
        settings.update(overlay.server_config)
    return settings


def _got_errors(evidence: Sequence[Evidence]) -> list[str]:
    """The components whose own commands Valkey answered with an error reply."""

    sources = [item.source for item in evidence_of(evidence, COMMAND_TABLE) if item.value["outcomes"]["error"]]
    for item in evidence_of(evidence, "valkey.command.replies"):
        if any("error" in record and not record.get("unanswered") for record in item.value):
            sources.append(item.source)
    # A BGSAVE refused while another child ran (sent again), or for any other reason.
    for item in evidence_of(evidence, "valkey.bgsave.result"):
        if item.value.get("refusals") or item.value.get("status") == "rejected":
            sources.append(item.source)
    for item in evidence_of(evidence, FAULT_INJECTED):
        replies = item.value.get("replies")
        if isinstance(replies, Mapping) and any(
            "error" in record and not record.get("unanswered") for record in replies.values()
        ):
            sources.append(item.source)
    return sources


def scenario_causes(context: Any, evidence: Sequence[Evidence]) -> dict[str, list[str]]:
    """What the scenario did that can raise an error counter, each cause with what did it.

    - ``links``: it broke replication or cluster links: a component stopped, paused, or partitioned an instance
      (an undo obligation of those kinds), killed one for good or moved a role (:data:`_LINK_EVIDENCE`), or was
      configured with a command that does (``REPLICAOF``, ``FAILOVER``, ``CLIENT KILL``, ``DEBUG``, ...).
    - ``storage``: it made writes to disk fail: a filled disk or a read-only directory (an undo obligation).
    - ``commands``: Valkey answered one of its own commands with an error reply: the command-table workload's,
      a configured command's, or a BGSAVE of ``valkey.action.bgsave``.
    - ``memory``: it set a ``maxmemory`` itself, in its server settings or with ``CONFIG SET``; the deployment
      provider's default is one no experiment's dataset reaches.
    """

    opened = context.obligations.report()
    changes = [*opened["undone"], *opened["still_active"]]
    commands = _configured_commands(context.plan)
    link_commands = [
        " ".join(words[:2]) for words in map(checks._words, commands) if words and words[0] in _LINK_COMMANDS
    ]
    causes = {
        LINKS: [f"{item['owner']} ({item['kind']})" for item in changes if item["kind"] in _LINK_CHANGES]
        + [
            f"{item.source} ({item.evidence_id})"
            for item in evidence
            if item.evidence_id in _LINK_EVIDENCE and (item.evidence_id != EXPECTED_DOWN or item.value["instances"])
        ]
        + link_commands
        + (["a configured command sets replication"] if checks._sets_replication(commands) else []),
        STORAGE: [f"{item['owner']} ({item['kind']})" for item in changes if item["kind"] in _STORAGE_CHANGES],
        COMMANDS: _got_errors(evidence),
        MEMORY: ["maxmemory"] if checks._limits_memory(commands, _scenario_settings(context.plan)) else [],
    }
    return {cause: sorted(set(what)) for cause, what in causes.items()}


def _explained_by(name: str, value: Any, causes: Mapping[str, Sequence[str]], replicas: int) -> Optional[str]:
    """The cause that explains counter ``name`` at ``value`` on an instance whose shard was planned ``replicas``
    replicas; None when nothing in the scenario does."""

    accepted = next(explained for pattern, explained in _RULES if fnmatch.fnmatchcase(name, pattern))
    cause = next((cause for cause in accepted if causes.get(cause)), None)
    if cause is None and name == "sync_partial_err" and value <= replicas:
        # A node that was a primary before it was made a replica asks to continue its own history, which its
        # new primary refuses once: how the deployment attaches its replicas when it forms.
        return "formation"
    return cause


class ErrorCountersOracle:
    """No error counter rose without something in the scenario that explains the rise.

    It reads ``INFO everything`` of each running instance and judges the counters whose names say something
    went wrong (:func:`risen`), a module's too. Each rise needs a cause from what the scenario did
    (:func:`scenario_causes`) that :data:`_RULES` accepts for the counter; a rise without one fails the run.
    A running instance it cannot read makes it ERROR, unless another rise is a finding.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        handle = deployment(context)

        def read(instance_id: str) -> tuple[bool, Any]:
            with handle.new_client(instance_id, timeout=5.0) as client:
                try:
                    return True, client.info("everything")
                except Exception as exc:
                    return False, f"{type(exc).__name__}: {exc}"

        polled = poll_each(context, _running(handle), read, timeout_s=_READ_TIMEOUT_S)
        causes = scenario_causes(context, evidence)
        shards = [instance.shard_id for instance in handle.instances if instance.role == "replica"]
        instances: dict[str, Any] = {}
        problems = []
        for instance in handle.instances:
            if not polled.get(instance.instance_id, (False, None))[0]:
                continue
            rose = {
                name: {"value": value, "explained_by": _explained_by(name, value, causes, shards.count(instance.shard_id))}
                for name, value in sorted(risen(polled[instance.instance_id][1]).items())
            }  # fmt: skip
            instances[instance.instance_id] = rose
            problems += [
                f"{instance.instance_id}: {name} " + ("is err" if item["value"] == "err" else f"rose to {item['value']}")
                for name, item in rose.items()
                if item["explained_by"] is None
            ]  # fmt: skip
        record = {"instances": instances, "scenario": causes}
        if problems:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(problems) + "; nothing the scenario did explains it",
                diagnostics=record,
            )
        unread = {instance_id: value for instance_id, (answered, value) in polled.items() if not answered}
        if unread:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "cannot read INFO of "
                + "; ".join(f"{instance_id}: {error}" for instance_id, error in sorted(unread.items())),
                diagnostics=record,
            )
        explained = sum(len(rose) for rose in instances.values())
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"no unexplained error counter on {len(instances)} instance(s)"
            + (f"; {explained} rise(s) the scenario explains" if explained else ""),
            evidence=(make_evidence(spec, "valkey.error-counters.verified", "oracle.completed", record),),
        )


# ---------------------------------------------------------------------------
# valkey.oracle.quiet-end — every Valkey experiment includes it
# ---------------------------------------------------------------------------

_QUIET_FIELDS = {
    # How long the deployment has to become quiet once the oracles run.
    "timeout_s": ConfigField(float, default=30.0, minimum=1, maximum=600),
    # The wall-clock window a server's CPU use is measured over, and the share of one core it may use in it.
    # An idle server uses about 0.01: its cron (hz), replication keepalives, and cluster bus. Half a core
    # leaves room for active defragmentation and expiry (up to a quarter each); a spinning event loop
    # uses a whole one.
    "cpu_window_s": ConfigField(float, default=1.0, minimum=0.1, maximum=60),
    "max_cpu": ConfigField(float, default=0.5, minimum=0.01, maximum=64),
}


def _cpu_seconds(info: Mapping[str, Any], threads: int) -> Optional[float]:
    """The CPU time the server's main thread used so far: as INFO reports it (Linux), else the process's shared
    among the ``threads`` that run whenever the main thread does. None when INFO has neither."""

    try:
        if "used_cpu_user_main_thread" in info:
            return float(info["used_cpu_user_main_thread"]) + float(info["used_cpu_sys_main_thread"])
        return (float(info["used_cpu_user"]) + float(info["used_cpu_sys"])) / threads
    except (KeyError, TypeError, ValueError):
        return None


def _spinning_threads(plan: ExecutionPlan, instance_id: str) -> int:
    """The threads of ``instance_id`` that run whenever its main thread does: with io-threads-always-active
    every IO thread spins while the main thread handles an event, so they share its load."""

    config = plan.instances.get(instance_id, {}).get("server_config", {})
    if str(config.get("io-threads-always-active", "no")) != "yes":
        return 1
    try:
        return max(1, int(config.get("io-threads", 1)))
    except (TypeError, ValueError):
        return 1


def _busy(info: Mapping[str, Any]) -> list[str]:
    """What one INFO reply shows the server still doing: clients blocked, a child process or a load in progress."""

    busy = []
    try:
        blocked = int(info.get("blocked_clients", 0))
    except (TypeError, ValueError):
        blocked = 0
    if blocked:
        busy.append(f"{blocked} blocked client(s)")
    # rdb_bgsave_in_progress, aof_rewrite_in_progress, module_fork_in_progress, slot_migration_fork_in_progress;
    # master_sync_in_progress is a link that is not up, which the links report.
    busy += sorted(
        name
        for name, value in info.items()
        if isinstance(name, str)
        and (name.endswith("_in_progress") or name in ("loading", "async_loading"))
        and name != "master_sync_in_progress"
        and str(value) not in ("0", "False")
    )
    return busy


class QuietEndOracle:
    """Once the workload and the faults are over, every running instance comes to rest within ``timeout_s``.

    At rest an instance has no blocked client, no child process (a save, an AOF rewrite, a module's fork) and no
    load in progress, its replication link is up unless its primary is down, in cluster mode it sees every
    running node connected and none failed, and its main thread used at most ``max_cpu`` of a core over the
    last ``cpu_window_s``: a server that keeps a core busy with nothing to do is spinning. The oracle reads
    every instance once per window until all are at rest at once; what is left when the time is up fails the
    run. An instance that did not answer then, or whose CPU use had no full window, makes it ERROR, unless
    another is a finding.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        ids = _running(handle)
        cluster = is_cluster(handle)
        threads = {instance_id: _spinning_threads(context.plan, instance_id) for instance_id in ids}
        deadline = time.monotonic() + remaining(context, config["timeout_s"])
        clocks: dict[str, tuple[float, float]] = {}
        while True:
            unquiet: dict[str, list[str]] = {}
            # Why an instance could not be judged in this round: it did not answer, or its CPU use has no window yet.
            unseen: dict[str, str] = {}
            cpu: dict[str, float] = {}
            for instance_id in ids:
                try:
                    with handle.new_client(instance_id, timeout=5.0) as client:
                        info = client.info()
                        nodes = (
                            parse_cluster_nodes(response_text(client.execute_command("CLUSTER", "NODES")))
                            if cluster
                            else []
                        )
                except Exception as exc:
                    unseen[instance_id] = f"{type(exc).__name__}: {exc}"
                    clocks.pop(instance_id, None)
                    continue
                reasons = _busy(info) + self._links(handle, info, nodes)
                now, used = time.monotonic(), _cpu_seconds(info, threads[instance_id])
                before = clocks.get(instance_id)
                if used is not None:
                    clocks[instance_id] = (now, used)
                    # A process that restarted counts its CPU time from zero again: its window starts over.
                    if before is None or now <= before[0] or used < before[1]:
                        unseen[instance_id] = "its CPU use was not measured over a window"
                    else:
                        cpu[instance_id] = round((used - before[1]) / (now - before[0]), 3)
                        if cpu[instance_id] > config["max_cpu"]:
                            reasons.append(f"used {cpu[instance_id]:.2f} of a core over {now - before[0]:.1f}s")
                if reasons:
                    unquiet[instance_id] = reasons
            if not (unquiet or unseen) or time.monotonic() >= deadline or not context.sleep(config["cpu_window_s"]):
                break
        record = {"instances": {item: {"cpu": cpu.get(item), "unquiet": unquiet.get(item, [])} for item in ids}}
        if unquiet:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(f"{item}: {', '.join(reasons)}" for item, reasons in sorted(unquiet.items()))
                + f"; not at rest within {config['timeout_s']:g}s",
                diagnostics={**record, "unseen": unseen},
            )
        if unseen:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "cannot judge " + "; ".join(f"{item}: {why}" for item, why in sorted(unseen.items())),
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(ids)} instance(s) at rest: no blocked client, child process, or load, links up, "
            f"at most {max(cpu.values(), default=0):.2f} of a core in use",
            evidence=(make_evidence(spec, "valkey.quiet-end.verified", "oracle.completed", record),),
        )

    @staticmethod
    def _links(handle: Any, info: Mapping[str, Any], nodes: Sequence[Mapping[str, Any]]) -> list[str]:
        """The links of one instance that are down though both ends run: its replication link, and in cluster
        mode (``nodes``, its CLUSTER NODES) its links to the other nodes. valkey.oracle.liveness judges an
        instance that is not running."""

        down = []
        link = parse_replication(info)
        if link.role == "replica" and not link.link_up:
            primary = instance_at(handle, link.primary_host, link.primary_port)
            if primary is None or handle.is_alive(primary):
                down.append(f"replication link to {primary or f'{link.primary_host}:{link.primary_port}'} is down")
        for node in nodes:
            peer = instance_at(handle, node["host"], node["port"])
            if node["is_myself"] or peer is None or not handle.is_alive(peer):
                continue
            if node["link_state"] != "connected":
                down.append(f"cluster link to {peer} is {node['link_state']}")
            elif node["is_fail"]:
                down.append(f"sees {peer} as failed")
        return down


_RECORD = mapping_of({"instances": MAPPING})

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.error-counters",
        ErrorCountersOracle,
        {},
        declares=Declarations(
            evidence={
                "valkey.error-counters.verified": declared(mapping_of({"instances": MAPPING, "scenario": MAPPING}))
            }
        ),
    ),
    component_registration(
        "valkey.oracle.quiet-end",
        QuietEndOracle,
        _QUIET_FIELDS,
        declares=Declarations(evidence={"valkey.quiet-end.verified": declared(_RECORD)}),
    ),
)
