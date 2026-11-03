"""``valkey.action.cluster-election``: force one failover election in a shard and wait for a stable winner.

The election starts with ``CLUSTER FAILOVER FORCE`` on the shard's replicas
at once (``force_failover``), or by failure detection after the primary is
killed (``kill_primary``) or paused (``pause_primary``).
"""

import functools
import signal
import threading
import time
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema

from .cluster import read_view, view_summary
from .faults import FAULT_INJECTED, INJECTED_PAYLOAD
from .instances import check_shard, deployment, wait_following
from .polling import poll_each, poll_until, remaining
from .processes import pause_owned, stop_owned, wait_killed
from .registration import component_registration
from .schemas import ANY, FLAG, INTEGER, LIST, MAPPING, NUMBER, TEXT, declared, mapping_of, nullable, optional


def _answered(view: Optional[dict[str, Any]]) -> tuple[bool, Optional[dict[str, Any]]]:
    return view is not None, view


_ELECTION_FIELDS = {
    "shard": ConfigField(int, default=0, minimum=0),
    # force_failover: CLUSTER FAILOVER FORCE on every replica of the shard at
    # once, so the replicas compete in the same epoch and the old primary
    # votes; kill_primary / pause_primary: SIGKILL or SIGSTOP the primary and
    # let failure detection start the election.
    "trigger": ConfigField(str, default="force_failover", choices=("force_failover", "kill_primary", "pause_primary")),
    "candidates": ConfigField(str, default="all", choices=("all", "one")),
    "attempts": ConfigField(int, default=3, minimum=1, maximum=20),
    "timeout_s": ConfigField(float, default=30.0, minimum=1, maximum=600),
    "settle_s": ConfigField(float, default=1.0, minimum=0, maximum=60),
    "pause_s": ConfigField(float, default=10.0, minimum=0.1, maximum=600),
    "restart_killed": ConfigField(bool, default=True),
    # Wait until the shard's other nodes replicate from the winner with an
    # up link, so later steps and oracles start from a settled shard.
    "wait_synced": ConfigField(bool, default=True),
}


def _validate_election(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    check_shard(config["shard"], plan.deployment["primaries"])


class ElectionAction:
    """Force one failover election in a shard and wait for a stable winner."""

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        members = [node.instance_id for node in handle.instances if node.shard_id == config["shard"]]
        timeout = config["timeout_s"]
        before, unread, owners = self._read_shard(context, handle, members, timeout)
        if unread:
            # A node that did not answer may be the shard's primary or its replica: the shard was not seen.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot read the cluster view of {', '.join(unread)} within {timeout:g}s before the election",
            )
        if len(owners) != 1:
            # The election was never started, so there is nothing of it to judge; the oracles judge the shard.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"shard {config['shard']} still had {len(owners)} slot-owning primaries {timeout:g}s after the "
                "first look, before the election",
                diagnostics={"before": before},
            )
        old_primary = owners[0]
        old_epoch = before[old_primary]["config_epoch"] or 0
        candidates = [instance_id for instance_id, view in before.items() if view["role"] == "replica"]
        if config["candidates"] == "one":
            candidates = candidates[:1]
        if not candidates:
            # No election can start: the run never reached what it tests.
            return make_result(spec, ComponentStatus.ERROR, f"shard {config['shard']} has no live replica to promote")
        record: dict[str, Any] = {
            "shard": config["shard"],
            "trigger": config["trigger"],
            "old_primary": old_primary,
            "old_config_epoch": old_epoch,
            "candidates": candidates,
            "before": {instance_id: view_summary(view) for instance_id, view in before.items()},
            "attempts": [],
        }
        context.event_bus.publish(
            f"{spec.id}.election-started",
            source=spec.id,
            payload={"shard": config["shard"], "trigger": config["trigger"], "old_primary": old_primary},
        )
        winner = None
        own = functools.partial(context.obligations.open, spec)
        if config["trigger"] != "force_failover":
            # Failure detection elects only a replica whose data is recent: Valkey refuses one disconnected from
            # its primary for longer than the data-age bound, which a replica restarted moments ago and still in
            # its first sync is. Start from candidates linked to the primary (FORCE skips that check by design).
            synced, record["candidates_synced"] = wait_following(context, handle, candidates, old_primary, timeout)
            if not synced:
                return make_result(
                    spec,
                    ComponentStatus.ERROR,
                    f"the replicas of shard {config['shard']} did not link to {old_primary} within {timeout}s, so "
                    f"no {config['trigger']} election could start from synced replicas: {record['candidates_synced']}",
                    diagnostics=record,
                )
        if config["trigger"] == "force_failover":
            for attempt in range(config["attempts"]):
                replies = _parallel_failover(handle, candidates)
                record["attempts"].append({"attempt": attempt, "replies": replies, "at": time.time()})
                winner, unread = self._wait_winner(context, handle, members, old_primary, old_epoch, config, timeout)
                if winner is not None:
                    break
        elif config["trigger"] == "kill_primary":
            killed = None
            # A crash the election-safety oracle and `spec conform` judge, with the vars the killed node restarts with.
            crash: dict[str, Any] = {"applied": True, "target": old_primary, "signal": "SIGKILL"}
            if config["restart_killed"]:
                killed = stop_owned(own, handle, old_primary, signal.SIGKILL, wait_ready=True)
            else:
                # A kill without restart_killed lasts until the deployment is removed.
                handle.send_signal(old_primary, signal.SIGKILL)
            record["killed_at"] = crash["applied_at"] = time.time()
            exited = wait_killed(handle, old_primary, signal.SIGKILL, cluster_vars=True)
            record["old_primary_exited"] = exited["exited"]
            crash["disk_vars_at_kill"] = exited["disk_vars_at_kill"]
            context.event_bus.publish(
                f"{spec.id}.injected", source=spec.id, payload={**crash, "trigger": config["trigger"]}
            )
            winner, unread = self._wait_winner(context, handle, members, old_primary, old_epoch, config, timeout)
            if killed is not None:
                try:
                    killed.discharge()
                    record["old_primary_restarted"] = True
                    # The new process can vote from its spawn on, before it is ready.
                    crash["restart_spawned_at"] = handle.started_at(old_primary)
                except Exception as exc:
                    record["restart_error"] = f"{type(exc).__name__}: {exc}"
            record["crash"] = crash
        else:
            paused = pause_owned(own, handle, old_primary)
            record["paused_at"] = time.time()
            try:
                winner, unread = self._wait_winner(
                    context, handle, members, old_primary, old_epoch, config, min(config["pause_s"], timeout)
                )
            finally:
                paused.discharge()
                record["resumed_at"] = time.time()
        if winner is not None and handle.is_alive(old_primary):
            record["old_primary_demoted"] = self._wait_demoted(context, handle, old_primary, config["timeout_s"])
        if winner is not None and config["wait_synced"]:
            record["replicas_synced"] = wait_following(context, handle, members, winner, config["timeout_s"])[1]
        handle.refresh_roles()
        after = {instance_id: read_view(handle, instance_id) for instance_id in members if handle.is_alive(instance_id)}
        record["after"] = {instance_id: view_summary(view) for instance_id, view in after.items()}
        record["winner"] = winner
        record["winner_config_epoch"] = (after.get(winner) or {}).get("config_epoch") if winner else None
        completed = context.event_bus.publish(
            f"{spec.id}.election-completed",
            source=spec.id,
            payload={
                "shard": config["shard"],
                "old_primary": old_primary,
                "winner": winner,
                "config_epoch": record["winner_config_epoch"],
            },
        )
        evidence = (
            make_evidence(
                spec, "valkey.cluster.election.result", "action.completed", record, event_sequence=completed.sequence
            ),
            # The kill of kill_primary, in the shape of a process-signal fault's, for the oracle's crash_faults.
            *((make_evidence(spec, FAULT_INJECTED, "action.completed", record["crash"]),) if "crash" in record else ()),
        )
        if winner is None:
            # A node that did not answer at the end may be the winner.
            record["unread_at_timeout"] = unread
            untriggered = self._not_triggered(record)
            if unread:
                summary = (
                    f"cannot tell whether a replica of shard {config['shard']} won an election ({config['trigger']}): "
                    f"{', '.join(unread)} did not answer"
                )
            elif untriggered:
                summary = f"no election of shard {config['shard']} started: {untriggered}"
            else:
                summary = f"no replica of shard {config['shard']} won an election ({config['trigger']})"
            return make_result(
                spec,
                ComponentStatus.ERROR if unread or untriggered else ComponentStatus.FAIL,
                summary,
                evidence=evidence,
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{winner} replaced {old_primary} (configEpoch {old_epoch} -> {record['winner_config_epoch']})",
            evidence=evidence,
        )

    @staticmethod
    def _read_shard(
        context: Any, handle: Any, members: list[str], timeout: float
    ) -> tuple[dict[str, Any], list[str], list[str]]:
        """The views of the shard's running members, those that did not answer, and its slot-owning primaries.

        Reads until exactly one primary owns slots, within ``timeout``: a node
        restarted from its nodes.conf (an old primary a fault killed) claims
        the slots it owned there until gossip of the newer config reaches it.
        """

        deadline = time.monotonic() + remaining(context, timeout)

        def seen() -> tuple[bool, tuple[dict[str, Any], list[str], list[str]]]:
            polled = poll_each(
                context,
                [instance_id for instance_id in members if handle.is_alive(instance_id)],
                lambda instance_id: _answered(read_view(handle, instance_id)),
                timeout_s=max(0.0, deadline - time.monotonic()),
            )
            views = {instance_id: view for instance_id, (_, view) in polled.items()}
            unread = sorted(instance_id for instance_id, (answered, _) in polled.items() if not answered)
            owners = [item for item, view in views.items() if view and view["role"] == "primary" and view["slots"]]
            return bool(unread) or len(owners) == 1, (views, unread, owners)

        return poll_until(seen, timeout_s=max(0.0, deadline - time.monotonic()), interval_s=0.2, context=context)[1]

    @staticmethod
    def _not_triggered(record: dict[str, Any]) -> Optional[str]:
        """Why the trigger never reached the shard, so no election was there to win; None when it did."""

        if record["trigger"] == "kill_primary" and not record["old_primary_exited"]:
            return f"{record['old_primary']} did not exit after SIGKILL"
        if record["trigger"] == "force_failover" and not any(
            "reply" in reply for attempt in record["attempts"] for reply in attempt["replies"].values()
        ):
            return "no candidate accepted CLUSTER FAILOVER FORCE"
        return None

    def _wait_winner(
        self,
        context: Any,
        handle: Any,
        members: list[str],
        old_primary: str,
        old_epoch: int,
        config: dict,
        timeout: float,
    ) -> tuple[Optional[str], list[str]]:
        """The shard's new primary once it has been the only one for ``settle_s`` (None if none was), and the
        running nodes whose view the last round could not read."""

        deadline = time.monotonic() + remaining(context, timeout)
        stable_since = None
        leader = None
        unread: list[str] = []
        while time.monotonic() < deadline:
            owners = []
            unread = []
            for instance_id in members:
                if instance_id == old_primary or not handle.is_alive(instance_id):
                    continue
                view = read_view(handle, instance_id, timeout=0.5)
                if view is None:
                    unread.append(instance_id)
                elif view["role"] == "primary" and view["slots"] and (view["config_epoch"] or 0) > old_epoch:
                    owners.append(instance_id)
            current = owners[0] if len(owners) == 1 else None
            if current is not None and current == leader:
                if time.monotonic() - stable_since >= config["settle_s"]:
                    return current, []
            else:
                leader = current
                stable_since = time.monotonic()
            if not context.sleep(0.05):
                break
        return None, unread

    def _wait_demoted(self, context: Any, handle: Any, instance_id: str, timeout: float) -> bool:
        def check() -> tuple[bool, None]:
            view = read_view(handle, instance_id, timeout=0.5)
            return bool(view and view["role"] == "replica"), None

        return poll_until(check, timeout_s=timeout, interval_s=0.1, context=context)[0]


def _parallel_failover(handle: Any, candidates: list[str]) -> dict[str, Any]:
    """Send CLUSTER FAILOVER FORCE to every candidate that answers PING, as close together as possible."""

    replies: dict[str, Any] = {}
    clients = {}
    for instance_id in candidates:
        client = handle.new_client(instance_id, timeout=5.0)
        try:
            client.ping()
            clients[instance_id] = client
        except Exception as exc:
            # One unreachable candidate must not hold the others at the barrier.
            replies[instance_id] = {"error": f"{type(exc).__name__}: {exc}"}
            client.close()
    if not clients:
        return replies
    barrier = threading.Barrier(len(clients))
    sent: dict[str, Any] = {}

    def send(instance_id: str) -> None:
        client = clients[instance_id]
        try:
            barrier.wait(timeout=10)
            sent_at = time.time()
            sent[instance_id] = {"reply": str(client.execute_command("CLUSTER", "FAILOVER", "FORCE")), "at": sent_at}
        except Exception as exc:
            sent[instance_id] = {"error": f"{type(exc).__name__}: {exc}"}
        finally:
            client.close()

    threads = {instance_id: threading.Thread(target=send, args=(instance_id,), daemon=True) for instance_id in clients}
    for thread in threads.values():
        thread.start()
    for instance_id, thread in threads.items():
        thread.join(timeout=30)
        # A thread still running owns its entry.
        replies[instance_id] = {"error": "no reply within 30s"} if thread.is_alive() else sent[instance_id]
    return replies


_ELECTION_DECLARES = Declarations(
    events={
        "{id}.election-started": ConfigSchema({"shard": INTEGER, "trigger": TEXT, "old_primary": TEXT}),
        # kill_primary's kill.
        "{id}.injected": INJECTED_PAYLOAD,
        "{id}.election-completed": ConfigSchema(
            {"shard": INTEGER, "old_primary": TEXT, "winner": nullable(str), "config_epoch": nullable(int)}
        ),
    },
    evidence={
        "valkey.cluster.election.result": declared(
            mapping_of(
                {
                    "shard": INTEGER,
                    "trigger": TEXT,
                    "old_primary": TEXT,
                    "old_config_epoch": INTEGER,
                    "candidates": LIST,
                    "before": MAPPING,
                    "attempts": LIST,
                    "after": MAPPING,
                    "winner": nullable(str),
                    "winner_config_epoch": nullable(int),
                },
                # When the trigger acted, and what became of the old primary.
                additional=ANY,
            )
        ),
        FAULT_INJECTED: declared(
            mapping_of(
                {
                    "applied": FLAG,
                    "target": TEXT,
                    "signal": TEXT,
                    "applied_at": NUMBER,
                    "disk_vars_at_kill": nullable(dict),
                    "restart_spawned_at": optional(float),
                }
            )
        ),
    },
    changes=("process.stopped", "process.paused"),
)

REGISTRATIONS = (
    component_registration(
        "valkey.action.cluster-election",
        ElectionAction,
        _ELECTION_FIELDS,
        check=_validate_election,
        deployment_modes=("cluster",),
        declares=_ELECTION_DECLARES,
    ),
)
