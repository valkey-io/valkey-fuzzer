"""Built-in actions: commands, BGSAVE, failover, REPLICAOF, restart, rolling upgrade, polls, and waits.

The runner publishes ``<id>.started`` and ``<id>.completed``
around every action, so a fault can trigger on, and an experiment can require, any
action finishing. Events an action publishes itself are also named after its
component (``<id>.save-started``), so they never collide.
"""

import functools
import os
import shutil
import time
from typing import Any, Optional

import valkey

from valkey_fuzzer._data import jsonable
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.queries import replication_role
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema

from .compression import rdb_format
from .instances import (
    check_shard,
    close_quietly,
    deployment,
    follows,
    graceful_failover,
    is_cluster,
    is_primary,
    replication_info,
    select_instance,
    select_instances,
    send_to,
    wait_following,
)
from .polling import poll_until, read_retrying, remaining
from .predicates import PREDICATE_FIELDS, compare, validate_predicate
from .processes import stop_owned, wait_exit_signalling
from .registration import component_registration, validate_command
from .schemas import ANY, FLAG, INTEGER, LIST, MAPPING, NUMBER, TEXT, declared, mapping_of, nullable, optional
from .server import binary_version, instance_identity, instance_role

# ---------------------------------------------------------------------------
# valkey.action.command
# ---------------------------------------------------------------------------

_COMMAND_FIELDS = {
    "instances": ConfigField((str, list), default="primaries"),
    "command": ConfigField(list),
    # A predicate on the reply, or ``error``: true, false, or an error substring.
    "expect": mapping_of({**PREDICATE_FIELDS, "error": optional((bool, str))}, allow_none=True, default=None),
    "repeat": ConfigField(int, default=1, minimum=1, maximum=100_000),
    "interval_s": ConfigField(float, default=0.0, minimum=0, maximum=3600),
    "timeout_s": ConfigField(float, default=10.0, minimum=0.1, maximum=3600),
}


def _validate_command(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    validate_command(config["command"])
    if config["expect"] is not None:
        expect = config["expect"]
        if "error" in expect:
            if set(expect) != {"error"}:
                raise ConfigError("expect.error cannot be combined with a reply predicate")
        else:
            validate_predicate(expect, "expect")


def _matches(expect: Optional[dict], reply: Any, error: Optional[str]) -> bool:
    if expect is None:
        return error is None
    if "error" in expect:
        wanted = expect["error"]
        if wanted is False:
            return error is None
        if wanted is True:
            return error is not None
        return error is not None and str(wanted) in error
    return error is None and compare(reply, expect)


def run_command_on_instances(context: Any, spec: ComponentSpec, config: dict) -> tuple[bool, list[dict]]:
    handle = deployment(context)
    replies = []
    for instance_id in select_instances(handle, config["instances"]):
        for attempt in range(config["repeat"]):
            record: dict[str, Any] = {"instance_id": instance_id, "attempt": attempt, "at": time.time()}
            record.update(
                send_to(handle, instance_id, config["command"], timeout_s=config["timeout_s"], context=context)
            )
            record["matched"] = not record.get("unanswered") and _matches(
                config["expect"], record.get("reply"), record.get("error")
            )
            replies.append(record)
            if config["interval_s"] and attempt + 1 < config["repeat"]:
                if not context.sleep(min(config["interval_s"], remaining(context, config["interval_s"]))):
                    # Cancelled: the result is discarded, so stop sending commands.
                    return False, replies
    return all(record["matched"] for record in replies), replies


class CommandAction:
    """Send one command to the selected instances, ``repeat`` times each, and match each reply against ``expect``.

    An experiment drives Valkey with it where no dedicated action exists (DEBUG, CLIENT KILL, CONFIG SET); the
    replies are evidence, and a reply ``expect`` rejects fails the run. A command that got no reply (the
    connection failed, or the reply timed out) leaves the run untested (ERROR), unless a reply that did come
    back was wrong.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        all_ok, replies = run_command_on_instances(context, spec, config)
        unanswered = sum(bool(item.get("unanswered")) for item in replies)
        wrong = sum(not item["matched"] and not item.get("unanswered") for item in replies)
        status = ComponentStatus.PASS if all_ok else ComponentStatus.FAIL if wrong else ComponentStatus.ERROR
        return make_result(
            spec,
            status,
            f"{' '.join(str(item) for item in config['command'])}: "
            f"{sum(item['matched'] for item in replies)}/{len(replies)} replies matched"
            + (f", {unanswered} unanswered" if unanswered else ""),
            evidence=(make_evidence(spec, "valkey.command.replies", "action.completed", replies),),
        )


# ---------------------------------------------------------------------------
# valkey.action.bgsave
# ---------------------------------------------------------------------------

_BGSAVE_FIELDS = {
    "instances": ConfigField((str, list), default="primaries"),
    "args": ConfigField(list, default=list, items=TEXT),
    "timeout_s": ConfigField(float, default=120.0, minimum=0.5, maximum=7200),
    "poll_interval_s": ConfigField(float, default=0.02, minimum=0.001, maximum=10),
    "expect": ConfigField(str, default="ok", choices=("ok", "err", "not_completed", "any")),
    "expect_type": ConfigField(str, allow_none=True, default=None, choices=("fork", "forkless", None)),
    "capture_rdb": ConfigField(bool, default=True),
}


def _snapshot_identity(path: str) -> Optional[tuple[int, int, int]]:
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return (stat.st_ino, stat.st_mtime_ns, stat.st_size)


def _persistence_record(persistence: dict, *fields: str) -> dict[str, Any]:
    return {field: persistence.get(field) for field in fields}


# The save statuses that say nothing of a save Valkey ran (the harness could not see it, or could not start it
# while another child ran): ERROR.
_UNSEEN = {
    "unanswered": "BGSAVE got no reply",
    "unavailable": "INFO persistence could not be read",
    "busy": "another child process ran until the timeout",
}
# Valkey's replies to a BGSAVE while another child process runs: retried, never judged.
_BGSAVE_BUSY = ("Background save already in progress", "Another child process is active")


class _SaveTarget:
    """One instance's BGSAVE: where its RDB lands, its persistence fields before and after, and its timing.

    The save counts as done (``ok``) only when the RDB file changed, so a save that reported success
    without writing is ``not_completed``. What the harness could not see is kept apart from what Valkey
    did: a BGSAVE with no reply is ``unanswered``, a save whose INFO could not be read until the timeout
    ``unavailable``, one refused while another child ran until the timeout ``busy``; any other error reply is
    ``rejected``.
    """

    def __init__(self, handle: Any, instance_id: str):
        self.instance_id = instance_id
        self.client = handle.new_client(instance_id, timeout=5.0)
        try:
            directory = self.client.config_get("dir").get("dir") or handle.data_dir(instance_id)
            dbfilename = self.client.config_get("dbfilename").get("dbfilename") or "dump.rdb"
            self.rdb_path = os.path.join(directory, dbfilename)
            self.read_before()
            # A primary's snapshot holds every write acknowledged before
            # BGSAVE was sent; a replica's may lag behind.
            self.role = replication_role(self.client.info("replication"))
        except BaseException:
            # The caller never receives this target, so it cannot close it.
            self.close()
            raise
        self.observed_types: set[str] = set()
        self.reply: Any = None
        self.error: Optional[str] = None
        self.unanswered = False
        self.busy = False
        # The last INFO read's error, None once one succeeded.
        self.read_error: Optional[str] = None
        self.finished = False
        self.final: dict[str, Any] = {}
        # When BGSAVE was sent, its reply arrived, and the save was seen finished.
        self.sent_at: Optional[float] = None
        self.replied_at: Optional[float] = None
        self.finished_at: Optional[float] = None

    def read_before(self) -> None:
        persistence = self.client.info("persistence")
        self.before = {
            **_persistence_record(persistence, "rdb_saves", "rdb_last_bgsave_type", "rdb_last_bgsave_status"),
            "snapshot": _snapshot_identity(self.rdb_path),
        }

    def send(self, args: list, context: Any, deadline: float, interval_s: float) -> None:
        """Send BGSAVE, again while another child (a replication RDB, an AOF rewrite) still runs, until
        ``deadline``: Valkey refuses a BGSAVE then, which says nothing about saving. The persistence fields
        before the save are read again for the save that is accepted."""

        while True:
            try:
                self.sent_at = time.time()
                self.reply = jsonable(self.client.execute_command("BGSAVE", *args))
                self.replied_at = time.time()
                return
            except valkey.exceptions.ResponseError as exc:
                busy = any(text in str(exc) for text in _BGSAVE_BUSY)
                if not busy or time.monotonic() >= deadline or not context.sleep(interval_s):
                    self.error = f"{type(exc).__name__}: {exc}"
                    self.busy, self.finished = busy, True
                    return
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                self.unanswered = self.finished = True
                return
            _, self.read_error = read_retrying(
                context, self.read_before, timeout_s=max(0.0, deadline - time.monotonic()), interval_s=interval_s
            )
            if self.read_error is not None:
                self.finished = True
                return

    def poll(self) -> None:
        """One INFO read; one that fails is tried again on the next poll (a restarted instance answers again)."""

        try:
            persistence = self.client.info("persistence")
        except (valkey.exceptions.ValkeyError, OSError) as exc:
            self.read_error = f"{type(exc).__name__}: {exc}"
            return
        self.read_error = None
        current = persistence.get("rdb_current_bgsave_type")
        if current not in (None, "none"):
            self.observed_types.add(str(current))
        if str(persistence.get("rdb_bgsave_in_progress")) == "0":
            self.finished = True
            self.finished_at = time.time()
            self.final = _persistence_record(
                persistence, "rdb_saves", "rdb_last_bgsave_type", "rdb_last_bgsave_status", "rdb_last_bgsave_time_sec"
            )

    def status(self) -> str:
        if self.unanswered:
            return "unanswered"
        if self.busy:
            return "busy"
        if self.reply is None and self.read_error is None:
            return "rejected"
        if self.read_error is not None:
            return "unavailable"
        if not self.finished:
            return "timeout"
        if self.final.get("rdb_last_bgsave_status") == "err":
            return "err"
        if _snapshot_identity(self.rdb_path) not in (None, self.before["snapshot"]):
            return "ok"
        return "not_completed"

    def close(self) -> None:
        close_quietly(self.client)


class BgsaveAction:
    """Issue BGSAVE on selected instances and follow it to completion.

    Publishes ``<id>.save-started`` once the BGSAVE commands were sent
    (faults that must hit a save in progress trigger on it) and
    ``<id>.save-completed`` once every save finished or timed out.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        instances = select_instances(handle, config["instances"])
        targets: list[_SaveTarget] = []
        try:
            # Built one at a time inside the try so that a target whose setup
            # fails (e.g. CONFIG GET is rejected) still closes the earlier ones.
            for instance_id in instances:
                targets.append(_SaveTarget(handle, instance_id))
            started_at = time.time()
            deadline = time.monotonic() + config["timeout_s"]
            for target in targets:
                target.send(config["args"], context, deadline, config["poll_interval_s"])
            started = context.event_bus.publish(
                f"{spec.id}.save-started",
                source=spec.id,
                payload={
                    "instances": instances,
                    "accepted": [target.instance_id for target in targets if target.error is None],
                },
            )

            def all_finished() -> tuple[bool, None]:
                for target in targets:
                    if not target.finished:
                        target.poll()
                return all(target.finished for target in targets), None

            poll_until(
                all_finished, timeout_s=config["timeout_s"], interval_s=config["poll_interval_s"], context=context
            )
            results = [self._record(context, spec, config, target) for target in targets]
            completed = context.event_bus.publish(
                f"{spec.id}.save-completed",
                source=spec.id,
                payload={"statuses": {record["instance_id"]: record["status"] for record in results}},
            )
        finally:
            for target in targets:
                target.close()

        evidence = [
            make_evidence(
                spec,
                "valkey.bgsave.result",
                "action.completed",
                record,
                event_sequence=completed.sequence,
                artifacts=[record["rdb_artifact"]] if "rdb_artifact" in record else (),
            )
            for record in results
        ]
        problems = []
        unseen = []
        for record in results:
            if config["expect"] != "any" and record["status"] in _UNSEEN:
                unseen.append(f"{record['instance_id']}: {_UNSEEN[record['status']]} ({record['error']})")
            elif config["expect"] != "any" and record["status"] != config["expect"]:
                problems.append(f"{record['instance_id']}: expected {config['expect']}, got {record['status']}")
            if config["expect_type"] and config["expect"] == "ok":
                last_type = record["after"].get("rdb_last_bgsave_type")
                if last_type != config["expect_type"]:
                    problems.append(
                        f"{record['instance_id']}: last save type {last_type!r}, expected {config['expect_type']}"
                    )
        return make_result(
            spec,
            ComponentStatus.FAIL if problems else ComponentStatus.ERROR if unseen else ComponentStatus.PASS,
            "; ".join([*problems, *unseen])
            or f"BGSAVE {config['expect']} on {len(results)} instance(s) in {time.time() - started_at:.2f}s",
            evidence=evidence,
            diagnostics={"started_event": started.sequence, "results": results},
        )

    @staticmethod
    def _record(context: Any, spec: ComponentSpec, config: dict, target: _SaveTarget) -> dict[str, Any]:
        status = target.status()
        record = {
            "instance_id": target.instance_id,
            "reply": target.reply,
            "status": status,
            "observed_types": sorted(target.observed_types),
            "before": jsonable(target.before),
            "after": jsonable(target.final),
            "rdb_path": target.rdb_path,
            "error": target.error or target.read_error,
            "role": target.role,
            "sent_at": target.sent_at,
            "replied_at": target.replied_at,
            "finished_at": target.finished_at,
        }
        if status != "ok":
            return record
        saved = target.rdb_path
        if config["capture_rdb"]:
            destination = os.path.join(component_artifact_dir(context, spec), f"{target.instance_id}.rdb")
            try:
                shutil.copyfile(target.rdb_path, destination)
                record["rdb_artifact"] = saved = destination
            except OSError as exc:
                record["capture_error"] = str(exc)
        record["rdb_format"] = rdb_format(saved)
        return record


# ---------------------------------------------------------------------------
# valkey.action.failover
# ---------------------------------------------------------------------------

_FAILOVER_FIELDS = {
    "shard": ConfigField(int, allow_none=True, default=None, minimum=0),
    "target": ConfigField(str, allow_none=True, default=None),
    "force": ConfigField(bool, default=False),
    "timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=600),
}


def _validate_failover(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if config["shard"] is not None and config["target"] is not None:
        raise ConfigError("set either shard or target, not both")
    if config["shard"] is not None:
        check_shard(config["shard"], plan.deployment["primaries"])


class FailoverAction:
    """Fail over a shard's current primary through the deployment's failover operation.

    ``target`` names a planned instance (its shard is failed over); ``shard``
    names the shard directly. The default is shard 0.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        shard = config["shard"]
        if config["target"] is not None:
            shard = handle.instance(select_instance(handle, config["target"])).shard_id
        shard = 0 if shard is None else shard
        target = f"shard-{shard}-primary"
        roles = handle.roles()
        # The primary that may lose acknowledged writes to a forced failover.
        primary_before = next(
            (
                instance.instance_id
                for instance in handle.instances
                if instance.shard_id == shard and roles.get(instance.instance_id) == "primary"
            ),
            None,
        )
        started_at = time.time()
        ok = handle.failover(
            shard,
            force=config["force"],
            timeout=config["timeout_s"],
            rng=context.rng(spec.id, "failover"),
        )
        if ok is None:
            # The shard, or every replica of its primary, is stopped: there was no failover to run.
            return make_result(spec, ComponentStatus.ERROR, f"{target} has nothing to fail over")
        record = {
            "target": target,
            "force": config["force"],
            "success": ok,
            "roles": handle.roles(),
            "primary_before": primary_before,
            "started_at": started_at,
            "completed_at": time.time(),
        }
        return make_result(
            spec,
            ComponentStatus.PASS if ok else ComponentStatus.FAIL,
            f"failover of {target} {'completed' if ok else 'failed'}",
            evidence=(make_evidence(spec, "valkey.failover.result", "action.completed", record),),
        )


# ---------------------------------------------------------------------------
# valkey.action.restart-instance
# ---------------------------------------------------------------------------

_RESTART_FIELDS = {
    "instance": ConfigField(str),
    "wait_ready": ConfigField(bool, default=True),
}


# How long a restart's SIGTERM may take to stop the instance.
_RESTART_EXIT_TIMEOUT_S = 30.0


class RestartAction:
    """Restart one instance (``SIGTERM``, then start it again); a stopped instance is just started.

    The stop is owned until the restart, so a failed restart leaves the instance for the runner to bring back.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        instance_id = select_instance(handle, config["instance"])
        try:
            if handle.is_alive(instance_id):
                own = functools.partial(context.obligations.open, spec)
                stopped = stop_owned(own, handle, instance_id, wait_ready=config["wait_ready"])
                exited = wait_exit_signalling(handle, instance_id, _RESTART_EXIT_TIMEOUT_S)
                # The restart stops a process that is still running, so the instance comes back either way.
                stopped.discharge()
                if not exited:
                    return make_result(
                        spec,
                        ComponentStatus.FAIL,
                        f"{instance_id} did not exit within {_RESTART_EXIT_TIMEOUT_S:g}s of SIGTERM, sent every second",
                    )
            else:
                handle.restart(instance_id, wait_ready=config["wait_ready"])
        except Exception as exc:
            # A restart that did not come up is the harness's to report; valkey.oracle.liveness judges a server
            # that crashed.
            return make_result(
                spec, ComponentStatus.ERROR, f"restart of {instance_id} failed: {type(exc).__name__}: {exc}"
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"restarted {instance_id}",
            evidence=(
                make_evidence(
                    spec,
                    "valkey.instance.restarted",
                    "action.completed",
                    {"instance_id": instance_id, "pid": handle.pid(instance_id)},
                ),
            ),
        )


# ---------------------------------------------------------------------------
# valkey.action.sleep and valkey.action.poll
# ---------------------------------------------------------------------------

_SLEEP_FIELDS = {"seconds": ConfigField(float, minimum=0, maximum=3600)}


class SleepAction:
    """Wait a fixed time, e.g. to let a workload run before a fault triggers on ``<id>.completed``."""

    def run(self, context: Any, spec: ComponentSpec):
        seconds = spec.config["seconds"]
        context.sleep(remaining(context, seconds))
        return make_result(spec, ComponentStatus.PASS, f"waited {seconds}s")


_POLL_FIELDS = {
    "instances": ConfigField((str, list), default="all"),
    # Poll either one INFO field ...
    "section": ConfigField(str, allow_none=True, default=None),
    "field": ConfigField(str, allow_none=True, default=None),
    # ... or a command's reply.
    "command": ConfigField(list, allow_none=True, default=None),
    "predicate": mapping_of(PREDICATE_FIELDS),
    "timeout_s": ConfigField(float, default=60.0, minimum=0.1, maximum=7200),
    "poll_interval_s": ConfigField(float, default=0.1, minimum=0.001, maximum=60),
}


def _validate_poll(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    info = config["section"] is not None or config["field"] is not None
    if info == (config["command"] is not None) or (info and not (config["section"] and config["field"])):
        raise ConfigError("poll needs either section and field, or command")
    if config["command"] is not None:
        validate_command(config["command"])
    validate_predicate(config["predicate"], "predicate")


class PollAction:
    """Poll an INFO field or a command reply on every selected instance until a predicate holds.

    At the timeout, an instance read and seen not to hold it is a FAIL. When
    every instance that does not hold it could not be read (the last read
    errored), nothing was seen, so the action is ERROR.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        instances = select_instances(handle, config["instances"])
        what = (
            f"{config['section']}.{config['field']}"
            if config["command"] is None
            else " ".join(str(item) for item in config["command"])
        )

        def read(instance_id: str) -> Any:
            with handle.new_client(instance_id, timeout=2.0) as client:
                if config["command"] is None:
                    return client.info(config["section"]).get(config["field"])
                return jsonable(client.execute_command(*[str(item) for item in config["command"]]))

        def check() -> tuple[bool, tuple[dict[str, Any], list[str], list[str]]]:
            values: dict[str, Any] = {}
            unsatisfied = []
            unread = []
            for instance_id in instances:
                try:
                    values[instance_id] = read(instance_id)
                except Exception as exc:
                    values[instance_id] = f"error: {exc}"
                    unread.append(instance_id)
                    continue
                if not compare(values[instance_id], config["predicate"]):
                    unsatisfied.append(instance_id)
            return not (unsatisfied or unread), (values, unsatisfied, unread)

        satisfied, (last, unsatisfied, unread) = poll_until(
            check, timeout_s=config["timeout_s"], interval_s=config["poll_interval_s"], context=context
        )
        if not satisfied:
            return make_result(
                spec,
                ComponentStatus.FAIL if unsatisfied else ComponentStatus.ERROR,
                f"{what} not satisfied before timeout"
                if unsatisfied
                else f"cannot read {what} on {', '.join(unread)} before timeout",
                diagnostics={"last_values": last},
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{what} satisfied on {len(instances)} instance(s)",
            evidence=(make_evidence(spec, "valkey.poll.satisfied", "action.completed", last),),
        )


# ---------------------------------------------------------------------------
# valkey.action.wait-event
# ---------------------------------------------------------------------------

_WAIT_EVENT_FIELDS = {
    "event": ConfigField(str),
    "min_count": ConfigField(int, default=1, minimum=1, maximum=1_000_000),
    "timeout_s": ConfigField(float, default=120.0, minimum=0.01, maximum=7200),
    "poll_interval_s": ConfigField(float, default=0.02, minimum=0.001, maximum=10),
    # Wait only if this event has already been dispatched (e.g. wait for a
    # restart only when the fault that kills the instance actually fired).
    "only_after": ConfigField(str, allow_none=True, default=None),
}


class WaitEventAction:
    """Block until an event (e.g. ``writes.drained``) has been dispatched ``min_count`` times.

    It only synchronizes: an event that did not come in time (a workload
    still draining on a slow host, a restart still running) says nothing
    about the server, so the timeout is ERROR, never FAIL.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        if config["only_after"] and not context.event_bus.committed_history(config["only_after"]):
            return make_result(
                spec, ComponentStatus.PASS, f"not waiting for {config['event']}: {config['only_after']} not observed"
            )

        def observed() -> tuple[bool, tuple]:
            events = context.event_bus.committed_history(config["event"])
            return len(events) >= config["min_count"], events

        seen, events = poll_until(
            observed, timeout_s=config["timeout_s"], interval_s=config["poll_interval_s"], context=context
        )
        if not seen:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"{config['event']} observed {len(events)}/{config['min_count']} times before timeout",
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"observed {config['event']} x{len(events)}",
            evidence=(
                make_evidence(
                    spec,
                    "valkey.wait.event-observed",
                    "action.completed",
                    {"event": config["event"], "count": len(events)},
                    event_sequence=events[config["min_count"] - 1].sequence,
                ),
            ),
        )


# ---------------------------------------------------------------------------
# valkey.action.replicaof
# ---------------------------------------------------------------------------

_REPLICAOF_FIELDS = {
    "instance": ConfigField(str),
    # The primary to follow; null makes the instance a primary (REPLICAOF NO ONE).
    "primary": ConfigField(str, allow_none=True, default=None),
    "wait_linked": ConfigField(bool, default=True),
    "timeout_s": ConfigField(float, default=60.0, minimum=0.1, maximum=3600),
}


class ReplicaofAction:
    """Point one instance at a primary, or make it a primary, and wait until that took effect."""

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        handle = deployment(context)
        instance_id = select_instance(handle, config["instance"])
        primary_id = None
        if config["primary"] is not None:
            candidates = [item for item in select_instances(handle, config["primary"]) if item != instance_id]
            if not candidates:
                return make_result(
                    spec,
                    ComponentStatus.ERROR,
                    f"no primary other than {instance_id} matches {config['primary']!r}; name another instance",
                )
            primary_id = candidates[0]
        sent_at = time.time()
        try:
            handle.replicate(instance_id, primary_id)
        except Exception as exc:
            # The command did not go through (a timeout, a lost connection): nothing was seen of Valkey.
            return make_result(
                spec, ComponentStatus.ERROR, f"REPLICAOF on {instance_id} failed: {type(exc).__name__}: {exc}"
            )
        record = {"instance_id": instance_id, "primary_id": primary_id, "sent_at": sent_at}
        if config["wait_linked"]:
            target = handle.instance(primary_id) if primary_id else None

            def took_effect() -> tuple[bool, Optional[dict]]:
                info = replication_info(handle, instance_id, 2.0)
                return (is_primary(info) if target is None else follows(info, target)), info

            done, info = poll_until(took_effect, timeout_s=config["timeout_s"], interval_s=0.1, context=context)
            if not done:
                outcome = f"linked to {primary_id}" if primary_id else "a primary"
                if info is None:
                    # The last look could not read the instance: whether it took effect is unknown.
                    return make_result(
                        spec,
                        ComponentStatus.ERROR,
                        f"cannot tell whether {instance_id} was {outcome} within {config['timeout_s']}s: "
                        "it did not answer INFO replication",
                    )
                return make_result(
                    spec, ComponentStatus.FAIL, f"{instance_id} was not {outcome} within {config['timeout_s']}s"
                )
            record["took_effect_at"] = time.time()
        handle.refresh_roles()
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{instance_id} now replicates {primary_id}" if primary_id else f"{instance_id} is now a primary",
            evidence=(make_evidence(spec, "valkey.replication.replicaof", "action.completed", record),),
        )


_REPLICAOF_DECLARES = Declarations(
    evidence={
        "valkey.replication.replicaof": declared(
            mapping_of(
                {
                    "instance_id": TEXT,
                    "primary_id": nullable(str),
                    "sent_at": NUMBER,
                    "took_effect_at": optional(float),
                }
            )
        )
    }
)


# ---------------------------------------------------------------------------
# valkey.action.rolling-upgrade
# ---------------------------------------------------------------------------

_UPGRADE_FIELDS = {
    # Usually ``{$ref: artifacts.<id>.binary}`` passed in through an experiment input.
    "to_binary": ConfigField(str, reference=True),
    "failover": ConfigField(bool, default=True),
    "settle_s": ConfigField(float, default=1.0, minimum=0, maximum=600),
    "step_timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=3600),
}


def _validate_upgrade(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if plan.deployment_mode == "standalone" and plan.deployment["replicas_per_primary"] == 0:
        raise ConfigError("a rolling upgrade of a standalone deployment needs replicas_per_primary >= 1")


class _UpgradeFailedError(RuntimeError):
    """A rolling-upgrade step saw Valkey do something wrong; any other exception is the harness's (ERROR)."""


def _roles_are(handle: Any, expected: dict[str, str]) -> Optional[bool]:
    """Whether every instance has its ``expected`` :func:`instance_role`: False once one answered with another,
    None when one did not answer."""

    roles = {instance_id: instance_role(handle, instance_id) for instance_id in expected}
    if any(role is not None and role != expected[instance_id] for instance_id, role in roles.items()):
        return False
    return None if None in roles.values() else True


def _wait(context: Any, handle: Any, expected: dict[str, str], timeout_s: float, failure: str) -> None:
    """Wait until the instances have their ``expected`` roles; a last look that could not read them is the
    harness's (ERROR), not Valkey's."""

    def check() -> tuple[bool, Optional[bool]]:
        done = _roles_are(handle, expected)
        return done is True, done

    done, last = poll_until(check, timeout_s=timeout_s, context=context)
    if not done:
        if last is None:
            raise RuntimeError(f"{failure}: an instance did not answer INFO replication")
        raise _UpgradeFailedError(failure)


class RollingUpgradeAction:
    """Move every instance to ``to_binary`` one at a time, replicas first.

    Per shard: restart each replica on the new build, gracefully fail over to
    an upgraded replica (``CLUSTER FAILOVER`` or ``FAILOVER TO``), then
    restart the former primary on the new build. Without replicas, or with
    ``failover: false``, the primary restarts in place (expected downtime).
    Publishes ``<id>.instance-upgraded`` per instance and
    ``<id>.failed-over`` per failover.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = {**spec.config, "target": binary_version(spec.config["to_binary"])}
        handle = deployment(context)
        cluster = is_cluster(handle)
        steps: list[dict[str, Any]] = []
        try:
            handle.refresh_roles()
            for shard in sorted({instance.shard_id for instance in handle.instances}):
                members = [instance.instance_id for instance in handle.instances if instance.shard_id == shard]
                roles = handle.roles()
                primaries = [instance_id for instance_id in members if roles.get(instance_id) == "primary"]
                if len(primaries) != 1:
                    raise RuntimeError(f"shard {shard} has {len(primaries)} live primaries: {primaries}")
                primary = primaries[0]
                replicas = [instance_id for instance_id in members if instance_id != primary]
                for replica in replicas:
                    self._upgrade(context, spec, handle, replica, config, steps)
                if config["failover"] and replicas:
                    self._failover(context, spec, handle, primary, replicas, cluster, config, steps)
                self._upgrade(context, spec, handle, primary, config, steps)
        except _UpgradeFailedError as exc:
            return make_result(
                spec, ComponentStatus.FAIL, f"rolling upgrade failed: {exc}", diagnostics={"steps": steps}
            )
        except Exception as exc:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"rolling upgrade could not run: {type(exc).__name__}: {exc}",
                diagnostics={"steps": steps},
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"upgraded {sum(step['kind'] == 'restart' for step in steps)} instance(s) to {config['target']['version']} "
            f"with {sum(step['kind'] == 'failover' for step in steps)} failover(s)",
            evidence=(
                make_evidence(
                    spec, "valkey.upgrade.completed", "action.completed", {"target": config["target"], "steps": steps}
                ),
            ),
            diagnostics={"steps": steps},
        )

    @staticmethod
    def _upgrade(context: Any, spec: ComponentSpec, handle: Any, instance_id: str, config: dict, steps: list) -> None:
        if os.path.realpath(handle.binary(instance_id)) == os.path.realpath(config["to_binary"]):
            steps.append({"kind": "skip", "instance_id": instance_id, "reason": "already on target"})
            return
        before = instance_identity(handle, instance_id)
        role = handle.refresh_roles().get(instance_id)
        # The new binary is meant to last; only the stop is undone.
        handle.set_binary(instance_id, config["to_binary"])
        stopped = stop_owned(functools.partial(context.obligations.open, spec), handle, instance_id, wait_ready=True)
        if not wait_exit_signalling(handle, instance_id, config["step_timeout_s"]):
            raise _UpgradeFailedError(f"{instance_id} did not exit within {config['step_timeout_s']}s")
        started = time.time()
        stopped.discharge()
        after = instance_identity(handle, instance_id)
        expected_version = config["target"].get("version")
        if expected_version and after["version"] != expected_version:
            # The harness put the binary in place and restarted it: a process on another build is its own doing.
            raise RuntimeError(f"{instance_id} restarted on {after['version']}, expected {expected_version}")
        steps.append(
            {
                "kind": "restart",
                "instance_id": instance_id,
                "before": before,
                "after": after,
                "seconds": round(time.time() - started, 3),
            }
        )
        if role == "replica":
            # A replica must be linked and synced before it can take over.
            _wait(
                context,
                handle,
                {instance_id: "replica"},
                config["step_timeout_s"],
                f"{instance_id} did not resynchronize within {config['step_timeout_s']}s",
            )
        context.event_bus.publish(f"{spec.id}.instance-upgraded", source=spec.id, payload={"instance_id": instance_id})
        if config["settle_s"] and not context.sleep(config["settle_s"]):
            raise RuntimeError("the rolling upgrade was cancelled")

    @staticmethod
    def _failover(
        context: Any,
        spec: ComponentSpec,
        handle: Any,
        primary: str,
        replicas: list,
        cluster: bool,
        config: dict,
        steps: list,
    ) -> None:
        target = replicas[0]
        started = time.time()
        if cluster:
            with handle.new_client(target, timeout=15.0) as client:
                client.execute_command("CLUSTER", "FAILOVER")
            _wait(
                context,
                handle,
                {target: "primary", primary: "replica"},
                config["step_timeout_s"],
                f"failover from {primary} to {target} did not complete",
            )
        else:
            done, error = graceful_failover(context, handle, primary, target, config["step_timeout_s"])
            if not done:
                failure = f"failover from {primary} to {target} did not complete within {config['step_timeout_s']}s"
                failure += f": {error}" if error else ""
                if done is None:
                    raise RuntimeError(f"{failure}, and the harness could not tell whether Valkey started it")
                raise _UpgradeFailedError(failure)
            # Other replicas keep following the demoted primary; point them at
            # the new one so replication stays a single-level star.
            for other in replicas[1:]:
                handle.replicate(other, target)
            following, status = wait_following(
                context, handle, [primary, *replicas[1:]], target, config["step_timeout_s"]
            )
            if not following:
                failure = f"replicas did not follow the new primary {target} in time: {status}"
                if False not in status.values():
                    raise RuntimeError(f"{failure}; an instance did not answer INFO replication")
                raise _UpgradeFailedError(failure)
        handle.refresh_roles()
        steps.append({"kind": "failover", "from": primary, "to": target, "seconds": round(time.time() - started, 3)})
        context.event_bus.publish(f"{spec.id}.failed-over", source=spec.id, payload={"from": primary, "to": target})


# One reply per instance and attempt: the reply, or the error it raised.
_REPLY = mapping_of(
    {
        "instance_id": TEXT,
        "attempt": INTEGER,
        "at": NUMBER,
        "reply": optional(object),
        "error": optional(str),
        # No reply came back (see send_to).
        "unanswered": optional(bool),
        "matched": FLAG,
    }
)
_COMMAND_DECLARES = Declarations(evidence={"valkey.command.replies": declared(ConfigField(list, items=_REPLY))})

_BGSAVE_DECLARES = Declarations(
    events={
        "{id}.save-started": ConfigSchema({"instances": LIST, "accepted": LIST}),
        "{id}.save-completed": ConfigSchema({"statuses": MAPPING}),
    },
    evidence={
        "valkey.bgsave.result": declared(
            mapping_of(
                {
                    "instance_id": TEXT,
                    "reply": ANY,
                    "status": ConfigField(str, choices=("ok", "err", "not_completed", "timeout", "rejected", *_UNSEEN)),
                    "observed_types": LIST,
                    "before": MAPPING,
                    "after": MAPPING,
                    "rdb_path": TEXT,
                    "error": nullable(str),
                    "role": ConfigField(str, allow_none=True, choices=("primary", "replica")),
                    "sent_at": nullable(float),
                    "replied_at": nullable(float),
                    "finished_at": nullable(float),
                    "rdb_artifact": optional(str),
                    "capture_error": optional(str),
                    "rdb_format": optional(dict),
                }
            ),
            many=True,
        )
    },
)

_FAILOVER_DECLARES = Declarations(
    evidence={
        "valkey.failover.result": declared(
            mapping_of(
                {
                    "target": TEXT,
                    "force": FLAG,
                    "success": FLAG,
                    "roles": MAPPING,
                    "primary_before": nullable(str),
                    "started_at": NUMBER,
                    "completed_at": NUMBER,
                }
            )
        )
    }
)

_RESTART_DECLARES = Declarations(
    evidence={"valkey.instance.restarted": declared(mapping_of({"instance_id": TEXT, "pid": nullable(int)}))},
    changes=("process.stopped",),
)

# {instance_id: the last value read}
_POLL_DECLARES = Declarations(evidence={"valkey.poll.satisfied": declared(mapping_of(additional=ANY))})

_WAIT_EVENT_DECLARES = Declarations(
    evidence={"valkey.wait.event-observed": declared(mapping_of({"event": TEXT, "count": INTEGER}))}
)

_UPGRADE_DECLARES = Declarations(
    events={
        "{id}.instance-upgraded": ConfigSchema({"instance_id": TEXT}),
        "{id}.failed-over": ConfigSchema({"from": TEXT, "to": TEXT}),
    },
    evidence={"valkey.upgrade.completed": declared(mapping_of({"target": MAPPING, "steps": LIST}))},
    changes=("process.stopped",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.action.command",
        CommandAction,
        _COMMAND_FIELDS,
        check=_validate_command,
        selectors={"instances": ()},
        declares=_COMMAND_DECLARES,
    ),
    component_registration(
        "valkey.action.bgsave",
        BgsaveAction,
        _BGSAVE_FIELDS,
        selectors={"instances": ()},
        declares=_BGSAVE_DECLARES,
    ),
    component_registration(
        "valkey.action.failover",
        FailoverAction,
        _FAILOVER_FIELDS,
        check=_validate_failover,
        selectors={"target": ()},
        deployment_modes=("cluster",),
        declares=_FAILOVER_DECLARES,
    ),
    component_registration(
        "valkey.action.restart-instance",
        RestartAction,
        _RESTART_FIELDS,
        selectors={"instance": ()},
        declares=_RESTART_DECLARES,
    ),
    component_registration("valkey.action.sleep", SleepAction, _SLEEP_FIELDS, declares=Declarations()),
    component_registration(
        "valkey.action.poll",
        PollAction,
        _POLL_FIELDS,
        check=_validate_poll,
        selectors={"instances": ()},
        declares=_POLL_DECLARES,
    ),
    component_registration(
        "valkey.action.wait-event", WaitEventAction, _WAIT_EVENT_FIELDS, declares=_WAIT_EVENT_DECLARES
    ),
    component_registration(
        "valkey.action.rolling-upgrade",
        RollingUpgradeAction,
        _UPGRADE_FIELDS,
        check=_validate_upgrade,
        deployment_modes=("standalone", "cluster"),
        declares=_UPGRADE_DECLARES,
    ),
    component_registration(
        "valkey.action.replicaof",
        ReplicaofAction,
        _REPLICAOF_FIELDS,
        selectors={"instance": (), "primary": ()},
        deployment_modes=("standalone",),
        declares=_REPLICAOF_DECLARES,
    ),
)
