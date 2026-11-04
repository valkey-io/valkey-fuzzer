"""Built-in actions: commands, BGSAVE, failover, REPLICAOF, restart, rolling upgrade, polls, and waits.

The runner publishes ``<id>.started`` and ``<id>.completed``
around every action, so a fault can trigger on, and an experiment can require, any
action finishing. Events an action publishes itself are also named after its
component (``<id>.save-started``), so they never collide.
"""

import base64
import functools
import hashlib
import json
import math
import os
import shutil
import socket
import subprocess
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

import valkey

from valkey_fuzzer._data import canonical_json, jsonable
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.queries import replication_role
from valkey_fuzzer.deployment.local.server_config import MANAGED_SERVER_OPTIONS, canonicalize_server_config
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import BASE64_KEY, ConfigError, ConfigField, ConfigSchema, plain_config

from .capabilities import resolve_binary, starts_with
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
from .oracles import CRASH_PATTERNS, crash_cause, fatal_exit, scan_for_crashes
from .polling import poll_until, read_retrying, remaining
from .predicates import PREDICATE_FIELDS, compare, validate_predicate
from .processes import stop_owned, wait_exit_signalling
from .registration import component_registration, validate_command
from .schemas import ANY, FLAG, INTEGER, LIST, MAPPING, NUMBER, TEXT, declared, mapping_of, nullable, optional
from .server import PrivateServer, binary_version, instance_identity, instance_role, server_arguments

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
# valkey.action.commands
# ---------------------------------------------------------------------------

_COMPARE_MODES = ("equal", "unordered", "unordered-pairs", "json", "skip")

# The predicates of valkey.action.command; ``equals`` also takes a list or a mapping, compared as JSON data.
_SEQUENCE_EXPECT = {
    **PREDICATE_FIELDS,
    "equals": optional((str, int, float, bool, list, dict)),
    "error": optional((bool, str)),
}
_SEQUENCE_COMMAND = mapping_of(
    {
        "command": ConfigField(list),
        "expect": mapping_of(_SEQUENCE_EXPECT, allow_none=True, default=None),
        # The change under test means this reply to differ from the parent's.
        "expect_difference": ConfigField(bool, default=False),
        "compare": ConfigField(str, default="equal", choices=_COMPARE_MODES),
    }
)
_COMMANDS_FIELDS = {
    "instances": ConfigField((str, list), default="primaries"),
    # A parent build, usually {$ref: artifacts.parent.binary}: the sequence then runs on two private servers,
    # one of the deployment's binary and one of this, never on the deployment's instances.
    "compare_with": ConfigField(str, allow_none=True, default=None, reference=True),
    # Module builds both private servers load (--loadmodule each), usually artifact references; the parent
    # loads modules_parent instead when it is given. modules_parent alone, without compare_with, compares two
    # builds of a module on one Valkey build: both servers run the deployment's binary.
    "modules": ConfigField(list, default=list, reference=True, items=ConfigField(str, reference=True)),
    "modules_parent": ConfigField(
        list, allow_none=True, default=None, reference=True, items=ConfigField(str, reference=True)
    ),
    # Both private servers run as a one-node cluster serving every slot.
    "cluster": ConfigField(bool, default=False),
    # Both private servers start with it; an option the parent rejects is dropped on both sides.
    "server_config": ConfigField(dict, default=dict),
    "resp": ConfigField(int, default=2, choices=(2, 3)),
    "timeout_s": ConfigField(float, default=10.0, minimum=0.1, maximum=3600),
    "commands": ConfigField(list, items=_SEQUENCE_COMMAND),
}

# Refused in every sequence: they end or restart the server the sequence talks to, or hold its connection.
_REFUSED = {
    "SHUTDOWN": "it stops the server",
    "MONITOR": "it holds the connection",
    "SUBSCRIBE": "it holds the connection",
    "PSUBSCRIBE": "it holds the connection",
    "SSUBSCRIBE": "it holds the connection",
}
_REFUSED_DEBUG = ("RESTART", "CRASH-AND-RECOVER", "RELOAD")
_REFUSED_MODULE = ("LOAD", "LOADEX", "UNLOAD")
# Refused on the deployment's instances, whose topology the deployment owns; allowed on the private servers.
_REFUSED_ON_INSTANCES = ("REPLICAOF", "SLAVEOF", "CLUSTER", "SYNC", "PSYNC", "REPLCONF")
# Commands a refusal names with their subcommand.
_CONTAINER_COMMANDS = ("DEBUG", "MODULE", "CONFIG", "CLUSTER")
# What both private servers run with unless server_config sets it: a crafted RESTORE payload that crashes the
# server is a real bug, and DEBUG is allowed (a DEBUG SEGFAULT crashes the server it is sent to, which is the
# test). An option the parent rejects is dropped on both sides, these included.
_PRIVATE_SERVER_DEFAULTS = {"sanitize-dump-payload": "yes", "enable-debug-command": "yes"}
# How long a private server may take to start, and how long a lost connection waits for its process to exit
# before the loss counts as anything but a crash.
_START_TIMEOUT_S = 15.0
_EXIT_WAIT_S = 5.0


def _is_binary(item: Any) -> bool:
    """Whether ``item`` is a binary argument, ``{"$base64": ...}``, as YAML ``!!binary`` loads."""

    return isinstance(item, Mapping) and set(item) == {BASE64_KEY} and isinstance(item[BASE64_KEY], str)


def _validate_sequence_command(command: Any, name: str) -> list[str]:
    """``command`` is a non-empty list of strings, numbers, and binary arguments; its words, upper-cased (a
    binary argument is an empty word)."""

    if not command or not all(
        _is_binary(item) or (isinstance(item, (str, int, float)) and not isinstance(item, bool)) for item in command
    ):
        raise ConfigError(f"{name} must be a non-empty list of strings, numbers, or !!binary values")
    return ["" if _is_binary(item) else str(item).upper() for item in command]


def _refusal(words: list[str], timeout_s: float, on_instances: bool) -> Optional[str]:
    """Why a command with these ``words`` is refused, or None."""

    name, rest = words[0], words[1:]
    if name in _REFUSED:
        return _REFUSED[name]
    if name == "DEBUG" and rest:
        if rest[0] in _REFUSED_DEBUG:
            return "it ends or restarts the server"
        if rest[0] == "SLEEP" and len(rest) > 1:
            try:
                if float(rest[1]) > timeout_s:
                    return f"it sleeps longer than timeout_s ({timeout_s:g}s)"
            except ValueError:
                pass
    if name == "MODULE" and rest and rest[0] in _REFUSED_MODULE:
        return "it loads or unloads a module"
    if name == "CONFIG" and rest and rest[0] == "SET":
        managed = [option for option in rest[1::2] if option.lower() in MANAGED_SERVER_OPTIONS]
        if managed:
            return f"{managed[0].lower()} is an option the harness manages"
    if on_instances and name in _REFUSED_ON_INSTANCES:
        return "it changes the deployment's topology; a sequence with compare_with may send it"
    return None


def _two_servers(config: Mapping[str, Any]) -> bool:
    """Whether the sequence runs on two private servers: with a parent build, or a parent build of the modules."""

    return config["compare_with"] is not None or config["modules_parent"] is not None


def _validate_commands(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    compared = _two_servers(config)
    if config["cluster"] and not compared:
        raise ConfigError(
            "cluster needs compare_with or modules_parent: on the deployment's instances the deployment decides the mode"
        )
    if config["modules"] and not compared:
        raise ConfigError(
            "modules needs compare_with or modules_parent: the deployment's instances load modules through "
            "deployment.server_config.loadmodule"
        )
    if config["modules_parent"] is not None and not config["modules"]:
        raise ConfigError("modules_parent needs modules: it replaces the candidate's modules on the parent")
    try:
        server_config = canonicalize_server_config(config["server_config"])
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"server_config: {exc}") from exc
    if "loadmodule" in server_config:
        raise ConfigError("server_config.loadmodule: list the modules in modules (and modules_parent)")
    if not config["commands"]:
        raise ConfigError("commands must list at least one command")
    for index, item in enumerate(config["commands"]):
        where = f"commands[{index}]"
        words = _validate_sequence_command(item["command"], f"{where}.command")
        refused = _refusal(words, config["timeout_s"], not compared)
        if refused is not None:
            named = " ".join(words[:2]) if words[0] in _CONTAINER_COMMANDS and len(words) > 1 else words[0]
            raise ConfigError(f"{where}: {named} is refused: {refused}")
        if item["expect"] is not None:
            if "error" in item["expect"]:
                if set(item["expect"]) != {"error"}:
                    raise ConfigError(f"{where}.expect.error cannot be combined with a reply predicate")
            else:
                validate_predicate(item["expect"], f"{where}.expect")
        if item["expect_difference"]:
            if not compared:
                raise ConfigError(f"{where}: expect_difference needs compare_with or modules_parent")
            if item["expect"] is None:
                raise ConfigError(f"{where}: a command marked expect_difference needs an expect")


def _argument(item: Any) -> Any:
    return base64.b64decode(item[BASE64_KEY]) if _is_binary(item) else item


def _text(value: bytes) -> Any:
    """A bulk string as the evidence records it: the string when it is valid UTF-8, else ``{"$base64": ...}``."""

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return {BASE64_KEY: base64.b64encode(value).decode("ascii")}


def raw_reply(value: Any) -> Any:
    """A reply as the connection parsed it, as JSON data: bytes through :func:`_text`, a RESP3 set sorted, an
    error inside an array (a transaction's) as ``{"$error": ...}``, and nothing reshaped."""

    if isinstance(value, bytes):
        return _text(value)
    if isinstance(value, (list, tuple)):
        return [raw_reply(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((raw_reply(item) for item in value), key=canonical_json)
    if isinstance(value, Mapping):
        keys = ((_text(key) if isinstance(key, bytes) else key) for key in value)
        return {
            (f"{BASE64_KEY}:{key[BASE64_KEY]}" if isinstance(key, dict) else str(key)): raw_reply(item)
            for key, item in zip(keys, value.values())
        }
    if isinstance(value, valkey.exceptions.ResponseError):
        return {"$error": str(value)}
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    return str(value)


class RawConnection:
    """One connection for a whole sequence: arguments go as bytes, replies come back as the server sent them
    (``decode_responses=False``, no response callbacks), and the protocol is the ``resp`` the sequence asks."""

    def __init__(self, connection: Any):
        self.connection = connection

    def send(self, command: Sequence[Any]) -> dict[str, Any]:
        """``{"reply": ...}``, the server's error as ``{"error": ...}``, or a lost connection or a reply that
        timed out as ``{"error": ..., "unanswered": True}``."""

        try:
            self.connection.send_command(*[_argument(item) for item in command])
            return {"reply": raw_reply(self.connection.read_response())}
        except valkey.exceptions.ResponseError as exc:
            return {"error": str(exc)}
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
            return {"error": f"{type(exc).__name__}: {exc}", "unanswered": True}

    def close(self) -> None:
        try:
            self.connection.disconnect()
        except Exception:
            pass


def open_raw_connection(
    resp: int, timeout_s: float, *, path: Optional[str] = None, host: Any = None, port: Any = None
) -> RawConnection:
    """A :class:`RawConnection` to the unix socket ``path``, or to ``host:port``, connected."""

    options = {"socket_timeout": timeout_s, "socket_connect_timeout": timeout_s, "protocol": resp}
    if path is not None:
        connection = valkey.connection.UnixDomainSocketConnection(path=path, **options)
    else:
        connection = valkey.connection.Connection(host=str(host), port=int(port), **options)
    connection.connect()
    return RawConnection(connection)


def _run_sequence(connection: RawConnection, commands: Sequence[Mapping[str, Any]], context: Any) -> list[dict]:
    """Every command in order on ``connection``, each with its index; it stops at the first that got no reply,
    since the connection's state is unknown after it, and when the call is cancelled."""

    replies: list[dict[str, Any]] = []
    for index, item in enumerate(commands):
        if context.cancelled:
            break
        record = {"index": index, **connection.send(item["command"])}
        replies.append(record)
        if record.get("unanswered"):
            break
    return replies


def _json_value(item: Any) -> Any:
    """A bulk string of JSON text as the canonical JSON of the value it holds (``{"$json": ...}``), else ``item``.

    Canonical text, not the parsed data, so ``true`` and ``1`` stay different (Python's ``True == 1``); a number
    with a fraction or an exponent compares by its value (``1.0`` as ``1``), and one too large for a float, or
    NaN, stays its text.
    """

    if not isinstance(item, str):
        return item

    def number(text: str) -> Any:
        value = float(text)
        if not math.isfinite(value):
            return text
        return int(value) if value.is_integer() and abs(value) < 2**53 else value

    try:
        return {"$json": canonical_json(json.loads(item, parse_float=number, parse_constant=str))}
    except ValueError:
        return item


def _compared(record: Mapping[str, Any], mode: str) -> Any:
    """What a reply record compares as under ``mode``: the reply, its items sorted (``unordered``), its
    field-value pairs sorted (``unordered-pairs``; a RESP3 map compares as a map), or the JSON a bulk string (or
    each item of an array) holds (``json``, so two JSON texts of the same value compare equal); an error as
    itself."""

    if "reply" not in record:
        return {key: value for key, value in record.items() if key != "index"}
    reply = record["reply"]
    if mode == "json":
        reply = [_json_value(item) for item in reply] if isinstance(reply, list) else _json_value(reply)
    elif isinstance(reply, list):
        if mode == "unordered":
            reply = sorted(reply, key=canonical_json)
        elif mode == "unordered-pairs" and len(reply) % 2 == 0:
            reply = sorted((reply[index : index + 2] for index in range(0, len(reply), 2)), key=canonical_json)
    return {"reply": reply}


def _sequence_matches(expect: Optional[Mapping[str, Any]], record: Mapping[str, Any]) -> bool:
    expect = None if expect is None else plain_config(expect)
    if expect is not None and isinstance(expect.get("equals"), (list, dict)):
        return "reply" in record and canonical_json(record["reply"]) == canonical_json(expect["equals"])
    return _matches(expect, record.get("reply"), record.get("error"))


def _side(replies: Sequence[Mapping[str, Any]], commands: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """One server's evidence: its replies and the SHA-256 of the canonical JSON of the compared ones."""

    compared = [
        _compared(record, commands[record["index"]]["compare"])
        for record in replies
        if commands[record["index"]]["compare"] != "skip"
    ]
    return {"replies": list(replies), "digest": hashlib.sha256(canonical_json(compared).encode()).hexdigest()}


def _command_text(command: Sequence[Any]) -> str:
    return " ".join("<binary>" if _is_binary(item) else str(item) for item in command)


class _Server:
    """One private server of a compared sequence: started, owned until stopped, its logs kept."""

    def __init__(self, label: str, binary: str):
        self.label, self.binary = label, binary
        # The server options of this server alone (its cluster port), beside the shared server_config.
        self.options: dict[str, Any] = {}
        self.server: Any = None
        self.owned: Any = None
        self.connection: Optional[RawConnection] = None
        self.replies: list[dict[str, Any]] = []
        self.exited: Optional[int] = None
        self.logs: list[str] = []

    def start(
        self, context: Any, spec: ComponentSpec, server_config: Mapping[str, Any], resp: int, timeout_s: float
    ) -> str:
        """Start it with ``server_config`` and its own options, and connect; what went wrong, or an empty string."""

        arguments = server_arguments({**server_config, **self.options})
        self.server = PrivateServer(self.binary, arguments, timeout_s=_START_TIMEOUT_S, prefix="vfc-")
        self.owned = context.obligations.open(spec, "process.private-server", self.server.workdir, self.server.stop)
        try:
            # PrivateServer.start polls with time.sleep: not cancellable, but bounded by its timeout.
            exit_output = self.server.start()
        except (OSError, TimeoutError) as exc:
            return f"the {self.label} server did not start: {exc}"
        if exit_output is not None:
            return f"the {self.label} server exited at startup: {exit_output[-500:]}"
        try:
            self.connection = open_raw_connection(resp, timeout_s, path=self.server.socket_path)
        except (valkey.exceptions.ValkeyError, OSError) as exc:
            return f"cannot connect to the {self.label} server with RESP{resp}: {type(exc).__name__}: {exc}"
        return ""

    def finish(self, context: Any, spec: ComponentSpec) -> None:
        """Note whether the process exited (a lost connection waits for it), keep its logs, stop it."""

        if self.connection is not None:
            self.connection.close()
        if self.server is not None and self.server.process is not None:
            if self.replies and self.replies[-1].get("unanswered"):
                try:
                    self.server.process.wait(timeout=_EXIT_WAIT_S)
                except subprocess.TimeoutExpired:
                    pass
            self.exited = self.server.process.poll()
            directory = component_artifact_dir(context, spec)
            for name, path in (("server.log", self.server.log_path), ("output.log", self.server.output_path)):
                destination = os.path.join(directory, f"{self.label}.{name}")
                try:
                    shutil.copyfile(path, destination)
                    self.logs.append(destination)
                except OSError:
                    pass
        if self.owned is not None:
            self.owned.discharge()

    def crash_cause(self) -> str:
        """Why it exited, from its kept logs: the crash report's cause, else its fatal line, else its status."""

        reports = scan_for_crashes({path: path for path in self.logs}, CRASH_PATTERNS)
        if reports:
            return crash_cause(next(iter(reports.values())))
        fatal = next((line for line in map(fatal_exit, self.logs) if line), None)
        return fatal or f"exited with status {self.exited}"


def _free_ports(count: int) -> list[int]:
    """``count`` distinct TCP ports free on the loopback address right now."""

    ports: list[int] = []
    while len(ports) < count:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in ports:
            ports.append(port)
    return ports


def _cluster_ready(connection: RawConnection, context: Any, timeout_s: float) -> Optional[str]:
    """Give a one-node cluster every slot and wait until its state is ok; why that did not happen, or None."""

    added = connection.send(["CLUSTER", "ADDSLOTSRANGE", "0", "16383"])
    if "reply" not in added:
        return f"CLUSTER ADDSLOTSRANGE failed: {added.get('error')}"

    def state_ok() -> tuple[bool, Any]:
        info = connection.send(["CLUSTER", "INFO"])
        return "cluster_state:ok" in str(info.get("reply", "")), info

    done, last = poll_until(state_ok, timeout_s=timeout_s, interval_s=0.1, context=context)
    return None if done else f"cluster_state is not ok within {timeout_s:g}s: {last}"


class CommandsAction:
    """Send a command sequence on one connection, and with ``compare_with`` compare the replies of two builds.

    With ``compare_with`` the sequence runs on two private servers the action starts from an empty dataset
    with the same ``server_config`` (``sanitize-dump-payload yes`` unless set, and ``cluster-enabled yes``
    with ``cluster``), one of the deployment's binary (the candidate) and one of ``compare_with`` (the
    parent); the deployment's instances are never touched. An option the parent rejects is dropped on both
    sides and recorded. Only the candidate is judged: a reply that violates its ``expect``, or a candidate
    that exits, is FAIL. A difference between the builds is never a FAIL by itself: a marked command whose
    replies are equal leaves the run ERROR ``unreached-change``, an unmarked command whose replies differ
    ERROR ``unexplained-difference``; a parent that dies on a marked command reached the change. Without
    ``compare_with`` the sequence runs on each selected instance, and ``expect`` alone judges it.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        if not _two_servers(config):
            return self._run_on_instances(context, spec, config)
        return self._run_compared(context, spec, config)

    def _run_on_instances(self, context: Any, spec: ComponentSpec, config: Mapping[str, Any]):
        handle = deployment(context)
        evidence, problems, unanswered = [], [], []
        for instance_id in select_instances(handle, config["instances"]):
            instance = handle.instance(instance_id)
            try:
                connection = open_raw_connection(
                    config["resp"], config["timeout_s"], host=instance.host, port=instance.port
                )
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return make_result(spec, ComponentStatus.ERROR, f"cannot connect to {instance_id}: {exc}")
            try:
                replies = _run_sequence(connection, config["commands"], context)
            finally:
                connection.close()
            problems.extend(f"{instance_id}: {text}" for text in _violations(config["commands"], replies))
            unanswered.extend(f"{instance_id}: {text}" for text in _unanswered(config["commands"], replies))
            record = _record(config, _side(replies, config["commands"]), None, None, [], [], [])
            record["instance_id"] = instance_id
            evidence.append(make_evidence(spec, "valkey.command.sequence", "action.completed", record))
        status = ComponentStatus.FAIL if problems else ComponentStatus.ERROR if unanswered else ComponentStatus.PASS
        summary = (
            "; ".join([*problems, *unanswered]) or f"{len(config['commands'])} commands on {len(evidence)} instance(s)"
        )
        return make_result(spec, status, summary, evidence=evidence)

    def _run_compared(self, context: Any, spec: ComponentSpec, config: Mapping[str, Any]):
        commands = config["commands"]
        try:
            candidate_binary = resolve_binary(plain_config(context.plan.deployment).get("valkey_binary") or "")
            # Without compare_with, both servers run the deployment's binary and differ by their modules.
            parent_binary = (
                candidate_binary if config["compare_with"] is None else resolve_binary(config["compare_with"])
            )
        except OSError as exc:
            return make_result(spec, ComponentStatus.ERROR, str(exc))
        candidate, parent = _Server("candidate", candidate_binary), _Server("parent", parent_binary)
        server_config = {**_PRIVATE_SERVER_DEFAULTS, **canonicalize_server_config(config["server_config"])}
        if config["cluster"]:
            # Each server needs a cluster bus port of its own; with no TCP port, Valkey would give both 10000.
            ports = iter(_free_ports(2))
            for server in (candidate, parent):
                server.options = {"cluster-enabled": "yes", "cluster-port": next(ports)}
        modules = {
            "candidate": list(config["modules"]),
            "parent": list(config["modules"] if config["modules_parent"] is None else config["modules_parent"]),
        }
        for server in (candidate, parent):
            if modules[server.label]:
                server.options["loadmodule"] = modules[server.label]

        def starts(server: _Server, options: Mapping[str, Any]) -> tuple[ComponentStatus, str]:
            probed = {**options, **server.options}
            status, summary, _ = starts_with(context, spec, server.binary, probed, _START_TIMEOUT_S)
            return status, summary

        # Configuration both builds accept: an option the parent rejects is dropped on both sides.
        dropped: list[str] = []
        if starts(parent, server_config)[0] is ComponentStatus.SKIP:
            dropped = [
                name
                for name, value in server_config.items()
                if starts(parent, {name: value})[0] is ComponentStatus.SKIP
            ]
            server_config = {name: value for name, value in server_config.items() if name not in dropped}
        for server in (parent, candidate):
            status, summary = starts(server, server_config)
            if status is not ComponentStatus.PASS:
                return make_result(
                    spec,
                    ComponentStatus.ERROR,
                    f"the {server.label} build ({server.binary}) does not start with {server_config}"
                    + (f" and modules {modules[server.label]}" if modules[server.label] else "")
                    + ("" if status is ComponentStatus.SKIP else ": the probe could not run")
                    + (f": {summary}" if modules[server.label] else ""),
                )

        try:
            for server in (candidate, parent):
                failed = server.start(context, spec, server_config, config["resp"], config["timeout_s"])
                if not failed and config["cluster"]:
                    failed = _cluster_ready(server.connection, context, config["timeout_s"])
                    failed = failed and f"the {server.label} server: {failed}"
                if failed:
                    return make_result(spec, ComponentStatus.ERROR, failed)
            for server in (candidate, parent):
                server.replies = _run_sequence(server.connection, commands, context)
        finally:
            for server in (candidate, parent):
                server.finish(context, spec)

        reached, unexplained, unreached, uncompared = _compare(commands, candidate, parent)
        record = _record(
            config,
            _side(candidate.replies, commands),
            _side(parent.replies, commands),
            modules,
            dropped,
            reached,
            unexplained,
        )
        for server in (candidate, parent):
            if server.exited is not None:
                record[server.label]["exited"] = server.exited
        evidence = (
            make_evidence(
                spec, "valkey.command.sequence", "action.completed", record, artifacts=[*candidate.logs, *parent.logs]
            ),
        )
        violations = _violations(commands, candidate.replies)
        if candidate.exited is not None:
            cause = candidate.crash_cause()
            at = candidate.replies[-1]["index"] if candidate.replies else None
            where = (
                "before the first command"
                if at is None
                else f"on command {at} ({_command_text(commands[at]['command'])})"
            )
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"the candidate exited {where}: {cause}",
                evidence=evidence,
                diagnostics={"crashes": {"candidate": cause}},
                crash=True,
            )
        if violations:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(violations), evidence=evidence)
        unanswered = _unanswered(commands, candidate.replies) + [
            f"the parent did not answer {text}" for text in uncompared
        ]
        if unanswered:
            return make_result(spec, ComponentStatus.ERROR, "; ".join(unanswered), evidence=evidence)
        if unexplained:
            differences = [
                {
                    "index": index,
                    "command": _command_text(commands[index]["command"]),
                    "candidate": _reply_at(candidate.replies, index),
                    "parent": _reply_at(parent.replies, index),
                }
                for index in unexplained
            ]
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "unmarked commands reply differently on the two builds: "
                + ", ".join(f"{item['index']} ({item['command']})" for item in differences),
                evidence=evidence,
                diagnostics={"reason": "unexplained-difference", "commands": differences},
            )
        if unreached:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "marked commands reply the same on both builds, so the sequence did not reach the change: "
                + ", ".join(f"{index} ({_command_text(commands[index]['command'])})" for index in unreached),
                evidence=evidence,
                diagnostics={"reason": "unreached-change"},
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(commands)} commands on both builds, {len(reached)} marked command(s) reached the change"
            + (f", dropped {', '.join(dropped)}" if dropped else ""),
            evidence=evidence,
        )


def _record(
    config: Mapping[str, Any],
    candidate: dict[str, Any],
    parent: Optional[dict[str, Any]],
    modules: Optional[Mapping[str, list[str]]],
    dropped: Sequence[str],
    reached: Sequence[int],
    unexplained: Sequence[int],
) -> dict[str, Any]:
    return {
        "candidate": candidate,
        "parent": parent,
        "modules": None if modules is None else dict(modules),
        "cluster": config["cluster"],
        "dropped_config": list(dropped),
        "reached": list(reached),
        "unexplained": list(unexplained),
    }


def _without_index(record: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "index"}


def _reply_at(replies: Sequence[Mapping[str, Any]], index: int) -> Optional[dict[str, Any]]:
    return next((_without_index(record) for record in replies if record["index"] == index), None)


def _violations(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[str]:
    """The answered replies that violate their command's ``expect``, described."""

    return [
        f"command {record['index']} ({_command_text(commands[record['index']]['command'])}) replied "
        f"{canonical_json(_without_index(record))}, not {canonical_json(plain_config(commands[record['index']]['expect']))}"
        for record in replies
        if not record.get("unanswered") and not _sequence_matches(commands[record["index"]]["expect"], record)
    ]


def _unanswered(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        f"command {record['index']} ({_command_text(commands[record['index']]['command'])}) got no reply: {record['error']}"
        for record in replies
        if record.get("unanswered")
    ]


def _compare(commands, candidate: _Server, parent: _Server) -> tuple[list[int], list[int], list[int], list[str]]:
    """Judge the parent's replies against the candidate's: the marked commands that reached the change, the
    unmarked ones that differ, the marked ones that do not, and the commands the parent did not answer
    without dying (described)."""

    reached: list[int] = []
    unexplained: list[int] = []
    unreached: list[int] = []
    uncompared: list[str] = []
    # The command the parent died on: its last record (the one that got no reply, or the last it answered).
    died_at = parent.replies[-1]["index"] if parent.exited is not None and parent.replies else None
    for record in candidate.replies:
        index = record["index"]
        item = commands[index]
        if record.get("unanswered") or item["compare"] == "skip":
            continue
        other = _reply_at(parent.replies, index)
        if other is None or other.get("unanswered"):
            if index == died_at:
                (reached if item["expect_difference"] else unexplained).append(index)
            elif parent.exited is None and other is not None:
                uncompared.append(f"command {index} ({_command_text(item['command'])}): {other['error']}")
            continue
        # As canonical JSON, not with ==, under which a RESP3 boolean true equals the integer 1.
        mine, theirs = _compared(record, item["compare"]), _compared({**other, "index": index}, item["compare"])
        if canonical_json(mine) == canonical_json(theirs):
            if item["expect_difference"]:
                unreached.append(index)
        else:
            (reached if item["expect_difference"] else unexplained).append(index)
    return reached, unexplained, unreached, uncompared


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

# One reply of a sequence: the reply as the server sent it, or the error it answered; no timestamps, so two
# runs' replies compare equal.
_SEQUENCE_REPLY = mapping_of(
    {"index": INTEGER, "reply": optional(object), "error": optional(str), "unanswered": optional(bool)}
)
# One server's side of a sequence: its replies, the digest of the compared ones, and its exit status if it exited.
_SEQUENCE_SIDE = mapping_of(
    {"replies": ConfigField(list, items=_SEQUENCE_REPLY), "digest": TEXT, "exited": optional(int)}
)
_COMMANDS_DECLARES = Declarations(
    evidence={
        "valkey.command.sequence": declared(
            mapping_of(
                {
                    "candidate": _SEQUENCE_SIDE,
                    # The parent's side with compare_with; null on a deployment instance.
                    "parent": ConfigField(dict, allow_none=True, schema=_SEQUENCE_SIDE.schema),
                    # The modules each private server loaded; null on a deployment instance.
                    "modules": ConfigField(
                        dict,
                        allow_none=True,
                        schema=mapping_of(
                            {"candidate": ConfigField(list, items=TEXT), "parent": ConfigField(list, items=TEXT)}
                        ).schema,
                    ),
                    "cluster": FLAG,
                    "dropped_config": ConfigField(list, items=TEXT),
                    # Marked commands whose replies differ, and unmarked ones whose replies differ, by index.
                    "reached": ConfigField(list, items=INTEGER),
                    "unexplained": ConfigField(list, items=INTEGER),
                    # Without compare_with: the instance the sequence ran on.
                    "instance_id": optional(str),
                }
            ),
            many=True,
        )
    },
    changes=("process.private-server",),
)

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
        "valkey.action.commands",
        CommandsAction,
        _COMMANDS_FIELDS,
        check=_validate_commands,
        selectors={"instances": ()},
        declares=_COMMANDS_DECLARES,
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
