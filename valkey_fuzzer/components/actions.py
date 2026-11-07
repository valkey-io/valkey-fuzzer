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
import posixpath
import random
import re
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, ClassVar, Optional

import valkey

from valkey_fuzzer._data import canonical_json, jsonable
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.ports import ephemeral_port_range
from valkey_fuzzer.deployment.local.queries import replication_role
from valkey_fuzzer.deployment.local.server_config import MANAGED_SERVER_OPTIONS, canonicalize_server_config
from valkey_fuzzer.hoststate import OWNER_FILE
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
_MAX_NODES = 6

# The predicates of valkey.action.command; ``equals`` also takes a list or a mapping, compared as JSON data, or
# null, a null reply.
_SEQUENCE_EXPECT = {
    **PREDICATE_FIELDS,
    "equals": optional((str, int, float, bool, list, dict), allow_none=True),
    "error": optional((bool, str)),
}
_SEQUENCE_COMMAND = mapping_of(
    {
        # A command; or raw: bytes sent as they are (a string as UTF-8, or !!binary), for the protocol parser; or
        # read: the replies waiting on the connection (Pub/Sub messages, pushes); or restart: stop the node
        # gracefully and start it again from its directory and config file.
        "command": ConfigField(list, allow_none=True, default=None),
        "raw": ConfigField((str, dict), allow_none=True, default=None),
        "read": ConfigField(bool, default=False),
        "restart": ConfigField(bool, default=False),
        # The node it goes to, by index (see nodes), and the connection to it, opened on first use; main is the one
        # every command takes unless it names another.
        "node": ConfigField(int, default=0, minimum=0, maximum=_MAX_NODES - 1),
        "connection": ConfigField(str, default="main"),
        "expect": mapping_of(_SEQUENCE_EXPECT, allow_none=True, default=None),
        # Send the command again until its reply meets expect, for this long at most: for asynchronous work the
        # sequence started (a slot migration, a replica's sync). Its last reply is the one recorded.
        "wait_s": ConfigField(float, allow_none=True, default=None, minimum=0.1, maximum=3600),
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
    # Every private server runs in cluster mode: node 0 serves every slot, and the other nodes join it as empty
    # primaries.
    "cluster": ConfigField(bool, default=False),
    # Each side runs on this many private servers; a command names its node.
    "nodes": ConfigField(int, default=1, minimum=1, maximum=_MAX_NODES),
    # Both private servers start with it; an option the parent rejects is dropped on both sides.
    "server_config": ConfigField(dict, default=dict),
    # Written into every private server's directory before it starts, by path relative to it: a string as UTF-8,
    # or !!binary. server_config may name them (aclfile: users.acl).
    "files": ConfigField(dict, default=dict),
    "resp": ConfigField(int, default=2, choices=(2, 3)),
    "timeout_s": ConfigField(float, default=10.0, minimum=0.1, maximum=3600),
    "commands": ConfigField(list, items=_SEQUENCE_COMMAND),
    # False: replies are recorded, never judged (no expect, comparison, or reached change decides the verdict), so
    # only a crash, a protocol error, a command with no reply, and the checks do; for a sequence replayed only for
    # its checks.
    "judge_replies": ConfigField(bool, default=True),
    # Checks that run the sequence again on fresh candidate servers (see checks.py); each is off unless set.
    "checks": mapping_of(
        {
            "variants": ConfigField(bool, default=False),
            "replica": ConfigField(bool, default=False),
            "acl": ConfigField(bool, default=False),
            "concurrency": ConfigField(int, default=0, minimum=0, maximum=16),
            "compat": ConfigField(bool, default=False),
            "faults": ConfigField(bool, default=False),
        },
        allow_none=True,
        default=None,
    ),
}

# Refused in every sequence: they end or restart the server the sequence talks to, or hold its connection.
_REFUSED = {"SHUTDOWN": "it stops the server", "MONITOR": "it holds the connection"}
# Refused on the main connection, which they would hold; allowed on a named one. Each channel of these and of the
# unsubscribing commands is confirmed by a reply of its own, so they take one each.
_SUBSCRIBE = ("SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE")
_UNSUBSCRIBE = ("UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE")
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
# How long a private server may take to start (a sanitizer build takes seconds, and many times that with dozens
# starting at once), and how long a lost connection waits for its process to exit before the loss counts as
# anything but a crash.
_START_TIMEOUT_S = 60.0
_EXIT_WAIT_S = 5.0
# How often a command with wait_s is sent again.
_WAIT_INTERVAL_S = 0.1
# How long raw bytes wait for a reply before they are recorded as answered by none.
_NO_REPLY_S = 1.0
# How much of a reply the client could not parse is recorded as its bytes.
_UNPARSED_BYTES = 4096
# How long a restart waits for the server to stop (it waits for lagging replicas up to shutdown-timeout, 10s).
_STOP_TIMEOUT_S = 30.0
# The files of a private server's directory the harness writes itself.
_HARNESS_FILES = ("valkey.conf", "server.log", "output.log", "s.sock", OWNER_FILE)
# A node of the side a command runs on, in an argument: <node.1.id>, <node.1.host>, <node.1.port>,
# <node.1.cluster-port>. Angle brackets, not braces: YAML reads {node.1.id} as a mapping, and Valkey braces as a
# hash tag.
_PLACEHOLDER = re.compile(r"<node\.(\d+)\.([a-z-]+)>")
_NODE_FIELDS = ("id", "host", "port", "cluster-port")
_CLUSTER_FIELDS = ("id", "cluster-port")


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


def _validate_placeholders(arguments: Sequence[Any], where: str, config: Mapping[str, Any], compared: bool) -> None:
    for argument in arguments:
        for match in _PLACEHOLDER.finditer(argument if isinstance(argument, str) else ""):
            text, node, field = match.group(0), int(match.group(1)), match.group(2)
            if not compared:
                raise ConfigError(f"{where}: {text} needs compare_with or modules_parent: it names a private server")
            if field not in _NODE_FIELDS:
                raise ConfigError(f"{where}: {text} names no field of a node; the fields are {', '.join(_NODE_FIELDS)}")
            if node >= config["nodes"]:
                raise ConfigError(f"{where}: {text} names node {node}, but each side runs on {config['nodes']} node(s)")
            if field in _CLUSTER_FIELDS and not config["cluster"]:
                raise ConfigError(f"{where}: {text} needs cluster")


def _two_servers(config: Mapping[str, Any]) -> bool:
    """Whether the sequence runs on two private servers: with a parent build, or a parent build of the modules."""

    return config["compare_with"] is not None or config["modules_parent"] is not None


def _validate_commands(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    compared = _two_servers(config)
    if config["cluster"] and not compared:
        raise ConfigError(
            "cluster needs compare_with or modules_parent: on the deployment's instances the deployment decides the mode"
        )
    if config["nodes"] > 1 and not compared:
        raise ConfigError("nodes needs compare_with or modules_parent: the deployment decides its instances")
    if config["files"] and not compared:
        raise ConfigError("files needs compare_with or modules_parent: they are written for the private servers")
    for name, content in config["files"].items():
        path = posixpath.normpath(name)
        if path != name or path.startswith("/") or path == ".." or path.startswith("../") or path in _HARNESS_FILES:
            raise ConfigError(
                f"files: {name!r} must be a plain relative path inside the server's directory, "
                f"none of {', '.join(_HARNESS_FILES)}"
            )
        if not isinstance(content, str) and not _is_binary(content):
            raise ConfigError(f"files: {name!r} must be a string or a !!binary value")
    if config["modules"] and not compared:
        raise ConfigError(
            "modules needs compare_with or modules_parent: the deployment's instances load modules through "
            "deployment.server_config.loadmodule"
        )
    if config["modules_parent"] is not None and not config["modules"]:
        raise ConfigError("modules_parent needs modules: it replaces the candidate's modules on the parent")
    if config["checks"] is not None:
        if not compared:
            raise ConfigError("checks needs compare_with or modules_parent: they run on private servers")
        if config["checks"]["compat"] and config["compare_with"] is None:
            raise ConfigError("checks.compat needs compare_with: it loads the parent build's data into the candidate")
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
        if [item["command"] is not None, item["raw"] is not None, item["read"], item["restart"]].count(True) != 1:
            raise ConfigError(f"{where} needs one of command, raw, read, and restart")
        if item["restart"]:
            if not compared:
                raise ConfigError(
                    f"{where}: restart needs compare_with or modules_parent: valkey.action.restart-instance "
                    "restarts the deployment's instances"
                )
            if item["connection"] != "main":
                raise ConfigError(f"{where}: restart restarts a node, every connection to it; it names none")
        if not item["connection"]:
            raise ConfigError(f"{where}.connection must name a connection")
        if item["wait_s"] is not None and item["command"] is None:
            raise ConfigError(f"{where}: wait_s sends a command again; raw bytes, a read, and a restart happen once")
        if item["raw"] is not None:
            if not item["raw"] or not (isinstance(item["raw"], str) or _is_binary(item["raw"])):
                raise ConfigError(f"{where}.raw must be a non-empty string or a !!binary value")
        elif item["command"] is not None:
            words = _validate_sequence_command(item["command"], f"{where}.command")
            refused = _refusal(words, config["timeout_s"], not compared)
            if refused is not None:
                named = " ".join(words[:2]) if words[0] in _CONTAINER_COMMANDS and len(words) > 1 else words[0]
                raise ConfigError(f"{where}: {named} is refused: {refused}")
            if words[0] in _SUBSCRIBE and item["connection"] == "main":
                raise ConfigError(
                    f"{where}: {words[0]} is refused on the main connection, which it would hold; "
                    "send it on a connection of its own (connection: <name>)"
                )
            if words[0] in (*_SUBSCRIBE, *_UNSUBSCRIBE) and len(words) != 2:
                raise ConfigError(f"{where}: {words[0]} takes one channel: each is confirmed by a reply of its own")
        if item["node"] >= config["nodes"]:
            raise ConfigError(f"{where}.node is {item['node']}, but each side runs on {config['nodes']} node(s)")
        _validate_placeholders(item["command"] or [item["raw"]], where, config, compared)
        if item["expect"] is not None:
            if "error" in item["expect"]:
                if set(item["expect"]) != {"error"}:
                    raise ConfigError(f"{where}.expect.error cannot be combined with a reply predicate")
            else:
                validate_predicate(item["expect"], f"{where}.expect")
        if item["wait_s"] is not None and item["expect"] is None:
            raise ConfigError(f"{where}: wait_s needs an expect to wait for")
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
        # The database a SELECT on it chose, for a reply read again unparsed (see _unparsed).
        self.db: Any = None

    def send(self, command: Sequence[Any]) -> dict[str, Any]:
        """``{"reply": ...}``, the server's error as ``{"error": ...}``, a reply the client could not parse as
        ``{"protocol_error": ...}``, or a lost connection or a reply that timed out as ``{"error": ...,
        "unanswered": True}``."""

        record = self._exchange(lambda: self.connection.send_command(*[_argument(item) for item in command]))
        if str(command[0]).upper() == "SELECT" and record.get("reply") == "OK":
            self.db = command[1]
        return record

    def send_raw(self, data: bytes) -> dict[str, Any]:
        """Send ``data`` as it is and read one reply, as :meth:`send`; ``{"no_reply": True}`` when none began
        within a second (the bytes are not a whole request, or the server holds them). A further reply stays on
        the connection, for the next command to read."""

        return self._exchange(lambda: self.connection.send_packed_command([data]), wait_s=_NO_REPLY_S)

    def read_waiting(self) -> dict[str, Any]:
        """Every reply waiting on the connection, and each that begins within a second of the last (Pub/Sub
        messages, RESP3 pushes): ``{"reply": [...]}``, an error among them as ``{"$error": ...}``; or a lost
        connection as :meth:`send` records it."""

        replies: list[Any] = []
        try:
            while self.connection.can_read(timeout=_NO_REPLY_S):
                try:
                    replies.append(raw_reply(self.connection.read_response(push_request=True)))
                except valkey.exceptions.ResponseError as exc:
                    replies.append(raw_reply(exc))
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
            return _lost(exc)
        return {"reply": replies}

    def _exchange(self, write: Any, wait_s: Optional[float] = None) -> dict[str, Any]:
        try:
            write()
            if wait_s is not None and not self.connection.can_read(timeout=wait_s):
                return {"no_reply": True}
            # A RESP3 push (a SUBSCRIBE's confirmation) is a reply too, not one for valkey-py to handle.
            return {"reply": raw_reply(self.connection.read_response(push_request=True))}
        except valkey.exceptions.ResponseError as exc:
            return {"error": str(exc)}
        except valkey.exceptions.InvalidResponse as exc:
            return {"protocol_error": str(exc)}
        except (valkey.exceptions.ConnectionError, valkey.exceptions.TimeoutError, OSError) as exc:
            return _lost(exc)

    def unparsed(self, request: Any) -> bytes:
        """Send ``request`` (a command, or bytes as they are) and return the bytes that come back as they are: up
        to 4 KB, until none come for a second or the server closes the connection."""

        if isinstance(request, bytes):
            self.connection.send_packed_command([request])
        else:
            self.connection.send_command(*[_argument(item) for item in request])
        sock = self.connection._sock
        sock.settimeout(_NO_REPLY_S)
        received = b""
        try:
            while len(received) < _UNPARSED_BYTES:
                chunk = sock.recv(_UNPARSED_BYTES - len(received))
                if not chunk:
                    break
                received += chunk
        except OSError:
            pass
        return received

    def close(self) -> None:
        try:
            self.connection.disconnect()
        except Exception:
            pass


def _lost(exc: BaseException) -> dict[str, Any]:
    """A command that got no reply: the reply timed out, or the connection was lost, ``closed`` when the server
    closed it (see :func:`_run_sequence`)."""

    record: dict[str, Any] = {"error": f"{type(exc).__name__}: {exc}", "unanswered": True}
    if isinstance(exc, valkey.exceptions.ConnectionError):
        record["closed"] = True
    return record


class _ErrorsAsSent(valkey.connection.DefaultParser):
    """valkey-py's parser, with every error reply a ResponseError of the server's own text: valkey-py strips the
    code of the errors it maps to exception classes (NOPERM, READONLY, ...), and raises WRONGPASS, NOAUTH,
    LOADING and "ERR max number of clients reached" as connection errors, which a sequence takes for a lost
    connection."""

    EXCEPTION_CLASSES: ClassVar[dict[str, Any]] = {}


def open_raw_connection(
    resp: int, timeout_s: float, *, path: Optional[str] = None, host: Any = None, port: Any = None
) -> RawConnection:
    """A :class:`RawConnection` to the unix socket ``path``, or to ``host:port``, connected."""

    options = {
        "socket_timeout": timeout_s,
        "socket_connect_timeout": timeout_s,
        "protocol": resp,
        "parser_class": _ErrorsAsSent,
    }
    if path is not None:
        connection = valkey.connection.UnixDomainSocketConnection(path=path, **options)
    else:
        connection = valkey.connection.Connection(host=str(host), port=int(port), **options)
    connection.connect()
    return RawConnection(connection)


def _run_sequence(target: Any, commands: Sequence[Mapping[str, Any]], context: Any) -> list[dict]:
    """Every command in order on its node's connection of ``target`` (a :class:`_Side` or an
    :class:`_Instance`), each with its index; it stops at the first that got no reply, since the connection's
    state is unknown after it, and when the call is cancelled. A connection the server closed is its answer,
    ``{"closed": True}``, while its node lives on (the connection opens anew on its next use), and a connection
    lost to a crash when not."""

    replies: list[dict[str, Any]] = []
    for index, item in enumerate(commands):
        if context.cancelled:
            break
        record = {"index": index, **_run_step(target, item, context)}
        if record.pop("closed", False) and target.survives(item["node"]):
            target.forget(item["node"], item["connection"])
            record = {"index": index, "closed": True}
        replies.append(record)
        if record.get("unanswered"):
            break
    return replies


def _run_step(target: Any, item: Mapping[str, Any], context: Any) -> dict[str, Any]:
    """One command of a sequence on ``target``: its record, with ``wait_s`` the last of its attempts."""

    if item["restart"]:
        return target.restart(item["node"], context)
    try:
        connection = target.connection(item["node"], item["connection"])
    except (valkey.exceptions.ValkeyError, OSError) as exc:
        return {"error": f"cannot connect: {type(exc).__name__}: {exc}", "unanswered": True}
    if item["read"]:
        return target.mask(connection.read_waiting())
    if item["raw"] is not None:
        raw = item["raw"]
        request = _argument(raw) if _is_binary(raw) else target.substitute(raw).encode()
        record = target.mask(connection.send_raw(request))
    elif item["wait_s"] is None:
        request = [target.substitute(argument) for argument in item["command"]]
        record = target.mask(connection.send(request))
    else:
        request = [target.substitute(argument) for argument in item["command"]]

        def attempt() -> tuple[bool, dict[str, Any]]:
            record = target.mask(connection.send(request))
            done = bool(record.get("unanswered")) or "protocol_error" in record
            return done or _sequence_matches(item["expect"], record, item["compare"]), record

        record = poll_until(attempt, timeout_s=item["wait_s"], interval_s=_WAIT_INTERVAL_S, context=context)[1]
    if "protocol_error" in record:
        received = target.mask(_unparsed(target, item, connection.db, request).decode("latin-1"))
        record["raw"] = received.encode("unicode_escape").decode("ascii")
    return record


def _unparsed(target: Any, item: Mapping[str, Any], db: Any, request: Any) -> bytes:
    """The bytes the server replies to ``request`` on a fresh connection to the node of ``item`` (the same RESP,
    and ``db`` SELECTed), for a reply the client could not parse. The connection the reply broke is closed
    first, and the fresh one after, so the next command opens one anew."""

    node, name = item["node"], item["connection"]
    target.forget(node, name)
    try:
        connection = target.connection(node, name)
    except (valkey.exceptions.ValkeyError, OSError):
        return b""
    try:
        if db is not None:
            connection.send(["SELECT", db])
        return connection.unparsed(request)
    except (valkey.exceptions.ValkeyError, OSError):
        return b""
    finally:
        target.forget(node, name)


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


def _json_expected(expected: Any, reply: Any) -> Any:
    """``expected`` as :func:`_compared` makes ``reply`` under ``json``: where the reply holds JSON text, the
    expectation may be that text or the value it holds."""

    def one(value: Any, item: Any) -> Any:
        if isinstance(item, str) and not isinstance(value, str):
            return _json_value(json.dumps(value))
        return _json_value(value)

    if isinstance(reply, list) and isinstance(expected, list) and len(reply) == len(expected):
        return [one(value, item) for value, item in zip(expected, reply)]
    return one(expected, reply)


def _sequence_matches(expect: Optional[Mapping[str, Any]], record: Mapping[str, Any], compare: str) -> bool:
    expect = None if expect is None else plain_config(expect)
    if expect is not None and "equals" in expect and expect["equals"] is None:
        return record.get("error") is None and record.get("reply") is None
    if compare == "json" and expect is not None and "equals" in expect:
        return "reply" in record and canonical_json(_compared(record, "json")["reply"]) == canonical_json(
            _json_expected(expect["equals"], record["reply"])
        )
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


def _step_text(item: Mapping[str, Any]) -> str:
    """A command of a sequence as a summary names it."""

    if item["read"] or item["restart"]:
        text = "read" if item["read"] else "restart"
    elif item["raw"] is not None:
        text = "raw <binary>" if _is_binary(item["raw"]) else f"raw {json.dumps(item['raw'])}"
    else:
        text = _command_text(item["command"])
    if item["connection"] != "main":
        text += f" on connection {item['connection']}"
    return text + (f" on node {item['node']}" if item["node"] else "")


class _Instance:
    """A deployment instance a sequence runs on: its connections by name, opened on first use, and no
    placeholders."""

    def __init__(self, connect: Any):
        self.connect = connect
        self.connections = {"main": connect()}

    def connection(self, node: int, name: str) -> RawConnection:
        if name not in self.connections:
            self.connections[name] = self.connect()
        return self.connections[name]

    def survives(self, node: int) -> bool:
        """Never known here: an instance's liveness is the liveness oracle's to judge, so a connection it closed
        stays a command without a reply."""

        return False

    def forget(self, node: int, name: str) -> None:
        self.connections.pop(name).close()

    def close(self) -> None:
        for connection in self.connections.values():
            connection.close()

    def substitute(self, argument: Any) -> Any:
        return argument

    def mask(self, value: Any) -> Any:
        return value


class _Node:
    """One private server of a side: its address options, the server, its connections by name, its kept logs."""

    def __init__(self, name: str):
        # The prefix of its kept logs: the side's label, with the node's index when the side has several.
        self.name = name
        self.options: dict[str, Any] = {}
        self.server: Any = None
        self.owned: Any = None
        self.connections: dict[str, RawConnection] = {}
        # Its cluster node ID, with cluster.
        self.id: Optional[str] = None
        self.logs: list[str] = []


class _Side:
    """One build's side of a compared sequence: its private servers (nodes), started, owned until stopped, their
    logs kept, and the placeholders that name them."""

    def __init__(self, label: str, binary: str, nodes: int, resp: int, timeout_s: float):
        self.label, self.binary = label, binary
        # The protocol and client timeout of every connection.
        self.resp, self.timeout_s = resp, timeout_s
        # The server options of this side's nodes (cluster mode, modules), beside the shared server_config.
        self.options: dict[str, Any] = {}
        self.nodes = [_Node(label if nodes == 1 else f"{label}.node{index}") for index in range(nodes)]
        self.replies: list[dict[str, Any]] = []
        # The exit status of the first node found exited, and that node.
        self.exited: Optional[int] = None
        self.exited_node: Optional[int] = None
        self.logs: list[str] = []
        # {placeholder: value}, and {node ID or port: placeholder} for the replies.
        self.values: dict[str, str] = {}
        self.names: dict[str, str] = {}
        self.pattern: Optional[re.Pattern] = None

    def start(
        self, context: Any, spec: ComponentSpec, server_config: Mapping[str, Any], files: Mapping[str, bytes]
    ) -> str:
        """Start every node from a config file of ``server_config`` and its options, ``files`` in its directory, and
        open its main connection; what went wrong, or an empty string."""

        for index, node in enumerate(self.nodes):
            arguments = server_arguments({**server_config, **self.options, **node.options})
            node.server = PrivateServer(
                self.binary, arguments, timeout_s=_START_TIMEOUT_S, prefix="vfc-", files=files, config_file=True
            )
            node.owned = context.obligations.open(spec, "process.private-server", node.server.workdir, node.server.stop)
            try:
                # PrivateServer.start polls with time.sleep: not cancellable, but bounded by its timeout.
                exit_output = node.server.start()
            except (OSError, TimeoutError) as exc:
                return f"the {node.name} server did not start: {exc}"
            if exit_output is not None:
                return f"the {node.name} server exited at startup: {exit_output[-500:]}"
            try:
                self.connection(index, "main")
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return f"cannot connect to the {node.name} server with RESP{self.resp}: {type(exc).__name__}: {exc}"
        return ""

    def form_cluster(self, context: Any, timeout_s: float) -> Optional[str]:
        """Give node 0 every slot, have every other node meet it, and wait until every node knows them all with
        its state ok; why that did not happen, or None. Each node's ID is recorded."""

        first = self.nodes[0]
        added = first.connections["main"].send(["CLUSTER", "ADDSLOTSRANGE", "0", "16383"])
        if "reply" not in added:
            return f"the {first.name} server: CLUSTER ADDSLOTSRANGE failed: {added.get('error')}"
        for node in self.nodes:
            myid = node.connections["main"].send(["CLUSTER", "MYID"])
            if "reply" not in myid:
                return f"the {node.name} server: CLUSTER MYID failed: {myid.get('error')}"
            node.id = str(myid["reply"])
        for node in self.nodes[1:]:
            met = node.connections["main"].send(
                ["CLUSTER", "MEET", "127.0.0.1", first.options["port"], first.options["cluster-port"]]
            )
            if "reply" not in met:
                return f"the {node.name} server: CLUSTER MEET failed: {met.get('error')}"
        for node in self.nodes:

            def formed(node: _Node = node) -> tuple[bool, Any]:
                info = node.connections["main"].send(["CLUSTER", "INFO"])
                fields = dict(line.split(":", 1) for line in str(info.get("reply", "")).split() if ":" in line)
                ready = fields.get("cluster_state") == "ok" and fields.get("cluster_known_nodes") == str(
                    len(self.nodes)
                )
                return ready, info

            done, last = poll_until(formed, timeout_s=timeout_s, interval_s=0.1, context=context)
            if not done:
                return (
                    f"the {node.name} server: cluster_state is not ok with {len(self.nodes)} known node(s) "
                    f"within {timeout_s:g}s: {last}"
                )
        return None

    def restart(self, index: int, context: Any) -> dict[str, Any]:
        """Stop node ``index`` gracefully and start it again from its directory and config file: ``{"reply": "OK"}``
        once it answers again, or why not as a command that got no reply. A cluster node answers once its
        cluster_state is ok again, as it was when the cluster formed: until then it refuses keys with CLUSTERDOWN.
        One whose state stays otherwise answers with that state, an error both builds compare on."""

        node = self.nodes[index]
        for connection in node.connections.values():
            connection.close()
        node.connections.clear()
        if not node.server.shut_down(_STOP_TIMEOUT_S):
            return {"error": f"the server did not stop within {_STOP_TIMEOUT_S:g}s", "unanswered": True}
        try:
            exit_output = node.server.start()
        except (OSError, TimeoutError) as exc:
            return {"error": f"the server did not start again: {exc}", "unanswered": True}
        if exit_output is not None:
            return {"error": f"the server exited at startup: {exit_output[-500:]}", "unanswered": True}
        if "cluster-port" in node.options:

            def settled() -> tuple[bool, str]:
                info = self.connection(index, "main").send(["CLUSTER", "INFO"])
                fields = dict(line.split(":", 1) for line in str(info.get("reply", "")).split() if ":" in line)
                return fields.get("cluster_state") == "ok", fields.get("cluster_state") or str(info)

            done, state = poll_until(settled, timeout_s=self.timeout_s, interval_s=0.1, context=context)
            if not done:
                return {"error": f"cluster_state is not ok within {self.timeout_s:g}s of the restart: {state}"}
        return {"reply": "OK"}

    def name_nodes(self) -> None:
        """The placeholders of its nodes, both ways: each to its value, and each node ID and port in a reply back to
        its placeholder (the host is the same on every side)."""

        for index, node in enumerate(self.nodes):
            fields = {
                "id": node.id,
                "host": "127.0.0.1",
                **{key: node.options.get(key) for key in ("port", "cluster-port")},
            }
            for field, value in fields.items():
                if value is None:
                    continue
                self.values[f"<node.{index}.{field}>"] = str(value)
                if field != "host":
                    self.names[str(value)] = f"<node.{index}.{field}>"
        if self.names:
            alternatives = "|".join(re.escape(value) for value in sorted(self.names, key=len, reverse=True))
            self.pattern = re.compile(f"(?<![0-9A-Za-z])(?:{alternatives})(?![0-9A-Za-z])")

    def connection(self, node: int, name: str) -> RawConnection:
        connections = self.nodes[node].connections
        if name not in connections:
            connections[name] = open_raw_connection(self.resp, self.timeout_s, path=self.nodes[node].server.socket_path)
        return connections[name]

    def survives(self, index: int) -> bool:
        """Whether node ``index`` still runs ``_EXIT_WAIT_S`` after a connection to it closed."""

        try:
            self.nodes[index].server.process.wait(timeout=_EXIT_WAIT_S)
        except subprocess.TimeoutExpired:
            return True
        return False

    def forget(self, index: int, name: str) -> None:
        self.nodes[index].connections.pop(name).close()

    def substitute(self, argument: Any) -> Any:
        """``argument`` with every placeholder in it replaced by this side's value."""

        if not isinstance(argument, str):
            return argument
        return _PLACEHOLDER.sub(lambda match: self.values[match.group(0)], argument)

    def mask(self, value: Any) -> Any:
        """``value`` (a record, or a part of one) with this side's node IDs and ports as their placeholders, so two
        sides that behave the same compare equal; a port is masked as an integer too, as CLUSTER SLOTS replies it."""

        if self.pattern is None or _is_binary(value):
            return value
        if isinstance(value, str):
            return self.pattern.sub(lambda match: self.names[match.group(0)], value)
        if isinstance(value, int) and not isinstance(value, bool):
            return self.names.get(str(value), value)
        if isinstance(value, list):
            return [self.mask(item) for item in value]
        if isinstance(value, dict):
            return {self.mask(key): self.mask(item) for key, item in value.items()}
        return value

    def finish(self, context: Any, spec: ComponentSpec, commands: Sequence[Mapping[str, Any]]) -> None:
        """Note whether a node exited (a lost connection waits for its node), keep every node's logs, stop them."""

        for node in self.nodes:
            for connection in node.connections.values():
                connection.close()
        if self.replies and self.replies[-1].get("unanswered"):
            lost = self.nodes[commands[self.replies[-1]["index"]]["node"]].server
            if lost is not None and lost.process is not None:
                try:
                    lost.process.wait(timeout=_EXIT_WAIT_S)
                except subprocess.TimeoutExpired:
                    pass
        directory = component_artifact_dir(context, spec)
        for index, node in enumerate(self.nodes):
            if node.server is not None and node.server.process is not None:
                status = node.server.process.poll()
                if status is not None and self.exited is None:
                    self.exited, self.exited_node = status, index
                for name, path in (("server.log", node.server.log_path), ("output.log", node.server.output_path)):
                    destination = os.path.join(directory, f"{node.name}.{name}")
                    try:
                        shutil.copyfile(path, destination)
                        node.logs.append(destination)
                    except OSError:
                        pass
            self.logs.extend(node.logs)
            if node.owned is not None:
                node.owned.discharge()

    def crash_cause(self) -> str:
        """Why its node exited, from that node's kept logs: the crash report's cause, else its fatal line, else
        its status."""

        logs = self.nodes[self.exited_node or 0].logs
        reports = scan_for_crashes({path: path for path in logs}, CRASH_PATTERNS)
        if reports:
            return crash_cause(next(iter(reports.values())))
        fatal = next((line for line in map(fatal_exit, logs) if line), None)
        return fatal or f"exited with status {self.exited}"

    def crashed(self) -> bool:
        """Whether its node's exit is a crash (a sanitizer report, assertion or signal), not a controlled fatal
        exit it logged on purpose (as a build hardened to reject bad data does on a load it refuses)."""

        logs = self.nodes[self.exited_node or 0].logs
        return bool(scan_for_crashes({path: path for path in logs}, CRASH_PATTERNS))


# The fewest ports above the host's ephemeral range for private servers to take theirs there.
_PRIVATE_SPAN_MIN = 1000


@functools.lru_cache(maxsize=1)
def _private_port_span() -> Optional[tuple[int, int]]:
    """Ports above the host's ephemeral range, where no outgoing connection takes one between the free-port check
    and Valkey's bind (the server would abort, "Failed listening on port"); None where there are too few, or the
    range cannot be read. Below it, a batch's workers own the ports of their deployments and cluster buses."""

    span = ephemeral_port_range()
    if span is None or 65535 - span[1] < _PRIVATE_SPAN_MIN:
        return None
    return span[1] + 1, 65535


def _free_ports(count: int) -> list[int]:
    """``count`` distinct TCP ports free on the loopback address right now: above the ephemeral range where it
    leaves room (see :func:`_private_port_span`), else any the kernel hands out."""

    span = _private_port_span()
    ports: list[int] = []
    while len(ports) < count:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", 0 if span is None else random.randint(*span)))
            except OSError:
                continue
            port = sock.getsockname()[1]
        if port not in ports:
            ports.append(port)
    return ports


def new_side(label: str, binary: str, config: Mapping[str, Any], modules: Sequence[str], ports: Iterator[int]) -> _Side:
    """A side of ``config``'s nodes, not started: every node listens on a loopback port of its own (and a cluster
    bus port) from ``ports``, so nodes can meet and replicate; the sequence talks to it over its unix socket."""

    side = _Side(label, binary, config["nodes"], config["resp"], config["timeout_s"])
    for node in side.nodes:
        node.options = {"bind": "127.0.0.1", "port": next(ports)}
        if config["cluster"]:
            node.options["cluster-port"] = next(ports)
    if config["cluster"]:
        side.options["cluster-enabled"] = "yes"
    if modules:
        side.options["loadmodule"] = list(modules)
    return side


class CommandsAction:
    """Send a command sequence, and with ``compare_with`` compare the replies of two builds.

    With ``compare_with`` the sequence runs on two sides of ``nodes`` private servers each, which the action
    starts from an empty dataset with the same ``server_config`` (``sanitize-dump-payload yes`` unless set, and
    ``cluster-enabled yes`` with ``cluster``), one side of the deployment's binary (the candidate) and one of
    ``compare_with`` (the parent); the deployment's instances are never touched. An option the parent rejects
    is dropped on both sides and recorded. Only the candidate is judged: a reply that violates its ``expect``,
    a reply the client cannot parse, or a candidate node that exits, is FAIL. A difference between the builds is
    never a FAIL by itself: a marked command whose replies are equal leaves the run ERROR ``unreached-change``, an
    unmarked command whose replies differ ERROR ``unexplained-difference``; a parent that dies on a marked command
    reached the change. Without ``compare_with`` the sequence runs on each selected instance, and ``expect`` alone
    judges it.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        if not _two_servers(config):
            return self._run_on_instances(context, spec, config)
        return self._run_compared(context, spec, config)

    def _run_on_instances(self, context: Any, spec: ComponentSpec, config: Mapping[str, Any]):
        handle = deployment(context)
        evidence, problems, unanswered, broken = [], [], [], []
        for instance_id in select_instances(handle, config["instances"]):
            instance = handle.instance(instance_id)
            try:
                target = _Instance(
                    functools.partial(
                        open_raw_connection, config["resp"], config["timeout_s"], host=instance.host, port=instance.port
                    )
                )
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return make_result(spec, ComponentStatus.ERROR, f"cannot connect to {instance_id}: {exc}")
            try:
                replies = _run_sequence(target, config["commands"], context)
            finally:
                target.close()
            if config["judge_replies"]:
                problems.extend(f"{instance_id}: {text}" for text in _violations(config["commands"], replies))
            unanswered.extend(f"{instance_id}: {text}" for text in _unanswered(config["commands"], replies))
            broken.extend(
                {"instance_id": instance_id, **item} for item in _protocol_errors(config["commands"], replies)
            )
            record = _record(config, _side(replies, config["commands"]), None, None, [], [], [])
            record["instance_id"] = instance_id
            evidence.append(make_evidence(spec, "valkey.command.sequence", "action.completed", record))
        if broken:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(f"{item['instance_id']}: {_protocol_error_text(item)}" for item in broken),
                evidence=evidence,
                diagnostics={"reason": "protocol-error", "commands": broken},
            )
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
        server_config = {**_PRIVATE_SERVER_DEFAULTS, **canonicalize_server_config(config["server_config"])}
        modules = {
            "candidate": list(config["modules"]),
            "parent": list(config["modules"] if config["modules_parent"] is None else config["modules_parent"]),
        }
        ports = iter(_free_ports(2 * config["nodes"] * (2 if config["cluster"] else 1)))
        candidate = new_side("candidate", candidate_binary, config, modules["candidate"], ports)
        parent = new_side("parent", parent_binary, config, modules["parent"], ports)

        files = {
            name: _argument(content) if _is_binary(content) else content.encode()
            for name, content in config["files"].items()
        }

        def starts(server: _Side, options: Mapping[str, Any]) -> tuple[ComponentStatus, str]:
            probed = {**options, **server.options, **server.nodes[0].options}
            status, summary, _ = starts_with(context, spec, server.binary, probed, _START_TIMEOUT_S, files)
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
                failed = server.start(context, spec, server_config, files)
                if not failed and config["cluster"]:
                    failed = server.form_cluster(context, config["timeout_s"]) or ""
                if failed:
                    return make_result(spec, ComponentStatus.ERROR, failed)
                server.name_nodes()
            for server in (candidate, parent):
                server.replies = _run_sequence(server, commands, context)
        finally:
            for server in (candidate, parent):
                server.finish(context, spec, commands)

        reached, unexplained, unreached, uncompared = _compare(commands, candidate, parent)
        if not config["judge_replies"]:
            unexplained, unreached = [], []
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
                record[server.label]["exited_node"] = server.exited_node
        record["violating"] = _violating(commands, candidate.replies)
        checks: dict[str, dict[str, Any]] = {}
        check_logs: list[str] = []
        if config["checks"] is not None:
            # checks.py builds on this module, so it is imported where it is used.
            from .checks import run_checks

            checks, check_logs = run_checks(context, spec, config, candidate, parent_binary, server_config, files)
            record["checks"] = checks
        failed = next((outcome for outcome in checks.values() if outcome["outcome"] == "fail"), None)
        evidence = (
            make_evidence(
                spec,
                "valkey.command.sequence",
                "action.completed",
                record,
                artifacts=[*candidate.logs, *parent.logs, *check_logs],
            ),
        )
        violations = _violations(commands, candidate.replies) if config["judge_replies"] else []
        # A violated expect on which the parent replied the same is about behaviour the change did not touch:
        # nearly always the test's own expectation, not the change. Recorded, never a FAIL.
        disputed = [
            index
            for index in _violating(commands, candidate.replies)
            if config["judge_replies"] and _same_on_parent(commands, index, candidate, parent)
        ]
        violations = [
            text for index, text in zip(_violating(commands, candidate.replies), violations) if index not in disputed
        ]
        if candidate.exited is not None:
            cause = candidate.crash_cause()
            at = candidate.replies[-1]["index"] if candidate.replies else None
            where = "before the first command" if at is None else f"on command {at} ({_step_text(commands[at])})"
            who = "the candidate" if config["nodes"] == 1 else f"the candidate's node {candidate.exited_node}"
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"{who} exited {where}: {cause}",
                evidence=evidence,
                diagnostics={"crashes": {"candidate": cause}},
                crash=True,
            )
        # A reply the client cannot parse is a protocol bug, a FAIL even where the parent gives the same one.
        broken = _protocol_errors(commands, candidate.replies)
        if broken:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(map(_protocol_error_text, broken)),
                evidence=evidence,
                diagnostics={"reason": "protocol-error", "commands": broken},
            )
        if violations:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(violations), evidence=evidence)
        if failed is not None:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                failed["summary"],
                evidence=evidence,
                diagnostics=failed["diagnostics"],
                crash=failed["crash"],
            )
        unanswered = _unanswered(commands, candidate.replies) + [
            f"the parent did not answer {text}" for text in uncompared
        ]
        if unanswered:
            return make_result(spec, ComponentStatus.ERROR, "; ".join(unanswered), evidence=evidence)
        if disputed:
            commands_disputed = [
                {
                    "index": index,
                    "command": _step_text(commands[index]),
                    "expect": plain_config(commands[index]["expect"]),
                    "candidate": _reply_at(candidate.replies, index),
                    "parent": _reply_at(parent.replies, index),
                }
                for index in disputed
            ]
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "expectations both builds violate alike, so they are not about the change: "
                + ", ".join(f"{item['index']} ({item['command']})" for item in commands_disputed),
                evidence=evidence,
                diagnostics={"reason": "expect-disputed", "commands": commands_disputed},
            )
        if unexplained:
            differences = [
                {
                    "index": index,
                    "command": _step_text(commands[index]),
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
                + ", ".join(f"{index} ({_step_text(commands[index])})" for index in unreached),
                evidence=evidence,
                diagnostics={"reason": "unreached-change"},
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(commands)} commands on both builds, {len(reached)} marked command(s) reached the change"
            + (f", dropped {', '.join(dropped)}" if dropped else "")
            + "".join(f", {name} {outcome['outcome']}" for name, outcome in checks.items()),
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
        "nodes": config["nodes"],
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
        f"command {record['index']} ({_step_text(commands[record['index']])}) replied "
        f"{canonical_json(_without_index(record))}, not {canonical_json(plain_config(commands[record['index']]['expect']))}"
        for record in replies
        if not record.get("unanswered")
        and not _sequence_matches(commands[record["index"]]["expect"], record, commands[record["index"]]["compare"])
    ]


def _violating(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[int]:
    """The indexes of the answered replies that violate their command's ``expect``, in the order of
    :func:`_violations`."""

    return [
        record["index"]
        for record in replies
        if not record.get("unanswered")
        and not _sequence_matches(commands[record["index"]]["expect"], record, commands[record["index"]]["compare"])
    ]


def _same_on_parent(commands: Sequence[Mapping[str, Any]], index: int, candidate: Any, parent: Any) -> bool:
    """Whether the parent answered command ``index`` and its reply compares equal to the candidate's."""

    mine, theirs = _reply_at(candidate.replies, index), _reply_at(parent.replies, index)
    if mine is None or theirs is None or theirs.get("unanswered"):
        return False
    mode = commands[index]["compare"]
    return canonical_json(_compared({**mine, "index": index}, mode)) == canonical_json(
        _compared({**theirs, "index": index}, mode)
    )


def _replication_links(commands: Sequence[Mapping[str, Any]]) -> set[int]:
    """The commands sent on a connection after it sent SYNC or PSYNC: it is a replica's link since, and what it
    reads is the replication stream, not replies."""

    links, found = set(), set()
    for index, item in enumerate(commands):
        if item["command"] is not None:
            words = [str(word) for word in item["command"][:1]]
        elif isinstance(item["raw"], str):
            words = item["raw"].split()[:1]
        else:
            continue
        key = (item["node"], item["connection"])
        if key in links:
            found.add(index)
        elif words and words[0].upper() in ("SYNC", "PSYNC"):
            links.add(key)
    return found


def _protocol_errors(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[dict]:
    """The commands whose reply the client could not parse: each with its index, the client's message, and the
    reply's bytes. A replica's link reads the replication stream, which is no reply."""

    links = _replication_links(commands)
    return [
        {
            "index": record["index"],
            "command": _step_text(commands[record["index"]]),
            "protocol_error": record["protocol_error"],
            "raw": record["raw"],
        }
        for record in replies
        if "protocol_error" in record and record["index"] not in links
    ]


def _protocol_error_text(item: Mapping[str, Any]) -> str:
    return f"command {item['index']} ({item['command']}) got a reply the client cannot parse: {item['protocol_error']}"


def _unanswered(commands: Sequence[Mapping[str, Any]], replies: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        f"command {record['index']} ({_step_text(commands[record['index']])}) got no reply: {record['error']}"
        for record in replies
        if record.get("unanswered")
    ]


def _compare(commands, candidate: _Side, parent: _Side) -> tuple[list[int], list[int], list[int], list[str]]:
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
                uncompared.append(f"command {index} ({_step_text(item)}): {other['error']}")
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
    {
        "index": INTEGER,
        "reply": optional(object),
        "error": optional(str),
        "unanswered": optional(bool),
        # Raw bytes no reply began to answer.
        "no_reply": optional(bool),
        # The server closed the connection without a reply, its node living on.
        "closed": optional(bool),
        # A reply the client could not parse: its message, and the reply's bytes, escaped (the first 4 KB).
        "protocol_error": optional(str),
        "raw": optional(str),
    }
)
# One side of a sequence: its replies, the digest of the compared ones, and the exit status of a node that exited,
# and which node.
_SEQUENCE_SIDE = mapping_of(
    {
        "replies": ConfigField(list, items=_SEQUENCE_REPLY),
        "digest": TEXT,
        "exited": optional(int),
        "exited_node": optional(int),
    }
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
                    "nodes": INTEGER,
                    "dropped_config": ConfigField(list, items=TEXT),
                    # Marked commands whose replies differ, and unmarked ones whose replies differ, by index.
                    "reached": ConfigField(list, items=INTEGER),
                    "unexplained": ConfigField(list, items=INTEGER),
                    # With compare_with: the commands whose reply on the build under test violates their expect.
                    "violating": optional(list),
                    # Without compare_with: the instance the sequence ran on.
                    "instance_id": optional(str),
                    # With checks: each enabled check's outcome, by name (see checks.py).
                    "checks": optional(dict),
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
