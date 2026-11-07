"""The ``checks`` of ``valkey.action.commands``: after the compared sequence, it runs again on fresh private
servers, each check its own way, and a check that sees Valkey do something wrong fails the run.

- ``variants``: the candidate with every compact encoding off, then with the other protocol, must reply as the
  base run did (``variant-mismatch``): replies compare as multisets of their values, RESP3 types as RESP2's.
- ``replica``: the candidate's node 0 has a replica; once the sequence ran and the replica caught up, both must
  have the same ``DEBUG DIGEST`` (``replica-diverged``).
- ``acl``: as a user denied one ACL category the sequence uses, then as one whose key pattern none of its keys
  match, every command the user may not run must reply NOPERM (``acl-bypass``), and no other (``acl-overreach``).
- ``concurrency``: N connections run the sequence at once, their replies unjudged: a command may go unanswered
  only while the server answers others (``hang``), and with ``replica`` the replica must catch up.
- ``compat``: the parent's data after the sequence, saved, must load into the candidate (``compat-load``) and
  replicate to it (``compat-sync``) with the parent's digest.
- ``faults``: the sequence runs again on servers that persist (an RDB saved at shutdown, or the AOF), a single
  node with a replica, and before commands drawn from the run's seed the check restarts a node or makes it DEBUG
  RELOAD; replies are not judged, but every node must come back, every DEBUG RELOAD must succeed
  (``reload-failed``), a node must keep its data (``lost-data``, judged when nothing it holds can expire), and a
  replica must catch up after its restarts and at the end (``replica-diverged``).

A candidate server that exits during a check crashed, and is reported as a crash. The checks run at once, each on
servers of its own.
"""

import contextvars
import fnmatch
import math
import os
import re
import shutil
import threading
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import valkey

from valkey_fuzzer._data import canonical_json
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import component_artifact_dir

from . import actions
from .oracles import CRASH_PATTERNS, crash_cause, scan_for_crashes
from .polling import poll_until

_ORDER = ("variants", "replica", "acl", "concurrency", "compat", "faults")
# What the faults check does to a node between two commands: restart it from its own directory, or make it save
# and load its data in place (DEBUG RELOAD).
_FAULTS = ("restart", "reload")
# How the faults check's servers persist their data across a restart (private servers save nothing otherwise):
# an RDB saved at shutdown, or the AOF.
_PERSISTENCE = ({"save": "3600 1"}, {"appendonly": "yes"})
# How long a node's DEBUG DIGEST must hold still before a fault for its data to be judged across it.
_SETTLE_S = 0.2
# Every compact encoding off: a hash, set, or sorted set is never a listpack or intset, and a list of two or more
# items is a quicklist of one-item nodes.
_ENCODINGS = {
    "hash-max-listpack-entries": 0,
    "set-max-listpack-entries": 0,
    "set-max-intset-entries": 0,
    "zset-max-listpack-entries": 0,
    "list-max-listpack-size": 0,
}
# Commands whose replies depend on the encoding, the protocol or the configuration by design: variants never
# compare them. DEBUG, OBJECT and MEMORY report internals (DEBUG HTSTATS-KEY a hashtable's buckets), DUMP serializes
# a key in its encoding, CONFIG reports the encoding thresholds the variant changed, a SCAN's call returns as much as
# the encoding holds together, SLOWLOG, LATENCY and TIME report times, and CLUSTER INFO reports gossip counters and
# a save timestamp that differ between two independently formed clusters.
_VARIES = (
    "DEBUG",
    "OBJECT",
    "MEMORY",
    "DUMP",
    "CONFIG",
    "CLIENT INFO",
    "CLIENT LIST",
    "CLUSTER INFO",
    "HELLO",
    "INFO",
    "SLOWLOG",
    "LATENCY",
    "TIME",
)
# A script's call of a command (redis.call, server.pcall) in its upper-cased words: the command's name and, when the
# next argument is a literal word too, that (CLIENT, INFO).
_SCRIPT_CALL = re.compile(r"""CALL\s*\(\s*(['"])([\w-]+)\1(?:\s*,\s*(['"])([\w-]+)\3)?""")
# Why the compat check leaves the parent's data uncompared: see changed_write.
_CHANGED = "the merge changes what {} writes, so the parent's data may hold what the candidate never makes"
# Commands that reply with the NOPERM of the commands they run: the ACL check does not judge them.
_RUNS_OTHERS = ("EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO")
# A NOPERM error, as the connection records it: valkey-py drops the NOPERM (and ERR) code, so it is known by its
# message ("User default has no permissions to run the 'hset' command", "No permissions to access a key").
_NOPERM = "no permissions"
# Errors Valkey replies before it checks a command's permissions, so a refused command may reply them.
_BEFORE_ACL = (
    "unknown command",
    "unknown subcommand",
    "wrong number of arguments",
    "Command not allowed inside a transaction",
)
# ACL categories of speed, not function: never the one the ACL check denies.
_SPEED = ("@fast", "@slow")
_KEY_PATTERN = "nomatch-*"
# The checks' own connection to a node, apart from every connection the sequence names.
_CHECK = "\x00check"
# A primary starts a replica's sync at once (the default waits 5 seconds for more replicas).
_QUICK_SYNC = {"repl-diskless-sync-delay": 0}
_SYNC_TIMEOUT_S = 30.0
_NUMBER = re.compile(r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?")
_INTEGER = re.compile(r"[-+]?\d+")


def run_checks(
    context: Any,
    spec: ComponentSpec,
    config: Mapping[str, Any],
    candidate: Any,
    parent_binary: str,
    server_config: Mapping[str, Any],
    files: Mapping[str, bytes],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """The enabled checks' outcomes by name, after the compared sequence whose candidate side is ``candidate``,
    and the files they kept (their servers' logs, the parent's RDB), for the evidence's artifacts.

    An outcome is ``{outcome: pass | skipped | error | fail}``, ``reason`` for skipped and error, and ``summary``,
    ``diagnostics``, and ``crash`` for fail; a check of two parts also has each part's outcome by name.
    """

    enabled = [name for name in _ORDER if config["checks"][name]]
    if candidate.exited is not None or any(record.get("unanswered") for record in candidate.replies):
        return {name: _skipped("the sequence did not complete on the candidate") for name in enabled}, []
    checks = {name: _Checks(context, spec, config, candidate, parent_binary, server_config, files) for name in enabled}
    # Each check runs in a copy of this call's context, so the runner's cancellation of the call reaches it; every
    # check returns, its servers stopped, before the outcomes are read, in _ORDER whichever finished first.
    with ThreadPoolExecutor(max_workers=max(len(enabled), 1)) as pool:
        futures = {name: pool.submit(contextvars.copy_context().run, getattr(checks[name], name)) for name in enabled}
    return {name: futures[name].result() for name in enabled}, [path for name in enabled for path in checks[name].kept]


def _skipped(reason: str) -> dict[str, Any]:
    return {"outcome": "skipped", "reason": reason}


def _errored(reason: str) -> dict[str, Any]:
    return {"outcome": "error", "reason": reason}


def _failed(summary: str, diagnostics: Mapping[str, Any], crash: bool = False) -> dict[str, Any]:
    return {"outcome": "fail", "summary": summary, "diagnostics": dict(diagnostics), "crash": crash}


def _combine(**parts: dict[str, Any]) -> dict[str, Any]:
    """The outcome of a check of several parts: its worst part's, and each part's by name."""

    order = ("fail", "error", "pass", "skipped")
    return {**min(parts.values(), key=lambda part: order.index(part["outcome"])), **parts}


def _words(item: Mapping[str, Any]) -> list[str]:
    return [] if item["command"] is None else actions._validate_sequence_command(item["command"], "command")


def _script_varies(words: list[str], commands: Sequence[Mapping[str, Any]]) -> bool:
    """Whether a script command calls a command in _VARIES: an EVAL from its script, an EVALSHA or FCALL from any
    script or library the sequence loads (fuzzer#90: an EVAL returned the length of a DUMP)."""

    if words[0] in ("EVAL", "EVAL_RO"):
        bodies = words[1:2]
    elif words[0] in _RUNS_OTHERS:
        loads = (["SCRIPT", "LOAD"], ["FUNCTION", "LOAD"])
        bodies = [_words(item)[-1] for item in commands if _words(item)[:2] in loads]
    else:
        return False
    return any(
        match[2] in _VARIES or f"{match[2]} {match[4]}" in _VARIES
        for body in bodies
        for match in _SCRIPT_CALL.finditer(body)
    )


def _subscribed(commands: Sequence[Mapping[str, Any]]) -> set[int]:
    """The commands sent on a connection that subscribed before them, and did not RESET since."""

    connections, found = set(), set()
    for index, item in enumerate(commands):
        words = _words(item)
        if not words:
            continue
        if item["connection"] in connections:
            found.add(index)
        name = words[0].upper()
        if name in actions._SUBSCRIBE:
            connections.add(item["connection"])
        elif name == "RESET":
            connections.discard(item["connection"])
    return found


def _sets_replication(commands: Sequence[Mapping[str, Any]]) -> bool:
    """Whether the sequence makes its node a replica (or a primary) itself: a replica whose own link is down
    refuses replicas of its own (NOMASTERLINK), so no replica of it can catch up."""

    return any(words and words[0].upper() in ("REPLICAOF", "SLAVEOF") for words in map(_words, commands))


def _manages_auth(commands: Sequence[Mapping[str, Any]]) -> bool:
    """Whether the sequence sets users, a password, or authenticates: ACL, AUTH, HELLO ... AUTH, or a CONFIG SET of
    requirepass."""

    return any(
        words
        and (
            words[0] in ("ACL", "AUTH")
            or (words[0] == "HELLO" and "AUTH" in words)
            or (words[:2] == ["CONFIG", "SET"] and "REQUIREPASS" in words)
        )
        for words in map(_words, commands)
    )


def _debugs_sync(commands: Sequence[Mapping[str, Any]]) -> bool:
    """Whether the sequence debugs a script with SCRIPT DEBUG SYNC: the server serves only the debugging client
    until its session ends, so every other connection waits on it by design."""

    return any([str(word).upper() for word in _words(item)[:3]] == ["SCRIPT", "DEBUG", "SYNC"] for item in commands)


def _writes_unreplicated(commands: Sequence[Mapping[str, Any]]) -> bool:
    """Whether the sequence writes keys its node does not replicate: DEBUG POPULATE adds them on the primary
    alone, and a script that calls set_repl may keep its writes from the replicas (fuzzer#88's replay)."""

    return any(
        [str(word).upper() for word in _words(item)[:2]] == ["DEBUG", "POPULATE"]
        or any("SET_REPL" in word for word in _words(item))
        for item in commands
    )


def _delays_persistence(commands: Sequence[Mapping[str, Any]], server_config: Mapping[str, Any]) -> bool:
    """Whether the sequence or its server_config sets rdb-key-save-delay or key-load-delay above 0: a save or a
    load then takes the delay once per key, so a restart's shutdown save or a DEBUG RELOAD outlasts any timeout
    (the walk's aaeee8d5b replay: 100,000 keys at 10 ms each is 1,000 s)."""

    delays = ("rdb-key-save-delay", "key-load-delay")
    settings = [(str(name).lower(), value) for name, value in server_config.items()]
    for item in commands:
        words = [str(word) for word in _words(item)]
        if [word.upper() for word in words[:2]] == ["CONFIG", "SET"]:
            settings += [(words[index].lower(), words[index + 1]) for index in range(2, len(words) - 1, 2)]
    return any(name in delays and str(value).strip() not in ("0", "") for name, value in settings)


def _debug_settings(commands: Sequence[Mapping[str, Any]]) -> bool:
    """Whether the sequence changes a DEBUG SET-* setting, which holds until the server restarts."""

    return any(
        len(words) >= 2 and words[0] == "DEBUG" and words[1].startswith("SET-")
        for words in ([str(word).upper() for word in _words(item)[:2]] for item in commands)
    )


def _set_on_replica(side: Any, commands: Sequence[Mapping[str, Any]]) -> None:
    """Make on node 1 the settings the sequence makes on node 0. They are not replicated, and some change how
    a replica applies the stream: DEBUG SET-SKIP-CHECKSUM-VALIDATION lets in a RESTORE the replica would
    refuse."""
    for item in commands:
        words = [str(word).upper() for word in _words(item)[:2]]
        if words == ["CONFIG", "SET"] or (len(words) == 2 and words[0] == "DEBUG" and words[1].startswith("SET-")):
            _ask(side, 1, *[side.substitute(argument) for argument in item["command"]])


def _scalar(value: Any) -> Any:
    """A reply value as RESP2 replies it: a boolean as 1 or null, a number (a double, or a numeric string) by its
    value."""

    if isinstance(value, bool):
        return 1 if value else None
    if isinstance(value, str) and _NUMBER.fullmatch(value):
        value = int(value) if _INTEGER.fullmatch(value) else float(value)
    if isinstance(value, float):
        return int(value) if value.is_integer() else value if math.isfinite(value) else str(value)
    return value


def _leaves(value: Any) -> list[Any]:
    """Every scalar of a reply, a map's keys and values alike, as :func:`_scalar` makes it."""

    if isinstance(value, dict):
        return [leaf for key, item in value.items() for leaf in (_scalar(key), *_leaves(item))]
    if isinstance(value, list):
        return [leaf for item in value for leaf in _leaves(item)]
    return [_scalar(value)]


def _shape(record: Optional[Mapping[str, Any]]) -> str:
    """A reply record as variants compare it: an array or map reply as the sorted multiset of its values (member
    order differs between encodings, and RESP3 nests what RESP2 flattens), any other reply by its value, and a
    record without a reply as itself."""

    if record is None or "reply" not in record:
        return canonical_json(record)
    if isinstance(record["reply"], (list, dict)):
        return canonical_json(sorted(canonical_json(leaf) for leaf in _leaves(record["reply"])))
    return canonical_json(_scalar(record["reply"]))


def _fields(record: Mapping[str, Any]) -> dict[str, str]:
    """The ``name:value`` fields of an INFO reply."""

    return dict(line.split(":", 1) for line in str(record.get("reply", "")).split() if ":" in line)


def _ask(side: Any, index: int, *command: Any) -> dict[str, Any]:
    """``command`` on node ``index`` of ``side``, on the checks' own connection: its record."""

    try:
        return side.connection(index, _CHECK).send(list(command))
    except (valkey.exceptions.ValkeyError, OSError) as exc:
        return {"error": f"cannot connect: {type(exc).__name__}: {exc}", "unanswered": True}


def _volatile(side: Any, index: int) -> bool:
    """Whether node ``index`` of ``side`` holds anything that may expire (a key, or a hash field), or cannot tell."""

    record = _ask(side, index, "INFO", "keyspace")
    if "reply" not in record:
        return True
    databases = (
        dict(item.split("=", 1) for item in value.split(",") if "=" in item) for value in _fields(record).values()
    )
    return any(int(db.get("expires", 0)) or int(db.get("keys_with_volatile_items", 0)) for db in databases)


def _owned_slots(side: Any, index: int) -> Optional[tuple[str, ...]]:
    """The slots (and slot ranges) node ``index`` of a cluster ``side`` owns, from its CLUSTER NODES; None if
    unread."""

    record = _ask(side, index, "CLUSTER", "NODES")
    myself = next((line.split() for line in str(record.get("reply", "")).splitlines() if "myself" in line), None)
    return None if "reply" not in record or myself is None else tuple(myself[8:])


def _ping(side: Any, index: int) -> dict[str, Any]:
    """PING on a new connection to node ``index`` of ``side``: its record."""

    try:
        connection = actions.open_raw_connection(side.resp, side.timeout_s, path=side.nodes[index].server.socket_path)
    except (valkey.exceptions.ValkeyError, OSError) as exc:
        return {"error": f"cannot connect: {type(exc).__name__}: {exc}", "unanswered": True}
    try:
        return connection.send(["PING"])
    finally:
        connection.close()


def _categories(side: Any, words: Sequence[str]) -> Optional[set[str]]:
    """The ACL categories of the command ``words`` name (its subcommand's and its container's together, for a
    container command, since denying the container denies every subcommand), from ``COMMAND INFO`` on node 0 of
    ``side``; None when it knows none."""

    info = (_ask(side, 0, "COMMAND", "INFO", words[0]).get("reply") or [None])[0]
    if not isinstance(info, list) or len(info) < 7:
        return None
    container = set(info[6])
    if len(info) > 9 and info[9] and len(words) > 1:
        name = f"{words[0]}|{words[1]}".lower()
        info = next((entry for entry in info[9] if str(entry[0]).lower() == name), None)
        if info is None:
            return None
    return container | set(info[6])


class _Worker:
    """One of the concurrency check's clients: the sequence on the side's nodes, on connections of its own."""

    def __init__(self, side: Any):
        self.side = side
        self.connections: dict[tuple[int, str], Any] = {}
        self.replies: list[dict[str, Any]] = []

    def run(self, commands: Sequence[Mapping[str, Any]], context: Any) -> None:
        self.replies = actions._run_sequence(self, commands, context)

    def connection(self, node: int, name: str) -> Any:
        if (node, name) not in self.connections:
            self.connections[node, name] = actions.open_raw_connection(
                self.side.resp, self.side.timeout_s, path=self.side.nodes[node].server.socket_path
            )
        return self.connections[node, name]

    def survives(self, node: int) -> bool:
        return self.side.survives(node)

    def forget(self, node: int, name: str) -> None:
        self.connections.pop((node, name)).close()

    def substitute(self, argument: Any) -> Any:
        return self.side.substitute(argument)

    def mask(self, value: Any) -> Any:
        return value

    def close(self) -> None:
        for connection in self.connections.values():
            connection.close()


class _Checks:
    """The checks of one compared sequence, each a method named after it that returns its outcome, and each run
    on an instance of its own; every server they start is fresh, from the compared run's configuration."""

    def __init__(
        self,
        context: Any,
        spec: ComponentSpec,
        config: Mapping[str, Any],
        base: Any,
        parent_binary: str,
        server_config: Mapping[str, Any],
        files: Mapping[str, bytes],
    ):
        self.context, self.spec, self.config = context, spec, config
        self.commands = config["commands"]
        # The compared run's candidate side, finished: the replies variants compare with.
        self.base = base
        self.binaries = {"candidate": base.binary, "parent": parent_binary}
        self.modules = {
            "candidate": config["modules"],
            "parent": config["modules"] if config["modules_parent"] is None else config["modules_parent"],
        }
        self.server_config, self.files = server_config, files
        # The ports every check of the sequence took, shared.
        self.kept: list[str] = []

    def free_ports(self, count: int) -> list[int]:
        """``count`` loopback ports, free now, that no check of the sequence took before (see
        :func:`actions._free_ports`, which never hands a port out twice in a run)."""

        return actions._free_ports(count, actions._port_block(self.context))

    def start(
        self,
        label: str,
        build: str = "candidate",
        *,
        nodes: Optional[int] = None,
        resp: Optional[int] = None,
        server_config: Optional[Mapping[str, Any]] = None,
        files: Optional[Mapping[str, bytes]] = None,
        options: Optional[Mapping[str, Any]] = None,
    ) -> tuple[Any, str]:
        """A fresh side of ``build``, started (its cluster formed) and its nodes named; and why it did not start,
        or an empty string. The caller finishes it either way."""

        config = {**self.config, "nodes": nodes or self.config["nodes"], "resp": resp or self.config["resp"]}
        ports = iter(self.free_ports(config["nodes"] * (2 if config["cluster"] else 1)))
        side = actions.new_side(label, self.binaries[build], config, self.modules[build], ports)
        side.options.update(options or {})
        failed = side.start(
            self.context,
            self.spec,
            self.server_config if server_config is None else server_config,
            self.files if files is None else files,
        )
        if not failed and config["cluster"]:
            failed = side.form_cluster(self.context, config["timeout_s"]) or ""
        if not failed:
            side.name_nodes()
        return side, failed

    def finish(self, side: Any, commands: Optional[Sequence[Mapping[str, Any]]] = None) -> None:
        side.finish(self.context, self.spec, self.commands if commands is None else commands)
        self.kept.extend(side.logs)

    @staticmethod
    def judge(check: str, side: Any, failed: str, **diagnostics: Any) -> Optional[dict[str, Any]]:
        """The outcome of a check whose finished ``side`` crashed (a node exited), or could not be set up (error);
        None otherwise."""

        if side.exited is None:
            return _errored(failed) if failed else None
        cause = side.crash_cause()
        return _failed(
            f"the candidate exited during the {check} check: {cause}",
            {"check": check, **diagnostics, "crashes": {"candidate": cause}},
            crash=True,
        )

    def replicate(
        self, primary: tuple[Any, int], replica: tuple[Any, int], digests: bool = True
    ) -> tuple[bool, dict[str, Any]]:
        """Make ``replica`` (a side and a node's index) a replica of ``primary``, and wait until it caught up (see
        :meth:`converged`)."""

        side, index = primary
        sent = _ask(*replica, "REPLICAOF", "127.0.0.1", side.nodes[index].options["port"])
        if "reply" not in sent:
            return False, {"replicaof": sent}
        return self.converged(primary, replica, digests)

    def converged(
        self, primary: tuple[Any, int], replica: tuple[Any, int], digests: bool = True
    ) -> tuple[bool, dict[str, Any]]:
        """Wait until ``replica``'s link is up and it has ``primary``'s replication offset and (with ``digests``)
        ``DEBUG DIGEST``: whether it did, and the last state read."""

        def attempt() -> tuple[bool, dict[str, Any]]:
            state: dict[str, Any] = {}
            for role, (side, index) in (("primary", primary), ("replica", replica)):
                info = _fields(_ask(side, index, "INFO", "replication"))
                state[role] = {"offset": info.get("master_repl_offset"), "digest": _ask(side, index, "DEBUG", "DIGEST")}
            state["link"] = info.get("master_link_status")
            if not digests:
                return state["link"] == "up" and state["primary"]["offset"] == state["replica"]["offset"], state
            done = state["link"] == "up" and state["primary"] == state["replica"]
            return done and "reply" in state["primary"]["digest"], state

        return poll_until(attempt, timeout_s=_SYNC_TIMEOUT_S, interval_s=0.1, context=self.context)

    @staticmethod
    def diverged(check: str, reason: str, synced: bool, state: Mapping[str, Any], **diagnostics: Any) -> dict:
        """The outcome of a replica that did (pass) or did not catch up with its primary."""

        if synced:
            return {"outcome": "pass"}
        if any("reply" not in state.get(role, {}).get("digest", {}) for role in ("primary", "replica")):
            return _errored(f"the replica's data could not be compared: {canonical_json(state)}")
        return _failed(
            f"the replica did not reach the primary's data within {_SYNC_TIMEOUT_S:g}s ({reason}): "
            f"primary {canonical_json(state['primary'])}, replica {canonical_json(state['replica'])}, "
            f"link {state['link']}",
            {"reason": reason, "check": check, **diagnostics, **state},
        )

    # -- variants --------------------------------------------------------------------------------------------

    def variants(self) -> dict[str, Any]:
        compared = [
            index
            for index, item in enumerate(self.commands)
            if item["command"] is not None
            and item["compare"] != "skip"
            and " ".join(_words(item)[:2]) not in _VARIES
            and _words(item)[0] not in _VARIES
            and not _words(item)[0].endswith("SCAN")
            and not _script_varies(_words(item), self.commands)
            # A command waited on until its reply met a predicate replies with whatever met it first, as far as
            # some asynchronous work had got (fuzzer#83); only a wait for an exact reply ends on that reply.
            and (item["wait_s"] is None or "equals" in (item["expect"] or {}))
        ]
        if self.modules["candidate"]:
            encodings = _skipped("modules are loaded")
        else:
            encodings = self.variant(
                "encodings", compared, {"server_config": _ENCODINGS}, server_config={**self.server_config, **_ENCODINGS}
            )
        resp = 5 - self.config["resp"]
        # A module's command (named with a dot, as test.callresp3attribute or JSON.GET; Valkey's never are) may reply by
        # the client's protocol on purpose: only Valkey's own commands must agree across RESP2 and RESP3.
        own = [index for index in compared if "." not in _words(self.commands[index])[0]]
        # A RESP2 client that subscribed may run only the Pub/Sub commands, PING, QUIT and RESET; a RESP3 one, any.
        subscribed = _subscribed(self.commands)
        own = [index for index in own if index not in subscribed]
        return _combine(encodings=encodings, protocol=self.variant("protocol", own, {"resp": resp}, resp=resp))

    def variant(self, name: str, compared: Sequence[int], change: Mapping[str, Any], **start: Any) -> dict[str, Any]:
        side, failed = self.start(f"candidate.{name}", **start)
        try:
            if not failed:
                side.replies = actions._run_sequence(side, self.commands, self.context)
        finally:
            self.finish(side)
        decided = self.judge("variants", side, failed, variant=name, **change)
        if decided is not None:
            return decided
        differences = []
        for index in compared:
            base, mine = actions._reply_at(self.base.replies, index), actions._reply_at(side.replies, index)
            if _shape(base) != _shape(mine):
                differences.append(
                    {"index": index, "command": actions._step_text(self.commands[index]), "base": base, "variant": mine}
                )
        if not differences:
            return {"outcome": "pass"}
        first = differences[0]
        return _failed(
            f"command {first['index']} ({first['command']}) replies otherwise with "
            + ", ".join(f"{key} {canonical_json(value)}" for key, value in change.items())
            + f": {canonical_json(first['variant'])}, not {canonical_json(first['base'])}",
            {"reason": "variant-mismatch", "check": "variants", "variant": name, **change, "commands": differences},
        )

    # -- replica ---------------------------------------------------------------------------------------------

    def replica(self) -> dict[str, Any]:
        if self.config["nodes"] > 1 or self.config["cluster"]:
            return _skipped("the sequence runs on several nodes or a cluster, whose topology it sets up itself")
        if _sets_replication(self.commands):
            return _skipped("the sequence sets its node's replication itself")
        if _writes_unreplicated(self.commands):
            return _skipped(
                "the sequence writes keys its node does not replicate (DEBUG POPULATE, or a script's set_repl)"
            )
        # Node 1 is the replica; the sequence names node 0 only.
        side, failed = self.start("candidate.replica", nodes=2, options=_QUICK_SYNC)
        synced, state = False, {}
        try:
            if not failed:
                synced, state = self.replicate((side, 0), (side, 1))
                if not synced:
                    failed = f"the replica did not sync before the sequence: {canonical_json(state)}"
            if not failed:
                _set_on_replica(side, self.commands)
                side.replies = actions._run_sequence(side, self.commands, self.context)
                synced, state = self.converged((side, 0), (side, 1))
        finally:
            self.finish(side)
        return self.judge("replica", side, failed) or self.diverged("replica", "replica-diverged", synced, state)

    # -- acl -------------------------------------------------------------------------------------------------

    def acl(self) -> dict[str, Any]:
        if any(
            words and (words[0] in ("ACL", "AUTH") or (words[0] == "HELLO" and "AUTH" in words))
            for words in map(_words, self.commands)
        ):
            return _skipped("the sequence manages users itself")
        # A restart would load the users back from the config file, and the replies are not judged, so restarts
        # are left out and nothing is waited for.
        steps = [index for index, item in enumerate(self.commands) if not item["restart"]]
        commands = [{**self.commands[index], "wait_s": None} for index in steps]
        return _combine(
            category=self.as_user("category", steps, commands, self.category_rule),
            keys=self.as_user("keys", steps, commands, self.key_rule),
        )

    def category_rule(self, side: Any, commands: Sequence[Mapping[str, Any]]) -> tuple[str, dict[int, bool]]:
        """A rule denying one ACL category the commands use, the one most of them (but not all) are in, and which
        commands it refuses."""

        categories = {}
        for index, item in enumerate(commands):
            words = _words(item)
            if words and words[0] not in _RUNS_OTHERS:
                found = _categories(side, words)
                if found is not None:
                    categories[index] = found
        counts = Counter(name for found in categories.values() for name in found if name not in _SPEED)
        if not counts:
            return "", {}
        chosen = max(sorted(counts), key=lambda name: (counts[name] < len(categories), counts[name]))
        return f"+@all -{chosen}", {index: chosen in found for index, found in categories.items()}

    def key_rule(self, side: Any, commands: Sequence[Mapping[str, Any]]) -> tuple[str, dict[int, bool]]:
        """A rule allowing only keys none of the commands name, and which commands it refuses: those with keys
        (``COMMAND GETKEYS``)."""

        refused = {}
        for index, item in enumerate(commands):
            words = _words(item)
            if not words or words[0] in _RUNS_OTHERS:
                continue
            keys = _ask(side, 0, "COMMAND", "GETKEYS", *[side.substitute(argument) for argument in item["command"]])
            if "no key arguments" in keys.get("error", ""):
                refused[index] = False
            elif keys.get("reply") and not any(fnmatch.fnmatchcase(str(key), _KEY_PATTERN) for key in keys["reply"]):
                refused[index] = True
        return f"+@all resetkeys ~{_KEY_PATTERN}", refused

    def as_user(self, name: str, steps: Sequence[int], commands: Sequence[Mapping[str, Any]], rule_of: Any) -> dict:
        """The sequence as the default user with the rule ``rule_of`` gives, on every node of a fresh side."""

        side, failed = self.start(f"candidate.acl-{name}")
        rule, refused = "", {}
        try:
            if not failed:
                rule, refused = rule_of(side, commands)
            if refused:
                for index in range(len(side.nodes)):
                    set_user = _ask(side, index, "ACL", "SETUSER", "default", *rule.split())
                    if "reply" not in set_user:
                        failed = f"ACL SETUSER default {rule}: {canonical_json(set_user)}"
                        break
            if refused and not failed:
                side.replies = actions._run_sequence(side, commands, self.context)
        finally:
            self.finish(side, commands)
        decided = self.judge("acl", side, failed, user=f"default {rule}")
        if decided is not None:
            return decided
        if not refused:
            return _skipped("no command of the sequence could be judged")
        bypass, overreach = [], []
        for record in side.replies:
            index = record["index"]
            if index not in refused or record.get("unanswered") or not ("reply" in record or "error" in record):
                continue
            error = record.get("error", "").removeprefix("ERR ")
            denied = _NOPERM in error.lower()
            found = {
                "index": steps[index],
                "command": actions._step_text(commands[index]),
                "reply": actions._without_index(record),
            }
            if refused[index] and not denied and not error.startswith(_BEFORE_ACL):
                bypass.append(found)
            elif not refused[index] and denied:
                overreach.append(found)
        for reason, found, verdict in (
            ("acl-bypass", bypass, "ran, though its rule refuses it"),
            ("acl-overreach", overreach, "was refused, though its rule allows it"),
        ):
            if found:
                return _failed(
                    f"command {found[0]['index']} ({found[0]['command']}) as the user 'default {rule}' {verdict}: "
                    f"{canonical_json(found[0]['reply'])}",
                    {"reason": reason, "check": "acl", "user": f"default {rule}", "commands": found},
                )
        return {"outcome": "pass", "user": f"default {rule}"}

    # -- concurrency -----------------------------------------------------------------------------------------

    def concurrency(self) -> dict[str, Any]:
        if _debugs_sync(self.commands):
            return _skipped(
                "the sequence debugs a script with SCRIPT DEBUG SYNC, which blocks the server for the session"
            )
        count = self.config["checks"]["concurrency"]
        # Replies are not judged, and N clients restarting one server is no sequence: restarts are left out and
        # nothing is waited for.
        steps = [index for index, item in enumerate(self.commands) if not item["restart"]]
        commands = [{**self.commands[index], "wait_s": None} for index in steps]
        # With a replica too, as the replica check would run one (see replica).
        replica = (
            self.config["checks"]["replica"]
            and self.config["nodes"] == 1
            and not self.config["cluster"]
            and not _sets_replication(self.commands)
            and not _writes_unreplicated(self.commands)
        )
        side, failed = self.start("candidate.concurrency", **({"nodes": 2, "options": _QUICK_SYNC} if replica else {}))
        workers: list[_Worker] = []
        hung = []
        # Whether each node a command went unanswered on answers a PING; one that exited crashed (finish finds it).
        answering: dict[int, bool] = {}
        synced, state = True, {}
        try:
            if not failed and replica:
                synced, state = self.replicate((side, 0), (side, 1))
                if not synced:
                    failed = f"the replica did not sync before the sequence: {canonical_json(state)}"
                else:
                    _set_on_replica(side, commands)
            if not failed:
                workers = [_Worker(side) for _ in range(count)]
                threads = [threading.Thread(target=worker.run, args=(commands, self.context)) for worker in workers]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                for number, worker in enumerate(workers):
                    last = worker.replies[-1] if worker.replies else {}
                    if not last.get("unanswered"):
                        continue
                    # So finish waits for a node lost on it.
                    side.replies = worker.replies
                    node = commands[last["index"]]["node"]
                    if node not in answering:
                        answering[node] = not side.survives(node) or "reply" in _ping(side, node)
                    if not answering[node]:
                        hung.append(
                            {
                                "connection": number,
                                "index": steps[last["index"]],
                                "command": actions._step_text(commands[last["index"]]),
                                "error": last["error"],
                            }
                        )
                if replica and not hung:
                    synced, state = self.converged((side, 0), (side, 1))
        finally:
            for worker in workers:
                worker.close()
            self.finish(side, commands)
        decided = self.judge("concurrency", side, failed, connections=count)
        if decided is not None:
            return decided
        if hung:
            return _failed(
                f"command {hung[0]['index']} ({hung[0]['command']}) on connection {hung[0]['connection']} of {count} "
                f"got no reply, and the server answered no PING within {self.config['timeout_s']:g}s",
                {"reason": "hang", "check": "concurrency", "connections": count, "commands": hung},
                crash=True,
            )
        return self.diverged("concurrency", "replica-diverged", synced, state, connections=count)

    # -- faults ----------------------------------------------------------------------------------------------

    def faults(self) -> dict[str, Any]:
        if _sets_replication(self.commands):
            return _skipped("the sequence sets its node's replication itself")
        if _manages_auth(self.commands):
            # The check's own connection would need the password or user the sequence set (fuzzer#88's replay).
            return _skipped("the sequence manages users or a password itself")
        if _debugs_sync(self.commands):
            # A SYNC debugging session holds the node: no restart or DEBUG RELOAD reaches it meanwhile.
            return _skipped(
                "the sequence debugs a script with SCRIPT DEBUG SYNC, which blocks the server for the session"
            )
        if _delays_persistence(self.commands, self.server_config):
            return _skipped("the sequence delays each key a save or a load takes, so no restart or reload ends")
        nodes = self.config["nodes"]
        # A single standalone node gets a replica, the side's last node; several nodes or a cluster keep the
        # topology the sequence sets up.
        single = nodes == 1 and not self.config["cluster"] and not _writes_unreplicated(self.commands)
        replica = nodes if single else None
        rng = self.context.rng(self.spec.id, "faults")
        # Each fault comes before a command, or after the last one (before == len(commands)).
        gaps = rng.sample(range(1, len(self.commands) + 1), k=min(len(self.commands), rng.randint(1, 3)))
        schedule = [
            {"before": before, "fault": rng.choice(_FAULTS), "node": rng.randrange(nodes + (replica is not None))}
            for before in sorted(gaps)
        ]
        persistence = rng.choice(_PERSISTENCE)
        if _writes_unreplicated(self.commands) or _debug_settings(self.commands):
            # DEBUG POPULATE's keys reach no AOF (fuzzer#86), and an AOF replays its commands without the DEBUG
            # SET-* settings they ran under, which a restart drops (a RESTORE of a payload whose checksum was not
            # checked is refused): only an RDB saved at shutdown keeps the data as it was.
            persistence = _PERSISTENCE[0]
        side, failed = self.start(
            "candidate.faults", nodes=nodes + (replica is not None), options={**_QUICK_SYNC, **persistence}
        )
        synced, state, problem, exact = True, {}, None, True
        try:
            if not failed and replica is not None:
                synced, state = self.replicate((side, 0), (side, replica))
                if not synced:
                    failed = f"the replica did not sync before the sequence: {canonical_json(state)}"
                _set_on_replica(side, self.commands)
            if not failed:
                side.replies, start = [], 0
                for fault in [*schedule, None]:
                    end = len(self.commands) if fault is None else fault["before"]
                    replies = actions._run_sequence(side, self.commands[start:end], self.context)
                    side.replies += [{**record, "index": record["index"] + start} for record in replies]
                    start = end
                    if fault is None or (side.replies and side.replies[-1].get("unanswered")):
                        break
                    # A DEBUG RELOAD, or a replica restarted (as a primary, until it is made a replica again), drops
                    # what expired by then, which the other node keeps: from there the replica must only catch up
                    # with its primary's offset (fuzzer#88's replay). Asked before the fault, which drops it.
                    drops = (fault["fault"] == "reload" or fault["node"] == replica) and _volatile(side, 0)
                    problem = self.inject(side, fault, replica, exact)
                    exact = exact and not drops
                    if problem is not None:
                        break
                answered = not (side.replies and side.replies[-1].get("unanswered"))
                if problem is None and answered and replica is not None:
                    synced, state = self.converged((side, 0), (side, replica), exact)
        finally:
            self.finish(side)
        decided = self.judge("faults", side, failed, faults=schedule, persistence=persistence)
        if decided is not None:
            return decided
        if problem is not None:
            return problem
        if side.replies and side.replies[-1].get("unanswered"):
            last = side.replies[-1]
            return _errored(
                f"command {last['index']} ({actions._step_text(self.commands[last['index']])}) got no reply "
                f"after the faults {canonical_json(schedule)}: {last.get('error')}"
            )
        outcome = self.diverged("faults", "replica-diverged", synced, state, faults=schedule)
        # What the check did, to reproduce it: the run's seed draws the same schedule again.
        return {**outcome, "faults": schedule, "persistence": persistence} if outcome["outcome"] == "pass" else outcome

    def settled_digest(self, side: Any, node: int) -> Optional[dict[str, Any]]:
        """Node ``node``'s DEBUG DIGEST if it holds still for a moment, else None (something changes its data)."""

        first = _ask(side, node, "DEBUG", "DIGEST")
        self.context.sleep(_SETTLE_S)
        return first if _ask(side, node, "DEBUG", "DIGEST") == first else None

    def loaded(self, side: Any, node: int) -> bool:
        """Wait until node ``node`` of ``side`` no longer loads its data: whether it did."""

        def attempt() -> tuple[bool, None]:
            record = _ask(side, node, "INFO", "persistence")
            return "reply" in record and _fields(record).get("loading") != "1", None

        return poll_until(attempt, timeout_s=_SYNC_TIMEOUT_S, interval_s=0.05, context=self.context)[0]

    def inject(
        self, side: Any, fault: Mapping[str, Any], replica: Optional[int], exact: bool = True
    ) -> Optional[dict[str, Any]]:
        """Apply one fault of the faults check to ``side``: None once it is in place and the side is whole again,
        else the check's outcome. ``exact``: a replica must also reach its primary's DEBUG DIGEST."""

        node, what = fault["node"], "DEBUG RELOAD" if fault["fault"] == "reload" else "a restart"
        if node == replica:
            # The replica's data before the fault is the primary's: it must have caught up first.
            synced, state = self.converged((side, 0), (side, replica), exact)
            if not synced:
                return self.diverged("faults", "replica-diverged", synced, state, fault=dict(fault))
        # The node must keep its data, judged when nothing else changes it meanwhile: nothing it holds can expire,
        # its data holds still (no asynchronous work, as a slot migration, moves it), and in a cluster it owns the
        # same slots before and after.
        before = None if _volatile(side, node) else self.settled_digest(side, node)
        slots = _owned_slots(side, node) if self.config["cluster"] else None
        if fault["fault"] == "reload":
            reloaded = _ask(side, node, "DEBUG", "RELOAD")
            if str(reloaded.get("error", "")).startswith("BUSY"):
                # A script the sequence left running holds the node (fuzzer#88's replay): no reload to judge.
                return None
            if "reply" not in reloaded:
                return _failed(
                    f"DEBUG RELOAD on node {node} before command {fault['before']} failed: {canonical_json(reloaded)}",
                    {"reason": "reload-failed", "check": "faults", "fault": dict(fault), "reply": reloaded},
                )
        else:
            restarted = side.restart(node, self.context)
            if "reply" not in restarted:
                # A node that exited at startup is judged as a crash (see faults); one that did not come back
                # otherwise, an error.
                return _errored(f"restarting node {node} before command {fault['before']}: {restarted['error']}")
            # A restarted node answers while it still loads its data (DEBUG DIGEST then digests what it loaded so
            # far): its data is judged once it loaded.
            if not self.loaded(side, node):
                return _errored(
                    f"node {node} was still loading {_SYNC_TIMEOUT_S:g}s after a restart before "
                    f"command {fault['before']}"
                )
        if before is not None and "reply" in before and (slots is None or _owned_slots(side, node) == slots):
            after = _ask(side, node, "DEBUG", "DIGEST")
            if "reply" in after and after != before:
                return _failed(
                    f"node {node} came back from {what} before command {fault['before']} with DEBUG DIGEST "
                    f"{canonical_json(after)}, not {canonical_json(before)}",
                    {"reason": "lost-data", "check": "faults", "fault": dict(fault), "before": before, "after": after},
                )
        if node == replica and fault["fault"] == "restart":
            # The replica starts as a primary: REPLICAOF was a command, not its configuration.
            # Restarted as a primary, it dropped what expired: its digest then need not match (see faults).
            synced, state = self.replicate((side, 0), (side, replica), exact and not _volatile(side, 0))
            _set_on_replica(side, self.commands)
            if not synced:
                return self.diverged("faults", "replica-diverged", synced, state, fault=dict(fault))
        return None

    # -- compat ----------------------------------------------------------------------------------------------

    def compat(self) -> dict[str, Any]:
        if self.config["nodes"] > 1 or self.config["cluster"]:
            return _skipped("the sequence runs on several nodes or a cluster")
        if bool(self.modules["candidate"]) != bool(self.modules["parent"]):
            return _skipped("a module is loaded on one build only")
        dbfilename = str(self.server_config.get("dbfilename", "dump.rdb"))
        parent, failed = self.start("parent.compat", "parent", options=_QUICK_SYNC)
        load = sync = None
        try:
            if not failed:
                parent.replies = actions._run_sequence(parent, self.commands, self.context)
                # The digest of what SAVE wrote: read before and after it, until no key expired in between.
                for _ in range(3):
                    digest, saved = _ask(parent, 0, "DEBUG", "DIGEST"), _ask(parent, 0, "SAVE")
                    if _ask(parent, 0, "DEBUG", "DIGEST") == digest:
                        break
                else:
                    saved = {"error": "its data changed while it saved, three times"}
                if "reply" not in saved or "reply" not in digest:
                    failed = f"the parent did not save its data: SAVE {canonical_json(saved)}, DEBUG DIGEST {canonical_json(digest)}"
            if not failed:
                rdb = os.path.join(component_artifact_dir(self.context, self.spec), "parent.compat.rdb")
                try:
                    shutil.copyfile(os.path.join(parent.nodes[0].server.workdir, dbfilename), rdb)
                except OSError as exc:
                    failed = f"the parent's RDB could not be kept: {exc}"
            if not failed:
                self.kept.append(rdb)
                changed = self.changed_write(parent)
                load = self.compat_load(rdb, dbfilename, digest, changed)
                sync = self.compat_sync(parent, changed)
        finally:
            self.finish(parent)
        if failed:
            return _errored(failed)
        return _combine(load=load, sync=sync)

    def changed_write(self, parent: Any) -> str:
        """The first write command the sequence marks as the merge's change and whose reply on the parent shows it,
        described, or an empty string: the merge changes what that command writes, so the parent's data may hold
        what the candidate never makes (a negative expire it refuses to write, and drops when it loads one)."""

        for index in actions._compare(self.commands, self.base, parent)[0]:
            if "@write" in (_categories(parent, _words(self.commands[index])) or ()):
                return f"command {index} ({actions._step_text(self.commands[index])})"
        return ""

    def compat_load(self, rdb: str, dbfilename: str, digest: Mapping[str, Any], changed: str) -> dict[str, Any]:
        with open(rdb, "rb") as handle:
            files = {**self.files, dbfilename: handle.read()}
        # The RDB is what it loads, whatever server_config says of the AOF.
        side, failed = self.start("candidate.compat-load", files=files, options={"appendonly": "no"})
        mine: dict[str, Any] = {}
        try:
            if not failed:
                # It loaded the RDB as a primary at startup, which drops the keys that expired since the parent saved;
                # loaded again as a replica (of no server that answers), it keeps them, as the parent's digest does.
                mine = _ask(side, 0, "REPLICAOF", "127.0.0.1", self.free_ports(1)[0])
                if "reply" in mine:
                    mine = _ask(side, 0, "DEBUG", "RELOAD", "NOSAVE")
                if "reply" in mine:
                    mine = _ask(side, 0, "DEBUG", "DIGEST")
        finally:
            self.finish(side)
        diagnostics = {"reason": "compat-load", "rdb": rdb}
        if failed and side.exited is not None:
            cause = side.crash_cause()
            # A controlled refusal to load the parent's data (a fatal log, not a crash) is expected when the
            # merge hardens a loader to reject what the parent wrote: the parent's RDB holds what the candidate
            # never makes, as on the digest path below.
            if changed and not side.crashed():
                return _skipped(_CHANGED.format(changed))
            return _failed(
                f"the candidate exited loading the parent's RDB: {cause}",
                {**diagnostics, "check": "compat", "crashes": {"candidate": cause}},
                crash=True,
            )
        decided = self.judge("compat", side, failed, **diagnostics)
        if decided is not None:
            return decided
        if mine != digest and mine != self.own_digest():
            if changed:
                return _skipped(_CHANGED.format(changed))
            return _failed(
                f"the parent's RDB loads into the candidate with DEBUG DIGEST {canonical_json(mine)}, "
                f"not the parent's {canonical_json(digest)}",
                {**diagnostics, "check": "compat", "parent": digest, "candidate": mine},
            )
        return {"outcome": "pass"}

    def compat_sync(self, parent: Any, changed: str) -> dict[str, Any]:
        if _sets_replication(self.commands):
            return _skipped("the sequence sets its node's replication itself")
        side, failed = self.start("candidate.compat-sync")
        synced, state = False, {}
        try:
            if not failed:
                synced, state = self.replicate((parent, 0), (side, 0))
        finally:
            self.finish(side)
        if not synced and state.get("link") == "up" and state["primary"]["offset"] == state["replica"]["offset"]:
            synced = state["replica"]["digest"] == self.own_digest()
        # Refusing to load what only the parent writes, as on the load path (fuzzer#84).
        if changed and side.exited is not None and not side.crashed():
            return _skipped(_CHANGED.format(changed))
        decided = self.judge("compat", side, failed)
        if decided is not None or synced:
            return decided or {"outcome": "pass"}
        # The parent is the primary: if it (or the fork child making the replica's RDB) crashed, as a bug the merge
        # fixes can make it, no RDB reached the candidate, and the candidate is not at fault.
        log = parent.nodes[0].server.log_path
        reports = scan_for_crashes({log: log}, CRASH_PATTERNS)
        if reports:
            return _errored(f"the parent crashed as the replica's primary: {crash_cause(next(iter(reports.values())))}")
        if changed and state.get("link") == "up" and state["primary"]["offset"] == state["replica"]["offset"]:
            return _skipped(_CHANGED.format(changed))
        return self.diverged("compat", "compat-sync", synced, state)

    def own_digest(self) -> Optional[Mapping[str, Any]]:
        """The ``DEBUG DIGEST`` of a fresh candidate after the sequence, or None if it could not be read. A merge may
        change on purpose what the parent's data becomes in the candidate; what the candidate makes of it is then
        compatible when it is what the candidate holds after the same sequence."""

        if not hasattr(self, "_own"):
            side, failed = self.start("candidate.compat-own")
            digest: Mapping[str, Any] = {}
            try:
                if not failed:
                    side.replies = actions._run_sequence(side, self.commands, self.context)
                    digest = _ask(side, 0, "DEBUG", "DIGEST")
            finally:
                self.finish(side)
            self._own = digest if side.exited is None and "reply" in digest else None
        return self._own
