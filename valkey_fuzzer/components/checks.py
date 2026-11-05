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
from .polling import poll_until

_ORDER = ("variants", "replica", "acl", "concurrency", "compat")
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
# compare them. DEBUG, OBJECT and MEMORY report internals (DEBUG HTSTATS-KEY a hashtable's buckets), and CONFIG
# the encoding thresholds the variant changed.
_VARIES = ("DEBUG", "OBJECT", "MEMORY", "CONFIG", "CLIENT INFO", "CLIENT LIST", "HELLO", "INFO")
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
# Guards the ports the checks of a sequence took (see _Checks.free_ports).
_PORTS_LOCK = threading.Lock()


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
    taken: set[int] = set()
    checks = {
        name: _Checks(context, spec, config, candidate, parent_binary, server_config, files, taken) for name in enabled
    }
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
    """The ACL categories of the command ``words`` name (its subcommand's, for a container command), from
    ``COMMAND INFO`` on node 0 of ``side``; None when it knows none."""

    info = (_ask(side, 0, "COMMAND", "INFO", words[0]).get("reply") or [None])[0]
    if not isinstance(info, list) or len(info) < 7:
        return None
    if len(info) > 9 and info[9] and len(words) > 1:
        name = f"{words[0]}|{words[1]}".lower()
        info = next((entry for entry in info[9] if str(entry[0]).lower() == name), None)
        if info is None:
            return None
    return set(info[6])


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
        taken: set[int],
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
        self.taken = taken
        self.kept: list[str] = []

    def free_ports(self, count: int) -> list[int]:
        """``count`` loopback ports, free now, that no check of the sequence took before: a port stays free until
        its server binds it, so a check running at once could be given it too."""

        with _PORTS_LOCK:
            # Of that many free ports, at most len(self.taken) were taken.
            ports = [port for port in actions._free_ports(count + len(self.taken)) if port not in self.taken][:count]
            self.taken.update(ports)
        return ports

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

    def replicate(self, primary: tuple[Any, int], replica: tuple[Any, int]) -> tuple[bool, dict[str, Any]]:
        """Make ``replica`` (a side and a node's index) a replica of ``primary``, and wait until it caught up (see
        :meth:`converged`)."""

        side, index = primary
        sent = _ask(*replica, "REPLICAOF", "127.0.0.1", side.nodes[index].options["port"])
        if "reply" not in sent:
            return False, {"replicaof": sent}
        return self.converged(primary, replica)

    def converged(self, primary: tuple[Any, int], replica: tuple[Any, int]) -> tuple[bool, dict[str, Any]]:
        """Wait until ``replica``'s link is up and it has ``primary``'s replication offset and ``DEBUG DIGEST``:
        whether it did, and the last state read."""

        def attempt() -> tuple[bool, dict[str, Any]]:
            state: dict[str, Any] = {}
            for role, (side, index) in (("primary", primary), ("replica", replica)):
                info = _fields(_ask(side, index, "INFO", "replication"))
                state[role] = {"offset": info.get("master_repl_offset"), "digest": _ask(side, index, "DEBUG", "DIGEST")}
            state["link"] = info.get("master_link_status")
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
        ]
        if self.modules["candidate"]:
            encodings = _skipped("modules are loaded")
        else:
            encodings = self.variant(
                "encodings", compared, {"server_config": _ENCODINGS}, server_config={**self.server_config, **_ENCODINGS}
            )
        resp = 5 - self.config["resp"]
        return _combine(encodings=encodings, protocol=self.variant("protocol", compared, {"resp": resp}, resp=resp))

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
        # Node 1 is the replica; the sequence names node 0 only.
        side, failed = self.start("candidate.replica", nodes=2, options=_QUICK_SYNC)
        synced, state = False, {}
        try:
            if not failed:
                synced, state = self.replicate((side, 0), (side, 1))
                if not synced:
                    failed = f"the replica did not sync before the sequence: {canonical_json(state)}"
            if not failed:
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
        count = self.config["checks"]["concurrency"]
        # Replies are not judged, and N clients restarting one server is no sequence: restarts are left out and
        # nothing is waited for.
        steps = [index for index, item in enumerate(self.commands) if not item["restart"]]
        commands = [{**self.commands[index], "wait_s": None} for index in steps]
        replica = self.config["checks"]["replica"] and self.config["nodes"] == 1 and not self.config["cluster"]
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
                load = self.compat_load(rdb, dbfilename, digest)
                sync = self.compat_sync(parent)
        finally:
            self.finish(parent)
        if failed:
            return _errored(failed)
        return _combine(load=load, sync=sync)

    def compat_load(self, rdb: str, dbfilename: str, digest: Mapping[str, Any]) -> dict[str, Any]:
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
            return _failed(
                f"the candidate exited loading the parent's RDB: {cause}",
                {**diagnostics, "check": "compat", "crashes": {"candidate": cause}},
                crash=True,
            )
        decided = self.judge("compat", side, failed, **diagnostics)
        if decided is not None:
            return decided
        if mine != digest:
            return _failed(
                f"the parent's RDB loads into the candidate with DEBUG DIGEST {canonical_json(mine)}, "
                f"not the parent's {canonical_json(digest)}",
                {**diagnostics, "check": "compat", "parent": digest, "candidate": mine},
            )
        return {"outcome": "pass"}

    def compat_sync(self, parent: Any) -> dict[str, Any]:
        side, failed = self.start("candidate.compat-sync")
        synced, state = False, {}
        try:
            if not failed:
                synced, state = self.replicate((parent, 0), (side, 0))
        finally:
            self.finish(side)
        return self.judge("compat", side, failed) or self.diverged("compat", "compat-sync", synced, state)
