"""The command-metadata oracle: what a build declares about its commands, held against what they do.

``COMMAND`` lists every command's arity, flags, and key specs (a module's commands too), and ``COMMAND GETKEYS``
names the keys of one command. Clients, proxies, ACL rules, and the cluster's routing act on what they say, so a
command that does otherwise is a bug in Valkey. The oracle sends the command-table workload's commands again, its
first draws from the run's seed in their order, to private servers of the deployment's build and modules, where
nothing else changes the data, and holds each command to its own metadata:

- ``keys``: ``COMMAND GETKEYS`` names the keys the command's key specs find (``begin_search`` and ``find_keys``),
  and a command without the ``movablekeys`` flag has them where ``first``, ``last``, and ``step`` say
  (``keys-mismatch``).
- ``arity``: a command with fewer or more arguments than its arity allows replies ``wrong number of arguments``
  (``arity-accepted``).
- ``readonly``: a command with the ``readonly`` flag leaves ``DEBUG DIGEST`` as it was and propagates nothing: the
  node's ``master_repl_offset`` and the bytes fed to its AOF stay the same (``readonly-wrote``). Left unjudged: a
  command whose own key specs declare a key written (``PFCOUNT``), one that runs other commands (a script), and
  one during which a key or a hash field expired, as Valkey counts them.
- ``replica``, on a standalone deployment: a read-only replica refuses a command with the ``write`` flag with
  ``READONLY`` (``replica-wrote``), and does not refuse one with the ``readonly`` flag (``replica-refused``).
- ``routing``, on a cluster deployment: a primary that owns no slot answers a command ``MOVED`` to the slot of
  the keys and shard channels it declares, ``CROSSSLOT`` when they are of several slots, and serves a command
  that declares none (``misrouted``).

A transaction (``MULTI``, commands, ``EXEC``) is judged ``readonly`` as a whole, when every command in it is.
"""

import json
import os
import re
from collections.abc import Mapping, Sequence
from typing import Any, Optional

import valkey
from valkey.crc import key_slot

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, Evidence, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField

from . import actions, checks, command_table
from .capabilities import resolve_binary
from .instances import deployment, is_cluster
from .polling import poll_until
from .registration import component_registration
from .schemas import INTEGER, LIST, TEXT, declared, mapping_of
from .snapshots import instance_modules

EVIDENCE = "valkey.command.metadata"
CHECKS = ("keys", "arity", "readonly", "replica", "routing")

# Commands upstream knows are misrouted in cluster mode and is fixing: the soak is not failed on them every
# run. Drop one when its fix lands. MSETEX's command JSON has no get_keys_function, so a node that owns no
# slot runs it instead of replying MOVED/CROSSSLOT (valkey-io/valkey#4936, fix PR #4951).
ROUTING_KNOWN_GAPS = {"MSETEX"}
# What a command that broke its metadata did, by the finding's reason: a run's summary names each such command
# once, in words that stay the same from run to run.
_BROKEN = {
    "keys-mismatch": "has other keys by COMMAND GETKEYS than by its key specs, or by its first, last, and step",
    "arity-accepted": "was not refused a number of arguments its arity excludes",
    "readonly-wrote": "has the readonly flag, yet changed data or propagated",
    "replica-wrote": "has the write flag, yet a read-only replica did not refuse it",
    "replica-refused": "has the readonly flag, yet a read-only replica refused it",
    "misrouted": "was not routed by the keys it declares on a node that owns no slot",
}
# Key-spec flags that declare the key written.
_WRITES = frozenset({"RW", "OW", "RM", "insert", "update", "delete"})
# What the nodes run with: an AOF, so every command node 0 propagates moves its replication offset and its AOF
# even without a replica (a cluster node has none here), and no AOF rewrite or PING to its replicas, which would
# move them when no command did.
_NODE_OPTIONS = {
    **checks._QUICK_SYNC,
    "appendonly": "yes",
    "auto-aof-rewrite-percentage": 0,
    "repl-ping-replica-period": 3600,
}
_NUMKEYS = re.compile(r"0|[1-9]\d*")
_VOLATILE_ITEMS = re.compile(r"keys_with_volatile_items=(\d+)")


def read_metadata(info: Sequence[Any]) -> dict[str, dict[str, Any]]:
    """Each command's metadata by the name the workload sends it under (``CONFIG GET``), from the reply to
    ``COMMAND`` (RESP2): its ``arity``, ``flags``, ``range`` (first, last, step), and key ``specs``."""

    metadata = {}
    entries = list(info)
    while entries:
        entry = entries.pop()
        if len(entry) > 9 and entry[9]:
            entries.extend(entry[9])
            continue
        specs = []
        for flat in entry[8] if len(entry) > 8 else ():
            spec = command_table._pairs(flat)
            begin, find = command_table._pairs(spec["begin_search"]), command_table._pairs(spec["find_keys"])
            specs.append(
                {
                    "flags": set(spec.get("flags") or ()),
                    "begin": (begin["type"], command_table._pairs(begin["spec"])),
                    "find": (find["type"], command_table._pairs(find["spec"])),
                }
            )
        metadata[entry[0].upper().replace("|", " ")] = {
            "arity": entry[1],
            "flags": set(entry[2]),
            "range": tuple(entry[3:6]),
            "specs": specs,
        }
    return metadata


def spec_positions(metadata: Mapping[str, Any], words: Sequence[str]) -> Optional[list[tuple[int, bool]]]:
    """Where in ``words`` the command's key specs find its keys, each with whether the spec says it is not a key
    (``not_key``: a shard channel, which the cluster routes like one). None when the specs do not say: one is
    ``incomplete`` or of an unknown type, or the arguments do not fit them (a syntax error)."""

    argc, found = len(words), []
    for spec in metadata["specs"]:
        if "incomplete" in spec["flags"]:
            return None
        kind, begin = spec["begin"]
        if kind == "index":
            first = begin["index"]
        elif kind == "keyword":
            # Searched from startfrom on, or backwards from the end for a negative one; the keys follow it. A
            # command without the keyword has none of the spec's keys.
            start = begin["startfrom"]
            indexes = range(start, argc - 1) if start > 0 else range(argc + start, 1, -1)
            keyword = str(begin["keyword"]).upper()
            first = next((index + 1 for index in indexes if 0 < index < argc and words[index].upper() == keyword), 0)
            if not first:
                continue
        else:
            return None
        kind, find = spec["find"]
        if kind == "range":
            step = find["keystep"]
            if find["lastkey"] >= 0:
                last = first + find["lastkey"]
            elif not find["limit"]:
                last = argc + find["lastkey"]
            else:
                # Of the arguments left, the first 1/limit are keys (XREAD's streams, before their IDs).
                last = first + (argc - first) // find["limit"] + find["lastkey"]
        elif kind == "keynum":
            step = find["keystep"]
            count = first + find["keynumidx"]
            if count >= argc or not _NUMKEYS.fullmatch(words[count]):
                return None
            first += find["firstkey"]
            last = first + (int(words[count]) - 1) * step
        else:
            return None
        if not first <= last < argc:
            return None
        found += [(index, "not_key" in spec["flags"]) for index in range(first, last + 1, step)]
    return found


def range_positions(metadata: Mapping[str, Any], words: Sequence[str]) -> Optional[list[int]]:
    """Where in ``words`` the command's ``first``, ``last``, and ``step`` put its keys; None when the arguments
    end before them."""

    first, last, step = metadata["range"]
    if not first:
        return []
    if last < 0:
        last += len(words)
    return list(range(first, last + 1, step)) if first <= last < len(words) else None


def _text(words: Sequence[str]) -> str:
    """A command as a summary names it: a long argument cut short (the artifact holds it whole)."""

    return " ".join(word if len(word) <= 64 else f"{word[:61]}..." for word in words)


class _Run:
    """One run of the checks on a started side: node 0 runs every command; node 1, a replica of node 0, or in
    cluster mode a primary that owns no slot, the commands it is judged on."""

    def __init__(self, side: Any, cluster: bool):
        self.side, self.cluster = side, cluster
        # Each command's metadata by its name (see read_metadata), once the side's command table is read.
        self.metadata: Mapping[str, Mapping[str, Any]] = {}
        self.judged = dict.fromkeys(CHECKS, 0)
        # The first finding of each command and reason, with how many times it was found.
        self.findings: dict[tuple[str, str], dict[str, Any]] = {}
        self.sent: list[list[str]] = []
        # The command that got no reply, which ends the run: its node, words, and record.
        self.stopped: Optional[tuple[int, list[str], dict[str, Any]]] = None

    def send(self, node: int, name: str, words: list[str]) -> dict[str, Any]:
        """Command ``name`` as ``words`` on the main connection to ``node``: its record. A connection the server
        had closed (its answer to a QUIT before) opens anew, once."""

        record: dict[str, Any] = {}
        for _ in range(2):
            try:
                record = self.side.connection(node, "main").send(words)
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                record = {"error": f"cannot connect: {type(exc).__name__}: {exc}", "unanswered": True}
                break
            if not record.pop("closed", False):
                break
            self.side.forget(node, "main")
        if record.get("unanswered"):
            self.stopped = (node, words, record)
        elif "pubsub" in self.metadata[name]["flags"]:
            # A Pub/Sub command confirms each channel with a reply of its own (UNSUBSCRIBE a b replies twice), so
            # the replies after its first no longer pair with commands: the next command takes a new connection.
            self.side.forget(node, "main")
        return record

    def find(
        self, reason: str, index: int, name: str, words: Sequence[str], declared: Any, observed: Any, detail: str
    ) -> None:
        """Record that command ``name``, sent to node 0 as number ``index``, broke its metadata."""

        finding = self.findings.setdefault(
            (name, reason),
            {
                "reason": reason,
                "name": name,
                "index": index,
                "command": _text(words),
                "declared": declared,
                "observed": observed,
                "detail": detail,
                "count": 0,
            },
        )
        finding["count"] += 1

    def run(self, group: Sequence[tuple[str, list[str]]]) -> None:
        """One draw of the workload: a command, or a transaction."""

        # A transaction's MULTI and EXEC are not judged; its commands are, each by its reply as it was queued.
        wrapped = len(group) > 1
        body = [(len(self.sent) + wrapped + offset, *item) for offset, item in enumerate(group[1:-1] or group)]
        keys = [self.keys(*item) for item in body]
        quiet = all(self.reads_only(name, words) for _, name, words in body)
        before = self.state(digest_last=True) if quiet else None
        replies = []
        for name, words in group:
            self.sent.append(words)
            replies.append(self.send(0, name, words))
            if self.stopped:
                return
        if before is not None:
            self.readonly(group, before)
        for item, routed, reply in zip(body, keys, replies[wrapped:]):
            self.arity(*item, reply)
            if self.stopped:
                return
            if self.cluster:
                # What node 0 replied to a queued command came with its EXEC: there is none to compare with.
                self.routing(*item, routed, None if wrapped else reply)
            else:
                self.replica(*item)

    # -- keys ------------------------------------------------------------------------------------------------

    def keys(self, index: int, name: str, words: list[str]) -> Optional[list[str]]:
        """Judge the command's keys, and return what the cluster routes it by: its keys as ``COMMAND GETKEYS``
        names them and the shard channels its key specs declare; None when they are not known."""

        metadata = self.metadata[name]
        record = checks._ask(self.side, 0, "COMMAND", "GETKEYS", *words)
        if "no key arguments" in str(record.get("error", "")):
            named: Optional[list[str]] = []
        else:
            # Any other error: the arguments are not the command's (their count, or a numkeys beyond them).
            named = record.get("reply") if isinstance(record.get("reply"), list) else None
        if named is None:
            return None
        if "movablekeys" in metadata["flags"] and not metadata["specs"]:
            # A module command that finds its keys by a callback alone (getkeys-api) has no key spec to hold
            # COMMAND GETKEYS to; the keys that names are what the cluster routes it by.
            return named
        positions = spec_positions(metadata, words)
        if positions is None:
            return None
        self.judged["keys"] += 1
        specified = [words[position] for position, not_key in positions if not not_key]
        declared = {"key_specs": specified}
        # first, last, and step count a shard channel too.
        ranged = None if "movablekeys" in metadata["flags"] else range_positions(metadata, words)
        if ranged is not None:
            declared["first_last_step"] = [words[position] for position in ranged]
        if sorted(specified) != sorted(named):
            detail = f"COMMAND GETKEYS names {named}, its key specs {specified}"
        elif ranged is not None and sorted(ranged) != sorted(position for position, _ in positions):
            detail = (
                f"first, last, and step {list(metadata['range'])} put its keys at {declared['first_last_step']}, "
                f"its key specs at {[words[position] for position, _ in positions]}"
            )
        else:
            return named + [words[position] for position, not_key in positions if not_key]
        self.find("keys-mismatch", index, name, words, declared, {"getkeys": named}, detail)
        return None

    # -- arity -----------------------------------------------------------------------------------------------

    def arity(self, index: int, name: str, words: list[str], reply: Mapping[str, Any]) -> None:
        arity = self.metadata[name]["arity"]
        if len(words) == arity or (arity < 0 and len(words) >= -arity):
            return
        self.judged["arity"] += 1
        if "wrong number of arguments" not in str(reply.get("error", "")):
            self.find(
                "arity-accepted",
                index,
                name,
                words,
                {"arity": arity},
                {"arguments": len(words), "reply": reply},
                f"its arity is {arity}, yet with {len(words)} arguments it replied {json.dumps(reply)[:200]}",
            )

    # -- readonly --------------------------------------------------------------------------------------------

    def reads_only(self, name: str, words: Sequence[str]) -> bool:
        """Whether the command's metadata says it changes nothing: the ``readonly`` flag, on a command whose key
        specs declare no key written (PFCOUNT's says it may change its key and propagate) and which runs no
        other command (an EVAL_RO may call PFCOUNT)."""

        metadata = self.metadata[name]
        return (
            "readonly" in metadata["flags"]
            and not any(spec["flags"] & _WRITES for spec in metadata["specs"])
            and words[0].upper() not in checks._RUNS_OTHERS
        )

    def state(self, digest_last: bool) -> Optional[dict[str, Any]]:
        """Node 0's data and what it propagated so far, or None if unread. The counters are read before the
        digest ahead of a command and after it behind one, so whatever moved a digest or an offset between the
        two digests lies between the two readings of the counters."""

        digest = None if digest_last else checks._ask(self.side, 0, "DEBUG", "DIGEST")
        info = checks._ask(self.side, 0, "INFO", "replication", "stats", "persistence", "keyspace")
        if digest_last:
            digest = checks._ask(self.side, 0, "DEBUG", "DIGEST")
        if "reply" not in info or "reply" not in digest:
            return None
        fields = checks._fields(info)
        return {
            "digest": digest["reply"],
            "master_repl_offset": fields.get("master_repl_offset"),
            # What was fed to the AOF: written, or waiting for the write.
            "aof": int(fields.get("aof_current_size", 0)) + int(fields.get("aof_buffer_length", 0)),
            "expired": [fields.get("expired_keys"), fields.get("expired_fields")],
            "volatile_items": sum(map(int, _VOLATILE_ITEMS.findall(str(info["reply"])))),
        }

    def readonly(self, group: Sequence[tuple[str, list[str]]], before: dict[str, Any]) -> None:
        after = self.state(digest_last=False)
        if after is None:
            return
        moved = {name: after[name] for name in ("digest", "master_repl_offset", "aof") if after[name] != before[name]}
        # A key or a hash field expired meanwhile (on access, or by the server's own cycle): its deletion
        # changes the data and is propagated, by design.
        if moved and after["expired"] != before["expired"]:
            return
        # DEBUG DIGEST leaves out a hash field from its expire time on, before the server deletes it and counts
        # it: a digest that changed while nothing was propagated may be that, and is not judged.
        if set(moved) == {"digest"} and (before["volatile_items"] or after["volatile_items"]):
            return
        self.judged["readonly"] += 1
        if moved:
            body = group[1:-1] or group
            self.find(
                "readonly-wrote",
                len(self.sent) - len(group),
                ", ".join(name for name, _ in body),
                # A transaction whole, as it was sent.
                [word for _, words in group for word in (";", *words)][1:],
                {"flags": sorted({flag for name, _ in body for flag in self.metadata[name]["flags"]})},
                {"before": {name: before[name] for name in moved}, "after": moved},
                "it changed " + ", ".join(sorted(moved)),
            )

    # -- replica ---------------------------------------------------------------------------------------------

    def replica(self, index: int, name: str, words: list[str]) -> None:
        flags = self.metadata[name]["flags"] & {"write", "readonly"}
        if not flags:
            return
        reply = self.send(1, name, words)
        if self.stopped:
            return
        error = str(reply.get("error", ""))
        refused = error.startswith("READONLY")
        # An error Valkey replies before it looks at the command's flags (its arity) is no answer to them.
        if not refused and checks._BEFORE_ACL.match(error.removeprefix("ERR ")):
            return
        self.judged["replica"] += 1
        if refused != ("write" in flags):
            self.find(
                "replica-wrote" if "write" in flags else "replica-refused",
                index,
                name,
                words,
                {"flags": sorted(self.metadata[name]["flags"])},
                {"replica": reply},
                f"a read-only replica replied {json.dumps(reply)[:200]}",
            )

    # -- routing ---------------------------------------------------------------------------------------------

    def routing(
        self, index: int, name: str, words: list[str], routed: Optional[list[str]], owner: Optional[Mapping[str, Any]]
    ) -> None:
        """Send the command to node 1, which owns no slot; ``owner``: what node 0, which owns them all, replied."""

        # A MULTI would leave node 1 queuing what follows instead of answering it.
        if name == "MULTI":
            return
        reply = self.send(1, name, words)
        if self.stopped or routed is None:
            return
        error = str(reply.get("error", ""))
        # A node that sees the cluster down (its peer answered no ping for cluster-node-timeout) routes nothing.
        if error.startswith("CLUSTERDOWN"):
            return
        # An error that the slots' owner replies too is no answer to the command's keys: Valkey refuses a wrong
        # number of arguments before it routes, and looks for no keys in arguments it cannot parse (an XREAD with
        # more streams than IDs), letting the command say so.
        if error and not error.startswith(("MOVED ", "CROSSSLOT ")) and (owner is None or reply == owner):
            return
        self.judged["routing"] += 1
        slots = sorted({key_slot(key.encode()) for key in routed})
        if not slots:
            kept = not error.startswith(("MOVED ", "CROSSSLOT "))
        else:
            kept = error.startswith(f"MOVED {slots[0]} " if len(slots) == 1 else "CROSSSLOT ")
        # A command upstream already knows is misrouted and is fixing (ROUTING_KNOWN_GAPS) is still sent and
        # counted, but does not fail the run; drop it from the set when its fix lands.
        if not kept and name not in ROUTING_KNOWN_GAPS:
            self.find(
                "misrouted",
                index,
                name,
                words,
                {"keys": routed, "slots": slots},
                {"node_without_slots": reply},
                f"it declares {routed or 'no key'}{f' (slot {slots[0]})' if len(slots) == 1 else ''}, yet a node "
                f"that owns no slot replied {json.dumps(reply)[:200]}",
            )


class CommandMetadataOracle:
    """Hold the command-table workload's commands to what the build declares about them.

    Where the workload sends commands (its ``rate`` is above 0), the oracle starts two private servers of the
    deployment's build and modules, a primary with a replica, or in cluster mode a node that owns every slot and
    one that owns none, and sends them the workload's first ``commands`` commands, drawn again from the run's
    seed: see this module's docstring for what it judges. A server that exits is a crash.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        workload = next(item for item in context.plan.components if item.id == config["workload"])
        if not workload.config["rate"]:
            return make_result(spec, ComponentStatus.PASS, "the workload sends no commands (rate 0): none to judge")
        handle = deployment(context)
        cluster = is_cluster(handle)
        instance_id = handle.instances[0].instance_id
        try:
            binary = resolve_binary(handle.binary(instance_id))
        except OSError as exc:
            return make_result(spec, ComponentStatus.ERROR, str(exc))
        side_config = {"nodes": 2, "resp": 2, "timeout_s": config["timeout_s"], "cluster": cluster}
        ports = iter(actions._free_ports(4 if cluster else 2, actions._port_block(context)))
        side = actions.new_side("metadata", binary, side_config, (), ports)
        side.options.update({**_NODE_OPTIONS, **instance_modules(context, instance_id)})
        run = _Run(side, cluster)
        try:
            failed = side.start(context, spec, actions._PRIVATE_SERVER_DEFAULTS, {})
            if not failed:
                failed = self._join(context, side, cluster, config["timeout_s"])
            if not failed:
                info, docs = checks._ask(side, 0, "COMMAND"), checks._ask(side, 0, "COMMAND", "DOCS")
                if "reply" not in info or "reply" not in docs:
                    failed = f"could not read the command table: {info.get('error') or docs.get('error')}"
            if not failed:
                run.metadata = read_metadata(info["reply"])
                # The workload's own draws, from their start: the commands it sends, in its order.
                generator = command_table.CommandGenerator(
                    command_table.read_command_table(info["reply"], docs["reply"])[0],
                    context.plan.rng(workload.id, "commands"),
                    workload.config["prefix"] or f"vf:{workload.id}",
                    workload.config["keys"],
                )
                while len(run.sent) < config["commands"] and run.stopped is None and not context.cancelled:
                    run.run(generator.next()[1])
        finally:
            if run.stopped is not None:
                # So finish waits for the node a command was lost on.
                side.replies = [{"index": 0, "unanswered": True}]
            side.finish(context, spec, [{"node": run.stopped[0] if run.stopped else 0}])
        if failed and side.exited is None:
            return make_result(spec, ComponentStatus.ERROR, failed)
        path = os.path.join(component_artifact_dir(context, spec), "commands.jsonl")
        with open(path, "w", encoding="utf-8") as sequence:
            sequence.writelines(json.dumps(words) + "\n" for words in run.sent)
        findings = list(run.findings.values())
        value = {"commands": len(run.sent), "judged": run.judged, "findings": findings, "sequence": path}
        recorded = (make_evidence(spec, EVIDENCE, "oracle.completed", value, artifacts=[*side.logs, path]),)
        last = f"command {len(run.sent) - 1} ({_text(run.stopped[1])})" if run.stopped else ""
        if side.exited is not None:
            cause = side.crash_cause()
            where = f"on {last}" if last else f"after {len(run.sent)} commands"
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"node {side.exited_node} exited {where} of the command-metadata check: {cause}",
                evidence=recorded,
                diagnostics={"crashes": {"candidate": cause}},
                crash=True,
            )
        if findings:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(sorted(f"{item['name']} {_BROKEN[item['reason']]}" for item in findings)),
                evidence=recorded,
                diagnostics={"reason": findings[0]["reason"], "findings": findings},
            )
        if run.stopped is not None:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"{last} on node {run.stopped[0]} got no reply: {run.stopped[2].get('error')}",
                evidence=recorded,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(run.sent)} commands kept to their metadata: "
            + ", ".join(f"{check} {judged}" for check, judged in run.judged.items()),
            evidence=recorded,
        )

    @staticmethod
    def _join(context: Any, side: Any, cluster: bool, timeout_s: float) -> str:
        """Form the side's cluster (node 0 owns every slot, node 1 none), or make node 1 a replica of node 0 and
        wait for its link; what went wrong, or an empty string."""

        if cluster:
            return side.form_cluster(context, timeout_s) or ""
        sent = checks._ask(side, 1, "REPLICAOF", "127.0.0.1", side.nodes[0].options["port"])
        if "reply" not in sent:
            return f"REPLICAOF failed on the replica: {sent.get('error')}"

        def linked() -> tuple[bool, Any]:
            fields = checks._fields(checks._ask(side, 1, "INFO", "replication"))
            return fields.get("master_link_status") == "up", fields.get("master_link_status")

        done, status = poll_until(linked, timeout_s=checks._SYNC_TIMEOUT_S, interval_s=0.05, context=context)
        return "" if done else f"the replica's link is {status} {checks._SYNC_TIMEOUT_S:g}s after REPLICAOF"


def _validate(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    workload = next((item for item in plan.components if item.id == config["workload"]), None)
    if workload is None or workload.use != "valkey.workload.command-table":
        raise ConfigError(f"workload {config['workload']!r} is not a valkey.workload.command-table of this plan")


_FIELDS = {
    # The command-table workload whose commands it sends again.
    "workload": ConfigField(str),
    # How many of them, from the first.
    "commands": ConfigField(int, default=1000, minimum=1, maximum=1_000_000),
    # Client timeout per command, and how long the private cluster may take to form.
    "timeout_s": ConfigField(float, default=10.0, minimum=0.1, maximum=3600),
}

_DECLARES = Declarations(
    evidence={
        EVIDENCE: declared(
            mapping_of(
                {
                    "commands": INTEGER,
                    "judged": mapping_of({check: INTEGER for check in CHECKS}),
                    "findings": LIST,
                    "sequence": TEXT,
                }
            )
        )
    },
    changes=("process.private-server",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.command-metadata", CommandMetadataOracle, _FIELDS, check=_validate, declares=_DECLARES
    ),
)
