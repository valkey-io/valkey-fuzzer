"""Commands from the whole command table, sent while the experiment's faults run.

The workload reads the command table of the build under test: ``COMMAND`` (each command's flags) and ``COMMAND
DOCS`` (its arguments), which the server builds from Valkey's ``src/commands/*.json`` specs, module commands
included. It then sends ``rate`` commands per second from one thread, each drawn uniformly from the table with
arguments drawn from its documented ones, from the run's seed, over RESP2 or RESP3. Error replies are expected and
only counted: the liveness and crash oracles judge what the commands do to the server. Every other reply is held to
the reply schema Valkey declares for its command (:mod:`.reply_schemas`), and ``valkey.oracle.reply-schema`` fails
the run for one that does not match.

Every key it names is ``<prefix>:{<tag>}:<type>:<n>``: the prefix keeps the keys apart from the other components'
(the ledger, the chaos test keys, digests restricted to a prefix), and one hash tag per command keeps a command's
keys in one slot, so a cluster runs multi-key commands instead of answering ``CROSSSLOT``. A pattern argument
matches only the prefix. In cluster mode each command goes to the primary serving its tag's slot, learned from
``MOVED`` replies.

Which commands run:

* An admin command (the ``admin`` flag) changes the server, its replication, or the cluster's topology, which the
  experiment's own components drive and judge (``SHUTDOWN``, ``REPLICAOF``, ``FAILOVER``, ``CLUSTER RESET``,
  ``CLUSTER SETSLOT``, ``CONFIG SET``, ``DEBUG``, ``BGSAVE``, ``CLIENT KILL``, ``CLIENT PAUSE``, ``MODULE LOAD``,
  ``ACL SETUSER``, ...). Only those in :data:`ADMIN_READS`, which read, run.
* Any other command runs unless :data:`EXCLUDED` names it, with the reason.
* ``MULTI`` runs as a transaction: ``MULTI``, one to three commands of the same hash tag, ``EXEC``.
* After a command of :data:`LEAVES` the workload closes the connection, with the reason.
"""

import random
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Optional

import valkey

from valkey_fuzzer._data import canonical_json
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, Evidence, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config

from .actions import open_raw_connection
from .instances import deployment, is_cluster
from .polling import StoppableThread
from .registration import component_registration
from .reply_schemas import mismatch, reply_schemas
from .schemas import INTEGER, LIST, TEXT, declared, mapping_of

EVIDENCE = "valkey.workload.command-table"
OUTCOMES = ("replied", "error", "unanswered", "protocol_error")

# Admin commands that only read.
ADMIN_READS = frozenset(
    {
        "ACL DRYRUN",
        "ACL GETUSER",
        "ACL LIST",
        "ACL LOG",
        "ACL USERS",
        "CLIENT LIST",
        "CLUSTER COUNT-FAILURE-REPORTS",
        "CLUSTER REPLICAS",
        "CLUSTER SLAVES",
        "COMMANDLOG GET",
        "COMMANDLOG LEN",
        "CONFIG GET",
        "LATENCY DOCTOR",
        "LATENCY GRAPH",
        "LATENCY HISTOGRAM",
        "LATENCY HISTORY",
        "LATENCY LATEST",
        "MODULE LIST",
        "SLOWLOG GET",
        "SLOWLOG LEN",
    }
)

# Commands that are not admin but would break the run or the other components.
EXCLUDED = {
    "FLUSHALL": "deletes every component's keys",
    "FLUSHDB": "deletes every component's keys",
    "SWAPDB": "moves every component's keys to another database",
    "KEYS": "walks every key, the other workloads' too, blocking the server as long",
    "MIGRATE": "blocks the server while it connects to the host it names, and moves keys out of the deployment",
    "SUBSCRIBE": "puts the connection in subscriber mode, where replies no longer pair with commands",
    "PSUBSCRIBE": "puts the connection in subscriber mode, where replies no longer pair with commands",
    "SSUBSCRIBE": "puts the connection in subscriber mode, where replies no longer pair with commands",
    "HELLO": "switches the connection's protocol under the client reading it",
    "CLIENT REPLY": "OFF and SKIP leave commands without the replies the workload waits for",
    "SCRIPT DEBUG": "SYNC blocks the server for the whole debugging session",
}

# Commands that run, but leave their connection where replies no longer pair with commands in the protocol it was
# opened with: once one is answered the workload closes the connection, and sends nothing more of its transaction.
LEAVES = {
    "RESET": "puts the connection back in RESP2",
    "CLIENT TRACKING": "makes the server push invalidations between replies",
    "UNSUBSCRIBE": "is answered once per channel",
    "PUNSUBSCRIBE": "is answered once per pattern",
    "SUNSUBSCRIBE": "is answered once per channel",
}
# Replies Valkey sends that the reply schema of their command leaves out, each by the schema of those replies.
# They are findings already reported; they are taken as matching, so that they do not fail every run, until
# Valkey's schema describes them and reply_schemas.json is written again. The four below are fixed by "Allow
# null command replies in schemas" (branch fix/nullable-command-reply-schemas), not in unstable c77a4e8da.
_NULL = {"type": "null"}
KNOWN_GAPS = {
    # geohashCommand (geo.c) replies null for a member the key does not hold; the schema's items are strings.
    "GEOHASH": {"type": "array", "items": {"anyOf": [{"type": "string"}, _NULL]}},
    # objectCommand (object.c) replies null for a key that does not exist; the three schemas are integers.
    "OBJECT FREQ": _NULL,
    "OBJECT IDLETIME": _NULL,
    "OBJECT REFCOUNT": _NULL,
}
# How much of a mismatching reply the evidence records, as JSON text.
_REPLY_TEXT = 4096

# Hash tags, so slots, the keys spread over.
_HASH_TAGS = 32
# Command groups whose keys hold one type; a command of another group (generic, scripting, ...) draws one of them.
_KEY_TYPES = ("string", "list", "set", "hash", "sorted_set", "stream", "hyperloglog", "geo", "bitmap")
# String arguments drawn from a small domain, so commands meet the members, fields, and groups others wrote.
_DOMAINS = {
    "field": 20,
    "member": 50,
    "member1": 50,
    "member2": 50,
    "element": 30,
    "pivot": 30,
    "group": 10,
    "consumer": 15,
}


def _pairs(flat: Sequence[Any]) -> dict[str, Any]:
    """A RESP2 map (``[key, value, key, value, ...]``) as a dict."""

    return dict(zip(flat[::2], flat[1::2]))


def _arguments(doc: Mapping[str, Any]) -> list[dict[str, Any]]:
    """A command's documented arguments, nested ones too, as dicts."""

    arguments = []
    for flat in doc.get("arguments") or ():
        argument = _pairs(flat)
        argument["arguments"] = _arguments(argument)
        arguments.append(argument)
    return arguments


def read_command_table(info: Sequence[Any], docs: Sequence[Any]) -> tuple[list[tuple[str, str, list]], list[str]]:
    """The commands to send, as ``(name, group, arguments)`` sorted by name, and the names of those skipped, from
    the replies to ``COMMAND`` and ``COMMAND DOCS`` (RESP2). A name is upper case, a subcommand's after its
    container's (``CONFIG GET``); a container itself is never sent."""

    documented = {}
    for name, flat in _pairs(docs).items():
        doc = _pairs(flat)
        documented[name] = doc
        for subname, subflat in _pairs(doc.get("subcommands") or ()).items():
            documented[subname] = _pairs(subflat)
    table, skipped = [], []
    entries = list(info)
    while entries:
        entry = entries.pop()
        if len(entry) > 9 and entry[9]:
            entries.extend(entry[9])
            continue
        name = entry[0].upper().replace("|", " ")
        if ("admin" in entry[2] and name not in ADMIN_READS) or name in EXCLUDED:
            skipped.append(name)
            continue
        doc = documented.get(entry[0], {})
        table.append((name, doc.get("group", "generic"), _arguments(doc)))
    return sorted(table, key=lambda item: item[0]), sorted(skipped)


def answered(exchanges: Sequence[tuple[str, Any, Mapping[str, Any]]]) -> Iterator[tuple[str, Any, Any]]:
    """What each reply of one connection's ``exchanges`` (``(name, command, record)``, in order) answers, as
    ``(name, command, reply)``, errors left out. Inside a transaction a command is answered ``QUEUED``, which is
    not its reply: that is its item of the EXEC's reply."""

    queued: Optional[list[tuple[str, Any]]] = None
    for name, command, record in exchanges:
        ends = name in ("EXEC", "DISCARD")
        if "reply" not in record:
            # An EXEC that fails (EXECABORT) ends its transaction too.
            queued = None if ends else queued
            continue
        reply = record["reply"]
        if queued is not None and not ends:
            if reply == "QUEUED":
                queued.append((name, command))
            continue
        yield name, command, reply
        if name == "EXEC" and isinstance(reply, list) and len(reply) == len(queued or ()):
            for (queued_name, queued_command), item in zip(queued or (), reply):
                if not (isinstance(item, dict) and set(item) == {"$error"}):
                    yield queued_name, queued_command, item
        queued = [] if name == "MULTI" else None


class CommandGenerator:
    """Draws commands from ``table`` with ``rng``; keys are ``<prefix>:{<tag>}:<type>:<n>`` with ``n`` below
    ``keys``."""

    def __init__(self, table: Sequence[tuple[str, str, list]], rng: random.Random, prefix: str, keys: int):
        self.table, self.rng, self.prefix, self.keys = table, rng, prefix, keys
        self._key_type = ""
        self._numkeys: Optional[int] = None
        self._tag = 0

    def next(self) -> tuple[int, list[tuple[str, list[str]]]]:
        """A hash tag and the commands to send on one connection, each with the table name it came from."""

        self._tag = self.rng.randrange(_HASH_TAGS)
        entry = self.rng.choice(self.table)
        if entry[0] != "MULTI":
            return self._tag, [self._command(*entry)]
        body = [self._command(*self.rng.choice(self.table)) for _ in range(self.rng.randint(1, 3))]
        return self._tag, [("MULTI", ["MULTI"]), *body, ("EXEC", ["EXEC"])]

    def _command(self, name: str, group: str, arguments: list) -> tuple[str, list[str]]:
        self._key_type = group if group in _KEY_TYPES else self.rng.choice(_KEY_TYPES)
        self._numkeys = None
        words = name.split(" ")
        self._fill(arguments, words)
        return name, words

    def _fill(self, arguments: list, out: list[str]) -> None:
        for argument in arguments:
            if "optional" in argument.get("flags", ()) and self.rng.random() < 0.5:
                continue
            self._argument(argument, out)

    def _argument(self, argument: Mapping[str, Any], out: list[str]) -> None:
        flags = argument.get("flags", ())
        count = 1
        if "multiple" in flags:
            # A numkeys argument fixes how many keys (and WEIGHTS) follow it.
            follows_numkeys = argument["type"] == "key" or argument.get("token") == "WEIGHTS"
            count = self._numkeys if self._numkeys and follows_numkeys else self.rng.randint(1, 3)
        for index in range(count):
            if "token" in argument and (index == 0 or "multiple_token" in flags):
                out.append(argument["token"])
            kind = argument["type"]
            if kind == "oneof":
                # The chosen alternative is never skipped, even when it is optional on its own.
                self._argument(self.rng.choice(argument["arguments"]), out)
            elif kind == "block":
                self._fill(argument["arguments"], out)
            elif kind != "pure-token":
                out.append(self._value(argument["name"], kind))

    def _value(self, name: str, kind: str) -> str:
        rng = self.rng
        if kind == "key":
            return f"{self.prefix}:{{{self._tag}}}:{self._key_type}:{rng.randrange(self.keys)}"
        if kind == "pattern":
            return f"{self.prefix}:*"
        if kind == "unix-time":
            return str(int(time.time()) + rng.randrange(60))
        if name in ("timeout", "milliseconds"):
            # A blocking command's wait (seconds as a double, else milliseconds): short, as 0 blocks forever.
            return f"{rng.uniform(0.01, 0.1):.3f}" if kind == "double" else str(rng.randint(10, 100))
        if name == "numkeys":
            self._numkeys = rng.randint(1, 3)
            return str(self._numkeys)
        if kind == "integer":
            return str(rng.randint(1, 50))
        if kind == "double":
            return f"{rng.uniform(1, 50):.3f}"
        if name == "value":
            # As valkey-benchmark --fuzz draws values: up to 1 KB, one in twenty up to 10 KB.
            return "x" * (rng.randint(1, 1024) if rng.random() < 0.95 else rng.randint(1024, 10240))
        if name in ("id", "last-id", "lastid", "max-deleted-id"):
            return "*" if rng.random() < 0.5 else f"{rng.randrange(1000)}-{rng.randrange(1000)}"
        if name in ("min", "max"):
            # A numeric bound is an integer or a double: a string one is a lexicographic range.
            return rng.choice(("-", "+", "[", "(")) + ("" if rng.random() < 0.2 else f"a{rng.randrange(100)}")
        if name in ("start", "stop", "end"):
            return str(rng.randint(-2, 50))
        if name == "encoding":
            return rng.choice("iu") + str(rng.choice((1, 8, 16, 32, 64)))
        return f"{name}:{rng.randrange(_DOMAINS.get(name, 1000))}"


class CommandTableWorkload:
    """Send commands drawn from the whole command table at ``rate`` per second until teardown."""

    def __init__(self) -> None:
        self._thread: Optional[StoppableThread] = None
        self._config: dict[str, Any] = {}
        self._handle: Any = None
        self._cluster = False
        self._addresses: dict[tuple[str, int], str] = {}
        # (instance, protocol) -> its connection.
        self._connections: dict[tuple[str, int], Any] = {}
        # Hash tag (0 for every tag outside cluster mode) -> the instance serving it.
        self._routes: dict[int, str] = {}
        self._table: list[tuple[str, str, list]] = []
        self._skipped: list[str] = []
        self._outcomes = dict.fromkeys(OUTCOMES, 0)
        self._sent: set[str] = set()
        self._protocol_errors: list[dict[str, Any]] = []
        self._validated = 0
        # (command name, protocol) -> the first of its replies that did not match its schema, and how many did not.
        self._mismatches: dict[tuple[str, int], dict[str, Any]] = {}

    def start(self, context: Any, spec: ComponentSpec):
        config = self._config = spec.config
        if config["rate"] <= 0:
            return make_result(spec, ComponentStatus.PASS, "rate 0: no commands sent")
        self._handle = deployment(context)
        self._cluster = is_cluster(self._handle)
        self._addresses = {(item.host, int(item.port)): item.instance_id for item in self._handle.instances}
        instance_id = self._instance(0)
        try:
            if instance_id is None:
                raise RuntimeError("no reachable primary")
            info = self._send(instance_id, 2, ["COMMAND"])
            docs = self._send(instance_id, 2, ["COMMAND", "DOCS"])
            for record in (info, docs):
                if "reply" not in record:
                    raise RuntimeError(record.get("error") or record.get("protocol_error"))
            self._table, self._skipped = read_command_table(info["reply"], docs["reply"])
        except Exception as exc:
            self._close()
            return make_result(spec, ComponentStatus.ERROR, f"could not read the command table: {exc}")
        generator = CommandGenerator(self._table, context.rng(spec.id, "commands"), self._prefix(spec), config["keys"])
        protocols = context.rng(spec.id, "protocol")
        interval = 1.0 / config["rate"]

        def loop(stop_event) -> None:
            next_at = time.monotonic()
            while not stop_event.is_set():
                tag, commands = generator.next()
                if not self._run(tag if self._cluster else 0, protocols.choice((2, 3)), commands):
                    stop_event.wait(0.05)
                next_at += interval
                delay = next_at - time.monotonic()
                if delay > 0:
                    stop_event.wait(delay)
                else:
                    next_at = time.monotonic()

        self._thread = StoppableThread(f"workload-{spec.id}", loop)
        self._thread.start()
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"sending {len(self._table)} commands at {config['rate']}/s ({len(self._skipped)} skipped)",
            diagnostics={"prefix": self._prefix(spec)},
        )

    def _prefix(self, spec: ComponentSpec) -> str:
        return self._config["prefix"] or f"vf:{spec.id}"

    def _instance(self, route: int) -> Optional[str]:
        if route not in self._routes:
            primaries = self._handle.primaries()
            if not primaries:
                return None
            # In cluster mode any primary: its MOVED reply names the one serving the tag.
            self._routes[route] = primaries[0]
        return self._routes[route]

    def _send(self, instance_id: str, resp: int, command: Sequence[str]) -> dict[str, Any]:
        """``command`` on the workload's RESP ``resp`` connection to ``instance_id``: :meth:`RawConnection.send`'s
        record."""

        connection = self._connections.get((instance_id, resp))
        if connection is None:
            instance = next(item for item in self._handle.instances if item.instance_id == instance_id)
            try:
                connection = open_raw_connection(
                    resp, self._config["op_timeout_s"], host=instance.host, port=instance.port
                )
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                return {"error": f"cannot connect: {type(exc).__name__}: {exc}", "unanswered": True}
            self._connections[instance_id, resp] = connection
        return connection.send(command)

    def _run(self, route: int, resp: int, commands: Sequence[tuple[str, list[str]]]) -> bool:
        """Send ``commands`` in order over RESP ``resp`` to the instance serving ``route``, and hold their replies
        to their reply schemas. False when one got no reply, or a ``READONLY`` error (its instance is not the
        primary any more): the rest are not sent, the connection is dropped, and the route is looked up again."""

        exchanges: list[tuple[str, list[str], dict[str, Any]]] = []
        try:
            return self._exchange(route, resp, commands, exchanges)
        finally:
            self._judge(resp, exchanges)

    def _exchange(self, route: int, resp: int, commands: Sequence[tuple[str, list[str]]], exchanges: list) -> bool:
        instance_id = self._instance(route)
        for name, command in commands:
            self._sent.add(name)
            if instance_id is None:
                record = {"error": "no reachable primary", "unanswered": True}
            else:
                record = self._send(instance_id, resp, command)
            exchanges.append((name, command, record))
            error = str(record.get("error", ""))
            if "protocol_error" in record:
                outcome = "protocol_error"
                if len(self._protocol_errors) < 10:
                    self._protocol_errors.append({"command": command[:8], "error": record["protocol_error"]})
            elif record.get("unanswered"):
                outcome = "unanswered"
            else:
                outcome = "error" if "error" in record else "replied"
            self._outcomes[outcome] += 1
            if error.startswith("MOVED "):
                host, _, port = error.split()[2].rpartition(":")
                moved_to = self._addresses.get((host, int(port)))
                if moved_to is None:
                    self._routes.pop(route, None)
                else:
                    self._routes[route] = moved_to
            elif outcome in ("unanswered", "protocol_error") or error.startswith("READONLY"):
                # A connection whose state is unknown, or an instance that is not the primary any more.
                self._routes.pop(route, None)
                if instance_id is not None:
                    self._forget(instance_id, resp)
                return False
            if name in LEAVES and outcome == "replied":
                self._forget(instance_id, resp)
                break
        return True

    def _judge(self, resp: int, exchanges: Sequence[tuple[str, list[str], dict[str, Any]]]) -> None:
        """Hold every reply of ``exchanges`` to the reply schema of its command, when Valkey declares one."""

        for name, command, reply in answered(exchanges):
            schema = reply_schemas().get(name)
            if schema is None:
                continue
            self._validated += 1
            problem = mismatch(schema, reply, resp)
            if problem is None or (name in KNOWN_GAPS and mismatch(KNOWN_GAPS[name], reply, resp) is None):
                continue
            if (name, resp) not in self._mismatches:
                self._mismatches[name, resp] = {
                    "name": name,
                    "resp": resp,
                    "count": 0,
                    "command": command,
                    "reply": canonical_json(reply)[:_REPLY_TEXT],
                    "problem": problem,
                    "schema": schema,
                }
            self._mismatches[name, resp]["count"] += 1

    def _forget(self, instance_id: Optional[str], resp: int) -> None:
        connection = self._connections.pop((instance_id, resp), None)
        if connection is not None:
            connection.close()

    def _close(self) -> None:
        for instance_id, resp in list(self._connections):
            self._forget(instance_id, resp)

    def stop(self, context: Any, spec: ComponentSpec):
        problem = None
        if self._thread is not None:
            problem = self._thread.stop(timeout=max(30.0, self._config["op_timeout_s"] * 3))
            if self._thread.is_alive():
                # A worker still running owns its connections and counts: nothing of them is read.
                return make_result(spec, ComponentStatus.ERROR, f"workload {problem}")
        self._close()
        summary = {
            "prefix": self._prefix(spec),
            "commands": len(self._table),
            "skipped": self._skipped,
            "sent": sum(self._outcomes.values()),
            "distinct_sent": len(self._sent),
            "outcomes": dict(self._outcomes),
            "protocol_errors": self._protocol_errors,
            "validated": self._validated,
            "schema_mismatches": [self._mismatches[key] for key in sorted(self._mismatches)],
        }
        evidence = (make_evidence(spec, EVIDENCE, "workload.quiesced", summary),)
        if problem is not None:
            return make_result(spec, ComponentStatus.ERROR, f"workload {problem}", evidence=evidence)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{summary['sent']} commands ({summary['distinct_sent']} distinct): "
            + ", ".join(f"{count} {outcome}" for outcome, count in self._outcomes.items())
            + f"; {self._validated} replies held to their reply schemas",
            evidence=evidence,
        )


_FIELDS = {
    # Commands per second; 0 sends none (an experiment's input turns the workload on).
    "rate": ConfigField(float, default=100.0, minimum=0, maximum=100_000),
    # Keys per hash tag and type.
    "keys": ConfigField(int, default=5, minimum=1, maximum=10_000),
    "prefix": ConfigField(str, allow_none=True, default=None),
    "op_timeout_s": ConfigField(float, default=1.0, minimum=0.05, maximum=60),
}

_DECLARES = Declarations(
    evidence={
        EVIDENCE: declared(
            mapping_of(
                {
                    "prefix": TEXT,
                    "commands": INTEGER,
                    "skipped": LIST,
                    "sent": INTEGER,
                    "distinct_sent": INTEGER,
                    "outcomes": mapping_of({outcome: INTEGER for outcome in OUTCOMES}),
                    "protocol_errors": LIST,
                    "validated": INTEGER,
                    "schema_mismatches": LIST,
                }
            )
        )
    },
)


def _validate_oracle(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    uses = {component.id: component.use for component in plan.components}
    if uses.get(config["workload"]) != "valkey.workload.command-table":
        raise ConfigError(f"workload {config['workload']!r} is not a valkey.workload.command-table of this plan")


class ReplySchemaOracle:
    """Every reply the ``workload`` held to its command's reply schema matched it.

    One that does not is a finding about Valkey either way: the reply is wrong, or the schema Valkey declares
    for it is. The summary names each command and protocol, which the failure signature groups runs by; the
    diagnostics hold, for each, the first such command, its reply, what in it does not match, and the schema.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        workload = spec.config["workload"]
        items = evidence_of(evidence, EVIDENCE, workload)
        if not items:
            return make_result(spec, ComponentStatus.ERROR, f"no {EVIDENCE} evidence from workload {workload!r}")
        summary = plain_config(items[-1].value)
        mismatches = summary["schema_mismatches"]
        if mismatches:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(
                    f"{item['name']} (RESP{item['resp']}) reply does not match its reply schema" for item in mismatches
                ),
                diagnostics={"mismatches": mismatches},
            )
        return make_result(
            spec, ComponentStatus.PASS, f"{summary['validated']} replies match their commands' reply schemas"
        )


REGISTRATIONS = (
    component_registration("valkey.workload.command-table", CommandTableWorkload, _FIELDS, declares=_DECLARES),
    component_registration(
        "valkey.oracle.reply-schema",
        ReplySchemaOracle,
        {"workload": ConfigField(str)},
        check=_validate_oracle,
        declares=Declarations(),
    ),
)
