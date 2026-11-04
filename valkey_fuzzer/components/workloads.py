"""Continuous key/value workload with an acknowledged-operation ledger.

Publishes ``<id>.seeded`` once the preload is written and, with
``max_ops``, ``<id>.drained`` once that many operations completed.

One writer thread issues operations sequentially, so every key has a total
order of client-visible operations.  Each ledger entry records what the client
observed: ``ok`` (acknowledged), ``error`` (an error reply: definitely not
applied), or ``ambiguous`` (timeout/disconnect; may or may not be applied).
Oracles reconstruct the set of acceptable final values from this ledger under
an explicit durability policy instead of guessing.

A ``rename`` in the ``mix`` is ``RENAME src dst`` between two mutable keys of
the same name length whose values the workload knows for certain (their last
operation was an acknowledged SET or the preload), so ``dst`` exists. The
ledger records it as two entries with one outcome: a ``del`` of ``src`` and a
``set`` of ``dst`` with the value ``src`` held, which is exactly what the
oracles' model needs. A rename whose source or destination is not known for
certain (its last write was ambiguous, or it was deleted) becomes a SET.

``list_keys`` lists of ``list_length`` compressible elements are preloaded
beside the keys, never mutated, and read by the ``lrange`` and ``lindex``
operations of the mix (``list_read_batch`` reads per operation, pipelined).
The reads are not ledger entries, only counted. Each list's length and element
digest go to ``lists.jsonl`` for the RDB snapshot oracle.
"""

import hashlib
import json
import os
import random
import string
import threading
import time
from collections.abc import Iterable, Iterator
from typing import Any, Optional

import valkey

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, ConfigSchema

from .files import write_jsonl
from .instances import close_quietly, deployment, is_cluster
from .polling import StoppableThread, poll_until
from .registration import component_registration
from .schemas import FLAG, INTEGER, NUMBER, TEXT, declared, mapping_of, nullable, optional

LEDGER_EVIDENCE = "valkey.workload.ledger"
OPERATIONS = ("set", "del", "get", "rename", "lrange", "lindex")
# Elements an lrange operation reads from its random start index.
_LRANGE_SPAN = 30

_WORKLOAD_FIELDS = {
    "stable_keys": ConfigField(int, default=0, minimum=0, maximum=5_000_000),
    "mutable_keys": ConfigField(int, default=100, minimum=0, maximum=1_000_000),
    "preload_mutable": ConfigField(bool, default=True),
    "rate": ConfigField(float, default=200.0, minimum=0, maximum=100_000),
    "value_size": ConfigField(int, default=64, minimum=1, maximum=1_048_576),
    "compressible": ConfigField(bool, default=False),
    # Relative weights of the operations; one left out never runs.
    "mix": mapping_of(
        {name: optional(float, minimum=0) for name in OPERATIONS},
        default=lambda: {"set": 0.7, "del": 0.1, "get": 0.2},
    ),
    # Lists preloaded beside the keys for the lrange and lindex operations.
    "list_keys": ConfigField(int, default=0, minimum=0, maximum=1000),
    "list_length": ConfigField(int, default=100_000, minimum=1, maximum=10_000_000),
    "list_element_size": ConfigField(int, default=120, minimum=1, maximum=65536),
    "list_read_batch": ConfigField(int, default=1, minimum=1, maximum=1000),
    "prefix": ConfigField(str, allow_none=True, default=None),
    "op_timeout_s": ConfigField(float, default=2.0, minimum=0.05, maximum=60),
    "preload_batch": ConfigField(int, default=1000, minimum=1, maximum=100_000),
    "max_ops": ConfigField(int, allow_none=True, default=None, minimum=1),
    "drain_on_stop": ConfigField(bool, default=False),
    "drain_timeout_s": ConfigField(float, default=120.0, minimum=0.1, maximum=7200),
    # After every acknowledged SET or DEL, WAIT for this many replicas (standalone
    # only; 0 never waits) and record the reply in the ledger entry.
    "wait_replicas": ConfigField(int, default=0, minimum=0, maximum=100),
    "wait_timeout_ms": ConfigField(int, default=100, minimum=1, maximum=60_000),
}


def _validate_workload(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    mix = config["mix"]
    if sum(mix.values()) <= 0:
        raise ConfigError(f"mix needs a positive weight for one of {', '.join(OPERATIONS)}")
    if mix.get("rename", 0) > 0:
        # RENAME needs both keys in one slot, and mutable keys are not hash-tagged.
        if plan.deployment_mode != "standalone":
            raise ConfigError("mix.rename needs a standalone deployment: a cluster rejects it with CROSSSLOT")
        if config["mutable_keys"] < 2:
            raise ConfigError("mix.rename needs at least 2 mutable_keys")
    if (mix.get("lrange", 0) > 0 or mix.get("lindex", 0) > 0) and config["list_keys"] == 0:
        raise ConfigError("mix.lrange and mix.lindex read lists: set list_keys above 0")
    if config["drain_on_stop"]:
        if config["max_ops"] is None:
            raise ConfigError("drain_on_stop requires max_ops")
        # Without a rate and mutable keys no operation thread runs, so max_ops
        # is never reached and <id>.drained is never published.
        if config["rate"] <= 0 or config["mutable_keys"] <= 0:
            raise ConfigError(
                "drain_on_stop requires rate > 0 and mutable_keys > 0: otherwise no operations run "
                f"and the drain to max_ops ({spec.id}.drained) can never complete"
            )
    if config["stable_keys"] == 0 and config["mutable_keys"] == 0:
        raise ConfigError("workload needs stable_keys or mutable_keys")
    if config["wait_replicas"]:
        if plan.deployment_mode != "standalone":
            raise ConfigError("wait_replicas needs a standalone deployment")
        replicas = plan.deployment["replicas_per_primary"]
        if config["wait_replicas"] > replicas:
            raise ConfigError(f"wait_replicas {config['wait_replicas']} exceeds replicas_per_primary {replicas}")


def key_prefix(spec: ComponentSpec, config: dict) -> str:
    return config["prefix"] or f"vf:{spec.id}"


def stable_key(prefix: str, index: int) -> str:
    return f"{prefix}:stable:{index}"


def is_stable_key(key: str) -> bool:
    """Whether ``key`` is one of the workload's stable keys (:func:`stable_key`), which it never mutates."""

    return key.rsplit(":", 2)[-2:-1] == ["stable"]


def make_value(rng: random.Random, size: int, compressible: bool, tag: str) -> str:
    if compressible:
        body = (tag + ":") * (size // (len(tag) + 1) + 1)
        return body[:size]
    alphabet = string.ascii_letters + string.digits
    return (tag + ":" + "".join(rng.choice(alphabet) for _ in range(size)))[: max(size, len(tag) + 1)]


def list_key(prefix: str, index: int) -> str:
    return f"{prefix}:list:{index}"


def list_elements(index: int, length: int, size: int) -> Iterator[str]:
    """The elements of list ``index``: compressible, so quicklist nodes compress under ``list-compress-depth``."""

    for position in range(length):
        tag = f"l{index}:{position}:"
        yield (tag * (size // len(tag) + 1))[:size]


def list_digest(elements: Iterable[str]) -> str:
    """Digest of a list's elements in order, as the RDB snapshot oracle recomputes it after a reload."""

    digest = hashlib.sha256()
    for element in elements:
        digest.update(element.encode("utf-8", errors="surrogatepass"))
        digest.update(b"\0")
    return digest.hexdigest()


class _Target:
    """Routes commands to the current writable endpoint."""

    def __init__(self, handle: Any, timeout: float):
        self.handle = handle
        self.timeout = timeout
        self.cluster = is_cluster(handle)
        self.client: Any = None
        self.instance_id: Optional[str] = None
        instances = getattr(handle, "instances", ()) if self.cluster else ()
        self._by_address = {(instance.host, int(instance.port)): instance.instance_id for instance in instances}
        ports = [int(instance.port) for instance in instances]
        self._by_port = {
            int(instance.port): instance.instance_id for instance in instances if ports.count(int(instance.port)) == 1
        }

    def connect(self) -> Any:
        if self.client is not None:
            return self.client
        if self.cluster:
            client = self.handle.cluster_client(timeout=self.timeout)
            # The cluster client re-sends a command after a timeout, a
            # connection error, or CLUSTERDOWN; an error reply to the last
            # attempt would then hide an earlier attempt that may have been
            # applied. One attempt per command keeps every outcome definite
            # (MOVED, ASK, and TRYAGAIN redirections are still followed: the
            # node that sends them did not execute the command).
            client.cluster_error_retry_attempts = 0
            self.client = client
            self.instance_id = "cluster"
        else:
            primaries = self.handle.primaries()
            if not primaries:
                raise valkey.exceptions.ConnectionError("no reachable primary")
            self.instance_id = primaries[0]
            self.client = self.handle.new_client(self.instance_id, timeout=self.timeout)
        return self.client

    def served_by(self, key: str) -> Optional[str]:
        """The instance that answered the last command on ``key``.

        In cluster mode, the primary the client's slot table routes ``key`` to:
        after a MOVED redirection the client updated that table before
        re-sending, so it names the node that answered. ``None`` when unknown.
        """

        if not self.cluster:
            return self.instance_id
        try:
            node = self.client.get_node_from_key(key)
            port = int(node.port)
        except Exception:
            return None
        return self._by_address.get((node.host, port)) or self._by_port.get(port)

    def reset(self) -> None:
        close_quietly(self.client)
        self.client = None


# Error replies that valkey-py raises as ConnectionError subclasses: the server
# answered the command with an error (NOAUTH/WRONGPASS, LOADING) without
# executing it.
_REJECTION_REPLIES = (valkey.exceptions.AuthenticationError, valkey.exceptions.BusyLoadingError)


# What a command can raise about the server or the connection; anything else is a harness bug, which crashes the
# workload thread (ERROR) instead of being recorded as an ambiguous write that loosens the oracle.
_CLIENT_ERRORS = (valkey.exceptions.ValkeyError, valkey.exceptions.ValkeyClusterException, OSError)


def _classify(exc: BaseException) -> str:
    """``error`` when the server definitely did not apply the command, else ``ambiguous``.

    Sound only because the workload's clients make one attempt per command:
    the deployment's standalone client never re-sends one, and the cluster
    client's retries are disabled in :meth:`_Target.connect`.

    * ``ResponseError``: the server rejected the command with an error reply
      before executing it: OOM, READONLY, NOPERM, WRONGTYPE, MISCONF,
      NOREPLICAS, CLUSTERDOWN, MASTERDOWN, and MOVED, ASK, or TRYAGAIN that
      escaped the client's redirections. A single-key SET, DEL, or GET is
      never partially applied.
    * NOAUTH/WRONGPASS and LOADING replies (raised as ``ConnectionError``
      subclasses): rejected before execution.
    * Anything else (timeouts, connection errors, the cluster client's own
      ``ClusterError``/``ValkeyClusterException`` after a failed refresh) may
      follow a command the server received and applied: ambiguous.
    """

    if isinstance(exc, (valkey.exceptions.ResponseError, *_REJECTION_REPLIES)):
        return "error"
    return "ambiguous"


class KeyValueWorkload:
    """Preload deterministic data, then mutate it continuously."""

    def __init__(self) -> None:
        self._thread: Optional[StoppableThread] = None
        self._ledger: list[dict[str, Any]] = []
        self._preload: dict[str, str] = {}
        # Per preloaded key: when its batch was acknowledged and which instance did.
        self._preload_acks: dict[str, tuple[float, Optional[str]]] = {}
        self._counters = {"ok": 0, "error": 0, "ambiguous": 0}
        # Keys whose current value is certain: their last write was an acknowledged SET (or the preload).
        self._known: dict[str, str] = {}
        # Per preloaded list: key, length, and element digest.
        self._lists: list[dict[str, Any]] = []
        self._list_reads = {"ok": 0, "failed": 0}
        self._started_at = 0.0
        self._target: Optional[_Target] = None
        self._config: dict[str, Any] = {}
        self._drained = threading.Event()

    def start(self, context: Any, spec: ComponentSpec):
        config = spec.config
        self._config = config
        handle = deployment(context)
        rng = context.rng(spec.id, "values")
        op_rng = context.rng(spec.id, "operations")
        prefix = key_prefix(spec, config)
        self._target = _Target(handle, config["op_timeout_s"])
        self._started_at = time.time()

        stable = {
            stable_key(prefix, index): make_value(rng, config["value_size"], config["compressible"], f"s{index}")
            for index in range(config["stable_keys"])
        }
        mutable_keys = [f"{prefix}:m:{index}" for index in range(config["mutable_keys"])]
        mutable_initial = {}
        if config["preload_mutable"]:
            mutable_initial = {
                key: make_value(rng, config["value_size"], config["compressible"], f"p{index}")
                for index, key in enumerate(mutable_keys)
            }
        preload = {**stable, **mutable_initial}
        list_keys = [list_key(prefix, index) for index in range(config["list_keys"])]
        try:
            self._load(context, preload, config["preload_batch"])
            self._load_lists(list_keys, config["list_length"], config["list_element_size"], config["preload_batch"])
        except Exception as exc:
            self._target.reset()
            return make_result(spec, ComponentStatus.ERROR, f"preload failed: {exc}")
        self._preload = preload
        self._known = dict(preload)
        context.event_bus.publish(
            f"{spec.id}.seeded",
            source=spec.id,
            payload={"keys": len(preload), "stable_keys": len(stable), "mutable_keys": len(mutable_keys)},
        )

        if config["rate"] > 0 and mutable_keys:
            ops = [name for name in OPERATIONS if config["mix"].get(name, 0) > 0]
            weights = [config["mix"][name] for name in ops]
            interval = 1.0 / config["rate"]
            sequence = {"value": 0}

            def loop(stop_event) -> None:
                next_at = time.monotonic()
                while not stop_event.is_set():
                    if config["max_ops"] is not None and sequence["value"] >= config["max_ops"]:
                        if not self._drained.is_set():
                            self._drained.set()
                            context.event_bus.publish(
                                f"{spec.id}.drained",
                                source=spec.id,
                                payload={"operations": sequence["value"]},
                            )
                        stop_event.wait(0.05)
                        continue
                    op = op_rng.choices(ops, weights)[0]
                    key = op_rng.choice(mutable_keys)
                    sequence["value"] += 1
                    op_id = sequence["value"]
                    if op in ("lrange", "lindex"):
                        outcome = self._read_list(op, op_rng.choice(list_keys), op_rng, config)
                    else:
                        value = dst = None
                        if op == "rename":
                            value = self._known.get(key)
                            peers = [peer for peer in self._known if len(peer) == len(key) and peer != key]
                            if value is None or not peers:
                                op = "set"  # The source or a same-length destination is not known for certain.
                            else:
                                dst = op_rng.choice(peers)
                                sequence["value"] += 1  # A rename is two ledger entries.
                        if op == "set":
                            value = make_value(rng, config["value_size"], config["compressible"], f"w{op_id}")
                        outcome = self._execute(op_id, op, key, value, dst)
                    if outcome != "ok":
                        stop_event.wait(0.02)
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
            f"seeded {len(preload)} keys; mutating {len(mutable_keys)} keys at {config['rate']}/s",
            diagnostics={"prefix": prefix},
        )

    def _load(self, context: Any, preload: dict[str, str], batch: int) -> None:
        items = list(preload.items())
        for start in range(0, len(items), batch):
            chunk = items[start : start + batch]
            for attempt in range(3):
                try:
                    client = self._target.connect()
                    pipe = client.pipeline(transaction=False)
                    for key, value in chunk:
                        pipe.set(key, value)
                    replies = pipe.execute()
                    acknowledged_at = time.time()
                    if not all(reply is True or reply == "OK" for reply in replies):
                        raise RuntimeError(f"unexpected preload replies: {replies[:3]}")
                    # A preloaded key is an acknowledged write: a crash soon
                    # after may lose it (e.g. appendfsync everysec).
                    for key, _ in chunk:
                        self._preload_acks[key] = (acknowledged_at, self._target.served_by(key))
                    break
                except (
                    valkey.exceptions.ConnectionError,
                    valkey.exceptions.TimeoutError,
                    valkey.exceptions.ClusterDownError,
                ):
                    # Re-sending the preload is harmless: it sets the same values.
                    self._target.reset()
                    if attempt == 2:
                        raise
                    context.sleep(0.2)

    def _load_lists(self, keys: list[str], length: int, size: int, batch: int) -> None:
        """RPUSH every list in batches; a failure stops the preload (an RPUSH sent again would append twice)."""

        for index, key in enumerate(keys):
            client = self._target.connect()
            digest = hashlib.sha256()
            chunk: list[str] = []
            for element in list_elements(index, length, size):
                digest.update(element.encode("utf-8"))
                digest.update(b"\0")
                chunk.append(element)
                if len(chunk) == batch:
                    client.rpush(key, *chunk)
                    chunk = []
            if chunk:
                client.rpush(key, *chunk)
            self._lists.append({"key": key, "length": length, "digest": digest.hexdigest()})

    def _read_list(self, op: str, key: str, op_rng: random.Random, config: dict[str, Any]) -> str:
        """Pipeline ``list_read_batch`` reads of ``key``; ``ok`` when every reply came back, else ``failed``."""

        length = config["list_length"]
        try:
            client = self._target.connect()
            pipe = client.pipeline(transaction=False)
            for _ in range(config["list_read_batch"]):
                start = op_rng.randrange(length)
                if op == "lrange":
                    pipe.lrange(key, start, start + _LRANGE_SPAN - 1)
                else:
                    pipe.lindex(key, start)
            pipe.execute()
        except _CLIENT_ERRORS as exc:
            self._list_reads["failed"] += 1
            self._target.reset()
            return f"failed: {type(exc).__name__}: {exc}"
        self._list_reads["ok"] += 1
        return "ok"

    def _execute(self, op_id: int, op: str, key: str, value: Optional[str], dst: Optional[str] = None) -> str:
        """Run one operation and record it; a ``rename`` of ``key`` to ``dst`` records a ``del`` and a ``set``."""

        entry: dict[str, Any] = {"id": op_id, "op": op, "key": key, "t_start": time.time()}
        if value is not None:
            entry["value"] = value
        entries = [entry]
        if op == "rename":
            entries = [
                {**entry, "op": "del", "rename": [key, dst]},
                {**entry, "id": op_id + 1, "op": "set", "key": dst, "rename": [key, dst]},
            ]
            del entries[0]["value"]
        try:
            client = self._target.connect()
        except Exception as exc:
            # Nothing was sent (e.g. no reachable primary): the operation was not applied.
            return self._record(entries, "error", exc)
        try:
            for item in entries:
                item["instance_id"] = self._target.instance_id
            if op == "set":
                reply = client.set(key, value)
                entry["reply"] = "OK" if reply is True else reply
            elif op == "del":
                entry["reply"] = client.delete(key)
            elif op == "rename":
                reply = client.rename(key, dst)
                for item in entries:
                    item["reply"] = "OK" if reply is True else reply
            else:
                entry["reply"] = client.get(key)
            # Which instance acknowledged it: under a partition or a forced
            # failover only the old primary's acknowledgements may be lost.
            for item in entries:
                item["instance_id"] = self._target.served_by(item["key"])
        except _CLIENT_ERRORS as exc:
            return self._record(entries, _classify(exc), exc)
        if op != "get" and self._config.get("wait_replicas"):
            # The write's own reply ends it; WAIT only adds how far it replicated.
            entry["t_end"] = time.time()
            self._wait(client, entry)
            for item in entries[1:]:
                item.update({field: entry[field] for field in ("t_end", "wait", "wait_error") if field in entry})
        return self._record(entries, "ok")

    def _record(self, entries: list[dict[str, Any]], outcome: str, exc: Optional[BaseException] = None) -> str:
        if exc is not None:
            # Reconnect after any failure: an error reply such as READONLY means
            # the endpoint stopped being the writable primary (e.g. failover).
            self._target.reset()
        t_end = time.time()
        for entry in entries:
            entry["outcome"] = outcome
            if exc is not None:
                entry["error"] = f"{type(exc).__name__}: {exc}"
            entry.setdefault("t_end", t_end)
            self._counters[outcome] += 1
            self._ledger.append(entry)
            if entry["op"] == "set" and outcome == "ok":
                self._known[entry["key"]] = entry["value"]
            elif entry["op"] != "get" and outcome != "error":
                self._known.pop(entry["key"], None)
        return outcome

    def _wait(self, client: Any, entry: dict[str, Any]) -> None:
        """Record how many replicas acknowledged the write (WAIT's reply); a failed WAIT leaves the write ``ok``."""

        try:
            entry["wait"] = int(
                client.execute_command("WAIT", self._config["wait_replicas"], self._config["wait_timeout_ms"])
            )
        except Exception as exc:
            entry["wait_error"] = f"{type(exc).__name__}: {exc}"
            self._target.reset()

    def stop(self, context: Any, spec: ComponentSpec):
        problem = None
        drained = True
        if self._config.get("drain_on_stop") and self._thread is not None:
            # poll_until waits with context.sleep, so a cancelled run stops waiting at once.
            poll_until(
                lambda: (self._drained.is_set() or not self._thread.is_alive(), None),
                timeout_s=self._config["drain_timeout_s"],
                interval_s=0.05,
                context=context,
            )
            drained = self._drained.is_set()
        if self._thread is not None:
            problem = self._thread.stop(timeout=max(30.0, self._config.get("op_timeout_s", 2.0) * 3))
            if self._thread.is_alive():
                # A worker still running owns its ledger, acknowledgements, and client: nothing of them is read.
                return make_result(spec, ComponentStatus.ERROR, f"workload {problem}")
        if self._target is not None:
            self._target.reset()
        directory = component_artifact_dir(context, spec)
        ledger_path = os.path.join(directory, "ledger.jsonl")
        preload_path = os.path.join(directory, "preload.jsonl")
        lists_path = None
        if self._lists:
            lists_path = os.path.join(directory, "lists.jsonl")
            write_jsonl(lists_path, self._lists)
        write_jsonl(ledger_path, self._ledger)
        write_jsonl(
            preload_path,
            (
                {
                    "key": key,
                    "value": value,
                    "t_end": self._preload_acks[key][0],
                    "instance_id": self._preload_acks[key][1],
                }
                for key, value in sorted(self._preload.items())
            ),
        )
        summary = {
            "operations": len(self._ledger),
            "acknowledged": self._counters["ok"],
            "rejected": self._counters["error"],
            "ambiguous": self._counters["ambiguous"],
            "preloaded_keys": len(self._preload),
            "stable_keys": self._config.get("stable_keys", 0),
            "prefix": key_prefix(spec, self._config) if self._config else None,
            "started_at": self._started_at,
            "stopped_at": time.time(),
            "ledger_path": ledger_path,
            "preload_path": preload_path,
            "replies_digest": replies_digest(self._ledger),
            "drained": self._drained.is_set(),
            "wait_replicas": self._config.get("wait_replicas", 0),
            "list_keys": len(self._lists),
            "lists_path": lists_path,
            "list_reads": dict(self._list_reads),
        }
        evidence = (
            make_evidence(
                spec,
                LEDGER_EVIDENCE,
                "workload.quiesced",
                summary,
                artifacts=(ledger_path, preload_path, *([lists_path] if lists_path else [])),
            ),
        )
        if problem is not None:
            return make_result(spec, ComponentStatus.ERROR, f"workload {problem}", evidence=evidence)
        if not drained:
            # A workload does not judge Valkey; a slow host may not drain in time either.
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"workload did not complete {self._config['max_ops']} operations within "
                f"{self._config['drain_timeout_s']}s ({len(self._ledger)} done)",
                evidence=evidence,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{summary['operations']} operations ({summary['acknowledged']} acknowledged, "
            f"{summary['ambiguous']} ambiguous, {summary['rejected']} rejected)",
            evidence=evidence,
            diagnostics={"counters": dict(self._counters)},
        )


def replies_digest(ledger: list[dict[str, Any]]) -> str:
    """Digest of the client-visible operation history (ops, keys, outcomes, replies).

    Identical seeds produce identical operation sequences, so two runs whose
    servers behave equivalently produce the same digest.
    """

    material = [
        [
            entry["id"],
            entry["op"],
            entry["key"],
            entry["outcome"],
            entry.get("reply") if entry["outcome"] == "ok" else None,
        ]
        for entry in ledger
    ]
    return hashlib.sha256(json.dumps(material, separators=(",", ":")).encode("utf-8")).hexdigest()


_WORKLOAD_DECLARES = Declarations(
    events={
        "{id}.seeded": ConfigSchema({"keys": INTEGER, "stable_keys": INTEGER, "mutable_keys": INTEGER}),
        "{id}.drained": ConfigSchema({"operations": INTEGER}),
    },
    evidence={
        LEDGER_EVIDENCE: declared(
            mapping_of(
                {
                    "operations": INTEGER,
                    "acknowledged": INTEGER,
                    "rejected": INTEGER,
                    "ambiguous": INTEGER,
                    "preloaded_keys": INTEGER,
                    "stable_keys": INTEGER,
                    "prefix": nullable(str),
                    "started_at": NUMBER,
                    "stopped_at": NUMBER,
                    "ledger_path": TEXT,
                    "preload_path": TEXT,
                    "replies_digest": TEXT,
                    "drained": FLAG,
                    "wait_replicas": INTEGER,
                    "list_keys": INTEGER,
                    "lists_path": nullable(str),
                    "list_reads": mapping_of({"ok": INTEGER, "failed": INTEGER}),
                }
            )
        )
    },
)

REGISTRATIONS = (
    component_registration(
        "valkey.workload.key-value",
        KeyValueWorkload,
        _WORKLOAD_FIELDS,
        check=_validate_workload,
        declares=_WORKLOAD_DECLARES,
    ),
)
