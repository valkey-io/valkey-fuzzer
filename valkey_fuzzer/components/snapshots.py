"""Dataset oracles: RDB snapshots reload correctly, and canonical dataset digests.

``valkey.oracle.rdb-snapshot`` reloads every RDB a ``valkey.action.bgsave``
captured in a private server and checks that each is a consistent cut of the
workload ledger's history;
``valkey.oracle.dataset-digest`` records a canonical digest of the live dataset
for differential comparison.

Both load the modules the deployment loads (its ``loadmodule``): a module's
values are digested from their DUMP payloads (:func:`canonical_value`), which
the digest oracle first checks the module writes the same way in every
process.
"""

import functools
import os
import re
import shutil
from collections.abc import Mapping, Sequence
from typing import Any, Optional

import valkey
from valkey.crc import key_slot

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.queries import parse_replication
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, Evidence, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config

from .compression import LOADED_COMPRESSED, RDB_STREAM_CODECS, rdb_format
from .files import read_jsonl, write_jsonl
from .instances import deployment, instance_at, is_cluster, stopped_on_purpose, unreachable_instances
from .keyspace import BUILTIN_TYPES, dump_body, keyspace_digest, keyspace_snapshot
from .ledger import LedgerModel, load_ledger, target_slot_ranges
from .observers import read_log
from .oracles import CRASH_PATTERNS
from .polling import poll_each, poll_until, read_retrying
from .registration import component_registration
from .schemas import INTEGER, LIST, MAPPING, TEXT, declared, mapping_of
from .server import PrivateServer, server_arguments
from .workloads import is_stable_key, list_digest


def instance_modules(context: Any, instance_id: str) -> dict[str, Any]:
    """``{"loadmodule": ...}`` as the deployment starts ``instance_id`` (its server config in the plan), or empty:
    a private server that holds the instance's data needs its modules."""

    instance = context.plan.instances.get(instance_id) or {}
    modules = plain_config(instance.get("server_config") or {}).get("loadmodule")
    return {} if modules is None else {"loadmodule": modules}


# ---------------------------------------------------------------------------
# valkey.oracle.rdb-snapshot — every captured RDB reloads with plausible data
# ---------------------------------------------------------------------------

_RDB_FIELDS = {
    "bgsave": ConfigField(str),
    "workload": ConfigField(str, allow_none=True, default=None),
    "timeout_s": ConfigField(float, default=60.0, minimum=1, maximum=3600),
    # The whole-stream codec every RDB must be in: none, lz4, zstd, or an
    # rdbcompression value (no, yes, and lzf write none).
    "expect_codec": ConfigField(str, allow_none=True, default=None, choices=(*RDB_STREAM_CODECS, None)),
    # With expect_codec: the saving instance's own log must show it loaded
    # data in that codec during the run (e.g. after a restart).
    "instance_reload": ConfigField(bool, default=False),
    # Load with this binary instead of the saving instance's (cross-binary
    # compatibility); usually an artifact reference.
    "load_binary": ConfigField(str, allow_none=True, default=None, reference=True),
    "load_config": ConfigField(dict, default=dict),
    # loads: every RDB must load; refused: the loading server must refuse it
    # and exit cleanly (no crash), logging `refused_log` when set.
    "expect": ConfigField(str, default="loads", choices=("loads", "refused")),
    "refused_log": ConfigField(str, allow_none=True, default=None),
}

# The loading server's verdict on an RDB it could not load at startup.
_LOAD_REFUSED = "Fatal error loading the DB"
_DB_LOADED = re.compile(r"DB loaded from (disk|append only file)")
_EXIT_STATUS = re.compile(r"^exit status (-?\d+)\n")


def _validate_rdb(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    components = {component.id: component for component in plan.components}
    for field in ("bgsave", "workload"):
        if config[field] is not None and config[field] not in components:
            raise ConfigError(f"{field} {config[field]!r} is not a component of this plan")
    bgsave = components[config["bgsave"]]
    if bgsave.use != "valkey.action.bgsave":
        raise ConfigError(
            f"bgsave {config['bgsave']!r} is a {bgsave.use}, not a valkey.action.bgsave that captures RDBs"
        )
    if plain_config(bgsave.config).get("capture_rdb") is False:
        raise ConfigError(f"bgsave {config['bgsave']!r} sets capture_rdb: false, so it captures no RDB to check")
    if config["instance_reload"] and config["expect_codec"] is None:
        raise ConfigError("instance_reload checks the codec of a reload, so it needs expect_codec")
    if config["refused_log"] is not None:
        if config["expect"] != "refused":
            raise ConfigError("refused_log applies only with expect: refused")
        try:
            re.compile(config["refused_log"])
        except re.error as exc:
            raise ConfigError(f"refused_log is not a valid regex: {exc}") from exc


class RdbLoadTimeoutError(Exception):
    """The private server did not start, or did not finish loading, in time: the harness cannot tell whether
    the RDB loads, so the oracle is ERROR, not FAIL."""


def load_rdb(
    context: Any, spec: ComponentSpec, binary: str, rdb_path: str, timeout_s: float, arguments: Sequence[str] = ()
) -> tuple[Optional[dict], str]:
    """Load ``rdb_path`` into a private server; return its contents, or ``None`` and why it failed.

    The contents include the server's ``log``. When the server exits during
    startup, the reason starts with ``exit status <code>`` and its output.
    The server is owned by component ``spec`` until it is stopped. Raises
    :class:`RdbLoadTimeoutError` when the server could not be started or was
    still loading (or not answering) after ``timeout_s``.
    """

    server = PrivateServer(binary, ["--dbfilename", "dump.rdb", *arguments], timeout_s=timeout_s, prefix="vfr-")
    owned = context.obligations.open(spec, "process.private-server", server.workdir, server.stop)
    try:
        shutil.copyfile(rdb_path, os.path.join(server.workdir, "dump.rdb"))
        try:
            exit_output = server.start()
        except (OSError, TimeoutError) as exc:
            raise RdbLoadTimeoutError(f"the loading server did not start: {exc}") from exc
        if exit_output is not None:
            return None, f"exit status {server.process.wait()}\n{exit_output}"

        def loaded() -> tuple[bool, bool]:
            # A server that refuses the RDB exits while it loads.
            if server.process.poll() is not None:
                return True, True
            try:
                return str(server.client.info("persistence").get("loading")) == "0", False
            except (valkey.exceptions.ValkeyError, OSError):
                return False, False

        done, exited = poll_until(loaded, timeout_s=timeout_s, interval_s=0.05, context=context)
        if exited:
            return None, f"exit status {server.process.wait()}\n{server.exit_output()}"
        if not done:
            raise RdbLoadTimeoutError(f"the RDB did not finish loading within {timeout_s}s")
        snapshot = keyspace_snapshot(server.client)
        values: dict[str, Optional[str]] = {
            key: (content if kind == "string" else f"<{kind}>") for key, (kind, content) in snapshot.items()
        }
        meta = {
            "keys_loaded": server.client.info("persistence").get("rdb_last_load_keys_loaded"),
            "dbsize": len(values),
            "content_digest": keyspace_digest(snapshot),
        }
        return {"values": values, "meta": meta, "snapshot": snapshot, "log": read_log(server.log_path)[0]}, ""
    finally:
        owned.discharge()


def codec_problems(expected: str, result: dict[str, Any], load_log: Optional[list[str]]) -> list[str]:
    """Why the saved RDB is not in codec ``expected`` (none, lz4, zstd).

    The file's envelope must name it, and a load log (when there is one) must
    show the matching path: ``Loading compressed RDB (algo=X)`` for a
    compressed codec, no such line for ``none``.
    """

    problems = []
    found = (result.get("rdb_format") or rdb_format(result["rdb_artifact"])).get("codec")
    if found != expected:
        problems.append(f"expected codec {expected}, the RDB is {'plain' if found == 'none' else found}")
    if load_log is not None:
        logged = sorted({match.group(1) for match in map(LOADED_COMPRESSED.search, load_log) if match})
        if logged != ([] if expected == "none" else [expected]):
            problems.append(f"expected codec {expected}, the load logged compressed codecs {logged or 'none'}")
    return problems


def _refusal_problem(error: str, pattern: Optional[str]) -> Optional[str]:
    """Why a failed load is not a clean refusal: the server must exit 1 with no crash report."""

    match = _EXIT_STATUS.match(error)
    if match is None:
        return f"expected a clean refusal, but the load did not exit: {error[-300:]}"
    output = error[match.end() :]
    crashed = any(re.search(pattern, output) for pattern in CRASH_PATTERNS)
    if match.group(1) != "1" or crashed or _LOAD_REFUSED not in output:
        return f"expected a clean refusal, but the loading server exited with status {match.group(1)}: {output[-300:]}"
    if pattern is not None and not re.search(pattern, output):
        return f"the refusal did not log {pattern!r}: {output[-300:]}"
    return None


def _intersect(left: list, right: list) -> list:
    """Intersection of two sorted lists of disjoint inclusive ``(low, high)`` ranges."""

    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        low = max(left[i][0], right[j][0])
        high = min(left[i][1], right[j][1])
        if low <= high:
            result.append((low, high))
        if left[i][1] < right[j][1]:
            i += 1
        else:
            j += 1
    return result


def _held(history: list, value: Optional[str], last: int) -> list:
    """The operation positions at which a key with ``history`` may hold ``value``."""

    ranges = []
    for index, (start, values) in enumerate(history):
        end = history[index + 1][0] - 1 if index + 1 < len(history) else last
        if value in values and start <= end:
            if ranges and ranges[-1][1] == start - 1:
                ranges[-1] = (ranges[-1][0], end)
            else:
                ranges.append((start, end))
    return ranges


def cut_bounds(ledger: list[dict[str, Any]], result: dict[str, Any]) -> tuple[int, int]:
    """The operation positions the snapshot's point in time may be at.

    Position ``p`` means: operations ``1..p`` were sent before the snapshot's
    point in time, later ones after it. A primary's snapshot follows every
    operation that completed before BGSAVE was sent; any snapshot precedes
    every operation sent after BGSAVE replied "started" (the fork, or the
    forkless iterator's consistent start, happens inside the command), or
    after the save finished.
    """

    last = ledger[-1]["id"] if ledger else 0
    low, high = 0, last
    sent_at = result.get("sent_at")
    if result.get("role") == "primary" and isinstance(sent_at, (int, float)):
        low = max((entry["id"] for entry in ledger if entry["t_end"] <= sent_at), default=0)
    upper = result.get("finished_at")
    if result.get("reply") == "Background saving started" and isinstance(result.get("replied_at"), (int, float)):
        upper = result["replied_at"]
    if isinstance(upper, (int, float)):
        high = min((entry["id"] for entry in ledger if entry["t_start"] >= upper), default=last + 1) - 1
    return low, high


def find_cut(model: LedgerModel, values: dict[str, Optional[str]], keys: Sequence[str], bounds: tuple, last: int):
    """Operation positions within ``bounds`` at which every key in ``keys`` could hold its value in ``values``.

    Returns ``(ranges, None)``, or ``([], conflict)`` naming the key that
    left no position.
    """

    candidate = [bounds] if bounds[0] <= bounds[1] else []
    for key in sorted(keys):
        history = model.history.get(key) or [(0, frozenset(model.possible.get(key, {None})))]
        value = values.get(key)
        held = _held(history, value, last)
        narrowed = _intersect(candidate, held)
        if not narrowed:
            return [], {
                "key": key,
                "value": value,
                "held_at": held[:5],
                "held_during_save": bool(_intersect([bounds], held)),
                "others_allow": candidate[:5],
                "bounds": list(bounds),
            }
        candidate = narrowed
    return candidate, None


def _list_problems(instance_id: str, lists: Mapping[str, Any], snapshot: Mapping[str, Any]) -> list[str]:
    """The workload's lists that the reloaded ``snapshot`` holds must have the length and elements it wrote.

    A list the snapshot lacks is not a problem here: in cluster mode an RDB holds its own shard's keys only,
    and a list that is missing where it should be is a key the cut check cannot explain either way.
    """

    problems = []
    for key, expected in lists.items():
        if key not in snapshot:
            continue
        kind, content = snapshot[key]
        if kind != "list":
            problems.append(f"{instance_id}: {key} reloaded as a {kind}, not the list the workload wrote")
        elif len(content) != expected["length"] or list_digest(content) != expected["digest"]:
            problems.append(
                f"{instance_id}: {key} reloaded with {len(content)} element(s), the workload wrote "
                f"{expected['length']}, or with different elements"
            )
    return problems


class RdbSnapshotOracle:
    """Every captured RDB loads (or, with ``expect: refused``, is refused cleanly).

    With a workload, each loaded RDB is a consistent cut of its write history;
    with ``expect_codec``, each is in that whole-stream codec and its load took
    the matching path.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        results = [plain_config(item.value) for item in evidence_of(evidence, "valkey.bgsave.result", config["bgsave"])]
        if not results:
            # The bgsave action judges its own saves; without its results there is nothing to load.
            return make_result(spec, ComponentStatus.ERROR, f"no RDB snapshot was captured by {config['bgsave']!r}")
        model = None
        ledger: list[dict[str, Any]] = []
        lists: dict[str, dict[str, Any]] = {}
        if config["workload"] is not None:
            try:
                preload, ledger, _, summary = load_ledger(evidence, config["workload"])
                if summary.get("lists_path"):
                    lists = {item["key"]: item for item in read_jsonl(summary["lists_path"])}
            except (LookupError, OSError, ValueError) as exc:
                return make_result(spec, ComponentStatus.ERROR, f"cannot load workload ledger: {exc}")
            # No loss windows: the save reads memory, which holds every
            # acknowledged write (experiments that lose writes before the save
            # do not check its contents against the ledger).
            model = LedgerModel(preload, ledger, [], history=True)
        last = ledger[-1]["id"] if ledger else 0
        loaded: dict[str, Optional[str]] = {}
        combined: dict[str, list] = {}
        problems = []
        errors = []
        per_instance: dict[str, Any] = {}
        codec = RDB_STREAM_CODECS.get(config["expect_codec"])
        for result in results:
            instance_id = result["instance_id"]
            if result.get("capture_error"):
                # Valkey saved, and the harness could not copy the file: nothing was judged.
                errors.append(f"{instance_id}: the saved RDB could not be captured ({result['capture_error']})")
                per_instance[instance_id] = {"loaded": False, "error": result["capture_error"]}
                continue
            if not result.get("rdb_artifact"):
                # Every target must have produced a snapshot; one that did not
                # must not hide behind another target's RDB. A save the action
                # could not follow (no reply, no INFO) is no failed save.
                reason = f"save status {result.get('status')!r}"
                unseen = result.get("status") in ("unanswered", "unavailable", "busy")
                (errors if unseen else problems).append(f"{instance_id}: no RDB was captured ({reason})")
                per_instance[instance_id] = {"loaded": False, "error": reason}
                continue
            binary = config["load_binary"] or handle.binary(instance_id)
            # The saving instance's modules, unless load_config loads its own.
            load_config = {**instance_modules(context, instance_id), **config["load_config"]}
            load_options = {"arguments": server_arguments(load_config)} if load_config else {}
            try:
                content, error = load_rdb(
                    context, spec, binary, result["rdb_artifact"], config["timeout_s"], **load_options
                )
            except RdbLoadTimeoutError as exc:
                errors.append(f"{instance_id}: {exc}")
                per_instance[instance_id] = {"loaded": False, "binary": binary, "error": str(exc)}
                continue
            per_instance[instance_id] = {"loaded": content is not None, "binary": binary}
            if codec is not None:
                found = codec_problems(codec, result, content.get("log", []) if content else None)
                if config["instance_reload"]:
                    lines, unread = read_log(handle.log_file(instance_id))
                    if unread is None:
                        found += self._reload_problems(lines, codec)
                    else:
                        errors.append(f"{instance_id}: its log could not be read: {unread}")
                problems.extend(f"{instance_id}: {problem}" for problem in found)
                per_instance[instance_id]["rdb_format"] = result.get("rdb_format")
            if config["expect"] == "refused":
                refusal = (
                    f"expected the load to be refused, but it loaded {content['meta'].get('dbsize')} key(s)"
                    if content is not None
                    else _refusal_problem(error, config["refused_log"])
                )
                if refusal is not None:
                    problems.append(f"{instance_id}: {refusal}")
                per_instance[instance_id]["refusal"] = error[-1000:] if content is None else None
                continue
            if content is None:
                problems.append(f"{instance_id}: RDB failed to load: {error[-300:]}")
                per_instance[instance_id]["error"] = error[-1000:]
                continue
            per_instance[instance_id].update(content["meta"])
            loaded.update(content["values"])
            combined.update(content["snapshot"])
            if model is not None:
                problems.extend(
                    self._check_cut(handle, model, ledger, last, result, content["values"], per_instance, lists)
                )
                problems.extend(_list_problems(instance_id, lists, content["snapshot"]))
        if model is not None and not problems and not errors and config["expect"] == "loads":
            stable = {key: value for key, value in model.preload.items() if is_stable_key(key)}
            missing = [key for key in stable if loaded.get(key) != stable[key]]
            if missing:
                problems.append(f"{len(missing)} stable key(s) missing or wrong in the snapshot (e.g. {missing[0]})")
        record = {"snapshots": per_instance, "keys_loaded": len(loaded), "content_digest": keyspace_digest(combined)}
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record)
        if errors:
            return make_result(spec, ComponentStatus.ERROR, "; ".join(errors), diagnostics=record)
        verdict = (
            f"{len(per_instance)} snapshot(s) refused cleanly"
            if config["expect"] == "refused"
            else f"{len(per_instance)} snapshot(s) reloaded with {len(loaded)} keys"
        )
        return make_result(
            spec,
            ComponentStatus.PASS,
            verdict + (f" in codec {codec}" if codec is not None else ""),
            evidence=(make_evidence(spec, "valkey.rdb.reload-verified", "oracle.completed", record),),
        )

    @staticmethod
    def _reload_problems(lines: Sequence[str], codec: str) -> list[str]:
        """The instance's own reloads during the run (e.g. after a restart), in its log ``lines``, must be in
        ``codec``: a reload from dump.rdb in that codec, one from the AOF plain (Valkey writes an AOF's base
        RDB uncompressed whatever rdbcompression is)."""

        sources = {match.group(1) for match in map(_DB_LOADED.search, lines) if match}
        if not sources:
            return ["the instance logged no reload during the run"]
        expected = codec if "disk" in sources else "none"
        logged = sorted({match.group(1) for match in map(LOADED_COMPRESSED.search, lines) if match})
        if logged != ([] if expected == "none" else [expected]):
            return [f"expected the instance to reload codec {expected}, it logged compressed codecs {logged or 'none'}"]
        return []

    @staticmethod
    def _check_cut(
        handle: Any,
        model: LedgerModel,
        ledger: list,
        last: int,
        result: dict,
        values: dict,
        per_instance: dict,
        lists: Mapping[str, Any] = {},
    ) -> list[str]:
        """Problems with one snapshot: keys nobody wrote, or no single point in the history explaining it."""

        instance_id = result["instance_id"]
        # The workload's lists are checked whole by _list_problems, not as points of the write history.
        values = {key: value for key, value in values.items() if key not in lists}
        foreign = sorted(key for key in values if key not in model.possible)
        if foreign:
            return [f"{instance_id}: {len(foreign)} key(s) the workload never wrote (e.g. {foreign[0]})"]
        # In cluster mode an RDB holds the slots of its instance's shard only.
        ranges = target_slot_ranges(handle, [instance_id])
        keys = set(values) | {
            key
            for key in model.possible
            if ranges is None
            or any(low <= key_slot(key.encode("utf-8", errors="surrogatepass")) <= high for low, high in ranges)
        }
        bounds = cut_bounds(ledger, result)
        cut, conflict = find_cut(model, values, keys, bounds, last)
        per_instance[instance_id]["cut"] = {"keys": len(keys), "bounds": list(bounds), "positions": cut[:5]}
        if conflict is None:
            return []
        per_instance[instance_id]["cut_conflict"] = conflict
        if not conflict["held_during_save"]:
            detail = (
                f"{conflict['key']} holds {conflict['value']!r}, which it never held between operations "
                f"{bounds[0]} and {bounds[1]} (while BGSAVE ran)"
            )
        else:
            detail = (
                f"{conflict['key']} holds {conflict['value']!r} (possible at operations {conflict['held_at']}), "
                f"but the other keys' values are possible only at operations {conflict['others_allow']}"
            )
        return [f"{instance_id}: the snapshot is not a consistent cut of the write history: {detail}"]


# ---------------------------------------------------------------------------
# valkey.oracle.dataset-digest — canonical digest of the live dataset
# ---------------------------------------------------------------------------

NONDETERMINISTIC = "this module's serialization is not deterministic across processes"
# How long each private server of the determinism probe may take to start.
_PROBE_START_TIMEOUT_S = 15.0


def serialization_problem(
    context: Any, spec: ComponentSpec, binary: str, server_config: Mapping[str, Any], payload: bytes
) -> Optional[str]:
    """Why a module type cannot be compared by its DUMP payload, or None.

    ``payload`` is RESTOREd on two private servers of ``binary`` started with ``server_config`` (the
    deployment's modules), and each DUMPs it: a module whose save writes, say, a hash table in iteration order
    writes differently in each process, whose hash seed differs, and its digests would differ between replicas
    holding the same data. Two different DUMPs are :data:`NONDETERMINISTIC`; a server that could not do it is
    described. Each server is owned until it is stopped.
    """

    bodies = []
    for _ in range(2):
        server = PrivateServer(binary, server_arguments(server_config), timeout_s=_PROBE_START_TIMEOUT_S, prefix="vfd-")
        owned = context.obligations.open(spec, "process.private-server", server.workdir, server.stop)
        try:
            # PrivateServer.start polls with time.sleep: not cancellable, but bounded by its timeout.
            exit_output = server.start()
            if exit_output is not None:
                return f"the determinism probe's server exited at startup: {exit_output[-300:]}"
            server.client.restore("probe", 0, payload)
            bodies.append(dump_body(server.client.dump("probe")))
        except (OSError, TimeoutError, valkey.exceptions.ValkeyError) as exc:
            return f"the determinism probe could not RESTORE and DUMP a value: {type(exc).__name__}: {exc}"
        finally:
            owned.discharge()
    return None if bodies[0] == bodies[1] else NONDETERMINISTIC


_DIGEST_FIELDS = {
    "match": ConfigField(str, allow_none=True, default=None),
    "include_replicas": ConfigField(bool, default=False),
    "converge_timeout_s": ConfigField(float, default=30.0, minimum=0, maximum=600),
    "max_keys_listed": ConfigField(int, default=100_000, minimum=0),
}


class DatasetDigestOracle:
    """Record a canonical digest of the dataset held by every primary.

    The digest itself never fails a run; it is evidence for differential
    comparison. The oracle fails when no instance is a primary, when in
    cluster mode a key is held by more than one primary (in standalone mode
    every primary may hold every key), or when ``include_replicas`` is set and a
    replica's digest differs from its view of the primaries' data. Reads are
    retried within ``converge_timeout_s``; an instance that stays unreachable
    or unreadable makes it ERROR, since a digest that silently left it out
    would compare as a different (or, worse, the same) dataset.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        combined: dict[str, list] = {}
        per_instance = {}
        problems = []
        errors = []
        cluster = is_cluster(handle)
        timeout_s = config["converge_timeout_s"]

        stopped = stopped_on_purpose(handle, evidence)

        def reachable() -> tuple[bool, list[str]]:
            unreachable = unreachable_instances(handle, stopped)
            return not unreachable, unreachable

        unreachable = poll_until(reachable, timeout_s=timeout_s, interval_s=0.2, context=context)[1]
        if unreachable:
            errors.append(f"unreachable instance(s): {', '.join(unreachable)}")
        primaries = handle.primaries()
        if not primaries and not unreachable:
            # Every instance answered as a replica: no primary holds the dataset.
            problems.append("no instance is a primary, so no dataset was read")

        def read_primary(instance_id: str) -> dict[str, list]:
            with handle.new_client(instance_id, timeout=10.0) as client:
                return keyspace_snapshot(client, config["match"])

        # One key of each module type, and the primary holding it: the determinism probe's sample.
        samples: dict[str, tuple[str, str]] = {}
        for instance_id in primaries:
            snapshot, error = read_retrying(context, functools.partial(read_primary, instance_id), timeout_s=timeout_s)
            if error is not None:
                errors.append(f"{instance_id}: {error}")
                continue
            for key, (kind, _) in sorted(snapshot.items()):
                if kind not in BUILTIN_TYPES:
                    samples.setdefault(kind, (instance_id, key))
            per_instance[instance_id] = {"keys": len(snapshot), "digest": keyspace_digest(snapshot)}
            duplicate = set(snapshot) & set(combined)
            if duplicate and cluster:
                problems.append(f"{len(duplicate)} key(s) are held by more than one primary")
            combined.update(snapshot)
        if not problems and not errors:
            # Before any digest of a module type is judged: its values compare by digest only if every process
            # serializes them the same way. One that does not is ERROR, never a FAIL across replicas.
            errors.extend(self._module_problems(context, spec, handle, samples))
        replica_report = {}
        if config["include_replicas"] and not problems and not errors:

            def matches_primary(instance_id: str) -> tuple[bool, dict[str, Any]]:
                error = None
                primary_id = None
                snapshot: dict[str, list] = {}
                with handle.new_client(instance_id, timeout=10.0) as client:
                    try:
                        info = client.info("replication")
                        link = parse_replication(info)
                        primary_id = instance_at(handle, link.primary_host, link.primary_port)
                        if cluster:
                            client.execute_command("READONLY")
                        snapshot = keyspace_snapshot(client, config["match"])
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                expected = per_instance.get(primary_id, {}).get("digest") if primary_id else None
                digest = keyspace_digest(snapshot)
                matched = error is None and expected is not None and digest == expected
                return matched, {
                    "primary": primary_id,
                    "keys": len(snapshot),
                    "digest": digest,
                    "matches_primary": matched,
                    "error": error,
                }

            polled = poll_each(
                context, handle.replicas(), matches_primary, timeout_s=config["converge_timeout_s"], interval_s=0.2
            )
            for instance_id, (matched, report) in polled.items():
                replica_report[instance_id] = report
                if report["error"] is not None:
                    errors.append(f"replica {instance_id}: {report['error']}")
                elif not matched:
                    problems.append(f"replica {instance_id} does not hold its primary's ({report['primary']}) dataset")
        digest = keyspace_digest(combined)
        listing_path = os.path.join(component_artifact_dir(context, spec), "keyspace.jsonl")
        write_jsonl(
            listing_path,
            (
                {"key": key, "type": value[0], "sha256": keyspace_digest({key: value})[:16]}
                for key, value in sorted(combined.items())[: config["max_keys_listed"]]
            ),
        )
        record = {
            "digest": digest,
            "keys": len(combined),
            "primaries": per_instance,
            "replicas": replica_report,
            "listing": listing_path,
            # The module types digested by their DUMP payloads, each probed deterministic.
            "module_types": sorted(samples),
        }
        if problems:
            return make_result(
                spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics={**record, "errors": errors}
            )
        if errors:
            return make_result(
                spec, ComponentStatus.ERROR, f"cannot read the dataset: {'; '.join(errors)}", diagnostics=record
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"dataset digest {digest[:12]} over {len(combined)} keys",
            evidence=(
                make_evidence(spec, "valkey.dataset.digest", "oracle.completed", record, artifacts=(listing_path,)),
            ),
        )

    @staticmethod
    def _module_problems(
        context: Any, spec: ComponentSpec, handle: Any, samples: Mapping[str, tuple[str, str]]
    ) -> list[str]:
        """Why the module types in ``samples`` cannot be compared by digest: one key of each, DUMPed from its
        primary, is probed with :func:`serialization_problem` on the primary's binary and modules."""

        found = []
        for kind, (instance_id, key) in sorted(samples.items()):
            try:
                with handle.new_client(instance_id, timeout=10.0) as client:
                    payload = client.dump(key)
            except (valkey.exceptions.ValkeyError, OSError) as exc:
                found.append(f"{kind}: cannot DUMP {key!r} on {instance_id}: {type(exc).__name__}: {exc}")
                continue
            if payload is None:
                found.append(f"{kind}: {key!r} was gone from {instance_id} before its DUMP could be probed")
                continue
            problem = serialization_problem(
                context, spec, handle.binary(instance_id), instance_modules(context, instance_id), payload
            )
            if problem is not None:
                found.append(f"{kind}: {problem}, so its values cannot be compared by digest")
        return found


_RDB_DECLARES = Declarations(
    evidence={
        "valkey.rdb.reload-verified": declared(
            mapping_of({"snapshots": MAPPING, "keys_loaded": INTEGER, "content_digest": TEXT})
        )
    },
    changes=("process.private-server",),
)
_DIGEST_DECLARES = Declarations(
    evidence={
        "valkey.dataset.digest": declared(
            mapping_of(
                {
                    "digest": TEXT,
                    "keys": INTEGER,
                    "primaries": MAPPING,
                    "replicas": MAPPING,
                    "listing": TEXT,
                    "module_types": LIST,
                }
            )
        )
    },
    changes=("process.private-server",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.rdb-snapshot", RdbSnapshotOracle, _RDB_FIELDS, check=_validate_rdb, declares=_RDB_DECLARES
    ),
    component_registration(
        "valkey.oracle.dataset-digest", DatasetDigestOracle, _DIGEST_FIELDS, declares=_DIGEST_DECLARES
    ),
)
