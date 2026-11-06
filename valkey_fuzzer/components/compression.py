"""``valkey.oracle.repl-compression``: every replication link uses the codec a model predicts.

The oracle reads each instance's ``repl-compression``, ``rdbcompression``,
``repl-diskless-sync``, and ``dual-channel-replication-enabled`` and predicts,
with its own copy of Valkey's rules (``src/replication.c``), the codec of
every primary-to-replica link and the transport of its last full sync:

- A replica advertises what its own ``repl-compression`` allows: ``zstd``
  advertises zstd and lz4, ``yes``/``lz4`` lz4, ``no`` nothing. A binary
  without ``repl-compression`` (an older build) advertises nothing.
- A primary uses the strongest codec its ``repl-compression`` allows (none <
  lz4 < zstd) that the replica advertised; an older primary sends plaintext.
- A full sync sends the RDB in that codec. It is disk-based only when the
  primary has ``repl-diskless-sync no``, the codec equals the whole-stream
  codec of its ``rdbcompression`` (``no``, ``yes``, and ``lzf`` are none),
  and the replica reads the primary's RDB version (an older build gets a
  diskless sync in its older format); otherwise it is diskless. Dual-channel
  needs it enabled on both sides and the primary's ``repl-diskless-sync yes``:
  without it the primary ignores the replica's dual-channel capability, even
  when the codec forces a diskless sync.
- A replica that loads from the socket logs the wire codec; one that loads
  from disk first rewrites the RDB into its own ``rdbcompression``.

It then checks what actually ran: ``INFO replication`` reports each link's
codec (``slaveN:...,repl_compression=X``, absent for plaintext) and that the
compressor saw bytes; the primary logs ``Replication compression enabled for
replica R (algo=X)`` in the link's last session, and never for a plaintext
link; with ``full_sync``, the primary's ``Starting BGSAVE for SYNC with
target: T using: U, compression: X`` and the replica's ``Loading compressed
RDB (algo=X)`` lines of the last full sync match the prediction.
"""

import os
import re
import time
from collections.abc import Sequence
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.queries import parse_replication, replication_role
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.results import ComponentStatus, Evidence, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField

from .instances import deployment, replica_links, stopped_on_purpose
from .observers import read_log
from .polling import remaining
from .registration import component_registration
from .schemas import LIST, MAPPING, declared, mapping_of

_STRENGTH = ("none", "lz4", "zstd")
# repl-compression value -> the strongest codec it allows.
_CEILING = {"no": "none", "yes": "lz4", "lz4": "lz4", "zstd": "zstd"}
# rdbcompression value (or a codec) -> the whole-stream codec of the RDB it writes; LZF (yes, lzf) compresses
# strings inside a plain RDB.
RDB_STREAM_CODECS = {"no": "none", "yes": "none", "lzf": "none", "none": "none", "lz4": "lz4", "zstd": "zstd"}
_CONFIGS = ("repl-compression", "rdbcompression", "repl-diskless-sync", "dual-channel-replication-enabled")

_SYNC_STARTED = re.compile(
    r"Starting BGSAVE for SYNC with target: (disk|replicas sockets)"
    r"(?: using: (dual-channel|normal sync))?(?:, compression: (\w+))?"
)
# A load of a whole-stream compressed RDB, from a file or the replication stream (src/rdb.c).
LOADED_COMPRESSED = re.compile(r"Loading compressed RDB \(algo=(\w+)\)")
_LOAD_DONE = re.compile(r"Loading RDB produced by|Done loading RDB")

_FIELDS = {
    "converge_timeout_s": ConfigField(float, default=30.0, minimum=0, maximum=600),
    # Every compressed link's uncompressed/compressed byte ratio must reach this.
    "min_ratio": ConfigField(float, allow_none=True, default=None, minimum=0),
    # Check each link's last full sync (BGSAVE codec, transport, and the
    # replica's load) against the prediction.
    "full_sync": ConfigField(bool, default=False),
    # Every link's predicted codec must be this (the scenario configured what
    # the experiment claims to test).
    "expect_algo": ConfigField(str, allow_none=True, default=None, choices=(*_STRENGTH, None)),
    "min_links": ConfigField(int, default=1, minimum=0),
}


def advertised(setting: Optional[str]) -> set[str]:
    """The codecs a replica with this ``repl-compression`` advertises (``None``: an older build)."""

    ceiling = _CEILING.get(setting or "no", "none")
    return set(_STRENGTH[1 : _STRENGTH.index(ceiling) + 1])


def negotiate(primary_setting: Optional[str], replica_setting: Optional[str]) -> str:
    """The codec a primary with ``primary_setting`` uses for a replica with ``replica_setting``."""

    if primary_setting is None:
        return "none"
    offers = advertised(replica_setting)
    ceiling = _STRENGTH.index(_CEILING.get(primary_setting, "none"))
    return next((algo for algo in reversed(_STRENGTH[1 : ceiling + 1]) if algo in offers), "none")


def unsettled_links(handle: Any) -> list[str]:
    """The live replication links still on another codec than their ends' ``repl-compression`` negotiate.

    A runtime ``repl-compression`` change takes effect only when the link
    reconnects: the primary's replication cron drops at most one online
    replica per second whose codec no longer matches, and a replica drops its
    own upstream link when what it advertised changed. Until then the link
    keeps the old codec, and a drop in the middle of a ``FAILOVER TO`` stops
    the failover. A link counts once both ends see it (the primary lists the
    replica online, the replica's link is up); an instance that does not
    answer is left out.
    """

    settings, infos = {}, {}
    for instance in handle.instances:
        if not handle.is_alive(instance.instance_id):
            continue
        try:
            with handle.new_client(instance.instance_id, timeout=2.0) as client:
                settings[instance.instance_id] = client.config_get("repl-compression").get("repl-compression")
                infos[instance.instance_id] = client.info("replication")
        except Exception:
            continue
    problems = []
    for primary_id, info in infos.items():
        for entry, replica_id in replica_links(info, handle):
            if entry.state != "online":
                continue
            if replica_id not in infos or not parse_replication(infos[replica_id]).link_up:
                continue
            expected = negotiate(settings[primary_id], settings[replica_id])
            observed = str(entry.fields.get("repl_compression", "none"))
            if observed != expected:
                problems.append(f"{primary_id} -> {replica_id} uses {observed}, its settings negotiate {expected}")
    return problems


def rdb_stream_codec(rdbcompression: Optional[str]) -> str:
    return RDB_STREAM_CODECS.get(rdbcompression or "", "none")


# Whole-stream codecs by their VCS envelope codec ID (src/compression_stream.h).
_VCS_CODECS = {1: "lz4", 2: "zstd"}


def rdb_format(path: str) -> dict[str, Any]:
    """The envelope and whole-stream codec of an RDB file, read from its first bytes.

    A streaming-compressed RDB starts with the 7-byte VCS envelope ``"VCS"``,
    version, codec ID, a reserved byte, and the stream kind; a plain one with
    ``REDIS<4 digits>`` or ``VALKEY<3 digits>``. LZF (``rdbcompression yes``)
    compresses strings inside a plain RDB, so its codec is ``none``.
    """

    try:
        size = os.path.getsize(path)
        with open(path, "rb") as handle:
            header = handle.read(9)
    except OSError as exc:
        return {"envelope": "unreadable", "codec": None, "size": None, "error": str(exc)}
    if header[:3] == b"VCS" and len(header) >= 7:
        return {
            "envelope": "VCS",
            "codec": _VCS_CODECS.get(header[4], f"unknown-{header[4]}"),
            "size": size,
            "vcs_version": header[3],
            "stream_kind": header[6],
        }
    for signature in (b"REDIS", b"VALKEY"):
        digits = header[len(signature) :]
        if header.startswith(signature) and digits.isdigit():
            return {"envelope": "plain", "codec": "none", "size": size, "rdb_version": int(digits)}
    return {"envelope": "unknown", "codec": None, "size": size, "header": header.hex()}


def expected_full_sync(primary: dict, replica: dict, codec: str) -> dict[str, str]:
    """The ``target`` and ``using`` of a full sync in ``codec`` from ``primary`` to ``replica``."""

    diskless = primary["repl-diskless-sync"] == "yes"
    if primary["repl-compression"] is not None:
        # A newer primary forces diskless for a codec other than the on-disk
        # format, and for an older replica's RDB version.
        diskless = diskless or replica["repl-compression"] is None
        diskless = diskless or codec != rdb_stream_codec(primary["rdbcompression"])
    # The primary accepts a replica's dual-channel capability only with
    # repl-diskless-sync yes; a sync that is diskless only because of its codec
    # or RDB version stays a normal sync.
    dual = primary["repl-diskless-sync"] == "yes" and primary["dual-channel-replication-enabled"] == "yes"
    dual = dual and replica["dual-channel-replication-enabled"] == "yes"
    return {
        "target": "replicas sockets" if diskless else "disk",
        "using": "dual-channel" if dual else "normal sync",
        "compression": codec,
    }


def _last_index(lines: Sequence[str], needle: str, before: Optional[int] = None) -> Optional[int]:
    end = len(lines) if before is None else before
    return next((index for index in range(end - 1, -1, -1) if needle in lines[index]), None)


def session_enablements(lines: Sequence[str], replica_name: str) -> Optional[list[str]]:
    """Codecs the primary enabled for ``replica_name`` in its last session; ``None`` if it never connected."""

    start = _last_index(lines, f"Replica {replica_name} asks for synchronization")
    if start is None:
        return None
    enabled = re.compile(rf"Replication compression enabled for replica {re.escape(replica_name)} \(algo=(\w+)\)")
    return [match.group(1) for match in map(enabled.search, lines[start:]) if match]


def full_sync_starts(lines: Sequence[str], replica_name: str) -> Optional[list[dict[str, str]]]:
    """The BGSAVE-for-SYNC lines of ``replica_name``'s last full sync; ``None`` without one.

    The sync ends at ``Synchronization with replica R succeeded`` and began
    after R's previous session ended. A replica that joined a save already in
    progress gets the last save started before its sync began.
    """

    end = _last_index(lines, f"Synchronization with replica {replica_name} succeeded")
    if end is None:
        return None
    begin = max(
        (
            index
            for index in (
                _last_index(lines, f"Synchronization with replica {replica_name} succeeded", end),
                _last_index(lines, f"Connection with replica {replica_name} lost", end),
                _last_index(lines, f"Partial resynchronization request from {replica_name} accepted", end),
            )
            if index is not None
        ),
        default=-1,
    )
    starts = [match for match in map(_SYNC_STARTED.search, lines[begin + 1 : end]) if match]
    if not starts:
        earlier = [match for match in map(_SYNC_STARTED.search, lines[: begin + 1]) if match]
        starts = earlier[-1:]
    return [
        {"target": match.group(1), "using": match.group(2) or "normal sync", "compression": match.group(3) or "none"}
        for match in starts
    ]


def replica_load(lines: Sequence[str]) -> Optional[dict[str, Any]]:
    """How the replica loaded its last full sync: ``{source, codecs}``; ``None`` without a load."""

    # Dual-channel replication logs its own start of a full sync. Valkey 7.2 says "master" where later releases
    # say "primary".
    starts = [
        _last_index(lines, "Full resync from primary"),
        _last_index(lines, "Full resync from master"),
        _last_index(lines, "PSYNC is not possible, initialize RDB"),
    ]
    start = max((index for index in starts if index is not None), default=None)
    if start is None:
        return None
    segment = lines[start:]
    if not any(_LOAD_DONE.search(line) for line in segment):
        return None
    socket = any("with EOF to parser" in line for line in segment)
    codecs = sorted({match.group(1) for match in map(LOADED_COMPRESSED.search, segment) if match})
    return {"source": "socket" if socket else "disk", "codecs": codecs}


class ReplCompressionOracle:
    """Every replication link negotiated and used the codec the model predicts.

    It reads every instance until the links match the model or
    ``converge_timeout_s`` passes. An instance still unreachable then makes it
    ERROR: the links it is part of were never seen, so their problems (a
    replica not attached, too few links) prove nothing about the server.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        deadline = time.monotonic() + remaining(context, config["converge_timeout_s"])
        while True:
            nodes, links, problems, unreachable = self._observe(handle, config, stopped_on_purpose(handle, evidence))
            if not (problems or unreachable) or time.monotonic() >= deadline or not context.sleep(0.5):
                break
        # The configs predict a codec the experiment does not test: an authoring or build problem, not Valkey's.
        unseen = [
            f"{link['primary']} -> {link['replica']}: the configs predict codec {link['algo']}, the experiment "
            f"expects {config['expect_algo']} (primary repl-compression {nodes[link['primary']]['repl-compression']}, "
            f"replica {nodes[link['replica']]['repl-compression']})"
            for link in links
            if config["expect_algo"] is not None and link["algo"] != config["expect_algo"]
        ]
        if not problems and not unreachable and not unseen:
            for link in links:
                found, unread = self._check_logs(handle, config, nodes, link)
                problems.extend(found)
                unseen.extend(unread)
        record = {"links": links, "instances": nodes}
        if unreachable:
            return make_result(
                spec, ComponentStatus.ERROR, "; ".join(unreachable[:10]), diagnostics={**record, "problems": problems}
            )
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems[:10]), diagnostics=record)
        if unseen:
            return make_result(spec, ComponentStatus.ERROR, "; ".join(unseen[:10]), diagnostics=record)
        compressed = sum(link["algo"] != "none" for link in links)
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(links)} link(s) use the predicted codec ({compressed} compressed)",
            evidence=(make_evidence(spec, "valkey.repl.compression-verified", "oracle.completed", record),),
        )

    @staticmethod
    def _observe(handle: Any, config: dict, stopped: set[str]) -> tuple[dict, list, list[str], list[str]]:
        """Read every instance not ``stopped`` on purpose, pair primaries with replicas, and compare INFO with
        the model: the instances, the links, the problems, and the instances that could not be read."""

        nodes: dict[str, dict[str, Any]] = {}
        problems: list[str] = []
        unreachable: list[str] = []
        for instance in handle.instances:
            if instance.instance_id in stopped:
                continue
            try:
                with handle.new_client(instance.instance_id, timeout=5.0) as client:
                    values = {name: client.config_get(name).get(name) for name in _CONFIGS}
                    info = client.info("replication")
            except Exception as exc:
                unreachable.append(f"{instance.instance_id} is unreachable: {type(exc).__name__}: {exc}")
                continue
            nodes[instance.instance_id] = {**values, "name": f"{instance.host}:{instance.port}", "info": info}
        links = []
        attached = set()
        for primary_id, primary in nodes.items():
            for entry, replica_id in replica_links(primary["info"], handle):
                if replica_id is None:
                    problems.append(f"{primary_id} serves an unknown replica {entry.ip}:{entry.port}")
                    continue
                if replica_id not in nodes:
                    # Unreachable from here: already a problem.
                    continue
                attached.add(replica_id)
                links.append(ReplCompressionOracle._link(config, primary_id, primary, replica_id, nodes, entry))
                problems.extend(links[-1].pop("problems"))
        for instance_id, node in nodes.items():
            if replication_role(node["info"]) == "replica" and instance_id not in attached:
                problems.append(f"replica {instance_id} is not attached to its primary")
        if len(links) < config["min_links"]:
            problems.append(f"{len(links)} replication link(s), fewer than {config['min_links']}")
        for node in nodes.values():
            node.pop("info")
        return nodes, links, problems, unreachable

    @staticmethod
    def _link(config: dict, primary_id: str, primary: dict, replica_id: str, nodes: dict, entry: Any) -> dict:
        replica = nodes[replica_id]
        expected = negotiate(primary["repl-compression"], replica["repl-compression"])
        observed = str(entry.fields.get("repl_compression", "none"))
        compressed, uncompressed = (
            None if entry.fields.get(name) is None else int(entry.fields[name])
            for name in ("repl_compressed_bytes", "repl_uncompressed_bytes")
        )
        link = {
            "primary": primary_id,
            "replica": replica_id,
            "algo": expected,
            "observed": observed,
            "state": entry.state,
            "compressed_bytes": compressed,
            "uncompressed_bytes": uncompressed,
            "ratio": round(uncompressed / compressed, 3) if expected != "none" and compressed else None,
            "problems": [],
        }
        where = f"{primary_id} -> {replica_id}"
        if entry.state != "online" or (entry.type or "replica") != "replica":
            link["problems"].append(f"{where} is not online (state {entry.state}, type {entry.type})")
        if observed != expected:
            link["problems"].append(
                f"{where}: expected codec {expected} (primary repl-compression {primary['repl-compression']}, "
                f"replica {replica['repl-compression']}), INFO reports {observed}"
            )
        elif expected != "none":
            if not uncompressed:
                link["problems"].append(f"{where}: the {expected} compressor has seen no bytes")
            elif config["min_ratio"] is not None and (link["ratio"] or 0) < config["min_ratio"]:
                link["problems"].append(
                    f"{where}: compression ratio {link['ratio']} ({uncompressed}/{compressed} bytes) "
                    f"is below {config['min_ratio']}"
                )
        return link

    @staticmethod
    def _check_logs(handle: Any, config: dict, nodes: dict, link: dict) -> tuple[list[str], list[str]]:
        """What the logs of ``link``'s instances show against the model, and the logs that could not be read."""

        primary, replica = nodes[link["primary"]], nodes[link["replica"]]
        where = f"{link['primary']} -> {link['replica']}"
        lines, unread = read_log(handle.log_file(link["primary"]))
        if unread is not None:
            return [], [f"{where}: the primary's log could not be read: {unread}"]
        enabled = session_enablements(lines, replica["name"])
        link["enabled_logged"] = enabled
        problems = []
        if enabled is None:
            problems.append(f"{where}: the primary never logged the replica's handshake")
        elif enabled != [link["algo"]] * len(enabled) or bool(enabled) != (link["algo"] != "none"):
            problems.append(f"{where}: expected codec {link['algo']}, the primary logged enabling {enabled or 'none'}")
        if not config["full_sync"]:
            return problems, []
        expected = expected_full_sync(primary, replica, link["algo"])
        starts = full_sync_starts(lines, replica["name"])
        link["full_sync"] = {"expected": expected, "logged": starts}
        if not starts:
            return [*problems, f"{where}: the primary logged no full sync of the replica"], []
        if expected not in starts:
            problems.append(f"{where}: expected a full sync {expected}, the primary logged {starts}")
        replica_lines, unread = read_log(handle.log_file(link["replica"]))
        if unread is not None:
            return problems, [f"{where}: the replica's log could not be read: {unread}"]
        load = replica_load(replica_lines)
        link["full_sync"]["load"] = load
        if load is None:
            return [*problems, f"{where}: the replica logged no full sync load"], []
        # A socket load reads the wire codec; a disk load the replica's own rdbcompression.
        codec = link["algo"] if load["source"] == "socket" else rdb_stream_codec(replica["rdbcompression"])
        if load["codecs"] != ([] if codec == "none" else [codec]):
            problems.append(
                f"{where}: the replica's {load['source']} load should be in codec {codec}, "
                f"it logged compressed codecs {load['codecs'] or 'none'}"
            )
        return problems, []


_DECLARES = Declarations(
    evidence={"valkey.repl.compression-verified": declared(mapping_of({"links": LIST, "instances": MAPPING}))}
)

REGISTRATIONS = (
    component_registration("valkey.oracle.repl-compression", ReplCompressionOracle, _FIELDS, declares=_DECLARES),
)
