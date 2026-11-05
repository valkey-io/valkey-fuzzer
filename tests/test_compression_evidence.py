"""Compression and snapshot evidence: the build probe, ``valkey.oracle.rdb-snapshot`` (codec, refusal, and
consistent-cut checks), and ``valkey.oracle.repl-compression``."""

import dataclasses
import os
from types import SimpleNamespace

import pytest
from valkey.exceptions import BusyLoadingError

from tests.support import VirtualClock, valkey_build
from tests.valkey_experiments_support import fake_context, resolved, unit_plan
from valkey_fuzzer.components import capabilities, server, snapshots
from valkey_fuzzer.components.compression import ReplCompressionOracle, unsettled_links
from valkey_fuzzer.components.files import write_jsonl
from valkey_fuzzer.components.workloads import LEDGER_EVIDENCE
from valkey_fuzzer.plan import ComponentSpec, InstanceSelector, StartupOverlay
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.results import Evidence
from valkey_fuzzer.testing import FakeContext

_LZ4 = b"VCS\x01\x01\x00\x01\x04\x22" + b"\x00" * 16
_PLAIN = b"VALKEY081\xfa" + b"\x00" * 16


class _ProbeServer:
    """A probe server that runs with the config it starts with plus ``configs``; a build without zstd refuses
    zstd, and CONFIG SET accepts only ``settable`` values."""

    def __init__(self, zstd, configs, settable, binary, arguments, **_):
        import valkey

        started = dict(zip((argument[2:] for argument in arguments[::2]), arguments[1::2]))
        self.config = {"bgsave-default-method": "fork", **configs, **started}
        self.zstd, self.workdir = zstd, "/nonexistent/probe"
        info = {"valkey_version": "9.0.0", "valkey_git_sha1": "abc", "rdb_current_bgsave_type": "none"}

        def config_set(name, value):
            if value not in settable.get(name, ()):
                raise valkey.exceptions.ResponseError(f"Invalid argument '{value}' for CONFIG SET '{name}'")

        self.client = SimpleNamespace(
            info=lambda section: info,
            config_get=lambda name: {name: self.config[name]} if name in self.config else {},
            config_set=config_set,
        )

    def start(self):
        if not self.zstd and "zstd" in self.config.values():
            return "FATAL CONFIG FILE ERROR: Invalid argument 'zstd' for CONFIG 'rdbcompression'"

    def stop(self):
        pass


_SETTABLE = {"repl-compression": ("no", "lz4")}


@pytest.mark.parametrize(
    ("zstd", "overlay", "probe", "configs", "settable", "status", "summary"),
    [
        # Known-bad: a scenario that saves with fork runs (and lacks the path evidence); a SKIP would hide it.
        (True, {"bgsave-default-method": "fork"}, {}, {}, {}, "PASS", ""),
        # A codec the build lacks, in a setting the experiment names: unsupported.
        (False, {"rdbcompression": "zstd"}, {}, {}, {}, "SKIP", "rdbcompression, which it may lack"),
        # Any other rejected setting is an error, so a typo cannot skip the run.
        (False, {"repl-compression": "zstd"}, {}, {}, {}, "ERROR", "not a setting the experiment says the build may lack"),
        (False, {"rdbcompresion": "zstd"}, {}, {}, {}, "ERROR", "not a setting the experiment says the build may lack"),
        # An "old" build that has the feature is not the build the experiment needs.
        (True, None, {"lacks_configs": ["repl-compression"]}, {"repl-compression": "no"}, {}, "ERROR", "must lack"),
        (True, None, {"lacks_configs": ["repl-compression"]}, {}, {}, "PASS", ""),
        (True, None, {"config_set": {"repl-compression": ["lz4", "zstd"]}}, {}, _SETTABLE, "SKIP", "CONFIG SET repl-compression zstd"),
        (True, None, {"config_set": {"repl-compression": ["lz4", "no"]}}, {}, _SETTABLE, "PASS", ""),
        # A second build is an input the operator gives: without it the run was set up wrong.
        (True, None, {"min_distinct_builds": 2}, {}, {}, "ERROR", "needs 2 distinct builds"),
    ],
    ids=["feature-off-runs", "named-setting-refused", "other-setting-refused", "misspelt-setting-refused",
         "lacks-config-present", "lacks-config-absent", "config-set-rejected", "config-set-accepted", "one-build"],
)  # fmt: skip
def test_the_build_probe_skips_only_a_build_that_cannot_run_the_scenario(
    monkeypatch, tmp_path, zstd, overlay, probe, configs, settable, status, summary
):
    binary = valkey_build(tmp_path, "bgsave-default-method")
    monkeypatch.setattr(capabilities, "PrivateServer", lambda *a, **k: _ProbeServer(zstd, configs, settable, *a, **k))
    overlays = (StartupOverlay("o", InstanceSelector.parse({}), overlay),) if overlay else ()
    plan = unit_plan(document={"mode": "standalone", "valkey_binary": str(binary)}, startup_overlays=overlays)
    config = {
        "deployment_config": ["rdbcompression"],
        "startup_config": {"bgsave-default-method": "forkless"},
        "config_get": {"bgsave-default-method": ["forkless"]},
        "info_fields": {"persistence": ["rdb_current_bgsave_type"]},
        **probe,
    }
    spec = resolved(
        ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.build-probe", id="b", config=config)
    )
    result = capabilities.BuildProbe().probe(FakeContext(plan=plan), spec)
    assert result.status.value == status, result.summary
    assert summary in result.summary


@pytest.mark.parametrize("written", ["bgsave-default-method", "forkless-policy"])
@pytest.mark.parametrize("knows", ["bgsave-default-method", "forkless-policy"])
def test_the_build_probe_reads_and_sets_a_renamed_config_by_the_name_the_build_knows(
    monkeypatch, tmp_path, written, knows
):
    forkless = {"bgsave-default-method": "forkless", "forkless-policy": "prefer-forkless"}
    # The build runs with forkless saves under the name it knows, as PrivateServer.start gives it.
    monkeypatch.setattr(
        capabilities,
        "PrivateServer",
        lambda binary, *_, **__: _ProbeServer(True, {knows: forkless[knows]}, {knows: (forkless[knows],)}, binary, []),
    )
    config = {"config_get": {written: [forkless[written]]}, "config_set": {written: [forkless[written]]}}
    spec = resolved(
        ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.build-probe", id="b", config=config)
    )
    status, summary, facts = capabilities.probe_binary(
        FakeContext(), spec, str(valkey_build(tmp_path, knows)), spec.config
    )
    assert status.value == "PASS", summary
    assert facts["config"] == {knows: forkless[knows]}


def test_a_probe_of_a_binary_that_is_not_executable_says_so(tmp_path):
    binary = tmp_path / "valkey-server"
    binary.write_bytes(b"")
    spec = resolved(ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.build-probe", id="b"))
    status, summary, _ = capabilities.probe_binary(FakeContext(), spec, str(binary), spec.config)
    assert (status.value, summary) == (
        "ERROR",
        f"Configured valkey-server binary '{binary}' exists but is not executable.",
    )


# valkey.oracle.rdb-snapshot


def _bgsave(tmp_path, instance_id="valkey-0", data=_PLAIN, sent_at=10.0, captured=True):
    path = tmp_path / f"{instance_id}.rdb"
    path.write_bytes(data)
    value = {
        "instance_id": instance_id,
        "role": "primary",
        "status": "ok" if captured else "err",
        "reply": "Background saving started",
        "sent_at": sent_at,
        "replied_at": sent_at + 0.01,
        "finished_at": sent_at + 1,
    }
    if captured:
        value["rdb_artifact"] = str(path)
    return Evidence("valkey.bgsave.result", "bgsave", "action.completed", 1.0, value)


class _ShardHandle:
    """Primaries valkey-0 (slots 0-8191) and valkey-1 (8192-16383) when ``cluster``, else one standalone primary;
    ``log`` is each instance's log file."""

    def __init__(self, cluster=False, log=None):
        self.mode = SimpleNamespace(value="cluster" if cluster else "standalone")
        self.instances = tuple(
            SimpleNamespace(
                instance_id=f"valkey-{i}",
                host="127.0.0.1",
                port=7000 + i,
                shard_id=i,
                slot_start=(0, 8192)[i] if cluster else None,
                slot_end=(8191, 16383)[i] if cluster else None,
            )
            for i in range(2 if cluster else 1)
        )
        self.log = log

    def roles(self):
        return {instance.instance_id: "primary" for instance in self.instances}

    def instance(self, instance_id):
        return next(instance for instance in self.instances if instance.instance_id == instance_id)

    def binary(self, instance_id):
        return "/bin/valkey-server"

    def log_file(self, instance_id):
        return self.log


def _snapshot(monkeypatch, evidence, loads, handle=None, **config):
    """rdb-snapshot over ``evidence``; ``loads`` maps an instance to its RDB's keys, or to a ``(content, error)``."""

    def fake_load(context, spec, binary, rdb_path, timeout_s, arguments=()):
        loaded = loads[os.path.basename(rdb_path)[: -len(".rdb")]]
        if isinstance(loaded, Exception):
            raise loaded
        if isinstance(loaded, tuple):
            return loaded
        snapshot = {key: ["string", value] for key, value in loaded.items()}
        return {"values": dict(loaded), "meta": {"dbsize": len(loaded)}, "snapshot": snapshot, "log": []}, ""

    monkeypatch.setattr(snapshots, "load_rdb", fake_load)
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.ORACLE, use="valkey.oracle.rdb-snapshot", id="s", config={"bgsave": "bgsave", **config}
        )
    )
    return snapshots.RdbSnapshotOracle().evaluate(fake_context(handle or _ShardHandle()), spec, evidence)


def _loaded(*log):
    return {"values": {"k": "v"}, "meta": {"dbsize": 1}, "snapshot": {"k": ["string", "v"]}, "log": list(log)}, ""


_LZ4_LOG = ("* Loading compressed RDB (algo=lz4) from dump.rdb", "* Done loading RDB, keys loaded: 1")
_REFUSED = "exit status 1\n# Wrong signature trying to load DB from file\n# Fatal error loading the DB, check server logs. Exiting."
_RELOADED = ("* Loading compressed RDB (algo=lz4) from dump.rdb", "* DB loaded from disk: 0.010 seconds")
# appendonly yes: the restart loads the AOF, whose base RDB Valkey writes plain whatever rdbcompression is.
_AOF_RELOADED = (
    "* Reading RDB base file on AOF loading...",
    "* DB loaded from base file appendonly.aof.2.base.rdb: 0.010 seconds",
    "* DB loaded from append only file: 0.010 seconds",
)


@pytest.mark.parametrize(
    ("rdb", "load", "config", "log", "status", "message"),
    [
        (_LZ4, _loaded(*_LZ4_LOG), {"expect_codec": "lz4"}, None, "PASS", ""),
        # Known-bad: forkless + rdbcompression lz4 on unstable writes a plain RDB.
        (_PLAIN, _loaded("* Done loading RDB"), {"expect_codec": "lz4"}, None, "FAIL", "expected codec lz4, the RDB is plain"),
        # The file is right but the load did not take the compressed path.
        (_LZ4, _loaded("* Done loading RDB"), {"expect_codec": "lz4"}, None, "FAIL", "the load logged compressed codecs none"),
        (_LZ4, _loaded(*_LZ4_LOG), {"expect_codec": "lz4", "instance_reload": True}, _RELOADED, "PASS", ""),
        (_LZ4, _loaded(*_LZ4_LOG), {"expect_codec": "lz4", "instance_reload": True}, ("* Ready to accept connections tcp",), "FAIL", "no reload"),
        (_LZ4, _loaded(*_LZ4_LOG), {"expect_codec": "lz4", "instance_reload": True}, ("* DB loaded from disk: 0.001 seconds",), "FAIL", "logged compressed codecs none"),
        (_LZ4, _loaded(*_LZ4_LOG), {"expect_codec": "lz4", "instance_reload": True}, _AOF_RELOADED, "PASS", ""),
        (_LZ4, _loaded(*_LZ4_LOG), {"expect_codec": "lz4", "instance_reload": True}, (_RELOADED[0], _AOF_RELOADED[-1]), "FAIL", "reload codec none, it logged compressed codecs ['lz4']"),
        (_LZ4, (None, _REFUSED), {"expect": "refused", "refused_log": "Wrong signature"}, None, "PASS", "refused cleanly"),
        # Known-bad: the file loads, but the experiment says it must be refused.
        (_LZ4, _loaded(), {"expect": "refused"}, None, "FAIL", "expected the load to be refused, but it loaded 1 key(s)"),
        (_LZ4, (None, "exit status -11\n=== VALKEY BUG REPORT START: Cut & paste"), {"expect": "refused"}, None, "FAIL", "exited with status -11"),
        # A configuration error is not a refusal of the RDB.
        (_LZ4, (None, "exit status 1\n*** FATAL CONFIG FILE ERROR ***\nBad directive"), {"expect": "refused"}, None, "FAIL", "status 1"),
        # A load that did not finish in time says nothing about the RDB, refused or not.
        (_LZ4, snapshots.RdbLoadTimeoutError("the RDB did not finish loading within 30s"), {"expect": "refused"}, None, "ERROR", "did not finish loading"),
        (_LZ4, snapshots.RdbLoadTimeoutError("the loading server did not start: timed out"), {"expect_codec": "lz4"}, None, "ERROR", "did not start"),
        (_LZ4, (None, _REFUSED), {"expect": "refused", "refused_log": "Invalid or unsupported RDB stream envelope"}, None, "FAIL", "did not log"),
    ],
)  # fmt: skip
def test_the_snapshot_oracle_checks_the_codec_and_a_refusal(
    monkeypatch, tmp_path, rdb, load, config, log, status, message
):
    handle = None
    if log is not None:
        (tmp_path / "valkey-0.log").write_text("\n".join(log) + "\n")
        handle = _ShardHandle(log=str(tmp_path / "valkey-0.log"))
    result = _snapshot(monkeypatch, [_bgsave(tmp_path, data=rdb)], {"valkey-0": load}, handle, **config)
    assert result.status.value == status, result.summary
    assert message in result.summary


def test_an_instance_log_that_cannot_be_read_is_no_missing_reload(monkeypatch, tmp_path):
    handle = _ShardHandle(log=str(tmp_path / "missing.log"))
    config = {"expect_codec": "lz4", "instance_reload": True}
    result = _snapshot(monkeypatch, [_bgsave(tmp_path, data=_LZ4)], {"valkey-0": _loaded(*_LZ4_LOG)}, handle, **config)
    assert result.status.value == "ERROR" and "valkey-0: its log could not be read" in result.summary, result.summary


def test_a_snapshot_the_harness_could_not_capture_is_an_error_not_a_failed_save(monkeypatch, tmp_path):
    # valkey-0 saved, and copying its RDB into the report failed: Valkey did nothing wrong.
    uncaptured = _bgsave(tmp_path, captured=False)
    uncaptured = dataclasses.replace(
        uncaptured, value={**uncaptured.value, "status": "ok", "capture_error": "No space left"}
    )
    result = _snapshot(monkeypatch, [uncaptured], {})
    assert result.status.value == "ERROR" and "could not be captured (No space left)" in result.summary
    # A save Valkey reported failed is still a finding, and so is a bgsave that left no result.
    assert _snapshot(monkeypatch, [_bgsave(tmp_path, captured=False)], {}).status.value == "FAIL"
    assert _snapshot(monkeypatch, [], {}).status.value == "ERROR"
    # A save the bgsave action could not follow (no reply, no INFO) is no failed save either.
    for unseen in ("unanswered", "unavailable"):
        save = _bgsave(tmp_path, captured=False)
        save = dataclasses.replace(save, value={**save.value, "status": unseen})
        result = _snapshot(monkeypatch, [save], {})
        assert result.status.value == "ERROR" and f"save status {unseen!r}" in result.summary, result.summary


def test_a_loading_server_that_never_becomes_ready_is_an_error_not_a_failed_load(tmp_path, monkeypatch):
    """The real load_rdb with a server that never answers: the harness cannot tell whether the RDB loads."""

    clock = VirtualClock().install(monkeypatch, server)

    silent = tmp_path / "silent-server"
    silent.write_text("#!/bin/sh\nexec sleep 30\n")
    silent.chmod(0o755)
    spec = resolved(
        ComponentSpec(
            role=ComponentRole.ORACLE,
            use="valkey.oracle.rdb-snapshot",
            id="s",
            config={"bgsave": "bgsave", "load_binary": str(silent), "timeout_s": 1},
        )
    )
    result = snapshots.RdbSnapshotOracle().evaluate(fake_context(_ShardHandle()), spec, [_bgsave(tmp_path)])
    assert result.status.value == "ERROR", result.summary
    assert "did not become ready" in result.summary and clock.elapsed >= 1


class _RefusesWhileLoading:
    """A private server that answered LOADING, then refused the RDB and exited 1."""

    def __init__(self, binary, arguments, timeout_s, prefix):
        self.workdir = os.path.join(_RefusesWhileLoading.root, "server")
        os.makedirs(self.workdir, exist_ok=True)
        self.polls = 0

        def info(section):
            self.polls += 1
            raise BusyLoadingError("Valkey is loading the dataset in memory")

        self.client = SimpleNamespace(info=info)
        self.process = SimpleNamespace(poll=lambda: 1 if self.polls else None, wait=lambda: 1)

    def start(self):
        return None

    def exit_output(self):
        return "# Wrong signature trying to load DB from file\n# Fatal error loading the DB. Exiting."

    def stop(self):
        pass


def test_a_server_that_exits_while_loading_reports_its_exit_status(monkeypatch, tmp_path):
    # It answered LOADING, then refused the RDB: a clean refusal, not a load that never exited.
    _RefusesWhileLoading.root = str(tmp_path)
    monkeypatch.setattr(snapshots, "PrivateServer", _RefusesWhileLoading)
    spec = resolved(
        ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.rdb-snapshot", id="s",
                      config={"bgsave": "bgsave", "expect": "refused", "timeout_s": 5})
    )  # fmt: skip
    result = snapshots.RdbSnapshotOracle().evaluate(fake_context(_ShardHandle()), spec, [_bgsave(tmp_path)])
    assert result.status.value == "PASS", result.summary


def _set(op_id, key, value, t_end):
    return {
        "id": op_id,
        "op": "set",
        "key": key,
        "value": value,
        "outcome": "ok",
        "t_start": t_end - 0.001,
        "t_end": t_end,
    }


# Slots: m:2 hashes into 0-8191 (valkey-0's shard), m:0 into 8192-16383 (valkey-1's).
_X, _Y = "vf:writes:m:2", "vf:writes:m:0"
_SHARDS = [_set(1, _X, "x1", 10.5), _set(2, _Y, "y1", 10.6)]


@pytest.mark.parametrize(
    ("preload", "ledger", "saves", "loads", "status", "message"),
    [
        # Every target must produce a snapshot.
        ({"k": "v0"}, [], [("valkey-0", 10.0, True), ("valkey-1", 10.0, False)], {"valkey-0": {"k": "v0"}}, "FAIL", "valkey-1"),
        ({"k": "v0"}, [], [("valkey-0", 10.0, True)], {"valkey-0": {"k": "v0", "intruder": "x"}}, "FAIL", "intruder"),
        # a was written before b: a snapshot with b's new value must hold a's too.
        ({"a": "a0", "b": "b0"}, [_set(1, "a", "a1", 10.002), _set(2, "b", "b1", 10.004)], [("valkey-0", 10.0, True)], {"valkey-0": {"a": "a0", "b": "b1"}}, "FAIL", "consistent"),
        ({"a": "a0", "b": "b0"}, [_set(1, "a", "a1", 10.002), _set(2, "b", "b1", 10.004)], [("valkey-0", 10.0, True)], {"valkey-0": {"a": "a1", "b": "b0"}}, "PASS", ""),
        # Every write completed before BGSAVE was sent, so the snapshot must hold them all.
        ({"a": "a0"}, [_set(1, "a", "a1", 5.0)], [("valkey-0", 10.0, True)], {"valkey-0": {"a": "a0"}}, "FAIL", ""),
        # Each primary saves its own slots at its own moment.
        ({_X: "x0", _Y: "y0"}, _SHARDS, [("valkey-0", 10.7, True), ("valkey-1", 10.0, True)], {"valkey-0": {_X: "x1"}, "valkey-1": {_Y: "y0"}}, "PASS", ""),
        ({_X: "x0", _Y: "y0"}, _SHARDS, [("valkey-0", 10.7, True), ("valkey-1", 10.0, True)], {"valkey-0": {}, "valkey-1": {_Y: "y0"}}, "FAIL", ""),
    ],
)  # fmt: skip
def test_the_snapshot_oracle_needs_one_consistent_cut_per_shard(
    monkeypatch, tmp_path, preload, ledger, saves, loads, status, message
):
    paths = {name: str(tmp_path / f"{name}.jsonl") for name in ("preload", "ledger")}
    write_jsonl(paths["preload"], ({"key": key, "value": value} for key, value in preload.items()))
    write_jsonl(paths["ledger"], ledger)
    evidence = [
        Evidence(LEDGER_EVIDENCE, "writes", "workload.quiesced", 1.0, {f"{n}_path": p for n, p in paths.items()})
    ]
    evidence += [_bgsave(tmp_path, instance, sent_at=at, captured=captured) for instance, at, captured in saves]
    result = _snapshot(monkeypatch, evidence, loads, _ShardHandle(cluster=_X in preload), workload="writes")
    assert result.status.value == status, result.summary
    assert message in result.summary


# valkey.oracle.repl-compression

_R = "127.0.0.1:7001"
_FULL_SYNC_LOG = [
    f"* Replica {_R} asks for synchronization",
    f"* Full resync requested by replica {_R}",
    "* Starting BGSAVE for SYNC with target: replicas sockets using: normal sync, compression: lz4",
    f"* Streamed RDB transfer with replica {_R} succeeded (socket). Waiting for REPLCONF ACK",
    f"* Replication compression enabled for replica {_R} (algo=lz4)",
    f"* Synchronization with replica {_R} succeeded",
]
_REPLICA_LOG = [
    "* Full resync from primary: abc:1",
    "* receiving streamed RDB from primary with EOF to parser",
    "* Loading compressed RDB (algo=lz4) from primary replication stream",
    "* Done loading RDB, keys loaded: 3",
]


# A log a node never wrote: the file is missing.
_UNREADABLE = ("<unreadable>",)


class _ReplHandle:
    def __init__(self, tmp_path, nodes):
        self.nodes, self.tmp_path = nodes, tmp_path
        self.instances = [
            SimpleNamespace(instance_id=i, host="127.0.0.1", port=node["port"]) for i, node in nodes.items()
        ]
        for instance_id, node in nodes.items():
            if node["log"] != _UNREADABLE:
                (tmp_path / f"{instance_id}.log").write_text("\n".join(node["log"]) + "\n")

    def new_client(self, instance_id, timeout=5.0):
        node = self.nodes[instance_id]
        if node.get("unreachable"):
            raise ConnectionError("refused")
        client = SimpleNamespace(
            config_get=lambda name: {} if node["config"].get(name) is None else {name: node["config"][name]},
            info=lambda section: node["info"],
        )
        return _Managed(client)

    def log_file(self, instance_id):
        return str(self.tmp_path / f"{instance_id}.log")


class _Managed:
    def __init__(self, client):
        self.client = client

    def __enter__(self):
        return self.client

    def __exit__(self, *exc_info):
        pass


def _config(repl, diskless="yes", dual="no", rdb="yes"):
    return {
        "repl-compression": repl,
        "rdbcompression": rdb,
        "repl-diskless-sync": diskless,
        "dual-channel-replication-enabled": dual,
    }


def _pair(primary="lz4", replica="lz4", observed="lz4", primary_log=None, replica_log=None, dual=None, **entry):
    """A primary valkey-0 and its replica valkey-1; ``dual`` enables dual-channel on both with the primary's
    repl-diskless-sync set to it."""

    slave = {"ip": "127.0.0.1", "port": 7001, "state": "online", "offset": 10, "lag": 0, "type": "replica"}
    if observed != "none":
        slave.update(repl_compression=observed, repl_compressed_bytes=100, repl_uncompressed_bytes=1000)
    slave.update(entry)
    primary_config = _config(primary, *(() if dual is None else (dual, "yes")))
    return {
        "valkey-0": {
            "port": 7000,
            "config": primary_config,
            "info": {"role": "master", "connected_slaves": 1, "slave0": slave},
            "log": _FULL_SYNC_LOG if primary_log is None else primary_log,
        },
        "valkey-1": {
            "port": 7001,
            "config": _config(replica, dual="no" if dual is None else "yes"),
            "info": {"role": "slave", "master_host": "127.0.0.1", "master_port": 7000},
            "log": _REPLICA_LOG if replica_log is None else replica_log,
        },
    }


def _detached():
    nodes = _pair()
    del nodes["valkey-0"]["info"]["slave0"]
    return nodes


def _unreachable_replica():
    nodes = _pair()
    nodes["valkey-1"]["unreachable"] = True
    return nodes


def _older_replica():
    primary_log = [
        _FULL_SYNC_LOG[0],
        "* Starting BGSAVE for SYNC with target: replicas sockets using: normal sync, compression: none",
        _FULL_SYNC_LOG[-1],
    ]
    replica_log = ["* Full resync from primary: a:1", "* with EOF to disk", "* Loading RDB produced by valkey 9.0"]
    nodes = _pair("zstd", None, "none", primary_log=primary_log, replica_log=replica_log)
    nodes["valkey-0"]["config"].update({"rdbcompression": "lz4", "repl-diskless-sync": "no"})
    return nodes


@pytest.mark.parametrize(
    ("nodes", "config", "status", "message"),
    [
        (_pair(), {"full_sync": True, "min_ratio": 1.5}, "PASS", ""),
        # The primary ignores dual-channel while repl-diskless-sync is no, even when the codec forces a socket sync.
        (_pair(dual="no"), {"full_sync": True}, "PASS", ""),
        # An older replica is a plaintext link with a diskless full sync.
        (_older_replica(), {"full_sync": True, "expect_algo": "none"}, "PASS", ""),
        # INFO reports a codec the configuration does not allow.
        (_pair(replica="zstd", observed="zstd"), {}, "FAIL", "expected codec lz4"),
        # repl-compression no everywhere while the experiment expects lz4: the experiment tests nothing it names
        # (identical binaries for a mixed-version pair, say), which says nothing about Valkey.
        (_pair("no", "no", "none", primary_log=_FULL_SYNC_LOG[:1]), {"expect_algo": "lz4"}, "ERROR", "the experiment expects lz4"),
        # A plaintext link must never log enabling compression.
        (_pair("lz4", "no", "none"), {}, "FAIL", "the primary logged enabling ['lz4']"),
        (_pair(primary_log=[*_FULL_SYNC_LOG[:5], _FULL_SYNC_LOG[0]]), {}, "FAIL", "logged enabling none"),
        (_pair(repl_uncompressed_bytes=0), {}, "FAIL", "has seen no bytes"),
        (_pair(repl_uncompressed_bytes=100_000, repl_compressed_bytes=90_000), {"min_ratio": 1.5}, "FAIL", "compression ratio 1.111 (100000/90000 bytes) is below 1.5"),
        (_pair(state="wait_bgsave"), {}, "FAIL", "is not online"),
        (_pair(port=7999), {}, "FAIL", "unknown replica"),
        # A replica its primary serves but the oracle cannot reach is unreachable, not also unknown; its link was
        # never seen, so the oracle cannot judge it.
        (_unreachable_replica(), {}, "ERROR", "valkey-1 is unreachable: ConnectionError: refused"),
        (_detached(), {}, "FAIL", "not attached"),
        (_pair(primary_log=[]), {}, "FAIL", "never logged the replica's handshake"),
        # A log that cannot be read shows nothing the primary or replica did.
        (_pair(primary_log=_UNREADABLE), {}, "ERROR", "the primary's log could not be read"),
        (_pair(replica_log=_UNREADABLE), {"full_sync": True}, "ERROR", "the replica's log could not be read"),
        (_pair(primary_log=[line.replace("compression: lz4", "compression: none") for line in _FULL_SYNC_LOG]), {"full_sync": True}, "FAIL", "expected a full sync"),
        (_pair(dual="yes"), {"full_sync": True}, "FAIL", "expected a full sync {'target': 'replicas sockets', 'using': 'dual-channel'"),
        (_pair(primary_log=_FULL_SYNC_LOG[:5]), {"full_sync": True}, "FAIL", "no full sync"),
        (_pair(replica_log=[*_REPLICA_LOG[:2], "* Done loading RDB"]), {"full_sync": True}, "FAIL", "should be in codec lz4"),
        ({}, {}, "FAIL", "fewer than 1"),
    ],
)  # fmt: skip
def test_repl_compression_fails_when_the_evidence_contradicts_the_model(tmp_path, nodes, config, status, message):
    config = {"converge_timeout_s": 0, **config}
    spec = resolved(
        ComponentSpec(role=ComponentRole.ORACLE, use="valkey.oracle.repl-compression", id="c", config=config)
    )
    result = ReplCompressionOracle().evaluate(fake_context(_ReplHandle(tmp_path, nodes)), spec, ())
    assert result.status.value == status, result.summary
    assert message in result.summary


def _codec_handle(codecs, links_up):
    """A primary (valkey-0) and two replicas, all set to ``repl-compression zstd``; each replica's link
    still reports the codec in ``codecs``."""

    instances = [SimpleNamespace(instance_id=f"valkey-{n}", host="127.0.0.1", port=11000 + n) for n in range(3)]
    replicas = {f"slave{n - 1}": {"ip": "127.0.0.1", "port": 11000 + n, "state": "online"} for n in (1, 2)}
    for n in (1, 2):
        replicas[f"slave{n - 1}"]["repl_compression"] = codecs[n - 1]

    def info(instance_id):
        return (
            replicas
            if instance_id == "valkey-0"
            else {"master_link_status": "up" if instance_id in links_up else "down"}
        )

    class Client:
        def __init__(self, instance_id):
            self.instance_id = instance_id

        def __enter__(self):
            return SimpleNamespace(config_get=lambda name: {name: "zstd"}, info=lambda section: info(self.instance_id))

        def __exit__(self, *exc):
            return False

    return SimpleNamespace(instances=instances, is_alive=lambda _: True, new_client=lambda i, timeout=None: Client(i))


@pytest.mark.parametrize(
    ("codecs", "links_up", "unsettled"),
    [
        (("lz4", "lz4"), ("valkey-1", "valkey-2"), ["valkey-1", "valkey-2"]),
        (("zstd", "zstd"), ("valkey-1", "valkey-2"), []),
        # A replica whose link is down reconnects with the new codec: nothing to wait for.
        (("lz4", "zstd"), ("valkey-2",), []),
    ],
)
def test_a_link_still_on_its_old_codec_is_unsettled_until_it_reconnects(codecs, links_up, unsettled):
    problems = unsettled_links(_codec_handle(codecs, links_up))
    assert problems == [f"valkey-0 -> {replica} uses lz4, its settings negotiate zstd" for replica in unsettled]
