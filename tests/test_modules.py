"""Module support without Valkey processes: a module type's digest from its DUMP payload, the determinism probe
of ``valkey.oracle.dataset-digest``, the deployment's modules on every private server that holds its data, and
``valkey.artifact.local-module``."""

import dataclasses
import hashlib
import os
from types import SimpleNamespace
from typing import ClassVar

import pytest

from tests.test_compression_evidence import _bgsave
from tests.test_valkey_components import FakeHandle, _spec
from tests.valkey_experiments_support import evidence_values, fake_context
from valkey_fuzzer.components import snapshots
from valkey_fuzzer.components.keyspace import canonical_value, keyspace_digest
from valkey_fuzzer.components.snapshots import NONDETERMINISTIC, DatasetDigestOracle

MODULE = "/opt/modules/datatype.so"


def _payload(body: bytes, version: bytes = b"\x0b\x00", crc: bytes = b"\x01" * 8) -> bytes:
    """A DUMP payload: the type's serialization, then the RDB version and the CRC64."""

    return body + version + crc


class ModuleClient:
    """A keyspace of module values: ``keys`` maps a key to its ``(type, DUMP payload)``."""

    def __init__(self, keys):
        self.keys = dict(keys)

    def type(self, key):
        return self.keys[key][0] if key in self.keys else "none"

    def dump(self, key):
        return self.keys[key][1] if key in self.keys else None

    def scan_iter(self, match=None, count=1000):
        return iter(sorted(self.keys))

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


def test_a_module_value_is_its_type_and_the_digest_of_its_dump_payload():
    client = ModuleClient({"k": ("test___dt", _payload(b"\x07value"))})

    assert canonical_value(client, "k") == ["test___dt", hashlib.sha256(b"\x07value").hexdigest()]
    assert "<test___dt>" not in str(canonical_value(client, "k")), "the placeholder is gone"
    assert canonical_value(client, "missing") is None


def test_two_payloads_that_differ_only_in_rdb_version_and_crc_digest_the_same():
    same = ModuleClient({"a": ("t", _payload(b"v")), "b": ("t", _payload(b"v", b"\x0c\x00", b"\x02" * 8))})
    other = ModuleClient({"a": ("t", _payload(b"w"))})

    assert canonical_value(same, "a") == canonical_value(same, "b")
    assert canonical_value(same, "a") != canonical_value(other, "a")


class ProbeServer:
    """Stands in for PrivateServer in the determinism probe: each server DUMPs a RESTOREd payload as ``dumps``
    says, by the order the servers started."""

    started: ClassVar[list["ProbeServer"]] = []
    dumps = staticmethod(lambda index, payload: payload)
    exit_output = None

    def __init__(self, binary, arguments=(), *, timeout_s=15.0, prefix="x-"):
        self.binary, self.arguments, self.workdir = binary, list(arguments), f"/tmp/{prefix}{len(self.started)}"
        self.index, self.stopped, self.stored = len(ProbeServer.started), False, {}
        ProbeServer.started.append(self)
        self.client = SimpleNamespace(restore=self._restore, dump=lambda key: self.stored[key])

    def _restore(self, key, ttl, payload):
        self.stored[key] = ProbeServer.dumps(self.index, payload)

    def start(self):
        return ProbeServer.exit_output

    def stop(self):
        self.stopped = True


@pytest.fixture
def probe_servers(monkeypatch):
    ProbeServer.started = []
    ProbeServer.dumps = staticmethod(lambda index, payload: payload)
    ProbeServer.exit_output = None
    monkeypatch.setattr(snapshots, "PrivateServer", ProbeServer)
    return ProbeServer


def _module_context(handle, tmp_path, loadmodule=MODULE):
    """A context whose plan starts every instance of ``handle`` with ``loadmodule``."""

    context = fake_context(handle, artifact_dir=str(tmp_path))
    instances = {
        node.instance_id: {"role": node.role, "server_config": {"loadmodule": loadmodule}} for node in handle.instances
    }
    context.plan = dataclasses.replace(context.plan, instances=instances)
    return context


def _module_handle(keys, replica_keys=None):
    handle = FakeHandle()
    handle.binary = lambda node_id: "/opt/valkey-server"
    handle.clients = {
        "valkey-0": ModuleClient(keys),
        "valkey-1": ModuleClient(keys if replica_keys is None else replica_keys),
    }
    return handle


def _digest(context, **config):
    spec = _spec("valkey.oracle.dataset-digest", "dataset", converge_timeout_s=0, **config)
    return DatasetDigestOracle().evaluate(context, spec, ())


def test_the_digest_oracle_probes_each_module_type_on_two_servers_of_the_primarys_binary_and_modules(
    tmp_path, probe_servers
):
    keys = {"a": ("test___dt", _payload(b"1")), "b": ("test___dt", _payload(b"2")), "s": ("other", _payload(b"3"))}
    context = _module_context(_module_handle(keys), tmp_path)

    result = _digest(context)

    assert result.status.value == "PASS", result.summary
    [record] = evidence_values(result.evidence, "valkey.dataset.digest")
    assert record["module_types"] == ["other", "test___dt"]
    # Two servers per type, each of the primary's binary with the deployment's modules, each stopped.
    assert len(probe_servers.started) == 4
    for server in probe_servers.started:
        assert server.binary == "/opt/valkey-server"
        assert server.arguments == ["--loadmodule", MODULE] and server.stopped
    assert context.violations == []


def test_a_module_that_serializes_differently_in_two_processes_is_an_error_not_a_failure(tmp_path, probe_servers):
    probe_servers.dumps = staticmethod(lambda index, payload: payload[:-10] + bytes([index]) + payload[-10:])
    keys = {"a": ("test___dt", _payload(b"1"))}
    # The replica's DUMP differs too: without the probe, include_replicas would FAIL it.
    context = _module_context(_module_handle(keys, {"a": ("test___dt", _payload(b"other order"))}), tmp_path)

    result = _digest(context, include_replicas=True)

    assert result.status.value == "ERROR", result.summary
    assert f"test___dt: {NONDETERMINISTIC}, so its values cannot be compared by digest" in result.summary


def test_a_probe_server_that_does_not_start_leaves_the_digest_unjudged(tmp_path, probe_servers):
    probe_servers.exit_output = "Module /opt/modules/datatype.so failed to load"
    context = _module_context(_module_handle({"a": ("test___dt", _payload(b"1"))}), tmp_path)

    result = _digest(context)

    assert result.status.value == "ERROR"
    assert "the determinism probe's server exited at startup: Module" in result.summary


def test_a_dataset_of_builtin_types_starts_no_probe(tmp_path, probe_servers):
    handle = _module_handle({})
    context = _module_context(handle, tmp_path)

    assert _digest(context).status.value == "PASS"
    assert probe_servers.started == []


def test_the_rdb_reload_loads_the_saving_instances_modules_unless_load_config_loads_its_own(tmp_path, monkeypatch):
    calls = []

    def fake_load(context, spec, binary, rdb_path, timeout_s, arguments=()):
        calls.append(list(arguments))
        return {"values": {}, "meta": {"dbsize": 0}, "snapshot": {}, "log": []}, ""

    monkeypatch.setattr(snapshots, "load_rdb", fake_load)
    evidence = (_bgsave(tmp_path),)
    for load_config, expected in (({}, ["--loadmodule", MODULE]), ({"loadmodule": "/x.so"}, ["--loadmodule", "/x.so"])):
        spec = _spec("valkey.oracle.rdb-snapshot", "s", bgsave="bgsave", load_config=load_config)
        context = _module_context(_module_handle({}), tmp_path)
        result = snapshots.RdbSnapshotOracle().evaluate(context, spec, evidence)
        assert result.status.value == "PASS", result.summary
        assert calls.pop() == expected


def test_a_digest_listing_records_a_module_value_by_its_digest(tmp_path, probe_servers):
    keys = {"a": ("test___dt", _payload(b"1"))}
    result = _digest(_module_context(_module_handle(keys), tmp_path))

    [record] = evidence_values(result.evidence, "valkey.dataset.digest")
    expected = {"a": ["test___dt", hashlib.sha256(b"1").hexdigest()]}
    assert record["digest"] == keyspace_digest(expected) and os.path.exists(record["listing"])
