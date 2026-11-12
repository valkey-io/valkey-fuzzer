"""Module support without Valkey processes: a module type's digest from its DUMP payload, the determinism probe
of ``valkey.oracle.dataset-digest``, the deployment's modules on every private server that holds its data, and
``valkey.artifact.local-module``."""

import base64
import dataclasses
import hashlib
import os
from types import SimpleNamespace
from typing import ClassVar

import pytest

from tests.test_valkey_components import FakeHandle, _spec
from tests.valkey_experiments_support import evidence_values, fake_context, resolved, unit_plan, valkey_service
from valkey_fuzzer.components import capabilities, providers, snapshots
from valkey_fuzzer.components.keyspace import canonical_value, keyspace_digest, keyspace_snapshot
from valkey_fuzzer.components.snapshots import NONDETERMINISTIC, DatasetDigestOracle
from valkey_fuzzer.environment import EnvironmentProviderRegistry, EnvironmentProvisioner, EnvironmentSpec
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.provenance import artifact_digests, replayed_drift
from valkey_fuzzer.references import EnvironmentKind, parse_environment_reference
from valkey_fuzzer.results import Evidence
from valkey_fuzzer.service import with_artifact_paths
from valkey_fuzzer.testing import FakeContext

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


_PLAIN = b"VALKEY081\xfa" + b"\x00" * 16


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


# valkey.artifact.local-module


def _acquire(tmp_path, path, run="r1"):
    """Acquire ``artifacts.module`` from ``path`` with the real provider; the session."""

    registry = EnvironmentProviderRegistry()
    registry.register_many(providers.ENVIRONMENT_PROVIDERS)
    spec = EnvironmentSpec(EnvironmentKind.ARTIFACT, "module", providers.LOCAL_MODULE_PROVIDER, config={"path": path})
    return EnvironmentProvisioner(registry=registry, artifact_root=tmp_path / run).acquire(
        (spec,), (), run_id=run, seed=1, deadline=None
    )


def test_a_local_module_is_its_absolute_path_and_its_sha256_is_compared_on_replay(tmp_path, monkeypatch):
    module = tmp_path / "libjson.so"
    module.write_bytes(b"build one")
    monkeypatch.chdir(tmp_path)

    session = _acquire(tmp_path, "libjson.so")
    assert session.resolve(parse_environment_reference("artifacts.module.path")) == str(module)
    recorded = {"diagnostics": {"environment_report": session.report.to_data()}}
    session.close()
    assert artifact_digests(recorded) == {"artifacts.module": hashlib.sha256(b"build one").hexdigest()}

    # A replay that loads another build of the module drifts, as a binary artifact does.
    module.write_bytes(b"build two")
    session = _acquire(tmp_path, str(module), run="r2")
    replayed = {"diagnostics": {"environment_report": session.report.to_data()}}
    session.close()
    context = FakeContext()
    outcome = SimpleNamespace(valkey_sha=None, to_data=lambda: replayed)
    drift = replayed_drift(
        valkey_service().catalog, context.plan, {"reproducer": {}, "outcome": recorded}, None, outcome
    )
    assert [entry["item"] for entry in drift] == ["artifacts.module sha256"]


def test_a_local_module_that_is_not_a_file_fails_its_acquisition(tmp_path):
    with pytest.raises(Exception, match=r"module .*missing\.so is not a readable file"):
        _acquire(tmp_path, str(tmp_path / "missing.so"))


# valkey.capability.build-probe: commands a module brings


class _ModuleProbeServer:
    """A probe server that implements JSON.GET only when started with ``--loadmodule /m/libjson.so``, and exits at
    startup with any other module."""

    def __init__(self, binary, arguments, **_):
        self.modules = [value for name, value in zip(arguments[::2], arguments[1::2]) if name == "--loadmodule"]
        self.workdir = "/nonexistent/probe"
        commands = {"json.get": [], "get": []} if "/m/libjson.so" in self.modules else {"get": []}

        def execute_command(*args):
            name = args[2].lower()
            if name not in commands:
                raise TypeError("cannot parse a nil entry")
            return {name: commands[name]}

        info = {"valkey_version": "9.0.0", "valkey_git_sha1": "abc"}
        self.client = SimpleNamespace(info=lambda section: info, execute_command=execute_command)

    def start(self):
        unknown = [module for module in self.modules if module != "/m/libjson.so"]
        return f"Module {unknown[0]} failed to load: no such file" if unknown else None

    def stop(self):
        pass


@pytest.mark.parametrize(
    ("loadmodule", "deployment_config", "status", "summary"),
    [
        ("/m/libjson.so", ["loadmodule"], "PASS", "every binary supports"),
        # A deployment that does not load the module lacks its commands: unsupported.
        (None, ["loadmodule"], "SKIP", "started with it, the build does not implement command 'JSON.GET'"),
        # Without deployment_config the commands are checked on the probe's own server, which loads no module.
        ("/m/libjson.so", [], "SKIP", "build does not implement command 'JSON.GET'"),
        # A module that does not load is a setup error, never a skip.
        ("/m/other.so", ["loadmodule"], "ERROR", "probe server exited: Module /m/other.so failed to load"),
    ],
    ids=["loaded", "not-loaded", "own-server", "load-fails"],
)
def test_the_build_probe_checks_commands_on_the_server_the_deployment_config_starts(
    monkeypatch, tmp_path, loadmodule, deployment_config, status, summary
):
    binary = tmp_path / "valkey-server"
    binary.write_bytes(b"")
    binary.chmod(0o755)
    monkeypatch.setattr(capabilities, "PrivateServer", _ModuleProbeServer)
    server_config = {} if loadmodule is None else {"loadmodule": loadmodule}
    plan = unit_plan(document={"mode": "standalone", "valkey_binary": str(binary), "server_config": server_config})
    config = {"commands": ["JSON.GET"], "deployment_config": deployment_config}
    spec = resolved(
        ComponentSpec(role=ComponentRole.CAPABILITY, use="valkey.capability.build-probe", id="b", config=config)
    )

    result = capabilities.BuildProbe().probe(FakeContext(plan=plan), spec)

    assert result.status.value == status, result.summary
    assert summary in result.summary


def test_an_artifact_override_resolves_a_local_module_elsewhere():
    module = {"use": "valkey.artifact.local-module", "with": {"path": "/ci/module-src/build/src/libjson.so"}}
    scenario = {"scenario_id": "s", "artifacts": {"module": module}}

    overridden = with_artifact_paths(scenario, {"module": "/here/libjson.so"})

    assert overridden["artifacts"]["module"] == {
        "use": "valkey.artifact.local-module",
        "with": {"path": "/here/libjson.so"},
    }


class BytesClient:
    """A client that does not decode: keys, types and values come back as bytes, as the digest oracle reads."""

    def __init__(self, hashes):
        self.hashes = hashes

    def type(self, key):
        return b"hash" if key in self.hashes else b"none"

    def hgetall(self, key):
        return dict(self.hashes[key])

    def scan_iter(self, match=None, count=1000):
        return iter(sorted(self.hashes))


def test_binary_values_are_read_exactly_not_decoded():
    # valkey-search's vectors are FLOAT32 bytes in a hash: the digest oracle could not read them (UnicodeDecodeError).
    vector = b"\x00\x00\x80?\xc3\x28\x00\x00"
    snapshot = keyspace_snapshot(BytesClient({b"d:1": {b"v": vector, b"tag": b"t1"}}))
    assert snapshot == {"d:1": ["hash", [["tag", "t1"], ["v", {"$base64": base64.b64encode(vector).decode()}]]]}
    other = keyspace_snapshot(BytesClient({b"d:1": {b"v": vector[:-1] + b"\x01", b"tag": b"t1"}}))
    assert keyspace_digest(other) != keyspace_digest(snapshot)
