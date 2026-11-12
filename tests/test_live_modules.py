"""Modules against real servers: Valkey's own test modules (``tests/modules`` of the build under test) loaded by a
deployment, its module data reloaded from an RDB and digested on primaries and replicas; the build probe's
commands checked on the deployment's config; ``valkey.action.commands`` loading two module builds
(``valkey.artifact.local-module``), and valkey-json's ``JSON.GET`` compared as JSON.

Needs a valkey-server (``VALKEY_FUZZER_BINARY`` or ``PATH``) and ``VALKEY_FUZZER_TEST_MODULES``, the
``tests/modules`` directory of that build, built (``make -C tests/modules``). The comparison of two commits'
modules also needs ``VALKEY_FUZZER_PARENT_BINARY`` and ``VALKEY_FUZZER_PARENT_TEST_MODULES``, a parent commit's
build and its built ``tests/modules``; the valkey-json rows need ``VALKEY_FUZZER_JSON_MODULE``, a build of
valkey-json (``libjson.so``) for that valkey-server.
"""

import base64
import os
from pathlib import Path

import pytest
import yaml

from tests.live_support import BINARY, check, evidence, run_scenario, scenario
from tests.valkey_experiments_support import registration_for, valkey_service
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.components import actions
from valkey_fuzzer.components.server import PrivateServer
from valkey_fuzzer.plan import ComponentSpec
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.schema import BASE64_KEY
from valkey_fuzzer.testing import FakeContext, resolved_spec

pytestmark = [pytest.mark.live, pytest.mark.slow]

TEST_MODULES = os.environ.get("VALKEY_FUZZER_TEST_MODULES")
if not BINARY or not TEST_MODULES:
    pytest.skip("set VALKEY_FUZZER_TEST_MODULES to a built tests/modules of the valkey-server", allow_module_level=True)


DATATYPE = os.path.join(TEST_MODULES or "", "datatype.so")
PARENT = os.environ.get("VALKEY_FUZZER_PARENT_BINARY")
PARENT_MODULES = os.environ.get("VALKEY_FUZZER_PARENT_TEST_MODULES")
needs_parent = pytest.mark.skipif(
    not (PARENT and PARENT_MODULES), reason="set VALKEY_FUZZER_PARENT_BINARY and VALKEY_FUZZER_PARENT_TEST_MODULES"
)
JSON_MODULE = os.environ.get("VALKEY_FUZZER_JSON_MODULE")
needs_json = pytest.mark.skipif(not JSON_MODULE, reason="set VALKEY_FUZZER_JSON_MODULE to a valkey-json build")
EXAMPLE_EXPERIMENTS = Path(__file__).resolve().parents[1] / "examples" / "valkey" / "experiments"


def module_payloads(count):
    """DUMP payloads of ``count`` datatype.so values, made on a private server: datatype.set does not replicate
    (the module never calls ReplicateVerbatim), RESTORE of its payload does."""

    server = PrivateServer(BINARY, ["--loadmodule", DATATYPE])
    try:
        assert server.start() is None
        payloads = []
        for index in range(count):
            server.client.execute_command("datatype.set", f"k{index}", index, f"value-{index}")
            payloads.append(base64.b64encode(server.client.dump(f"k{index}")).decode("ascii"))
        return payloads
    finally:
        server.stop()


def module_data_experiment(payloads):
    """Module values RESTOREd on the primaries, saved and reloaded, and digested on every instance."""

    restores = [{"command": ["RESTORE", f"k{index}", 0, {BASE64_KEY: payload}], "expect": {"equals": "OK"}}
                for index, payload in enumerate(payloads)]  # fmt: skip
    return {
        "id": "test.module-data",
        "extends": ["valkey.liveness"],
        "deployment": {"modes": ["standalone"]},
        "metadata": {"description": "Module values saved, reloaded, and digested"},
        "components": [
            {"use": "valkey.action.commands", "id": "restore", "config": {"commands": restores}},
            {"use": "valkey.action.bgsave", "id": "bgsave", "depends_on": ["restore"]},
            {"use": "valkey.oracle.rdb-snapshot", "id": "snapshot", "config": {"bgsave": "bgsave"}},
            {"use": "valkey.oracle.dataset-digest", "id": "dataset", "config": {"include_replicas": True}},
        ],
        "required_evidence": [
            {"evidence_id": "valkey.rdb.reload-verified", "source": "snapshot"},
            {"evidence_id": "valkey.dataset.digest", "source": "dataset"},
        ],
    }


def _runtime(tmp_path, *documents):
    directory = tmp_path / "experiments"
    directory.mkdir()
    for document in documents:
        (directory / f"{document['id']}.yaml").write_text(yaml.safe_dump(document, sort_keys=False))
    return valkey_service(experiment_directories=(str(directory),), artifact_dir=str(tmp_path / "artifacts"))


def test_module_data_reloads_with_the_deployments_modules_and_digests_the_same_on_replicas(tmp_path):
    assert os.path.isfile(DATATYPE), f"{DATATYPE} is not built"
    document = scenario("test.module-data", replicas=1, server_config={"loadmodule": DATATYPE})

    outcome = run_scenario(_runtime(tmp_path, module_data_experiment(module_payloads(3))), document)

    check(outcome, "PASS")
    digest = evidence(outcome, "valkey.dataset.digest")
    assert digest["module_types"] == ["test___dt"] and digest["keys"] == 3
    assert all(report["matches_primary"] for report in digest["replicas"].values())
    reloaded = evidence(outcome, "valkey.rdb.reload-verified")
    assert reloaded["keys_loaded"] == 3


def _probing(commands):
    """An experiment whose build probe needs ``commands``, which the deployment's modules bring."""

    return {
        "id": "test.module-commands",
        "extends": ["valkey.liveness"],
        "deployment": {"modes": ["standalone"]},
        "metadata": {"description": "A build probe of commands a module brings"},
        "components": [
            {
                "use": "valkey.capability.build-probe",
                "id": "build",
                "config": {"commands": commands, "deployment_config": ["loadmodule"]},
            },
        ],
        "required_evidence": [{"evidence_id": "valkey.capability.supported", "source": "build"}],
    }


def test_the_build_probe_finds_a_modules_commands_only_where_the_deployment_loads_it(tmp_path):
    runtime = _runtime(tmp_path, _probing(["datatype.get"]))

    loaded = run_scenario(runtime, scenario("test.module-commands", server_config={"loadmodule": DATATYPE}))
    check(loaded, "PASS")
    missing = run_scenario(runtime, scenario("test.module-commands"))
    check(missing, "SKIP_UNSUPPORTED", says=["the build does not implement command 'datatype.get'"])


def run(commands, **config):
    """valkey.action.commands with BINARY as the deployment's binary; the result and its evidence value."""

    spec = ComponentSpec(
        role=ComponentRole.ACTION, use="valkey.action.commands", id="seq", config={"commands": commands, **config}
    )
    spec = resolved_spec(registration_for("valkey.action.commands"), spec)
    context = FakeContext(spec, deployment={"valkey_binary": BINARY})
    result = actions.CommandsAction().run(context, spec)
    return result, (to_plain_data(result.evidence[0].value) if result.evidence else None)


@pytest.mark.parametrize("resp", [2, 3])
def test_two_servers_of_one_build_load_their_modules(resp):
    commands = [
        {"command": ["datatype.set", "k", 1, "one"], "expect": {"equals": "OK"}},
        {"command": ["datatype.get", "k"], "expect": {"equals": [1, "one"]}},
        {"command": ["TYPE", "k"], "expect": {"equals": "test___dt"}},
    ]
    result, value = run(commands, modules=[DATATYPE], modules_parent=[DATATYPE], resp=resp)

    assert result.status.value == "PASS", result.summary
    assert value["modules"] == {"candidate": [DATATYPE], "parent": [DATATYPE]}
    assert value["candidate"]["digest"] == value["parent"]["digest"]


def test_a_module_command_the_parent_server_lacks_reached_the_change():
    # The parent server loads no datatype module: its reply to datatype.get is an unknown-command error.
    commands = [
        {"command": ["datatype.set", "k", 1, "one"], "expect_difference": True, "expect": {"equals": "OK"}},
        {"command": ["datatype.get", "k"], "expect_difference": True, "expect": {"equals": [1, "one"]}},
    ]
    other = os.path.join(TEST_MODULES, "basics.so")
    result, value = run(commands, modules=[DATATYPE], modules_parent=[other])

    assert result.status.value == "PASS", result.summary
    assert value["reached"] == [0, 1]
    assert "unknown command" in value["parent"]["replies"][0]["error"]


@needs_parent
def test_the_example_compares_two_commits_tests_modules_on_one_build(tmp_path):
    runtime = valkey_service(
        experiment_directories=[str(EXAMPLE_EXPERIMENTS)], artifact_dir=str(tmp_path / "artifacts")
    )
    inputs = {"module": {"$ref": "artifacts.module.path"}, "module_parent": {"$ref": "artifacts.module_parent.path"}}
    document = scenario("example.module-commands-compare", inputs=inputs)
    document["artifacts"] = {
        "module": {"use": "valkey.artifact.local-module", "with": {"path": DATATYPE}},
        "module_parent": {
            "use": "valkey.artifact.local-module",
            "with": {"path": os.path.join(PARENT_MODULES, "datatype.so")},
        },
    }
    outcome = run_scenario(runtime, document)

    check(outcome, "PASS")
    value = evidence(outcome, "valkey.command.sequence")
    assert value["modules"]["parent"] == [os.path.join(PARENT_MODULES, "datatype.so")]
    records = to_plain_data(outcome.diagnostics)["environment_report"]["records"]
    acquired = {record["dependency"]: record for record in records if record["phase"] == "acquire"}
    assert len(acquired["artifacts.module"]["details"]["result"]["provenance"]["sha256"]) == 64


@needs_parent
def test_a_core_change_compares_both_valkey_builds_each_with_its_own_tests_modules():
    commands = [
        {"command": ["datatype.set", "k", 1, "one"], "expect": {"equals": "OK"}},
        {"command": ["datatype.get", "k"], "expect": {"equals": [1, "one"]}},
    ]
    parent_datatype = os.path.join(PARENT_MODULES, "datatype.so")
    result, value = run(commands, compare_with=PARENT, modules=[DATATYPE], modules_parent=[parent_datatype])

    assert result.status.value == "PASS", result.summary
    assert value["modules"] == {"candidate": [DATATYPE], "parent": [parent_datatype]}


@needs_json
@pytest.mark.parametrize("resp", [2, 3])
def test_json_get_compares_as_json_on_two_servers_loading_valkey_json(resp):
    commands = [
        {"command": ["JSON.SET", "doc", "$", '{"a": 1, "b": [1, 2], "c": {"d": "x"}}'], "expect": {"equals": "OK"}},
        {"command": ["JSON.GET", "doc"], "compare": "json"},
        {"command": ["JSON.GET", "doc", "$.b", "$.c"], "compare": "json"},
        {"command": ["JSON.NUMINCRBY", "doc", "$.a", 1.5], "compare": "json"},
        {"command": ["JSON.MGET", "doc", "missing", "$.a"], "compare": "json"},
        {"command": ["TYPE", "doc"], "expect": {"equals": "ReJSON-RL"}},
    ]
    result, value = run(commands, modules=[JSON_MODULE], modules_parent=[JSON_MODULE], resp=resp)

    assert result.status.value == "PASS", result.summary
    assert value["candidate"]["digest"] == value["parent"]["digest"]


@needs_json
def test_the_build_probe_finds_json_get_where_the_deployment_loads_valkey_json(tmp_path):
    runtime = _runtime(tmp_path, _probing(["JSON.GET"]))

    check(run_scenario(runtime, scenario("test.module-commands", server_config={"loadmodule": JSON_MODULE})), "PASS")
    check(run_scenario(runtime, scenario("test.module-commands")), "SKIP_UNSUPPORTED")
