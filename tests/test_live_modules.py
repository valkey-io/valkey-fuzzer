"""Modules against real servers: Valkey's own test modules (``tests/modules`` of the build under test) loaded by a
deployment, its module data reloaded from an RDB and digested on primaries and replicas.

Needs a valkey-server (``VALKEY_FUZZER_BINARY`` or ``PATH``) and ``VALKEY_FUZZER_TEST_MODULES``, the
``tests/modules`` directory of that build, built (``make -C tests/modules``).
"""

import base64
import os

import pytest
import yaml

from tests.live_support import BINARY, check, evidence, run_scenario, scenario
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.components.server import PrivateServer
from valkey_fuzzer.schema import BASE64_KEY

pytestmark = [pytest.mark.live, pytest.mark.slow]

TEST_MODULES = os.environ.get("VALKEY_FUZZER_TEST_MODULES")
if not BINARY or not TEST_MODULES:
    pytest.skip("set VALKEY_FUZZER_TEST_MODULES to a built tests/modules of the valkey-server", allow_module_level=True)


DATATYPE = os.path.join(TEST_MODULES or "", "datatype.so")


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
