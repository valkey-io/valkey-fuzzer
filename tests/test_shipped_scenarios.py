"""Every shipped example (each differential variant, each distinct batch scenario) compiles and passes preflight,
and the run service example script validates one."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.batch import BatchOptions, expand, load_spec
from valkey_fuzzer.differential import Differential

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
DOCUMENTS = sorted([*EXAMPLES.glob("*.yaml"), *(EXAMPLES / "valkey").glob("*.yaml")])


@pytest.fixture(scope="module")
def runtime():
    return valkey_service(experiment_directories=[str(EXAMPLES / "experiments")])


@pytest.mark.parametrize("path", DOCUMENTS, ids=lambda path: str(path.relative_to(EXAMPLES)))
def test_every_shipped_example_compiles_and_passes_preflight(runtime, path):
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    scenarios = [document]
    if "differential_id" in document:
        differential = Differential.from_document(document)
        scenarios = [differential.scenario(variant) for variant in differential.variants]
    elif "sources" in document:
        options, sources = load_spec(str(path))
        jobs = expand(sources, runtime, BatchOptions(**options)).jobs
        found = [job["scenario"] for job in jobs if "scenario" in job]
        assert found and len({scenario["seed"] for scenario in found}) == len(found), f"{path} repeats a seed"
        distinct = {
            repr(sorted((k, v) for k, v in item.items() if k not in ("seed", "scenario_id"))): item for item in found
        }
        scenarios = list(distinct.values())
    for scenario in scenarios:
        runtime.validate_plan(runtime.compile(scenario))


def test_the_run_service_example_validates_a_scenario(monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location("run_fuzzer", EXAMPLES / "run_fuzzer.py")
    run_fuzzer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(run_fuzzer)
    scenario = EXAMPLES / "standalone_scenario.yaml"
    monkeypatch.setattr(sys, "argv", ["run_fuzzer.py", "validate", str(scenario)])

    assert run_fuzzer.main() == 0
    assert json.loads(capsys.readouterr().out)["scenario_id"] == "standalone-deployment-smoke"
