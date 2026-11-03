"""The register bundle end to end on a real valkey-server: translate, validate, run, conform.

The bundle (``tests/fixtures/spec/register``) brings its own data-only
experiment: its negative control's counterexample becomes scenarios, they run
on Valkey, and the runs are conformed back to the model. Needs java and
``$TLA2TOOLS_JAR``.
"""

import json
import os
import shutil
from pathlib import Path

import pytest
import yaml

from tests.live_support import BINARY
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.deployment.local.ports import port_unavailable_reason

pytestmark = [pytest.mark.live, pytest.mark.slow]

JAR = os.environ.get("TLA2TOOLS_JAR")
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "spec"
EXPERIMENTS = FIXTURES / "register" / "experiments"
# The base port the register mapping's scenario section sets.
PORT = 7940

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)
if not (JAR and os.path.exists(JAR) and shutil.which("java")):
    pytest.skip("needs java and TLA2TOOLS_JAR", allow_module_level=True)


def _cli(*argv):
    args = create_parser().parse_args([str(item) for item in argv])
    return args.handler(args)


def _run(scenario, artifacts):
    assert _cli(
        "run", scenario, "--experiment-dir", EXPERIMENTS, "--valkey-binary", BINARY, "--artifact-dir", artifacts
    ) in (0, 1)
    (report,) = artifacts.glob(f"runs/*-{yaml.safe_load(scenario.read_text())['scenario_id']}-*/report.json")
    return json.loads(report.read_text())


def test_the_register_counterexample_runs_on_valkey_and_conforms(tmp_path, monkeypatch, capsys):
    if port_unavailable_reason("127.0.0.1", PORT):
        pytest.skip(f"port {PORT} is in use")
    monkeypatch.setenv("VALKEY_FUZZER_SPEC_PATH", str(FIXTURES))
    scenarios = tmp_path / "scenarios"
    artifacts = tmp_path / "artifacts"
    # translate: TLC finds the counterexample of ack-before-save (acknowledge before save, then crash).
    assert _cli("spec", "translate", "register", "--control", "ack-before-save", "--out", scenarios) == 0
    direct = scenarios / "spec-register-ack-before-save.yaml"
    assert yaml.safe_load(direct.read_text())["experiment"]["inputs"] == {"appendonly": "no"}
    files = sorted(scenarios.glob("*.yaml"))
    assert _cli("validate", *files, "--experiment-dir", EXPERIMENTS) == 0

    # run: Valkey without appendonly loses the acknowledged writes, as the model says it may.
    lost = _run(direct, artifacts)
    assert lost["status"] == "FAIL", lost["summary"]
    data = next(item for item in lost["outcome"]["component_results"] if item["component_id"] == "data")
    assert "acknowledged write(s) lost" in data["summary"]
    # The same scenario under the claim (appendonly yes, appendfsync always) keeps every write.
    document = yaml.safe_load(direct.read_text())
    document.update(
        scenario_id="register-synced", experiment={**document["experiment"], "inputs": {"appendonly": "yes"}}
    )
    (tmp_path / "synced.yaml").write_text(yaml.safe_dump(document, sort_keys=False))
    kept = _run(tmp_path / "synced.yaml", artifacts)
    assert kept["status"] == "PASS", kept["summary"]

    # conform: both runs map onto the model; the loss is outside the claim, so it is no failure.
    capsys.readouterr()
    output = tmp_path / "conform.json"
    assert _cli("spec", "conform", "register", artifacts, "--group-by", "appendonly", "--output", output) == 0
    report = json.loads(output.read_text())
    runs = {run["scenario_id"]: run for run in report["runs"]}
    lost = runs["spec-register-ack-before-save"]["properties"]["AckedDurable"]
    assert (lost["status"], lost["claimed"], lost["outside_claim"]) == ("violated", False, {"SyncBeforeAck": "FALSE"})
    assert lost["first"]["fields"] == {"keys": 0} and lost["states"] and "previous" in lost
    assert runs["register-synced"]["properties"]["AckedDurable"] == {"status": "holds", "claimed": True}
    assert report["action_coverage"]["observed"] == {"Ack": 2, "Crash": 2, "Write": 2}
    assert report["status"] == "pass"
