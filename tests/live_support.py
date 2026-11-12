"""What the live tests share: the builds under test, free port blocks, and running and checking a scenario.

``VALKEY_FUZZER_BINARY`` (or ``valkey-server`` on PATH) is the build under
test; ``VALKEY_FUZZER_OLD_BINARY`` is an older release for cross-version tests.
"""

import os
import shutil
import time
from pathlib import Path

import pytest
import yaml

from tests.valkey_experiments_support import evidence_values, valkey_service
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.components import EXPERIMENT_DIRECTORY
from valkey_fuzzer.deployment.local.ports import highest_base_port, port_unavailable_reason
from valkey_fuzzer.differential import Differential, run_differential

BINARY = os.environ.get("VALKEY_FUZZER_BINARY") or shutil.which("valkey-server")
OLD_BINARY = os.environ.get("VALKEY_FUZZER_OLD_BINARY")
needs_old_binary = pytest.mark.skipif(not OLD_BINARY, reason="set VALKEY_FUZZER_OLD_BINARY to an older build")
EXAMPLES = Path(__file__).resolve().parents[1] / "examples" / "valkey"


# Live runs take their ports in order from one cursor, wrapping around once it reaches the host's ephemeral
# range, so two runs never share a block.
_FIRST_PORT = 8000
_next_port = _FIRST_PORT


def free_port_block(width: int) -> int:
    """The next ``width`` client ports whose cluster bus ports are free too, all below this host's ephemeral port
    range: an outgoing connection there can take a port before Valkey binds it."""

    global _next_port
    highest = highest_base_port(width, cluster=True)
    for _ in range(2):
        while _next_port <= highest:
            base, _next_port = _next_port, _next_port + width
            ports = [base + offset for offset in range(width)]
            if all(port_unavailable_reason("127.0.0.1", port) is None for port in ports + [p + 10000 for p in ports]):
                return base
        _next_port = _FIRST_PORT
    raise RuntimeError(f"no free {width}-port block from {_FIRST_PORT} to {highest}")


def free_base_port() -> int:
    return free_port_block(40)


def scenario(experiment, mode="standalone", inputs=None, *, replicas=None, seed=7, binary=None, **deployment):
    """A live scenario of ``experiment`` on a ``mode`` deployment with ``replicas`` per primary, on free ports."""

    if replicas is not None:
        deployment["replicas_per_primary"] = replicas
    return {
        "scenario_id": f"live-{experiment.split('.')[-1]}-{mode}",
        "seed": seed,
        "experiment": {"id": experiment, "inputs": inputs or {}},
        "deployment": {"mode": mode, "base_port": free_base_port(), "valkey_binary": binary or BINARY, **deployment},
    }


def run_scenario(runtime, scenario):
    """Run ``scenario``, for 10 minutes at most."""

    return runtime.runner.run(runtime.compile(scenario), deadline=time.monotonic() + 600)


def explain(outcome) -> str:
    lines = [f"{outcome.status.value}: {outcome.summary}"]
    return "\n".join(
        lines + [f"  {item.component_id}: {item.status.value} {item.summary}" for item in outcome.component_results]
    )


def evidence(outcome, evidence_id):
    """The value of the first ``evidence_id`` evidence of ``outcome``."""

    return evidence_values(outcome.all_evidence, evidence_id)[0]


def check(outcome, status="PASS", *, failed=(), missing=(), says=(), fields=None):
    """``outcome`` has ``status``, its ``failed`` components FAILed, its ``missing`` required evidence was not
    observed, its explanation says each of ``says``, and its evidence has ``fields`` (evidence ID -> values)."""

    assert outcome.status.value == status, explain(outcome)
    statuses = {item.component_id: item.status.value for item in outcome.component_results}
    assert all(statuses.get(component) == "FAIL" for component in failed), explain(outcome)
    observed = {item["evidence_id"] for item in to_plain_data(outcome.diagnostics).get("missing_required_evidence", ())}
    assert set(missing) <= observed, explain(outcome)
    assert all(text in explain(outcome) for text in says), explain(outcome)
    for evidence_id, values in (fields or {}).items():
        value = evidence(outcome, evidence_id)
        assert all(value[key] == wanted for key, wanted in values.items()), (evidence_id, value)


def variant_runtime(tmp_path, experiment_file, variant, edit):
    """A run service with one experiment copied into an experiment directory, edited, and named ``<id>-<variant>``."""

    document = yaml.safe_load((EXPERIMENT_DIRECTORY / experiment_file).read_text(encoding="utf-8"))
    document["id"] = f"{document['id']}-{variant}"
    edit(document)
    directory = tmp_path / "experiments"
    directory.mkdir(exist_ok=True)
    (directory / experiment_file).write_text(yaml.safe_dump(document), encoding="utf-8")
    return valkey_service(experiment_directories=(str(directory),), artifact_dir=str(tmp_path / "artifacts"))


def builds(scenario, baseline, candidate):
    """``scenario`` with the ``baseline`` and ``candidate`` binaries as its artifacts."""

    scenario["artifacts"] = {
        name: {"use": "valkey.artifact.local-binary", "with": {"path": path}}
        for name, path in (("baseline", baseline), ("candidate", candidate))
    }
    return scenario


def differential(runtime, document):
    def execute(name, scenario, plan):
        return runtime.runner.run(plan, deadline=time.monotonic() + 600), None

    return run_differential(
        Differential.from_document(document),
        compile_fn=runtime.compile,
        execute_fn=execute,
        components=runtime.catalog.components,
    )
