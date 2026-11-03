"""valkey.replication-chaos against real valkey-server processes, with its known-bad controls.

Partition waves also need Linux, root, and iptables. Generated waves run in
tests/test_live_random_scenarios.py. Run only these with ``pytest -m live -k replication_chaos``.
"""

import pytest

from tests.live_support import BINARY, check, evidence, free_port_block, run_scenario, scenario, variant_runtime
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.components import capabilities
from valkey_fuzzer.components.workloads import KeyValueWorkload
from valkey_fuzzer.deployment.local.service import ValkeyDeploymentService

pytestmark = [pytest.mark.live, pytest.mark.slow]

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)

EXPERIMENT = "valkey.replication-chaos"
SPLIT_BRAIN = [[{"op": "promote", "target": "replica", "mode": "force", "split_s": 1.5}]]


def _chaos(waves, replicas, experiment=EXPERIMENT, **inputs):
    built = scenario(experiment, inputs={"waves": waves, **inputs}, replicas=replicas)
    built["deployment"]["base_port"] = free_port_block(10)
    return built


@pytest.fixture
def runtime(tmp_path):
    return valkey_service(artifact_dir=str(tmp_path / "artifacts"))


def test_graceful_promotions_lose_nothing_and_wait_confirmed_writes_survive_a_split_brain(runtime):
    waves = [
        [{"op": "promote", "target": "replica", "mode": "graceful"}],
        [{"op": "promote", "target": "replica", "mode": "graceful"}, {"op": "kill", "target": "replica"}],
        [{"op": "repoint", "target": "replica", "to": "replica"}, {"op": "stall", "target": "replica"}],
    ]
    outcome = run_scenario(runtime, _chaos(waves, 3, mutation_rate=400))
    check(outcome)
    assert not [item for item in outcome.all_evidence if item.evidence_id == "valkey.data.loss-window"]
    schedule = evidence(outcome, "valkey.chaos.schedule")
    assert all(wave["validation"]["overall_success"] for wave in schedule["waves"])
    assert any(record["op"] == "promote" and record["success"] for record in schedule["operations"])

    # During the split brain the old primary has no replica, so WAIT confirms none of its writes and they may
    # be lost; every write before it reached the replica that took over and must survive.
    outcome = run_scenario(runtime, _chaos(SPLIT_BRAIN, 1, wait_replicas=1, mutation_rate=300))
    check(outcome, fields={"valkey.data.ledger-verified": {"wait_confirmed_replicas": 1}})
    assert evidence(outcome, "valkey.data.ledger-verified")["loss_permitted_ops"] > 0


def _windows_off(tmp_path, monkeypatch):
    def edit(document):
        next(item for item in document["components"] if item["id"] == "data")["config"]["loss_window_faults"] = []

    return variant_runtime(tmp_path, f"{EXPERIMENT}.yaml", "windows-off", edit), f"{EXPERIMENT}-windows-off"


def _replicate_running_only(tmp_path, monkeypatch):
    # The deployment before replicate(): a stopped instance keeps its recorded role.
    original = ValkeyDeploymentService.replicate
    monkeypatch.setattr(
        ValkeyDeploymentService,
        "replicate",
        lambda self, instance_id, primary_id, timeout=5.0: (
            original(self, instance_id, primary_id, timeout) if self.is_alive(instance_id) else None
        ),
    )
    return valkey_service(artifact_dir=str(tmp_path / "artifacts")), EXPERIMENT


def _lying_wait(tmp_path, monkeypatch):
    original = KeyValueWorkload._wait

    def wait(self, client, entry):
        original(self, client, entry)
        entry["wait"] = self._config["wait_replicas"]  # claims every replica had the write

    monkeypatch.setattr(KeyValueWorkload, "_wait", wait)
    return valkey_service(artifact_dir=str(tmp_path / "artifacts")), EXPERIMENT


@pytest.mark.parametrize(
    ("setup", "waves", "replicas", "inputs", "status", "expect"),
    [
        (_windows_off, SPLIT_BRAIN, 1, {"mutation_rate": 400, "mutable_keys": 5000}, "FAIL", {"failed": ["data"], "says": ["chaos: PASS"]}),
        (_replicate_running_only, [[{"op": "kill", "target": "primary"}]], 2, {}, "FAIL", {"failed": ["chaos"], "says": ["validation failed"]}),
        (_lying_wait, SPLIT_BRAIN, 1, {"wait_replicas": 1, "mutation_rate": 300, "mutable_keys": 5000}, "FAIL", {"failed": ["data"]}),
        (None, [[{"op": "kill", "target": "valkey-1"}, {"op": "kill", "target": "valkey-1", "delay_s": 0.3}]], 1, {}, "ERROR", {"says": ["had no target"]}),
    ],
    ids=["loss-windows-off", "killed-primary-returns-as-a-second-primary", "fake-wait-reply", "every-target-already-killed"],
)  # fmt: skip
def test_known_bad_replication_chaos_does_not_pass(
    tmp_path, monkeypatch, setup, waves, replicas, inputs, status, expect
):
    runtime, experiment = (
        setup(tmp_path, monkeypatch) if setup else (valkey_service(artifact_dir=str(tmp_path)), EXPERIMENT)
    )
    check(run_scenario(runtime, _chaos(waves, replicas, experiment, **inputs)), status, **expect)


@pytest.mark.skipif(
    not (capabilities.iptables_supported()[0] and capabilities.per_instance_loopback_supported()[0]),
    reason="needs Linux root with iptables and 127.0.0.x addresses",
)
def test_a_partitioned_primary_with_a_forced_promotion_bounds_its_loss(runtime):
    waves = [[{"op": "partition", "target": "primary", "duration_s": 2.0},
              {"op": "promote", "target": "replica", "mode": "force", "delay_s": 0.5}]]  # fmt: skip
    built = _chaos(waves, 2)
    built["deployment"]["addressing"] = "per_instance_loopback"
    check(run_scenario(runtime, built))
