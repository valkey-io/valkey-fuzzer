"""Every Valkey experiment is judged by liveness, a local binary is the one the scenario names, and disk-fill
only ever sees a tmpfs that a write would reach."""

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.components import experiments, providers
from valkey_fuzzer.components.providers import ENVIRONMENT_PROVIDERS, LocalBinaryProvider
from valkey_fuzzer.plan import RunMode
from valkey_fuzzer.schema import ScenarioCompileError

REPOSITORY = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def runtime():
    return valkey_service()


@pytest.mark.parametrize("experiment", experiments(), ids=lambda experiment: experiment.experiment_id)
def test_every_experiment_is_judged_by_liveness_and_one_without_required_inputs_refuses_too_few_replicas(
    runtime, experiment
):
    # test_generator compiles each experiment's generated scenarios.
    assert experiment.mode is RunMode.EXPERIMENT and experiment.required_evidence
    liveness = [component for component in experiment.components if component.use == "valkey.oracle.liveness"]
    assert len(liveness) == 1 and liveness[0].required
    if "replicas_per_primary" in experiment.deployment_requirements and not experiment.inputs.get("required"):
        deployment = {"mode": experiment.deployment_modes[0], "replicas_per_primary": 0}
        scenario = {
            "scenario_id": "check",
            "seed": 1,
            "experiment": {"id": experiment.experiment_id},
            "deployment": deployment,
        }
        with pytest.raises(ScenarioCompileError, match=r"replicas_per_primary must be at least 1, got 0"):
            runtime.compile(scenario)


def test_a_primary_killed_mid_save_in_cluster_mode_is_never_failed_over(runtime):
    # A failover after the restart would lose what the restarted primary acknowledged until it was replaced.
    scenario = {
        "scenario_id": "check",
        "seed": 1,
        "experiment": {"id": "valkey.forkless-save-crash"},
        "deployment": {"mode": "cluster", "primaries": 3, "replicas_per_primary": 1},
    }
    instances = to_plain_data(runtime.compile(scenario).instances)
    replicas = [instance["server_config"] for instance in instances.values() if instance["role"] == "replica"]
    assert len(replicas) == 3 and all(config["cluster-replica-no-failover"] == "yes" for config in replicas)


def test_mounted_tmpfs_bytes_reads_only_a_tmpfs_a_write_would_reach(tmp_path, monkeypatch):
    mount_point = tmp_path / "a disk"
    mount_point.mkdir()
    escaped = os.path.realpath(mount_point).replace(" ", "\\040")
    mounts = tmp_path / "mounts"
    monkeypatch.setattr(providers, "_PROC_MOUNTS", str(mounts))
    mounts.write_text(f"/dev/sda1 / ext4 rw 0 0\ntmpfs {escaped} tmpfs rw,size=2048k 0 0\n")
    stats = os.statvfs(mount_point)
    assert providers.mounted_tmpfs_bytes(str(mount_point)) == stats.f_blocks * stats.f_frsize
    assert providers.mounted_tmpfs_bytes(str(tmp_path)) is None
    # Mounted over by a disk: the tmpfs underneath is not what a write reaches.
    mounts.write_text(f"tmpfs {escaped} tmpfs rw 0 0\n/dev/sdb1 {escaped} ext4 rw 0 0\n")
    assert providers.mounted_tmpfs_bytes(str(mount_point)) is None


def test_a_local_binary_is_fingerprinted_and_must_be_the_version_the_scenario_names(tmp_path):
    binary = tmp_path / "valkey-server"
    binary.write_text(f"#!{sys.executable}\nprint('Valkey server v=9.1.0 sha=abc12345:0 malloc=libc bits=64')\n")
    binary.chmod(0o755)
    registration = next(item for item in ENVIRONMENT_PROVIDERS if item.provider_id == "valkey.artifact.local-binary")
    spec = SimpleNamespace(provider_id=registration.provider_id, id="server")

    def resolve(**config):
        return LocalBinaryProvider().acquire(None, spec, registration.config.resolve(config, "with"))

    artifact = resolve(path=str(binary), expected_version="9.1.0", expected_sha="abc1")
    assert artifact.outputs == {"binary": str(binary)}
    assert (artifact.provenance["version"], artifact.provenance["git_sha"]) == ("9.1.0", "abc12345")
    with pytest.raises(ValueError, match=r"expected '8\.1\.4'"):
        resolve(path=str(binary), expected_version="8.1.4")
    with pytest.raises(ValueError, match="does not match expected_sha"):
        resolve(path=str(binary), expected_sha="ffff")
    with pytest.raises(FileNotFoundError):
        resolve(path=str(tmp_path / "missing"))
