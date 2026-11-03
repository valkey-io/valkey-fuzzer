"""Each Valkey experiment passes on a real deployment, a broken variant of it fails, and one missing a
prerequisite is unsupported.

Needs a valkey-server (``VALKEY_FUZZER_BINARY`` or ``PATH``); forkless rows need
a build with forkless save, old-binary rows ``VALKEY_FUZZER_OLD_BINARY``. One
experiment runs per scenario, so each row picks one topology, and the rows
together cover standalone, replicated, and cluster deployments.
"""

import os
import subprocess

import pytest

from tests import live_support as live
from tests.live_support import BINARY, OLD_BINARY, check, evidence, run_scenario, scenario
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.components import capabilities, providers

pytestmark = [pytest.mark.live, pytest.mark.slow]

if not BINARY:
    pytest.skip("valkey-server is not available", allow_module_level=True)

FORKLESS = live.build_accepts(
    BINARY,
    {"forkless-infrastructure-enabled": "yes", "bgsave-default-method": "forkless"},
    config_get={"bgsave-default-method": ["forkless"]},
    info_fields={"persistence": ["rdb_current_bgsave_type"]},
)
needs_forkless = pytest.mark.skipif(not FORKLESS, reason="valkey-server build lacks forkless save")
PARTITIONS = capabilities.iptables_supported()[0] and capabilities.per_instance_loopback_supported()[0]
ELECTION = "valkey.cluster-election-durability"


@pytest.fixture
def runtime(tmp_path):
    return valkey_service(artifact_dir=str(tmp_path / "artifacts"))


def _two_builds(experiment, mode, inputs=None, *, on, replicas=None):
    """``experiment`` with the old build as ``baseline``, the build under test as ``candidate``, starting on ``on``."""

    built = live.builds(scenario(experiment, mode, inputs, replicas=replicas), OLD_BINARY, BINARY)
    built["deployment"]["valkey_binary"] = {"$ref": f"artifacts.{on}.binary"}
    if experiment == "valkey.mixed-version-cluster":
        built["deployment"]["instance_binaries"] = {"replica": {"$ref": "artifacts.baseline.binary"}}
    return built


def _every_wave_validated(outcome, report):
    schedule = evidence(outcome, "valkey.chaos.schedule")
    assert [wave["wave"] for wave in schedule["waves"]] == [1, 2]
    assert all(wave["validation"]["overall_success"] for wave in schedule["waves"])
    assert schedule["kills"] and all(kill["success"] for kill in schedule["kills"])
    killed = sorted(kill["instance_id"] for kill in schedule["kills"])
    assert sorted(evidence(outcome, "valkey.instances.expected-down")["instances"]) == killed
    assert "liveness: PASS" in live.explain(outcome) and "stopped on purpose" in live.explain(outcome)


def _upgraded_every_instance(outcome, report):
    restarts = [step for step in evidence(outcome, "valkey.upgrade.completed")["steps"] if step["kind"] == "restart"]
    assert len(restarts) == len(report["outcome"]["diagnostics"]["deployment"]["instances"])
    assert all(step["before"]["version"] != step["after"]["version"] for step in restarts)


_WAVES = [
    [{"op": "kill", "target": "any"}, {"op": "failover", "shard": 0, "delay_s": 0.5},
     {"op": "failover", "shard": 1, "force": True, "delay_s": 0.5}],
    [{"op": "failover", "shard": 0, "force": True}, {"op": "kill", "target": "replica", "after": 0}],
]  # fmt: skip


@pytest.mark.parametrize(
    ("build", "fields", "then"),
    [
        pytest.param(lambda: scenario("valkey.forkless-save", "cluster"),
                     {"valkey.forkless.in-progress": {}, "valkey.forkless.log-completed": {}, "valkey.rdb.reload-verified": {}},
                     None, marks=needs_forkless, id="forkless-save-cluster"),
        pytest.param(lambda: scenario("valkey.forkless-save-cancel", replicas=1), {"valkey.forkless.log-cancelled": {}},
                     None, marks=needs_forkless, id="forkless-save-cancel-replicas"),
        pytest.param(lambda: scenario("valkey.forkless-save-crash"), {"valkey.fault.injected": {"restarted": True}},
                     None, marks=needs_forkless, id="forkless-save-crash-standalone"),
        pytest.param(lambda: scenario("valkey.aof-durability", replicas=1), {}, None, id="aof-durability-replicas"),
        pytest.param(lambda: scenario("valkey.replication-partial-resync", replicas=1), {}, None, id="replication-partial-resync-replicas"),
        pytest.param(lambda: scenario("valkey.replication-full-resync", "cluster"), {}, None, id="replication-full-resync-cluster"),
        pytest.param(lambda: scenario("valkey.cluster-chaos", "cluster", {"waves": _WAVES}, primaries=3, replicas=2), {},
                     _every_wave_validated, id="cluster-chaos-cluster"),
        pytest.param(lambda: scenario(ELECTION, "cluster", {"save_behavior": "best-effort", "election": "kill_primary"}, primaries=3, replicas=2),
                     {"valkey.cluster.election-safety.verified": {"require_durable_votes": "never"}}, None, id="election-best-effort-cluster"),
        pytest.param(lambda: _two_builds("valkey.mixed-version-cluster", "cluster", on="candidate"),
                     {"valkey.binary.verified": {"mixed": True}}, None, marks=live.needs_old_binary, id="mixed-version-cluster"),
        pytest.param(lambda: _two_builds("valkey.rolling-upgrade", "standalone", {"to_binary": {"$ref": "artifacts.candidate.binary"}},
                                         on="baseline", replicas=1),
                     {}, _upgraded_every_instance, marks=live.needs_old_binary, id="rolling-upgrade-replicas"),
    ],
)  # fmt: skip
def test_an_experiment_passes_on_a_real_deployment(runtime, build, fields, then):
    built = build()
    outcome = run_scenario(runtime, built)
    check(outcome, fields=fields)
    report = runtime.run_report(outcome, scenario=built)
    if then:
        then(outcome, report)
    assert report["coverage"]["tuple"]["deployment"]["mode"] == built["deployment"]["mode"]
    assert all(item["binary_sha256"] for item in report["outcome"]["diagnostics"]["deployment"]["instances"])


def _rogue_kill(document):
    # Directory experiments do not get the shipped experiments' default liveness fragment.
    document["extends"] = ["valkey.liveness"]
    config = {"when_event": "chaos.wave-validated", "instance": "replica", "signal": "SIGKILL", "restart": False}
    document["components"].insert(0, {"use": "valkey.fault.process-signal", "id": "rogue", "config": config})


def _save_with_fork(document):
    # The overlay comes from the valkey.forkless fragment; a same-ID entry deep-merges onto it.
    document["startup_overlays"] = [
        {"overlay_id": "forkless-enabled", "server_config": {"bgsave-default-method": "fork"}}
    ]


def _without_aof(document):
    document["startup_overlays"][0]["server_config"]["appendonly"] = "no"


@pytest.mark.parametrize(
    ("experiment", "edit", "mode", "inputs", "expect"),
    [
        # A replica a fault kills on purpose is no Valkey failure, for liveness and the state check alike; the
        # bug zoo's broken builds are cluster chaos's negative controls.
        pytest.param("valkey.cluster-chaos", _rogue_kill, "cluster", {"waves": [[{"op": "failover", "shard": 0}]]},
                     {"status": "PASS", "says": ["1 stopped on purpose"]}, id="cluster-chaos-rogue-kill"),
        # The feature is configured but not exercised: the save runs with fork.
        pytest.param("valkey.forkless-save", _save_with_fork, "standalone", None,
                     {"missing": ["valkey.forkless.in-progress", "valkey.forkless.log-started"]}, marks=needs_forkless,
                     id="forkless-save-with-fork"),
        # Without AOF a crash loses acknowledged writes.
        pytest.param("valkey.aof-durability", _without_aof, "standalone", None, {"failed": ["data"], "says": ["lost"]},
                     id="aof-durability-without-aof"),
    ],
)  # fmt: skip
def test_a_variant_of_an_experiment_gets_the_verdict_its_change_calls_for(
    tmp_path, experiment, edit, mode, inputs, expect
):
    variant = edit.__name__.strip("_").replace("_", "-")
    runtime = live.variant_runtime(tmp_path, f"{experiment}.yaml", variant, edit)
    extra = {"primaries": 3} if mode == "cluster" else {}
    check(
        run_scenario(runtime, scenario(f"{experiment}-{variant}", mode, inputs, **extra)),
        **{"status": "FAIL", **expect},
    )


def _disk_full():
    built = scenario("valkey.bgsave-disk-full")
    built["resources"] = {"disk": {"use": "valkey.resource.tmpfs", "with": {"size_mb": 32}}}
    built["experiment"]["inputs"] = {"disk": {"$ref": "resources.disk.path"}}
    built["deployment"]["instance_data_roots"] = {"primary": {"$ref": "resources.disk.path"}}
    return built


def _partition():
    return scenario("valkey.cluster-partition", "cluster", addressing="per_instance_loopback")


@pytest.mark.parametrize(
    ("build", "patch"),
    [
        pytest.param(_disk_full, (providers, "tmpfs_supported", lambda: (False, "requires Linux")), id="disk-full"),
        pytest.param(_partition, (capabilities, "iptables_supported", lambda: (False, "requires iptables")), id="cluster-partition"),
    ],
)  # fmt: skip
def test_an_experiment_is_unsupported_without_its_prerequisites(runtime, monkeypatch, build, patch):
    monkeypatch.setattr(*patch)
    check(run_scenario(runtime, build()), "SKIP_UNSUPPORTED")


def test_a_mixed_version_experiment_with_one_build_was_set_up_wrong(runtime):
    # A second build is an input the operator gives, not a feature the host lacks.
    check(run_scenario(runtime, scenario("valkey.mixed-version-cluster", "cluster")), "ERROR")


# Root only: CI runs these two as root, in a step of their own (.github/workflows/valkey-experiments-live.yml).
@pytest.mark.skipif(not providers.tmpfs_supported()[0], reason=providers.tmpfs_supported()[1] or "no tmpfs")
def test_disk_full_degrades_gracefully_and_recovers(runtime):
    check(run_scenario(runtime, _disk_full()))


@pytest.mark.skipif(not PARTITIONS, reason="needs Linux root with iptables and 127.0.0.x addresses")
def test_cluster_partition_fails_over_heals_and_bounds_loss(runtime):
    check(
        run_scenario(runtime, _partition()), fields={"valkey.data.ledger-verified": {"loss_scope_slots": [[0, 5460]]}}
    )
    assert "VF-" not in subprocess.run(["iptables", "-S"], capture_output=True, text=True).stdout, "chains removed"


def test_cluster_election_with_sync_saves_every_vote_before_it_leaves_and_conforms_to_the_model(runtime):
    from valkey_fuzzer.spec import TlaTools, collect_runs, load_bundle
    from valkey_fuzzer.spec.conform import ObservedRun, check_run

    inputs = {"save_behavior": "sync", "election": "force_failover"}
    outcome = run_scenario(runtime, scenario(ELECTION, "cluster", inputs, primaries=3, replicas=2))
    check(
        outcome,
        fields={
            "valkey.cluster.election-safety.verified": {
                "require_durable_votes": "always",
                "unsaved_votes_delivered": [],
            }
        },
    )
    killed = evidence(outcome, "valkey.fault.injected")
    assert killed["disk_vars_at_kill"] is not None and killed["trigger_payload"]["candidate"]
    bundle = load_bundle("cluster-election")
    (run,) = collect_runs([evidence(outcome, "valkey.cluster.trace.recorded")["path"]])
    observed = ObservedRun(bundle.mapping, run)
    observed.replay()
    assert observed.constants()["PersistBeforeAck"] == "TRUE"
    assert {"StartElection", "HandleRequest", "ReceiveVote", "Win", "Crash"} <= set(observed.actions)
    jar = os.environ.get("TLA2TOOLS_JAR")
    if jar and os.path.exists(jar):
        assert check_run(bundle, run, TlaTools.locate(jar, workers="1"))["status"] == "pass"


_DIGESTS = [
    {"evidence_id": "valkey.dataset.digest", "source": "dataset", "field": "digest"},
    {"evidence_id": "valkey.rdb.reload-verified", "source": "snapshot", "field": "content_digest"},
    {"evidence_id": "valkey.workload.ledger", "source": "writes", "field": "replies_digest"},
]


def _differential(variants, compare):
    base = scenario("valkey.snapshot-equivalence", inputs={"operations": 1500})
    return {"differential_id": "live", "base": base, "variants": variants, "compare": compare}


_SAVE_TYPE = {"evidence_id": "valkey.bgsave.result", "source": "bgsave", "field": "after.rdb_last_bgsave_type"}


@pytest.mark.parametrize(
    ("document", "status"),
    [
        pytest.param(lambda: _differential({method: {"deployment.server_config": {"forkless-infrastructure-enabled": "yes", "bgsave-default-method": method}}
                                            for method in ("fork", "forkless")}, {"equal": _DIGESTS, "differ": [_SAVE_TYPE]}),
                     "PASS", marks=needs_forkless, id="fork-vs-forkless"),
        # Negative control: changing the generated data must break equality.
        pytest.param(lambda: _differential({"small": {}, "large": {"experiment.inputs.value_size": 65}}, {"equal": _DIGESTS[:1]}),
                     "FAIL", id="divergent-data"),
    ],
)  # fmt: skip
def test_a_differential_compares_its_variants_on_real_deployments(runtime, document, status):
    report = live.differential(runtime, document())
    assert report["status"] == status, report["summary"]
    assert status == "PASS" or report["comparisons"][0]["result"] == "mismatch"


RESTART_EXPERIMENT = """
extends: [valkey.liveness]
id: tests.restart-under-writes
deployment: {modes: [standalone]}
components:
  - {use: valkey.workload.key-value, id: writes, config: {stable_keys: 200, mutable_keys: 50, rate: 200}}
  - {use: valkey.action.sleep, id: warmup, depends_on: [writes], config: {seconds: 0.5}}
  - {use: valkey.action.restart-instance, id: restart, depends_on: [warmup], config: {instance: primary}}
  - {use: valkey.action.command, id: dbsize, depends_on: [restart], config: {instances: primary, command: [DBSIZE], expect: {min: 200}}}
  - {use: valkey.oracle.ledger, id: data, config: {workload: writes}}
required_events: [restart.completed]
required_evidence:
  - {evidence_id: valkey.instance.restarted, source: restart}
  - {evidence_id: valkey.data.ledger-verified, source: data}
"""


def test_data_only_experiment_runs_live_from_an_experiment_directory(tmp_path):
    (tmp_path / "experiments").mkdir()
    (tmp_path / "experiments" / "restart.yaml").write_text(RESTART_EXPERIMENT)
    runtime = valkey_service(experiment_directories=(str(tmp_path / "experiments"),), artifact_dir=str(tmp_path / "a"))
    check(run_scenario(runtime, scenario("tests.restart-under-writes", replicas=1)))
