"""The local deployment's document, config layers, startup overlays, effective config, and fitting state checks."""

import copy
from pathlib import Path

import pytest
import yaml

from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.compiler import ScenarioCompileError
from valkey_fuzzer.components import EXPERIMENT_DIRECTORY
from valkey_fuzzer.deployment.local.config import deployment_config_from_document, normalize_deployment_document
from valkey_fuzzer.deployment.local.models import (
    DeploymentMode,
    InstancePlan,
)
from valkey_fuzzer.deployment.local.planner import apply_startup_overlays, plan_deployment
from valkey_fuzzer.deployment.local.processes import ProcessManager
from valkey_fuzzer.deployment.local.provider import LocalDeploymentProvider
from valkey_fuzzer.deployment.local.server_config import DEFAULT_SERVER_CONFIG, build_server_config_arguments
from valkey_fuzzer.deployment.local.validation.check import adapt_validation_config
from valkey_fuzzer.deployment.local.validation.config import validation_config_from_document
from valkey_fuzzer.plan import InstanceSelector, StartupOverlay
from valkey_fuzzer.runner import RunnerError


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"mode": "cluster", "primaries": 2}, "deployment.primaries must be at least 3"),
        ({"mode": "standalone", "base_data_dir": ""}, "base_data_dir must be a non-empty path"),
        ({"mode": "standalone", "server_config": {"repl_compression": 1, "repl-compression": 2}}, "both normalize"),
        ({"mode": "standalone", "server_config": {"bind": "0.0.0.0"}}, "managed by the local deployment"),
        # Per-instance addressing sets it so partition faults can match instance pairs.
        ({"mode": "standalone", "server_config": {"bind_source_addr": "127.0.0.9"}}, "managed by the local deployment"),
        ({"mode": "standalone", "instance_binaries": {"valkey-9": "/x"}}, "Unknown instance_binaries selector"),
        ({"mode": "standalone", "addressing": "public"}, "addressing must be one of"),
        ({"mode": "replication"}, "deployment.mode must be one of"),
        ({"mode": "cluster", "replicas_per_primary": 6}, "deployment.replicas_per_primary must be at most 5"),
        ({"mode": "standalone", "valkey_binary": None}, "valkey_binary must be a string"),
        ({"mode": "standalone", "enable_cleanup": 1}, "enable_cleanup must be a boolean"),
        ({"mode": "standalone", "instance_data_roots": {"valkey-0": ""}}, "must be a non-empty path"),
        ({"mode": "standalone", "instance_configs": {"valkey-9": {}}}, "Unknown instance_configs selector"),
        ({"mode": "standalone", "loopback_block": 256}, "deployment.loopback_block must be at most 255"),
        ({"mode": "standalone", "replicas_per_primary": 1, "base_port": 65535}, "maximum is 65535"),
    ],
)
def test_an_invalid_or_unsafe_deployment_document_is_rejected(document, message):
    with pytest.raises(ValueError, match=message):
        plan_deployment(deployment_config_from_document(document))
    if set(document) <= {"mode", "primaries", "replicas_per_primary", "num_shards"}:  # the normalizer rejects it too
        with pytest.raises(ValueError, match=message):
            normalize_deployment_document(document)


def test_config_layers_resolve_global_then_role_then_instance_with_canonical_names():
    document = {
        "mode": "standalone",
        "replicas_per_primary": 2,
        "server_config": {"repl_compression": "none", "loglevel": "warning", "save": ["900 1"]},
        "instance_configs": {
            "primary": {"--REPL-COMPRESSION": False, "loglevel": "verbose"},
            "replica": {"replica-priority": 100},
            "valkey-0": {"repl-compression": "lz4"},
            "valkey-2": {"replica-priority": 25},
        },
        "instance_binaries": {"replica": "/old", "valkey-2": "/exact"},
    }
    assert normalize_deployment_document({"mode": "cluster"}) == {
        "mode": "cluster",
        "primaries": 3,
        "replicas_per_primary": 1,
    }
    config = deployment_config_from_document(document)
    document["server_config"]["save"].append("60 1000")  # the config is a deep snapshot

    plans = plan_deployment(config)

    assert [(plan.role, plan.primary_id) for plan in plans] == [("primary", None), *[("replica", "valkey-0")] * 2]
    assert plans[0].server_config == {"repl-compression": "lz4", "loglevel": "verbose", "save": ["900 1"]}
    assert [plan.server_config.get("replica-priority") for plan in plans] == [None, 100, 25]
    assert [plan.valkey_binary for plan in plans] == [None, "/old", "/exact"]


def _plans(*configs):
    return [
        InstancePlan(f"valkey-{i}", "primary" if i == 0 else "replica", 0, 7000 + i, 17000 + i, server_config=dict(c))
        for i, c in enumerate(configs)
    ]


def test_startup_overlays_apply_in_order_to_the_instances_they_select():
    plans = _plans({"repl-compression": "none", "appendonly": False}, {}, {})  # planned configs are canonical
    replicas = InstanceSelector(roles=("replica",))

    apply_startup_overlays(
        plans,
        (
            StartupOverlay("all", server_config={"--REPL-COMPRESSION": "lz4"}),
            StartupOverlay("replicas", replicas, {"replica-priority": 50}),
            StartupOverlay("last", InstanceSelector(instance_ids=("valkey-2",)), {"replica-priority": 25}),
            StartupOverlay("one", InstanceSelector(instance_ids=("valkey-1",)), {"loglevel": "x"}),
            # The error policy looks only at the instances it selects, and accepts a value that means the same.
            StartupOverlay("exact", InstanceSelector(("replica",), ("valkey-2",)), {"loglevel": "debug"}, "error"),
            StartupOverlay("same", server_config={"appendonly": "no"}, conflict_policy="error"),
        ),
    )

    assert plans[0].server_config == {"repl-compression": "lz4", "appendonly": False}
    assert [(p.server_config.get("replica-priority"), p.server_config.get("loglevel")) for p in plans[1:]] == [
        (50, "x"),
        (25, "debug"),
    ]


@pytest.mark.parametrize(
    ("selector", "config", "policy", "message"),
    [
        (InstanceSelector(roles=("sentinel",)), {"appendonly": "no"}, "override", "unknown roles"),
        (InstanceSelector(instance_ids=("valkey-9",)), {"appendonly": "no"}, "override", "unknown instance IDs"),
        (InstanceSelector(("primary",), ("valkey-1",)), {"appendonly": "no"}, "override", "does not match any"),
        (InstanceSelector(instance_ids=("valkey-1",)), {"appendonly": "no"}, "error", "conflicts"),
        (InstanceSelector(), {"port": 9000}, "override", "managed by the local deployment"),
    ],
)
def test_a_rejected_overlay_changes_no_instance(selector, config, policy, message):
    plans = _plans({"loglevel": "notice"}, {"appendonly": True})
    originals = [dict(plan.server_config) for plan in plans]
    overlays = (
        StartupOverlay("first", server_config={"maxmemory": "1mb"}),
        StartupOverlay("o", selector, config, policy),
    )

    with pytest.raises(ValueError, match=message):
        apply_startup_overlays(plans, overlays)

    assert [plan.server_config for plan in plans] == originals


def _scenario(deployment, experiment="valkey.aof-durability", **inputs):
    return {"scenario_id": "s", "seed": 1, "experiment": {"id": experiment, "inputs": inputs}, "deployment": deployment}


def test_instances_start_with_exactly_the_effective_config_the_plan_records(tmp_path):
    runtime = valkey_service()
    deployment = {
        "mode": "standalone",
        "replicas_per_primary": 1,
        "instance_configs": {"replica": {"maxmemory": "1gb"}},
    }
    plan = runtime.compile(_scenario(deployment, appendfsync="always"))
    instances = to_plain_data(plan.instances)
    primary, replica = instances["valkey-0"]["server_config"], instances["valkey-1"]["server_config"]
    # The provider's defaults, the deployment's layers, and the experiment's overlay, in that order.
    assert (primary["appendfsync"], primary["appendonly"], replica["maxmemory"]) == ("always", "yes", "1gb")
    assert primary["maxmemory"] == DEFAULT_SERVER_CONFIG["maxmemory"]
    # A crash is reported as itself, not as what the crash-time memory test trips under a sanitizer.
    assert primary["crash-memcheck-enabled"] == "no"
    # Valkey's debug-only assertions are on, as in its own test suite.
    assert primary["enable-debug-assert"] == "yes"

    config, plans = LocalDeploymentProvider._prepare(plan)
    for item in plans:
        command = ProcessManager(config, 1).build_instance_command(7000, "/d", "/l", item.server_config)
        recorded = build_server_config_arguments(instances[item.instance_id]["server_config"])
        assert command[-len(recorded) :] == recorded

    # Known-bad: a recorded plan from a provider with other defaults must not run as if nothing changed.
    instances["valkey-0"]["server_config"]["appendfsync"] = "no"
    with pytest.raises(RunnerError, match="no longer plans"):
        runtime.validate_plan(type(plan).from_data({**to_plain_data(plan), "instances": instances}))


@pytest.mark.parametrize("overlay", [None, {"appendonly": "yes", "appendfsync": "everysec"}])
def test_a_scenario_relying_on_the_defaults_is_checked_like_one_that_spells_them_out(tmp_path, overlay):
    document = copy.deepcopy(yaml.safe_load((EXPERIMENT_DIRECTORY / "valkey.aof-durability.yaml").read_text()))
    document.update(id="tests.aof-defaults", extends=["valkey.base", "valkey.compression"])
    if overlay is None:
        document.pop("startup_overlays")
    else:
        document["startup_overlays"][0]["server_config"] = overlay
    (tmp_path / "variant.yaml").write_text(yaml.safe_dump(document))
    runtime = valkey_service(experiment_directories=(str(tmp_path),))

    with pytest.raises(ScenarioCompileError, match="every instance runs appendfsync everysec"):
        runtime.compile(_scenario({"mode": "standalone"}, "tests.aof-defaults", loss_window_s=1))
    runtime.compile(_scenario({"mode": "standalone"}, "tests.aof-defaults", loss_window_s=2.5))


@pytest.mark.parametrize(
    ("mode", "replicas", "replication", "min_replicas"),
    [("standalone", 1, True, 1), ("standalone", 0, False, 0), ("cluster", 0, True, 0), ("cluster", 3, True, 2)],
)
def test_validation_keeps_only_the_checks_that_fit_the_deployment(mode, replicas, replication, min_replicas):
    config = validation_config_from_document(
        {"check_roles": False, "replication_config": {"min_replicas_per_shard": 2}}
    )

    adapt_validation_config(config, DeploymentMode(mode), replicas)

    cluster = mode == "cluster"
    assert (config.check_replication, config.replication_config.min_replicas_per_shard) == (replication, min_replicas)
    # A standalone deployment always checks roles; a cluster keeps the scenario's choice.
    assert (config.check_roles, config.check_data_consistency) == (not cluster, True)
    assert (config.check_cluster_status, config.check_slot_coverage, config.check_view_consistency) == (cluster,) * 3


@pytest.mark.parametrize(
    ("document", "message"),
    [
        # It would sleep the whole budget and then always fail.
        ({"stabilization_wait": 30, "validation_timeout": 30}, "stabilization_wait"),
        ({"stabilization_wait": 45, "validation_timeout": 30}, "stabilization_wait"),
        # strict_mode could not change the verdict: both modes failed on the same mismatches.
        ({"roles_config": {"strict_mode": False}}, "roles_config"),
    ],
)
def test_an_impossible_validation_config_is_rejected(document, message):
    with pytest.raises(ValueError, match=message):
        validation_config_from_document(document)


@pytest.mark.parametrize(("wait", "valid"), [(29, True), (30, False), (45, False)])
def test_a_scenario_whose_stabilization_wait_takes_the_whole_validation_timeout_does_not_validate(
    tmp_path, wait, valid
):
    scenario = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "examples/valkey/replication_chaos.yaml").read_text()
    )
    scenario["experiment"]["inputs"]["validation"] = {"stabilization_wait": wait, "validation_timeout": 30}
    path = tmp_path / "scenario.yaml"
    path.write_text(yaml.safe_dump(scenario))
    service = valkey_service()
    if valid:
        service.validate(str(path))
    else:
        with pytest.raises(ScenarioCompileError, match=r"stabilization_wait \(\d+s\) must be less than"):
            service.validate(str(path))
