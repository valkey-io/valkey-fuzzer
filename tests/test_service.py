"""RunService below the CLI: its catalog, report folders, replay provenance, and the run reports it refuses."""

import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest

from tests.support import component_registration, deployment_registrations, make_plan
from valkey_fuzzer import hoststate
from valkey_fuzzer._data import file_sha256
from valkey_fuzzer.catalog import (
    Catalog,
    DuplicateExperimentError,
    load_experiment_yaml,
)
from valkey_fuzzer.deployment.local.rerun import binary_drift
from valkey_fuzzer.environment import EnvironmentSpec
from valkey_fuzzer.plan import ExecutionPlan, ExperimentSpec, RunMode
from valkey_fuzzer.provenance import provenance_drift, reproducer
from valkey_fuzzer.references import EnvironmentKind, parse_environment_reference
from valkey_fuzzer.registrations import DeploymentProviderRegistration
from valkey_fuzzer.registry import FrozenRegistryError
from valkey_fuzzer.reports import InputError, ReportWriteError, new_report_folder
from valkey_fuzzer.results import ComponentResult, ComponentStatus, OutcomeStatus
from valkey_fuzzer.runner import RunnerError
from valkey_fuzzer.schema import ExperimentDocumentError, ScenarioCompileError
from valkey_fuzzer.service import RunService
from valkey_fuzzer.testing import FakeDeploymentProvider

EXAMPLE = str(Path(__file__).resolve().parents[1] / "examples" / "valkey" / "snapshot_equivalence.yaml")


def test_report_folders_sort_by_start_time_and_never_collide(tmp_path, monkeypatch):
    suffixes = iter(["aaaaaa", "aaaaaa", "bbbbbb", "cccccc"])
    monkeypatch.setattr(hoststate.secrets, "token_hex", lambda size: next(suffixes))
    monkeypatch.setattr(hoststate.time, "time", lambda: 1_700_000_000.25)
    first = new_report_folder(str(tmp_path), "snapshot-equivalence")
    # Same millisecond and the same random suffix: the next suffix is tried.
    second = new_report_folder(str(tmp_path), "snapshot-equivalence")
    monkeypatch.setattr(hoststate.time, "time", lambda: 1_700_000_000.5)
    third = new_report_folder(str(tmp_path), "a/b c")
    assert [Path(item).name for item in (first, second, third)] == [
        "20231114T221320.250Z-snapshot-equivalence-aaaaaa",
        "20231114T221320.250Z-snapshot-equivalence-bbbbbb",
        "20231114T221320.500Z-a-b-c-cccccc",
    ]
    (tmp_path / "file").write_text("")
    with pytest.raises(ReportWriteError, match="could not create a report folder under"):
        new_report_folder(str(tmp_path / "file"), "run")


def test_provenance_of_the_same_run_matches_but_a_garbled_record_drifts(service):
    plan = service.prepare_run(EXAMPLE).plan
    recorded = reproducer(service.catalog, plan)
    assert provenance_drift(service.catalog, plan, recorded) == ([], [])
    # Known-bad: a malformed list compares as empty, so what it should hold drifts.
    recorded["components"] = "garbled"
    items = {entry["item"] for entry in provenance_drift(service.catalog, plan, recorded)[0]}
    assert {f"component {item.use}" for item in plan.components} <= items


@pytest.mark.parametrize(
    ("edit", "missing"),
    [
        (lambda report: report.pop("reproducer"), "reproducer"),
        (lambda report: report.pop("scenario"), "scenario"),
        (lambda report: report["reproducer"].pop("components"), "reproducer.components"),
        (lambda report: report.update(reproducer=[]), "reproducer"),
        (lambda report: report.pop("outcome"), "outcome"),
        (lambda report: report.update(outcome=[]), "outcome"),
        (lambda report: report["outcome"].pop("plan"), "outcome.plan"),
    ],
)
def test_replay_and_minimize_reject_an_incomplete_run_report(service, tmp_path, edit, missing):
    prepared = service.prepare_run(EXAMPLE)
    report = copy.deepcopy(service.execute(prepared.plan, scenario=prepared.scenario).report)
    edit(report)
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report))
    for prepare in (service.prepare_replay, service.prepare_minimization):
        with pytest.raises(InputError, match=rf"not a complete run report: it has no {missing}\b"):
            prepare(str(path))


def test_a_document_that_cannot_be_read_is_an_input_error(service, tmp_path):
    # The decoding error is an input error, not an unwrapped UnicodeDecodeError.
    path = tmp_path / "report.json"
    path.write_bytes(b"\xff\xfe{}")
    for prepare in (service.prepare_replay, service.prepare_minimization, service.read_base):
        with pytest.raises(InputError, match="unable to read"):
            prepare(str(path))


# ---------------------------------------------------------------------------
# The catalog: components and experiment directories load into one frozen catalog, which compiles and runs on the
# deployment provider the scenario picks
# ---------------------------------------------------------------------------


def _service(*experiment_directories, **options) -> RunService:
    """A run service whose catalog is built now, as a command's is on first use."""

    service = RunService(experiment_directories=experiment_directories, **options)
    service.catalog  # noqa: B018
    return service


def _service_of(**entries) -> RunService:
    """A run service whose frozen catalog holds only ``entries`` (see :meth:`Catalog.register`)."""

    catalog = Catalog()
    catalog.register(**entries)
    catalog.freeze()
    return RunService(catalog=catalog)


SCENARIO = {
    "scenario_id": "s",
    "seed": 41,
    "experiment": {"id": "example.feature"},
    "deployment": {"mode": "standalone"},
}


def _yaml(experiment_id="example.feature"):
    return f"id: {experiment_id}\nmode: smoke\ndeployment: {{modes: [standalone]}}\ncomponents: [{{use: example.action.fake, id: act}}]\n"


class _Action:
    def run(self, context, spec):
        return ComponentResult(spec.id, ComponentStatus.PASS, "done")


def test_registered_components_and_directory_experiments_compile_and_run_on_a_frozen_catalog(tmp_path):
    calls = []
    (tmp_path / "experiment.yaml").write_text(_yaml(), encoding="utf-8")
    providers = deployment_registrations(standalone=lambda: FakeDeploymentProvider(calls=calls))
    catalog = Catalog()
    catalog.register(
        components=(component_registration("example.action.fake", factory=_Action),),
        deployment_providers=tuple(providers.snapshot().values()),
    )
    catalog.load_experiment_directories([tmp_path])
    catalog.freeze()
    service = RunService(catalog=catalog)

    outcome = service.runner.run(service.compile(SCENARIO))
    assert outcome.status is OutcomeStatus.PASS and outcome.component_results[0].component_id == "act"
    assert calls == ["validate", "provision", "cleanup"]
    assert catalog.experiment_directory("example.feature") == str(tmp_path)
    with pytest.raises(FrozenRegistryError):
        service.catalog.register(components=(component_registration("example.action.late"),))


@pytest.mark.parametrize(
    ("files", "error"),
    [
        ({"first/a.yaml": "example.a", "second/a.yaml": "example.a"}, DuplicateExperimentError),
        ({"first/a.yaml": "valkey.snapshot-equivalence"}, DuplicateExperimentError),
        ({"first/a.yaml": "example.a", "second/broken.yaml": None}, ExperimentDocumentError),
    ],
    ids=["one-id-in-two-directories", "a-directory-and-a-shipped-experiment", "a-malformed-document"],
)  # fmt: skip
def test_a_catalog_with_a_conflict_or_a_malformed_document_is_never_built(tmp_path, files, error):
    for name, experiment_id in files.items():
        (tmp_path / name).parent.mkdir(exist_ok=True)
        (tmp_path / name).write_text(_yaml(experiment_id) if experiment_id else "id: a\nid: b\n", encoding="utf-8")

    with pytest.raises(error):
        _service(*sorted({path.parent for path in tmp_path.glob("*/*.yaml")}), deployment_providers=())


def _deployment(provider_id="example.deployment.fake", factory=object):
    return DeploymentProviderRegistration(deployment_mode="example-mode", provider_id=provider_id, factory=factory)


def test_a_scenario_picks_one_of_several_providers_of_its_mode_and_replay_keeps_that_one():
    calls, local_calls = [], []
    local = _deployment(factory=lambda: FakeDeploymentProvider(calls=local_calls))
    docker = _deployment("example.deployment.docker", lambda: FakeDeploymentProvider(calls=calls))
    experiment = ExperimentSpec("example.choice", mode=RunMode.SMOKE, deployment_modes=("example-mode",), components=())
    scenario = {"scenario_id": "choice", "seed": 7, "experiment": {"id": "example.choice"}}
    service = _service_of(experiments=(experiment,), deployment_providers=(local, docker))

    with pytest.raises(ScenarioCompileError, match="multiple registered providers"):
        service.compile({**scenario, "deployment": {"mode": "example-mode"}})
    plan = service.compile(
        {**scenario, "deployment": {"mode": "example-mode", "provider": "example.deployment.docker"}}
    )
    assert service.runner.run(ExecutionPlan.from_data(json.loads(plan.to_json()))).status is OutcomeStatus.PASS
    assert (calls, local_calls) == (["validate", "provision", "cleanup"], [])

    without = _service_of(experiments=(experiment,), deployment_providers=(local,))
    with pytest.raises(RunnerError, match=r"'example\.deployment\.docker' is not registered for 'example-mode'"):
        without.validate_plan(plan)
    without_any = _service_of(experiments=(experiment,))
    with pytest.raises(ScenarioCompileError, match="has no registered deployment provider"):
        without_any.compile({**scenario, "deployment": {"mode": "example-mode"}})


def test_the_service_changes_a_deployment_only_through_its_providers_hooks(tmp_path):
    # Both providers run the local deployment's mode, and their documents would parse as local ones; only the
    # hooks a provider registers give its deployment another binary or other ports.
    plain = DeploymentProviderRegistration("standalone", "example.deployment.plain", FakeDeploymentProvider)
    hooked = replace(
        plain,
        provider_id="example.deployment.hooked",
        binary_override=lambda path: {"build": path},
        relocate_ports=lambda plan: ({"port": 7100}, "moved to port 7100"),
    )
    experiment = load_experiment_yaml(
        "id: example.random\nmode: smoke\ndeployment: {modes: [standalone]}\ncomponents: []\n"
        "metadata: {generator: {deployment: {mode: [standalone]}}}\n"
    )
    service = _service_of(experiments=(experiment,), deployment_providers=(plain, hooked))

    def draw(provider, **options):
        base = {"deployment": {"provider": f"example.deployment.{provider}"}}
        return service.prepare_random(seed=1, experiment="example.random", base=base, **options)

    prepared = draw("plain")
    assert prepared.scenario["deployment"] == {"mode": "standalone", "provider": "example.deployment.plain"}
    assert prepared.notes == ()
    with pytest.raises(ScenarioCompileError, match=r"'example\.deployment\.plain' cannot run another Valkey binary"):
        draw("plain", valkey_binary="/opt/valkey-server")
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(prepared.plan.to_json(), encoding="utf-8")
    with pytest.raises(ScenarioCompileError, match="cannot run another Valkey binary"):
        service.prepare_replay(str(plan_path), valkey_binary="/opt/valkey-server")

    prepared = draw("hooked", valkey_binary="/opt/valkey-server")
    assert prepared.scenario["deployment"] == {
        "mode": "standalone",
        "provider": "example.deployment.hooked",
        "build": "/opt/valkey-server",
        "port": 7100,
    }
    assert prepared.plan.deployment["port"] == 7100 and prepared.notes == ("moved to port 7100",)
    # A batch's binary is only for scenarios that name none.
    base = {"deployment": {"provider": "example.deployment.hooked", "build": "/own/valkey-server"}}
    for keep, build in ((True, "/own/valkey-server"), (False, "/opt/valkey-server")):
        prepared = service.prepare_random(
            seed=1, experiment="example.random", base=base, valkey_binary="/opt/valkey-server", keep_own_binary=keep
        )
        assert prepared.scenario["deployment"]["build"] == build


def test_binary_drift_names_a_referenced_binary_it_cannot_read():
    # The message names the reference's path, not its serialized marker.
    plan = make_plan(
        deployment={"mode": "standalone", "valkey_binary": parse_environment_reference("artifacts.build.path")},
        artifacts=(EnvironmentSpec(EnvironmentKind.ARTIFACT, "build", "tests.artifact.fake"),),
    )
    recorded = [{"instance_id": "valkey-0", "binary": "/old/valkey-server", "binary_sha256": "ab" * 32}]

    drift, unchecked = binary_drift(plan, recorded, replayed=[{"instance_id": "valkey-0"}])

    assert drift == []
    assert unchecked == ["Valkey binary of valkey-0: the replay's binary (artifacts.build.path) cannot be read"]


def test_binary_drift_reads_the_source_built_default_binary_as_a_run_would_start_it(tmp_path, monkeypatch):
    # With no binary configured and none on PATH, a run starts the one built under /tmp/valkey-build; a replay
    # compares that one too rather than call it unreadable.
    from valkey_fuzzer.deployment.local import processes

    built = tmp_path / "valkey-server"
    built.write_text("#!/bin/sh\n")
    monkeypatch.setattr(processes.shutil, "which", lambda name: None)
    monkeypatch.setattr(processes, "_built_binary", lambda base_dir: str(built))
    plan = make_plan(deployment={"mode": "standalone"})
    recorded = [{"instance_id": "valkey-0", "binary": str(built), "binary_sha256": file_sha256(str(built))}]

    assert binary_drift(plan, recorded) == ([], [])


def test_replay_compares_each_artifact_by_its_recorded_sha256(service, canned_runs, tmp_path):
    # rolling-upgrade's to_binary names only an artifact; no instance starts on it, so the
    # per-instance comparison never sees it.
    binaries = {}
    for name in ("baseline", "candidate"):
        binaries[name] = tmp_path / f"{name}-server"
        binaries[name].write_text(f"#!/bin/sh\n# {name}\n")
        binaries[name].chmod(0o755)
    scenario = tmp_path / "upgrade.yaml"
    scenario.write_text(
        json.dumps(
            {
                "scenario_id": "rolling-upgrade",
                "seed": 61,
                "artifacts": {
                    name: {"use": "valkey.artifact.local-binary", "with": {"path": str(path)}}
                    for name, path in binaries.items()
                },
                "experiment": {
                    "id": "valkey.rolling-upgrade",
                    "inputs": {"to_binary": {"$ref": "artifacts.candidate.binary"}},
                },
                "deployment": {
                    "mode": "standalone",
                    "replicas_per_primary": 1,
                    "valkey_binary": {"$ref": "artifacts.baseline.binary"},
                },
            }
        )
    )

    def acquired(name, digest):
        result = {"provenance": {} if digest is None else {"sha256": digest}}
        return {
            "dependency": f"artifacts.{name}",
            "phase": "acquire",
            "outcome": "succeeded",
            "details": {"result": result},
        }

    # What the provisioner records: each resolved artifact's provenance, here as the local-binary provider does.
    unrecorded = set()
    canned_runs.diagnostics = lambda plan: {
        "environment_report": {
            "records": [
                acquired(name, None if name in unrecorded else file_sha256(str(path)))
                for name, path in binaries.items()
            ]
        }
    }
    prepared = service.prepare_run(str(scenario))
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r"))
    binaries["candidate"].write_text("#!/bin/sh\n# rebuilt from another commit\n")

    replay = service.prepare_replay(record.report_path)
    assert replay.drift == []
    late = service.replay(replay).late_drift
    assert [entry["item"] for entry in late] == ["artifacts.candidate sha256"]

    unrecorded.add("candidate")
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r2"))
    assert (
        "artifacts.candidate: the recorded run has no sha256 for it"
        in service.prepare_replay(record.report_path).not_verified
    )
