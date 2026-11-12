"""What a run's report is trusted for: a run whose report cannot be written fails, replay refuses changed
code or a plan its scenario does not compile to, minimization keeps only the same failure, and a differential
comparison never passes on missing evidence."""

import shutil
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.support import make_plan, recorded_report
from valkey_fuzzer.cli import main
from valkey_fuzzer.differential import Differential, compare_outcomes, differential_status
from valkey_fuzzer.minimize import failure_signature, same_failure
from valkey_fuzzer.reports import InputError, ReportWriteError, export_scenario
from valkey_fuzzer.results import ComponentResult, ComponentStatus, Evidence, OutcomeStatus, RunOutcome
from valkey_fuzzer.service import ReplayDriftError

GENERATED = ["--seed", "9", "--experiment", "valkey.cluster-chaos"]
EXAMPLE = str(Path(__file__).resolve().parents[1] / "examples" / "valkey" / "snapshot_equivalence.yaml")
DIFFERENTIAL = Path(__file__).resolve().parents[1] / "examples" / "valkey" / "differential_baseline_vs_candidate.yaml"


@pytest.mark.usefixtures("report_root")
@pytest.mark.parametrize(
    ("argv", "status", "code"),
    [
        (["run", *GENERATED, "--export-scenario"], OutcomeStatus.FAIL, 1),
        (["run", EXAMPLE, "--output"], OutcomeStatus.PASS, 2),
        # A FAIL stands when only the copy of its report cannot be written, as for run.
        (["replay", "REPORT", "--output"], OutcomeStatus.FAIL, 1),
        (["replay", "REPORT", "--output"], OutcomeStatus.PASS, 2),
        (["diff", str(DIFFERENTIAL), "--output"], OutcomeStatus.FAIL, 1),
    ],
)
def test_a_run_whose_requested_output_cannot_be_written_does_not_succeed(
    tmp_path, canned_runs, capsys, argv, status, code
):
    canned_runs.status = status
    (tmp_path / "file").write_text("")
    if "REPORT" in argv:
        report = str(tmp_path / "recorded.json")
        with patch("sys.argv", ["valkey-fuzzer", "run", EXAMPLE, "--output", report]):
            main()
        argv = [report if item == "REPORT" else item for item in argv]
    with patch("sys.argv", ["valkey-fuzzer", *argv, str(tmp_path / "file" / "out")]):
        assert main() == code
    assert "could not" in capsys.readouterr().err


def _edited(package, relative):
    """Make ``package`` load from a copy of its folder in which ``relative`` is edited."""

    def edit(tmp_path, monkeypatch):
        module = sys.modules[package]
        copy = tmp_path / "edited" / package
        shutil.copytree(Path(module.__file__).parent, copy, ignore=shutil.ignore_patterns("__pycache__"))
        (copy / relative).write_text((copy / relative).read_text() + "\n# changed\n")
        monkeypatch.setattr(module, "__file__", str(copy / "__init__.py"))

    return edit


def _seed(report):
    report["scenario"]["seed"] += 1


@pytest.mark.parametrize(
    ("changed", "edit", "item"),
    [
        # Shared code no component class is defined in: the verdict, and a helper the Valkey components call.
        (_edited("valkey_fuzzer", "verdict.py"), None, "fuzzer code"),
        (_edited("valkey_fuzzer", "components/polling.py"), None, "fuzzer code"),
        (_edited("valkey_fuzzer", "deployment/local/provider.py"), None, "deployment provider for standalone code"),
        (None, _seed, "the report's scenario and plan"),
    ],
)
def test_replay_refuses_a_report_whose_code_or_plan_changed(service, tmp_path, monkeypatch, changed, edit, item):
    recorded = recorded_report(service, tmp_path, edit)
    assert edit or service.prepare_replay(recorded).verified
    if changed is not None:
        changed(tmp_path, monkeypatch)

    with pytest.raises(ReplayDriftError) as raised:
        service.prepare_replay(recorded)
    entries = {entry["item"]: entry for entry in raised.value.drift}
    assert item in entries and entries[item]["recorded"] != entries[item]["replay"]


def test_replay_says_code_without_a_recorded_digest_was_not_verified(service, tmp_path):
    def drop_digests(report):
        report["reproducer"]["fuzzer_digest"] = None

    prepared = service.prepare_replay(recorded_report(service, tmp_path, drop_digests))
    assert "fuzzer code: its source cannot be read" in prepared.not_verified


def _failing(*results, diagnostics=None):
    results = tuple(ComponentResult(*result) for result in results)
    return RunOutcome(make_plan(), OutcomeStatus.FAIL, "FAIL", component_results=results, diagnostics=diagnostics or {})


LOST = ("ledger", ComponentStatus.FAIL, "3 acknowledged write(s) lost (budget 0); valkey-4: RDB failed to load")


@pytest.mark.parametrize(
    ("candidate", "same"),
    [
        (_failing(("ledger", ComponentStatus.FAIL, "1 acknowledged write(s) lost (budget 0); valkey-2: RDB failed to load")), True),
        (_failing(("ledger", ComponentStatus.FAIL, "cannot read dataset: ConnectionError")), False),
        (_failing(("ledger", ComponentStatus.ERROR, "component execution failed")), False),
        (_failing(LOST, diagnostics={"missing_required_events": ["bgsave.save-started"]}), False),
    ],
    ids=["other-counts-and-instances", "other-verdict", "fail-became-error", "missing-event"],
)  # fmt: skip
def test_minimization_keeps_only_the_same_failure_of_a_component(candidate, same):
    assert same_failure(failure_signature(_failing(LOST)), failure_signature(candidate)) is same


def test_a_differential_item_without_the_compared_field_leaves_the_comparison_unmade():
    base = {"seed": 5, "experiment": {"id": "valkey.snapshot-equivalence"}, "deployment": {"mode": "standalone"}}
    rule = {"evidence_id": "valkey.dataset.digest", "source": "dataset", "field": "digest"}
    differential = Differential.from_document(
        {"differential_id": "unit", "base": base, "variants": {"a": {}, "b": {}}, "compare": {"equal": [rule]}}
    )

    def digests(*values):
        evidence = tuple(Evidence("valkey.dataset.digest", "dataset", "oracle.completed", 1.0, item) for item in values)
        return RunOutcome(plan=make_plan(), status=OutcomeStatus.PASS, summary="PASS", evidence=evidence)

    # The item without a digest counts: dropped, both variants would compare as "d1".
    outcomes = {"a": digests({"digest": "d1"}, {"shard": 2}), "b": digests({"digest": "d1"})}
    comparisons = compare_outcomes(differential, outcomes)
    assert comparisons[0]["result"] == "missing"
    # Not a pass, and not a finding about the server either: the harness could not compare.
    assert differential_status(differential, outcomes, comparisons)[0] == "ERROR"


def test_a_report_with_a_small_float_replays_and_minimizes_as_written(service, canned_runs, tmp_path):
    # The report JSON holds 5e-05, which a YAML 1.1 loader reads back as the string "5e-05".
    scenario = tmp_path / "cancel.yaml"
    scenario.write_text(
        "scenario_id: cancel\nseed: 11\nexperiment:\n  id: valkey.aof-durability\n"
        "  inputs: {run_after_crash_s: 0.00005}\ndeployment: {mode: standalone, replicas_per_primary: 1}\n"
    )
    canned_runs.status = OutcomeStatus.FAIL
    prepared = service.prepare_run(str(scenario))
    record = service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r"))
    assert '"run_after_crash_s": 5e-05' in Path(record.report_path).read_text()

    assert service.prepare_replay(record.report_path).drift == []
    minimization = service.prepare_minimization(record.report_path)
    assert minimization.scenario["experiment"]["inputs"]["run_after_crash_s"] == 5e-05

    duplicated = tmp_path / "duplicated.json"
    duplicated.write_text('{"outcome": 1, "outcome": 2}')
    with pytest.raises(InputError, match="duplicate key 'outcome'"):
        service.prepare_replay(str(duplicated))


def test_an_exported_scenario_replaces_the_old_file_whole_or_not_at_all(tmp_path, monkeypatch):
    target = tmp_path / "scenario.yaml"
    target.write_text("old: scenario\n", encoding="utf-8")

    def interrupted(source, destination):
        raise OSError("disk full")

    monkeypatch.setattr("valkey_fuzzer.reports.os.replace", interrupted)
    with pytest.raises(ReportWriteError, match="could not write the scenario"):
        export_scenario({"scenario_id": "s", "seed": 1}, str(target))
    assert target.read_text(encoding="utf-8") == "old: scenario\n" and list(tmp_path.iterdir()) == [target]
