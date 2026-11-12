"""The bug zoo's loader and verdicts, and that every shipped case compiles against the Valkey experiments."""

import os
from pathlib import Path

import pytest
import yaml

from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer import zoo

COMMIT, CASES = zoo.load_zoo()
_CASE = {"description": "broken", "catches": "a check", "runs": 5, "min_detection_rate": 0.8,
         "scenarios": [{"file": "scenario.yaml", "fail": ["data"]}]}  # fmt: skip
_SCENARIO = {"scenario_id": "s", "seed": 1, "experiment": {"id": "x"}, "deployment": {}}


def test_the_zoo_pins_one_commit_beside_its_build_script_and_its_cases_catch_distinct_checks():
    assert len(COMMIT) == 40 and (zoo.ZOO_DIRECTORY / "valkey-commit").read_text().strip() == COMMIT
    assert os.access(zoo.ZOO_DIRECTORY / "build.sh", os.X_OK) and all(
        Path(case.directory).parent == zoo.ZOO_DIRECTORY for case in CASES
    )
    assert len(CASES) >= 8 and len({case.catches for case in CASES}) == len(CASES)
    assert any(not case.patched for case in CASES), "the zoo needs a configuration-level case"


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.case_id)
def test_every_zoo_run_compiles_with_its_expected_components(tmp_path, case):
    for name in ["clean", *(item.case_id for item in CASES if item.patched)]:
        (tmp_path / name / "src").mkdir(parents=True)
        (tmp_path / name / "src" / "valkey-server").write_text("#!/bin/sh\n")
        (tmp_path / name / "src" / "valkey-server").chmod(0o755)
    runtime = valkey_service(artifact_dir=str(tmp_path / "artifacts"))
    runs = zoo.zoo_runs(case, tmp_path, runtime.catalog)
    assert [run.control for run in runs].count(True) == len(case.scenarios) and len(runs) == len(case.scenarios) * (
        case.runs + 1
    )
    for run in runs:
        components = {component.id for component in runtime.compile(dict(run.scenario)).components}
        assert set(run.fail) <= components, f"{run.file}: {set(run.fail) - components} not in the plan"
        assert run.scenario["deployment"]["valkey_binary"] == zoo.binary(tmp_path, None if run.control else case)
    assert len({run.scenario["seed"] for run in runs if not run.control}) == case.runs
    if case.overrides:  # The control runs without the overrides that break the case.
        keys = set(case.overrides["deployment"]["server_config"])
        assert not keys & set(runs[0].scenario["deployment"].get("server_config") or {})
        assert keys <= set(runs[1].scenario["deployment"]["server_config"])


def _case(root, case, scenario=_SCENARIO, patch=True):
    (root / "case").mkdir()
    (root / "case" / "case.yaml").write_text(yaml.safe_dump(case))
    (root / "case" / "scenario.yaml").write_text(yaml.safe_dump(scenario))
    if patch:
        (root / "case" / "patch.diff").write_text("diff\n")
    return zoo.load_case(root / "case")


@pytest.mark.parametrize(
    ("edit", "message", "options"),
    [
        ({"runs": 4}, "case.runs must be at least 5", {}),
        ({"min_detection_rate": 0}, "min_detection_rate", {}),
        ({"surprise": 1}, "unknown fields", {}),
        ({"requires": ["macos"]}, r"case.requires\[0\] must be one of \['linux', 'root'\]", {}),
        ({"scenarios": [{"file": "scenario.yaml", "fail": []}]}, "name the components that must FAIL", {}),
        ({"scenarios": [{"file": "scenario.yaml", "fail": "data"}]}, r"case.scenarios\[0\].fail must be a list", {}),
        ({"scenarios": [{"file": "scenario.yaml", "fails": ["data"]}]}, r"case.scenarios\[0\] contains unknown fields", {}),
        ({"scenarios": [{"file": "missing.yaml", "fail": ["data"]}]}, "not found", {}),
        ({"catches": ""}, "catches must say what is broken", {}),
        ({}, r"needs a patch\.diff or overrides", {"patch": False}),
        ({"skipped_until": "valkey-io/valkey#1"}, "skipped_until names a fix the pinned commit lacks", {}),
        ({}, r"must not set deployment\.valkey_binary", {"scenario": {**_SCENARIO, "deployment": {"valkey_binary": "/x"}}}),
    ],
)  # fmt: skip
def test_a_malformed_case_unknown_case_or_missing_build_is_rejected(tmp_path, edit, message, options):
    with pytest.raises(zoo.ZooError, match=message):
        _case(tmp_path, {**_CASE, **edit}, **options)
    with pytest.raises(zoo.ZooError, match="no such zoo case: nope"):
        zoo.load_zoo(zoo.ZOO_DIRECTORY, ["nope"])
    with pytest.raises(zoo.ZooError, match=r"zoo/build\.sh"):
        zoo.zoo_runs(CASES[0], tmp_path, valkey_service().catalog)


def _observed(status, failed=(), passed=()):
    results = [{"component_id": item, "status": "FAIL"} for item in failed] + [
        {"component_id": item, "status": "PASS"} for item in passed
    ]
    return zoo.observe({"component_results": results}, status)


def test_a_case_holds_only_with_a_passing_control_and_runs_that_fail_through_every_expected_check(tmp_path):
    run = zoo.ZooRun("c", "scenario.yaml", False, {"seed": 1}, ("data", "liveness"))
    assert zoo.detected(run, _observed("FAIL", failed=["data", "liveness", "dataset"]))
    # Failing for another reason is not detection: the expected check went blind.
    assert not zoo.detected(run, _observed("FAIL", failed=["data"], passed=["liveness"]))
    assert not zoo.detected(run, _observed("ERROR", failed=["data", "liveness"]))
    assert zoo.observe(None, "ERROR") == {"status": "ERROR", "failed": []}, "no report detects nothing"

    case = _case(tmp_path, {**_CASE, "requires": ["root"]})  # min_detection_rate 0.8

    def report(control_status, detections):
        results = [
            (zoo.ZooRun(case.case_id, "scenario.yaml", True, {"seed": 1}, ()), _observed(control_status), "control")
        ]
        for index, hit in enumerate(detections):
            run = zoo.ZooRun(case.case_id, "scenario.yaml", False, {"seed": 2 + index}, ("data",))
            observed = _observed("FAIL", failed=["data"]) if hit is True else _observed(hit or "PASS")
            results.append((run, observed, f"r{index}"))
        # Judged by the terms the batch recorded, not by the case directory, which may have changed since.
        return zoo.case_report(case.terms(), results)

    assert report("PASS", [True] * 4 + [False])["scenarios"]["scenario.yaml"]["detection_rate"] == 0.8
    assert report("PASS", [True] * 4 + [False])["holds"] and report("PASS", [True] * 3 + [False] * 2)["holds"] is False
    # A control that fails proves nothing, however often the broken build fails.
    assert report("FAIL", [True] * 5)["holds"] is False
    # A run the harness could not observe decides nothing: the case is undecided, not broken.
    for undecided in (report("ERROR", [True] * 5), report("PASS", [True] * 4 + ["SKIP_UNSUPPORTED"])):
        assert undecided["holds"] is None and undecided["undecided"] in ("ERROR", "SKIP_UNSUPPORTED")
    assert report("FAIL", [True] * 4 + ["ERROR"])["holds"] is False, "a failing control decides it anyway"
    assert case.unmet_requirements() == ([] if os.geteuid() == 0 else ["root"]), "a root-only case is skipped elsewhere"


def test_a_case_whose_fix_the_pinned_commit_lacks_is_skipped_until_the_pin_has_it(tmp_path):
    case = _case(tmp_path, {**_CASE, "skipped_until": "valkey-io/valkey#5206"}, patch=False)
    assert not case.patched and case.skipped_until == "valkey-io/valkey#5206"
    assert "a pinned Valkey commit with valkey-io/valkey#5206" in case.unmet_requirements()
    # A shipped case of a fix merged after the pin carries no patch.
    skipped = [item for item in CASES if item.skipped_until]
    assert not any(item.patched or item.overrides for item in skipped)


def test_a_zoo_directory_without_the_zoo_names_where_the_zoo_is(tmp_path):
    # An installed package ships no zoo/: the zoo is read from a checkout of the repository.
    with pytest.raises(zoo.ZooError, match=r"no bug zoo.*checkout of the repository"):
        zoo.load_zoo(tmp_path)
