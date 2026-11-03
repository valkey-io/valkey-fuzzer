"""The machine-readable catalog (``list ... --format json``) and ``validate --format json``.

A tool writes scenarios from the catalog, so it must say exactly what compiling enforces;
``validate`` reports every file with structured errors.
"""

import json
from pathlib import Path

import pytest

from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.components import component_registrations
from valkey_fuzzer.service import RunService

pytestmark = pytest.mark.usefixtures("report_root")


def _cli(capsys, *argv):
    capsys.readouterr()
    args = create_parser().parse_args(list(argv))
    code = args.handler(args)
    return code, capsys.readouterr().out


def _outside(field):
    """Values the catalog says ``field`` rejects: just past each bound, and a value outside its enum."""

    values = [field["maximum"] + 1] if "maximum" in field else []
    values += [field["minimum"] - 1] if "minimum" in field else []
    return values + (["not-one-of-the-listed-values"] if "enum" in field else [])


def _allowed(rule):
    return rule["minimum"] if "minimum" in rule else rule["enum"][0]


def test_every_listed_bound_and_enum_is_what_compiling_enforces_and_a_scenario_from_the_catalog_compiles(capsys):
    runtime = valkey_service()
    rejected = compiled = 0
    _, out = _cli(capsys, "list", "experiments", "--format", "json")
    for row in json.loads(out):
        # A required input (an artifact binary, a chaos schedule) must be supplied first.
        if row["inputs"]["required"]:
            continue
        rules = row["deployment_requirements"]["properties"]
        for mode in row["deployment_modes"]:
            deployment = {"mode": mode, **{name: _allowed(rule) for name, rule in rules.items()}}
            scenario = {"scenario_id": "drafted", "seed": 7, "experiment": {"id": row["id"]}, "deployment": deployment}
            assert runtime.compile(scenario).experiment_id == row["id"]
            compiled += 1
            fields = [("experiment.inputs", name, field) for name, field in row["inputs"]["properties"].items()]
            fields += [("deployment", name, rule) for name, rule in rules.items()]
            for where, name, field in fields:
                for value in _outside(field):
                    bad = json.loads(json.dumps(scenario))
                    if where == "deployment":
                        bad["deployment"][name] = value
                    else:
                        bad["experiment"]["inputs"] = {name: value}
                    with pytest.raises(Exception) as caught:
                        runtime.compile(bad)
                    assert getattr(caught.value, "path", None) == f"{where}.{name}", (row["id"], name, value)
                    rejected += 1
    assert compiled >= 14 and rejected > 100
    # Every component and generator hook is listed, as text and as JSON with its config schema.
    _, out = _cli(capsys, "list", "components", "--format", "json")
    assert {row["component_id"] for row in json.loads(out)} == {item.component_id for item in component_registrations()}
    for what in ("components", "generators"):
        code, out = _cli(capsys, "list", what)
        assert code == 0 and len(out.splitlines()) >= 3


GOOD = "scenario_id: good\nseed: 1\nexperiment: {id: valkey.rdb-compression, inputs: {rdbcompression: lz4}}\n"
GOOD += "deployment: {mode: standalone}\n"
DRAFT = (
    "id: draft.bgsave\nmode: smoke\ndeployment: {modes: [standalone]}\ninputs: {properties: {expect: {type: string}}}\n"
    "components: [{use: valkey.action.bgsave, id: save, config: {expect: {$ref: inputs.expect}}}]\n"
)
CASES = [
    (GOOD, []),
    (GOOD.replace("lz4", "brotli"), ["experiment.inputs.rdbcompression"]),
    (GOOD.replace("mode: standalone", "mode: cluster"), ["deployment.mode"]),
    (GOOD.replace("seed: 1", "seed: 1\nretries: 3"), ["retries"]),
    (GOOD.replace("valkey.rdb-compression", "valkey.no-such"), ["experiment.id"]),
    (GOOD.replace("valkey.rdb-compression, inputs: {rdbcompression: lz4}", "draft.bgsave, inputs: {expect: maybe}"),
     ["components.save.config.expect"]),
    ("scenario_id: [\n", [""]),
    (None, [""]),  # a file that does not exist
]  # fmt: skip


def test_validate_reports_every_file_with_structured_errors_and_passes_only_if_all_do(tmp_path, capsys):
    (tmp_path / "experiments").mkdir()
    (tmp_path / "experiments" / "draft.yaml").write_text(DRAFT, encoding="utf-8")
    files = [str(tmp_path / f"{index}.yaml") for index in range(len(CASES))]
    for path, (text, _) in zip(files, CASES):
        if text is not None:
            Path(path).write_text(text, encoding="utf-8")

    code, out = _cli(capsys, "validate", *files, "--experiment-dir", str(tmp_path / "experiments"), "--format", "json")

    rows = json.loads(out)["files"]
    assert code != 0 and json.loads(out)["ok"] is False
    assert [(row["file"], row["ok"], [error["path"] for error in row["errors"]]) for row in rows] == [
        (path, not paths, paths) for path, (_, paths) in zip(files, CASES)
    ]
    assert (rows[0]["experiment"], rows[0]["scenario_id"]) == ("valkey.rdb-compression", "good")
    assert all(error["message"] for row in rows for error in row["errors"])
    assert _cli(capsys, "validate", files[0], files[0])[0] == 0
    code, out = _cli(capsys, "validate", files[0], files[1])
    assert code != 0 and f"1/2 files passed; failed: {files[1]}" in out
    code, out = _cli(capsys, "validate", files[0])
    # One line says what was checked, not four lines of narration.
    assert code == 0 and out.count("valid without provisioning") == 1 and "passed structural validation" not in out


def test_validate_raises_a_bug_in_the_compiler_instead_of_calling_the_scenario_invalid(tmp_path, capsys, monkeypatch):
    path = tmp_path / "good.yaml"
    path.write_text(GOOD, encoding="utf-8")
    monkeypatch.setattr(RunService, "compile", lambda self, scenario: {}["missing"])
    with pytest.raises(KeyError):
        _cli(capsys, "validate", str(path), "--format", "json")
