"""Guards for the GitHub workflows: their inputs, jobs, and CLI invocations are a contract with their callers."""

import ast
import contextlib
import importlib.util
import io
import json
import os
import re
import shlex
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from _pytest.mark.expression import Expression

from valkey_fuzzer.cli import create_parser
from valkey_fuzzer.reports import write_document
from valkey_fuzzer.results import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome

from .generator_support import generate_scenario
from .valkey_experiments_support import valkey_service

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
REGISTER = ROOT / "tests" / "fixtures" / "spec" / "register"
# The live tests that need root: a tmpfs mount and iptables partitions.
ROOT_ONLY_LIVE_TESTS = (
    "test_disk_full_degrades_gracefully_and_recovers",
    "test_cluster_partition_fails_over_heals_and_bounds_loss",
)


def _workflow(name):
    document = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    document["on"] = document.pop(True, document.get("on"))  # PyYAML reads the bare `on` key as True.
    return document


def _run_scripts(workflow):
    return [step["run"] for job in workflow["jobs"].values() for step in job.get("steps", ()) if "run" in step]


def test_pr_fuzzer_keeps_its_reusable_workflow_inputs_and_jobs():
    workflow = _workflow("valkey-pr-fuzzer.yml")
    inputs = workflow["on"]["workflow_call"]["inputs"]
    assert inputs["valkey_ref"]["required"] is True
    # Existing callers do not pass these, so they must stay optional.
    for name in (
        "fuzzer_repository",
        "fuzzer_ref",
        "valkey_repository",
        "pr_number",
        "pr_url",
        "label_name",
        "run_count",
    ):
        assert name in inputs and not inputs[name].get("required"), name
    assert {"resolve-inputs", "prepare-matrix", "build-valkey", "random-fuzzer", "summarize"} <= set(workflow["jobs"])


WORKFLOW_NAMES = sorted(path.name for path in WORKFLOWS.glob("*.yml"))


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_valkey_fuzzer_command_in_a_workflow_parses_with_the_cli(name):
    scripts = "\n".join(_run_scripts(_workflow(name)))
    # A command run as root keeps the runner's PATH: read it as the command it runs.
    commands = [
        re.sub(r'^sudo -E env "PATH=\$\{?PATH\}?" ', "", line.strip())
        for line in re.sub(r"\\\n\s*", " ", scripts).splitlines()
    ]
    # The workflows that only run pytest or ruff name no valkey-fuzzer command.
    no_cli = ("lint.yml", "test.yml", "valkey-experiments-live.yml")
    assert any(line.startswith("valkey-fuzzer ") for line in commands) or name in no_cli
    # A bash array ("${name[@]}") stands for the arguments its literal lists; one assigned in a loop stands
    # for none.
    # An array literal ends with ")" at the end of a line; a ")" inside, as in "$((16#...))", does not end it.
    arrays = {name: shlex.split(body) for name, body in re.findall(r"^\s*(\w+)=\((.*?)\)[ \t]*$", scripts, re.M | re.S)}
    for line in commands:
        if line.startswith("valkey-fuzzer "):
            tokens = shlex.split(re.split(r"\s(?:2>&1|\|\||\||>)\s", line)[0])
            arguments = []
            for token in tokens[1:]:
                array = re.fullmatch(r"\$\{(\w+)\[@\]\}", token)
                arguments += arrays.get(array.group(1), []) if array else [token]
            # A shell variable stands in as "1", which every option type (a path, a count, a seed) accepts.
            create_parser().parse_args(["1" if "$" in token else token for token in arguments])


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_file_a_workflow_names_exists(name):
    scripts = "\n".join(_run_scripts(_workflow(name)))
    for path in re.findall(r"(?:tests|examples|zoo|\.github/scripts)/[\w./-]+", scripts):
        assert (ROOT / path).exists(), f"{name} references missing {path}"


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_workflow_pins_its_actions_to_a_commit(name):
    for action in re.findall(r"^\s*(?:-\s+)?uses:\s*(\S.*)$", (WORKFLOWS / name).read_text(), re.M):
        assert action.startswith("./") or re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40} # v\d+(\.\d+)*", action), action


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_workflow_sets_its_token_permissions(name):
    # Without a permissions block a workflow gets the repository's default token, which may be able to write.
    assert "contents" in _workflow(name).get("permissions", {}), name


def test_lint_installs_the_ruff_version_the_dev_extra_pins():
    (install,) = [script.strip() for script in _run_scripts(_workflow("lint.yml")) if "pip install" in script]
    (pinned,) = re.findall(r'"ruff==([\d.]+)"', (ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    # Quoted, or the shell would read a version specifier such as >= as a redirect.
    assert install == f"pip install 'ruff=={pinned}'"


def _live_tests_selected(script):
    """The test functions of test_live_valkey_experiments.py that ``script``'s pytest ``-k`` expression selects."""

    tokens = shlex.split(re.sub(r"\\\n\s*", " ", script))
    expression = Expression.compile(tokens[tokens.index("-k") + 1])
    source = ast.parse((ROOT / "tests" / "test_live_valkey_experiments.py").read_text(encoding="utf-8"))
    names = {node.name for node in source.body if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")}
    return {name for name in names if _k_matches(expression, name)}, names


def _k_matches(expression, name):
    # -k matches a name case-insensitively, by substring.
    return expression.evaluate(lambda word, **_: word.lower() in name.lower())


def test_daily_live_workflow_runs_only_the_root_only_tests_as_root():
    steps = _workflow("valkey-experiments-live.yml")["jobs"]["valkey-experiments-live"]["steps"]
    unprivileged, privileged = [step["run"] for step in steps if "-m live" in step.get("run", "")]
    as_user, every_test = _live_tests_selected(unprivileged)
    as_root, _ = _live_tests_selected(privileged)
    assert as_root == set(ROOT_ONLY_LIVE_TESTS) and as_user == every_test - as_root
    assert "sudo" not in shlex.split(unprivileged) and "sudo" in shlex.split(privileged)
    # Root lets the tmpfs and iptables experiments run instead of skipping, and the step fails if they skip anyway.
    assert "SKIPPED" in privileged


def _load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / ".github" / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CI = _load_script("ci")
PR_7 = {
    "valkey_repository": "valkey-io/valkey",
    "valkey_ref": "refs/pull/7/merge",
    "pr_number": "7",
    "pr_url": "https://github.com/valkey-io/valkey/pull/7",
    "concurrency_key": "valkey-pr-fuzzer-valkey-io/valkey-pr-7",
}


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        ({"pr_link": "https://github.com/valkey-io/valkey/pull/7"}, PR_7),
        ({"pr_link": " https://github.com/valkey-io/valkey/pull/7/files?w=1#r1 "}, PR_7),
        # A PR URL pasted into pr_number counts as pr_link; matching overrides are fine.
        ({"pr_number": "https://github.com/valkey-io/valkey/pull/7", "valkey_ref": "refs/pull/7/merge"}, PR_7),
        ({"valkey_ref": "refs/pull/7/merge", "pr_number": "7", "pr_url": "https://elsewhere"}, PR_7),
        ({"valkey_repository": "me/valkey", "valkey_ref": "unstable"},
         {"valkey_repository": "me/valkey", "valkey_ref": "unstable", "pr_number": "", "pr_url": "",
          "concurrency_key": "valkey-pr-fuzzer-me/valkey-unstable"}),
        ({"pr_link": "https://github.com/valkey-io/valkey/pull/7", "valkey_ref": "unstable"}, "Conflicting inputs"),
        ({"pr_link": "https://github.com/valkey-io/valkey/pull/7", "pr_number": "8"}, "Conflicting inputs"),
        ({"pr_link": "https://gitlab.com/valkey-io/valkey/pull/7"}, "Invalid PR link format"),
        ({"pr_number": "seven", "valkey_ref": "unstable"}, "must be a numeric PR number"),
        ({}, "Either pr_link or valkey_ref must be provided"),
    ],
)  # fmt: skip
def test_the_pr_fuzzer_resolves_its_target_from_the_inputs(inputs, expected):
    if isinstance(expected, dict):
        assert CI.resolve(**inputs) == expected
        return
    with pytest.raises(SystemExit, match=expected):
        CI.resolve(**inputs)


@pytest.mark.parametrize(("raw", "expected"), [("10", 10), ("1", 1), ("3.0", 3), ("256", 256)])
def test_the_pr_fuzzer_queues_run_count_runs(raw, expected, monkeypatch, capsys):
    monkeypatch.setenv("RUN_COUNT", raw)
    CI.main("matrix")
    outputs = dict(line.split("=", 1) for line in capsys.readouterr().out.splitlines())
    assert outputs == {"run_count": str(expected), "runs": json.dumps([{"run_id": n} for n in range(1, expected + 1)])}


# GitHub runs at most 256 jobs from one matrix, so a larger run_count would fail only once the matrix expands.
@pytest.mark.parametrize("raw", ["0", "-1", "257", "1.5", "ten", "", "nan", "inf"])
def test_the_pr_fuzzer_refuses_a_run_count_that_is_not_a_whole_number_from_1_to_256(raw):
    with pytest.raises(SystemExit, match="must be a whole number from 1 to 256"):
        CI.run_count(raw)


def test_the_pr_fuzzer_metadata_reads_a_real_run_report(tmp_path):
    batch = tmp_path / "batch"
    folder = batch / "runs" / "random-valkey.cluster-chaos-s31"
    folder.mkdir(parents=True)
    results = (ComponentResult("chaos", ComponentStatus.FAIL, "wave 1 validation failed (replication)"),)
    outcome = RunOutcome(
        plan=valkey_service().compile(generate_scenario(31)),
        status=OutcomeStatus.FAIL,
        summary="chaos failed",
        component_results=results,
        started_at=10.0,
        ended_at=52.5,
        valkey_sha="deadbeef",
    )
    write_document(str(folder / "report.json"), valkey_service().run_report(outcome))
    run = {"id": folder.name, "status": "FAIL", "seed": 31, "summary": "x", "report": f"runs/{folder.name}/report.json"}
    (batch / "summary.json").write_text(json.dumps({"status": "FAIL", "runs": [run]}))

    metadata = CI.metadata(1, str(batch), "success", "failure")

    assert metadata == {
        "run_id": 1, "status": "failure", "stage": "fuzzer", "scenario_id": outcome.plan.scenario_id, "seed": 31,
        "outcome": "FAIL", "duration": 42.5, "failed_components": ["chaos: wave 1 validation failed (replication)"],
        "summary": "x", "valkey_sha": "deadbeef",
    }  # fmt: skip


def test_the_pr_fuzzer_metadata_of_a_run_without_a_report_keeps_the_batch_verdict(tmp_path):
    run = {"id": "s7", "status": "ERROR", "seed": 7, "summary": "WorkerExit: no run report", "report": None}
    (tmp_path / "summary.json").write_text(json.dumps({"status": "ERROR", "runs": [run]}))

    metadata = CI.metadata(2, str(tmp_path), "success", "failure")

    assert {key: metadata[key] for key in ("status", "stage", "outcome", "seed", "summary")} == {
        "status": "failure", "stage": "fuzzer", "outcome": "ERROR", "seed": 7, "summary": "WorkerExit: no run report",
    }  # fmt: skip


def test_the_pr_fuzzer_metadata_of_a_run_whose_setup_failed_names_the_setup_stage(tmp_path):
    metadata = CI.metadata(3, str(tmp_path / "batch"), "failure", "skipped")

    assert (metadata["status"], metadata["stage"], metadata["outcome"]) == ("failure", "setup", None)


def test_every_pr_fuzzer_step_passes_the_environment_its_command_reads(tmp_path, monkeypatch):
    jobs = _workflow("valkey-pr-fuzzer.yml")["jobs"].values()
    steps = [step for job in jobs for step in job["steps"] if "ci.py" in step.get("run", "")]
    values = {"INPUT_VALKEY_REF": "unstable", "RUN_COUNT": "1", "RUN_ID": "1", "RUN_DIR": str(tmp_path / "run-1"),
              "RESULTS_DIR": str(tmp_path), "PREPARE_MATRIX_RESULT": "success", "BUILD_RESULT": "success"}  # fmt: skip
    # What the runner sets for every step.
    runner = {"GITHUB_OUTPUT": str(tmp_path / "output"), "GITHUB_STEP_SUMMARY": str(tmp_path / "summary")}
    monkeypatch.chdir(tmp_path)
    commands = []
    for step in steps:
        (command,) = re.findall(r"ci\.py ([\w-]+)", step["run"])
        with patch.dict(os.environ, {**{name: values.get(name, "") for name in step["env"]}, **runner}, clear=True):
            CI.main(command)
        commands.append(command)
    assert commands == ["resolve", "matrix", "metadata", "summarize"]


def _summary_env(**overrides):
    env = {
        "PREPARE_MATRIX_RESULT": "success", "RUN_COUNT": "2", "BUILD_RESULT": "success", "RESOLVED_SHA": "abc123",
        "FUZZER_REPOSITORY": "valkey-io/valkey-fuzzer", "FUZZER_REF": "main", "VALKEY_REPOSITORY": "valkey-io/valkey",
        "VALKEY_REF": "refs/pull/7/merge", "PR_NUMBER": "7", "PR_URL": "https://github.com/valkey-io/valkey/pull/7",
        "LABEL_NAME": "run-cluster-fuzzer", "RUN_URL": "https://github.com/valkey-io/valkey-fuzzer/actions/runs/1",
    }  # fmt: skip
    return {**env, **overrides}


def _run_result(run_id, status="success", **fields):
    return {"run_id": run_id, "status": status, "stage": "fuzzer", "seed": run_id, "outcome": "PASS", **fields}


def test_the_pr_fuzzer_summary_passes_when_every_run_passed():
    summary, comment, failed = CI.summarize(_summary_env(PR_NUMBER=""), {1: _run_result(1), 2: _run_result(2)})

    assert not failed and comment is None
    assert "- Overall result: `PASS`" in summary and "- Passed runs: 2/2" in summary
    assert "| 2 | success | 2 | fuzzer | - | PASS | - |" in summary


def test_the_pr_fuzzer_summary_fails_on_a_failed_or_missing_run_and_details_them_in_the_comment():
    failing = _run_result(1, "failure", outcome="FAIL", failed_components=["chaos: lost | 3 keys\nafter wave 1"])

    summary, comment, failed = CI.summarize(_summary_env(), {1: failing})

    assert failed
    assert "| 1 | failure | 1 | fuzzer | - | FAIL | chaos: lost / 3 keys after wave 1 |" in summary
    assert "| 2 | missing | - | artifact | - | - | artifact not uploaded |" in summary
    assert comment.startswith("<!-- valkey-cluster-fuzzer-results -->\n") and "- Overall result: `FAIL`" in comment
    assert "| 1 | failure | 1 | fuzzer | FAIL | chaos: lost / 3 keys after wave 1 |" in comment
    assert (
        "Missing artifacts for runs: 2" in comment
        and "- PR: [#7](https://github.com/valkey-io/valkey/pull/7)" in summary
    )


def test_the_pr_fuzzer_summary_fails_when_no_runs_were_queued():
    summary, comment, failed = CI.summarize(_summary_env(PREPARE_MATRIX_RESULT="failure", RUN_COUNT=""), {})

    assert failed and "No runs were queued" in summary and "No runs were queued" in comment
    assert "| Run |" not in summary and "<details>" not in comment


def test_the_pr_batch_workflow_holds_no_credentials_and_splices_no_input_into_a_shell():
    workflow = _workflow("fuzzer-pr-batch.yml")
    assert set(workflow["on"]) == {"workflow_dispatch"} and workflow["permissions"] == {"contents": "read"}
    (job,) = workflow["jobs"].values()
    text = (WORKFLOWS / "fuzzer-pr-batch.yml").read_text(encoding="utf-8")
    assert "permissions" not in job and "secrets." not in text and "github.token" not in text
    for step in job["steps"]:
        assert step.get("with", {}).get("persist-credentials", False) is False
        assert "${{ inputs." not in step.get("run", "")


SCENARIO = yaml.safe_dump(
    {"scenario_id": "s", "seed": 1, "experiment": {"id": "toy.register"}, "deployment": {"mode": "standalone"}}
)
BUNDLE = {str(path.relative_to(REGISTER)): path.read_text() for path in sorted(REGISTER.rglob("*")) if path.is_file()}
BINARY = yaml.safe_dump({"deployment": {"valkey_binary": "/bin/sh"}})
INSTANCE_BINARIES = yaml.safe_dump({"deployment": {"mode": "standalone", "instance_binaries": {"replica": "/bin/sh"}}})
ARTIFACT = yaml.safe_dump({"artifacts": {"old": {"use": "valkey.artifact.local-binary", "with": {"path": "/bin/sh"}}}})


def _check_pr_batch_inputs(tmp_path, scenarios, inputs):
    """Run the PR batch's input step as CI does; its error, or None."""

    env = {
        "INPUT_SCENARIOS": scenarios if isinstance(scenarios, str) else json.dumps(scenarios),
        "INPUT_SPEC_BUNDLES": json.dumps(inputs["bundles"]) if "bundles" in inputs else "",
        "INPUT_VALKEY_REPOSITORY": inputs.get("repository", "valkey-io/valkey"),
        "INPUT_VALKEY_SHA": inputs.get("sha", "a" * 40),
        "INPUT_RANDOM_SEEDS": inputs.get("random_seeds", "0"),
        "INPUT_REQUEST_ID": inputs.get("request_id", ""),
        "INPUT_EXCLUDE_EXPERIMENTS": inputs.get("exclude_experiments", ""),
        "CANDIDATES_DIR": str(tmp_path / "candidates"),
        "SPEC_DIR": str(tmp_path / "spec"),
    }
    with patch.dict(os.environ, env), contextlib.redirect_stdout(io.StringIO()):
        try:
            CI.main("check-inputs")
        except SystemExit as exc:
            return str(exc)
    return None


def test_the_pr_batch_runs_a_random_baseline_without_scenarios(tmp_path):
    inputs = {"random_seeds": "4", "request_id": "replay-007-122ec7e16"}
    inputs["exclude_experiments"] = "valkey.replication-chaos  valkey.repl-compression-full-sync"
    assert _check_pr_batch_inputs(tmp_path, {}, inputs) is None
    assert list((tmp_path / "candidates").iterdir()) == []


def test_the_pr_batch_writes_the_scenarios_and_bundles_it_is_sent(tmp_path):
    scenarios = {"proposal-1": SCENARIO, "spec.register": SCENARIO}

    assert _check_pr_batch_inputs(tmp_path, scenarios, {"bundles": {"register": BUNDLE}}) is None
    assert sorted(path.name for path in (tmp_path / "candidates").iterdir()) == [
        "proposal-1.yaml",
        "spec.register.yaml",
    ]
    written = tmp_path / "spec" / "register"
    assert {str(path.relative_to(written)) for path in written.rglob("*") if path.is_file()} == set(BUNDLE)


@pytest.mark.parametrize(
    ("scenarios", "inputs", "message"),
    [
        ("x" * 60001, {}, "the limit is 60,000"),
        ("not json", {}, "not JSON"),
        ({}, {}, "there is nothing to run"),
        ([], {}, "must be a JSON object"),
        ({"a": SCENARIO}, {"random_seeds": "5"}, "random_seeds must be 0 to 4"),
        ({"a": SCENARIO}, {"request_id": "a b"}, "request_id must be"),
        ({"a": SCENARIO}, {"random_seeds": "2", "exclude_experiments": "x;rm"}, "exclude_experiments must be"),
        ({"a": SCENARIO}, {"exclude_experiments": "valkey.replication-chaos"}, "which random_seeds 0 skips"),
        ({"../escape": SCENARIO}, {}, "must be letters"),
        ({"a": "[1, 2]"}, {}, "must be a mapping"),
        ({"a": BINARY}, {}, "sets deployment.valkey_binary; the batch runs valkey_sha's build"),
        ({"a": INSTANCE_BINARIES}, {}, "sets deployment.instance_binaries; the batch runs valkey_sha's build"),
        ({"a": ARTIFACT}, {}, "declares artifacts; the batch runs valkey_sha's build"),
        ({"a": SCENARIO}, {"sha": "main"}, "full 40-character lowercase commit SHA"),
        ({"a": SCENARIO}, {"repository": "evil; rm -rf /"}, "OWNER/NAME"),
        ({"a": SCENARIO}, {"bundles": {"../up": {}}}, "must be lowercase letters"),
        ({"a": SCENARIO}, {"bundles": {"register": {"model.tla": "x"}}}, "has no claims.yaml"),
        ({"a": SCENARIO}, {"bundles": {"register": {**BUNDLE, "../evil.cfg": "x"}}}, "must be claims.yaml"),
        ({"a": SCENARIO}, {"bundles": {"register": {**BUNDLE, "model.tla": "---- MODULE x ----\n===="}}}, "is invalid"),
    ],
)
def test_the_pr_batch_refuses_bad_inputs(tmp_path, scenarios, inputs, message):
    error = _check_pr_batch_inputs(tmp_path, scenarios, inputs)

    assert error and message in error, (message, error)


def test_the_unit_tests_run_on_every_python_version_the_package_declares():
    declared = re.findall(r'"Programming Language :: Python :: (3\.\d+)"', (ROOT / "pyproject.toml").read_text())
    assert _workflow("test.yml")["jobs"]["test"]["strategy"]["matrix"]["python-version"] == declared


def test_the_pr_gating_server_tests_build_the_pinned_valkey_commit():
    steps = _workflow("test.yml")["jobs"]["server-tests"]["steps"]
    (build,) = [step["run"] for step in steps if "make -C" in step.get("run", "")]
    # Unstable HEAD would let a Valkey regression fail an unrelated PR; the daily live workflow follows it instead.
    assert "zoo/valkey-commit" in build and "git clone" not in build


def test_the_scheduled_soak_runs_a_build_of_valkey_unstable():
    scripts = "\n".join(_run_scripts(_workflow("fuzzer-run.yml")))
    # Without --valkey-binary the local deployment builds its fixed fallback release, never a new Valkey commit.
    (binary,) = re.findall(r"--valkey-binary (\S+)", scripts)
    source = binary.removesuffix("/src/valkey-server")
    assert re.search(rf"git clone --depth 1 https://github.com/valkey-io/valkey.git {re.escape(source)}\n", scripts)
    assert f"make -C {source} " in scripts


def test_the_spec_check_runs_on_every_change_to_main():
    # Its experiment gate compiles the Valkey experiments with the core compiler: a path filter narrower than
    # the whole repository would let a change that breaks the translated scenarios pass.
    triggers = _workflow("tla-model-check.yml")["on"]
    assert all("paths" not in (triggers[event] or {}) for event in ("push", "pull_request"))


# Each job that runs tests backed by TLC, and the pytest steps in it that run them.
TLC_JOBS = {
    ("tla-model-check.yml", "tlc"): 1,
    ("test.yml", "server-tests"): 2,
    ("valkey-experiments-live.yml", "valkey-experiments-live"): 1,
}


@pytest.mark.parametrize(("name", "job"), sorted(TLC_JOBS))
def test_every_job_with_tlc_backed_tests_has_java_and_the_pinned_tla_tools(name, job):
    # Without them those tests skip, and CI never checks the spec bridge end to end.
    workflow = _workflow(name)
    pinned = {key: _workflow("tla-model-check.yml")["env"][key] for key in ("TLA_TOOLS_VERSION", "TLA_TOOLS_SHA256")}
    assert {key: workflow.get("env", {}).get(key) for key in pinned} == pinned
    steps = workflow["jobs"][job]["steps"]
    assert any(step.get("uses", "").startswith("actions/setup-java@") for step in steps)
    (download,) = [step["run"] for step in steps if "tla2tools.jar" in step.get("run", "") and "curl" in step["run"]]
    assert "${TLA_TOOLS_SHA256}" in download and "sha256sum -c" in download
    tests = [step for step in steps if "pytest" in step.get("run", "")]
    with_jar = [
        step for step in tests if step.get("env", {}).get("TLA2TOOLS_JAR") == "${{ runner.temp }}/tla2tools.jar"
    ]
    assert len(with_jar) == TLC_JOBS[name, job], [step.get("name") for step in tests]


@pytest.mark.parametrize("name", WORKFLOW_NAMES)
def test_every_checkout_leaves_no_credentials_behind(name):
    # Later steps run scenario, fuzzer, and test code that has no use for the token.
    for job in _workflow(name)["jobs"].values():
        for step in job.get("steps", ()):
            if step.get("uses", "").startswith("actions/checkout@"):
                assert step.get("with", {}).get("persist-credentials") is False, (name, step.get("name"))


def _step_env(workflow, command):
    """The environment of each step of ``workflow`` that runs ``ci.py <command>``: its job's and its own."""

    jobs = _workflow(workflow)["jobs"].values()
    return [
        {**job.get("env", {}), **step.get("env", {})}
        for job in jobs
        for step in job["steps"]
        if re.search(rf"ci\.py {command}\b", step.get("run", ""))
    ]


def test_the_batch_workflows_summarize_through_the_ci_script():
    for workflow in ("fuzzer-pr-batch.yml", "fuzzer-run.yml"):
        (env,) = _step_env(workflow, "summary")
        assert {"SUMMARY_TITLE", "BATCH_DIR"} <= set(env), workflow
        assert "python - <<" not in (WORKFLOWS / workflow).read_text(), "no workflow keeps a script of its own"
    (env,) = _step_env("fuzzer-pr-batch.yml", "check-inputs")
    names = {"INPUT_SCENARIOS", "INPUT_SPEC_BUNDLES", "INPUT_VALKEY_REPOSITORY", "INPUT_VALKEY_SHA"}
    names |= {"INPUT_RANDOM_SEEDS", "INPUT_REQUEST_ID", "INPUT_EXCLUDE_EXPERIMENTS"}
    assert names | {"CANDIDATES_DIR", "SPEC_DIR"} <= set(env)


def test_the_pr_batch_drops_the_write_ledgers_of_the_runs_that_passed(tmp_path):
    batch = tmp_path / "batch"
    runs = []
    for run_id, status in (("passed", "PASS"), ("failed", "FAIL")):
        writes = batch / "runs" / run_id / "components" / "writes"
        writes.mkdir(parents=True)
        for name in ("ledger.jsonl", "preload.jsonl", "result.json"):
            (writes / name).write_text("{}")
        (batch / "runs" / run_id / "report.json").write_text("{}")
        runs.append({"id": run_id, "status": status, "summary": "x", "report": f"runs/{run_id}/report.json"})
    runs.append({"id": "lost", "status": "PASS", "summary": "x", "report": None})
    (batch / "summary.json").write_text(json.dumps({"runs": runs}))
    (env,) = _step_env("fuzzer-pr-batch.yml", "drop-ledgers")
    assert "BATCH_DIR" in env

    with patch.dict(os.environ, {"BATCH_DIR": str(batch)}):
        CI.main("drop-ledgers")

    kept = {path.relative_to(batch).as_posix() for path in batch.rglob("*.*")}
    assert "runs/passed/components/writes/ledger.jsonl" not in kept
    assert "runs/passed/components/writes/preload.jsonl" not in kept
    assert "runs/passed/components/writes/result.json" in kept
    assert {"runs/failed/components/writes/ledger.jsonl", "runs/failed/components/writes/preload.jsonl"} <= kept


def test_the_pr_comment_is_found_by_the_marker_the_summary_writes():
    text = (WORKFLOWS / "valkey-pr-fuzzer.yml").read_text()
    assert CI.COMMENT_MARKER not in text and "const marker = body.split('\\n', 1)[0];" in text


def test_the_batch_summary_lists_every_run_with_its_report_and_the_signatures(tmp_path):
    batch = tmp_path / "batch"
    assert "The batch wrote no summary" in CI.batch_summary("Soak", str(batch))
    folder = batch / "runs" / "random-valkey.cluster-chaos-s31"
    folder.mkdir(parents=True)
    results = (ComponentResult("chaos", ComponentStatus.FAIL, "wave 1 | lost keys"),)
    outcome = RunOutcome(
        plan=valkey_service().compile(generate_scenario(31)),
        status=OutcomeStatus.FAIL,
        summary="chaos failed",
        component_results=results,
        valkey_sha="deadbeef",
    )
    write_document(str(folder / "report.json"), valkey_service().run_report(outcome))
    runs = [
        {"id": folder.name, "status": "FAIL", "seed": 31, "summary": "x", "report": f"runs/{folder.name}/report.json"},
        {"id": "s7", "status": "ERROR", "seed": 7, "summary": "WorkerExit: no run report", "report": None},
    ]
    signatures = [{"count": 1, "label": "FAIL | valkey.cluster-chaos | chaos:FAIL"}]
    (batch / "summary.json").write_text(
        json.dumps({"status": "FAIL", "counts": {"FAIL": 1, "ERROR": 1}, "runs": runs, "signatures": signatures})
    )

    summary = CI.batch_summary("Soak", str(batch), "`valkey-io/valkey` `-` `abc`")

    assert summary.startswith("## Soak\n\n- Valkey: `valkey-io/valkey` `-` `abc`\n- Batch status: `FAIL`")
    plan = outcome.plan
    assert f"| {folder.name} | FAIL | {plan.scenario_id} | 31 | deadbeef | x | chaos: wave 1 / lost keys |" in summary
    assert "| s7 | ERROR | - | 7 | - | WorkerExit: no run report | - |" in summary
    assert "- Signature (1 run(s)): FAIL | valkey.cluster-chaos | chaos:FAIL" in summary
