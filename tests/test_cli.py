"""The commands, from the command line to their exit codes and files, on canned runs (no Valkey starts)."""

import argparse
import hashlib
import json
import os
import shlex
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from tests.support import sigterm_during
from tests.valkey_experiments_support import valkey_service
from valkey_fuzzer.cli import main
from valkey_fuzzer.commands._common import add_common_options
from valkey_fuzzer.deployment.local import config as local_config
from valkey_fuzzer.outcomes import EXIT_FAILURE, EXIT_HARNESS_ERROR, EXIT_INTERRUPTED, EXIT_UNSUPPORTED
from valkey_fuzzer.reports import SCENARIO_FILE
from valkey_fuzzer.results import Evidence, OutcomeStatus
from valkey_fuzzer.runner import PreflightError

EXAMPLES = Path(__file__).resolve().parents[1] / "examples" / "valkey"
FORKLESS = str(EXAMPLES / "forkless_save.yaml")
NO_ZSTD = ("--exclude", "server_config.rdbcompression=zstd", "--exclude", "server_config.repl-compression=zstd")

pytestmark = pytest.mark.usefixtures("report_root")


def _cli(*argv):
    """Run ``valkey-fuzzer argv`` and return its exit code."""

    with patch("sys.argv", ["valkey-fuzzer", *map(str, argv)]):
        return main()


def _run_report(tmp_path, *options):
    output = tmp_path / "report.json"
    _cli("run", FORKLESS, "--output", output, *options)
    return output


@pytest.mark.parametrize(
    ("status", "options", "code"),
    [
        (OutcomeStatus.PASS, (), 0),
        (OutcomeStatus.FAIL, (), EXIT_FAILURE),
        (OutcomeStatus.ERROR, (), EXIT_HARNESS_ERROR),
        (OutcomeStatus.SKIP_UNSUPPORTED, (), EXIT_UNSUPPORTED),
        (OutcomeStatus.SKIP_UNSUPPORTED, ("--allow-unsupported",), 0),
    ],
)
def test_run_and_replay_exit_with_the_code_of_their_outcome(tmp_path, canned_runs, status, options, code):
    canned_runs.status = status
    report = _run_report(tmp_path, "--artifact-dir", tmp_path / "a", *options)
    (saved,) = (tmp_path / "a" / "runs").glob("*-forkless-save-*/report.json")
    assert json.loads(saved.read_text()) == json.loads(report.read_text())

    assert _cli("replay", report, *options) == code
    assert canned_runs.plans[1] == canned_runs.plans[0]


@pytest.mark.parametrize(
    "text",
    [
        "scenario_id: a\nscenario_id: b\nseed: 1\nexperiment: {id: valkey.forkless-save}\n",
        "api_version: old\nscenario_id: a\nseed: 1\nexperiment: {id: valkey.forkless-save}\n",
        "scenario_id: a\nseed: 1\nexperiment: {id: valkey.forkless-save, inputs: {nope: 1}}\n",
    ],
)
def test_a_scenario_that_does_not_compile_is_a_harness_error_and_never_runs(tmp_path, canned_runs, text):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text(text + "deployment: {mode: standalone}\n")
    assert _cli("run", scenario) == _cli("validate", scenario) == EXIT_HARNESS_ERROR
    assert _cli("validate", FORKLESS) == 0
    assert not canned_runs.plans


USAGE_ERRORS = [("run", FORKLESS, "--random"), ("run", FORKLESS, "--experiment", "valkey.forkless-save"), ("run",)]  # fmt: skip
PARSER_ERRORS = [("run", "--random", "--exclude", "x"), ("run", "--timeout", "nan")]  # fmt: skip


def test_bad_inputs_are_harness_errors_that_run_nothing(tmp_path, canned_runs):
    for command in ("run", "validate", "diff", "minimize", "replay"):
        assert _cli(command, tmp_path / "missing.yaml") == EXIT_HARNESS_ERROR, command
    assert _cli("run", "--seed", 1, "--experiment", "valkey.no-such-experiment") == EXIT_HARNESS_ERROR
    every = ("--exclude", "inputs.compressible=true", "--exclude", "inputs.compressible=false")
    assert _cli("run", "--seed", 1, "--experiment", "valkey.forkless-save", *every) == EXIT_HARNESS_ERROR
    base = tmp_path / "base.yaml"
    base.write_text("[1]")
    assert _cli("run", "--seed", 1, "--experiment", "valkey.forkless-save", "--base", base) == EXIT_HARNESS_ERROR
    # Mixed-up options are usage errors (a harness error, never exit 1, a FAIL), and the parser refuses
    # malformed values.
    for argv in USAGE_ERRORS:
        assert _cli(*argv) == EXIT_HARNESS_ERROR, argv
    for argv in PARSER_ERRORS:
        with pytest.raises(SystemExit):
            _cli(*argv)
    assert not canned_runs.plans
    # A report that cannot be written fails the run and leaves no temporary file.
    assert _cli("run", FORKLESS, "--output", tmp_path) == EXIT_HARNESS_ERROR
    assert not tuple(tmp_path.parent.glob(f".{tmp_path.name}.*.tmp"))


def test_a_run_that_fails_its_execution_preflight_leaves_an_error_report(tmp_path, canned_runs):
    message = "unknown instance_configs selector 'replica'"
    canned_runs.error = PreflightError(message)
    canned_runs.error.__cause__ = ValueError(message)
    assert _cli("run", FORKLESS, "--output", tmp_path / "report.json") == EXIT_HARNESS_ERROR
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["status"] == "ERROR" and "run failed before execution" in report["summary"]
    # Recorded as the runner and the batch worker record errors, phase included, so batch groups one error
    # under one signature.
    assert report["outcome"]["diagnostics"]["error"] == {"type": "ValueError", "message": message, "phase": "preflight"}
    # A runner that fails once the plan passed preflight is no preflight failure, in the summary or the signature.
    canned_runs.error = RuntimeError("the event journal broke")
    assert _cli("run", FORKLESS, "--output", tmp_path / "report.json") == EXIT_HARNESS_ERROR
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["summary"] == "the runner failed: the event journal broke"
    assert report["outcome"]["diagnostics"]["error"]["phase"] == "runner"


@pytest.mark.parametrize(
    ("argv", "error", "code"),
    [((), None, EXIT_HARNESS_ERROR), (("run", "s.yaml"), KeyboardInterrupt(), EXIT_INTERRUPTED),
     (("run", "s.yaml"), RuntimeError("boom"), EXIT_HARNESS_ERROR)],
)  # fmt: skip
def test_main_maps_no_command_an_interrupt_and_an_unexpected_error_to_exit_codes(argv, error, code):
    with patch("valkey_fuzzer.commands.run.run_command", side_effect=error):
        assert _cli(*argv) == code


@pytest.mark.parametrize(
    ("target", "call"),
    [
        ("valkey_fuzzer.commands.run.run_command", lambda: _cli("run", "s.yaml")),
    ],
    ids=["cli"],
)
def test_sigterm_takes_the_interrupt_path_and_exits_130(target, call):
    assert sigterm_during(target, call) == (["cleaned up"], EXIT_INTERRUPTED)


def test_a_seeded_run_saves_a_scenario_that_runs_the_same_plan_as_the_seed(tmp_path, canned_runs):
    exported = tmp_path / "exported.yaml"
    seeded = ("run", "--seed", 42, "--experiment", "valkey.forkless-save", *NO_ZSTD, "--valkey-binary", "/opt/valkey")
    assert _cli(*seeded, "--export-scenario", exported) == 0
    (path,) = (tmp_path / "valkey-fuzzer" / "runs").glob("*/report.json")
    report = json.loads(path.read_text())
    assert report["reproducer"]["generated"]["seed"] == 42
    saved = path.parent / SCENARIO_FILE
    assert yaml.safe_load(saved.read_text()) == report["scenario"] == yaml.safe_load(exported.read_text())

    assert _cli("run", saved) == _cli(*seeded) == _cli("run", exported) == 0
    plans = {plan.to_json() for plan in canned_runs.plans}
    assert len(plans) == 1 and canned_runs.plans[0].deployment["valkey_binary"] == "/opt/valkey"


def test_a_run_of_a_scenario_file_saves_the_scenario_it_ran(tmp_path, canned_runs):
    # Every run saves its scenario, not only a generated one.
    assert _cli("run", FORKLESS, "--valkey-binary", "/opt/valkey") == 0
    (path,) = (tmp_path / "valkey-fuzzer" / "runs").glob("*/report.json")
    saved = path.parent / SCENARIO_FILE
    assert yaml.safe_load(saved.read_text()) == json.loads(path.read_text())["scenario"]
    assert _cli("run", saved) == 0 and len({plan.to_json() for plan in canned_runs.plans}) == 1


def test_the_printed_reproduce_command_draws_the_same_scenario(tmp_path, canned_runs, capsys):
    # The binary decides what rolling-upgrade's hooks name as the second build, so the line must carry it.
    (tmp_path / "no experiments").mkdir()
    first = ("run", "--seed", 5, "--experiment", "valkey.rolling-upgrade", "--valkey-binary", "/opt/my builds/valkey")
    first += ("--exclude", "deployment.replicas_per_primary=2", "--experiment-dir", tmp_path / "no experiments")
    assert _cli(*first) == 0
    (line,) = [line for line in capsys.readouterr().out.splitlines() if line.startswith("Reproduce: ")]
    argv = shlex.split(line[len("Reproduce: ") :])
    assert argv[0] == "valkey-fuzzer" and "--valkey-binary" in argv and "--experiment-dir" in argv
    assert _cli(*argv[1:]) == 0
    assert canned_runs.plans[0].to_json() == canned_runs.plans[1].to_json()


def test_random_runs_use_fresh_seeds_honour_exclusions_and_a_base_overrides_the_draws(tmp_path, canned_runs):
    excluded = ("--exclude", "experiment=valkey.cluster-chaos", "--exclude", "experiment=valkey.snapshot-equivalence")
    for _ in range(6):
        assert _cli("run", "--random", *NO_ZSTD, *excluded) == 0
    assert len({plan.seed for plan in canned_runs.plans}) == 6
    assert not {plan.experiment_id for plan in canned_runs.plans} & {
        "valkey.cluster-chaos",
        "valkey.snapshot-equivalence",
    }
    base = tmp_path / "base.yaml"
    base.write_text(
        yaml.safe_dump({"deployment": {"replicas_per_primary": 2}, "experiment": {"inputs": {"pause_s": 4}}})
    )
    assert _cli("run", "--seed", 1, "--experiment", "valkey.replication-full-resync", *NO_ZSTD, "--base", base) == 0
    assert (canned_runs.plans[-1].deployment["replicas_per_primary"], canned_runs.plans[-1].inputs["pause_s"]) == (2, 4)


@pytest.mark.parametrize(
    ("default", "ephemeral"),
    [(7000, None), (7100, None), (7000, (7001, 60999))],
    ids=["the-local-default", "a-moved-default", "never-into-the-ephemeral-range"],
)
def test_a_generated_scenario_moves_off_busy_ports_but_a_batch_workers_never_does(monkeypatch, default, ephemeral):
    # It starts from the deployment's own default base port, not a copy of it (7000) the service keeps.
    if default != 7000:
        planned = local_config.deployment_config_from_document

        def moved(document):
            return planned(document) if "base_port" in document else replace(planned(document), base_port=default)

        monkeypatch.setattr(local_config, "deployment_config_from_document", moved)
    service = valkey_service()
    busy = lambda deployment: [str(default)] if deployment.get("base_port", default) == default else []  # noqa: E731
    monkeypatch.setattr("valkey_fuzzer.deployment.local.ports.busy_planned_ports", busy)
    if ephemeral:
        monkeypatch.setattr("valkey_fuzzer.deployment.local.ports.ephemeral_port_range", lambda: ephemeral)
    prepared = service.prepare_random(seed=1, experiment="valkey.aof-durability")
    assert prepared.plan.deployment["base_port"] == prepared.scenario["deployment"]["base_port"] != default
    if ephemeral:
        # Its ports moved below the range (the busy block's neighbors above sit inside it).
        assert prepared.plan.deployment["base_port"] + len(prepared.plan.instances) <= ephemeral[0]
    assert any(f"plans base_port {default}" in note for note in prepared.notes)
    kept = service.prepare_random(seed=1, experiment="valkey.aof-durability", free_ports=False)
    assert "base_port" not in kept.scenario["deployment"]


def test_replay_refuses_a_drifted_environment_unless_allowed_and_then_records_the_drift(tmp_path, canned_runs):
    report = _run_report(tmp_path)
    data = json.loads(report.read_text())
    data["reproducer"]["plan_digest"] = "1" * 64
    data["reproducer"]["experiment"]["digest"] = "0" * 64
    data["reproducer"]["components"][0]["implementation"] = "old.Component"
    report.write_text(json.dumps(data))

    assert _cli("replay", report) == EXIT_HARNESS_ERROR
    assert len(canned_runs.plans) == 1, "a drifted replay must not run"
    assert _cli("replay", report, "--allow-drift", "--output", tmp_path / "replay.json") == 0
    provenance = json.loads((tmp_path / "replay.json").read_text())["provenance"]
    assert provenance["drift_allowed"] is True and "plan digest" in {entry["item"] for entry in provenance["drift"]}


def test_replay_runs_a_bare_plan_or_a_yaml_report_but_minimize_needs_a_run_report(tmp_path, canned_runs):
    plan = tmp_path / "plan.json"
    plan.write_text(valkey_service().compile(yaml.safe_load(Path(FORKLESS).read_text())).to_json())
    assert _cli("replay", plan, "--output", tmp_path / "replay.json") == 0
    assert json.loads((tmp_path / "replay.json").read_text())["provenance"]["verified"] is False
    assert _cli("minimize", plan) == EXIT_HARNESS_ERROR
    # --output-format is the --output file's; --format, in list, validate, and sweep, is stdout's.
    assert _cli("run", FORKLESS, "--output-format", "yaml") == EXIT_HARNESS_ERROR, "it needs --output"
    assert _cli("run", FORKLESS, "--output", tmp_path / "r.yaml", "--output-format", "yaml") == 0
    assert yaml.safe_load((tmp_path / "r.yaml").read_text())["status"] == "PASS"
    assert _cli("replay", tmp_path / "r.yaml") == 0
    assert canned_runs.plans[0] == canned_runs.plans[1] == canned_runs.plans[2]


def test_replay_checks_a_binary_an_environment_provider_supplies_after_the_run(tmp_path, canned_runs):
    digests = iter(["a" * 64, "b" * 64, "b" * 64])
    instance = {"instance_id": "valkey-0", "binary": "/opt/valkey"}
    canned_runs.diagnostics = lambda plan: {"deployment": {"instances": [{**instance, "binary_sha256": next(digests)}]}}
    report = tmp_path / "report.json"
    assert _cli("run", EXAMPLES / "mixed_version_cluster.yaml", "--output", report) == 0
    assert _cli("replay", report) == EXIT_HARNESS_ERROR
    assert _cli("replay", report, "--allow-drift") == 0


def test_replay_compares_the_valkey_binary_digest_and_a_chosen_binary_is_no_drift(tmp_path, canned_runs):
    binary, other = tmp_path / "valkey-server", tmp_path / "candidate-server"
    for path in (binary, other):
        path.write_bytes(path.name.encode())
        path.chmod(0o755)

    def digests(plan):
        path = plan.deployment["valkey_binary"]
        digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        return {"deployment": {"instances": [{"instance_id": "valkey-0", "binary": path, "binary_sha256": digest}]}}

    canned_runs.diagnostics = digests
    report = _run_report(tmp_path, "--valkey-binary", binary)
    assert _cli("replay", report) == 0
    binary.write_bytes(b"build two")
    assert _cli("replay", report) == EXIT_HARNESS_ERROR
    assert len(canned_runs.plans) == 2, "a replay of another build must not run"
    assert _cli("replay", report, "--valkey-binary", other) == 0


@pytest.mark.parametrize(("diverges", "code"), [(False, 0), (True, EXIT_FAILURE)])
def test_diff_passes_only_when_the_variants_evidence_agrees(tmp_path, canned_runs, diverges, code):
    def evidence(plan):
        method = plan.deployment["server_config"]["bgsave-default-method"]
        return (
            Evidence(
                "valkey.dataset.digest", "dataset", "oracle.completed", 1.0, {"digest": method if diverges else 1}
            ),
            Evidence("valkey.rdb.reload-verified", "snapshot", "oracle.completed", 1.0, {"content_digest": 1}),
            Evidence("valkey.workload.ledger", "writes", "workload.stopped", 1.0, {"replies_digest": 1}),
            Evidence(
                "valkey.bgsave.result", "bgsave", "action.completed", 1.0, {"after": {"rdb_last_bgsave_type": method}}
            ),
        )

    canned_runs.evidence = evidence
    output = tmp_path / "diff.json"
    assert (
        _cli("diff", EXAMPLES / "differential_fork_vs_forkless.yaml", "--output", output, "--artifact-dir", tmp_path)
        == code
    )
    (session,) = (tmp_path / "differential").iterdir()
    assert json.loads((session / "differential.json").read_text()) == json.loads(output.read_text())
    assert len(list(session.glob("*/report.json"))) == 2


def test_minimize_refuses_a_drifted_environment_and_a_budget_below_one(tmp_path, canned_runs, capsys):
    canned_runs.status = OutcomeStatus.FAIL
    report = _run_report(tmp_path)
    data = json.loads(report.read_text())
    data["reproducer"]["fuzzer_digest"] = "0" * 64
    report.write_text(json.dumps(data))
    # Reductions are judged by whether they fail the same way: on other code that proves nothing.
    assert _cli("minimize", report) == EXIT_HARNESS_ERROR
    assert "refusing to minimize" in capsys.readouterr().err and len(canned_runs.plans) == 1
    assert _cli("minimize", report, "--allow-drift", "--max-attempts", 1) == 0
    for budget in (0, -3):
        with pytest.raises(SystemExit):
            _cli("minimize", report, "--max-attempts", budget)


def test_minimize_shrinks_a_failing_run_and_leaves_a_passing_one_alone(tmp_path, canned_runs):
    assert _cli("minimize", _run_report(tmp_path)) == 0 and len(canned_runs.plans) == 1
    canned_runs.status = OutcomeStatus.FAIL
    output = tmp_path / "minimized.yaml"
    report = _run_report(tmp_path / "fail")
    assert _cli("minimize", report, "--output", output, "--max-attempts", 8, "--artifact-dir", tmp_path) == 0
    minimized = yaml.safe_load(output.read_text())["experiment"]["inputs"]
    assert minimized["stable_keys"] < yaml.safe_load(Path(FORKLESS).read_text())["experiment"]["inputs"]["stable_keys"]
    assert list((tmp_path / "minimize").glob("*-forkless-save-*/attempt-1/report.json"))


@pytest.mark.parametrize(
    ("answer", "denied", "code", "left"),
    [(None, None, 0, []),
     # Declined at the prompt, or one delete failed: a harness failure, never a pass.
     ("n", None, EXIT_HARNESS_ERROR, ["valkey-0", "valkey-1"]),
     (None, "valkey-0", EXIT_HARNESS_ERROR, ["valkey-0"])],
)  # fmt: skip
def test_cleanup_logs_succeeds_only_when_every_log_of_the_seed_is_deleted(tmp_path, answer, denied, code, left):
    (tmp_path / "logs").mkdir()
    for name in ("valkey-0", "valkey-1"):
        (tmp_path / "logs" / f"42-abcd1234-{name}.log").write_text("log\n")
    real_remove = os.remove

    def remove(path):
        if denied and path.endswith(f"{denied}.log"):
            raise PermissionError("denied")
        real_remove(path)

    force = [] if answer else ["--force"]
    with patch("builtins.input", return_value=answer), patch("os.remove", side_effect=remove):
        assert _cli("cleanup-logs", 42, "--data-dir", tmp_path, *force) == code
    assert sorted(path.stem.split("-", 2)[2] for path in (tmp_path / "logs").iterdir()) == left


def test_the_common_options_set_no_default_for_an_option_a_command_defines_itself():
    # A default for an option the parser does not define would override the command's own, whichever came first.
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", default=5.0)
    add_common_options(parser)
    args = parser.parse_args([])
    assert args.timeout == 5.0 and not hasattr(args, "valkey_binary") and not hasattr(args, "allow_unsupported")


@pytest.mark.parametrize(
    ("error", "shown"),
    [
        (KeyError("x"), "Error: KeyError: 'x'"),
        (RuntimeError(), "Error: RuntimeError"),
        (ValueError("bad"), "Error: bad"),
    ],
)
def test_an_error_goes_to_stderr_and_names_its_type_when_its_message_does_not(capsys, monkeypatch, error, shown):
    # A JSON consumer of `list --format json` reads stdout: an error there would corrupt its input.
    def fail(args):
        raise error

    monkeypatch.setattr("valkey_fuzzer.commands.listing.list_command", fail)
    with patch("sys.argv", ["valkey-fuzzer", "list", "experiments", "--format", "json"]):
        assert main() == EXIT_HARNESS_ERROR
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err.strip() == shown
