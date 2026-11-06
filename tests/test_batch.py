"""The batch runner, with canned runs (tests/batch_worker_stub.py).

Most tests run the workers in this process (``workers``); the ``slow`` ones start real worker processes for
what needs them: parallel runs, a real leak and hang, and killing a batch.
"""

import argparse
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from valkey_fuzzer import batch, hoststate
from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.batch import PORT_BLOCK, BatchError, BatchOptions, BatchRunner, expand, normalize_source
from valkey_fuzzer.cli import main
from valkey_fuzzer.commands import batch as batch_command
from valkey_fuzzer.deployment.local import ports
from valkey_fuzzer.hoststate import OWNER_FILE, sweep
from valkey_fuzzer.results import OutcomeStatus, RunOutcome
from valkey_fuzzer.service import RunService

from .batch_worker_stub import InProcessWorkers
from .valkey_experiments_support import valkey_service

ROOT = Path(__file__).resolve().parent.parent
STUB = [sys.executable, str(ROOT / "tests" / "batch_worker_stub.py")]
SNAPSHOT = "valkey.snapshot-equivalence"
NO_ZSTD = ["server_config.rdbcompression=zstd", "server_config.repl-compression=zstd"]


def _id(seed):
    return f"random-{SNAPSHOT}-s{seed}"


@pytest.fixture
def stub(tmp_path, monkeypatch):
    """``behave(seed or build=behavior)`` sets what each canned run does; ``runs()`` is what each saw."""

    plan, log = tmp_path / "stub-plan.json", tmp_path / "stub-log.jsonl"
    plan.write_text("{}")
    monkeypatch.setenv("BATCH_STUB_PLAN", str(plan))
    monkeypatch.setenv("BATCH_STUB_LOG", str(log))
    # Test processes running at once (other checkouts' suites) would otherwise refuse each other's batches.
    monkeypatch.setattr(batch, "HOST_LOCK", str(tmp_path / "host-batch.lock"))
    monkeypatch.setattr(batch, "WORKER_COMMAND", tuple(STUB))
    return type("Stub", (), {
        "behave": staticmethod(lambda **items: plan.write_text(json.dumps({str(k).replace("_", "-"): v for k, v in items.items()}))),
        "runs": staticmethod(lambda: [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []),
    })()  # fmt: skip


@pytest.fixture
def workers(stub, monkeypatch):
    return InProcessWorkers(monkeypatch)


def _random(seeds, **extra):
    return normalize_source(
        {"random": {"experiments": [SNAPSHOT], "seeds": seeds, "first_seed": 0, "excludes": NO_ZSTD, **extra}}, "/"
    )


def _batch(tmp_path, sources, name="b", **options):
    options = BatchOptions(**{"port_base": 13000, **options})
    options.check()
    expansion = expand(sources, valkey_service(), options)
    return BatchRunner.create(str(tmp_path / name), options, sources, expansion)


def _log(runner):
    """The batch's batch.log, without its times."""

    return [line.split(" ", 1)[1] for line in Path(runner.folder, "batch.log").read_text().splitlines()]


def _results(runner):
    return {path.stem: json.loads(path.read_text()) for path in Path(runner.folder, "results").glob("*.json")}


def _leaked_and_removed(stub):
    leaked = [run for run in stub.runs() if "leaked_pid" in run or "leaked_dir" in run]
    for run in leaked:
        if "leaked_pid" in run:
            with pytest.raises(ProcessLookupError):
                os.kill(run["leaked_pid"], 0)
        else:
            assert not os.path.exists(run["leaked_dir"])
    return len(leaked)


def test_the_worker_isolates_its_run_keeps_a_zoo_build_and_refuses_a_scenario_too_big_for_its_ports(
    tmp_path, canned_runs, monkeypatch
):
    def task(job):
        (tmp_path / job["id"]).mkdir()
        (tmp_path / f"{job['id']}.json").write_text(json.dumps({
            "job": job, "report_dir": str(tmp_path / job["id"]), "base_port": 40000, "loopback_block": 1,
            "data_dir": str(tmp_path / "data"), "artifact_dir": str(tmp_path), "timeout_s": 5,
            "valkey_binary": "/default/valkey-server", "experiment_directories": [],
        }))  # fmt: skip
        return batch.run_worker(str(tmp_path / f"{job['id']}.json"))

    assert (
        task(
            {
                "id": "g",
                "generate": {"seed": 9, "experiment": SNAPSHOT, "excludes": NO_ZSTD, "base": {}, "profile": "pr-quick"},
            }
        )
        == 0
    )
    scenario = {"scenario_id": "given", "seed": 3, "experiment": {"id": SNAPSHOT},
                "deployment": {"mode": "standalone", "base_port": 7000, "valkey_binary": "/zoo/clean/src/valkey-server"}}  # fmt: skip
    assert task({"id": "s", "scenario": scenario}) == 0
    generated, given = canned_runs.plans
    assert {plan.deployment["base_port"] for plan in (generated, given)} == {40000}
    assert {plan.deployment["base_data_dir"] for plan in (generated, given)} == {str(tmp_path / "data")}
    assert (generated.deployment["valkey_binary"], given.deployment["valkey_binary"]) == (
        "/default/valkey-server",
        "/zoo/clean/src/valkey-server",
    )
    assert json.loads((tmp_path / "g" / "report.json").read_text())["reproducer"]["generated"]["profile"] == "pr-quick"
    assert yaml.safe_load((tmp_path / "s" / "scenario.yaml").read_text())["deployment"]["base_port"] == 40000

    # Linux's default ephemeral range: an outgoing connection there can take a port before Valkey binds it.
    monkeypatch.setattr(ports, "ephemeral_port_range", lambda: (32768, 60999))
    for options, message in (({"workers": 0}, "workers"), ({"workers": 90}, "use fewer workers"), ({"port_base": 80}, "at least 1024"),
                             ({"port_base": 60000, "workers": 2}, "or a lower port_base"), ({"timeout_s": -1}, "greater than zero"),
                             ({"port_base": 22600, "workers": 2}, "32600..32839 overlap this host's ephemeral port range "
                              "32768-60999, .*: pick a port_base of at most 22528")):  # fmt: skip
        with pytest.raises(BatchError, match=message):
            BatchOptions(**options).check()
    monkeypatch.setattr(batch, "PORT_BLOCK", 5)
    assert (
        task(
            {
                "id": "big",
                "scenario": {**scenario, "deployment": {"mode": "cluster", "primaries": 3, "replicas_per_primary": 1}},
            }
        )
        == 2
    )
    assert "plans 6 instances; a batch worker's port block holds 5" in (tmp_path / "big" / "error.json").read_text()
    assert len(canned_runs.plans) == 2, "nothing started"


def test_a_batch_folder_keeps_the_experiments_its_runs_load(tmp_path):
    first, second = tmp_path / "first", tmp_path / "second"
    for directory in (first, second):
        directory.mkdir()
    (first / "a.yaml").write_text("id: proposed.a\n")
    (first / "b.yml").write_text("id: proposed.b\n")
    (first / "notes.txt").write_text("not an experiment")
    (second / "c.yaml").write_text("id: proposed.c\n")

    runner = _batch(tmp_path, [_random(1)], experiment_directories=(str(first), str(second)))

    kept = Path(runner.folder, "experiments")
    assert sorted(path.name for path in kept.iterdir()) == ["a.yaml", "b.yml", "c.yaml"]
    assert (kept / "c.yaml").read_text() == "id: proposed.c\n"
    assert not Path(_batch(tmp_path, [_random(1)], name="none").folder, "experiments").exists()
    (second / "a.yaml").write_text("id: proposed.other\n")
    with pytest.raises(BatchError, match=r"two experiment directories hold a\.yaml"):
        _batch(tmp_path, [_random(1)], name="clash", experiment_directories=(str(first), str(second)))


def test_a_run_that_leaks_crashes_or_hangs_is_an_error_and_the_sweep_removes_what_it_left(
    tmp_path, workers, stub, monkeypatch
):
    monkeypatch.setattr(batch, "DEADLINE_GRACE_S", 0.0)
    behaviors = {
        "0": "leak-server",
        "1": "leak-dir",
        "2": "pass",
        "3": "crash",
        "4": "hang",
        "5": "leak-private-server",
    }
    stub.behave(**behaviors)
    summary = (runner := _batch(tmp_path, [_random(6)], workers=2, timeout_s=0.05)).run()

    server, directory, passed, crashed, hung, private = (_results(runner)[_id(seed)] for seed in range(6))
    assert summary["counts"] == {"ERROR": 5, "PASS": 1} and summary["exit_code"] == 2 and passed["status"] == "PASS"
    # The per-job sweep looks beyond the worker's data/ and tmp/: a private server's directory under /tmp is
    # reported and removed too.
    assert [leak["kind"] for leak in server["leaks"] + directory["leaks"] + private["leaks"]] == [
        "server",
        "directory",
        "directory",
    ]
    assert {leak["run"] for leak in summary["leaks"]} == {server["id"], directory["id"], private["id"]}
    assert _leaked_and_removed(stub) == 3
    # Known-bad: a run that passed its checks but leaked never says it passed.
    assert (
        server["summary"] == "passed its checks but left 1 leftover: server"
        and "leaked server" in server["signature_label"]
    )
    assert next(line for line in _log(runner) if server["id"] in line).startswith("[ERROR")
    # A worker that exits without a report, and one past its deadline (stopped, by SIGINT first).
    assert not crashed["timed_out"] and crashed["exit_code"] == 9 and "WorkerExit" in crashed["summary"]
    assert (
        hung["timed_out"]
        and hung["report"] is None
        and hung["exit_code"] == -2
        and "Deadline" in hung["signature_label"]
    )


def test_a_worker_is_interrupted_alone_and_only_a_kill_reaches_its_process_group(monkeypatch):
    # The worker's servers share its process group; its run stops them, so SIGINT and SIGTERM are its alone.
    sent = []
    monkeypatch.setattr(batch.os, "kill", lambda pid, sig: sent.append(("kill", pid, sig)))
    monkeypatch.setattr(batch.os, "killpg", lambda pid, sig: sent.append(("killpg", pid, sig)))
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        batch._signal_worker(7, sig)
    assert sent == [("kill", 7, signal.SIGINT), ("kill", 7, signal.SIGTERM), ("killpg", 7, signal.SIGKILL)]


def test_what_a_worker_leaves_in_its_process_group_is_killed_after_the_sweep(tmp_path, workers, stub):
    _batch(tmp_path, [_random(2)]).run()
    assert [sig for _, sig in workers.signals] == [signal.SIGKILL, signal.SIGKILL]


@pytest.mark.parametrize(
    ("rerun", "verdict"),
    [("fail:data", "reproduced"), ("pass", "not reproduced"), ("fail:dataset", "failed differently")],
)
def test_failures_are_grouped_by_the_minimizers_signature_and_rerun_serially(tmp_path, workers, stub, rerun, verdict):
    stub.behave(**{"0": "fail:data", "1": "fail:data", "2": "fail:dataset", "3": "error", "4": "pass"})
    runner = _batch(tmp_path, [_random(5)], workers=2, serial_reruns=1)
    runner.serial_jobs = lambda: []
    summary = runner.run()
    # "lost 0 keys" and "lost 1 keys" mask to the same verdict: one signature.
    data = next(group for group in summary["signatures"] if group["runs"] == [_id(0), _id(1)])
    assert (
        data["status"] == "FAIL"
        and "data:FAIL" in data["label"]
        and data["signature"]["failing_components"] == ["data"]
    )
    assert sorted(group["count"] for group in summary["signatures"]) == [1, 1, 2]
    assert any("TimeoutError during workload" in group["label"] for group in summary["signatures"])
    assert summary["counts"] == {"FAIL": 3, "ERROR": 1, "PASS": 1} and summary["exit_code"] == 1

    del runner.serial_jobs
    stub.behave(**{"0": rerun, "1": "fail:data", "2": "fail:dataset", "3": "error"})
    runner.stop_requested = False
    reruns = next(group for group in runner.run()["signatures"] if group["runs"] == [_id(0), _id(1)])["serial_reruns"]
    assert (
        reruns["verdict"] == verdict and len(reruns["runs"]) == 1 and reruns["reproduced"] == (verdict == "reproduced")
    )


def test_only_failed_and_errored_runs_are_rerun_serially(tmp_path, workers, stub):
    # Load cannot explain an unsupported run; re-running it serially tells nothing.
    stub.behave(**{"0": "skip", "1": "fail:data", "2": "error"})
    runner = _batch(tmp_path, [_random(3)], serial_reruns=1)
    runner.run()
    assert sorted(job["origin"]["job"] for job in runner.serial_jobs()) == [_id(1), _id(2)]


def test_a_later_batch_reruns_one_sample_of_each_failure_of_an_earlier_batch(tmp_path, workers, stub):
    stub.behave(**{"0": "fail:data", "1": "fail:data", "2": "pass", "3": "error"})
    first = _batch(tmp_path, [_random(4)], name="first", workers=2)
    first.run()
    failures = [normalize_source({"failures": {"batch": first.folder, "per_signature": 1}}, "/")]
    second = _batch(tmp_path, failures, name="second")
    assert [job["id"] for job in second.jobs] == [f"rerun-{_id(0)}", f"rerun-{_id(3)}"]
    # The scenario each failed run drew, not its seed: code changed since (a fix, say) would draw another one.
    for job, seed in zip(second.jobs, (0, 3)):
        recorded = yaml.safe_load(Path(first.folder, "runs", _id(seed), "scenario.yaml").read_text())
        assert "generate" not in job and job["scenario"] == recorded and recorded["seed"] == seed
    # A random job that never drew a scenario (its draw failed) can only draw again.
    Path(first.folder, "runs", _id(3), "scenario.yaml").unlink()
    assert second.jobs[1]["scenario"] and _batch(tmp_path, failures, name="redraw").jobs[1]["generate"]["seed"] == 3
    summary = second.run()
    assert summary["counts"] == {"FAIL": 1, "ERROR": 1} and {run["origin"]["job"] for run in summary["runs"]} == {
        _id(0),
        _id(3),
    }


def test_a_resumed_batch_sweeps_and_reruns_only_the_job_that_was_running_and_one_process_runs_it(
    tmp_path, workers, stub
):
    runner = _batch(tmp_path, [_random(3)])
    runner.run()
    # As a killed batch leaves it: no result for s1, its worker in workers.json, and what that worker left.
    Path(runner.folder, "results", f"{_id(1)}.json").unlink()
    owner = InProcessWorkers.owner(5_999_999)
    leftover = Path(runner.folder, "work", "w0", "data", "deployment-left")
    leftover.mkdir(parents=True)
    (leftover / OWNER_FILE).write_text(json.dumps({"owner": owner.token, "pid": owner.pid, "kind": "deployment"}))
    Path(runner.folder, "workers.json").write_text(
        json.dumps({"0": {"slot": 0, "job": _id(1), "pid": owner.pid, "owner": owner.token}})
    )

    summary = BatchRunner(runner.folder).run()

    assert summary["complete"] and summary["counts"] == {"PASS": 3} and summary["exit_code"] == 0
    assert sorted(run["seed"] for run in stub.runs()) == [0, 1, 1, 2], "only the interrupted job runs again"
    assert f"interrupted job {_id(1)}: removed 1 leftover(s)" in _log(runner) and not leftover.exists()
    assert not Path(runner.folder, "workers.json").exists() and not Path(runner.folder, "work").exists()
    with open(Path(runner.folder, "batch.lock"), "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(BatchError, match="another process is running"):
            runner.run()


def test_a_resume_keeps_the_run_folder_of_a_job_that_finished_before_its_worker_was_forgotten(tmp_path, workers, stub):
    runner = _batch(tmp_path, [_random(1)])
    runner.run()
    # Killed between writing the job's result and rewriting workers.json.
    owner = InProcessWorkers.owner(5_999_998)
    Path(runner.folder, "workers.json").write_text(
        json.dumps({"0": {"slot": 0, "job": _id(0), "pid": owner.pid, "owner": owner.token}})
    )

    summary = BatchRunner(runner.folder).run()

    assert summary["counts"] == {"PASS": 1} and len(stub.runs()) == 1
    assert Path(runner.folder, _results(runner)[_id(0)]["report"]).exists()


def test_a_worker_slot_whose_sweep_could_not_remove_a_leftover_runs_no_more_jobs(tmp_path, workers, stub):
    runner = _batch(tmp_path, [_random(4)], workers=2)
    sweep_slot, failed = runner._sweep, {"kind": "mount", "target": "/m", "owner": "1-000000", "age_s": None}
    stuck = {"slot": None}

    def sweep_once(slot, owner):
        # The first job's sweep cannot unmount what it left: that slot's ports and addresses stay taken.
        if stuck["slot"] is None:
            stuck["slot"] = slot
            return [{**failed, "detail": "", "action": "failed", "reason": "umount: busy"}]
        return sweep_slot(slot, owner)

    runner._sweep = sweep_once
    summary = runner.run()

    results = _results(runner)
    first = next(result for result in results.values() if result["leaks"])
    assert summary["complete"] and summary["counts"] == {"ERROR": 1, "PASS": 3}
    assert [result["worker"] for result in results.values() if result is not first] == [1 - stuck["slot"]] * 3
    assert any(f"worker slot {stuck['slot']} runs no more jobs" in line for line in _log(runner))

    # With every slot retired the batch stops, and a resume (after a sweep) runs the rest.
    runner = _batch(tmp_path, [_random(2)], name="one")
    runner._sweep = lambda slot, owner: [{**failed, "detail": "", "action": "failed", "reason": "umount: busy"}]
    summary = runner.run()
    assert not summary["complete"] and summary["finished"] == 1 and summary["exit_code"] == 130


def test_a_resume_runs_nothing_on_a_slot_whose_interrupted_job_left_what_the_sweep_could_not_remove(
    tmp_path, workers, stub
):
    runner = _batch(tmp_path, [_random(2)], workers=2)
    runner.run()
    Path(runner.folder, "results", f"{_id(1)}.json").unlink()
    owner = InProcessWorkers.owner(5_999_997)
    Path(runner.folder, "workers.json").write_text(
        json.dumps({"0": {"slot": 0, "job": _id(1), "pid": owner.pid, "owner": owner.token}})
    )
    resumed = BatchRunner(runner.folder)
    sweep_slot = resumed._sweep
    # A chain a root batch left, swept by a resume that is not root: it would partition slot 0's addresses.
    stuck = {"kind": "chain", "target": "VF-x", "owner": owner.token, "age_s": None, "detail": "", "action": "failed",
             "reason": "permission denied"}  # fmt: skip
    gone = {**stuck, "kind": "directory", "target": "/d", "action": "removed", "reason": ""}
    resumed._sweep = lambda slot, token: [stuck, gone] if token == owner.token else sweep_slot(slot, token)

    summary = resumed.run()

    assert summary["complete"] and summary["counts"] == {"PASS": 2}
    assert _results(resumed)[_id(1)]["worker"] == 1, "the job runs again on the other slot"
    log = _log(resumed)
    assert f"interrupted job {_id(1)}: removed 1 leftover(s), could not remove 1 (permission denied)" in log
    assert any("worker slot 0 runs no more jobs" in line for line in log)


def test_a_batch_removes_what_dead_fuzzers_left_on_the_host_before_its_first_job(tmp_path, workers, stub, monkeypatch):
    # A killed batch's chain matches the addresses of the next batch's slots: left in place, it is a partition
    # nobody configured, and a FAIL Valkey did not cause.
    runner, swept = _batch(tmp_path, [_random(1)]), []
    chain = hoststate.Leftover("chain", "VF-1-000000-00000000", "1-000000", None)
    worker_sweep = batch.sweep

    def host_sweep(roots=None, *, owners=None, **options):
        if owners is not None:
            return worker_sweep(roots, owners=owners, **options)
        swept.append(roots)
        return hoststate.SweepReport(dry_run=False, leftovers=[chain])

    monkeypatch.setattr(batch, "sweep", host_sweep)
    chain.action, chain.reason = "failed", "iptables: busy"
    with pytest.raises(BatchError, match=r"could not remove 1 leftover.*iptables: busy.*sweep the host"):
        runner.run()
    assert swept == [[]] and stub.runs() == []

    chain.action, chain.reason = "removed", ""
    assert runner.run()["complete"] and len(stub.runs()) == 1
    assert "removed 1 leftover(s) of dead fuzzer processes: chain VF-1-000000-00000000" in _log(runner)


def test_a_report_the_batch_cannot_read_is_an_error_but_a_bug_in_its_signature_is_raised(tmp_path, monkeypatch):
    job = {"id": "j", "source": "scenarios", "scenario": {"seed": 1, "experiment": {"id": SNAPSHOT}}}
    report = {"status": "FAIL", "summary": "lost keys", "outcome": {"status": "FAIL", "plan": {"seed": "x"}}}
    (tmp_path / "report.json").write_text(json.dumps(report))
    result = batch.job_result(job, str(tmp_path), exit_code=1, timed_out=False, leaks=[], extra={})
    assert result["status"] == "ERROR" and result["signature"]["error_type"] == "UnreadableReport"

    # Known-bad: an internal error is no unreadable report, which would turn a real FAIL into an ERROR.
    scenario = {"scenario_id": "j", "seed": 1, "experiment": {"id": SNAPSHOT}, "deployment": {"mode": "standalone"}}
    outcome = RunOutcome(plan=valkey_service().compile(scenario), status=OutcomeStatus.FAIL, summary="lost keys")
    (tmp_path / "report.json").write_text(json.dumps({**report, "outcome": to_plain_data(outcome)}))
    monkeypatch.setattr(batch, "failure_signature", lambda *args: {}["missing"])
    with pytest.raises(KeyError):
        batch.job_result(job, str(tmp_path), exit_code=1, timed_out=False, leaks=[], extra={})


def test_a_second_ctrl_c_sends_the_next_signal_at_once(tmp_path, workers, stub):
    runner = _batch(tmp_path, [_random(1)])
    worker = type("Worker", (), {"pid": 42})()
    now = time.monotonic()
    item = batch._Running(job=runner.jobs[0], slot=0, process=worker, owner="42-000000", started=now,
                          deadline=now + 600, load_start=0.0)  # fmt: skip
    runner._running[0] = item
    runner._request_stop(signal.SIGINT, None)
    runner._escalate(item)
    runner._escalate(item)
    assert workers.signals == [(42, signal.SIGINT)], "the next signal waits its grace period"
    runner._request_stop(signal.SIGINT, None)
    runner._escalate(item)
    runner._request_stop(signal.SIGINT, None)
    runner._escalate(item)
    assert workers.signals == [(42, signal.SIGINT), (42, signal.SIGTERM), (42, signal.SIGKILL)]


def test_a_matrix_files_cases_that_do_not_compile_but_raises_a_compiler_bug(monkeypatch):
    base = {"seed": 1, "experiment": {"id": SNAPSHOT}, "deployment": {"mode": "standalone"}}
    source = normalize_source(
        {"matrix": {"base": base, "dimensions": {"deployment.mode": ["standalone", "nope"]}}}, "/"
    )
    service, options = valkey_service(), BatchOptions()
    expansion = expand([source], service, options)
    assert [job["scenario"]["deployment"]["mode"] for job in expansion.jobs] == ["standalone"]
    assert [case["assignment"] for case in expansion.invalid] == [{"deployment.mode": "nope"}]

    # An internal error is not "a combination the experiment does not support": it stops the batch.
    monkeypatch.setattr(service, "compile", lambda scenario: {}["missing"])
    with pytest.raises(KeyError):
        expand([source], service, options)


def test_a_matrix_case_that_does_not_compile_keeps_the_batch_from_passing(tmp_path, workers, stub):
    # A misspelled dimension value would otherwise leave a PASS batch over the other combinations.
    base = {"scenario_id": "m", "seed": 1, "experiment": {"id": SNAPSHOT}, "deployment": {"mode": "standalone"}}
    source = normalize_source(
        {"matrix": {"base": base, "dimensions": {"deployment.mode": ["standalone", "nope"]}}}, "/"
    )
    summary = _batch(tmp_path, [source]).run()
    assert summary["counts"] == {"PASS": 1} and len(summary["invalid"]) == 1
    assert summary["status"] == "ERROR" and summary["exit_code"] == 2


def test_a_batch_over_the_job_limit_is_refused_while_its_sources_expand(monkeypatch):
    monkeypatch.setattr(batch, "MAX_JOBS", 3)
    service, compiled = valkey_service(), []
    monkeypatch.setattr(service, "compile", compiled.append)
    base = {"seed": 1, "experiment": {"id": SNAPSHOT}, "deployment": {"mode": "standalone"}}
    matrix = normalize_source(
        {"matrix": {"base": base, "dimensions": {"deployment.base_port": list(range(1000))}}}, "/"
    )
    with pytest.raises(BatchError, match="more than 3 jobs"):
        expand([matrix], service, BatchOptions())
    assert len(compiled) == 4, "the expansion stops at the limit"


def test_a_second_batch_on_the_host_is_refused_before_it_starts_a_job(tmp_path, workers, stub):
    # Every batch's workers use loopback blocks 1, 2, ...: a second batch's jobs would all fail to claim them.
    runner = _batch(tmp_path, [_random(1)])
    with open(batch.HOST_LOCK, "w") as other:
        fcntl.flock(other, fcntl.LOCK_EX)
        with pytest.raises(BatchError, match="another batch is running on this host"):
            runner.run()
    assert stub.runs() == []
    assert runner.run()["complete"]


COMMIT = "f2ca837edc75e2238f880d89ade60babb6e6ed9e"


def test_zoo_cases_run_through_the_batch_and_one_that_does_not_hold_fails_it(tmp_path, workers, stub):
    zoo, builds = tmp_path / "zoo", tmp_path / "builds"
    zoo.mkdir()
    (zoo / "valkey-commit").write_text(COMMIT + "\n")
    scenario = {
        "scenario_id": "zoo",
        "seed": 100,
        "experiment": {"id": SNAPSHOT},
        "deployment": {"mode": "standalone", "replicas_per_primary": 1},
    }
    # mis-specified expects the snapshot oracle to fail, but its build breaks the data check.
    for case, expected in (("drops-writes", "data"), ("mis-specified", "snapshot")):
        (zoo / case).mkdir()
        (zoo / case / "patch.diff").write_text("")
        (zoo / case / "scenario.yaml").write_text(yaml.safe_dump(scenario))
        (zoo / case / "case.yaml").write_text(yaml.safe_dump({"description": case, "catches": "data", "runs": 5, "min_detection_rate": 1.0,
                                                              "scenarios": [{"file": "scenario.yaml", "fail": [expected]}]}))  # fmt: skip
    for build in ("clean", "drops-writes", "mis-specified"):
        (builds / build / "src").mkdir(parents=True)
        (builds / build / "src" / "valkey-server").write_text(f"#!/bin/sh\necho 'Valkey server v=255.255.255 sha={COMMIT[:8]}:0 malloc=libc'\n")  # fmt: skip
        (builds / build / "src" / "valkey-server").chmod(0o755)
    stub.behave(clean="pass", drops_writes="fail:data", mis_specified="fail:data")
    sources = [_random(2), normalize_source({"zoo": {"builds": str(builds), "directory": str(zoo)}}, "/")]
    runner = _batch(tmp_path, sources, workers=3)
    assert len([job for job in runner.jobs if "zoo" in job]) == 12

    summary = runner.run()

    cases = {case["case"]: case for case in summary["zoo"]["cases"]}
    assert cases["drops-writes"]["holds"] is True and [row["status"] for row in cases["drops-writes"]["controls"]] == [
        "PASS"
    ]
    assert cases["drops-writes"]["scenarios"]["scenario.yaml"]["detection_rate"] == 1.0
    assert cases["mis-specified"]["holds"] is False and summary["zoo"]["not_held"] == ["mis-specified"]
    # Every other run passed and the zoo's expected FAILs are no findings, yet the batch fails.
    assert summary["counts"] == {"PASS": 2} and not summary["signatures"] and summary["exit_code"] == 1
    # The verdict is the batch's: editing or removing a case later does not change it.
    zoo.rename(tmp_path / "zoo-gone")
    assert runner.summary()["zoo"]["not_held"] == ["mis-specified"]
    (tmp_path / "zoo-gone").rename(zoo)
    # A control the harness could not observe leaves a case undecided: the batch is an ERROR, not a finding.
    Path(runner.folder, "results", "zoo-drops-writes-scenario-control.json").unlink()
    stub.behave(clean="error", drops_writes="fail:data", mis_specified="fail:data")
    summary = BatchRunner(runner.folder).run()
    cases = {case["case"]: case for case in summary["zoo"]["cases"]}
    assert cases["drops-writes"]["holds"] is None and cases["drops-writes"]["undecided"] == "ERROR"
    assert summary["zoo"]["undecided"] == ["drops-writes"] and summary["status"] == "FAIL"
    (builds / "clean" / "src" / "valkey-server").write_text("#!/bin/sh\necho 'Valkey server sha=deadbeef:0'\n")
    with pytest.raises(Exception, match="not built from the pinned commit"):
        _batch(tmp_path, sources, name="other-commit")


def test_a_profile_sets_the_seeds_and_deadline_and_pruning_keeps_a_passing_runs_report(tmp_path, workers, stub):
    stub.behave(**{"1": "fail:data"})
    sources = [normalize_source({"random": {"experiments": [SNAPSHOT], "first_seed": 0, "excludes": NO_ZSTD}}, "/")]
    runner = _batch(tmp_path, sources, profile="pr-quick", workers=3, prune_pass=True)
    runner.run()
    # The profile's seeds and deadline, unless the deadline is given.
    assert len(runner.jobs) == 3 and {run["timeout"] for run in stub.runs()} == {600}
    assert _batch(tmp_path, sources, name="explicit", profile="pr-quick", timeout_s=30).timeout_s == 30
    assert {item.name for item in Path(runner.folder, "runs", _id(0)).iterdir()} == {"report.json", "scenario.yaml"}
    assert "plan.json" in {item.name for item in Path(runner.folder, "runs", _id(1)).iterdir()}


def test_the_command_runs_a_batch_and_exits_with_its_worst_result(tmp_path, workers, stub, capsys):
    common = [
        "--random",
        SNAPSHOT,
        "--seeds",
        "2",
        "--first-seed",
        "0",
        *[arg for item in NO_ZSTD for arg in ("--exclude", item)],
        "--port-base",
        "12000",
    ]

    def run(*argv):
        with patch("sys.argv", ["valkey-fuzzer", "batch", *argv]):
            return main()

    assert run(*common, "--dry-run") == 0 and _id(1) in capsys.readouterr().out
    assert run(*common, "--out", str(tmp_path / "ok"), "--workers", "2") == 0
    stub.behave(**{"1": "skip"})
    assert run(*common, "--out", str(tmp_path / "skip")) == 3
    assert run("--resume", str(tmp_path / "skip"), "--allow-unsupported") == 0
    stub.behave(**{"0": "error", "1": "fail:data"})
    assert run(*common, "--out", str(tmp_path / "bad")) == 1 and "data:FAIL" in capsys.readouterr().out
    assert run("--resume", str(tmp_path / "ok"), "--random", SNAPSHOT) == 2


@pytest.mark.slow
def test_real_workers_run_in_parallel_in_isolation_and_a_leaked_or_hung_one_is_cleaned_up(tmp_path, stub, monkeypatch):
    monkeypatch.setattr(batch, "DEADLINE_GRACE_S", 0.0)
    monkeypatch.setattr(batch, "STOP_GRACE_S", (0.5, 0.5))
    stub.behave(**{"0": "sleep:0.6", "1": "sleep:0.6", "2": "sleep:0.6", "3": "leak-server", "4": "hang"})
    started = time.monotonic()
    summary = (runner := _batch(tmp_path, [_random(5)], workers=3, timeout_s=3)).run()

    assert time.monotonic() - started < 60 and summary["counts"] == {"PASS": 3, "ERROR": 2}
    assert _results(runner)[_id(4)]["timed_out"] and _leaked_and_removed(stub) == 1
    runs = stub.runs()
    overlapping = [(a, b) for a in runs for b in runs if a is not b and a["start"] < b["end"] and b["start"] < a["end"]]
    assert overlapping, "the workers ran in parallel"
    for a, b in overlapping:
        assert (
            a["base_port"] != b["base_port"]
            and not a["data_dir"].startswith(b["data_dir"])
            and not a["tmpdir"].startswith(b["tmpdir"])
        )
    assert {run["base_port"] for run in runs} <= {13000 + slot * PORT_BLOCK for slot in range(3)}
    for result in _results(runner).values():
        if result["report"]:
            assert result["ports"][1] - result["ports"][0] == PORT_BLOCK - 1
            assert (
                json.loads(Path(runner.folder, result["report"]).read_text())["scenario"]["deployment"]["base_port"]
                == result["ports"][0]
            )
            assert {"plan.json", "scenario.yaml", "console.log"} <= {
                item.name for item in Path(runner.folder, "runs", result["id"]).iterdir()
            }
    assert not any(Path(runner.folder, "work").glob("w*/*")), "worker scratch is wiped after every job"


_DRIVER = "import sys; sys.path.insert(0, {root!r}); from tests.test_batch import STUB; from valkey_fuzzer import batch; batch.WORKER_COMMAND = tuple(STUB); batch.BatchRunner({folder!r}).run()"


@pytest.mark.slow
@pytest.mark.parametrize("kill", [signal.SIGKILL, signal.SIGTERM])
def test_a_killed_batch_resumes_without_rerunning_finished_jobs_or_leaving_anything(tmp_path, stub, kill):
    stub.behave(**{str(seed): "sleep:1" for seed in range(5)})
    runner, results = _batch(tmp_path, [_random(5)], workers=2), Path(tmp_path, "b", "results")
    script = _DRIVER.format(root=str(ROOT), folder=runner.folder)
    driver = subprocess.Popen([sys.executable, "-c", script], cwd=str(ROOT), start_new_session=True)
    deadline = time.monotonic() + 60
    while not (results.exists() and len(list(results.glob("*.json"))) >= 2):
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.1)
    os.kill(driver.pid, kill)
    driver.wait(timeout=60)
    finished = {path.stem for path in results.glob("*.json")}
    assert 2 <= len(finished) < 5
    if kill == signal.SIGTERM:
        # A stopped batch interrupts its runs, records no result for them, and says how far it got.
        assert driver.returncode in (0, -signal.SIGTERM) and not Path(runner.folder, "workers.json").exists()
        assert not json.loads(Path(runner.folder, "summary.json").read_text())["complete"]

    summary = BatchRunner(runner.folder).run()

    assert summary["complete"] and summary["counts"] == {"PASS": 5} and summary["exit_code"] == 0
    runs = stub.runs()
    assert all(len([run for run in runs if run["seed"] == int(job.rsplit("-s", 1)[1])]) == 1 for job in finished), (
        "finished jobs never rerun"
    )
    owners = {f"{run['pid']}" for run in runs}
    assert not [item for item in sweep(dry_run=True).leftovers if item.owner.split("-")[0] in owners], "nothing is left"
    assert not Path(runner.folder, "workers.json").exists() and not Path(runner.folder, "work").exists()


RESUME_REFUSES = [
    ["--dry-run"], ["--timeout", "5"], ["--profile", "adversarial"], ["--port-base", "15000"],
    ["--serial-reruns", "1"], ["--prune-pass"], ["--valkey-binary", "/other/valkey-server"],
    ["--batch-id", "x"], ["--out", "elsewhere"], ["--experiment-dir", "experiments"],
    ["--artifact-dir", "artifacts"], ["--random", SNAPSHOT], ["--seeds", "3"],
]  # fmt: skip


def test_a_resume_refuses_every_option_but_workers_and_allow_unsupported(tmp_path, workers, stub, capsys):
    # The folder holds the batch's options; a resume that took others would silently ignore them, and a
    # --dry-run would run the jobs.
    runner = _batch(tmp_path, [_random(2)])
    for option in RESUME_REFUSES:
        with patch("sys.argv", ["valkey-fuzzer", "batch", "--resume", runner.folder, *option]):
            assert main() == 2, option
        assert "--resume takes only --workers and --allow-unsupported" in capsys.readouterr().err, option
    assert stub.runs() == []


def test_a_resume_refuses_an_option_added_to_the_command_later(tmp_path, workers, stub):
    # A new option a resume would silently ignore, unless the refusal follows the parser.
    subparsers = argparse.ArgumentParser().add_subparsers()
    batch_command.register(subparsers)
    parser = subparsers.choices["batch"]
    parser.add_argument("--new-option")
    runner = _batch(tmp_path, [_random(1)])
    args = parser.parse_args(["--resume", runner.folder, "--new-option", "x"])
    with pytest.raises(BatchError, match="not --new-option"):
        args.handler(args)
    assert stub.runs() == []


def test_a_resume_refuses_random_jobs_that_the_installed_code_would_draw_differently(
    tmp_path, workers, stub, monkeypatch, capsys
):
    runner = _batch(tmp_path, [_random(2)])
    with monkeypatch.context() as changed:
        changed.setattr("valkey_fuzzer.provenance.source_digest", lambda packages: "changed")
        with patch("sys.argv", ["valkey-fuzzer", "batch", "--resume", runner.folder]):
            assert main() == 2
    assert stub.runs() == []
    assert "would draw other scenarios" in capsys.readouterr().err
    resume = ["valkey-fuzzer", "batch", "--resume", runner.folder, "--workers", "2"]
    with patch("sys.argv", resume):
        assert main() == 0
    assert len(stub.runs()) == 2


@pytest.fixture
def coverage(workers, monkeypatch):
    """Fake code coverage: ``reach(plan)`` is what a run reaches; by default one feature per drawn value."""

    plans, canned = [], RunService.run_plan

    def run_plan(self, plan, **kwargs):
        plans.append(plan)
        return canned(self, plan, **kwargs)

    fake = {"reach": lambda plan: [f"{name}={value}" for name, value in plan.inputs.items()]}
    monkeypatch.setattr(RunService, "run_plan", run_plan)
    # The worker reads its run's coverage right after the run, in this process.
    monkeypatch.setattr(batch, "gcov_features", lambda directory: fake["reach"](plans[-1]))
    return fake


def _drawn(runner, job_id):
    return json.loads(Path(runner.folder, "runs", job_id, "report.json").read_text())["reproducer"]["generated"]


def test_a_guided_batch_derives_its_runs_from_those_that_reached_new_code(tmp_path, workers, stub, coverage):
    runner = _batch(tmp_path, [_random(3, guided=6)])
    summary = runner.run()

    results = _results(runner)
    guided = [job for job in runner.jobs if job["source"] == "guided"]
    assert summary["complete"] and summary["counts"] == {"PASS": 9} and len(guided) == 6
    corpus = json.loads(Path(runner.folder, "corpus.json").read_text())
    entries = [entry["job"] for entry in corpus["entries"]]
    # The first run reaches everything first; a run's new features are those no run before it reached.
    assert entries[0] == _id(0) and all(results[job]["code_coverage"]["new"] > 0 for job in entries)
    assert summary["code_coverage"] == {"covered": corpus["covered"], "corpus": len(entries)}
    assert sum(result["code_coverage"]["new"] for result in results.values()) == corpus["covered"]
    for job in guided:
        parent, changed = job["guided"]["parent"], job["guided"]["mutation"]
        assert parent in entries and results[job["id"]]["guided"] == job["guided"]
        before, after = _drawn(runner, parent), _drawn(runner, job["id"])
        # A new seed, and every draw but the one it changes kept (snapshot-equivalence has no hooks).
        assert after["seed"] == job["generate"]["seed"] != before["seed"]
        assert job["generate"]["pinned"] == {name: value for name, value in before["draws"].items() if name != changed}
        assert all(after["draws"][name] == value for name, value in job["generate"]["pinned"].items())
        assert changed == "seed" or changed in before["draws"]
    assert all(" coverage +" in line for line in _log(runner))


def test_a_guided_batch_without_coverage_draws_fresh_seeds_and_a_resume_draws_the_rest(
    tmp_path, workers, stub, coverage, monkeypatch
):
    def unreadable(plan):
        raise OSError("gcov: not found")

    coverage["reach"] = unreadable
    runner = _batch(tmp_path, [_random(1, guided=3)])
    # Stopped once it drew its first guided job, before that job starts.
    drawn = BatchRunner._next_guided

    def stop_after(self):
        job = drawn(self)
        self.stop_requested = True
        return job

    with monkeypatch.context() as stopping:
        stopping.setattr(BatchRunner, "_next_guided", stop_after)
        assert not runner.run()["complete"]
    assert [job["source"] for job in runner.jobs] == ["random", "guided"] and len(_results(runner)) == 1

    summary = BatchRunner(runner.folder).run()

    jobs = json.loads(Path(runner.folder, "jobs.json").read_text())
    assert summary["complete"] and summary["counts"] == {"PASS": 4} and len(stub.runs()) == 4
    assert [job["source"] for job in jobs] == ["random"] + ["guided"] * 3 and len({job["id"] for job in jobs}) == 4
    # With no corpus, a guided job is a fresh seed; a run whose coverage cannot be read keeps its result.
    assert all(job["guided"]["parent"] is None and "pinned" not in job["generate"] for job in jobs[1:])
    assert all(result["code_coverage"] is None for result in _results(runner).values())
    assert summary["code_coverage"] == {"covered": 0, "corpus": 0}
    assert (
        "No code coverage: OSError: gcov: not found"
        in Path(runner.folder, "runs", jobs[1]["id"], "console.log").read_text()
    )
