"""Fault ownership: a change to host or server state is owned by an undo obligation until it is undone.

Known-bad cases: a fault that forgets its undo, an undo that fails, a change of an undeclared kind or after its
owner finished. Each makes the run ERROR, and the runner undoes what was left; an undo run twice is harmless.
"""

import threading
import time

import pytest

from valkey_fuzzer._data import to_plain_data
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExperimentSpec, RunMode
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration, DeploymentProviderRegistration
from valkey_fuzzer.results import ComponentStatus, OutcomeStatus, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema
from valkey_fuzzer.testing import FakeDeploymentProvider, run_experiment

# What every undo did, in order, per test.
UNDONE: list[str] = []
# Undo attempts that fail before one succeeds, per target.
FAILURES: dict[str, int] = {}


@pytest.fixture(autouse=True)
def _reset():
    UNDONE.clear()
    FAILURES.clear()


def _undo(target):
    def undo():
        if target == "ctrl-c":
            # Ctrl-C while the runner's undo restarts a killed instance (a 30-60 s readiness wait).
            raise KeyboardInterrupt
        if FAILURES.get(target, 0) > 0:
            FAILURES[target] -= 1
            raise OSError(f"cannot undo {target}")
        UNDONE.append(target)

    return undo


class Fault:
    """Applies one change per target at inject; ``recover`` undoes them unless told to forget."""

    def inject(self, context, spec):
        self._obligations = [
            context.obligations.open(spec, spec.config["kind"], target, _undo(target))
            for target in spec.config["targets"]
        ]
        return make_result(spec, ComponentStatus.PASS, "applied")

    def recover(self, context, spec):
        if spec.config["forget"]:
            return make_result(spec, ComponentStatus.PASS, "recovered (it says)")
        errors = []
        for obligation in reversed(self._obligations):
            for _ in range(spec.config["discharges"]):
                try:
                    obligation.discharge()
                except OSError as exc:
                    errors.append(str(exc))
        return make_result(spec, ComponentStatus.ERROR if errors else ComponentStatus.PASS, "; ".join(errors) or "ok")


class Late:
    """An action that changes state after its call returned, from a callback it left behind."""

    def run(self, context, spec):
        context.event_bus.subscribe("validation.completed", lambda event: self._change(context, spec))
        return make_result(spec, ComponentStatus.PASS, "ran")

    def _change(self, context, spec):
        try:
            context.obligations.open(spec, "tests.late", "late", _undo("late"))
        except Exception:
            pass


FAULT_CONFIG = ConfigSchema({"kind": ConfigField(str, default="tests.stopped"), "targets": ConfigField(list, default=lambda: ["one"]),
                             "forget": ConfigField(bool, default=False), "discharges": ConfigField(int, default=1)})  # fmt: skip
REGISTRATIONS = (
    ComponentRegistration("tests.fault.change", ComponentRole.FAULT, Fault, config=FAULT_CONFIG,
                          declares=Declarations(changes=("tests.stopped",))),
    ComponentRegistration("tests.action.late", ComponentRole.ACTION, Late, declares=Declarations(changes=("tests.late",))),
)  # fmt: skip


def _run(**config):
    components = [ComponentSpec(use="tests.fault.change", id="fault", config=config)]
    if config.pop("late", False):
        components = [ComponentSpec(use="tests.action.late", id="late")]
    experiment = ExperimentSpec(
        experiment_id="tests.ownership", mode=RunMode.SMOKE, deployment_modes=("fake",), components=tuple(components)
    )
    return run_experiment(experiment, REGISTRATIONS)


def test_a_change_its_owner_undoes_once_passes_and_is_reported():
    outcome = _run(targets=["one", "two"], discharges=2)

    assert outcome.status is OutcomeStatus.PASS, outcome.summary
    assert UNDONE == ["two", "one"]
    report = to_plain_data(outcome.diagnostics["obligations"])
    assert report["planned"] == [{"owner": "fault", "kinds": ["tests.stopped"]}]
    assert [(item["target"], item["undone_by"]) for item in report["undone"]] == [("one", "owner"), ("two", "owner")]
    assert report["still_active"] == [] and "open_obligations" not in outcome.diagnostics


@pytest.mark.parametrize(
    ("config", "failures", "undone", "summary", "errors_by"),
    [
        # The runner undoes what a fault forgot, newest first.
        ({"targets": ["one", "two"], "forget": True}, 0, ["two", "one"],
         "component 'fault' did not undo its tests.stopped on one: it was undone by the runner", []),
        ({}, 1, ["one"], "after a failed undo", ["owner"]),
        # Still failing: the runner retries at the end of the run and at final cleanup, and it stays active.
        ({}, 10, [], "it is still active", ["owner", "runner", "runner"]),
    ],
    ids=["forgotten", "failed-once", "keeps-failing"],
)  # fmt: skip
def test_a_change_its_owner_did_not_undo_makes_the_run_an_error(config, failures, undone, summary, errors_by):
    FAILURES["one"] = failures
    outcome = _run(**config)

    assert outcome.status is OutcomeStatus.ERROR and summary in outcome.summary, outcome.summary
    assert undone == UNDONE
    obligations = to_plain_data(outcome.diagnostics["obligations"])
    left = obligations["still_active"] or to_plain_data(outcome.diagnostics["open_obligations"])
    assert all(item["undone_by"] == "runner" for item in left if "undone_by" in item)
    assert [error["by"] for error in left[-1].get("errors", [])] == errors_by
    assert all(
        error["type"] == "OSError" and error["message"] == "cannot undo one" for error in left[-1].get("errors", [])
    )


@pytest.mark.parametrize(
    ("config", "message", "undone"),
    [({"kind": "tests.other"}, "changed undeclared state 'tests.other'", "one"),
     ({"late": True}, "changed 'tests.late' after its last call returned", "late")],
)  # fmt: skip
def test_a_change_the_owner_may_not_make_is_an_error_and_still_undone(config, message, undone):
    outcome = _run(**config)

    assert outcome.status is OutcomeStatus.ERROR
    [violation] = outcome.diagnostics["contract_violations"]
    assert violation["kind"] == "change" and message in violation["message"]
    assert [undone] == UNDONE


# The abandoned inject calls' threads, the gate they block on, and what a late open raised.
THREADS: list[threading.Thread] = []
GATE = threading.Event()
REFUSED: list[str] = []


class Abandoned:
    """A fault whose inject the runner gives up on: deaf to cancellation, it changes state before or after
    blocking (like a partition's iptables call waiting on the xtables lock)."""

    def inject(self, context, spec):
        THREADS.append(threading.current_thread())
        if spec.config["change"] == "before":
            context.obligations.open(spec, "tests.stopped", "early", _undo("early"))
        GATE.wait(20)
        if spec.config["change"] == "after":
            try:
                context.obligations.open(spec, "tests.stopped", "late", _undo("late"))
            except RuntimeError as exc:
                REFUSED.append(str(exc))
                raise
        return make_result(spec, ComponentStatus.PASS, "applied")

    def recover(self, context, spec):
        return make_result(spec, ComponentStatus.PASS, "recovered")


@pytest.mark.parametrize("change", ["before", "after"])
def test_a_change_an_abandoned_start_owns_is_undone_once_it_returns(monkeypatch, change):
    # Both are undone: a change made after the run's final sweep (A), and one a start call still running at
    # the end owned (B), which both sweeps leave alone. Neither may outlive the run.
    monkeypatch.setattr("valkey_fuzzer.deadlines.CANCEL_GRACE_S", 0.1)
    THREADS.clear()
    REFUSED.clear()
    GATE.clear()
    registration = ComponentRegistration(
        "tests.fault.abandoned", ComponentRole.FAULT, Abandoned, declares=Declarations(changes=("tests.stopped",)),
        config=ConfigSchema({"change": ConfigField(str)}),
    )  # fmt: skip
    experiment = ExperimentSpec(
        experiment_id="tests.abandoned", mode=RunMode.SMOKE, deployment_modes=("fake",),
        components=(ComponentSpec(use="tests.fault.abandoned", id="fault", config={"change": change}, timeout_s=0.2),),
    )  # fmt: skip
    try:
        outcome = run_experiment(experiment, (registration,), deadline=time.monotonic() + 30)
        assert outcome.status is OutcomeStatus.ERROR and UNDONE == []
    finally:
        GATE.set()
        for thread in THREADS:
            thread.join(5)
    if change == "before":
        assert UNDONE == ["early"]
    else:
        assert UNDONE == ["late"] and "after its run ended" in REFUSED[0]


def test_an_interrupt_during_the_runners_undo_still_undoes_the_rest_and_cleans_up_the_deployment():
    # The sweep catches more than Exception: Ctrl-C in one undo must not skip the rest, or the deployment
    # cleanup, which would leave every server of a local deployment running.
    calls = []
    experiment = ExperimentSpec(
        experiment_id="tests.ctrl-c", mode=RunMode.SMOKE, deployment_modes=("tracked",),
        components=(ComponentSpec(use="tests.fault.change", id="fault",
                                  config={"targets": ["one", "ctrl-c", "two"], "forget": True}),),
    )  # fmt: skip
    provider = DeploymentProviderRegistration(
        deployment_mode="tracked", provider_id="tests.deployment.tracked",
        factory=lambda: FakeDeploymentProvider(calls=calls),
    )  # fmt: skip

    with pytest.raises(KeyboardInterrupt):
        run_experiment(experiment, REGISTRATIONS, deployment_providers=(provider,))

    assert UNDONE == ["two", "one"] and calls == ["validate", "provision", "cleanup"]
