"""The run executor. Only an oracle or action FAIL judges the system under test; any other required FAIL or
SKIP leaves no trustworthy verdict (ERROR), and cleanup failures never hide a trustworthy FAIL."""

import contextvars
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from tests.support import component_registration, deployment_identity, deployment_registrations, make_catalog
from valkey_fuzzer import deadlines
from valkey_fuzzer import runner as runner_module
from valkey_fuzzer.catalog import ComponentRegistry
from valkey_fuzzer.declarations import Declarations, EvidenceDeclaration
from valkey_fuzzer.plan import ComponentSpec, EvidenceRequirement, ExecutionPlan, ExperimentSpec, RunMode
from valkey_fuzzer.protocols import ComponentRole, component_artifact_dir
from valkey_fuzzer.results import ComponentResult, ComponentStatus, Evidence, OutcomeStatus
from valkey_fuzzer.runner import PreflightError, Runner, RunnerError
from valkey_fuzzer.schema import ConfigField, ConfigSchema
from valkey_fuzzer.testing import FakeDeploymentProvider

PASS, FAIL, SKIP, ERR = ComponentStatus.PASS, ComponentStatus.FAIL, ComponentStatus.SKIP, ComponentStatus.ERROR
CAPABILITY, OBSERVER, WORKLOAD = ComponentRole.CAPABILITY, ComponentRole.OBSERVER, ComponentRole.WORKLOAD
FAULT, ACTION, ORACLE = ComponentRole.FAULT, ComponentRole.ACTION, ComponentRole.ORACLE
DECLARES = Declarations(
    events={"feature.path-entered": ConfigSchema({})},
    evidence={"tests.{id}": EvidenceDeclaration(ConfigField(dict), many=True)},
)


def _provider(calls, mode="standalone", **failures):
    return FakeDeploymentProvider(calls=calls, name=mode, failures=failures)


def _method(name, teardown=False):
    def call(self, context, spec, evidence=()):
        self.calls.append(f"{spec.id}.{name}")
        if self.seen is not None:
            self.seen.extend(item.evidence_id for item in evidence)
        status, cleanup = self.script.get(spec.id, (self.status, self.cleanup))
        if self.hang is not None and not teardown:
            self.hang.update(
                slept=context.sleep(30), cancelled=context.cancelled, expired=context.deadline <= time.monotonic()
            )
        if self.folders is not None:
            self.folders.append(component_artifact_dir(context, spec))
        error = self.cleanup_error if teardown else self.error
        if error is not None:
            raise error
        item = Evidence(f"tests.{spec.id}", spec.id, f"{spec.role.value}.completed", time.monotonic(), {})
        return ComponentResult(spec.id, cleanup if teardown else status, f"{spec.id} {name}", evidence=(item,))

    return call


class _Component:
    """Every role's methods: records ``<id>.<method>`` and emits ``tests.<id>``; the status comes from
    ``script[id]`` or ``status`` (``cleanup`` for stop and recover), unless it raises ``error`` (``cleanup_error``)."""

    def __init__(self, calls=None, status=PASS, *, cleanup=PASS, error=None, cleanup_error=None, **options):
        self.calls = [] if calls is None else calls
        self.status, self.cleanup, self.error, self.cleanup_error = status, cleanup, error, cleanup_error
        # seen: evidence the oracle saw; hang: how a cooperative sleep ended; folders: its artifact folders
        self.seen, self.script = options.get("seen"), options.get("script", {})
        self.hang, self.folders = options.get("hang"), options.get("folders")

    probe, start, inject, run, evaluate = (_method(name) for name in ("probe", "start", "inject", "run", "evaluate"))
    stop, recover = _method("stop", True), _method("recover", True)


def _execute(plan, *entries, calls=None, provider=None, executor=None, **options):
    registry = ComponentRegistry()
    registry.register_many(
        component_registration(f"tests.{role.value}.fake", factory=factory, declares=DECLARES)
        for role, factory in entries
    )
    provider = provider or _provider([] if calls is None else calls)
    providers = deployment_registrations(**{plan.deployment_mode: lambda: provider})
    return Runner(make_catalog(registry, providers), **(executor or {})).run(plan, **options)


def _spec(role, component_id, *, required=True, depends_on=(), timeout_s=None):
    use = f"tests.{role.value}.fake"
    return ComponentSpec(use, component_id, role, required=required, depends_on=depends_on, timeout_s=timeout_s)


def _plan(components, *, deployment_mode="standalone", **requirements):
    experiment = ExperimentSpec(
        experiment_id="tests.feature",
        mode=RunMode.EXPERIMENT if requirements.get("required_evidence") else RunMode.SMOKE,
        deployment_modes=("standalone", "cluster"),
        components=tuple(components),
        **requirements,
    )
    return ExecutionPlan.from_experiment(
        experiment,
        scenario_id="runner-test",
        seed=7,
        deployment_mode=deployment_mode,
        deployment_provider=deployment_identity(),
        deployment={"mode": deployment_mode},
    )


@pytest.mark.parametrize("mode", ("standalone", "cluster"))
def test_every_role_runs_in_dependency_order_on_any_deployment_mode(mode, tmp_path):
    calls, seen, folders = [], [], []
    components = (
        _spec(ACTION, "act", depends_on=("inject",)),
        _spec(ORACLE, "check", depends_on=("act",)),
        _spec(FAULT, "inject", depends_on=("load",)),
        _spec(WORKLOAD, "load", depends_on=("observe",)),
        _spec(OBSERVER, "observe", depends_on=("probe",)),
        _spec(CAPABILITY, "probe"),
    )
    plan = _plan(
        components,
        deployment_mode=mode,
        required_events=("deployment.ready", "validation.completed"),
        required_evidence=(EvidenceRequirement("tests.act", "act"),),
    )
    entries = ((role, lambda: _Component(calls, seen=seen, folders=folders)) for role in ComponentRole)
    outcome = _execute(plan, *entries, provider=_provider(calls, mode), executor={"artifact_dir": str(tmp_path)})

    assert outcome.status is OutcomeStatus.PASS, outcome.summary
    assert " ".join(calls) == (
        f"{mode}.validate probe.probe {mode}.provision observe.start load.start inject.inject act.run "
        f"inject.recover load.stop observe.stop check.evaluate {mode}.cleanup"
    )
    # The oracle sees everything emitted before it, teardown included.
    assert seen == [
        f"tests.{name}" for name in ("probe", "observe", "load", "inject", "act", "inject", "load", "observe")
    ]
    assert " ".join(event["name"] for event in outcome.diagnostics["events"]) == (
        "run.started probe.started probe.completed deployment.provisioning deployment.ready observe.started "
        "observe.completed load.started load.completed inject.started inject.completed act.started act.completed "
        "inject.stopping inject.stopped load.stopping load.stopped observe.stopping observe.stopped check.started "
        "check.completed validation.completed deployment.cleanup.started deployment.cleanup.completed run.completed"
    )
    # Each component writes under its own folder of this run's folder.
    (run_dir,) = (tmp_path / "runs").iterdir()
    assert {str(run_dir / "components" / spec.id) for spec in components} == set(folders)


def _c(role, component_id, status=PASS, cleanup=PASS, depends_on=(), required=True):
    return role, component_id, status, cleanup, depends_on, required


TRUSTED, UNTRUSTED = "required components did not pass", "no trustworthy verdict"
# The summary says why the runner stopped, not only where.
OracleError = "runner execution failed during oracle.evaluate: RunnerError: oracle 'data' returned ERROR: data evaluate"


@pytest.mark.parametrize(
    ("components", "status", "summary", "provisioned"),
    [
        pytest.param([_c(ORACLE, "check", FAIL)], "FAIL", TRUSTED, True, id="oracle-fail"),
        pytest.param([_c(ACTION, "act", FAIL)], "FAIL", TRUSTED, True, id="action-fail"),
        pytest.param([_c(FAULT, "setup", FAIL), _c(ORACLE, "check")], "ERROR", UNTRUSTED, True, id="fault-fail"),
        pytest.param([_c(ACTION, "act", SKIP)], "ERROR", UNTRUSTED, True, id="required-skip"),
        pytest.param([_c(CAPABILITY, "probe", FAIL)], "ERROR", "capability 'probe' failed", False, id="capability-fail"),
        pytest.param([_c(CAPABILITY, "probe", SKIP)], "SKIP_UNSUPPORTED", "", False, id="capability-skip"),
        pytest.param([_c(ACTION, "act", depends_on=("probe",)), _c(CAPABILITY, "probe", SKIP, required=False)],
                     "SKIP_UNSUPPORTED", "", False, id="needed-optional-capability-skip"),
        pytest.param([_c(ACTION, "act", depends_on=("probe",)), _c(CAPABILITY, "probe", FAIL, required=False)],
                     "ERROR", "", False, id="needed-optional-capability-fail"),
        pytest.param([_c(CAPABILITY, "probe", SKIP, required=False), _c(ACTION, "act")], "PASS", "", True,
                     id="independent-optional-capability-skip"),
        pytest.param([_c(ACTION, "optional", FAIL, required=False)], "PASS", "", True, id="independent-optional-fail"),
        pytest.param([_c(ACTION, "act", depends_on=("optional",)), _c(ACTION, "optional", FAIL, required=False)],
                     "FAIL", f"{TRUSTED}: optional, act", True, id="needed-optional-fail"),
        pytest.param([_c(ACTION, "act", FAIL), _c(ORACLE, "check", depends_on=("act",))], "FAIL",
                     f"{TRUSTED}: act, check", True, id="oracle-blocked-by-failed-action"),
        # A finding that does not depend on a component that did not do its part stands.
        pytest.param([_c(WORKLOAD, "setup", FAIL), _c(ORACLE, "check", FAIL)], "FAIL", TRUSTED, True,
                     id="an-oracle-fail-independent-of-a-failed-setup"),
        pytest.param([_c(WORKLOAD, "setup", FAIL), _c(ORACLE, "check", FAIL, depends_on=("setup",))], "ERROR",
                     UNTRUSTED, True, id="an-oracle-fail-that-depends-on-a-failed-setup"),
        # A failed teardown is the component's result: it did not do its part.
        pytest.param([_c(FAULT, "fault", cleanup=FAIL), _c(ORACLE, "check")], "ERROR", UNTRUSTED, True,
                     id="cleanup-failure-alone"),
        # Oracles judge a deployment whose teardown failed; an action found its failure before the teardown.
        pytest.param([_c(FAULT, "fault", cleanup=FAIL), _c(ORACLE, "check", FAIL)], "ERROR", UNTRUSTED, True,
                     id="an-oracle-fail-after-a-failed-teardown"),
        pytest.param([_c(FAULT, "fault", cleanup=FAIL), _c(ACTION, "act", FAIL)], "FAIL", TRUSTED, True,
                     id="an-action-fail-after-a-failed-teardown"),
        pytest.param([_c(FAULT, "fault", cleanup=ERR), _c(ACTION, "act", FAIL, depends_on=("fault",))], "ERROR",
                     UNTRUSTED, True, id="an-action-fail-that-depends-on-a-fault-that-did-not-recover"),
        pytest.param([_c(WORKLOAD, "setup", FAIL, FAIL), _c(ORACLE, "check", FAIL)], "ERROR", UNTRUSTED, True,
                     id="untrusted-failure-with-cleanup-errors"),
        pytest.param([_c(OBSERVER, "observe", cleanup=FAIL), _c(ACTION, "act", SKIP, depends_on=("observe",))],
                     "ERROR", "", True, id="cleanup-failure-with-a-required-skip"),
        # Oracles judge on their own: one that could not tell does not hide another's finding.
        pytest.param([_c(ORACLE, "data", ERR), _c(ORACLE, "liveness", FAIL)], "FAIL", f"{TRUSTED}: liveness", True,
                     id="oracle-error-keeps-another-oracles-fail"),
        pytest.param([_c(ORACLE, "data", ERR), _c(ORACLE, "liveness")], "ERROR", OracleError, True,
                     id="oracle-error-alone"),
        pytest.param([_c(ORACLE, "data", ERR), _c(ORACLE, "liveness", FAIL, required=False)], "ERROR",
                     OracleError, True, id="oracle-error-and-an-optional-fail"),
    ],
)  # fmt: skip
def test_component_results_decide_the_verdict(components, status, summary, provisioned):
    calls = []
    script = {component_id: (item_status, cleanup) for _, component_id, item_status, cleanup, _, _ in components}
    plan = _plan(
        _spec(role, name, depends_on=after, required=required) for role, name, _, _, after, required in components
    )
    roles = dict.fromkeys(role for role, *_ in components)

    outcome = _execute(plan, *((role, lambda: _Component(calls, script=script)) for role in roles), calls=calls)

    assert outcome.status.value == status, outcome.summary
    assert summary in outcome.summary
    assert ("standalone.provision" in calls) is provisioned
    assert calls[-1] == ("standalone.cleanup" if provisioned else "probe.probe")
    owners = [error["owner"] for error in outcome.diagnostics.get("cleanup_errors", ())]
    assert owners == [name for _, name, _, cleanup, _, _ in components if cleanup is not PASS]


@pytest.mark.parametrize("liveness", [FAIL, PASS])
def test_an_oracle_that_raises_does_not_stop_the_oracles_after_it(liveness):
    # The raise does not leave the loop: the liveness oracle still runs, and its FAIL is kept.
    class Oracles:
        def evaluate(self, context, spec, evidence):
            if spec.id == "data":
                raise RuntimeError("cannot read the dataset")
            return ComponentResult(spec.id, liveness, "the server is dead" if liveness is FAIL else "alive")

    outcome = _execute(_plan((_spec(ORACLE, "data"), _spec(ORACLE, "liveness"))), (ORACLE, Oracles))

    results = {result.component_id: result for result in outcome.component_results}
    assert results["data"].status is ERR and results["liveness"].status is liveness
    if liveness is FAIL:
        assert outcome.status is OutcomeStatus.FAIL and outcome.summary.endswith(f"{TRUSTED}: liveness")
    else:
        assert outcome.status is OutcomeStatus.ERROR
        error = outcome.diagnostics["error"]
        assert (error["type"], error["phase"], error["message"]) == (
            "RuntimeError", "oracle.evaluate", "cannot read the dataset"
        )  # fmt: skip


@pytest.mark.parametrize(
    ("requirements", "diagnostic", "expected"),
    [
        ({"required_events": ("feature.path-entered",)}, "missing_required_events", ("feature.path-entered",)),
        ({"required_evidence": (EvidenceRequirement("tests.act", "act", min_count=2),)}, "missing_required_evidence",
         ({"evidence_id": "tests.act", "source": "act", "min_count": 2, "observed_count": 1},)),
    ],
)  # fmt: skip
@pytest.mark.parametrize("check", [PASS, FAIL])
def test_missing_required_events_or_evidence_make_a_run_error_unless_a_check_failed(
    requirements, diagnostic, expected, check
):
    # The run did not reach what it tests; a FAIL an oracle found anyway stands, and the missing item stays listed.
    plan = _plan((_spec(ACTION, "act"), _spec(ORACLE, "check", depends_on=("act",))), **requirements)
    outcome = _execute(plan, (ACTION, _Component), (ORACLE, lambda: _Component(status=check)))

    assert outcome.status is (OutcomeStatus.FAIL if check is FAIL else OutcomeStatus.ERROR), outcome.summary
    assert outcome.diagnostics[diagnostic] == expected


@pytest.mark.parametrize("error", [RuntimeError("cannot start"), KeyboardInterrupt("stop now"), "hang"])
def test_a_raising_or_hung_component_tears_down_what_started_and_an_interrupt_is_reraised(error):
    calls, seen = [], {}
    act = _spec(ACTION, "act", depends_on=("observe",), timeout_s=0.2 if error == "hang" else None)
    plan = _plan((_spec(OBSERVER, "observe"), act))
    acts = _Component(calls, error=None if error == "hang" else error, hang=seen if error == "hang" else None)
    started = time.monotonic()

    def run():
        # A run deadline puts every call on a watched thread, whose errors still reach the runner.
        return _execute(
            plan, (OBSERVER, lambda: _Component(calls)), (ACTION, lambda: acts), calls=calls, deadline=started + 30
        )

    if isinstance(error, KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt) as caught:
            run()
        assert caught.value is error
    else:
        outcome = run()
        assert outcome.status is OutcomeStatus.ERROR
        if error == "hang":
            # Cancelled at its timeout: its sleep ends early and it sees the cancellation.
            assert "was cancelled" in outcome.diagnostics["error"]["message"]
            assert seen == {"slept": False, "cancelled": True, "expired": True} and time.monotonic() - started < 10
        else:
            assert outcome.component_results[-1].diagnostics["exception"]["message"] == "cannot start"
            assert outcome.component_results[-1].summary == "component execution failed: RuntimeError: cannot start"
            assert outcome.summary == "runner execution failed during action.run: RuntimeError: cannot start"
    assert calls[-2:] == ["observe.stop", "standalone.cleanup"]


class _NotAResult:
    def run(self, context, spec):
        return object()


EXECUTOR_ERROR, DEADLINE_ERROR = (
    "valkey_fuzzer.runner.RunnerError",
    "valkey_fuzzer.deadlines.CooperativeDeadlineError",
)


@pytest.mark.parametrize(
    ("factory", "options", "error_type", "phase"),
    [
        (object, {}, EXECUTOR_ERROR, "preflight"),
        (lambda: _Component, {}, EXECUTOR_ERROR, "preflight"),
        (lambda: [][0], {}, EXECUTOR_ERROR, "preflight"),
        (_NotAResult, {}, EXECUTOR_ERROR, "action.run"),
        (_Component, {"deadline": 0.0}, DEADLINE_ERROR, "deployment.provision"),
    ],
    ids=["no-protocol", "a-class-not-an-instance", "a-failing-factory", "not-a-result", "an-expired-deadline"],
)  # fmt: skip
def test_a_broken_component_or_an_expired_deadline_is_an_error(factory, options, error_type, phase):
    calls = []
    outcome = _execute(_plan((_spec(ACTION, "act"),)), (ACTION, factory), calls=calls, **options)

    assert outcome.status is OutcomeStatus.ERROR
    error = outcome.diagnostics["error"]
    assert (error["type"], error["phase"]) == (error_type, phase)
    assert ("standalone.provision" in calls) is (phase == "action.run")


def test_a_rejected_deployment_or_requirement_raises_before_anything_is_created_or_provisioned():
    calls = []
    provider = _provider(calls, validate=ValueError("deployment document is invalid"))
    with pytest.raises(PreflightError, match="deployment document is invalid") as raised:
        _execute(_plan((_spec(ACTION, "act"),)), (ACTION, lambda: calls.append("factory")), provider=provider)
    assert isinstance(raised.value.__cause__, ValueError)
    assert calls == ["standalone.validate"]
    with pytest.raises(RunnerError, match=r"'never\.declared' is not published"):
        _execute(_plan((_spec(ACTION, "act"),), required_events=("never.declared",)), (ACTION, _Component))


def test_a_failed_plan_check_is_a_runner_error_that_names_the_field():
    with pytest.raises(PreflightError, match="is not registered") as raised:
        _execute(_plan((_spec(ACTION, "act"),)))
    cause = raised.value.__cause__
    assert type(cause) is RunnerError and cause.path == "components.act.use"


@pytest.mark.parametrize(
    ("provision", "rollback"),
    [(RuntimeError("partial provision"), None), ("invalid", None), (RuntimeError("partial"), RuntimeError("rollback failed"))],
)  # fmt: skip
def test_a_failed_provision_is_rolled_back_and_a_failed_rollback_does_not_hide_it(provision, rollback):
    calls = []
    provider = _provider(
        calls, **{"provision": provision, "rollback": rollback} if rollback else {"provision": provision}
    )
    if provision == "invalid":
        provider.provision = lambda context: calls.append("standalone.provision")  # returns None, not a deployment
    outcome = _execute(_plan((_spec(ACTION, "act"),)), (ACTION, _Component), provider=provider)

    assert outcome.status is OutcomeStatus.ERROR
    assert calls == ["standalone.validate", "standalone.provision", "standalone.rollback"]
    assert outcome.diagnostics["error"]["phase"] == "deployment.provision"
    phases = [item["phase"] for item in outcome.diagnostics.get("cleanup_errors", ())]
    assert phases == (["rollback"] if rollback else [])


@pytest.fixture
def short_grace(monkeypatch):
    monkeypatch.setattr("valkey_fuzzer.deadlines.CANCEL_GRACE_S", 0.1)
    monkeypatch.setattr("valkey_fuzzer.runner._CLEANUP_DISPATCH_S", 0.5)
    yield
    until = time.monotonic() + 10
    while deadlines.abandoned_calls() and time.monotonic() < until:
        time.sleep(0.01)
    assert not deadlines.abandoned_calls()


@pytest.mark.parametrize("released_by_cleanup", [True, False])
def test_teardown_waits_for_a_timed_out_start_or_reports_it_and_refuses_later_runs(short_grace, released_by_cleanup):
    created, threads, gate = [], [], threading.Event()

    class StartsLate:
        def start(self, context, spec):
            # Deaf to cancellation: it creates its resource only after the runner gave up on it.
            threads.append(threading.current_thread())
            gate.wait(20)
            created.append(spec.id)
            return ComponentResult(spec.id, PASS, "started")

        def stop(self, context, spec):
            created.clear()
            return ComponentResult(spec.id, PASS, "stopped")

    provider = _provider([])
    if released_by_cleanup:
        provider.cleanup = lambda context, deployment: gate.set()

    try:
        outcome = _execute(_plan((_spec(WORKLOAD, "tmpfs", timeout_s=0.2),)), (WORKLOAD, StartsLate), provider=provider)
        assert outcome.status is OutcomeStatus.ERROR
        if not released_by_cleanup:
            (error,) = [item for item in outcome.diagnostics["cleanup_errors"] if item["owner"] == "tmpfs"]
            assert "not torn down" in error["exception"]["message"]
            abandoned = ({"scenario_id": "runner-test", "call": "component 'tmpfs' start"},)
            assert outcome.diagnostics["abandoned_calls"] == abandoned
            refused = _execute(_plan((_spec(ACTION, "act"),)), (ACTION, _Component))
            assert (
                refused.summary.startswith("refused to start") and refused.diagnostics["abandoned_calls"] == abandoned
            )
    finally:
        gate.set()
        for thread in threads:
            thread.join(5)

    if released_by_cleanup:
        assert created == [], "the late start's resource was never released"
        assert "abandoned_calls" not in outcome.diagnostics
    assert _execute(_plan((_spec(ACTION, "act"),)), (ACTION, _Component)).status is OutcomeStatus.PASS


@pytest.mark.parametrize("event", ["act.started", "load.stopping"])
def test_a_blocking_subscriber_cannot_hang_the_run_past_its_deadline(short_grace, event):
    release, calls = threading.Event(), []

    class Subscribes(_Component):
        def start(self, context, spec):
            context.event_bus.subscribe(event, lambda _: release.wait(20))
            return ComponentResult(spec.id, PASS, "subscribed")

    started = time.monotonic()
    plan = _plan((_spec(WORKLOAD, "load"), _spec(ACTION, "act", depends_on=("load",))))
    try:
        outcome = _execute(
            plan, (WORKLOAD, lambda: Subscribes(calls)), (ACTION, _Component), calls=calls, deadline=started + 0.5
        )
    finally:
        release.set()
    assert time.monotonic() - started < 6
    assert outcome.status is OutcomeStatus.ERROR and calls[-1] == "standalone.cleanup"
    assert f"dispatch of event {event!r}" in str(outcome.diagnostics)


def test_a_component_whose_start_never_ran_is_not_torn_down(monkeypatch):
    # The run deadline passes while "first.completed" is dispatched, so "second.started" cannot be, and
    # second's start never runs: neither may its stop.
    calls, offset, real = [], [0.0], time.monotonic
    monkeypatch.setattr(runner_module, "time", SimpleNamespace(time=time.time, monotonic=lambda: real() + offset[0]))

    class Watches(_Component):
        def start(self, context, spec):
            context.event_bus.subscribe("first.completed", lambda _: offset.__setitem__(0, 1000.0))
            return ComponentResult(spec.id, PASS, "watching")

    plan = _plan(
        (
            _spec(OBSERVER, "watch"),
            _spec(WORKLOAD, "first", depends_on=("watch",)),
            _spec(WORKLOAD, "second", depends_on=("first",)),
        )
    )
    outcome = _execute(
        plan, (OBSERVER, lambda: Watches(calls)), (WORKLOAD, lambda: _Component(calls)), calls=calls,
        deadline=real() + 30,
    )  # fmt: skip

    assert outcome.status is OutcomeStatus.ERROR
    assert "second.start" not in calls and "second.stop" not in calls
    assert calls[-3:] == ["first.stop", "watch.stop", "standalone.cleanup"]


def test_cancelled_calls_share_one_grace_period(monkeypatch):
    joined = []
    call = type("Call", (), {"join": lambda self, timeout: joined.append(timeout)})()
    execution = runner_module._Execution.__new__(runner_module._Execution)
    execution.cancelled_calls, execution.waited_for_cancelled_calls = [call, call], False
    monkeypatch.setattr(runner_module, "time", SimpleNamespace(monotonic=iter((100.0, 103.0, 104.0)).__next__))

    execution.wait_for_cancelled_calls()

    assert joined == [deadlines.CANCEL_GRACE_S - 3.0, deadlines.CANCEL_GRACE_S - 4.0]


def test_ctrl_c_cancels_the_call_in_flight(short_grace, monkeypatch):
    # Ctrl-C must cancel the watched call, not only end the runner's wait for it: otherwise a chaos action keeps
    # injecting after deployment cleanup, and after the run has raised.
    calls, entered, seen = [], threading.Event(), {}

    class Busy:
        def run(self, context, spec):
            entered.set()
            seen.update(slept=context.sleep(20), cancelled=context.cancelled)
            return ComponentResult(spec.id, PASS, "done")

    real_join, interrupted = deadlines.ComponentCall.join, []

    def join(call, timeout=None):
        if call.label == "component 'act' run" and not interrupted:
            interrupted.append(call)
            entered.wait(5)
            raise KeyboardInterrupt  # What SIGINT raises on the main thread, waiting in join().
        return real_join(call, timeout)

    monkeypatch.setattr(deadlines.ComponentCall, "join", join)
    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        _execute(_plan((_spec(ACTION, "act"),)), (ACTION, Busy), calls=calls, deadline=started + 30)

    until = time.monotonic() + 5
    while not seen and time.monotonic() < until:
        time.sleep(0.01)
    assert seen == {"slept": False, "cancelled": True} and calls[-1] == "standalone.cleanup"
    assert time.monotonic() - started < 5


def test_ctrl_c_during_a_start_defers_its_teardown_until_the_start_returns(short_grace, monkeypatch):
    # An interrupted start defers its teardown as a timed-out one does: recover must not run while inject still runs.
    calls, entered, gate = [], threading.Event(), threading.Event()

    class Injects:
        def inject(self, context, spec):
            entered.set()
            gate.wait(20)  # Deaf to cancellation, like a blocking client call.
            calls.append("f.inject returned")
            return ComponentResult(spec.id, PASS, "injected")

        def recover(self, context, spec):
            calls.append("f.recover")
            return ComponentResult(spec.id, PASS, "recovered")

    def cleanup(context, deployment):
        calls.append("standalone.cleanup")
        gate.set()

    provider = _provider(calls)
    provider.cleanup = cleanup
    real_join, interrupted = deadlines.ComponentCall.join, []

    def join(call, timeout=None):
        if call.label == "component 'f' inject" and not interrupted:
            interrupted.append(call)
            entered.wait(5)
            raise KeyboardInterrupt
        return real_join(call, timeout)

    monkeypatch.setattr(deadlines.ComponentCall, "join", join)
    try:
        with pytest.raises(KeyboardInterrupt):
            _execute(_plan((_spec(FAULT, "f"),)), (FAULT, Injects), provider=provider, deadline=time.monotonic() + 30)
    finally:
        gate.set()
    assert calls[-3:] == ["standalone.cleanup", "f.inject returned", "f.recover"]


def test_a_thread_a_call_starts_under_a_copy_of_its_context_shares_its_cancellation(short_grace):
    # A call's cancellation reaches the threads it starts: were context.sleep time.sleep and context.cancelled
    # False on a wave's pool thread, a cancelled chaos action would sleep through every delay_s and duration_s,
    # and be abandoned.
    seen = {}

    class Pooled:
        def run(self, context, spec):
            def operation():
                seen.update(slept=context.sleep(8), cancelled=context.cancelled)

            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(contextvars.copy_context().run, operation).result()
            return ComponentResult(spec.id, PASS, "done")

    started = time.monotonic()
    outcome = _execute(_plan((_spec(ACTION, "act", timeout_s=0.2),)), (ACTION, Pooled), deadline=started + 30)

    assert outcome.status is OutcomeStatus.ERROR and "abandoned_calls" not in outcome.diagnostics
    assert seen == {"slept": False, "cancelled": True} and time.monotonic() - started < 5
