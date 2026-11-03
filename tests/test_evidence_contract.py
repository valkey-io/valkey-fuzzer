"""The evidence contract: components are held to the events and evidence they declare.

A component that breaks a rule makes the run ERROR, names itself, and what it
emitted reaches no subscriber or oracle.
"""

import gc
import tempfile
import threading
import time
from pathlib import Path

import pytest

from tests.support import make_plan
from tests.valkey_experiments_support import fake_context, resolved
from valkey_fuzzer import verdict
from valkey_fuzzer.compiler import ScenarioCompileError
from valkey_fuzzer.components import actions
from valkey_fuzzer.declarations import Declarations, EvidenceDeclaration
from valkey_fuzzer.evidence import ContractViolationError
from valkey_fuzzer.plan import ComponentSpec, EvidenceRequirement, ExperimentSpec, RunMode
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration
from valkey_fuzzer.results import ComponentStatus, Evidence, OutcomeStatus, make_evidence, make_result
from valkey_fuzzer.schema import ConfigField, ConfigSchema
from valkey_fuzzer.testing import FakeContext, run_experiment

COUNT = EvidenceDeclaration(ConfigField(dict, schema=ConfigSchema({"n": ConfigField(int)})))
SEEN: dict[str, list] = {"evidence": [], "events": [], "late": []}


def _late_ping(bus):
    time.sleep(0.2)
    try:
        bus.publish("emit.ping", source="emit", payload={"n": 1})
    except Exception as exc:
        SEEN["events"].append(f"rejected: {type(exc).__name__}")


class Emitter:
    """Does one thing its ``mode`` names, right or wrong."""

    def run(self, context, spec):
        mode, bus, evidence = spec.config["mode"], context.event_bus, []
        if mode in ("good", "twice", "uncommitted"):
            sequence = bus.publish("emit.ping", source=spec.id, payload={"n": 1}).sequence
            sequence += 1000 if mode == "uncommitted" else 0
            evidence.append(make_evidence(spec, "tests.count", "action.completed", {"n": 1}, event_sequence=sequence))
        if mode == "twice":
            evidence.append(make_evidence(spec, "tests.count", "action.completed", {"n": 2}))
        elif mode == "foreign-evidence":
            evidence.append(Evidence("tests.count", "intruder", "action.completed", time.monotonic(), {"n": 1}))
        elif mode == "undeclared-evidence":
            evidence.append(make_evidence(spec, "tests.other", "action.completed", {"n": 1}))
        elif mode == "malformed-evidence":
            evidence.append(make_evidence(spec, "tests.count", "action.completed", {"n": "one"}))
        elif mode == "late-event":
            SEEN["late"].append(threading.Thread(target=_late_ping, args=(bus,)))
            SEEN["late"][-1].start()
        else:
            name, source, payload = {
                "undeclared-event": ("emit.other", spec.id, {"n": 1}),
                "malformed-event": ("emit.ping", spec.id, {"n": "one"}),
                "lifecycle-event": ("verify.completed", spec.id, {}),
                "foreign-source": ("intruder.ping", "intruder", {"n": 1}),
            }.get(mode, (None, None, None))
            if name:
                bus.publish(name, source=source, payload=payload)
        return make_result(spec, ComponentStatus.PASS, mode, evidence=evidence)


class Watcher:
    def start(self, context, spec):
        context.event_bus.subscribe("*", lambda event: SEEN["events"].append(event.name))
        return make_result(spec, ComponentStatus.PASS, "watching")

    def stop(self, context, spec):
        for thread in SEEN["late"]:
            thread.join()
        return make_result(spec, ComponentStatus.PASS, "stopped")


class Checker:
    def evaluate(self, context, spec, evidence):
        SEEN["evidence"].extend(evidence)
        return make_result(spec, ComponentStatus.PASS, "checked")


def _registration(component_id, factory, declares=None, **kwargs):
    role = ComponentRole(component_id.split(".")[1])
    return ComponentRegistration(component_id, role, factory, declares=declares or Declarations(), **kwargs)


REGISTRATIONS = (
    _registration(
        "tests.action.emit",
        Emitter,
        Declarations(events={"{id}.ping": ConfigSchema({"n": ConfigField(int)})}, evidence={"tests.count": COUNT}),
        config=ConfigSchema({"mode": ConfigField(str)}),
    ),
    _registration("tests.observer.watch", Watcher),
    _registration("tests.oracle.check", Checker),
)


COUNTED = (EvidenceRequirement("tests.count", "emit"),)


def _run(mode="good", required_evidence=COUNTED, required_events=()):
    for items in SEEN.values():
        items.clear()
    experiment = ExperimentSpec(
        experiment_id="tests.contract",
        mode=RunMode.EXPERIMENT if required_evidence else RunMode.SMOKE,
        deployment_modes=("fake",),
        components=(
            ComponentSpec(use="tests.observer.watch", id="watch"),
            ComponentSpec(use="tests.action.emit", id="emit", config={"mode": mode}),
            ComponentSpec(use="tests.oracle.check", id="verify", depends_on=("emit",)),
        ),
        required_events=required_events,
        required_evidence=required_evidence,
    )
    return run_experiment(experiment, REGISTRATIONS)


def test_declared_events_and_evidence_pass():
    outcome = _run("good", required_events=("emit.ping", "watch.stopped", "deployment.ready"))

    assert outcome.status is OutcomeStatus.PASS, outcome.summary
    assert [item.evidence_id for item in SEEN["evidence"]] == ["tests.count"]
    assert "emit.ping" in SEEN["events"]


@pytest.mark.parametrize(
    ("mode", "component", "kind", "name", "message"),
    [
        ("foreign-evidence", "emit", "evidence", "tests.count", "emitted evidence owned by 'intruder'"),
        ("undeclared-evidence", "emit", "evidence", "tests.other", "emitted undeclared evidence 'tests.other'"),
        ("malformed-evidence", "emit", "evidence", "tests.count", "emitted malformed 'tests.count'"),
        ("twice", "emit", "evidence", "tests.count", "emitted 'tests.count' 2 times, but declares it as a single value"),
        ("uncommitted", "emit", "evidence", "tests.count", "referencing uncommitted event sequence"),
        ("undeclared-event", "emit", "event", "emit.other", "published undeclared event 'emit.other'"),
        ("malformed-event", "emit", "event", "emit.ping", "published a malformed 'emit.ping'"),
        ("lifecycle-event", "emit", "event", "verify.completed", "which only the runner publishes"),
        ("foreign-source", "intruder", "event", "intruder.ping", "is not a component of the plan"),
        ("late-event", "emit", "event", "emit.ping", "after its last call returned"),
    ],
)  # fmt: skip
def test_a_contract_violation_is_an_error_naming_the_component(mode, component, kind, name, message):
    outcome = _run(mode, required_evidence=())

    assert outcome.status is OutcomeStatus.ERROR
    assert outcome.summary.startswith(f"evidence contract violated: component '{component}'")
    (violation,) = outcome.diagnostics["contract_violations"]
    assert (violation["component"], violation["kind"], violation["name"]) == (component, kind, name)
    assert message in violation["message"]
    # Nothing emitted against the declarations reached the oracle or a subscriber.
    assert not any(item.source == component for item in [*SEEN["evidence"], *outcome.all_evidence])
    assert name not in SEEN["events"]


@pytest.mark.parametrize(
    ("requirements", "message"),
    [
        ({"required_evidence": (EvidenceRequirement("tests.missing", "emit"),)}, "not declared by component 'emit'"),
        ({"required_evidence": (EvidenceRequirement("tests.missing"),)}, "not declared by any component"),
        ({"required_evidence": (EvidenceRequirement("tests.count", "emit", min_count=2),)}, "as a single value"),
        ({"required_events": ("emit.never",)}, "required event 'emit.never' is not published"),
    ],
)  # fmt: skip
def test_requirements_nothing_in_the_plan_declares_fail_to_compile(requirements, message):
    with pytest.raises(ScenarioCompileError, match=message):
        _run("good", **requirements)


@pytest.mark.parametrize(
    ("declare", "message"),
    [
        (lambda: ComponentRegistration("tests.action.emit", ComponentRole.ACTION, Emitter), "must declare the events"),
        (lambda: Declarations(events={"{id}.completed": ConfigSchema()}), "published by the runner"),
        (lambda: Declarations(events={"{name}.x": ConfigSchema()}), "placeholder other than"),
        (lambda: Declarations(evidence={"unqualified": COUNT}), "namespaced identifier"),
        (lambda: Declarations(evidence={"tests.x": ConfigField(dict)}), "must be a EvidenceDeclaration"),
    ],
)  # fmt: skip
def test_a_missing_or_malformed_declaration_is_rejected_at_registration(declare, message):
    with pytest.raises((TypeError, ValueError), match=message):
        declare()


# conftest.py's strict_component_contract holds Valkey components a unit test calls to their declarations.
def test_strict_mode_fails_a_component_that_breaks_its_declarations(monkeypatch, strict_component_contract):
    # Known-bad: replies without the declared fields, as a drifted command action would emit them.
    replies = [{"matched": True, "reply": "OK"}]
    monkeypatch.setattr(actions, "run_command_on_instances", lambda context, spec, config: (True, replies))
    spec = ComponentSpec("valkey.action.command", "subject", ComponentRole.ACTION, {"command": ["PING"]})

    with pytest.raises(ContractViolationError, match=r"emitted malformed 'valkey\.command\.replies'"):
        actions.CommandAction().run(fake_context(object()), resolved(spec))
    (checked,) = strict_component_contract
    assert [item["name"] for item in checked.violations] == ["valkey.command.replies"]
    checked.violations.clear()


def _decide(components, results, *, invoked=None, crash=(), **state):
    """The verdict on ``results`` (ID -> status) of a plan of ``components`` (ID -> (role, required, depends_on));
    the FAILs of the components named in ``crash`` report a crash, and ``state`` sets the other run state
    (``required_events`` and ``required_evidence`` go to the plan)."""

    specs = tuple(
        ComponentSpec(f"tests.{role}.{name}", name, role, required=required, depends_on=depends_on)
        for name, (role, required, depends_on) in components.items()
    )
    requirements = {key: state.pop(key) for key in ("required_events", "required_evidence") if key in state}
    mode = RunMode.EXPERIMENT if requirements.get("required_evidence") else RunMode.SMOKE
    plan = make_plan(mode=mode, components=specs, **requirements)
    by_id = {spec.id: spec for spec in specs}
    return verdict.decide(
        verdict.RunState(
            plan=plan,
            results={
                name: make_result(by_id[name], status, "ran", crash=name in crash) for name, status in results.items()
            },
            invoked=set(results) if invoked is None else invoked,
            evidence=(),
            committed=lambda name: (),
            **state,
        )
    )


PASSED, FAILED, SKIPPED = ComponentStatus.PASS, ComponentStatus.FAIL, ComponentStatus.SKIP
LOOK = {"look": ("observer", True, ()), "act": ("action", True, ()), "check": ("oracle", True, ())}


@pytest.mark.parametrize(
    "requirements",
    [
        pytest.param({"required_events": ("tests.vote.granted",)}, id="event"),
        pytest.param({"required_evidence": (EvidenceRequirement("tests.vote-logged", "look"),)}, id="evidence"),
    ],
)
@pytest.mark.parametrize(
    ("results", "status"),
    [
        pytest.param({"look": PASSED, "act": PASSED, "check": PASSED}, "ERROR", id="nothing-failed"),
        pytest.param({"look": PASSED, "act": PASSED, "check": FAILED}, "FAIL", id="an-oracle-failed-anyway"),
        pytest.param({"look": PASSED, "act": FAILED, "check": PASSED}, "FAIL", id="an-action-failed-anyway"),
    ],
)
def test_missing_path_evidence_is_an_error_unless_a_check_failed(requirements, results, status):
    # Missing path evidence or a missing required event means the run did not test what it claims; a FAIL an
    # oracle or action found anyway still stands, and the missing item stays in the diagnostics.
    decision = _decide(LOOK, results, **requirements)

    assert decision.status.value == status, decision.summary
    key = "missing_required_events" if "required_events" in requirements else "missing_required_evidence"
    assert decision.diagnostics[key]
    if status == "ERROR":
        assert "the run did not reach what it tests" in decision.summary


TORN_DOWN = {"setup": ("fault", True, ()), "act": ("action", True, ()), "check": ("oracle", True, ())}
_OBLIGATION = {"owner": "setup", "kind": "network.partition", "target": "valkey-0", "undone_at": 1.0}


@pytest.mark.parametrize(
    "unclean",
    [
        pytest.param({"teardown_failed": True}, id="a-teardown-failed"),
        pytest.param({"open_obligations": ({**_OBLIGATION, "undone_by": "runner"},)}, id="the-runner-undid-a-change"),
    ],
)
@pytest.mark.parametrize(
    ("results", "crash", "status"),
    [
        pytest.param({"setup": PASSED, "act": PASSED, "check": FAILED}, (), "ERROR", id="an-oracle-fail-is-untrusted"),
        pytest.param({"setup": PASSED, "act": PASSED, "check": FAILED}, ("check",), "FAIL", id="a-crash-stands"),
        pytest.param({"setup": PASSED, "act": FAILED, "check": PASSED}, (), "FAIL", id="an-action-fail-stands"),
        pytest.param({"setup": PASSED, "act": PASSED, "check": PASSED}, (), "ERROR", id="nothing-failed"),
    ],
)
def test_oracles_judge_only_a_cleanly_torn_down_run(unclean, results, crash, status):
    decision = _decide(TORN_DOWN, results, crash=crash, **unclean)

    assert decision.status.value == status, decision.summary
    if results["check"] is FAILED and not crash:
        assert decision.diagnostics["untrusted_component_failures"] == ("check",)


def test_an_oracle_fail_after_a_cleanup_that_followed_it_stands():
    # A deployment cleanup after the oracles cannot change what they saw.
    decision = _decide(TORN_DOWN, {"setup": PASSED, "act": PASSED, "check": FAILED}, cleanup_failed=True)
    assert decision.status is OutcomeStatus.FAIL, decision.summary
    assert _decide(TORN_DOWN, dict.fromkeys(TORN_DOWN, PASSED), cleanup_failed=True).status is OutcomeStatus.ERROR


# The testing kit holds a component a test calls directly to the same contract.
@pytest.mark.parametrize("mode", ["undeclared-event", "malformed-evidence", "lifecycle-event"])
def test_fake_context_holds_a_component_it_calls_to_its_declarations(mode):
    # The FakeContext itself checks a component against its declarations, not only this repository's conftest.
    spec = ComponentSpec(use="tests.action.emit", id="emit", role=ComponentRole.ACTION, config={"mode": mode})
    context = FakeContext(spec, registrations=REGISTRATIONS)
    with pytest.raises(ContractViolationError):
        context.call(Emitter().run, spec)
    assert len(context.violations) == 1
    context.violations.clear()

    good = ComponentSpec(use="tests.action.emit", id="emit", role=ComponentRole.ACTION, config={"mode": "good"})
    assert FakeContext(good, registrations=REGISTRATIONS).call(Emitter().run, good).status is ComponentStatus.PASS


def test_the_testing_kit_removes_the_temporary_folders_it_makes(tmp_path, monkeypatch, strict_component_contract):
    # A run_experiment or FakeContext without an artifact_dir leaves no folder behind.
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    _run("good")
    context = FakeContext()
    folder = Path(context.artifact_dir)
    assert folder.is_dir()
    strict_component_contract.remove(context)
    del context
    gc.collect()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("role", "depends", "status"),
    [
        pytest.param("fault", True, "ERROR", id="a-fault-it-waited-for-never-fired"),
        pytest.param("workload", True, "ERROR", id="its-workload-stopped"),
        pytest.param("observer", True, "ERROR", id="its-observer-failed"),
        pytest.param("fault", False, "FAIL", id="a-failure-that-does-not-depend-on-the-fault"),
    ],
)
def test_a_failure_that_depends_on_a_component_that_could_not_do_its_part_is_untrusted(role, depends, status):
    # The action waits for the effect of "setup"; a crash found before a fault fired does not depend on it.
    components = {"setup": (role, True, ()), "wait": ("action", True, ("setup",) if depends else ())}
    decision = _decide(components, {"setup": ComponentStatus.ERROR, "wait": FAILED})
    assert decision.status.value == status, decision.summary
