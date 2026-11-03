"""The runner: a generic, deployment-neutral executor of compiled execution plans.

Deployments and what an experiment tests enter only through provider and
component registrations; the runner never dispatches on an individual experiment.

For every component the runner publishes ``<id>.started`` before its
probe/start/inject/run/evaluate call and ``<id>.completed`` (PASS) or
``<id>.failed`` after it; teardown publishes ``<id>.stopping``
and then ``<id>.stopped`` or ``<id>.stop-failed``.
Experiments use these to trigger faults and as required events. Dispatch is
synchronous, and in a run with a deadline the subscribers of these events run
on a watched thread, so a blocking subscriber cannot hold the run past it
(see :meth:`_Execution.emit`).

A run goes: static preflight, acquire the environment, resolve references into
the executable plan, provision the deployment, start observers/workloads/faults/
actions in dependency order, tear them down in reverse, evaluate oracles, clean
up the deployment, and release the environment. Capability probes run before
the environment is acquired when the plan holds no environment references, and
otherwise right after references are resolved; either way before the
deployment is provisioned.
"""

import functools
import logging
import os
import random
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from types import MappingProxyType
from typing import Any, Optional

from . import deadlines, verdict
from ._data import error_record, exception_type, to_plain_data
from .catalog import Catalog
from .compiler import check_plan, deployment_registration, with_planned_instances
from .deadlines import ComponentCall, ComponentTimeoutError, CooperativeDeadlineError
from .environment import (
    EnvironmentLease,
    EnvironmentLifecycleError,
    EnvironmentProvisioner,
    EnvironmentReport,
    UnsupportedEnvironmentError,
    deployment_has_references,
    plan_has_references,
    resolve_plan,
)
from .events import (
    DEPLOYMENT_PROVISIONING,
    DEPLOYMENT_READY,
    DEPLOYMENT_RELEASE_EVENTS,
    RUN_COMPLETED,
    RUN_STARTED,
    VALIDATION_COMPLETED,
    EventBus,
)
from .evidence import EvidenceContract
from .fields import carry_path
from .hoststate import new_run_folder, normalize_artifact_root
from .obligations import ObligationLedger
from .plan import ComponentSpec, ExecutionPlan
from .protocols import ROLES, ComponentRole, DeploymentProvider, ProvisionedDeployment, Stage
from .results import ComponentResult, ComponentStatus, Evidence, OutcomeStatus, RunOutcome
from .schema import ScenarioCompileError

logger = logging.getLogger(__name__)


class RunnerError(RuntimeError):
    """A plan, component, or provider broke the runner contract."""


class PreflightError(RunnerError):
    """A plan failed the runner's static checks before anything was acquired; raised from the check's error."""


@contextmanager
def _plan_check() -> Iterator[None]:
    """A failed plan check (:func:`~valkey_fuzzer.compiler.check_plan` and its parts) as the runner's error."""

    try:
        yield
    except ScenarioCompileError as exc:
        raise carry_path(RunnerError(str(exc)), exc) from exc


# How long the subscribers of one event the runner publishes during teardown
# and cleanup may run, when the run has a deadline: teardown ignores the run
# deadline, but a blocking subscriber must not hold the run forever.
_CLEANUP_DISPATCH_S = 60.0


def _result(spec: ComponentSpec, status: ComponentStatus, summary: str, **diagnostics: Any) -> ComponentResult:
    return ComponentResult(spec.id, status, summary, diagnostics=diagnostics)


def _component_error(spec: ComponentSpec, exc: BaseException, phase: str) -> ComponentResult:
    summary = f"component execution failed: {type(exc).__name__}: {exc}"
    return _result(spec, ComponentStatus.ERROR, summary, exception=error_record(exc, phase))


def _timed(result: ComponentResult, started: float, ended: float) -> ComponentResult:
    """``result`` with the runner's wall-clock start and end times where the component left them out."""

    # Wall time can step back (NTP); a result must not end before it started.
    ended = max(started, ended)
    return replace(
        result,
        started_at=started if result.started_at is None else result.started_at,
        ended_at=ended if result.ended_at is None else result.ended_at,
    )


class _Context:
    """The :class:`~valkey_fuzzer.protocols.RunContext` for one run."""

    def __init__(
        self,
        plan: ExecutionPlan,
        event_bus: EventBus,
        obligations: ObligationLedger,
        artifact_root: str,
        deadline: Optional[float],
        run_dir: Optional[str] = None,
    ):
        self.source_plan = plan
        self.plan = plan
        self.event_bus = event_bus
        self._artifact_root = artifact_root
        # Absolute, so evidence names artifact paths that do not depend on the working directory.
        self._run_dir = None if run_dir is None else os.path.abspath(run_dir)
        self._run_dir_lock = threading.Lock()
        self.run_deadline = deadline
        self.cleaning_up = False
        self.services: Mapping[str, Any] = MappingProxyType({"deployment": MappingProxyType({})})
        self.obligations = obligations
        self._rngs: dict[tuple[str, str], random.Random] = {}
        self._rng_lock = threading.Lock()

    @property
    def artifact_dir(self) -> str:
        """This run's folder: the caller's, or a new ``<artifact root>/runs/<time>-<scenario>-<suffix>`` on first use."""

        with self._run_dir_lock:
            if self._run_dir is None:
                self._run_dir = new_run_folder(os.path.join(self._artifact_root, "runs"), self.source_plan.scenario_id)
            return self._run_dir

    @property
    def deadline(self) -> Optional[float]:
        call = deadlines.current_call.get()
        if call is not None:
            # A component call sees its own deadline, whatever the runner is
            # doing on other threads: the earlier of the run's and its
            # timeout_s, or a teardown's own timeout_s window. Once it is
            # cancelled, its time is up.
            return call.cancelled_at if call.cancel_event.is_set() else call.deadline
        # The runner's own thread (calls without a deadline, provisioning, and
        # deployment cleanup) and threads that components start outside the
        # call's context see the run deadline, except while the runner tears
        # down or cleans up, which ignores it.
        return None if self.cleaning_up else self.run_deadline

    @property
    def cancelled(self) -> bool:
        return deadlines.cancelled_call() is not None

    def sleep(self, seconds: float) -> bool:
        call = deadlines.current_call.get()
        if call is not None:
            return not call.cancel_event.wait(max(0.0, seconds))
        time.sleep(max(0.0, seconds))
        return True

    def rng(self, component_id: str, stream: str = "default") -> random.Random:
        """This run's continuing generator for one component stream."""

        with self._rng_lock:
            key = (component_id, stream)
            if key not in self._rngs:
                self._rngs[key] = self.plan.rng(component_id, stream)
            return self._rngs[key]

    def bind_deployment(self, services: Mapping[str, Any]) -> None:
        self.services = MappingProxyType({"deployment": MappingProxyType(dict(services))})


class _EarlyTerminalError(Exception):
    def __init__(self, status: OutcomeStatus, summary: str):
        self.status = status
        self.summary = summary
        super().__init__(summary)


def _preflight(catalog: Catalog, plan: ExecutionPlan) -> tuple[ExecutionPlan, DeploymentProvider]:
    """Static checks that need no environment: the provider, then :func:`~valkey_fuzzer.compiler.check_plan`.

    Returns the checked plan and its deployment provider.
    """

    with _plan_check():
        registration = deployment_registration(plan, catalog)
    try:
        provider = registration.factory()
    except Exception as exc:
        raise RunnerError(f"deployment provider factory for {plan.deployment_mode!r} failed: {exc}") from exc
    if isinstance(provider, type) or not isinstance(provider, DeploymentProvider):
        raise RunnerError(
            f"deployment provider factory for {plan.deployment_mode!r} returned {type(provider).__name__}, "
            "which does not implement DeploymentProvider"
        )
    # Plans whose deployment still holds references are validated once bound.
    bound = not deployment_has_references(plan)
    if bound:
        provider.validate(plan)
    # A plan built without the compiler gets its instances and its components' defaults here.
    with _plan_check():
        if bound:
            plan = with_planned_instances(plan, registration)
        return check_plan(plan, catalog), provider


def _environment_diagnostics(
    report: Optional[EnvironmentReport], error: Optional[BaseException] = None
) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}
    if report is not None:
        diagnostics["environment_report"] = report.to_data()
    if error is not None:
        diagnostics["environment_error"] = {"type": exception_type(error)}
    return diagnostics


class Runner:
    """Execute compiled plans through the components and providers of one catalog."""

    def __init__(
        self,
        catalog: Catalog,
        *,
        artifact_dir: str = "",
    ):
        self._catalog = catalog
        self._artifact_dir = normalize_artifact_root(artifact_dir)
        self._environment_provisioner = EnvironmentProvisioner(
            registry=catalog.environment_providers, artifact_root=self._artifact_dir
        )

    def validate(self, plan: ExecutionPlan) -> None:
        """Preflight a plan without acquiring, provisioning, or running anything."""

        _preflight(self._catalog, plan)

    def run(
        self,
        plan: ExecutionPlan,
        *,
        deadline: Optional[float] = None,
        run_dir: Optional[str] = None,
    ) -> RunOutcome:
        """Acquire, execute, and fully tear down one plan.

        ``deadline`` is an absolute :func:`time.monotonic` time. Components see
        it as ``RunContext.deadline``; a call still running at its deadline is
        cancelled and the run is an error (see :func:`~valkey_fuzzer.deadlines.call_with_watchdog`).
        While a cancelled call from any earlier run is still running in this
        process, the run is refused as an ERROR before anything happens.

        ``run_dir`` is the run's own folder, which components see as
        ``RunContext.artifact_dir`` (a run report folder, for example).
        Without it the first component that writes an artifact gets a new
        folder ``<artifact root>/runs/<UTC time>-<scenario_id>-<suffix>``.
        """

        return _Execution(
            self._catalog,
            self._environment_provisioner,
            plan,
            artifact_root=self._artifact_dir,
            deadline=deadline,
            run_dir=run_dir,
        ).run()


class _Execution:
    """Mutable state of one run, from its preflight to the release of its environment."""

    def __init__(
        self,
        catalog: Catalog,
        provisioner: EnvironmentProvisioner,
        plan: ExecutionPlan,
        *,
        artifact_root: str,
        deadline: Optional[float],
        run_dir: Optional[str],
    ):
        self.catalog = catalog
        self.provisioner = provisioner
        self.started_at = time.time()
        self.contract = EvidenceContract(catalog.components)
        bus, self.publish_unchecked = EventBus.owned(self.contract.check_event)
        self.context = _Context(
            plan,
            bus,
            ObligationLedger(check=self.contract.check_change),
            artifact_root,
            deadline,
            run_dir,
        )
        self.bus = self.context.event_bus
        # Set by the preflight: the deployment provider, and the components
        # in dependency order (the resolved specs once references are bound).
        self.provider: Optional[DeploymentProvider] = None
        self.ordered: tuple[ComponentSpec, ...] = ()
        self.required: frozenset[str] = frozenset()
        self.lease: Optional[EnvironmentLease] = None
        self.environment_report: Optional[EnvironmentReport] = None
        self.environment_error: Optional[BaseException] = None
        # Diagnostics of a run that ended before its components ran.
        self.early_diagnostics: dict[str, Any] = {}
        self.results: dict[str, ComponentResult] = {}
        self.invoked: set[str] = set()
        self.evidence: list[Evidence] = []
        self.cleanup_errors: list[Mapping[str, Any]] = []
        # How many of them were recorded before the oracles ran.
        self.teardown_errors = 0
        self.objects: dict[str, Any] = {}
        self.teardowns: list[ComponentSpec] = []
        self.deployment: Optional[ProvisionedDeployment] = None
        self.error: Optional[BaseException] = None
        self.error_phase = "preflight"
        self.terminal: Optional[_EarlyTerminalError] = None
        self.interrupt: Optional[BaseException] = None
        self.current: Optional[ComponentSpec] = None
        self.cancelled_calls: list[ComponentCall] = []
        self.waited_for_cancelled_calls = False
        # Start calls the runner gave up on (timed out or interrupted), by
        # component, and the teardowns deferred until they return.
        self.running_starts: dict[str, ComponentCall] = {}
        self.deferred_teardowns: list[ComponentSpec] = []

    # -- helpers ---------------------------------------------------------------

    def guard(self, step: Callable[[], None]) -> None:
        """Run one phase, turning failures into the run's error or terminal state."""

        try:
            step()
        except _EarlyTerminalError as terminal:
            self.terminal = terminal
        except Exception as exc:
            self.error = exc
            if self.current is not None and self.current.id not in self.results:
                self.results[self.current.id] = _component_error(self.current, exc, self.error_phase)
        except BaseException as exc:
            self.interrupt = exc
        self.current = None

    def cleanup_step(self, owner: str, phase: str, step: Callable[[], Any]) -> bool:
        """Run one teardown step; record its failure and keep tearing down."""

        try:
            step()
            return True
        except BaseException as exc:
            self.record_cleanup_error(owner, phase, exc)
            return False

    def record_cleanup_error(self, owner: str, phase: str, exc: BaseException) -> None:
        self.cleanup_errors.append({"owner": owner, "phase": phase, "exception": error_record(exc)})
        if not isinstance(exc, Exception) and self.interrupt is None:
            self.interrupt = exc

    def blocked(self, spec: ComponentSpec) -> bool:
        reasons = []
        for dependency in spec.depends_on:
            result = self.results.get(dependency)
            if result is None:
                reasons.append(f"dependency {dependency!r} has no result")
            elif result.status is not ComponentStatus.PASS:
                reasons.append(f"dependency {dependency!r} is {result.status.value}")
        if reasons:
            self.results[spec.id] = _result(
                spec, ComponentStatus.SKIP, f"blocked: {', '.join(reasons)}", blocked_by=tuple(reasons)
            )
        return bool(reasons)

    def check_result(self, spec: ComponentSpec, result: Any, label: str) -> None:
        if not isinstance(result, ComponentResult):
            raise RunnerError(f"{label} {spec.id!r} returned {type(result).__name__}, expected ComponentResult")
        if result.component_id != spec.id:
            raise RunnerError(f"{label} {spec.id!r} returned result for {result.component_id!r}")
        self.contract.check_evidence(spec, result.evidence, {event.sequence for event in self.bus.committed_history()})

    def watch(
        self, function: Callable[[], Any], *, deadline: Optional[float], label: str, start_of: Optional[str] = None
    ) -> Any:
        """:func:`~valkey_fuzzer.deadlines.call_with_watchdog`, tracking calls still running after cancellation.

        ``start_of`` names the component whose start this is: if the call is
        abandoned, timed out or interrupted, its teardown waits for it.
        """

        def abandon(call: ComponentCall, cause: BaseException) -> None:
            self.cancelled_calls.append(call)
            if start_of is not None:
                self.running_starts[start_of] = call
            deadlines.record_abandoned_call(self.context.source_plan.scenario_id, call)
            # An abandoned call stuck in a subscriber would hold the event
            # bus, and every later event would wait on it.
            self.bus.abort(cause, dispatcher=call.ident)

        def interrupted(call: ComponentCall) -> None:
            abandon(call, RunnerError(f"{label} was interrupted"))

        try:
            return deadlines.call_with_watchdog(function, deadline=deadline, label=label, interrupted=interrupted)
        except ComponentTimeoutError as exc:
            if exc.call is not None:
                abandon(exc.call, exc)
            raise

    def emit(
        self, name: str, *, source: str, payload: Optional[Mapping[str, Any]] = None, cleanup: bool = False
    ) -> None:
        """Publish a runner event, its subscribers bounded like a component call.

        Dispatch stays synchronous: this returns once every subscriber has.
        With a run deadline the subscribers run on a watched thread, until the
        run deadline, or for ``cleanup`` events (teardown, deployment release,
        ``run.completed``) for ``_CLEANUP_DISPATCH_S``, since teardown ignores
        the run deadline. A dispatch still running then is cancelled and the
        event bus aborted: no later event is dispatched or committed, so the
        run is an ERROR. Without a run deadline, they run on this thread.
        """

        publish = functools.partial(self.publish_unchecked, name, source=source, payload=payload)
        deadline = self.context.run_deadline
        if deadline is None:
            publish()
            return
        if cleanup:
            deadline = time.monotonic() + _CLEANUP_DISPATCH_S
        elif time.monotonic() >= deadline:
            raise CooperativeDeadlineError(f"deadline expired before event {name!r} could be dispatched")
        try:
            self.watch(publish, deadline=deadline, label=f"dispatch of event {name!r}")
        except ComponentTimeoutError as exc:
            if exc.call is not None:
                self.bus.abort(exc)
            raise

    def call(self, spec: ComponentSpec, method: str, *args: Any, applying: bool = False) -> ComponentResult:
        """Invoke a lifecycle method under the component and run deadlines.

        ``applying`` (a start with a teardown): until the call returns, the
        obligation sweeps leave what the component owns alone.
        """

        started_at, started = time.time(), time.monotonic()
        limits = [item for item in (self.context.run_deadline,) if item is not None]
        if spec.timeout_s is not None:
            limits.append(started + spec.timeout_s)
        deadline = min(limits) if limits else None
        if deadline is not None and started >= deadline:
            raise CooperativeDeadlineError(f"deadline expired before component {spec.id!r} could run")

        def function() -> Any:
            if not applying:
                return getattr(self.objects[spec.id], method)(self.context, spec, *args)
            with self.context.obligations.applying(spec.id):
                return getattr(self.objects[spec.id], method)(self.context, spec, *args)

        result = self.watch(
            function, deadline=deadline, label=f"component {spec.id!r} {method}", start_of=spec.id if applying else None
        )
        if deadline is not None and time.monotonic() > deadline:
            raise CooperativeDeadlineError(f"component {spec.id!r} exceeded its cooperative deadline")
        self.check_result(spec, result, "component")
        return _timed(result, started_at, time.time())

    def publish_result(
        self,
        spec: ComponentSpec,
        result: ComponentResult,
        passed: str = "completed",
        failed: str = "failed",
        *,
        cleanup: bool = False,
    ) -> None:
        self.emit(
            f"{spec.id}.{passed if result.status is ComponentStatus.PASS else failed}",
            source=spec.id,
            payload={"status": result.status.value, "summary": result.summary},
            cleanup=cleanup,
        )

    def invoke(
        self, spec: ComponentSpec, method: str, *args: Any, applying: bool = False, raise_error: bool = True
    ) -> ComponentResult:
        """Publish ``<id>.started``, call ``method``, record its result, and publish it; ERROR fails the run
        (unless ``raise_error`` is False: the caller decides)."""

        self.current = spec
        self.emit(f"{spec.id}.started", source=spec.id)
        if ROLES[spec.role].teardown is not None:
            # Torn down once its start is called, whatever the call does; never if it is not.
            self.teardowns.append(spec)
        try:
            result = self.call(spec, method, *args, applying=applying)
        finally:
            if ROLES[spec.role].teardown is None:
                self.contract.finished(spec.id)
        self.results[spec.id] = result
        self.invoked.add(spec.id)
        self.evidence.extend(result.evidence)
        self.publish_result(spec, result)
        if raise_error and result.status is ComponentStatus.ERROR:
            raise RunnerError(f"{spec.role.value} {spec.id!r} returned ERROR: {result.summary}")
        return result

    def instantiate(self) -> None:
        for spec in self.ordered:
            registration = self.catalog.components.resolve(spec.use)
            try:
                instance = registration.factory()
            except Exception as exc:
                raise RunnerError(f"factory for component {spec.use!r} failed: {exc}") from exc
            protocol = ROLES[spec.role].protocol
            if isinstance(instance, type) or not isinstance(instance, protocol):
                raise RunnerError(
                    f"factory for component {spec.use!r} returned {type(instance).__name__}, "
                    f"which does not implement {protocol.__name__}"
                )
            self.objects[spec.id] = instance

    # -- phases ----------------------------------------------------------------

    def revalidate(self) -> None:
        """Check the executable plan, whose environment references are now resolved, as the compiled one was."""

        plan = self.context.plan
        bound = deployment_has_references(self.context.source_plan)
        if bound:
            self.provider.validate(plan)
        with _plan_check():
            if bound:
                plan = with_planned_instances(plan, deployment_registration(plan, self.catalog))
            plan = self.context.plan = check_plan(plan, self.catalog)
        self.contract.bind(plan)
        # Execute the resolved component specs, not the compiled ones.
        self.ordered = plan.ordered_components

    def probe_capabilities(self) -> None:
        self.error_phase = "capability.probe"
        required = self.required
        for spec in self.ordered:
            if spec.role is not ComponentRole.CAPABILITY or self.blocked(spec):
                continue
            result = self.invoke(spec, ROLES[spec.role].call)
            if spec.id in required and result.status is ComponentStatus.SKIP:
                raise _EarlyTerminalError(
                    OutcomeStatus.SKIP_UNSUPPORTED, f"required capability {spec.id!r} is unsupported"
                )
            if spec.id in required and result.status is ComponentStatus.FAIL:
                raise _EarlyTerminalError(
                    OutcomeStatus.ERROR,
                    f"required capability {spec.id!r} failed; a capability probe establishes support, "
                    "not the behavior under test, so no trustworthy verdict is possible",
                )

    def provision(self) -> None:
        self.error_phase = "deployment.provision"
        plan = self.context.plan
        deadline = self.context.run_deadline
        if deadline is not None and time.monotonic() >= deadline:
            raise CooperativeDeadlineError("deadline expired before deployment provision")
        provider = self.provider
        self.emit(DEPLOYMENT_PROVISIONING, source="runner", payload={"deployment_mode": plan.deployment_mode})
        try:
            deployment = provider.provision(self.context)
            if not isinstance(deployment, ProvisionedDeployment):
                raise RunnerError(
                    f"deployment provider {plan.deployment_mode!r} returned {type(deployment).__name__}, "
                    "expected ProvisionedDeployment"
                )
        except BaseException as provision_error:
            self.release_deployment("rollback", provider.rollback)
            if self.interrupt is not None and isinstance(provision_error, Exception):
                raise self.interrupt from provision_error
            raise
        self.deployment = deployment
        self.context.bind_deployment(deployment.services)
        if deadline is not None and time.monotonic() >= deadline:
            raise CooperativeDeadlineError("deployment provision exceeded the run deadline")
        self.emit(DEPLOYMENT_READY, source="runner", payload={"deployment_mode": plan.deployment_mode})

    def release_deployment(self, step: str, method: Callable[..., Any], *args: Any) -> None:
        """Roll back or clean up the deployment, publishing ``deployment.<step>.started`` and ``.completed``."""

        started, completed = DEPLOYMENT_RELEASE_EVENTS[step]
        self.cleanup_step(
            "deployment",
            f"{step} event",
            lambda: self.emit(started, source="runner", cleanup=True),
        )
        self.context.cleaning_up = True
        try:
            released = self.cleanup_step("deployment", step, lambda: method(self.context, *args))
        finally:
            self.context.cleaning_up = False
        if released:
            self.cleanup_step(
                "deployment", f"{step} result event",
                lambda: self.emit(completed, source="runner", cleanup=True),
            )  # fmt: skip

    def start_components(self) -> None:
        for spec in self.ordered:
            lifecycle = ROLES[spec.role]
            if lifecycle.stage is not Stage.START or self.blocked(spec):
                continue
            self.error_phase = f"{spec.role.value}.{lifecycle.call}"
            self.invoke(spec, lifecycle.call, applying=lifecycle.teardown is not None)

    def tear_down_components(self) -> None:
        for spec in reversed(self.teardowns):
            call = self.running_starts.get(spec.id)
            if call is not None and call.is_alive():
                # Its start call is still running: a teardown now would race
                # it, and whatever it created afterwards would never be released.
                self.deferred_teardowns.append(spec)
                continue
            self.tear_down(spec)

    def tear_down_deferred(self) -> None:
        """Tear down the components whose start outlived the teardown, once it returned; report the rest.

        Runs after deployment cleanup, which may stop what a start blocks on.
        """

        if not self.deferred_teardowns:
            return
        self.wait_for_cancelled_calls()
        for spec in self.deferred_teardowns:
            if not self.running_starts[spec.id].is_alive():
                self.tear_down(spec)
                continue
            self.record_cleanup_error(
                spec.id,
                ROLES[spec.role].teardown,
                RunnerError(
                    f"component {spec.id!r} was not torn down: its {ROLES[spec.role].call} call is still running, "
                    "so anything it creates is not released"
                ),
            )

    def tear_down(self, spec: ComponentSpec) -> None:
        method = ROLES[spec.role].teardown
        self.cleanup_step(
            spec.id,
            "cleanup event",
            lambda: self.emit(f"{spec.id}.stopping", source=spec.id, cleanup=True),
        )
        started_at, started = time.time(), time.monotonic()
        self.context.cleaning_up = True
        try:
            # Teardown ignores the run deadline but gets the component's own
            # timeout as a fresh window, so a hung stop cannot hang the run.
            result = self.watch(
                lambda: getattr(self.objects[spec.id], method)(self.context, spec),
                deadline=None if spec.timeout_s is None else started + spec.timeout_s,
                label=f"component {spec.id!r} {method}",
            )
            self.check_result(spec, result, "cleanup for component")
        except BaseException as exc:
            self.record_cleanup_error(spec.id, method, exc)
            original = self.results.get(spec.id)
            if original is not None:
                # It did not do its part: a finding that depends on it is not trusted.
                summary = f"{method} failed: {type(exc).__name__}: {exc}"
                self.results[spec.id] = replace(original, status=ComponentStatus.ERROR, summary=summary)
            return
        finally:
            self.context.cleaning_up = False
            self.contract.finished(spec.id)
        result = _timed(result, started_at, time.time())
        self.evidence.extend(result.evidence)
        original = self.results.get(spec.id)
        if original is not None:
            diagnostics = dict(to_plain_data(original.diagnostics))
            diagnostics["teardown"] = {
                "status": result.status.value,
                "summary": result.summary,
                "diagnostics": to_plain_data(result.diagnostics),
            }
            # A teardown that did not pass (a fault that never fired, an observer that could not read the
            # server at the end) is the component's result: it did not do its part.
            failed = result.status is not ComponentStatus.PASS
            self.results[spec.id] = replace(
                original,
                status=result.status if failed else original.status,
                summary=result.summary if failed else original.summary,
                evidence=original.evidence + result.evidence,
                diagnostics=diagnostics,
                ended_at=result.ended_at,
            )
        self.cleanup_step(
            spec.id,
            "cleanup result event",
            lambda: self.publish_result(spec, result, passed="stopped", failed="stop-failed", cleanup=True),
        )
        if result.status is not ComponentStatus.PASS:
            self.cleanup_errors.append(
                {"owner": spec.id, "phase": method, "status": result.status.value, "summary": result.summary}
            )

    def evaluate_oracles(self) -> None:
        """Evaluate every oracle. Each judges on its own, so one that could not tell (it returned ERROR or
        raised) does not stop the rest: when another required oracle FAILs, that finding about the server
        stands, as when one oracle finds the server dead that another could not read. Otherwise the first
        ERROR fails the run."""

        errored: list[tuple[ComponentSpec, Optional[Exception]]] = []
        for spec in self.ordered:
            if spec.role is not ComponentRole.ORACLE or self.blocked(spec):
                continue
            self.error_phase = "oracle.evaluate"
            try:
                result = self.invoke(spec, ROLES[spec.role].call, tuple(self.evidence), raise_error=False)
            except Exception as exc:
                self.results[spec.id] = _component_error(spec, exc, self.error_phase)
                errored.append((spec, exc))
                continue
            if result.status is ComponentStatus.ERROR:
                errored.append((spec, None))
        found = any(
            spec.role is ComponentRole.ORACLE
            and spec.id in self.required
            and self.results[spec.id].status is ComponentStatus.FAIL
            for spec in self.ordered
            if spec.id in self.results
        )
        if errored and not found:
            first, raised = errored[0]
            if raised is not None:
                raise raised
            raise RunnerError(f"oracle {first.id!r} returned ERROR: {self.results[first.id].summary}")
        self.error_phase = "validation"
        self.emit(VALIDATION_COMPLETED, source="runner")

    def clean_up_deployment(self) -> None:
        if self.deployment is not None:
            self.release_deployment("cleanup", self.provider.cleanup, self.deployment)

    def release_obligations(self, *, final: bool = False) -> None:
        """Undo every obligation still open, except those of a start call still running; ``final`` closes the
        ledger, so whatever that call, or anything else, changes from now on is undone once it can be."""

        self.context.cleaning_up = True
        try:
            if final:
                self.context.obligations.close()
            else:
                self.context.obligations.sweep()
        except BaseException as exc:
            # Ctrl-C during an undo: the rest are undone, the deployment still cleaned up, and then it is raised.
            self.interrupt = self.interrupt or exc
        finally:
            self.context.cleaning_up = False

    def obligation_diagnostics(self) -> dict[str, Any]:
        ledger = self.context.obligations
        diagnostics: dict[str, Any] = {}
        if not ledger.empty:
            diagnostics["obligations"] = ledger.report()
        failures = ledger.failures()
        if failures:
            diagnostics["open_obligations"] = failures
        return diagnostics

    def wait_for_cancelled_calls(self) -> None:
        """Wait once more for cancelled calls, now that cleanup stopped what they may block on."""

        if self.waited_for_cancelled_calls:
            return
        self.waited_for_cancelled_calls = True
        until = time.monotonic() + deadlines.CANCEL_GRACE_S
        try:
            for call in self.cancelled_calls:
                call.join(max(0.0, until - time.monotonic()))
        except BaseException as exc:
            # Ctrl-C ends the wait, not the cleanup after it.
            self.interrupt = self.interrupt or exc

    def close_journal(self) -> None:
        """Publish ``run.completed`` and quiesce the event bus; either step runs even if the other failed."""

        steps = (
            (lambda: self.emit(RUN_COMPLETED, source="runner", cleanup=True), RUN_COMPLETED),
            (self.bus.quiesce, "event_bus.quiesce"),
        )
        for step, phase in steps:
            try:
                step()
            except BaseException as exc:
                if not isinstance(exc, Exception):
                    self.interrupt = self.interrupt or exc
                elif self.error is None:
                    self.error, self.error_phase = exc, phase

    # -- the run ---------------------------------------------------------------

    @property
    def ok(self) -> bool:
        return self.terminal is None and self.error is None and self.interrupt is None

    def run(self) -> RunOutcome:
        """Every phase of the run, in order; a failed preflight raises :class:`PreflightError`."""

        plan = self.context.plan
        self.publish_unchecked(RUN_STARTED, source="runner", payload={"scenario_id": plan.scenario_id})
        abandoned = deadlines.abandoned_calls()
        if abandoned:
            self.early_diagnostics["abandoned_calls"] = abandoned
            self.terminal = _EarlyTerminalError(
                OutcomeStatus.ERROR,
                "refused to start: a component call abandoned by an earlier run is still running in this process",
            )
            return self.finish()
        try:
            plan, self.provider = _preflight(self.catalog, plan)
        except Exception as exc:
            raise PreflightError(str(exc)) from exc
        self.context.plan = self.context.source_plan = plan
        self.ordered, self.required = plan.ordered_components, verdict.required_components(plan)
        self.contract.bind(plan)
        self.context.obligations.plan(self.contract.planned_changes())
        # Probes see the executable plan. Without environment references that
        # is the compiled plan, so they can decide support before acquisition.
        references = plan_has_references(plan)

        def setup() -> None:
            self.instantiate()
            if not references:
                self.probe_capabilities()
            self.acquire()
            self.bind(references)
            self.provision()
            self.start_components()

        self.guard(setup)
        self.tear_down_components()
        # What the components did not undo is undone before the oracles judge the deployment.
        self.release_obligations()
        # A teardown that failed so far may leave the oracles a deployment the plan does not describe.
        self.teardown_errors = len(self.cleanup_errors)
        if self.ok:
            self.guard(self.evaluate_oracles)
        self.clean_up_deployment()
        self.tear_down_deferred()
        return self.finish()

    def acquire(self) -> None:
        plan = self.context.plan
        try:
            self.lease = self.provisioner.acquire(
                plan.artifacts,
                plan.resources,
                run_id=uuid.uuid4().hex,
                seed=plan.seed,
                deadline=self.context.run_deadline,
                policies=plan.policies,
            )
        except Exception as exc:
            self.early_diagnostics.update(_environment_diagnostics(getattr(exc, "environment_report", None), exc))
            if isinstance(exc, UnsupportedEnvironmentError):
                raise _EarlyTerminalError(
                    OutcomeStatus.SKIP_UNSUPPORTED, "the requested environment is unsupported"
                ) from exc
            raise _EarlyTerminalError(OutcomeStatus.ERROR, "environment acquisition failed") from exc

    def bind(self, references: bool) -> None:
        """Resolve the plan's environment references; a plan that had some is revalidated and probed now."""

        self.error_phase = "environment binding"
        self.context.plan = resolve_plan(self.context.plan, self.lease)
        if references:
            self.error_phase = "preflight"
            self.revalidate()
            self.probe_capabilities()

    def release_environment(self) -> None:
        if self.lease is None:
            return
        try:
            self.environment_report = self.lease.close()
        except BaseException as exc:
            if self.interrupt is not None:
                # The interrupt is raised next, so no report records this.
                logger.error("Releasing the environment of an interrupted run failed: %s", exception_type(exc))
                return
            if not isinstance(exc, EnvironmentLifecycleError):
                raise
            self.environment_error = exc
            self.environment_report = exc.environment_report or self.lease.report

    def finish(self) -> RunOutcome:
        self.wait_for_cancelled_calls()
        # The final cleanup: retry every undo that failed, now that the deployment is gone.
        self.release_obligations(final=True)
        self.close_journal()
        self.release_environment()
        if self.interrupt is not None:
            raise self.interrupt
        return self.verdict()

    # -- verdict ---------------------------------------------------------------

    def verdict(self) -> RunOutcome:
        plan = self.context.source_plan
        decision = verdict.decide(
            verdict.RunState(
                plan=plan,
                results=self.results,
                invoked=self.invoked,
                evidence=self.evidence,
                committed=self.bus.committed_history,
                terminal=None if self.terminal is None else (self.terminal.status, self.terminal.summary),
                runner_failure=None
                if self.error is None
                else f"{self.error_phase}: {type(self.error).__name__}: {self.error}",
                teardown_failed=self.teardown_errors > 0,
                cleanup_failed=len(self.cleanup_errors) > self.teardown_errors,
                violations=tuple(self.contract.violations),
                open_obligations=self.context.obligations.failures(),
            )
        )
        diagnostics: dict[str, Any] = dict(self.early_diagnostics)
        if self.deployment is not None and self.deployment.metadata:
            diagnostics["deployment"] = self.deployment.metadata
        if self.cleanup_errors:
            diagnostics["cleanup_errors"] = tuple(self.cleanup_errors)
        abandoned = [call.label for call in self.cancelled_calls if call.is_alive()]
        if abandoned:
            # Every later run in this process is refused until these return.
            diagnostics["abandoned_calls"] = tuple(
                {"scenario_id": plan.scenario_id, "call": label} for label in abandoned
            )
        diagnostics.update(decision.diagnostics)
        diagnostics.update(self.obligation_diagnostics())
        if self.error is not None:
            diagnostics["error"] = error_record(self.error, self.error_phase)
        status, summary = decision.status, decision.summary
        if self.lease is not None:
            diagnostics.update(_environment_diagnostics(self.environment_report, self.environment_error))
            status, summary = verdict.environment_status(
                status,
                summary,
                trusted=decision.trusted,
                cleaned=self.environment_error is None and self.environment_report.status == "cleaned",
            )
        # Every published event, and whether its dispatch completed (only committed events count as observed).
        committed = {event.sequence for event in self.bus.committed_history()}
        diagnostics["events"] = [
            {**to_plain_data(event), "committed": event.sequence in committed} for event in self.bus.attempted_history()
        ]
        return RunOutcome(
            plan=plan,
            status=status,
            summary=summary,
            component_results=tuple(self.results[spec.id] for spec in self.ordered if spec.id in self.results),
            diagnostics=diagnostics,
            started_at=self.started_at,
            ended_at=max(self.started_at, time.time()),
            valkey_sha=None if self.deployment is None else self.deployment.valkey_sha,
        )
