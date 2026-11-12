"""Unit-test helpers: run an experiment in-process, or call one component directly.

``run_experiment`` compiles and executes an experiment through the real
compiler and runner with a fake deployment whose services you supply, so a
test can check components, events, evidence, and verdicts without starting
servers::

    outcome = run_experiment(experiment, component_registrations(), services={"valkey.deployment": handle})
    assert outcome.status is OutcomeStatus.PASS

``FakeContext`` is a :class:`~valkey_fuzzer.protocols.RunContext` for calling
a single component's lifecycle methods, and ``resolved_spec`` gives that
component its config with defaults, as the compiler would. Given the
components' registrations, :meth:`FakeContext.call` holds a call to its
declarations, as the runner does::

    context = FakeContext(spec, registrations=REGISTRATIONS)
    result = context.call(Touch().run, spec)
"""

import random
import shutil
import tempfile
import threading
import time
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, Callable, Optional, Union

from .catalog import Catalog, ComponentRegistry
from .environment import EnvironmentProviderRegistration
from .events import EventBus
from .evidence import ContractViolationError, EvidenceContract
from .obligations import ObligationLedger
from .plan import ComponentSpec, DeploymentProviderIdentity, ExecutionPlan, ExperimentSpec, RunMode
from .protocols import ROLES, ProvisionedDeployment
from .registrations import ComponentRegistration, DeploymentProviderRegistration
from .results import ComponentResult, OutcomeStatus, RunOutcome
from .service import RunService

DEPLOYMENT_SERVICES = Union[Mapping[str, Any], Callable[[ExecutionPlan], Mapping[str, Any]]]
FAKE_PROVIDER_ID = "valkey-fuzzer.deployment.fake"


class FakeDeploymentProvider:
    """A deployment provider that starts nothing and publishes the given services.

    Every call is appended to ``calls`` (a new list, or one you share with
    other fakes) as its method name, or as ``<name>.<method>`` given a
    ``name``. ``failures`` maps a method name to the exception it raises.
    """

    def __init__(
        self,
        services: DEPLOYMENT_SERVICES = None,
        *,
        calls: Optional[list[str]] = None,
        name: Optional[str] = None,
        failures: Optional[Mapping[str, BaseException]] = None,
    ):
        self._services = services or {}
        self.calls: list[str] = [] if calls is None else calls
        self._prefix = "" if name is None else f"{name}."
        self._failures = dict(failures or {})

    def _record(self, method: str) -> None:
        self.calls.append(f"{self._prefix}{method}")
        if method in self._failures:
            raise self._failures[method]

    def validate(self, plan: ExecutionPlan) -> None:
        self._record("validate")

    def provision(self, context: Any) -> ProvisionedDeployment:
        self._record("provision")
        services = self._services(context.plan) if callable(self._services) else self._services
        return ProvisionedDeployment(services=services)

    def rollback(self, context: Any) -> None:
        self._record("rollback")

    def cleanup(self, context: Any, deployment: ProvisionedDeployment) -> None:
        self._record("cleanup")


def fake_deployment_providers(
    deployment_modes: Sequence[str], services: DEPLOYMENT_SERVICES = None
) -> tuple[DeploymentProviderRegistration, ...]:
    """Registrations of :class:`FakeDeploymentProvider` for each deployment mode."""

    return tuple(
        DeploymentProviderRegistration(
            deployment_mode=mode,
            provider_id=FAKE_PROVIDER_ID,
            factory=lambda: FakeDeploymentProvider(services),
        )
        for mode in deployment_modes
    )


def run_experiment(
    experiment: ExperimentSpec,
    registrations: Sequence[ComponentRegistration] = (),
    *,
    inputs: Optional[Mapping[str, Any]] = None,
    deployment_mode: Optional[str] = None,
    deployment: Optional[Mapping[str, Any]] = None,
    services: DEPLOYMENT_SERVICES = None,
    environment_providers: Sequence[EnvironmentProviderRegistration] = (),
    deployment_providers: Sequence[DeploymentProviderRegistration] = (),
    seed: int = 1,
    scenario_id: str = "test",
    artifact_dir: Optional[str] = None,
    deadline: Optional[float] = None,
) -> RunOutcome:
    """Compile and run ``experiment`` with the components of ``registrations`` against a fake deployment.

    ``deployment_mode`` defaults to the experiment's first supported mode.
    Modes ``deployment_providers`` do not provide get a
    :class:`FakeDeploymentProvider` whose ``services`` (a mapping, or a
    function of the executable plan) become ``context.services["deployment"]``.
    Only the given entries are registered. Without ``artifact_dir`` the run's
    files go to a temporary folder that is removed when it returns.
    """

    if artifact_dir is None:
        with tempfile.TemporaryDirectory(prefix="valkey-fuzzer-test-") as temporary:
            return run_experiment(
                experiment, registrations, inputs=inputs, deployment_mode=deployment_mode, deployment=deployment,
                services=services, environment_providers=environment_providers,
                deployment_providers=deployment_providers, seed=seed, scenario_id=scenario_id,
                artifact_dir=temporary, deadline=deadline,
            )  # fmt: skip
    provided = {entry.deployment_mode for entry in deployment_providers}
    fakes = fake_deployment_providers([mode for mode in experiment.deployment_modes if mode not in provided], services)
    catalog = Catalog()
    catalog.register(
        components=registrations,
        experiments=(experiment,),
        environment_providers=environment_providers,
        deployment_providers=(*deployment_providers, *fakes),
    )
    catalog.freeze()
    service = RunService(catalog=catalog, artifact_dir=artifact_dir)
    document = dict(deployment or {})
    document.setdefault("mode", deployment_mode or experiment.deployment_modes[0])
    plan = service.compile(
        {
            "scenario_id": scenario_id,
            "seed": seed,
            "experiment": {"id": experiment.experiment_id, "inputs": dict(inputs or {})},
            "deployment": document,
        }
    )
    # Through the runner, so a preflight error raises rather than becoming an ERROR outcome.
    return service.runner.run(plan, deadline=deadline)


def resolved_spec(registration: ComponentRegistration, spec: ComponentSpec) -> ComponentSpec:
    """``spec`` with its config resolved by ``registration``'s schema, as a compiled plan holds it.

    Components read their defaults from ``spec.config``, so a test that calls
    one directly passes it this. The registration's ``check`` does not run.
    """

    if registration.config is None:
        return spec
    return replace(spec, config=registration.config.resolve(spec.config, f"components.{spec.id}.config"))


class FakeContext:
    """A run context for calling one component's lifecycle methods directly.

    Without ``artifact_dir``, components write to a temporary folder that is
    removed with the context. :meth:`cancel` does what the runner does when a
    call outlives its timeout. Given ``registrations`` (the components'), :meth:`call`
    holds a component to its registration's declarations.
    """

    def __init__(
        self,
        *components: ComponentSpec,
        services: Optional[Mapping[str, Any]] = None,
        plan: Optional[ExecutionPlan] = None,
        deployment_mode: str = "standalone",
        deployment: Optional[Mapping[str, Any]] = None,
        seed: int = 1,
        artifact_dir: Optional[str] = None,
        deadline: Optional[float] = None,
        registrations: Sequence[ComponentRegistration] = (),
    ):
        if plan is None:
            experiment = ExperimentSpec(
                experiment_id="testing.component",
                mode=RunMode.SMOKE,
                deployment_modes=(deployment_mode,),
                components=components,
            )
            plan = ExecutionPlan.from_experiment(
                experiment,
                scenario_id="component-test",
                seed=seed,
                deployment_mode=deployment_mode,
                deployment_provider=DeploymentProviderIdentity(FAKE_PROVIDER_ID),
                deployment={"mode": deployment_mode, **(deployment or {})},
            )
        self.plan = plan
        self._components = ComponentRegistry()
        self._components.register_many(registrations)
        self._contract = EvidenceContract(self._components)
        # The components held to account: those called through call().
        self._checked: dict[str, ComponentSpec] = {}
        self.event_bus = EventBus(check=self._check_event)
        self.services = {"deployment": dict(services or {})}
        if artifact_dir is None:
            artifact_dir = tempfile.mkdtemp(prefix="valkey-fuzzer-test-")
            weakref.finalize(self, shutil.rmtree, artifact_dir, ignore_errors=True)
        self.artifact_dir = artifact_dir
        self.deadline = deadline
        # Obligations opened here are undone only when the test discharges them.
        self.obligations = ObligationLedger(check=self._check_change)
        self._rngs: dict[tuple[str, str], random.Random] = {}
        self._cancelled = threading.Event()

    @property
    def violations(self) -> list[Mapping[str, Any]]:
        """Every declaration a component called through :meth:`call` broke; a test may clear what it expects."""

        return self._contract.violations

    def call(self, method: Callable[..., Any], spec: ComponentSpec, *args: Any) -> Any:
        """``method(self, spec, *args)``, a component's bound lifecycle method, held to its declarations.

        As in a run, the events it publishes, the state it changes, and the
        evidence it returns must be ones its registration declares, and after
        its last call (any but a start with a teardown) it may publish or
        change nothing. A violation raises :class:`ContractViolationError`,
        even one the component caught, and stays in :attr:`violations`.
        """

        registration = self._components.resolve(spec.use)
        if spec.id not in self._checked:
            self._checked[spec.id] = replace(spec, role=registration.role)
            components = tuple(self._checked.values())
            self._contract.bind(replace(self.plan, components=components, required_events=(), required_evidence=()))
        lifecycle = ROLES[registration.role]
        try:
            result = method(self, spec, *args)
        finally:
            if lifecycle.teardown is None or method.__name__ == lifecycle.teardown:
                self._contract.finished(spec.id)
        if isinstance(result, ComponentResult):
            committed = {event.sequence for event in self.event_bus.committed_history()}
            self._contract.check_evidence(spec, result.evidence, committed)
        if self.violations:
            raise ContractViolationError(self.violations[0]["message"])
        return result

    def _check_event(self, name: str, source: str, payload: Optional[Mapping[str, Any]]) -> None:
        if source in self._checked:
            self._contract.check_event(name, source, payload)

    def _check_change(self, owner: str, kind: str) -> None:
        if owner in self._checked:
            self._contract.check_change(owner, kind)

    def rng(self, component_id: str, stream: str = "default") -> random.Random:
        key = (component_id, stream)
        if key not in self._rngs:
            self._rngs[key] = self.plan.rng(component_id, stream)
        return self._rngs[key]

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def cancel(self) -> None:
        """Cancel the call under test: ``cancelled`` turns true, ``sleep`` returns early, the deadline passes."""

        self.deadline = time.monotonic()
        self._cancelled.set()

    def sleep(self, seconds: float) -> bool:
        return not self._cancelled.wait(max(0.0, seconds))


__all__ = [
    "FAKE_PROVIDER_ID",
    "FakeContext",
    "FakeDeploymentProvider",
    "OutcomeStatus",
    "fake_deployment_providers",
    "resolved_spec",
    "run_experiment",
]
