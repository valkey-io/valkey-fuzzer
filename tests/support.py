"""Builders and fakes that the framework tests share."""

import copy
import json
import os
import signal
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import patch

from valkey_fuzzer.catalog import Catalog, ComponentRegistry, DeploymentProviderRegistry
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.environment import (
    EnvironmentProviderRegistration,
    EnvironmentProviderRegistry,
    EnvironmentResult,
    SupportDecision,
)
from valkey_fuzzer.plan import DeploymentProviderIdentity, ExecutionPlan, RunMode
from valkey_fuzzer.protocols import ComponentRole
from valkey_fuzzer.registrations import ComponentRegistration, DeploymentProviderRegistration

FAKE_DEPLOYMENT_PROVIDER = "tests.deployment.fake"


def component_registration(component_id="example.action.fake", role=None, *, factory=object, **options):
    """A registration of ``component_id`` in the role its ID names, declaring nothing unless ``declares`` says."""

    role = role or ComponentRole(component_id.split(".")[1])
    options.setdefault("declares", Declarations())
    return ComponentRegistration(component_id=component_id, role=role, factory=factory, **options)


def deployment_identity() -> DeploymentProviderIdentity:
    """The identity of the provider :func:`deployment_registrations` registers."""

    return DeploymentProviderIdentity(FAKE_DEPLOYMENT_PROVIDER)


def deployment_registrations(**factories) -> DeploymentProviderRegistry:
    """A registry with a :data:`FAKE_DEPLOYMENT_PROVIDER` registration per ``<deployment mode>=<factory>``."""

    registry = DeploymentProviderRegistry()
    registry.register_many(
        DeploymentProviderRegistration(deployment_mode=mode, provider_id=FAKE_DEPLOYMENT_PROVIDER, factory=factory)
        for mode, factory in factories.items()
    )
    return registry


def make_catalog(
    components: Optional[ComponentRegistry] = None,
    deployments: Optional[DeploymentProviderRegistry] = None,
    *,
    environment: Optional[EnvironmentProviderRegistry] = None,
) -> Catalog:
    """A catalog holding the entries of the given registries, as a runner or compiler is given one."""

    catalog = Catalog()
    for registry, source in (
        (catalog.components, components),
        (catalog.deployment_providers, deployments),
        (catalog.environment_providers, environment),
    ):
        if source is not None:
            registry.register_many(source.snapshot().values())
    return catalog


def make_plan(**fields: Any) -> ExecutionPlan:
    """A smoke-mode standalone plan without components for the fake deployment provider; ``fields`` override."""

    defaults = {"scenario_id": "test-scenario", "experiment_id": "tests.feature", "seed": 1, "components": ()}
    defaults.update(deployment_mode="standalone", deployment_provider=deployment_identity(), mode=RunMode.SMOKE)
    return ExecutionPlan(**{**defaults, **fields})


class ProviderBehavior:
    """What a :class:`FakeEnvironmentProvider` returns or raises, and the calls it saw.

    ``support`` is the support decision, or an exception ``check_support`` raises. Each call is recorded in
    ``calls`` as ``(method, dependency path)``, with its context and resolved config; ``log``, a list
    several fakes share, also gets ``<kind>.<method>``.
    """

    def __init__(self, provider_id, *, outputs=None, provenance=None, support=None, log=None, **errors):
        self.provider_id = provider_id
        self.outputs, self.provenance = dict(outputs or {}), dict(provenance or {})
        self.support = SupportDecision.allow() if support is None else support
        # acquire_error, rollback_error, cleanup_error
        self.errors, self.log = errors, log
        self.calls: list[tuple[str, str]] = []
        self.contexts: dict[str, Any] = {}
        self.configs: dict[str, Any] = {}

    def capture(self, method, context, spec, config=None) -> None:
        self.calls.append((method, spec.dependency.path))
        if self.log is not None:
            self.log.append(f"{spec.kind.name.lower()}.{method}")
        self.contexts[method] = context
        if config is not None:
            self.configs[method] = config
        error = self.errors.get(f"{method}_error")
        if error is not None:
            raise error


class FakeEnvironmentProvider:
    """An environment provider that does what its :class:`ProviderBehavior` says."""

    def __init__(self, behavior: ProviderBehavior):
        self.behavior = behavior

    def check_support(self, context, spec, resolved_config):
        self.behavior.capture("support", context, spec, resolved_config)
        if isinstance(self.behavior.support, BaseException):
            raise self.behavior.support
        return self.behavior.support

    def acquire(self, context, spec, resolved_config):
        self.behavior.capture("acquire", context, spec, resolved_config)
        return EnvironmentResult(self.behavior.outputs, self.behavior.provenance)

    def rollback(self, context, spec) -> None:
        self.behavior.capture("rollback", context, spec)

    def cleanup(self, context, spec, result) -> None:
        assert result.outputs == self.behavior.outputs
        self.behavior.capture("cleanup", context, spec)


def environment_provider(behavior: ProviderBehavior, kind) -> EnvironmentProviderRegistration:
    """``behavior``'s provider registered for ``kind`` declarations, declaring its outputs."""

    factory = lambda: FakeEnvironmentProvider(behavior)  # noqa: E731
    return EnvironmentProviderRegistration(kind, behavior.provider_id, factory, outputs=tuple(behavior.outputs))


def recorded_report(service, tmp_path, edit=None) -> str:
    """Run examples/valkey/forkless_save.yaml with ``service`` and save its report, changed by ``edit``, for a replay."""

    prepared = service.prepare_run(
        str(Path(__file__).resolve().parents[1] / "examples" / "valkey" / "forkless_save.yaml")
    )
    report = copy.deepcopy(
        service.execute(prepared.plan, scenario=prepared.scenario, report_dir=str(tmp_path / "r")).report
    )
    if edit is not None:
        edit(report)
    (tmp_path / "report.json").write_text(json.dumps(report))
    return str(tmp_path / "report.json")


class VirtualClock:
    """A clock that moves only when the code under test waits: each sleep returns at once, that much later.

    :meth:`install` makes it the ``time`` of the given modules (their ``monotonic``, ``time``, and ``sleep``);
    :meth:`drive` makes a FakeContext's ``sleep`` advance it. A wait still ends on what it waits for, or at its
    deadline, in no real time. For one thread's waits: a sleep moves the clock for every thread. The clock starts
    at the real time, so compare ``elapsed`` with ``pytest.approx``: deadlines computed from it round.
    """

    def __init__(self):
        self.elapsed = 0.0
        self._start, self._epoch = time.monotonic(), time.time()
        self._time = SimpleNamespace(**{name: getattr(time, name) for name in dir(time) if not name.startswith("_")})
        # Through the instance, so a test can replace one of them (a wall clock that jumps, say).
        self._time.monotonic = lambda: self.monotonic()
        self._time.time = lambda: self.time()
        self._time.sleep = lambda seconds: self.sleep(seconds)

    def monotonic(self) -> float:
        return self._start + self.elapsed

    def time(self) -> float:
        return self._epoch + self.elapsed

    def sleep(self, seconds: float) -> None:
        self.elapsed += max(0.0, seconds)

    def install(self, monkeypatch: Any, *modules: Any) -> "VirtualClock":
        for module in modules:
            monkeypatch.setattr(module, "time", self._time)
        return self

    def drive(self, context: Any) -> Any:
        """``context`` (a FakeContext), whose ``sleep`` advances this clock unless the call was cancelled."""

        context.sleep = lambda seconds: not context.cancelled and (self.sleep(seconds) or True)
        return context


def sigterm_during(target, call):
    """``call()`` with ``target`` replaced by a handler that gets SIGTERM; what ran, and ``call``'s result.

    SIGTERM must reach the handler, never kill the process outright (exit 143: no teardown, no deployment
    cleanup, servers left running). Here a stand-in handler records it instead of dying, so a missed SIGTERM
    fails the test.
    """

    seen = []

    def stopped(*args, **kwargs):
        try:
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(2)
        finally:
            seen.append("cleaned up")
        return 0

    def stand_in(*_):
        seen.append("terminated")

    previous = signal.signal(signal.SIGTERM, stand_in)
    try:
        with patch(target, side_effect=stopped):
            code = call()
        assert signal.getsignal(signal.SIGTERM) is stand_in, "the handler it replaced is back in place"
    finally:
        signal.signal(signal.SIGTERM, previous)
    return seen, code


def valkey_build(directory: Path, knows: str) -> Path:
    """A valkey-server in ``directory`` that knows forkless saves only by config ``knows`` (bgsave-default-method,
    before valkey-io/valkey#5261, or forkless-policy): it rejects the other name as Valkey rejects a directive it
    does not know, and exits printing its arguments, never starting."""

    other = {"bgsave-default-method": "forkless-policy", "forkless-policy": "bgsave-default-method"}[knows]
    binary = directory / "valkey-server"
    binary.write_text(
        f'#!/bin/sh\ncase " $* " in *" --{other} "*) echo "Bad directive or wrong number of arguments";; esac\necho "$*"\nexit 1\n'
    )
    binary.chmod(0o755)
    return binary
