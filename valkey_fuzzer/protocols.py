"""The runtime contracts: the run context components and deployment providers receive, the protocols they
implement, how the runner drives each role (:data:`ROLES`), and the folders a component writes its files in.
"""

import os
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

from ._data import freeze_mapping
from .events import EventBus
from .fields import require_type
from .obligations import ObligationLedger

if TYPE_CHECKING:
    # The plan and results modules import this one for the roles.
    from .plan import ComponentSpec, ExecutionPlan
    from .results import ComponentResult, Evidence


class ComponentRole(str, Enum):
    """The role a component is registered for, which decides when the runner calls it."""

    CAPABILITY = "capability"
    WORKLOAD = "workload"
    ACTION = "action"
    FAULT = "fault"
    OBSERVER = "observer"
    ORACLE = "oracle"


class RunContext(Protocol):
    """Runner-owned services visible to components and deployment providers.

    ``plan`` is the executable plan, with environment references resolved.
    ``services["deployment"]`` holds the provisioned deployment's services.
    ``artifact_dir`` is this run's own folder for the files components write
    (its report folder, when the run has one); see :func:`component_artifact_dir`.
    ``deadline`` is an absolute monotonic time or ``None``: in a component
    call, the call's own deadline (the earlier of the run's and the
    component's ``timeout_s``; in ``stop`` and ``recover``, which ignore the
    run's, the ``timeout_s`` window alone).

    A component call that outlives its timeout is cancelled: on the thread the
    runner called it on, ``cancelled`` becomes true, ``sleep(seconds)``
    returns ``False`` early (``True`` after a full sleep), and ``deadline``
    has passed. Its result is discarded, so it should return promptly.

    ``obligations`` is the run's :class:`~valkey_fuzzer.obligations.ObligationLedger`:
    a component that changes host or server state opens an obligation when it
    applies the change and discharges it when it undoes it.
    """

    @property
    def plan(self) -> "ExecutionPlan": ...

    @property
    def event_bus(self) -> EventBus: ...

    def rng(self, component_id: str, stream: str = "default") -> random.Random: ...

    @property
    def services(self) -> Mapping[str, Any]: ...

    @property
    def artifact_dir(self) -> str: ...

    @property
    def deadline(self) -> Optional[float]: ...

    @property
    def cancelled(self) -> bool: ...

    def sleep(self, seconds: float) -> bool: ...

    @property
    def obligations(self) -> ObligationLedger: ...


@dataclass(frozen=True, eq=False)
class ProvisionedDeployment:
    """Runtime handle and services returned by a deployment provider."""

    handle: Any = field(default=None, repr=False, metadata={"serialize": False})
    services: Mapping[str, Any] = field(default_factory=dict, repr=False, metadata={"serialize": False})
    # Report-safe identity of the server build, recorded on the outcome.
    valkey_sha: Optional[str] = None
    # Report-safe provenance (instance layout, binaries, processes) recorded in the
    # outcome diagnostics so a reproducer shows what was actually tested.
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.valkey_sha is not None:
            require_type(self.valkey_sha, str, "valkey_sha")
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata, "deployment metadata"))
        if not isinstance(self.services, Mapping) or any(not isinstance(key, str) for key in self.services):
            raise TypeError("deployment services must be a mapping with string keys")
        object.__setattr__(self, "services", MappingProxyType(dict(self.services)))


@runtime_checkable
class DeploymentProvider(Protocol):
    """The deployment lifecycle a deployment provider implements.

    ``validate`` is pure. If ``provision`` fails the runner calls ``rollback``;
    after a successful provision it calls ``cleanup`` once with the returned
    deployment.
    """

    def validate(self, plan: "ExecutionPlan") -> None: ...

    def provision(self, context: RunContext) -> ProvisionedDeployment: ...

    def rollback(self, context: RunContext) -> None: ...

    def cleanup(self, context: RunContext, deployment: ProvisionedDeployment) -> None: ...


@runtime_checkable
class CapabilityProbe(Protocol):
    def probe(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...


@runtime_checkable
class Workload(Protocol):
    def start(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...

    def stop(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...


@runtime_checkable
class Action(Protocol):
    def run(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...


@runtime_checkable
class Fault(Protocol):
    """``inject`` installs the fault's trigger, and the fault is injected when it
    fires; ``recover`` undoes it at teardown and reports whether it was injected."""

    def inject(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...

    def recover(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...


@runtime_checkable
class Observer(Protocol):
    def start(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...

    def stop(self, context: RunContext, spec: "ComponentSpec") -> "ComponentResult": ...


@runtime_checkable
class Oracle(Protocol):
    def evaluate(
        self, context: RunContext, spec: "ComponentSpec", evidence: Sequence["Evidence"]
    ) -> "ComponentResult": ...


class Stage(IntEnum):
    """When the runner calls a role: a component may depend only on components of its stage or an earlier one."""

    # Capabilities are probed before the deployment exists.
    PROBE = 0
    # Observers, workloads, faults, and actions start on the deployment and are torn down before the oracles.
    START = 1
    # Oracles judge the run once everything is torn down.
    EVALUATE = 2


@dataclass(frozen=True)
class RoleLifecycle:
    """How the runner drives one role: the protocol its components implement, the method it calls, at which
    stage, the method that tears it down (if it keeps running), and whether its FAIL judges the system under
    test rather than leaving the run without a verdict."""

    protocol: type
    call: str
    stage: Stage
    teardown: Optional[str] = None
    judges: bool = False


ROLES: Mapping[ComponentRole, RoleLifecycle] = MappingProxyType(
    {
        ComponentRole.CAPABILITY: RoleLifecycle(CapabilityProbe, "probe", Stage.PROBE),
        ComponentRole.OBSERVER: RoleLifecycle(Observer, "start", Stage.START, teardown="stop"),
        ComponentRole.WORKLOAD: RoleLifecycle(Workload, "start", Stage.START, teardown="stop"),
        ComponentRole.FAULT: RoleLifecycle(Fault, "inject", Stage.START, teardown="recover"),
        ComponentRole.ACTION: RoleLifecycle(Action, "run", Stage.START, judges=True),
        ComponentRole.ORACLE: RoleLifecycle(Oracle, "evaluate", Stage.EVALUATE, judges=True),
    }
)


def run_artifact_dir(context: RunContext) -> str:
    """This run's own folder, ``context.artifact_dir`` (created on demand)."""

    directory = context.artifact_dir
    os.makedirs(directory, exist_ok=True)
    return directory


def component_artifact_dir(context: RunContext, spec: "ComponentSpec") -> str:
    """This run's directory for one component (created on demand)."""

    path = os.path.join(run_artifact_dir(context), "components", spec.id)
    os.makedirs(path, exist_ok=True)
    return path
