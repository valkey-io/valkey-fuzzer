"""The run's event journal: ordered events, subscriptions, and synchronous dispatch.

Components publish report-safe events and subscribe to them; the runner reads
the committed history for required events and the outcome's event trace. The
names of the events the runner itself publishes are defined here too.
"""

import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Optional

from ._data import freeze_mapping
from .fields import require_name, require_number

# Events the runner publishes for every component use: before and after its
# call, and around the teardown of the roles that have one. No component may
# publish them, or declare them.
LIFECYCLE_EVENTS = ("started", "completed", "failed")
TEARDOWN_EVENTS = ("stopping", "stopped", "stop-failed")

# Events the runner publishes for the run as a whole.
RUN_STARTED = "run.started"
RUN_COMPLETED = "run.completed"
DEPLOYMENT_PROVISIONING = "deployment.provisioning"
DEPLOYMENT_READY = "deployment.ready"
VALIDATION_COMPLETED = "validation.completed"
# Around a rollback after a failed provision, and around the cleanup of a provisioned deployment.
DEPLOYMENT_RELEASE_EVENTS = MappingProxyType(
    {
        "rollback": ("deployment.rollback.started", "deployment.rollback.completed"),
        "cleanup": ("deployment.cleanup.started", "deployment.cleanup.completed"),
    }
)
RUNNER_EVENTS = frozenset(
    {
        RUN_STARTED,
        RUN_COMPLETED,
        DEPLOYMENT_PROVISIONING,
        DEPLOYMENT_READY,
        VALIDATION_COMPLETED,
        *(name for events in DEPLOYMENT_RELEASE_EVENTS.values() for name in events),
    }
)


@dataclass(frozen=True)
class RunEvent:
    """One ordered event in the run's event journal."""

    name: str
    sequence: int
    timestamp: float
    source: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_name(self.name, "event name")
        if require_number(self.sequence, "event sequence", integer=True) < 1:
            raise ValueError("event sequence must be a positive integer")
        object.__setattr__(self, "timestamp", float(require_number(self.timestamp, "event timestamp")))
        require_name(self.source, "event source")
        object.__setattr__(self, "payload", freeze_mapping(self.payload, "event payload"))


EventHandler = Callable[[RunEvent], None]


@dataclass(frozen=True)
class EventSubscription:
    """Opaque handle used to remove an event subscription."""

    token: int
    event_name: str


class EventDispatchError(RuntimeError):
    """A subscriber failed while handling an event."""

    def __init__(self, event: RunEvent, subscription: EventSubscription, cause: BaseException):
        self.event = event
        self.subscription = subscription
        self.cause = cause
        super().__init__(f"subscriber {subscription.token} failed while handling {event.name!r}: {cause}")


class EventDispatchAbortedError(RuntimeError):
    """An event was not dispatched because an earlier event's dispatch failed."""

    def __init__(self, event: RunEvent, cause: BaseException):
        self.event = event
        self.cause = cause
        failed = cause.event if isinstance(cause, EventDispatchError) else None
        reason = (
            f"dispatch terminated with {type(cause).__name__}: {cause}"
            if failed is None
            else f"earlier event {failed.name!r} (sequence {failed.sequence}) failed"
        )
        super().__init__(f"event {event.name!r} (sequence {event.sequence}) was not dispatched because {reason}")


class EventBusQuiescedError(RuntimeError):
    """The event journal has been frozen for final verdict calculation."""


@dataclass
class _Pending:
    event: RunEvent
    done: bool = False
    error: Optional[BaseException] = None


class EventBus:
    """The run's event stream: a thread-safe synchronous journal with deterministic FIFO dispatch.

    Components publish report-safe events, subscribe to them, and read the
    events whose subscriber dispatch completed (:meth:`committed_history`).

    ``check(name, source, payload)``, when given, vets every :meth:`publish`
    and raises to reject an event. Subscribers (``"*"`` receives everything)
    run in registration order. A
    publisher on another thread waits until its own event was dispatched; a
    publish from inside a handler is queued and returns at once, so handlers
    cannot deadlock. Events are stamped with wall time (``time.time``), as
    evidence and obligations are, so a report has one time base.

    :meth:`abort` gives up on a dispatch whose subscriber does not return:
    every event not yet dispatched, and every later publish, fails with
    :class:`EventDispatchAbortedError`, and nothing more is committed.
    """

    def __init__(self, check: Optional[Callable[[str, str, Optional[Mapping[str, Any]]], None]] = None):
        self._check = check
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._next_sequence = 1
        self._next_token = 1
        self._subscribers: dict[int, tuple[EventSubscription, EventHandler]] = {}
        self._attempted: list[RunEvent] = []
        self._committed: list[RunEvent] = []
        self._queue: deque[_Pending] = deque()
        self._dispatcher: Optional[int] = None
        self._current: Optional[_Pending] = None
        self._aborted: Optional[BaseException] = None
        self._quiesced = False

    @classmethod
    def owned(
        cls, check: Callable[[str, str, Optional[Mapping[str, Any]]], None]
    ) -> tuple["EventBus", Callable[..., RunEvent]]:
        """A bus whose :meth:`publish` ``check`` vets, and the unchecked publisher its owner (the runner)
        keeps for the events only it may publish, such as each component's lifecycle events."""

        bus = cls(check=check)
        return bus, bus._publish_unchecked

    def subscribe(self, event_name: str, handler: EventHandler) -> EventSubscription:
        if event_name != "*":
            require_name(event_name, "event_name")
        with self._condition:
            if self._quiesced:
                raise EventBusQuiescedError("cannot subscribe after the event bus is quiesced")
            subscription = EventSubscription(self._next_token, event_name)
            self._next_token += 1
            self._subscribers[subscription.token] = (subscription, handler)
            return subscription

    def unsubscribe(self, subscription: EventSubscription) -> bool:
        with self._condition:
            return self._subscribers.pop(subscription.token, None) is not None

    def publish(self, name: str, *, source: str, payload: Optional[Mapping[str, Any]] = None) -> RunEvent:
        """Publish an event; with a ``check``, one it rejects is neither recorded nor dispatched."""

        if self._quiesced:
            raise EventBusQuiescedError("cannot publish after the event bus is quiesced")
        if self._check is not None:
            self._check(name, source, payload)
        return self._publish_unchecked(name, source=source, payload=payload)

    def _publish_unchecked(self, name: str, *, source: str, payload: Optional[Mapping[str, Any]] = None) -> RunEvent:
        """Publish without the ``check``: what :meth:`owned` hands the bus's owner."""

        me = threading.get_ident()
        with self._condition:
            if self._quiesced:
                raise EventBusQuiescedError("cannot publish after the event bus is quiesced")
            event = RunEvent(name, self._next_sequence, time.time(), source, payload or {})
            self._next_sequence += 1
            self._attempted.append(event)
            if self._aborted is not None:
                raise EventDispatchAbortedError(event, self._aborted)
            pending = _Pending(event)
            self._queue.append(pending)
            if self._dispatcher is not None:
                if self._dispatcher == me:
                    return event  # re-entrant: dispatched after the current handler returns
                while not pending.done:
                    self._condition.wait()
                if pending.error is not None:
                    raise pending.error
                return event
            self._dispatcher = me

        current: Optional[_Pending] = None
        try:
            while True:
                with self._condition:
                    if self._aborted is not None:
                        raise EventDispatchAbortedError(event, self._aborted)
                    if not self._queue:
                        self._dispatcher = None
                        self._condition.notify_all()
                        return event
                    current = self._current = self._queue.popleft()
                    subscribers = [
                        entry
                        for entry in self._subscribers.values()
                        if entry[0].event_name in (current.event.name, "*")
                    ]
                for subscription, handler in subscribers:
                    try:
                        handler(current.event)
                    except Exception as exc:
                        raise EventDispatchError(current.event, subscription, exc) from exc
                    if self._aborted is not None:
                        break
                with self._condition:
                    if self._aborted is not None:
                        # This dispatch was given up on while a handler ran.
                        raise EventDispatchAbortedError(current.event, self._aborted)
                    self._committed.append(current.event)
                    current.done = True
                    self._current = None
                    self._condition.notify_all()
                current = None
        except BaseException as exc:
            with self._condition:
                if self._aborted is not None and self._dispatcher != me:
                    # abort() already failed every pending event and released the bus.
                    raise
                self._current = None
                if current is not None:
                    current.error = (
                        exc if isinstance(exc, EventDispatchError) else EventDispatchAbortedError(current.event, exc)
                    )
                    current.done = True
                while self._queue:
                    aborted = self._queue.popleft()
                    aborted.error = EventDispatchAbortedError(aborted.event, exc)
                    aborted.done = True
                self._dispatcher = None
                self._condition.notify_all()
            raise

    def abort(self, cause: BaseException, *, dispatcher: Optional[int] = None) -> bool:
        """Give up on the current dispatch for good; with ``dispatcher``, only if that thread is dispatching.

        The event being dispatched, every queued event, and every later
        publish fail with :class:`EventDispatchAbortedError`; publishers
        waiting on them return at once, and the bus can be quiesced. A
        handler still running commits nothing when it returns. Returns
        whether the bus was aborted.
        """

        with self._condition:
            if dispatcher is not None and self._dispatcher != dispatcher:
                return False
            if self._aborted is None:
                self._aborted = cause
            pending = [self._current, *self._queue] if self._current is not None else list(self._queue)
            self._queue.clear()
            for item in pending:
                if not item.done:
                    item.error = EventDispatchAbortedError(item.event, cause)
                    item.done = True
            self._current = None
            self._dispatcher = None
            self._condition.notify_all()
            return True

    def _history(self, events: list[RunEvent], event_name: Optional[str]) -> tuple[RunEvent, ...]:
        with self._condition:
            return tuple(event for event in events if event_name is None or event.name == event_name)

    def attempted_history(self, event_name: Optional[str] = None) -> tuple[RunEvent, ...]:
        """Every published event, including ones whose dispatch failed."""

        return self._history(self._attempted, event_name)

    def committed_history(self, event_name: Optional[str] = None) -> tuple[RunEvent, ...]:
        """Only events whose complete subscriber dispatch succeeded."""

        return self._history(self._committed, event_name)

    def quiesce(self) -> None:
        """Wait for active dispatch to finish, then reject further activity."""

        with self._condition:
            if self._dispatcher == threading.get_ident():
                raise RuntimeError("cannot quiesce the event bus from its dispatch thread")
            while self._dispatcher is not None:
                self._condition.wait()
            self._quiesced = True
