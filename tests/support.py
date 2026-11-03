"""Builders and fakes that the framework tests share."""

import time
from types import SimpleNamespace
from typing import Any

from valkey_fuzzer.plan import DeploymentProviderIdentity

FAKE_DEPLOYMENT_PROVIDER = "tests.deployment.fake"


def deployment_identity() -> DeploymentProviderIdentity:
    """The identity of the provider :func:`deployment_registrations` registers."""

    return DeploymentProviderIdentity(FAKE_DEPLOYMENT_PROVIDER)


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
