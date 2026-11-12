"""Waiting within the run's deadline: polls, retried reads, and worker threads that are always joined."""

import threading
import time
from collections.abc import Sequence
from typing import Any, Callable, Optional


def remaining(context: Any, fallback: float) -> float:
    """Seconds left before the cooperative deadline, bounded by ``fallback``."""

    deadline = context.deadline
    if deadline is None:
        return fallback
    return max(0.0, min(fallback, deadline - time.monotonic()))


def poll_until(
    check: Callable[[], tuple[bool, Any]],
    *,
    timeout_s: float,
    interval_s: float = 0.1,
    context: Any,
) -> tuple[bool, Any]:
    """Call ``check() -> (done, value)`` until done or ``timeout_s`` (bounded by the run deadline).

    Returns the last ``(done, value)``; ``check`` always runs at least once.
    It waits with ``context.sleep``, so a cancelled call returns at once.
    """

    deadline = time.monotonic() + remaining(context, timeout_s)
    while True:
        done, value = check()
        if done or time.monotonic() >= deadline or not context.sleep(interval_s):
            return done, value


def read_retrying(
    context: Any, read: Callable[[], Any], *, timeout_s: float, interval_s: float = 0.2
) -> tuple[Any, Optional[str]]:
    """``read()`` until it returns, within ``timeout_s`` (bounded by the run deadline): ``(value, None)``, or
    ``(None, error)`` with the last error when it never did.

    A client error (a timeout, LOADING, CLUSTERDOWN while the cluster converges, a host out of local ports)
    says the harness could not read, not what the server holds: an oracle retries it, and returns ERROR if it
    persists. ``valkey.oracle.liveness`` judges a server that is down or does not answer.
    """

    def attempt() -> tuple[bool, tuple[Any, Optional[str]]]:
        try:
            return True, (read(), None)
        except Exception as exc:
            return False, (None, f"{type(exc).__name__}: {exc}")

    return poll_until(attempt, timeout_s=timeout_s, interval_s=interval_s, context=context)[1]


def poll_each(
    context: Any,
    ids: Sequence[str],
    check: Callable[[str], tuple[bool, Any]],
    *,
    timeout_s: float,
    interval_s: float = 0.1,
) -> dict[str, tuple[bool, Any]]:
    """:func:`poll_until` ``check(id)`` for each of ``ids`` in turn, all within one ``timeout_s``: ``{id: (done, value)}``.

    The budget is shared, so many instances that each fail slowly cannot
    stretch a check past ``timeout_s`` (or the run deadline); each id is still
    checked at least once.
    """

    deadline = time.monotonic() + remaining(context, timeout_s)
    return {
        item: poll_until(
            lambda item=item: check(item),
            timeout_s=max(0.0, deadline - time.monotonic()),
            interval_s=interval_s,
            context=context,
        )
        for item in ids
    }


class StoppableThread:
    """A daemon worker that is always joined by its owner."""

    def __init__(self, name: str, target: Callable[[threading.Event], None]):
        self.stop_event = threading.Event()
        self.error: Optional[BaseException] = None

        def run() -> None:
            try:
                target(self.stop_event)
            except BaseException as exc:
                self.error = exc

        self._thread = threading.Thread(target=run, name=name, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout: float = 30.0) -> Optional[str]:
        """Stop the worker and join it: None when it ended cleanly, else why not (still running, or crashed).

        A worker still running owns its state; the caller must not touch it.
        """

        self.stop_event.set()
        self._thread.join(timeout)
        if self._thread.is_alive():
            return f"thread {self._thread.name} did not stop within {timeout:g}s"
        if self.error is not None:
            return f"thread {self._thread.name} crashed: {type(self.error).__name__}: {self.error}"
        return None
