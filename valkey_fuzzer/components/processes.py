"""Stopping and pausing instance processes, each owned by an undo obligation until it is undone."""

import signal
import time
from typing import Any, Callable

from .files import nodes_conf_path, read_cluster_vars

# Opens the undo obligation of a change just applied: ``own(kind, target, undo)``.
Own = Callable[[str, str, Callable[[], None]], Any]


def restart_once(handle: Any, instance_id: str, **restart: Any) -> Callable[[], None]:
    """The undo of a stopped instance: restart it once with ``restart``'s arguments.

    A restart also stops a process that has not exited yet; once it succeeded,
    the undo does nothing more.
    """

    restarted = []

    def undo() -> None:
        if not restarted:
            handle.restart(instance_id, **restart)
            restarted.append(instance_id)

    return undo


def resume_if_alive(handle: Any, instance_id: str) -> None:
    """The undo of a paused instance: SIGCONT it, unless its process is gone."""

    if handle.is_alive(instance_id):
        handle.send_signal(instance_id, signal.SIGCONT)


def stop_owned(own: Own, handle: Any, instance_id: str, signum: int = signal.SIGTERM, **restart: Any) -> Any:
    """Send ``instance_id`` ``signum``; the stop is owned (``process.stopped``) until it restarts once with
    ``restart``'s arguments, the returned obligation's discharge."""

    handle.send_signal(instance_id, signum)
    return own("process.stopped", instance_id, restart_once(handle, instance_id, **restart))


# How often a stop is signalled again while the instance has not exited.
RESIGNAL_INTERVAL_S = 1.0


def wait_exit_signalling(handle: Any, instance_id: str, timeout_s: float, signum: int = signal.SIGTERM) -> bool:
    """Wait up to ``timeout_s`` for ``instance_id`` to exit, sending ``signum`` again every second until it does.

    Valkey drops a SIGTERM it cannot act on yet and never acts on it later:
    while it writes its initial AOF ("Writing initial AOF, can't exit", as a
    replica does right after a full sync) or when its shutdown save fails.
    A repeated SIGTERM is harmless once a shutdown is under way.
    """

    deadline = time.monotonic() + timeout_s
    while not handle.wait_exit(instance_id, timeout=max(0.0, min(RESIGNAL_INTERVAL_S, deadline - time.monotonic()))):
        if time.monotonic() >= deadline:
            return False
        try:
            handle.send_signal(instance_id, signum)
        except ProcessLookupError:
            pass  # It exited meanwhile: the next wait reaps it.
    return True


# How long a kill waits for its target to exit.
KILL_EXIT_TIMEOUT_S = 30.0


def wait_killed(handle: Any, instance_id: str, signum: int, *, cluster_vars: bool = False) -> dict[str, Any]:
    """Wait for ``instance_id`` to exit after ``signum`` (:func:`wait_exit_signalling`): ``exited``, and once it
    did, ``exited_at``; with ``cluster_vars``, ``disk_vars_at_kill``.

    The process can acknowledge writes until it is gone, so ``exited_at``,
    not the signal, ends a loss window around the kill. ``disk_vars_at_kill``
    are the vars of its nodes.conf as the restarted process loads them, read
    once the process is gone: a vote it saved right before the kill is on
    disk. None while it still runs, when the file can still change.
    """

    record: dict[str, Any] = {"exited": wait_exit_signalling(handle, instance_id, KILL_EXIT_TIMEOUT_S, signum)}
    if record["exited"]:
        record["exited_at"] = time.time()
    if cluster_vars:
        record["disk_vars_at_kill"] = (
            read_cluster_vars(nodes_conf_path(handle, instance_id)) if record["exited"] else None
        )
    return record


def pause_owned(own: Own, handle: Any, instance_id: str) -> Any:
    """SIGSTOP ``instance_id``; the pause is owned (``process.paused``) until it is resumed, unless it is gone."""

    handle.send_signal(instance_id, signal.SIGSTOP)
    return own("process.paused", instance_id, lambda: resume_if_alive(handle, instance_id))
