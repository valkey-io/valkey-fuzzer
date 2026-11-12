"""A batch worker whose runs are canned: ``python tests/batch_worker_stub.py TASK``.

It is the real worker (:func:`valkey_fuzzer.batch.run_worker`) with the plan's execution replaced.
``BATCH_STUB_PLAN`` (a JSON file) maps a scenario's seed, or the build directory its ``valkey_binary`` is in,
to what the run does: ``pass``, ``fail:<component>``, ``error``, ``skip``, ``sleep:<seconds>``, ``hang``,
``crash``, ``leak-server``, ``leak-private-server``, or ``leak-dir``. Every run appends what it saw (PID, ports, directories, times)
to ``BATCH_STUB_LOG``. :class:`InProcessWorkers` runs the same worker without a process.
"""

import contextlib
import itertools
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from valkey_fuzzer import batch, hoststate
from valkey_fuzzer.hoststate import TITLE_TAG, Owner, current_owner, tag_directory
from valkey_fuzzer.results import ComponentResult, ComponentStatus, OutcomeStatus, RunOutcome
from valkey_fuzzer.service import RunService


class _Exited(BaseException):
    """An in-process worker exits with ``code`` (None while it hangs)."""

    def __init__(self, code):
        self.code = code


def _exit(code):
    """How the worker hangs (``None``) or crashes; :class:`InProcessWorkers` raises instead."""

    if code is None:
        time.sleep(3600)
    os._exit(code)


def run_plan(self, plan, *, timeout=None, run_dir=None):
    behaviors = json.loads(Path(os.environ["BATCH_STUB_PLAN"]).read_text()) if "BATCH_STUB_PLAN" in os.environ else {}
    binary = str(plan.deployment.get("valkey_binary") or "")
    build = os.path.basename(os.path.dirname(os.path.dirname(binary))) if binary else ""
    behavior = behaviors.get(build) or behaviors.get(str(plan.seed)) or "pass"
    record = {"pid": os.getpid(), "seed": plan.seed, "base_port": plan.deployment["base_port"], "timeout": timeout,
              "data_dir": plan.deployment["base_data_dir"], "tmpdir": os.environ.get("TMPDIR"), "start": time.time()}  # fmt: skip
    if behavior.startswith("sleep:"):
        time.sleep(float(behavior.split(":", 1)[1]))
    elif behavior in ("hang", "crash"):
        _exit(None if behavior == "hang" else 9)
    elif behavior == "leak-server":
        # A "valkey-server" carrying this worker's tag, which nothing stops; bash starts it and exits, so it is
        # no child of this process.
        command = [
            "bash",
            "-c",
            'exec -a "$0" /bin/sleep 300 </dev/null >/dev/null 2>&1 & echo $!',
            f"valkey-server {TITLE_TAG}{current_owner().token}",
        ]
        record["leaked_pid"] = int(
            subprocess.run(command, capture_output=True, text=True, check=True, start_new_session=True).stdout
        )
        _SERVERS.append(record["leaked_pid"])
    elif behavior == "leak-private-server":
        # A component's private server directory (made under /tmp, whatever TMPDIR is), never stopped.
        from valkey_fuzzer.components.server import PrivateServer

        record["leaked_dir"] = PrivateServer("/bin/true").workdir
    elif behavior == "leak-dir":
        record["leaked_dir"] = os.path.join(plan.deployment["base_data_dir"], "deployment-leaked")
        os.makedirs(record["leaked_dir"])
        tag_directory(record["leaked_dir"], "deployment")
    record["end"] = time.time()
    if "BATCH_STUB_LOG" in os.environ:
        with open(os.environ["BATCH_STUB_LOG"], "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
    status, results, diagnostics = OutcomeStatus.PASS, (), {}
    if behavior.startswith("fail:"):
        status = OutcomeStatus.FAIL
        component = behavior.split(":")[1]
        results = (
            ComponentResult(component_id=component, status=ComponentStatus.FAIL, summary=f"lost {plan.seed} keys"),
        )
    elif behavior == "error":
        status = OutcomeStatus.ERROR
        diagnostics = {"error": {"type": "TimeoutError", "message": "boom", "phase": "workload"}}
    elif behavior == "skip":
        status = OutcomeStatus.SKIP_UNSUPPORTED
    return RunOutcome(
        plan=plan,
        status=status,
        summary=status.value,
        component_results=results,
        diagnostics=diagnostics,
        started_at=record["start"],
        ended_at=time.time(),
    )


# The PIDs of the servers canned runs leaked; :class:`InProcessWorkers` lists processes only once there is one.
_SERVERS = []

# Above any PID either platform hands out, so every worker is a dead owner; the start key is this test
# process's, so two test processes never share an owner.
_PIDS = itertools.count(5_000_000)


class InProcessWorkers:
    """Batch workers that run their job at once in this process when the batch starts them.

    Output goes to the job's console and ``TMPDIR`` is set as the batch sets it; a ``hang`` keeps running
    until the batch signals it, and a ``crash`` exits 9 without a report. Each worker has its own PID and
    host-state owner, of no live process, so the batch's sweep finds what its run left behind.
    """

    def __init__(self, monkeypatch):
        self._running = {}
        # Every (pid, signal) the batch sent.
        self.signals = []
        monkeypatch.setattr(RunService, "run_plan", run_plan)
        monkeypatch.setattr(sys.modules[__name__], "_exit", self._exit)
        monkeypatch.setattr(
            batch,
            "subprocess",
            type("Subprocess", (), {"Popen": self._popen, "STDOUT": subprocess.STDOUT, "DEVNULL": subprocess.DEVNULL}),
        )
        monkeypatch.setattr(batch, "owner_of", self.owner)
        monkeypatch.setattr(batch, "_signal_worker", self._signal)
        monkeypatch.setattr(batch, "POLL_S", 0.01)
        # Until a run leaks a server, no process carries an owner of these workers, so the sweep's process listing
        # (a ps call each) can find none.
        _SERVERS.clear()
        processes = hoststate.own_processes
        monkeypatch.setattr(hoststate, "own_processes", lambda: processes() if _SERVERS else [])

    @staticmethod
    def owner(pid):
        return Owner(pid, f"{os.getpid() % 0xFFFFFF:06x}")

    @staticmethod
    def _exit(code):
        raise _Exited(code)

    def _popen(self, command, *, stdout, env, **_):
        worker = type("Worker", (), {"pid": next(_PIDS), "returncode": None, "poll": lambda self: self.returncode})()
        own, saved = os.getpid(), (hoststate._CURRENT.get(os.getpid()), os.environ.get("TMPDIR"))
        hoststate._CURRENT[own], os.environ["TMPDIR"] = self.owner(worker.pid), env["TMPDIR"]
        try:
            with contextlib.redirect_stdout(stdout):
                worker.returncode = batch.run_worker(command[-1])
        except _Exited as exited:
            worker.returncode = exited.code
        finally:
            for mapping, key, value in ((hoststate._CURRENT, own, saved[0]), (os.environ, "TMPDIR", saved[1])):
                if value is None:
                    mapping.pop(key, None)
                else:
                    mapping[key] = value
        self._running[worker.pid] = worker
        return worker

    def _signal(self, pid, sig):
        self.signals.append((pid, sig))
        worker = self._running.get(pid)
        if worker is not None and worker.returncode is None:
            worker.returncode = -signal.Signals(sig)


if __name__ == "__main__":
    RunService.run_plan = run_plan
    sys.exit(batch.main())
