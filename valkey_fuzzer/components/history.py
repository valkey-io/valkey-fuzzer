"""The ``valkey.oracle.history`` oracle: the key-value workload's history must be linearizable.

The ``valkey.workload.key-value`` ledger is a history: every operation's
invocation and completion, its connection, key, the value written or read,
and its outcome. This oracle hands the mutable keys' history, closed by a
final read of every key, to Porcupine (https://github.com/anishathalye/porcupine,
built from ``porcupine/`` in the repository), which checks that each key
behaves as one register. It catches what a single final read cannot: a read
that returned a value already overwritten (a stale read after a failover or
a restart), or a value that later vanished.

It judges under the loss policy of a ``valkey.oracle.ledger`` component
(``ledger``), so a loss that policy allows is not a violation:

* an ambiguous write (a timeout or a lost connection), and an acknowledged
  write the policy lets be lost, are indeterminate: each may take effect at
  any point after its invocation, or never;
* a read that returned the value of a write the policy lets be lost is not
  checked (the write may be lost after the read), nor, with
  ``window_reads: written``, one that completed inside a loss window;
* a rejected write is left out: it was not applied.
"""

import json
import os
import shutil
import subprocess
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, Evidence, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config

from .files import write_jsonl
from .instances import deployment
from .ledger import _lossy, _read_keys, load_ledger, loss_policy
from .polling import read_retrying
from .registration import component_registration
from .schemas import INTEGER, TEXT, declared, mapping_of
from .workloads import is_stable_key

CHECKER = "porcupine"
# Porcupine caches a bitset of a key's operations per step of its search, so
# checking a key of n operations takes about n * n / 8 bytes (368 MB for 50,000).
_CHECKER_MEMORY_BYTES = 2 << 30


def _may_be_lost(entry: Mapping[str, Any], windows: Sequence[tuple], lossy_key: Any, wait_confirmed: Any) -> bool:
    """Whether the policy lets write ``entry`` (acknowledged or ambiguous) be lost, as :class:`LedgerModel` rules."""

    if entry["outcome"] == "ok":
        if wait_confirmed is not None and entry.get("wait", -1) >= wait_confirmed:
            return False
        return _lossy(entry["key"], entry["t_end"], entry.get("instance_id"), windows, lossy_key)
    # An ambiguous write may be applied any time after it was sent, so it may be lost if a window ends after that.
    in_scope = lossy_key is None or lossy_key(entry["key"])
    return in_scope and any(entry["t_start"] < window[1] for window in windows)


def register_history(
    preload: Mapping[str, str],
    ledger: Sequence[Mapping[str, Any]],
    final: tuple[Mapping[str, Optional[str]], float, float],
    *,
    started_at: float,
    preload_acks: Optional[Mapping[str, tuple]] = None,
    loss_windows: Sequence[tuple] = (),
    lossy_key: Optional[Any] = None,
    window_reads: str = "strict",
    wait_confirmed: Optional[int] = None,
) -> tuple[list[dict[str, Any]], int]:
    """Porcupine's history of the workload's mutable keys, and how many reads the policy leaves unchecked.

    ``final`` is the final read: every key's value, and when it started and
    ended. The preload is acknowledged writes on connection 0, invoked at
    ``started_at``; the final read runs on a connection after every other.
    """

    acks = preload_acks or {}
    writes = [
        {
            "op": "set",
            "key": key,
            "value": value,
            "outcome": "ok",
            "process": 0,
            "t_start": started_at,
            "t_end": acks.get(key, (started_at,))[0],
            "instance_id": acks.get(key, (None, None))[1],
        }
        for key, value in preload.items()
        if not is_stable_key(key)
    ]
    writes += [entry for entry in ledger if entry["op"] != "get" and entry["outcome"] != "error"]
    history = []
    # Per key, the values of writes that may be lost.
    lost: dict[str, set] = {}
    for entry in writes:
        indeterminate = entry["outcome"] != "ok"
        if _may_be_lost(entry, loss_windows, lossy_key, wait_confirmed):
            lost.setdefault(entry["key"], set()).add(entry.get("value"))
            indeterminate = True
        value = entry.get("value") if entry["op"] == "set" else None
        history.append(_operation(entry, "write", value, None if indeterminate else entry["t_end"]))
    unchecked = 0
    for entry in ledger:
        if entry["op"] != "get" or entry["outcome"] != "ok":
            continue
        in_window = (
            window_reads == "written"
            and (lossy_key is None or lossy_key(entry["key"]))
            and any(window[0] < entry["t_end"] <= window[1] for window in loss_windows)
        )
        if in_window or entry.get("reply") in lost.get(entry["key"], ()):
            unchecked += 1
            continue
        history.append(_operation(entry, "read", entry.get("reply"), entry["t_end"]))
    values, read_at, read_end = final
    process = max((entry.get("process", 0) for entry in ledger), default=0) + 1
    for key in sorted(values):
        read = {"key": key, "process": process, "t_start": read_at}
        history.append(_operation(read, "read", values[key], read_end))
    return history, unchecked


def _operation(entry: Mapping[str, Any], f: str, value: Optional[str], returned: Optional[float]) -> dict[str, Any]:
    return {
        "process": entry.get("process", 0),
        "key": entry["key"],
        "f": f,
        "value": value,
        "call": _nanoseconds(entry["t_start"]),
        "return": None if returned is None else _nanoseconds(returned),
    }


def _nanoseconds(at: float) -> int:
    return round(at * 1e9)


def check_linearizable(history_path: str, timeout_s: float, visualization_path: str) -> dict[str, Any]:
    """Porcupine's verdict on the history at ``history_path``: ``{"result", "illegal_keys"}``."""

    checker = shutil.which(CHECKER)
    if checker is None:
        raise FileNotFoundError(
            f"{CHECKER} is not on PATH: build it from a valkey-fuzzer checkout with "
            f"`go -C porcupine build -o <a directory on PATH>`"
        )
    command = [checker, "-timeout", f"{timeout_s}s", "-visualize", visualization_path, history_path]
    # An Illegal result searches the illegal keys once more, for the visualization.
    completed = subprocess.run(command, capture_output=True, text=True, timeout=2 * timeout_s + 60, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"{CHECKER} exited {completed.returncode}: {completed.stderr.strip()[-500:]}")
    return json.loads(completed.stdout)


_HISTORY_FIELDS = {
    # The valkey.oracle.ledger component whose workload and loss policy this oracle uses.
    "ledger": ConfigField(str),
    "timeout_s": ConfigField(float, default=300.0, minimum=1, maximum=3600),
}


def _validate_history(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    ledger = next((component for component in plan.components if component.id == config["ledger"]), None)
    if ledger is None or ledger.use != "valkey.oracle.ledger":
        raise ConfigError(f"ledger {config['ledger']!r} is not a valkey.oracle.ledger component of this plan")
    ledger_config = plain_config(ledger.config)
    # Every read and every acknowledged write the policy does not let be lost is checked.
    if ledger_config.get("lost_writes_budget", 0) or ledger_config.get("check_reads") is False:
        raise ConfigError(
            f"ledger {config['ledger']!r} allows lost writes (lost_writes_budget) or unchecked reads "
            "(check_reads: false), which a history check cannot honor"
        )


class HistoryOracle:
    """Porcupine finds the workload's history linearizable under the ledger oracle's loss policy.

    A checker that is not installed, a final read that keeps failing, a
    history too large to check, or a check that times out makes the oracle
    ERROR, never FAIL: none says anything about the data.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        ledger_config = next(
            component.config for component in context.plan.components if component.id == config["ledger"]
        )
        handle = deployment(context)
        try:
            preload, ledger, preload_acks, summary = load_ledger(evidence, ledger_config["workload"])
        except (LookupError, OSError, ValueError) as exc:
            return make_result(spec, ComponentStatus.ERROR, f"cannot load workload ledger: {exc}")
        windows, lossy_key, _, wait_confirmed = loss_policy(ledger_config, evidence, handle, summary)
        keys = sorted({entry["key"] for entry in ledger} | {key for key in preload if not is_stable_key(key)})
        read_at = time.time()
        values, error = read_retrying(
            context, lambda: _read_keys(handle, None, keys), timeout_s=ledger_config["converge_timeout_s"]
        )
        if error is not None:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"cannot read the dataset within {ledger_config['converge_timeout_s']:g}s: {error}",
            )
        history, unchecked = register_history(
            preload,
            ledger,
            (values, read_at, time.time()),
            started_at=summary.get("started_at", 0.0),
            preload_acks=preload_acks,
            loss_windows=windows,
            lossy_key=lossy_key,
            window_reads=ledger_config["window_reads"],
            wait_confirmed=wait_confirmed,
        )
        memory = sum(count * count for count in Counter(op["key"] for op in history).values()) // 8
        if memory > _CHECKER_MEMORY_BYTES:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"the history ({len(history)} operations on {len(keys)} keys) needs about {memory >> 20} MB to "
                f"check, over the {_CHECKER_MEMORY_BYTES >> 20} MB limit: spread the writes over more mutable_keys",
            )
        directory = component_artifact_dir(context, spec)
        history_path = os.path.join(directory, "history.jsonl")
        visualization_path = os.path.join(directory, "illegal.html")
        write_jsonl(history_path, history)
        try:
            verdict = check_linearizable(history_path, config["timeout_s"], visualization_path)
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
            return make_result(spec, ComponentStatus.ERROR, f"cannot check the history: {exc}")
        record = {
            "operations": len(history),
            "keys": len(keys),
            "indeterminate": sum(op["return"] is None for op in history),
            "unchecked_reads": unchecked,
            "history_path": history_path,
        }
        if verdict["result"] == "Illegal":
            illegal = verdict["illegal_keys"]
            return make_result(
                spec,
                ComponentStatus.FAIL,
                f"the history of {len(illegal)} key(s) is not linearizable: {', '.join(illegal[:5])}",
                diagnostics={**record, "illegal_keys": illegal[:20], "visualization_path": visualization_path},
            )
        if verdict["result"] != "Ok":
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"{CHECKER} did not finish within {config['timeout_s']:g}s",
                diagnostics=record,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"the history of {len(keys)} keys is linearizable ({len(history)} operations)",
            evidence=(make_evidence(spec, "valkey.data.linearizable", "oracle.completed", record),),
        )


_HISTORY_DECLARES = Declarations(
    evidence={
        "valkey.data.linearizable": declared(
            mapping_of(
                {
                    "operations": INTEGER,
                    "keys": INTEGER,
                    "indeterminate": INTEGER,
                    "unchecked_reads": INTEGER,
                    "history_path": TEXT,
                }
            )
        )
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.history", HistoryOracle, _HISTORY_FIELDS, check=_validate_history, declares=_HISTORY_DECLARES
    ),
)
