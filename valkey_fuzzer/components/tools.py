"""``valkey.action.tools``: run Valkey's own tools from two builds and compare what they do.

valkey-cli, valkey-benchmark, valkey-check-rdb, and valkey-check-aof sit beside valkey-server in a build's
``src/``. A change to one of them is tested by running the tool of the build under test and the same tool of
its parent build with the same arguments, stdin, and files, and comparing their exit status, stdout, and
stderr, with the verdicts of ``valkey.action.commands``.
"""

import base64
import hashlib
import os
import re
import subprocess
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from valkey_fuzzer._data import canonical_json
from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.protocols import component_artifact_dir
from valkey_fuzzer.results import ComponentStatus, make_evidence, make_result
from valkey_fuzzer.schema import BASE64_KEY, ConfigError, ConfigField, plain_config

from .actions import _PRIVATE_SERVER_DEFAULTS, _free_ports, _is_binary, _port_block, _text
from .actions import _Side as _ServerSide
from .capabilities import resolve_binary
from .oracles import CRASH_PATTERNS, crash_cause
from .predicates import PREDICATE_FIELDS, compare, validate_predicate
from .registration import component_registration
from .schemas import FLAG, INTEGER, TEXT, declared, mapping_of, optional

TOOLS = ("valkey-cli", "valkey-benchmark", "valkey-check-rdb", "valkey-check-aof")
_COMPARE_MODES = ("equal", "unordered", "skip")
# What an argument may name of its side's private server. Each side's own value replaces the name in its
# arguments, and the name replaces the value in its output, so the two sides' outputs compare equal.
_PLACEHOLDERS = ("$port", "$socket")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Variables valkey-cli reads (VALKEYCLI_AUTH, REDISCLI_HISTFILE, ...): only a run's own env sets them.
_CLI_ENV_PREFIXES = ("VALKEYCLI_", "REDISCLI_")
_CRASH_LINES = re.compile("|".join(CRASH_PATTERNS))
# A run whose resident memory passes this is stopped, so a tool that runs away (valkey-cli --hotkeys-count -1
# asked for 64 GB of counters before valkey-io/valkey#5173) fails its run instead of taking the host down. An
# address-space limit would not do: an ASan build reserves terabytes of it.
_MAX_RESIDENT_BYTES = 4 << 30
_POLL_S = 0.05


def _resident_bytes(pid: int) -> Optional[int]:
    """The resident memory of process ``pid``, from ``/proc`` (Linux); None where it cannot be read."""

    try:
        with open(f"/proc/{pid}/statm") as handle:
            return int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


_PREDICATE = mapping_of(PREDICATE_FIELDS, required=False)
_RUN = mapping_of(
    {
        "tool": ConfigField(str, choices=TOOLS),
        "args": ConfigField(list, default=list),
        "stdin": ConfigField((str, dict), allow_none=True, default=None),
        "env": ConfigField(dict, default=dict),
        "expect": mapping_of(
            {"exit": _PREDICATE, "stdout": _PREDICATE, "stderr": _PREDICATE}, allow_none=True, default=None
        ),
        # The change under test means this run to differ from the parent's.
        "expect_difference": ConfigField(bool, default=False),
        "compare": ConfigField(str, default="equal", choices=_COMPARE_MODES),
    }
)
_TOOLS_FIELDS = {
    # The parent build's valkey-server, usually {$ref: artifacts.parent.binary}; its tools sit beside it.
    "compare_with": ConfigField(str, reference=True),
    # Each side starts a private server of its own build for its tools to talk to.
    "server": ConfigField(bool, default=False),
    "cluster": ConfigField(bool, default=False),
    # {name: text or !!binary}, written into each side's working directory before the first run.
    "files": ConfigField(dict, default=dict),
    # Regular expressions whose matches are removed from stdout and stderr before they compare.
    "ignore": ConfigField(list, default=list, items=TEXT),
    "timeout_s": ConfigField(float, default=30.0, minimum=0.1, maximum=3600),
    "runs": ConfigField(list, items=_RUN),
}


def _outside(text: str) -> bool:
    """Whether ``text``, as a path, could name something outside the working directory."""

    return text.startswith(("/", "~")) or ".." in text.split("/")


def _validate_tools(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if config["cluster"] and not config["server"]:
        raise ConfigError("cluster needs server")
    for name, content in config["files"].items():
        if not name or name in (".", "..") or "/" in name or "\0" in name:
            raise ConfigError(f"files: {name!r} must be a plain file name in the working directory")
        if not (isinstance(content, str) or _is_binary(content)):
            raise ConfigError(f"files.{name} must be text or a !!binary value")
    for index, pattern in enumerate(config["ignore"]):
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ConfigError(f"ignore[{index}] is not a regular expression: {exc}") from exc
    if not config["runs"]:
        raise ConfigError("runs must list at least one run")
    for index, run in enumerate(config["runs"]):
        where = f"runs[{index}]"
        for item in run["args"]:
            if isinstance(item, bool) or not isinstance(item, (str, int, float)):
                raise ConfigError(f"{where}.args must be a list of strings or numbers")
            if isinstance(item, str) and _outside(item):
                raise ConfigError(f"{where}.args: {item!r} names a path outside the working directory")
            if not config["server"] and any(name in str(item) for name in _PLACEHOLDERS):
                raise ConfigError(f"{where}.args: {item!r} names the private server, which needs server")
        if run["stdin"] is not None and not (isinstance(run["stdin"], str) or _is_binary(run["stdin"])):
            raise ConfigError(f"{where}.stdin must be text or a !!binary value")
        for name, value in run["env"].items():
            if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
                raise ConfigError(f"{where}.env: {name!r} is not an environment variable name")
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise ConfigError(f"{where}.env.{name} must be a string or a number")
        if run["expect"] is not None:
            if not run["expect"]:
                raise ConfigError(f"{where}.expect needs exit, stdout, or stderr")
            for stream, rule in run["expect"].items():
                validate_predicate(rule, f"{where}.expect.{stream}")
        if run["expect_difference"] and run["expect"] is None:
            raise ConfigError(f"{where}: a run marked expect_difference needs an expect")


def _content(value: Any) -> bytes:
    """A file's or stdin's bytes: a ``!!binary`` value decoded, text as UTF-8."""

    return base64.b64decode(value[BASE64_KEY]) if _is_binary(value) else str(value).encode("utf-8")


class _Side:
    """One build's side: its tools, its working directory, its private server, and its runs."""

    def __init__(self, label: str, binary: str, workdir: str):
        self.label, self.binary, self.workdir = label, binary, workdir
        # Its private server: a side of valkey.action.commands with one node.
        self.server: Optional[_ServerSide] = None
        self.port: Optional[int] = None
        self.runs: list[dict[str, Any]] = []
        # Per run: (stdout, stderr) as bytes with the placeholders written back, for comparing.
        self.output: list[tuple[bytes, bytes]] = []
        self.crashes: dict[int, str] = {}

    def tool(self, name: str) -> str:
        return os.path.join(os.path.dirname(self.binary), name)

    def values(self) -> dict[str, str]:
        """``{placeholder: value}`` for this side's private server."""

        if self.server is None:
            return {}
        return {"$port": str(self.port), "$socket": self.server.nodes[0].server.socket_path}

    def run(self, index: int, run: Mapping[str, Any], timeout_s: float) -> None:
        values = self.values()
        arguments = [str(item) for item in run["args"]]
        for name, value in values.items():
            arguments = [item.replace(name, value) for item in arguments]
        env = {name: value for name, value in os.environ.items() if not name.startswith(_CLI_ENV_PREFIXES)}
        env.update({"HOME": self.workdir, **{name: str(value) for name, value in run["env"].items()}})
        record: dict[str, Any] = {"index": index, "tool": run["tool"]}
        # argv[0] is the bare tool name, so a usage line reads the same on both sides.
        process = subprocess.Popen(
            [run["tool"], *arguments],
            executable=self.tool(run["tool"]),
            cwd=self.workdir,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        data: Optional[bytes] = b"" if run["stdin"] is None else _content(run["stdin"])
        deadline, stopped = time.monotonic() + timeout_s, None
        while True:
            try:
                stdout, stderr = process.communicate(data, timeout=_POLL_S)
                break
            except subprocess.TimeoutExpired:
                # The input is sent once; communicating again keeps the output read so far.
                data = None
            if (_resident_bytes(process.pid) or 0) > _MAX_RESIDENT_BYTES:
                stopped = "memory_exceeded"
            elif time.monotonic() >= deadline:
                stopped = "timed_out"
            else:
                continue
            process.kill()
            stdout, stderr = process.communicate()
            break
        if stopped:
            record.update({"exit": None, stopped: True})
        else:
            record["exit"] = process.returncode if process.returncode >= 0 else None
            if process.returncode < 0:
                record["signal"] = -process.returncode
        # Longest first, so a value inside another (the port's digits in a path) is not replaced inside it.
        written_back = sorted({**values, "$dir": self.workdir}.items(), key=lambda item: -len(item[1]))
        nodes = [] if self.server is None else self.server.nodes
        streams = []
        for data in (stdout, stderr):
            for name, value in written_back:
                pattern = rf"\b{re.escape(value)}\b" if name == "$port" else re.escape(value)
                data = re.sub(pattern.encode(), name.encode(), data)
            # Each node ID as valkey.action.commands names it: in full, or as valkey-cli abbreviates it.
            for index, node in enumerate(nodes):
                if node.id is not None:
                    pattern = rf"\b(?:{re.escape(node.id)}\b|{re.escape(node.id[:8])}\.\.\.)"
                    data = re.sub(pattern.encode(), f"<node.{index}.id>".encode(), data)
            streams.append(data)
        record["stdout"], record["stderr"] = _text(streams[0]), _text(streams[1])
        self.runs.append(record)
        self.output.append((streams[0], streams[1]))
        lines = [line for data in streams for line in data.decode("utf-8", "replace").splitlines()]
        reports = [line for line in lines if _CRASH_LINES.search(line)]
        if reports:
            self.crashes[index] = crash_cause(reports)
        elif record.get("memory_exceeded"):
            self.crashes[index] = f"used more than {_MAX_RESIDENT_BYTES >> 30} GiB of memory"
        elif "signal" in record:
            self.crashes[index] = f"killed by signal {record['signal']}"


def _compared(side: _Side, index: int, mode: str, ignore: Sequence["re.Pattern[bytes]"]) -> Any:
    """What run ``index`` of ``side`` compares as: its exit status, and its stdout and stderr without what
    ``ignore`` matches, their lines sorted with ``unordered``."""

    record = side.runs[index]
    compared: dict[str, Any] = {
        key: record[key] for key in ("exit", "signal", "timed_out", "memory_exceeded") if key in record
    }
    for name, data in zip(("stdout", "stderr"), side.output[index]):
        for pattern in ignore:
            data = pattern.sub(b"", data)
        if mode == "unordered":
            data = b"\n".join(sorted(data.splitlines()))
        compared[name] = _text(data)
    return compared


def _matches(expect: Optional[Mapping[str, Any]], record: Mapping[str, Any]) -> bool:
    if expect is None:
        return True
    return all(compare(record.get(stream), rule) for stream, rule in plain_config(expect).items())


class ToolsAction:
    """Run each of ``runs`` with the tools of the deployment's build (the candidate) and of ``compare_with``
    (the parent), and compare what they do.

    Each side runs in its own working directory, in the component's folder, holding ``files``; with
    ``server`` each side's tools talk to a private server of their own build, started from an empty dataset
    with that directory as its ``dir``. Only the candidate is judged, as ``valkey.action.commands`` judges it:
    a candidate tool or server that crashes, or a run that violates its ``expect`` where the parent ran
    otherwise, is FAIL; a candidate run that does not exit within ``timeout_s`` is ERROR; and a difference
    between the builds is ERROR ``unreached-change`` or ``unexplained-difference``, never a FAIL by itself.
    """

    def run(self, context: Any, spec: ComponentSpec):
        config = spec.config
        runs = config["runs"]
        ignore = [re.compile(pattern.encode(), re.MULTILINE) for pattern in config["ignore"]]
        try:
            binaries = {
                "candidate": resolve_binary(plain_config(context.plan.deployment).get("valkey_binary") or ""),
                "parent": resolve_binary(config["compare_with"]),
            }
        except OSError as exc:
            return make_result(spec, ComponentStatus.ERROR, str(exc))
        directory = os.path.realpath(component_artifact_dir(context, spec))
        sides = [_Side(label, binary, os.path.join(directory, label)) for label, binary in binaries.items()]
        candidate, parent = sides
        for side in sides:
            for tool in sorted({run["tool"] for run in runs}):
                if not os.access(side.tool(tool), os.X_OK):
                    return make_result(
                        spec,
                        ComponentStatus.ERROR,
                        f"the {side.label} build has no {tool} beside its valkey-server: {side.tool(tool)}",
                    )
            os.makedirs(side.workdir, exist_ok=True)
            for name, content in config["files"].items():
                with open(os.path.join(side.workdir, name), "wb") as handle:
                    handle.write(_content(content))

        try:
            if config["server"]:
                ports = iter(_free_ports(4 if config["cluster"] else 2, _port_block(context)))
                for side in sides:
                    side.port = next(ports)
                    side.server = _ServerSide(side.label, side.binary, 1, 2, config["timeout_s"])
                    # The loopback addresses, ::1 only where the host has it.
                    options = {"port": side.port, "bind": "127.0.0.1 -::1", "dir": side.workdir}
                    if config["cluster"]:
                        options["cluster-port"] = next(ports)
                        side.server.options["cluster-enabled"] = "yes"
                    side.server.nodes[0].options = options
                    failed = side.server.start(context, spec, _PRIVATE_SERVER_DEFAULTS, {})
                    if not failed and config["cluster"]:
                        failed = side.server.form_cluster(context, config["timeout_s"]) or ""
                    if failed:
                        return make_result(spec, ComponentStatus.ERROR, failed)
            for side in sides:
                for index, run in enumerate(runs):
                    if context.cancelled:
                        break
                    side.run(index, run, config["timeout_s"])
        finally:
            for side in sides:
                if side.server is not None:
                    side.server.finish(context, spec, ())

        reached, unexplained, unreached, disputed, violations = [], [], [], [], []
        for index, run in enumerate(runs[: min(len(candidate.runs), len(parent.runs))]):
            # A skipped run still tells a disputed expect by its output as it is.
            mode = "equal" if run["compare"] == "skip" else run["compare"]
            same = canonical_json(_compared(candidate, index, mode, ignore)) == canonical_json(
                _compared(parent, index, mode, ignore)
            )
            if not candidate.runs[index].get("timed_out") and not _matches(run["expect"], candidate.runs[index]):
                (disputed if same else violations).append(index)
            if run["compare"] == "skip":
                continue
            if same and run["expect_difference"]:
                unreached.append(index)
            elif not same:
                (reached if run["expect_difference"] else unexplained).append(index)

        def side_record(side: _Side) -> dict[str, Any]:
            compared = [
                _compared(side, index, runs[index]["compare"], ignore)
                for index in range(len(side.runs))
                if runs[index]["compare"] != "skip"
            ]
            record = {"runs": side.runs, "digest": hashlib.sha256(canonical_json(compared).encode()).hexdigest()}
            if side.server is not None and side.server.exited is not None:
                record["server_exited"] = side.server.exited
            return record

        logs = [path for side in sides if side.server is not None for path in side.server.logs]
        record = {
            "candidate": side_record(candidate),
            "parent": side_record(parent),
            "server": config["server"],
            "reached": reached,
            "unexplained": unexplained,
            "violating": sorted(disputed + violations),
        }
        evidence = (make_evidence(spec, "valkey.tool.runs", "action.completed", record, artifacts=logs),)

        def named(index: int) -> str:
            return f"{index} ({' '.join([runs[index]['tool'], *map(str, runs[index]['args'])])})"

        def both(index: int, **extra: Any) -> dict[str, Any]:
            return {
                "index": index,
                "run": named(index),
                **extra,
                "candidate": _without_index(candidate.runs[index]),
                "parent": _without_index(parent.runs[index]),
            }

        crashes = {f"run {index}": cause for index, cause in candidate.crashes.items()}
        if candidate.server is not None and candidate.server.exited is not None:
            crashes["server"] = candidate.server.crash_cause()
        if crashes:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "the candidate crashed: " + "; ".join(f"{where}: {cause}" for where, cause in crashes.items()),
                evidence=evidence,
                diagnostics={"crashes": crashes},
                crash=True,
            )
        if violations:
            return make_result(
                spec,
                ComponentStatus.FAIL,
                "; ".join(
                    f"run {named(index)} gave {canonical_json(_without_index(candidate.runs[index]))}, not "
                    f"{canonical_json(plain_config(runs[index]['expect']))}"
                    for index in violations
                ),
                evidence=evidence,
            )
        timed_out = [index for index, item in enumerate(candidate.runs) if item.get("timed_out")]
        if timed_out or len(candidate.runs) < len(runs):
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "; ".join(f"run {named(index)} did not exit within {config['timeout_s']:g}s" for index in timed_out)
                or "cancelled before every run ran",
                evidence=evidence,
            )
        if disputed:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "expectations both builds violate alike, so they are not about the change: "
                + ", ".join(map(named, disputed)),
                evidence=evidence,
                diagnostics={
                    "reason": "expect-disputed",
                    "runs": [both(index, expect=plain_config(runs[index]["expect"])) for index in disputed],
                },
            )
        if unexplained:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "unmarked runs differ on the two builds: " + ", ".join(map(named, unexplained)),
                evidence=evidence,
                diagnostics={"reason": "unexplained-difference", "runs": [both(index) for index in unexplained]},
            )
        if unreached:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "marked runs are the same on both builds, so they did not reach the change: "
                + ", ".join(map(named, unreached)),
                evidence=evidence,
                diagnostics={"reason": "unreached-change"},
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(runs)} runs on both builds, {len(reached)} marked run(s) reached the change",
            evidence=evidence,
        )


def _without_index(record: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "index"}


# One run of one side: its exit status (null when a signal ended it, or it timed out), and its output with
# the side's port, socket, and working directory written back as $port, $socket, and $dir.
_TOOL_RUN = mapping_of(
    {
        "index": INTEGER,
        "tool": TEXT,
        "exit": ConfigField(int, allow_none=True),
        "signal": optional(int),
        "timed_out": optional(bool),
        "memory_exceeded": optional(bool),
        "stdout": ConfigField((str, dict)),
        "stderr": ConfigField((str, dict)),
    }
)
_TOOL_SIDE = mapping_of({"runs": ConfigField(list, items=_TOOL_RUN), "digest": TEXT, "server_exited": optional(int)})
_TOOLS_DECLARES = Declarations(
    evidence={
        "valkey.tool.runs": declared(
            mapping_of(
                {
                    "candidate": _TOOL_SIDE,
                    "parent": _TOOL_SIDE,
                    "server": FLAG,
                    # Marked runs that differ, and unmarked ones that differ, by index.
                    "reached": ConfigField(list, items=INTEGER),
                    "unexplained": ConfigField(list, items=INTEGER),
                    # The runs whose record on the build under test violates their expect.
                    "violating": ConfigField(list, items=INTEGER),
                }
            )
        )
    },
    changes=("process.private-server",),
)

REGISTRATIONS = (
    component_registration(
        "valkey.action.tools", ToolsAction, _TOOLS_FIELDS, check=_validate_tools, declares=_TOOLS_DECLARES
    ),
)
