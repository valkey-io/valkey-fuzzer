"""TLA+ values, module and config scanning, and running SANY and TLC.

Values printed by TLC (states, ``PrintT`` output, config constants) parse to
plain Python: integers, booleans, strings (:class:`str`), model values
(:class:`ModelValue`), sets (:class:`TlaSet`), sequences and tuples
(:class:`tuple`), records (:class:`dict`), and functions
(:class:`TlaFunction`). :func:`render` writes the same values back as TLA+.
"""

import hashlib
import os
import re
import shutil
import signal
import subprocess
import tempfile
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Optional

# TLC's exit codes (tlc2.output.EC.ExitStatus).
TLC_OK = 0
TLC_VIOLATION_SAFETY = 12
TLC_VIOLATION_LIVENESS = 13
TLC_VIOLATION_DEADLOCK = 11


class TlaError(RuntimeError):
    """A TLA+ tool could not run, or its output could not be read."""


class ModelValue(str):
    """A TLC model value or other bare identifier, rendered without quotes."""

    def __repr__(self) -> str:
        return f"ModelValue({str(self)!r})"


class TlaSet(list):
    """A TLA+ set: a list without duplicates whose order does not matter."""

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, list):
            return False
        return sorted(map(render, self)) == sorted(map(render, other))

    def __ne__(self, other: Any) -> bool:
        return not self == other

    __hash__ = None  # type: ignore[assignment]


class TlaFunction(dict):
    """A TLA+ function written ``(a :> 1 @@ b :> 2)``."""


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------

_TOKEN = re.compile(
    r"""\s*(?:
        (?P<string>"(?:[^"\\]|\\.)*")
      | (?P<number>-?\d+)
      | (?P<symbol><<|>>|\|->|:>|@@|[{}\[\](),])
      | (?P<name>[A-Za-z_][A-Za-z0-9_]*)
    )""",
    re.VERBOSE,
)


def _tokens(text: str) -> list[tuple[str, str]]:
    tokens, position = [], 0
    text = text.rstrip()
    while position < len(text):
        match = _TOKEN.match(text, position)
        if match is None or match.end() == position:
            raise TlaError(f"cannot read the TLA+ value at {text[position : position + 40]!r}")
        position = match.end()
        kind = match.lastgroup
        tokens.append((kind, match.group(kind)))
    return tokens


class _Parser:
    def __init__(self, text: str):
        self.tokens = _tokens(text)
        self.index = 0

    def peek(self) -> Optional[str]:
        return self.tokens[self.index][1] if self.index < len(self.tokens) else None

    def take(self, expected: Optional[str] = None) -> tuple[str, str]:
        if self.index >= len(self.tokens):
            raise TlaError("unexpected end of a TLA+ value")
        token = self.tokens[self.index]
        if expected is not None and token[1] != expected:
            raise TlaError(f"expected {expected!r} in a TLA+ value, found {token[1]!r}")
        self.index += 1
        return token

    def items(self, close: str) -> list[Any]:
        values = []
        while self.peek() != close:
            values.append(self.value())
            if self.peek() == ",":
                self.take(",")
        self.take(close)
        return values

    def value(self) -> Any:
        kind, text = self.take()
        if kind == "string":
            return text[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        if kind == "number":
            return int(text)
        if kind == "name":
            return {"TRUE": True, "FALSE": False}.get(text, ModelValue(text))
        if text == "{":
            values = TlaSet()
            for item in self.items("}"):
                if item not in values:
                    values.append(item)
            return values
        if text == "<<":
            return tuple(self.items(">>"))
        if text == "[":
            record = {}
            while self.peek() != "]":
                key = self.take()[1]
                self.take("|->")
                record[key] = self.value()
                if self.peek() == ",":
                    self.take(",")
            self.take("]")
            return record
        if text == "(":
            first = self.value()
            if self.peek() != ":>":
                self.take(")")
                return first
            function = TlaFunction()
            key = first
            while True:
                self.take(":>")
                function[_key(key)] = self.value()
                if self.peek() != "@@":
                    break
                self.take("@@")
                key = self.value()
            self.take(")")
            return function
        raise TlaError(f"unexpected {text!r} in a TLA+ value")


def _key(value: Any) -> Any:
    if isinstance(value, (TlaSet, dict, list)):
        raise TlaError(f"unsupported function argument {value!r}")
    return value


def parse_value(text: str) -> Any:
    """Parse one TLA+ value as TLC prints it."""

    parser = _Parser(text)
    value = parser.value()
    if parser.peek() is not None:
        raise TlaError(f"trailing text after a TLA+ value: {text!r}")
    return value


def render(value: Any) -> str:
    """``value`` as TLA+ text; sets and functions are written in a canonical order."""

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, ModelValue):
        return str(value)
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(value, TlaSet):
        return "{" + ", ".join(sorted(render(item) for item in value)) + "}"
    if isinstance(value, TlaFunction):
        if not value:
            return "<<>>"
        return "(" + " @@ ".join(f"{render(key)} :> {render(item)}" for key, item in sorted_items(value)) + ")"
    if isinstance(value, dict):
        return "[" + ", ".join(f"{key} |-> {render(item)}" for key, item in sorted(value.items())) + "]"
    if isinstance(value, tuple):
        return "<<" + ", ".join(render(item) for item in value) + ">>"
    raise TypeError(f"cannot write {value!r} as TLA+")


def sorted_items(function: Mapping[Any, Any]) -> list[tuple[Any, Any]]:
    return sorted(function.items(), key=lambda item: render(item[0]))


def changed_keys(before: Any, after: Any) -> list[Any]:
    """The arguments at which two functions (or sequences) differ, in a canonical order."""

    if isinstance(before, tuple) and isinstance(after, tuple):
        before = TlaFunction(enumerate(before, start=1))
        after = TlaFunction(enumerate(after, start=1))
    if not isinstance(before, dict) or not isinstance(after, dict):
        return []
    keys = set(before) | set(after)
    return sorted((key for key in keys if before.get(key) != after.get(key)), key=render)


# ---------------------------------------------------------------------------
# Modules and configs
# ---------------------------------------------------------------------------


def strip_comments(text: str) -> str:
    """``text`` without ``(* ... *)`` blocks and ``\\*`` line comments."""

    text = re.sub(r"\(\*.*?\*\)", " ", text, flags=re.DOTALL)
    return re.sub(r"\\\*[^\n]*", "", text)


def module_name(text: str) -> Optional[str]:
    match = re.search(r"-{4,}\s*MODULE\s+(\w+)\s*-{4,}", text)
    return match.group(1) if match else None


def _declared(body: str, keyword: str) -> tuple[str, ...]:
    names = []
    for match in re.finditer(rf"\b{keyword}S?\s+(\w+(?:\s*,\s*\w+)*)", body):
        names.extend(name.strip() for name in match.group(1).split(","))
    return tuple(names)


@dataclass(frozen=True)
class ModelSymbols:
    """What a module declares: its top-level definitions, constants, and variables."""

    definitions: frozenset[str]
    constants: tuple[str, ...]
    variables: tuple[str, ...]


def symbols(text: str) -> ModelSymbols:
    """The module's operator definitions (``Name ==`` or ``Name(args) ==``) and its
    ``CONSTANT(S)`` and ``VARIABLE(S)`` names, read in one pass over the uncommented text."""

    body = strip_comments(text)
    return ModelSymbols(
        definitions=frozenset(re.findall(r"^([A-Za-z_]\w*)\s*(?:\([^)]*\))?\s*==", body, flags=re.MULTILINE)),
        constants=_declared(body, "CONSTANT"),
        variables=_declared(body, "VARIABLE"),
    )


# Where a top-level definition ends: the next definition, declaration, or the module's end.
_TOP_LEVEL = re.compile(
    r"^(?:([A-Za-z_]\w*)\s*(?:\([^)]*\))?\s*==|(?:VARIABLES?|CONSTANTS?|ASSUME|THEOREM|LOCAL|INSTANCE|EXTENDS)\b|-{4,}|={4,})",
    flags=re.MULTILINE,
)


def definitions(text: str) -> dict[str, str]:
    """Each top-level operator definition's text (``Name == ...``), comments removed and whitespace collapsed."""

    body = strip_comments(text)
    starts = list(_TOP_LEVEL.finditer(body))
    found = {}
    for index, match in enumerate(starts):
        if match.group(1):
            end = starts[index + 1].start() if index + 1 < len(starts) else len(body)
            found[match.group(1)] = " ".join(body[match.start() : end].split())
    return found


# The modules every TLA+ distribution ships (Specifying Systems, plus TLC's): a
# bundle's modules may extend or instantiate only these and each other.
STANDARD_MODULES = frozenset({"Bags", "FiniteSets", "Integers", "Naturals", "RealTime", "Reals", "Sequences", "TLC"})


def module_references(text: str) -> set[str]:
    """The modules a TLA+ module extends or instantiates."""

    body = strip_comments(text)
    names: set[str] = set()
    for match in re.finditer(r"\bEXTENDS\b((?:\s*\w+\s*,)*\s*\w+)", body):
        names.update(name.strip() for name in match.group(1).split(","))
    names.update(re.findall(r"\bINSTANCE\s+(\w+)", body))
    return names


@dataclass(frozen=True)
class TlcConfig:
    """A TLC ``.cfg`` file, as far as the bridge reads one."""

    constants: dict[str, str]
    invariants: tuple[str, ...]
    properties: tuple[str, ...]

    @property
    def checked(self) -> tuple[str, ...]:
        return self.invariants + self.properties


_CFG_KEYWORDS = (
    "CONSTANTS",
    "CONSTANT",
    "INVARIANTS",
    "INVARIANT",
    "PROPERTIES",
    "PROPERTY",
    "SPECIFICATION",
    "INIT",
    "NEXT",
    "SYMMETRY",
    "CONSTRAINTS",
    "CONSTRAINT",
    "ACTION_CONSTRAINTS",
    "ACTION_CONSTRAINT",
    "VIEW",
    "CHECK_DEADLOCK",
    "POSTCONDITIONS",
    "POSTCONDITION",
    "ALIAS",
)
_CFG_SECTION = re.compile(r"\b(" + "|".join(_CFG_KEYWORDS) + r")\b")


def parse_config(text: str) -> TlcConfig:
    """Read a TLC config: its constant assignments and the invariants and properties it checks."""

    body = strip_comments(text)
    sections: dict[str, str] = {}
    marks = list(_CFG_SECTION.finditer(body))
    for index, mark in enumerate(marks):
        end = marks[index + 1].start() if index + 1 < len(marks) else len(body)
        keyword = mark.group(1)
        keyword = {"CONSTANTS": "CONSTANT", "INVARIANTS": "INVARIANT", "PROPERTIES": "PROPERTY"}.get(keyword, keyword)
        sections[keyword] = sections.get(keyword, "") + " " + body[mark.end() : end]
    constants: dict[str, str] = {}
    assignments = list(re.finditer(r"([A-Za-z_]\w*)\s*(?:=|<-)", sections.get("CONSTANT", "")))
    for index, assignment in enumerate(assignments):
        end = assignments[index + 1].start() if index + 1 < len(assignments) else len(sections["CONSTANT"])
        constants[assignment.group(1)] = " ".join(sections["CONSTANT"][assignment.end() : end].split())
    return TlcConfig(
        constants=constants,
        invariants=tuple(sections.get("INVARIANT", "").split()),
        properties=tuple(sections.get("PROPERTY", "").split()),
    )


def normalize(text: str) -> str:
    """TLA+ value text with whitespace removed, for comparing configured bounds."""

    return re.sub(r"\s+", "", str(text))


# ---------------------------------------------------------------------------
# Running the tools
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TlaTools:
    """Where ``tla2tools.jar`` and ``java`` are, and how TLC runs.

    :meth:`within` sets ``deadline`` (a :func:`time.monotonic` time)
    ``timeout_s`` seconds from now: a run still going at the deadline is
    killed with its process group and reported as timed out, and no run
    starts after it. Without a deadline a run takes as long as it needs.
    """

    jar: str
    java: str = "java"
    workers: str = "auto"
    timeout_s: Optional[float] = None
    deadline: Optional[float] = None

    @classmethod
    def locate(cls, jar: Optional[str], *, workers: str = "auto") -> "TlaTools":
        """The tools at ``jar`` (or ``$TLA2TOOLS_JAR``); raise :class:`TlaError` if java or the jar is missing."""

        path = jar or os.environ.get("TLA2TOOLS_JAR")
        if not path or not os.path.isfile(path):
            raise TlaError("tla2tools.jar not found: pass --tla2tools or set TLA2TOOLS_JAR")
        java = shutil.which("java")
        if java is None:
            raise TlaError("java not found on PATH")
        return cls(jar=os.path.abspath(path), java=java, workers=workers)

    def within(self, seconds: float) -> "TlaTools":
        """These tools with every run ending at most ``seconds`` from now."""

        return replace(self, timeout_s=seconds, deadline=time.monotonic() + seconds)

    def identity(self) -> dict[str, Any]:
        with open(self.jar, "rb") as handle:
            digest = hashlib.sha256(handle.read()).hexdigest()
        return {"jar": os.path.basename(self.jar), "sha256": digest}


@dataclass
class ToolRun:
    """One SANY or TLC process: its command, exit code, output, and duration."""

    command: list[str]
    exit_code: int
    output: str
    seconds: float
    timed_out: bool = False

    def tail(self, lines: int = 30) -> list[str]:
        return self.output.strip().splitlines()[-lines:]

    @property
    def tool(self) -> str:
        return "TLC" if "tlc2.TLC" in self.command else "SANY"

    @property
    def timeout_error(self) -> str:
        return f"{self.tool} was stopped after {self.seconds:.1f}s: the time limit ran out before it finished"

    @property
    def harness_error(self) -> Optional[str]:
        """Why this run says nothing about the model (it timed out, or the tool never started), else None.

        A tool that ran prints its banner (TLC's version, SANY's parsing); a
        JVM that cannot load it from the jar, or that dies first, prints neither.
        """

        if self.timed_out:
            return self.timeout_error
        if any(banner in self.output for banner in ("TLC2 Version", "SANY2 Version", "Parsing file")):
            return None
        last = (self.tail(1) or ["no output"])[0]
        return f"{self.tool} did not start (exit {self.exit_code}: {last}); check --tla2tools and java"


def _run(tools: TlaTools, arguments: list[str], files: Mapping[str, str]) -> ToolRun:
    """Run ``java -cp jar <arguments>`` in a fresh directory holding ``files`` ({name: text}).

    The JVM runs in its own process group; past the tools' deadline the whole
    group is killed and the run comes back with ``timed_out`` set.
    """

    timeout = None if tools.deadline is None else tools.deadline - time.monotonic()
    command = [tools.java, "-XX:+UseParallelGC", "-cp", tools.jar, *arguments]
    if timeout is not None and timeout <= 0:
        return ToolRun(command=command, exit_code=-1, output="", seconds=0.0, timed_out=True)
    with tempfile.TemporaryDirectory(prefix="valkey-fuzzer-tla-") as directory:
        for name, text in files.items():
            with open(os.path.join(directory, name), "w", encoding="utf-8") as handle:
                handle.write(text)
        started = time.monotonic()
        process = subprocess.Popen(
            command,
            cwd=directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
        except BaseException:
            # An interrupted check must not leave TLC running.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
        return ToolRun(
            command=command,
            exit_code=process.returncode,
            output=(stdout or "") + (stderr or ""),
            seconds=round(time.monotonic() - started, 2),
            timed_out=timed_out,
        )


def sany(tools: TlaTools, module: str, files: Mapping[str, str]) -> tuple[bool, ToolRun]:
    """Parse ``module`` (a file name in ``files``) with SANY; True when it parsed without errors."""

    run = _run(tools, ["tla2sany.SANY", module], files)
    failed = (
        run.timed_out
        or run.exit_code != 0
        or "*** Errors:" in run.output
        or "Could not parse" in run.output
        or "Fatal errors" in run.output
    )
    return not failed, run


def tlc(
    tools: TlaTools, module: str, config: str, files: Mapping[str, str], *, coverage: bool = False
) -> tuple["TlcResult", ToolRun]:
    """Model-check ``module`` with ``config`` (file names in ``files``)."""

    arguments = ["tlc2.TLC", "-workers", tools.workers, "-metadir", "tlc-states", "-config", config]
    if coverage:
        arguments += ["-coverage", "1"]
    run = _run(tools, [*arguments, module], files)
    result = read_tlc_output(run.output, run.exit_code)
    if run.timed_out:
        result.errors.append(run.timeout_error)
    return result, run


# ---------------------------------------------------------------------------
# TLC output
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TraceState:
    """One state of a TLC error trace: the action that produced it and every variable's value."""

    index: int
    action: str
    values: dict[str, Any]


@dataclass
class TlcResult:
    """What a TLC run found."""

    exit_code: int
    finished: bool
    violation: Optional[str] = None
    violated: Optional[str] = None
    trace: list[TraceState] = field(default_factory=list)
    coverage: dict[str, dict[str, int]] = field(default_factory=dict)
    distinct_states: Optional[int] = None
    generated_states: Optional[int] = None
    depth: Optional[int] = None
    version: Optional[str] = None
    errors: list[str] = field(default_factory=list)

    @property
    def no_error(self) -> bool:
        return self.exit_code == TLC_OK and self.finished and self.violation is None and not self.errors


_VIOLATIONS = (
    (re.compile(r"^Error: Invariant (\w+) is violated"), "invariant"),
    (re.compile(r"^Error: Action property (\w+) is violated"), "action-property"),
    (re.compile(r"^Error: Temporal properties were violated"), "temporal"),
    (re.compile(r"^Error: Deadlock reached"), "deadlock"),
)
_STATE = re.compile(r"^State (\d+): <(.*)>\s*$")
_COVERAGE = re.compile(
    r"^<(\w+) line \d+, col \d+ to line \d+, col \d+ of module \w+(?: \([\d ]+\))?>: (\d+):(\d+)\s*$"
)


def read_tlc_output(output: str, exit_code: int = 0) -> TlcResult:
    """Read a TLC run's text output: the verdict, the error trace, coverage, and state counts."""

    lines = output.splitlines()
    result = TlcResult(exit_code=exit_code, finished="Finished in" in output or "Model checking completed" in output)
    version = re.search(r"^TLC2 Version (.+)$", output, flags=re.MULTILINE)
    result.version = version.group(1).strip() if version else None
    for line in lines:
        for pattern, kind in _VIOLATIONS:
            match = pattern.match(line)
            if match and result.violation is None:
                result.violation = kind
                result.violated = match.group(1) if match.groups() else None
    # Coverage is printed periodically; the last block is the final one.
    blocks = [index for index, line in enumerate(lines) if line.startswith("The coverage statistics")]
    if blocks:
        for line in lines[blocks[-1] :]:
            match = _COVERAGE.match(line)
            if match and match.group(1) != "Init":
                result.coverage[match.group(1)] = {"distinct": int(match.group(2)), "generated": int(match.group(3))}
            if line.startswith("End of statistics"):
                break
    counts = re.findall(r"^(\d+) states generated, (\d+) distinct states found", output, flags=re.MULTILINE)
    if counts:
        result.generated_states, result.distinct_states = int(counts[-1][0]), int(counts[-1][1])
    depth = re.search(r"The depth of the complete state graph search is (\d+)", output)
    result.depth = int(depth.group(1)) if depth else None
    result.trace = read_trace(output)
    for line in lines:
        if re.match(r"^Error: ", line) and not any(pattern.match(line) for pattern, _ in _VIOLATIONS):
            if not line.startswith("Error: The behavior up to this point is"):
                result.errors.append(line)
    if exit_code not in (TLC_OK, TLC_VIOLATION_SAFETY, TLC_VIOLATION_LIVENESS, TLC_VIOLATION_DEADLOCK):
        result.errors.append(f"TLC exited with {exit_code}")
    return result


def read_trace(output: str) -> list[TraceState]:
    """The first error trace in TLC's text output (``State N: <Action ...>`` blocks)."""

    states: list[TraceState] = []
    current: Optional[tuple[int, str]] = None
    body: list[str] = []

    def close() -> None:
        if current is not None:
            states.append(TraceState(index=current[0], action=current[1], values=_assignments(body)))

    for line in output.splitlines():
        match = _STATE.match(line)
        if match:
            index = int(match.group(1))
            if states and index == 1 and current is None:
                break  # a second trace (TLC -continue): read only the first
            close()
            label = match.group(2)
            action = "Init" if label.startswith("Initial predicate") else label.split(" ")[0]
            current, body = (index, action), []
            continue
        if current is None:
            continue
        if (
            not line.strip()
            or line.startswith(("Back to state", "Error:", "Progress(", "The coverage"))
            or re.match(r"^\d+ states generated", line)
        ):
            close()
            current, body = None, []
            continue
        body.append(line)
    close()
    return states


def _assignments(lines: Iterable[str]) -> dict[str, Any]:
    """``/\\ name = value`` lines (values may wrap) as {name: value}."""

    joined: list[str] = []
    for line in lines:
        stripped = line.strip()
        if re.match(r"^(/\\\s+)?[A-Za-z_]\w*\s+=\s", stripped):
            joined.append(re.sub(r"^/\\\s+", "", stripped))
        elif joined:
            joined[-1] += " " + stripped
    values = {}
    for text in joined:
        name, _, value = text.partition(" = ")
        values[name.strip()] = parse_value(value)
    return values


def print_values(output: str, tag: str) -> list[Any]:
    """Every ``PrintT(<<tag, ...>>)`` tuple in TLC's output, parsed; TLC wraps long values over several lines."""

    found, buffer = [], None
    for line in output.splitlines():
        if buffer is None and not re.match(rf'^<<\s*"{re.escape(tag)}"', line):
            continue
        buffer = line if buffer is None else f"{buffer} {line.strip()}"
        if buffer.count("<<") == buffer.count(">>") and buffer.count("{") == buffer.count("}"):
            found.append(parse_value(buffer))
            buffer = None
    return found
