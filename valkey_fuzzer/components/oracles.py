"""Run-level oracles: liveness and crash reports, deployment state, binaries, evidence assertions.

The workload-ledger oracle lives in :mod:`.ledger` and the dataset oracles in
:mod:`.snapshots`.
"""

import os
import re
from collections.abc import Sequence
from typing import Any, Optional

from valkey_fuzzer.declarations import Declarations
from valkey_fuzzer.deployment.local.validation.config import STATE_VALIDATION_SCHEMA
from valkey_fuzzer.plan import ComponentSpec, ExecutionPlan
from valkey_fuzzer.results import ComponentStatus, Evidence, evidence_of, make_evidence, make_result
from valkey_fuzzer.schema import ConfigError, ConfigField, plain_config

from .files import LOG_LINE
from .instances import deployment, stopped_on_purpose
from .observers import FORBIDDEN_LOG_MATCH
from .polling import poll_each
from .registration import component_registration
from .schemas import FLAG, LIST, MAPPING, TEXT, declared, mapping_of, optional
from .server import binary_version, server_identity

CRASH_PATTERNS = (
    r"=== (VALKEY|REDIS) BUG REPORT START",
    r"ASSERTION FAILED",
    r"==> \S+:\d+ '.*' is not true",
    r"Guru Meditation",
    r"crashed by signal",
    r"ERROR: AddressSanitizer",
    r"ERROR: LeakSanitizer",
    r"WARNING: ThreadSanitizer",
    r"runtime error: ",
)


# ---------------------------------------------------------------------------
# valkey.oracle.liveness — every Valkey experiment includes it
# ---------------------------------------------------------------------------

_LIVENESS_FIELDS = {
    "timeout_s": ConfigField(float, default=30.0, minimum=0.1, maximum=600),
    "extra_crash_patterns": ConfigField(list, default=list, items=TEXT),
}


def _validate_liveness(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    for index, pattern in enumerate(config["extra_crash_patterns"]):
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ConfigError(f"extra_crash_patterns[{index}] is not a valid regex: {exc}") from exc


def scan_for_crashes(paths: dict[str, str], patterns: Sequence[str]) -> dict[str, list[str]]:
    compiled = [re.compile(pattern) for pattern in patterns]
    findings: dict[str, list[str]] = {}
    for instance_id, path in paths.items():
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if any(pattern.search(line) for pattern in compiled):
                        findings.setdefault(instance_id, []).append(line.rstrip()[:500])
        except OSError:
            continue
    return findings


# What a crash report says caused it, most specific first: an assertion's file:line and condition, a panic,
# a sanitizer's finding, then the signal. Within one kind the first in the log wins: a second assertion
# while the bug report is written is not the cause.
_CRASH_CAUSES = tuple(
    re.compile(pattern)
    for pattern in (
        r"==> (\S+:\d+ '.*' is not true)",
        r"Guru Meditation: (.*?)\s*$",
        r"ERROR: ((?:Address|Leak)Sanitizer: \S+)",
        r"WARNING: (ThreadSanitizer: [^(]*?)\s*(?:\(|$)",
        r"(\S+: runtime error: .*?)\s*$",
        r"(crashed by signal: \d+)",
    )
)


def crash_cause(lines: Sequence[str]) -> str:
    """What caused the crash whose report ``lines`` hold (see ``_CRASH_CAUSES``); its first line otherwise."""

    for pattern in _CRASH_CAUSES:
        for line in lines:
            match = pattern.search(line)
            if match:
                return match.group(1)
    match = LOG_LINE.match(lines[0])
    return (match["message"] if match else lines[0]).strip()


# A fatal exit without a crash report: Valkey logs why at warning level ("#") just before exit(1), as in
# "Short read or OOM loading DB. Unrecoverable error, aborting now.", "Failed listening on port N (cluster),
# aborting.", "Fatal error loading the DB, check server logs. Exiting." or "Terminating server after rdb file
# reading failure.". Other warnings that abort something ("Aborting dual channel sync.") carry on.
_FATAL_EXIT = re.compile(
    r"Unrecoverable error|\b[Aa]borting(?: now|\.|$| \S+ startup)|\b[Ee]xiting\b|\bexit now\b|\bwill now exit\b"
    r"|^(?:\*+ )?(?:FATAL|Fatal)\b|^Terminating server"
)
# A new life of the process, or a load that went on after a fatal-looking line ("... assuming connection
# error, resuming operation."): what came before is not why the process is gone.
_CARRIED_ON = re.compile(r"oO0OoO0OoO0Oo \w+ is starting|resuming operation")


def _warning(line: str) -> Optional[str]:
    """The message of a warning-level log line (``#``), with or without the :data:`LOG_LINE` prefix; else None."""

    match = LOG_LINE.match(line)
    if match is not None:
        return match["message"].rstrip() if match["level"] == "#" else None
    return line[2:].rstrip() if line.startswith("# ") else None


def fatal_exit(path: str) -> Optional[str]:
    """The last fatal-looking warning (see ``_FATAL_EXIT``) the process logging to ``path`` wrote before it
    exited; None if it logged none since it last started or carried on."""

    last = None
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if _CARRIED_ON.search(line):
                    last = None
                    continue
                warning = _warning(line)
                if warning is not None and _FATAL_EXIT.search(warning):
                    last = line.rstrip()[:500]
    except OSError:
        return None
    return last


_HEALTHY = ("ok", "stopped on purpose")


class LivenessOracle:
    """Every instance is alive and responsive, with no crash or sanitizer report.

    Instances listed by ``valkey.instances.expected-down`` evidence
    (``{"instances": [...]}`` from any component that stopped them on purpose)
    may be down; their logs are still scanned for crash reports. Each crash
    is reported by its cause (:func:`crash_cause`), in the summary and as
    ``crashes`` in the diagnostics, which the failure signature uses: one
    crash is one signature, whatever failed after it. An instance that died
    unexpectedly without a crash report exited on a fatal error: its cause
    is the last fatal line it logged (:func:`fatal_exit`), if any. Each of
    these is a crash: the FAIL is marked ``crash``, so it stands even when the
    teardown before the oracles failed. A ``valkey.log.forbidden-match`` from a
    ``valkey.observer.log`` observer fails the run too, naming the patterns and
    the instances that logged them; on its own that is no crash.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)

        def responsive(instance_id: str) -> tuple[bool, str]:
            # One stopped on purpose may exit while it is polled.
            if instance_id in stopped_on_purpose(handle, evidence):
                return True, "stopped on purpose"
            with handle.new_client(instance_id, timeout=2.0) as client:
                try:
                    client.ping()
                    if str(client.info("persistence").get("loading", "0")) in ("0", "False", "false"):
                        return True, "ok"
                    return False, "still loading"
                except Exception as exc:
                    return False, f"{type(exc).__name__}: {exc}"

        ids = [instance.instance_id for instance in handle.instances]
        polled = poll_each(
            context, [item for item in ids if handle.is_alive(item)], responsive, timeout_s=config["timeout_s"]
        )
        stopped = stopped_on_purpose(handle, evidence)
        status = {
            item: polled[item][1]
            if item in polled
            else "stopped on purpose"
            if item in stopped
            else "process not running"
            for item in ids
        }
        patterns = CRASH_PATTERNS + tuple(config["extra_crash_patterns"])
        crashes = scan_for_crashes(
            {instance.instance_id: handle.log_file(instance.instance_id) for instance in handle.instances}, patterns
        )
        # Sanitizers report on stderr, which the deployment captures separately.
        outputs = {
            instance.instance_id: handle.process_output_file(instance.instance_id) for instance in handle.instances
        }
        found = scan_for_crashes({key: path for key, path in outputs.items() if path}, patterns)
        for instance_id, lines in found.items():
            crashes.setdefault(instance_id, []).extend(lines)
        problems = [f"{instance_id}: {reason}" for instance_id, reason in status.items() if reason not in _HEALTHY]
        causes = {instance_id: crash_cause(lines) for instance_id, lines in crashes.items()}
        problems += [f"{instance_id}: crash report ({cause})" for instance_id, cause in causes.items()]
        for instance_id, reason in status.items():
            if reason != "process not running" or instance_id in crashes:
                continue
            line = fatal_exit(handle.log_file(instance_id))
            if line:
                crashes[instance_id] = [line]
                causes[instance_id] = _warning(line)
                problems.append(f"{instance_id}: fatal exit ({causes[instance_id]})")
        # Every problem so far is a server that crashed, exited, or stopped answering.
        crashed = bool(problems)
        forbidden: dict[str, dict[str, Any]] = {}
        for item in evidence_of(evidence, FORBIDDEN_LOG_MATCH):
            forbidden[item.source] = dict(item.value)
            problems += [
                f"{', '.join(sorted(hits))}: forbidden log pattern {pattern!r} matched (seen by {item.source})"
                for pattern, hits in sorted(item.value.items())
            ]
        record = {"instances": status, "crash_reports": crashes, "crashes": causes, "forbidden_log_matches": forbidden}
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=record, crash=crashed)
        down = sum(reason == "stopped on purpose" for reason in status.values())
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(status) - down} instance(s) alive and responsive"
            + (f", {down} stopped on purpose" if down else "")
            + "; no crash reports",
            evidence=(make_evidence(spec, "valkey.liveness.verified", "oracle.completed", record),),
        )


# ---------------------------------------------------------------------------
# valkey.oracle.deployment-state — the deployment's state validators
# ---------------------------------------------------------------------------

_DEPLOYMENT_STATE_FIELDS = {
    # The deployment's state validation config (``handle.state_check``), as the chaos actions take it.
    "validation": ConfigField(dict, default=dict, schema=STATE_VALIDATION_SCHEMA),
    # The chaos action whose kills, failovers, and test keys to use; any by default.
    "chaos": ConfigField(str, allow_none=True, default=None),
}


class DeploymentStateOracle:
    """Run the deployment's own state validators at the end of the run: replication, cluster state, slots, roles,
    views, and, after a chaos action, its test keys and the logs of the shards it failed over.

    Every experiment that leaves the deployment as it formed it checks that it still is. A chaos action's
    evidence (``valkey.chaos.schedule`` and ``valkey.chaos.test-keys``, from ``chaos`` or any) marks its
    kills as stopped on purpose, enables the log check for the shards its failovers touched, and gives the
    keys the data check reads; without test keys the data check is off, and without a schedule the log check.
    An instance any component reports down on purpose (``valkey.instances.expected-down``) is stopped on
    purpose too, as the liveness oracle judges it.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)
        try:
            state = handle.state_check(config["validation"], seed=context.plan.seed)
        except ValueError as exc:
            return make_result(spec, ComponentStatus.ERROR, f"invalid validation config: {exc}")
        keys = evidence_of(evidence, "valkey.chaos.test-keys", config["chaos"])
        notes = []
        if keys:
            state.load_test_keys(keys[-1].value["keys"])
        elif state.config.check_data_consistency:
            state.config.check_data_consistency = False
            notes.append("data consistency not checked: no test keys were recorded")
        schedules = evidence_of(evidence, "valkey.chaos.schedule", config["chaos"])
        operations = [] if schedules else None
        killed = set()
        for item in schedules:
            for kill in item.value["kills"]:
                if kill["success"]:
                    # The live shard it served: replica migration and failovers move nodes away from the plan.
                    state.mark_stopped(kill["instance_id"], kill["role"], shard=kill["shard"])
                    killed.add(kill["instance_id"])
            # Failovers enable the log checks of the shards they touched.
            operations.extend(
                record for record in item.value["operations"] if record["success"] and record["op"] == "failover"
            )
        down = stopped_on_purpose(handle, evidence) - killed
        for instance in handle.instances:
            if instance.instance_id in down:
                state.mark_stopped(instance.instance_id, instance.role)
        summary = state.validate(operations)
        summary["notes"] = notes
        if not summary["overall_success"]:
            # An inconclusive validation never saw the deployment (an instance did not answer, time ran out).
            return make_result(
                spec,
                ComponentStatus.ERROR if summary["inconclusive"] else ComponentStatus.FAIL,
                "; ".join(summary["error_messages"]) or f"failed checks: {', '.join(summary['failed_checks'])}",
                diagnostics=summary,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{handle.mode.value} state validated ({len(summary['checks'])} check(s))",
            evidence=(make_evidence(spec, "valkey.deployment.validated", "oracle.completed", summary),),
        )


# ---------------------------------------------------------------------------
# valkey.oracle.evidence — generic evidence predicates
# ---------------------------------------------------------------------------

# Evidence ``evidence_id`` from component ``source``, at least ``min_count`` times.
_EVIDENCE_RULE = mapping_of(
    {
        "evidence_id": TEXT,
        "source": TEXT,
        "min_count": ConfigField(int, default=1, minimum=1),
    }
)
_ASSERT_FIELDS = {
    "require": ConfigField(list, default=list, items=_EVIDENCE_RULE),
    "forbid": ConfigField(list, default=list, items=_EVIDENCE_RULE),
    # Evidence the run must have recorded for the rules to apply: without it the run never reached the state
    # they are about, and the oracle cannot judge them.
    "preconditions": ConfigField(list, default=list, items=_EVIDENCE_RULE),
}
_RULE_GROUPS = ("require", "forbid", "preconditions")


def _validate_assert(config: dict, spec: ComponentSpec, plan: ExecutionPlan) -> None:
    if not config["require"] and not config["forbid"]:
        raise ConfigError("evidence assertion needs require or forbid rules")
    # A source that could not do its part makes a FAIL that depends on it untrusted, which only a dependency does.
    dependencies = plan.dependencies(spec.id)
    for group in _RULE_GROUPS:
        for index, rule in enumerate(config[group]):
            if rule["source"] not in dependencies:
                raise ConfigError(
                    f"{group}[{index}] reads evidence from {rule['source']!r}, which the oracle must depend on "
                    "(depends_on, directly or through others)"
                )


def _assert_references(spec: ComponentSpec) -> list[tuple[str, Optional[str]]]:
    """Every rule's evidence: the compiler rejects one its source does not declare, so a misspelt ``forbid``
    cannot pass vacuously and a misspelt ``require`` cannot fail every run."""

    config = plain_config(spec.config)
    return [(rule["evidence_id"], rule["source"]) for group in _RULE_GROUPS for rule in config[group]]


def _count(evidence: Sequence[Evidence], rule: dict) -> int:
    return len(evidence_of(evidence, rule["evidence_id"], rule["source"]))


class EvidenceAssertOracle:
    """``require`` rules: evidence that must have been recorded; ``forbid`` rules: evidence that must not.

    Observers only record what the server did; this oracle judges it. Missing
    required evidence and recorded forbidden evidence are both findings
    (FAIL). Every rule's ``source`` is a component the oracle depends on, so a
    source that could not read the server (it returned ERROR) leaves the
    finding untrusted. Without its ``preconditions`` the run never reached the
    state the rules are about, and the oracle cannot judge them (ERROR).
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        unmet = [
            f"{rule['evidence_id']} from {rule['source']}"
            for rule in config["preconditions"]
            if _count(evidence, rule) < rule["min_count"]
        ]
        if unmet:
            return make_result(
                spec, ComponentStatus.ERROR, f"not judged: the preconditions were not recorded: {', '.join(unmet)}"
            )
        missing = [
            f"missing {rule['evidence_id']} from {rule['source']} ({count}/{rule['min_count']})"
            for rule in config["require"]
            if (count := _count(evidence, rule)) < rule["min_count"]
        ]
        forbidden = [
            f"forbidden evidence {rule['evidence_id']} from {rule['source']} was observed"
            for rule in config["forbid"]
            if _count(evidence, rule)
        ]
        if forbidden or missing:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(forbidden + missing))
        return make_result(spec, ComponentStatus.PASS, "evidence assertions hold")


# ---------------------------------------------------------------------------
# valkey.oracle.binary-check — every instance runs the binary it was assigned
# ---------------------------------------------------------------------------

_BINARY_FIELDS = {
    "min_distinct_builds": ConfigField(int, default=1, minimum=1, maximum=64),
    # Every instance must run this binary, e.g. ``{$ref: inputs.to_binary}``.
    "expect_binary": ConfigField(str, allow_none=True, default=None, reference=True),
}


def _same_file(left: Optional[str], right: Optional[str]) -> bool:
    return bool(left) and bool(right) and os.path.realpath(left) == os.path.realpath(right)


# How long the oracle retries reading an instance's INFO server (a restarted instance may still be loading).
_BINARY_READ_TIMEOUT_S = 30.0


class BinaryCheckOracle:
    """Each instance's running server is the managed process on its assigned binary.

    A running instance whose INFO server cannot be read within
    ``_BINARY_READ_TIMEOUT_S`` makes it ERROR; an instance that is not running
    has no server to check, and ``valkey.oracle.liveness`` judges it.
    """

    def evaluate(self, context: Any, spec: ComponentSpec, evidence: Sequence[Evidence]):
        config = spec.config
        handle = deployment(context)

        def read(instance_id: str) -> tuple[bool, Any]:
            with handle.new_client(instance_id, timeout=5.0) as client:
                try:
                    return True, client.info("server")
                except Exception as exc:
                    return False, f"{type(exc).__name__}: {exc}"

        ids = [instance.instance_id for instance in handle.instances]
        polled = poll_each(
            context, [item for item in ids if handle.is_alive(item)], read, timeout_s=_BINARY_READ_TIMEOUT_S
        )
        unread = {instance_id: value for instance_id, (answered, value) in polled.items() if not answered}
        unchecked = [item for item in ids if item not in polled or item in unread]
        expected_by_binary: dict[str, dict[str, Optional[str]]] = {}
        instances = {}
        problems = []
        for instance_id, (answered, info) in polled.items():
            if not answered:
                continue
            reported = server_identity(info)
            assigned = handle.binary(instance_id)
            if assigned not in expected_by_binary:
                expected_by_binary[assigned] = binary_version(assigned)
            expected = expected_by_binary[assigned]
            instances[instance_id] = {"binary": assigned, "expected": expected, "reported": reported}
            if config["expect_binary"] is not None and not _same_file(assigned, config["expect_binary"]):
                problems.append(f"{instance_id}: assigned {assigned}, expected {config['expect_binary']}")
            if reported["executable"] and not _same_file(reported["executable"], assigned):
                problems.append(f"{instance_id}: runs {reported['executable']}, assigned {assigned}")
            if expected["version"] and expected["version"] != reported["version"]:
                problems.append(f"{instance_id}: runs {reported['version']}, its binary is {expected['version']}")
            # INFO parsing turns an all-digit SHA into an integer, so compare
            # without leading zeros.
            expected_sha = (expected["git_sha"] or "").lower().lstrip("0")
            reported_sha = reported["git_sha"].lstrip("0")
            if expected_sha and reported_sha:
                short = min(len(expected_sha), len(reported_sha))
                if expected_sha[:short] != reported_sha[:short]:
                    problems.append(f"{instance_id}: git sha {reported['git_sha']} does not match its binary")
            if str(reported["process_id"]) != str(handle.pid(instance_id)):
                problems.append(f"{instance_id}: INFO process_id does not match the managed process")
        versions = sorted(
            {(record["reported"]["version"], record["reported"]["git_sha"]) for record in instances.values()}
        )
        # An instance that was not checked may run the build that is missing.
        if len(versions) < config["min_distinct_builds"] and not unchecked:
            problems.append(
                f"only {len(versions)} distinct build(s) are running; expected {config['min_distinct_builds']}"
            )
        result = {
            "instances": instances,
            "distinct_builds": [list(item) for item in versions],
            "mixed": len(versions) > 1,
        }
        if problems:
            return make_result(spec, ComponentStatus.FAIL, "; ".join(problems), diagnostics=result)
        if unread:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                "cannot read INFO server of "
                + "; ".join(f"{instance_id}: {error}" for instance_id, error in sorted(unread.items())),
                diagnostics=result,
            )
        if len(versions) < config["min_distinct_builds"]:
            return make_result(
                spec,
                ComponentStatus.ERROR,
                f"{len(versions)} distinct build(s) among the running instances, expected "
                f"{config['min_distinct_builds']}; {', '.join(unchecked)} not running, so not checked",
                diagnostics=result,
            )
        return make_result(
            spec,
            ComponentStatus.PASS,
            f"{len(instances)} instance(s) run their assigned binaries ({len(versions)} distinct build(s))",
            evidence=(make_evidence(spec, "valkey.binary.verified", "oracle.completed", result),),
        )


_LIVENESS_DECLARES = Declarations(
    evidence={
        "valkey.liveness.verified": declared(
            mapping_of(
                {"instances": MAPPING, "crash_reports": MAPPING, "crashes": MAPPING, "forbidden_log_matches": MAPPING}
            )
        )
    }
)
# The deployment's report-safe validation summary, with the oracle's notes.
_VALIDATED = mapping_of(
    {
        "overall_success": FLAG,
        "inconclusive": FLAG,
        "critical": FLAG,
        "failed_checks": LIST,
        "error_messages": LIST,
        "duration_s": optional(float),
        "checks": MAPPING,
        "topology": optional(list),
        "notes": LIST,
    }
)
_STATE_DECLARES = Declarations(evidence={"valkey.deployment.validated": declared(_VALIDATED)})
_BINARY_DECLARES = Declarations(
    evidence={
        "valkey.binary.verified": declared(mapping_of({"instances": MAPPING, "distinct_builds": LIST, "mixed": FLAG}))
    }
)

REGISTRATIONS = (
    component_registration(
        "valkey.oracle.liveness",
        LivenessOracle,
        _LIVENESS_FIELDS,
        check=_validate_liveness,
        declares=_LIVENESS_DECLARES,
    ),
    component_registration(
        "valkey.oracle.deployment-state",
        DeploymentStateOracle,
        _DEPLOYMENT_STATE_FIELDS,
        declares=_STATE_DECLARES,
    ),
    component_registration("valkey.oracle.binary-check", BinaryCheckOracle, _BINARY_FIELDS, declares=_BINARY_DECLARES),
    component_registration(
        "valkey.oracle.evidence",
        EvidenceAssertOracle,
        _ASSERT_FIELDS,
        check=_validate_assert,
        references_evidence=_assert_references,
        declares=Declarations(),
    ),
)
