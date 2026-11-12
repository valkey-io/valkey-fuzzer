"""State checks for a locally provisioned deployment.

:class:`DeploymentStateCheck` lets a component seed validation keys, record
the instances it stopped on purpose, and validate replication, cluster
state, slot coverage, roles, views, data, and instance logs, receiving
report-safe dicts. The local deployment service creates these checks;
components use a check's methods and its ``config``, never the check
functions and result models below it.
"""

import hashlib
import logging
import random
import time
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from typing import Any, Callable, Optional

import valkey

from ....protocols import RunContext
from ..models import DeploymentMode, InstanceInfo
from ..queries import WaitInterruptedError, pause, query_replication, valkey_client, wait_until
from ..topology import DeploymentConnection
from .cluster_state import check_cluster_status, check_slot_coverage
from .config import StateValidationConfig
from .data import check_data, write_test_keys
from .logs import LogNode, LogValidationResult, check_logs
from .replication import check_replication
from .results import (
    ClusterStatusValidation,
    DataConsistencyValidation,
    ReplicationValidation,
    RoleValidation,
    SlotCoverageValidation,
    StateValidationResult,
    ViewConsistencyValidation,
)
from .snapshot import Snapshot, take_snapshot
from .views import ExpectedRoles, check_roles, check_views

logger = logging.getLogger(__name__)


def scenario_rng(seed: Optional[int], stream: str) -> random.Random:
    """A deterministic RNG stream for one purpose of one seeded run."""

    if seed is None:
        return random.Random()
    material = f"valkey-fuzzer:{seed}:{stream}".encode()
    return random.Random(int.from_bytes(hashlib.sha256(material).digest(), byteorder="big"))


def adapt_validation_config(config: StateValidationConfig, mode: DeploymentMode, replicas_per_primary: int) -> None:
    """Fit the checks to the deployment, in place: no cluster checks outside cluster mode, where the role check
    always runs, no replication check without replicas, and never more required replicas than each primary has."""

    if mode is not DeploymentMode.CLUSTER:
        config.check_cluster_status = False
        config.check_slot_coverage = False
        config.check_view_consistency = False
        config.check_logs = False
        # Outside cluster mode the role check discovers nodes directly, so
        # it doubles as the liveness invariant; keep it on.
        config.check_roles = True
        if replicas_per_primary <= 0:
            config.check_replication = False
    config.replication_config.min_replicas_per_shard = min(
        config.replication_config.min_replicas_per_shard, max(0, replicas_per_primary)
    )


def _check_summaries(result: StateValidationResult) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    if result.replication:
        checks["replication"] = {
            "success": result.replication.success,
            "all_replicas_synced": result.replication.all_replicas_synced,
            "max_lag": result.replication.max_lag,
            "lagging_replicas": len(result.replication.lagging_replicas),
            "disconnected_replicas": len(result.replication.disconnected_replicas),
            "error": result.replication.error_message,
        }
    if result.cluster_status:
        checks["cluster_status"] = {
            "success": result.cluster_status.success,
            "cluster_state": result.cluster_status.cluster_state,
            "has_quorum": result.cluster_status.has_quorum,
            "nodes_in_fail_state": len(result.cluster_status.nodes_in_fail_state),
            "error": result.cluster_status.error_message,
        }
    if result.slot_coverage:
        checks["slot_coverage"] = {
            "success": result.slot_coverage.success,
            "total_slots_assigned": result.slot_coverage.total_slots_assigned,
            "unassigned_slots": len(result.slot_coverage.unassigned_slots),
            "conflicting_slots": len(result.slot_coverage.conflicting_slots),
            "error": result.slot_coverage.error_message,
        }
    if result.roles:
        checks["roles"] = {
            "success": result.roles.success,
            "expected_primaries": result.roles.expected_primaries,
            "actual_primaries": result.roles.actual_primaries,
            "expected_replicas": result.roles.expected_replicas,
            "actual_replicas": result.roles.actual_replicas,
            "mismatches": len(result.roles.role_mismatches),
            "error": result.roles.error_message,
        }
    if result.view_consistency:
        checks["view_consistency"] = {
            "success": result.view_consistency.success,
            "nodes_checked": result.view_consistency.nodes_checked,
            "consistent_views": result.view_consistency.consistent_views,
            "split_brain_detected": result.view_consistency.split_brain_detected,
            "consensus_percentage": result.view_consistency.consensus_percentage,
            "discrepancies": len(result.view_consistency.view_discrepancies),
            "error": result.view_consistency.error_message,
        }
    if result.data_consistency:
        checks["data_consistency"] = {
            "success": result.data_consistency.success,
            "test_keys_checked": result.data_consistency.test_keys_checked,
            "missing_keys": len(result.data_consistency.missing_keys),
            "inconsistent_keys": len(result.data_consistency.inconsistent_keys),
            "unreachable_keys": len(result.data_consistency.unreachable_keys),
            "error": result.data_consistency.error_message,
            # A few of the wrong keys, with where each was read and what it held.
            "examples": [
                {
                    "key": item.key,
                    "type": item.inconsistency_type,
                    "values": {str(where): str(value)[:200] for where, value in item.actual_values.items()},
                }
                for item in result.data_consistency.inconsistent_keys[:5]
            ],
        }
    if result.log_validation:
        checks["logs"] = {
            "success": result.log_validation.success,
            "shards_checked": result.log_validation.shards_checked,
            "findings": [
                {
                    "instance_id": finding.instance_id,
                    "shard": finding.shard_id,
                    "severity": finding.severity,
                    "message": finding.message,
                    "log_line": finding.log_line[:500],
                }
                for finding in result.log_validation.findings
            ],
        }
    return checks


def validation_summary(result: StateValidationResult) -> dict[str, Any]:
    """Report-safe form of one validation result."""

    return {
        "overall_success": bool(result.overall_success),
        # Nothing was judged: the oracles that read this report ERROR, not FAIL.
        "inconclusive": bool(result.inconclusive),
        "critical": bool(not result.overall_success and result.is_critical_failure()),
        "failed_checks": [str(check) for check in result.failed_checks],
        "error_messages": [str(message) for message in result.error_messages],
        "duration_s": round(result.validation_duration, 3),
        "checks": _check_summaries(result),
    }


# How long one validation waits, in all, for replicas to finish a sync in progress before judging them.
SYNC_WAIT_S = 60.0
SYNC_POLL_S = 0.5


def syncing_replicas(connection: DeploymentConnection) -> list[str]:
    """The reachable replicas whose link is down while the primary they follow answers: they are in a sync.

    Valkey moves a replica to an orphaned primary by itself (replica
    migration), and a replica resyncs after its primary failed over or
    restarted; until the full sync finishes (5 s of diskless-sync delay, then
    the load) its link is down and it holds the old shard's keys. That ends by
    itself. A replica whose primary does not answer is not syncing, and the
    checks judge it.
    """
    syncing = []
    for instance in connection.instances:
        try:
            with valkey_client(instance.host, instance.port, timeout=1.0) as client:
                info = query_replication(client)
            if info.role != "replica" or info.link_up:
                continue
            with valkey_client(info.primary_host, info.primary_port, timeout=1.0) as primary:
                primary.ping()
        except (valkey.ValkeyError, OSError):
            continue
        syncing.append(f"{instance.host}:{instance.port}")
    return syncing


class DeploymentStateCheck:
    """One deployment's state checks, with its test keys and the instances stopped on purpose.

    Each validation attempt reads the deployment once (:func:`take_snapshot`)
    and runs every enabled check on that one moment, within
    ``validation_timeout``; a failed attempt is retried as configured. An
    attempt in which a running instance did not answer, the time ran out, or
    a check broke is inconclusive: it judges nothing, and is retried. Its
    waits end at the run's deadline or a cancellation of ``context``'s call,
    raising :class:`~..queries.WaitInterruptedError`.
    """

    def __init__(
        self,
        connection: DeploymentConnection,
        instances: Sequence[InstanceInfo],
        planned_roles: Mapping[str, str],
        config: StateValidationConfig,
        *,
        rng: random.Random,
        guard: Callable[[], AbstractContextManager] = nullcontext,
        context: Optional[RunContext] = None,
    ):
        self.config = config
        self._context = context
        self._connection = connection
        self._instances = {instance.instance_id: instance for instance in instances}
        self._guard = guard
        self._rng = rng
        roles = list(planned_roles.values())
        self._expected = ExpectedRoles(num_primaries=roles.count("primary"), num_replicas=roles.count("replica"))
        self._test_keys: dict[str, str] = {}
        # The instances stopped on purpose, by address, with their role when they stopped.
        self._stopped: dict[str, str] = {}
        self._shards_with_primary_stopped: set[int] = set()
        self._snapshot: Optional[Snapshot] = None

    def write_test_keys(self) -> dict[str, str]:
        """Write the data-consistency keys; ``{}`` when that check is off."""

        if not self.config.check_data_consistency:
            return {}
        with self._guard():
            self._test_keys = write_test_keys(self._connection, self.config.data_consistency_config, self._rng)
        return dict(self._test_keys)

    def load_test_keys(self, keys: Mapping[str, str]) -> None:
        """Validate keys another check wrote (e.g. recorded in evidence)."""

        self._test_keys = {str(key): str(value) for key, value in keys.items()}

    def mark_stopped(self, instance_id: str, role: str, shard: Optional[int] = None) -> None:
        """Record an instance stopped on purpose, with its role and the live shard it served when it stopped
        (its planned shard when ``shard`` is None): replica migration and failovers move instances between
        shards, and the log check reads the shard whose primary stopped."""

        instance = self._instances[instance_id]
        self._stopped[f"{instance.host}:{instance.port}"] = role
        if role == "primary":
            self._shards_with_primary_stopped.add(instance.shard_id if shard is None else shard)

    def validate(self, operations: Optional[Sequence[Mapping[str, Any]]] = None) -> dict:
        """Run every enabled check; ``operations`` (failover records with a
        ``shard``) enable the log checks for the shards they touched."""

        failover_shards = None if operations is None else {int(record["shard"]) for record in operations}
        with self._guard():
            result = self._validate_with_retry(failover_shards)
            summary = validation_summary(result)
            if not summary["overall_success"]:
                summary["topology"] = self.topology()
            return summary

    def topology(self) -> list[dict[str, Any]]:
        """The deployment's live topology, as the last checks saw it: each instance's role, shard, primary,
        and status."""

        try:
            rows = self._snapshot.rows if self._snapshot else self._connection.get_current_nodes(include_failed=True)
        except Exception as exc:
            return [{"error": f"{type(exc).__name__}: {exc}"}]
        by_node_id = {row.node_id: row.instance_id for row in rows}
        return [
            {
                "instance_id": row.instance_id,
                "role": row.role,
                "shard": row.shard_id,
                "primary": by_node_id.get(row.primary_id, row.primary_id) if row.primary_id else None,
                "status": "failed" if row.failed else "connected",
                **({"slots": len(row.slots)} if row.slots is not None else {}),
            }
            for row in rows
        ]

    # -- one attempt -----------------------------------------------------------

    def _stopped_shards(self, snapshot: Snapshot) -> set[int]:
        """Live shards whose every member was stopped on purpose.

        Members come from the live topology (a slot owner and its replicas),
        not the plan: replica migration moves replicas between shards, so a
        planned shard can be all stopped while a migrated replica could still
        take over its slots, and a live shard can lose every member while a
        replica it gave away lives on elsewhere.
        """
        members: dict[int, list[str]] = {}
        for node in snapshot.rows:
            if node.shard_id is not None:
                members.setdefault(node.shard_id, []).append(node.address)
        return {shard for shard, addresses in members.items() if all(address in self._stopped for address in addresses)}

    def _snapshot_timeout(self) -> float:
        """The longest per-query timeout of the enabled checks that read the snapshot."""
        config = self.config
        timeouts = [
            sub.timeout
            for enabled, sub in (
                (config.check_replication, config.replication_config),
                (config.check_cluster_status, config.cluster_status_config),
                (config.check_slot_coverage, config.slot_coverage_config),
                (config.check_view_consistency, config.view_consistency_config),
            )
            if enabled
        ]
        return max(timeouts, default=config.replication_config.timeout)

    def _validate_once(self, failover_shards: Optional[set[int]]) -> StateValidationResult:
        """One attempt: read the deployment once, then run every enabled check on that snapshot, a ``failover_shards``
        set (even an empty one) enabling the log check."""
        config = self.config
        started = time.monotonic()
        deadline = started + config.validation_timeout
        failed_checks: list[str] = []
        error_messages: list[str] = []
        results: dict[str, Any] = {}
        log_result = None
        # Something was not judged (an instance did not answer, the time ran out, a check broke or could not
        # read); the attempt is inconclusive unless a check saw Valkey do something wrong.
        inconclusive = False
        judged_failure = False

        # The budget is enforced between checks: a check that starts runs to the end, bounded by its own
        # per-query timeouts, so its result is never mistaken for a timeout or the other way round.
        def out_of_time() -> bool:
            nonlocal inconclusive
            if time.monotonic() < deadline:
                return False
            message = f"Validation timeout ({config.validation_timeout}s) exceeded"
            logger.error(message)
            failed_checks.append("validation_timeout")
            error_messages.append(message)
            inconclusive = True
            return True

        if config.stabilization_wait > 0:
            logger.info("Waiting %ss for cluster stabilization before validation", config.stabilization_wait)
            pause(self._context, max(0.0, min(config.stabilization_wait, deadline - time.monotonic())), "stabilization")

        stopped = set(self._stopped)
        checks = (
            ("replication", config.check_replication, ReplicationValidation,
             lambda: check_replication(snapshot, config.replication_config, stopped)),
            ("cluster_status", config.check_cluster_status, ClusterStatusValidation,
             lambda: check_cluster_status(snapshot, config.cluster_status_config, stopped)),
            ("slot_coverage", config.check_slot_coverage, SlotCoverageValidation,
             lambda: check_slot_coverage(snapshot, config.slot_coverage_config, stopped, stopped_shards)),
            ("roles", config.check_roles, RoleValidation,
             lambda: check_roles(snapshot, self._expected, stopped, self._stopped, stopped_shards)),
            ("view_consistency", config.check_view_consistency, ViewConsistencyValidation,
             lambda: check_views(snapshot, config.view_consistency_config, stopped)),
            ("data_consistency", config.check_data_consistency, DataConsistencyValidation,
             lambda: check_data(snapshot, self._test_keys, config.data_consistency_config)),
        )  # fmt: skip
        check_logs_now = config.check_logs and failover_shards is not None
        try:
            if not out_of_time() and (check_logs_now or any(enabled for _, enabled, _, _ in checks)):
                snapshot = self._snapshot = take_snapshot(self._connection, self._snapshot_timeout(), self._context)
                stopped_shards = self._stopped_shards(snapshot)
                if snapshot.unanswered:
                    # The checks would judge instances the snapshot never saw.
                    inconclusive = True
                    failed_checks.append("unanswered")
                    error_messages.append(
                        "running instance(s) did not answer: "
                        + ", ".join(f"{address} ({why})" for address, why in sorted(snapshot.unanswered.items()))
                    )
                    checks = ()
                    check_logs_now = False
                for name, enabled, result_type, check in checks:
                    if enabled and out_of_time():
                        break
                    if enabled:
                        result, judged = self._run_check(name, result_type, check, failed_checks, error_messages)
                        results[name] = result
                        inconclusive |= not judged
                        judged_failure |= judged and not result.success
                else:
                    if check_logs_now and not out_of_time():
                        log_result = self._check_logs(snapshot, failover_shards)
                        if not log_result.success:
                            failed_checks.append("log_validation")
                            judged_failure = True
                        for finding in log_result.findings:
                            logger.log(
                                logging.ERROR if finding.severity == "error" else logging.WARNING,
                                "Log validation %s - Shard %s, Node %s: %s | Log: %s",
                                finding.severity,
                                finding.shard_id,
                                finding.instance_id,
                                finding.message,
                                finding.log_line,
                            )
        except WaitInterruptedError:
            raise
        except Exception as e:
            logger.error("Unexpected error during validation: %s", e)
            failed_checks.append("validation_framework")
            error_messages.append(f"Unexpected validation error: {e!s}")
            inconclusive = True

        result = StateValidationResult(
            overall_success=not failed_checks,
            validation_duration=time.monotonic() - started,
            inconclusive=inconclusive and not judged_failure,
            log_validation=log_result,
            failed_checks=failed_checks,
            error_messages=error_messages,
            **results,
        )
        if result.overall_success:
            logger.info("State validation PASSED (duration: %.2fs)", result.validation_duration)
        else:
            logger.error(
                "State validation FAILED (duration: %.2fs, failed checks: %s%s)",
                result.validation_duration,
                ", ".join(failed_checks),
                "; critical" if result.is_critical_failure() else "",
            )
        return result

    @staticmethod
    def _run_check(
        name: str, result_type: type, check: Callable[[], Any], failed_checks: list[str], error_messages: list[str]
    ) -> tuple[Any, bool]:
        """Run one check: its result, and whether it judged the deployment. One that raises judged nothing; it
        fails with its error, so the summary still reports it."""
        display_name = name.replace("_", " ").title()
        try:
            result = check()
            judged = not getattr(result, "inconclusive", False)
        except Exception as e:
            logger.error("%s validation error: %s", display_name, e)
            result, judged = result_type(success=False, error_message=f"Validation error: {e!s}"), False
        if not result.success:
            failed_checks.append(name)
            if result.error_message:
                error_messages.append(f"{display_name}: {result.error_message}")
        logger.info("%s validation: %s", display_name, "PASSED" if result.success else "FAILED")
        return result, judged

    def _check_logs(self, snapshot: Snapshot, failover_shards: set[int]) -> LogValidationResult:
        """The log check over each instance's log, with its live role and shard."""
        by_id = {info.instance_id: info for info in snapshot.instances}
        nodes = [
            LogNode(
                instance_id=info.instance_id,
                address=f"{info.host}:{info.port}",
                cluster_node_id=info.cluster_node_id,
                role=row.role,
                shard_id=row.shard_id,
                log_file=info.log_file,
            )
            for row in snapshot.rows
            for info in [by_id.get(row.instance_id)]
            if info is not None
        ]
        return check_logs(nodes, failover_shards, set(self._stopped), self._shards_with_primary_stopped)

    # -- retries ----------------------------------------------------------------

    def _validate_with_retry(self, failover_shards: Optional[set[int]]) -> StateValidationResult:
        """Validate, retrying transient failures.

        A failed attempt while replicas are in a sync (:func:`syncing_replicas`)
        waits for the sync to end, up to :data:`SYNC_WAIT_S` in all, and runs
        again without counting: a replica mid-sync has its link down and the
        old shard's keys, which looks like an unexpected disconnection and
        missing data. One still syncing after that is judged as it is. Any
        other failed or inconclusive attempt is retried while attempts remain,
        a critical one and one that fails like the last too: a cluster whose
        failover after a kill is still running has a primary too few, or slots
        on the stopped node, until Valkey promotes a replica. Only views that
        disagree with ``allow_transient_inconsistency`` off are not retried.
        """
        config = self.config
        max_attempts = config.max_retries + 1
        attempt = 0
        sync_deadline = time.monotonic() + SYNC_WAIT_S
        while True:
            attempt += 1
            if attempt > 1:
                logger.info("Validation attempt %s/%s", attempt, max_attempts)
            result = self._validate_once(failover_shards)
            if result.overall_success:
                return result
            if (
                "view_consistency" in result.failed_checks
                and not config.view_consistency_config.allow_transient_inconsistency
            ):
                logger.error("Views disagree and allow_transient_inconsistency is off; not retrying")
                return result
            if self._wait_for_syncs(sync_deadline):
                attempt -= 1
                continue
            failed = ", ".join(result.failed_checks)
            if attempt >= max_attempts or not config.retry_on_transient_failure:
                logger.error("Validation failed after %s attempt(s). Failed checks: %s", attempt, failed)
                return result
            backoff_delay = config.retry_delay * (2 ** (attempt - 1))
            logger.warning("Validation failed (attempt %s/%s), retrying in %.1fs", attempt, max_attempts, backoff_delay)
            pause(self._context, backoff_delay, "the next validation attempt")

    def _wait_for_syncs(self, deadline: float) -> bool:
        """Whether replicas were in a sync and every one finished it before ``deadline``."""

        if time.monotonic() >= deadline:
            return False
        syncing = syncing_replicas(self._connection)
        if not syncing:
            return False
        logger.info("Replicas in a sync, waiting for them before validating again: %s", syncing)
        if wait_until(
            lambda: not syncing_replicas(self._connection),
            timeout=deadline - time.monotonic(),
            interval=SYNC_POLL_S,
            what="replicas in a sync",
            context=self._context,
        ):
            return True
        logger.error("Replicas still in a sync after %.0fs: %s", SYNC_WAIT_S, syncing)
        return False
