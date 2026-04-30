"""
Unit tests for _collect_failure_reasons — specifically the chaos-tolerance logic.

Each test targets a concrete edge case raised during code review.
"""

import time
from unittest.mock import Mock

from src.fuzzer_engine.fuzzer_engine import FuzzerEngine
from src.models import (
    ChaosConfig,
    ChaosCoordination,
    ChaosResult,
    ChaosTiming,
    ChaosType,
    ProcessChaosType,
    TargetSelection,
)


def _chaos_event(success: bool, phase: str = "during") -> ChaosResult:
    return ChaosResult(
        chaos_id="c1",
        chaos_type=ChaosType.PROCESS_KILL,
        target_node="node-0",
        success=success,
        start_time=time.time(),
        end_time=time.time(),
        chaos_phase=phase,
    )


def _validation(overall_success: bool):
    v = Mock()
    v.overall_success = overall_success
    v.failed_checks = [] if overall_success else ["topology"]
    return v


def _chaos_config(before=False, during=False, after=False, randomize=False):
    return ChaosConfig(
        chaos_type=ChaosType.PROCESS_KILL,
        target_selection=TargetSelection(strategy="random"),
        timing=ChaosTiming(),
        coordination=ChaosCoordination(
            chaos_before_operation=before,
            chaos_during_operation=during,
            chaos_after_operation=after,
        ),
        process_chaos_type=ProcessChaosType.SIGKILL,
        randomize_per_operation=randomize,
    )


engine = FuzzerEngine()


# ── Basic: no chaos config at all ──────────────────────────────────────


def test_no_chaos_config_operation_failure_is_real():
    """Without any chaos config, operation failures must always be reported."""
    reasons = engine._collect_failure_reasons(
        total_operations=3,
        operations_executed=2,
        chaos_events=[_chaos_event(True)],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=None,
    )
    assert any("1 operation(s) failed" in r for r in reasons)


# ── During-chaos: the common case ──────────────────────────────────────


def test_during_chaos_all_validations_pass_tolerates():
    """During-chaos + all validations pass → tolerate operation failures."""
    reasons = engine._collect_failure_reasons(
        total_operations=3,
        operations_executed=1,
        chaos_events=[_chaos_event(True), _chaos_event(True)],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=_chaos_config(during=True),
    )
    assert not any("operation(s) failed" in r for r in reasons)


def test_during_chaos_validation_fails_not_tolerated():
    """During-chaos but final validation fails → operation failure reported."""
    reasons = engine._collect_failure_reasons(
        total_operations=3,
        operations_executed=2,
        chaos_events=[_chaos_event(True)],
        validation_results=[],
        final_validation_result=_validation(False),
        chaos_config=_chaos_config(during=True),
    )
    assert any("1 operation(s) failed" in r for r in reasons)


# ── After-only chaos: must NOT tolerate ────────────────────────────────


def test_after_only_chaos_not_tolerated():
    """chaos_after_operation only — operation ran before chaos, failure is real."""
    reasons = engine._collect_failure_reasons(
        total_operations=3,
        operations_executed=2,
        chaos_events=[_chaos_event(True, phase="after")],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=_chaos_config(after=True),
    )
    assert any("1 operation(s) failed" in r for r in reasons)


# ── Mixed before/during + after: the tricky edge case ─────────────────


def test_mixed_before_and_after_all_before_failed():
    """before + after enabled, but only after-events succeeded.

    The before-events all failed, so no chaos actually interfered with
    operations.  Must NOT tolerate.
    """
    reasons = engine._collect_failure_reasons(
        total_operations=2,
        operations_executed=1,
        chaos_events=[
            _chaos_event(False, phase="before"),  # before op-1 failed
            _chaos_event(True, phase="after"),  # after op-1 succeeded
            _chaos_event(False, phase="before"),  # before op-2 failed
            _chaos_event(True, phase="after"),  # after op-2 succeeded
        ],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=_chaos_config(before=True, after=True),
    )
    assert any("1 operation(s) failed" in r for r in reasons)


def test_mixed_during_and_after_some_during_succeeded():
    """during + after enabled, and at least one during-event succeeded.
    Should tolerate.
    """
    reasons = engine._collect_failure_reasons(
        total_operations=2,
        operations_executed=1,
        chaos_events=[
            _chaos_event(True, phase="during"),  # during op-1
            _chaos_event(True, phase="after"),  # after op-1
            _chaos_event(True, phase="during"),  # during op-2
            _chaos_event(False, phase="after"),  # after op-2 failed
        ],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=_chaos_config(during=True, after=True),
    )
    assert not any("operation(s) failed" in r for r in reasons)


# ── randomize_per_operation ────────────────────────────────────────────


def test_randomize_per_operation_tolerates():
    """When randomize_per_operation is enabled, any operation may have had
    before/during chaos, so tolerance applies."""
    reasons = engine._collect_failure_reasons(
        total_operations=3,
        operations_executed=2,
        chaos_events=[_chaos_event(True)],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=_chaos_config(after=True, randomize=True),
    )
    assert not any("operation(s) failed" in r for r in reasons)


# ── No successful chaos events at all ──────────────────────────────────


def test_during_chaos_all_events_failed_not_tolerated():
    """Chaos was configured during but every injection failed — no
    actual interference, so operation failure is real."""
    reasons = engine._collect_failure_reasons(
        total_operations=2,
        operations_executed=1,
        chaos_events=[_chaos_event(False), _chaos_event(False)],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=_chaos_config(during=True),
    )
    assert any("1 operation(s) failed" in r for r in reasons)


# ── Chaos injection failures are always reported ───────────────────────


def test_failed_chaos_injections_always_reported():
    """Failed chaos injections are reported regardless of tolerance."""
    reasons = engine._collect_failure_reasons(
        total_operations=2,
        operations_executed=2,
        chaos_events=[_chaos_event(False)],
        validation_results=[],
        final_validation_result=_validation(True),
        chaos_config=_chaos_config(during=True),
    )
    assert any("chaos injection(s) failed" in r for r in reasons)
