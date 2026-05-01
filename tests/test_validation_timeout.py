"""
Tests for validation timeout enforcement
"""

import time
from unittest.mock import Mock

import pytest

from src.fuzzer_engine.state_validator import StateValidator, ValidationTimeoutError, validation_timeout
from src.models import ClusterConnection, StateValidationConfig


def test_validation_timeout_sub_second():
    """Test that sub-second timeouts are properly enforced (not rounded up to 1 second)"""

    def slow_operation():
        time.sleep(2.0)

    start = time.time()
    with pytest.raises(ValidationTimeoutError), validation_timeout(0.25):
        slow_operation()
    elapsed = time.time() - start

    # Should timeout close to 0.25s, not be rounded up to 1.0s
    assert elapsed < 0.5, f"Timeout took {elapsed}s, should be close to 0.25s (not rounded to 1s)"


def test_stabilization_wait_respects_timeout():
    """Test that stabilization wait is capped by validation timeout"""
    # Config where stabilization exceeds timeout
    config = StateValidationConfig(
        stabilization_wait=60.0,
        validation_timeout=10.0,
        check_replication=False,
        check_cluster_status=False,
        check_slot_coverage=False,
        check_topology=False,
        check_view_consistency=False,
        check_data_consistency=False,
    )

    validator = StateValidator(config)
    mock_connection = Mock(spec=ClusterConnection)

    start_time = time.time()
    result = validator.validate_state(mock_connection)
    elapsed_time = time.time() - start_time

    # Should complete within timeout, not wait full 60 seconds
    assert elapsed_time < 15.0
    assert not result.overall_success
    assert "validation_timeout" in result.failed_checks


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
