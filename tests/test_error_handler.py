"""
Tests for error handling and recovery mechanisms
"""

import time
from unittest.mock import Mock

from src.fuzzer_engine.error_handler import (
    ErrorCategory,
    ErrorContext,
    ErrorHandler,
    ErrorSeverity,
    RetryConfig,
    get_error_handler,
)


class TestErrorContext:
    """Test ErrorContext dataclass"""

    def test_create_error_context(self):
        """Test creating error context"""
        context = ErrorContext(
            category=ErrorCategory.CLUSTER_CREATION,
            severity=ErrorSeverity.HIGH,
            message="Test error",
            cluster_id="test-cluster",
        )

        assert context.category == ErrorCategory.CLUSTER_CREATION
        assert context.severity == ErrorSeverity.HIGH
        assert context.message == "Test error"
        assert context.cluster_id == "test-cluster"


class TestRetryConfig:
    """Test RetryConfig dataclass"""

    def test_default_retry_config(self):
        """Test default retry configuration"""
        config = RetryConfig()

        assert config.max_attempts == 3
        assert config.initial_delay == 1.0
        assert config.exponential_base == 2.0
        assert config.jitter is True

    def test_custom_retry_config(self):
        """Test custom retry configuration"""
        config = RetryConfig(max_attempts=5, initial_delay=2.0, max_delay=30.0, exponential_base=3.0, jitter=False)

        assert config.max_attempts == 5
        assert config.initial_delay == 2.0
        assert config.max_delay == 30.0
        assert config.exponential_base == 3.0
        assert config.jitter is False


class TestErrorHandler:
    """Test ErrorHandler class"""

    def test_initialization(self):
        """Test error handler initialization"""
        handler = ErrorHandler()

        assert len(handler.error_history) == 0
        assert len(handler.recovery_strategies) > 0

    def test_handle_error_low_severity(self):
        """Test handling low severity error"""
        handler = ErrorHandler()

        context = ErrorContext(
            category=ErrorCategory.CHAOS_INJECTION, severity=ErrorSeverity.LOW, message="Minor chaos injection issue"
        )

        result = handler.handle_error(context)

        assert result is True
        assert len(handler.error_history) == 1

    def test_handle_error_fatal_severity(self):
        """Test handling fatal error"""
        handler = ErrorHandler()

        context = ErrorContext(
            category=ErrorCategory.CLUSTER_CREATION,
            severity=ErrorSeverity.FATAL,
            message="Fatal cluster creation error",
        )

        result = handler.handle_error(context)

        assert result is False
        assert len(handler.error_history) == 1

    def test_handle_error_with_recovery(self):
        """Test error handling with recovery strategy"""
        handler = ErrorHandler()

        # Chaos injection errors should recover gracefully
        context = ErrorContext(
            category=ErrorCategory.CHAOS_INJECTION, severity=ErrorSeverity.MEDIUM, message="Chaos injection failed"
        )

        result = handler.handle_error(context)

        assert result is True  # Should recover

    def test_retry_with_backoff_success_first_attempt(self):
        """Test retry succeeds on first attempt"""
        handler = ErrorHandler()

        mock_operation = Mock(return_value="success")
        config = RetryConfig(max_attempts=3)

        success, result = handler.retry_with_backoff(
            operation=mock_operation,
            config=config,
            error_category=ErrorCategory.OPERATION_EXECUTION,
            operation_name="test operation",
        )

        assert success is True
        assert result == "success"
        assert mock_operation.call_count == 1

    def test_retry_with_backoff_success_after_retries(self):
        """Test retry succeeds after some failures"""
        handler = ErrorHandler()

        # Fail twice, then succeed
        mock_operation = Mock(side_effect=[Exception("Fail 1"), Exception("Fail 2"), "success"])

        config = RetryConfig(max_attempts=3, initial_delay=0.1)

        success, result = handler.retry_with_backoff(
            operation=mock_operation,
            config=config,
            error_category=ErrorCategory.OPERATION_EXECUTION,
            operation_name="test operation",
        )

        assert success is True
        assert result == "success"
        assert mock_operation.call_count == 3

    def test_retry_with_backoff_all_failures(self):
        """Test retry fails after all attempts"""
        handler = ErrorHandler()

        mock_operation = Mock(side_effect=Exception("Always fails"))
        config = RetryConfig(max_attempts=3, initial_delay=0.1)

        success, result = handler.retry_with_backoff(
            operation=mock_operation,
            config=config,
            error_category=ErrorCategory.OPERATION_EXECUTION,
            operation_name="test operation",
        )

        assert success is False
        assert result is None
        assert mock_operation.call_count == 3
        assert len(handler.error_history) == 4  # 3 attempts + 1 final error

    def test_retry_exponential_backoff_timing(self):
        """Test exponential backoff timing"""
        handler = ErrorHandler()

        call_times = []

        def failing_operation():
            call_times.append(time.time())
            raise Exception("Fail")

        config = RetryConfig(max_attempts=3, initial_delay=0.1, exponential_base=2.0, jitter=False)

        start_time = time.time()
        handler.retry_with_backoff(
            operation=failing_operation,
            config=config,
            error_category=ErrorCategory.OPERATION_EXECUTION,
            operation_name="test operation",
        )

        # Verify exponential backoff occurred
        assert len(call_times) == 3

        # First retry should be after ~0.1s
        # Second retry should be after ~0.2s more
        # Total time should be at least 0.3s
        total_time = time.time() - start_time
        assert total_time >= 0.3

    def test_cleanup_after_failure(self):
        """Test comprehensive cleanup after failure"""
        handler = ErrorHandler()

        # Create mock components
        mock_cluster_instance = Mock()
        mock_cluster_instance.cluster_id = "test-cluster"
        mock_cluster_instance.nodes = [Mock(node_id="node-1", process=Mock(poll=Mock(return_value=None)))]

        mock_chaos_coordinator = Mock()
        mock_cluster_coordinator = Mock()

        result = handler.cleanup_after_failure(
            cluster_instance=mock_cluster_instance,
            chaos_coordinator=mock_chaos_coordinator,
            cluster_coordinator=mock_cluster_coordinator,
        )

        assert result is True
        mock_chaos_coordinator.stop_all_chaos.assert_called_once()
        mock_chaos_coordinator.cleanup_chaos.assert_called_once_with("test-cluster")
        mock_cluster_coordinator.destroy_cluster.assert_called_once_with("test-cluster")

    def test_cleanup_after_failure_with_errors(self):
        """Test cleanup continues despite errors"""
        handler = ErrorHandler()

        mock_cluster_instance = Mock()
        mock_cluster_instance.cluster_id = "test-cluster"
        mock_cluster_instance.nodes = []

        # Make chaos coordinator fail
        mock_chaos_coordinator = Mock()
        mock_chaos_coordinator.stop_all_chaos.side_effect = Exception("Chaos stop failed")

        mock_cluster_coordinator = Mock()

        result = handler.cleanup_after_failure(
            cluster_instance=mock_cluster_instance,
            chaos_coordinator=mock_chaos_coordinator,
            cluster_coordinator=mock_cluster_coordinator,
        )

        # Should still attempt cluster cleanup despite chaos failure
        assert result is False  # Failed due to chaos error
        mock_cluster_coordinator.destroy_cluster.assert_called_once()

    def test_get_error_summary(self):
        """Test getting error summary"""
        handler = ErrorHandler()

        # Add some errors
        handler.error_history.extend(
            [
                ErrorContext(category=ErrorCategory.CLUSTER_CREATION, severity=ErrorSeverity.HIGH, message="Error 1"),
                ErrorContext(category=ErrorCategory.CLUSTER_CREATION, severity=ErrorSeverity.MEDIUM, message="Error 2"),
                ErrorContext(category=ErrorCategory.CHAOS_INJECTION, severity=ErrorSeverity.LOW, message="Error 3"),
            ]
        )

        summary = handler.get_error_summary()

        assert summary["total_errors"] == 3
        assert summary["by_category"]["cluster_creation"] == 2
        assert summary["by_category"]["chaos_injection"] == 1
        assert summary["by_severity"]["high"] == 1
        assert summary["by_severity"]["medium"] == 1
        assert summary["by_severity"]["low"] == 1
        assert len(summary["recent_errors"]) == 3

    def test_clear_history(self):
        """Test clearing error history"""
        handler = ErrorHandler()

        # Add some errors
        handler.error_history.append(
            ErrorContext(category=ErrorCategory.CLUSTER_CREATION, severity=ErrorSeverity.HIGH, message="Error")
        )

        assert len(handler.error_history) == 1

        handler.clear_history()

        assert len(handler.error_history) == 0

    def test_recovery_strategy_operation_execution(self):
        """Test recovery strategy for operation execution"""
        handler = ErrorHandler()

        # Medium severity operation errors should recover
        context = ErrorContext(
            category=ErrorCategory.OPERATION_EXECUTION, severity=ErrorSeverity.MEDIUM, message="Operation failed"
        )

        result = handler._recover_operation_execution(context)
        assert result is True

        # High severity should not recover
        context.severity = ErrorSeverity.HIGH
        result = handler._recover_operation_execution(context)
        assert result is False

    def test_recovery_strategy_chaos_injection(self):
        """Test recovery strategy for chaos injection"""
        handler = ErrorHandler()

        context = ErrorContext(
            category=ErrorCategory.CHAOS_INJECTION, severity=ErrorSeverity.MEDIUM, message="Chaos injection failed"
        )

        # Chaos injection failures should always recover (graceful degradation)
        result = handler._recover_chaos_injection(context)
        assert result is True


class TestGlobalErrorHandler:
    """Test global error handler instance"""

    def test_get_error_handler_singleton(self):
        """Test global error handler is singleton"""
        handler1 = get_error_handler()
        handler2 = get_error_handler()

        assert handler1 is handler2

    def test_get_error_handler_returns_instance(self):
        """Test global error handler returns ErrorHandler instance"""
        handler = get_error_handler()

        assert isinstance(handler, ErrorHandler)


class TestErrorHandlerIntegration:
    """Integration tests for error handler"""

    def test_full_retry_workflow(self):
        """Test complete retry workflow with recovery"""
        handler = ErrorHandler()

        attempt_count = 0

        def flaky_operation():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception(f"Attempt {attempt_count} failed")
            return "success"

        config = RetryConfig(max_attempts=5, initial_delay=0.1)

        success, result = handler.retry_with_backoff(
            operation=flaky_operation,
            config=config,
            error_category=ErrorCategory.CLUSTER_FORMATION,
            operation_name="flaky operation",
        )

        assert success is True
        assert result == "success"
        assert attempt_count == 3

        # Check error history
        summary = handler.get_error_summary()
        assert summary["total_errors"] == 2  # 2 failed attempts

    def test_error_context_with_metadata(self):
        """Test error context with metadata"""
        handler = ErrorHandler()

        context = ErrorContext(
            category=ErrorCategory.OPERATION_EXECUTION,
            severity=ErrorSeverity.MEDIUM,
            message="Operation failed",
            cluster_id="test-cluster",
            node_id="node-1",
            operation_id="op-123",
            metadata={"operation_type": "failover", "target": "primary-1", "attempt": 2},
        )

        handler.handle_error(context)

        assert len(handler.error_history) == 1
        stored_context = handler.error_history[0]
        assert stored_context.metadata["operation_type"] == "failover"
        assert stored_context.metadata["attempt"] == 2
