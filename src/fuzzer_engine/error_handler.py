"""
Error Handler - Comprehensive error handling and recovery mechanisms

Provides centralized error handling, retry logic with exponential backoff,
and cleanup procedures for all component failure scenarios.
"""
import time
import logging
import random
from typing import Optional, Callable, Any, Dict, List
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger()


class ErrorSeverity(Enum):
    """Severity levels for errors"""
    LOW = "low"  # Non-critical, can continue
    MEDIUM = "medium"  # Degraded operation
    HIGH = "high"  # Critical, requires recovery
    FATAL = "fatal"  # Unrecoverable, must abort


class ErrorCategory(Enum):
    """Categories of errors for targeted handling"""
    CLUSTER_CREATION = "cluster_creation"
    CLUSTER_FORMATION = "cluster_formation"
    CLUSTER_VALIDATION = "cluster_validation"
    OPERATION_EXECUTION = "operation_execution"
    CHAOS_INJECTION = "chaos_injection"
    STATE_VALIDATION = "state_validation"
    RESOURCE_CLEANUP = "resource_cleanup"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    CONFIGURATION = "configuration"


@dataclass
class ErrorContext:
    """Context information for an error"""
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    exception: Optional[Exception] = None
    component: Optional[str] = None
    cluster_id: Optional[str] = None
    node_id: Optional[str] = None
    operation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


class ErrorHandler:
    """
    Centralized error handling and recovery for the Cluster Bus Fuzzer.
    
    Provides:
    - Retry logic with exponential backoff
    - Error categorization and severity assessment
    - Recovery strategies for different error types
    - Cleanup procedures for failed operations
    """
    
    def __init__(self):
        self.error_history: List[ErrorContext] = []
        self.recovery_strategies: Dict[ErrorCategory, Callable] = {}
        self._register_default_strategies()
    
    def _register_default_strategies(self):
        """Register default recovery strategies for error categories"""
        self.recovery_strategies[ErrorCategory.CLUSTER_CREATION] = self._recover_cluster_creation
        self.recovery_strategies[ErrorCategory.CLUSTER_FORMATION] = self._recover_cluster_formation
        self.recovery_strategies[ErrorCategory.OPERATION_EXECUTION] = self._recover_operation_execution
        self.recovery_strategies[ErrorCategory.CHAOS_INJECTION] = self._recover_chaos_injection
        self.recovery_strategies[ErrorCategory.STATE_VALIDATION] = self._recover_state_validation
    
    def handle_error(self, error_context: ErrorContext) -> bool:
        """Handle an error with appropriate recovery strategy"""
        # Log error
        self._log_error(error_context)
        
        # Store in history
        self.error_history.append(error_context)
        
        # Check if error is fatal
        if error_context.severity == ErrorSeverity.FATAL:
            logger.error(f"Fatal error encountered: {error_context.message}")
            return False
        
        # Attempt recovery
        if error_context.category in self.recovery_strategies:
            try:
                recovery_func = self.recovery_strategies[error_context.category]
                return recovery_func(error_context)
            except Exception as e:
                logger.error(f"Recovery strategy failed: {e}")
                return False
        
        # No recovery strategy available
        logger.warning(f"No recovery strategy for {error_context.category}")
        return error_context.severity == ErrorSeverity.LOW
    
    def retry_with_backoff(
        self,
        operation: Callable,
        config: RetryConfig,
        error_category: ErrorCategory,
        operation_name: str = "operation",
        **kwargs
    ) -> tuple[bool, Any]:
        """Execute an operation with retry logic and exponential backoff"""
        last_exception = None
        delay = config.initial_delay
        
        for attempt in range(config.max_attempts):
            try:
                logger.debug(f"Executing {operation_name} (attempt {attempt + 1}/{config.max_attempts})")
                
                result = operation(**kwargs)
                
                logger.info(f"{operation_name} succeeded on attempt {attempt + 1}")
                return True, result
                
            except Exception as e:
                last_exception = e
                logger.warning(f"{operation_name} failed on attempt {attempt + 1}: {e}")
                
                # Record error
                error_context = ErrorContext(
                    category=error_category,
                    severity=ErrorSeverity.MEDIUM if attempt < config.max_attempts - 1 else ErrorSeverity.HIGH,
                    message=f"{operation_name} failed: {e}",
                    exception=e,
                    metadata={'attempt': attempt + 1, 'max_attempts': config.max_attempts}
                )
                self.error_history.append(error_context)
                
                # Check if we should retry
                if attempt < config.max_attempts - 1:
                    # Calculate backoff delay
                    backoff_delay = min(
                        delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    
                    # Add jitter if enabled
                    if config.jitter:
                        backoff_delay *= (0.5 + random.random())
                    
                    logger.info(f"Retrying in {backoff_delay:.2f} seconds")
                    time.sleep(backoff_delay)
        
        # All attempts failed
        logger.error(f"{operation_name} failed after {config.max_attempts} attempts")
        
        error_context = ErrorContext(
            category=error_category,
            severity=ErrorSeverity.HIGH,
            message=f"{operation_name} failed after all retry attempts",
            exception=last_exception,
            metadata={'attempts': config.max_attempts}
        )
        self.error_history.append(error_context)
        
        return False, None
    
    def _recover_cluster_creation(self, error_context: ErrorContext) -> bool:
        """Recovery strategy for cluster creation failures"""
        logger.info("Attempting cluster creation recovery")
        
        # For cluster creation failures, we typically need to:
        # 1. Clean up any partially created resources
        # 2. Release allocated ports
        # 3. Remove temporary directories
        
        # This is handled by the cleanup procedures
        # Return False to indicate retry is needed
        return False
    
    def _recover_cluster_formation(self, error_context: ErrorContext) -> bool:
        """Recovery strategy for cluster formation failures"""
        logger.info("Attempting cluster formation recovery")
        
        # For formation failures:
        # 1. Verify all nodes are running
        # 2. Check network connectivity
        # 3. Retry cluster meet commands
        
        # Return False to indicate retry is needed
        return False
    
    def _recover_operation_execution(self, error_context: ErrorContext) -> bool:
        """Recovery strategy for operation execution failures"""
        logger.info("Attempting operation execution recovery")
        
        # For operation failures:
        # 1. Verify cluster is still healthy
        # 2. Check target node is accessible
        # 3. Continue with graceful degradation
        
        # Return True for graceful degradation (continue with next operation)
        return error_context.severity in [ErrorSeverity.LOW, ErrorSeverity.MEDIUM]
    
    def _recover_chaos_injection(self, error_context: ErrorContext) -> bool:
        """Recovery strategy for chaos injection failures"""
        logger.info("Attempting chaos injection recovery")
        
        # For chaos injection failures:
        # 1. Log the failure
        # 2. Continue without chaos (graceful degradation)
        
        # Chaos injection failures are non-critical
        logger.warning("Continuing without chaos injection")
        return True
    
    def _recover_state_validation(self, error_context: ErrorContext) -> bool:
        """Recovery strategy for state validation failures"""
        logger.info("Attempting state validation recovery")
        
        # For validation failures:
        # 1. Wait for cluster to stabilize
        # 2. Retry validation
        # 3. If persistent, mark as inconclusive
        
        # Return False to indicate retry is needed
        return False
    
    def cleanup_after_failure(
        self,
        cluster_instance=None,
        cluster_connection=None,
        chaos_coordinator=None,
        cluster_coordinator=None
    ) -> bool:
        """Comprehensive cleanup after test failure"""
        logger.info("Starting comprehensive cleanup after failure")
        
        cleanup_success = True
        
        # Stop all chaos injections
        if chaos_coordinator:
            try:
                logger.debug("Stopping all active chaos injections")
                chaos_coordinator.stop_all_chaos()
            except Exception as e:
                logger.error(f"Failed to stop chaos: {e}")
                cleanup_success = False
        
        # Cleanup chaos for cluster
        if chaos_coordinator and cluster_instance:
            try:
                chaos_coordinator.cleanup_chaos(cluster_instance.cluster_id)
            except Exception as e:
                logger.error(f"Failed to cleanup chaos: {e}")
                cleanup_success = False
        
        # Destroy cluster
        if cluster_coordinator and cluster_instance:
            try:
                cluster_coordinator.destroy_cluster(cluster_instance.cluster_id)
            except Exception as e:
                logger.error(f"Failed to destroy cluster: {e}")
                cleanup_success = False
        
        # Force cleanup of any remaining processes
        if cluster_instance:
            try:
                logger.debug("Force cleaning up any remaining processes")
                self._force_cleanup_processes(cluster_instance)
            except Exception as e:
                logger.error(f"Failed to force cleanup processes: {e}")
                cleanup_success = False
        
        if cleanup_success:
            logger.info("Cleanup completed successfully")
        else:
            logger.warning("Cleanup completed with errors")
        
        return cleanup_success
    
    def _force_cleanup_processes(self, cluster_instance):
        """Force cleanup of any remaining cluster processes"""
        for node in cluster_instance.nodes:
            try:
                if node.process and node.process.poll() is None:
                    logger.info(f"Force terminating node {node.node_id} (PID: {node.pid})")
                    node.process.terminate()
                    
                    # Wait briefly for graceful termination
                    try:
                        node.process.wait(timeout=2)
                    except:
                        # Force kill if still running
                        node.process.kill()
                        node.process.wait()
            except Exception as e:
                logger.error(f"Failed to cleanup node {node.node_id}: {e}")
    
    def _log_error(self, error_context: ErrorContext):
        """Log error with appropriate level based on severity"""
        log_message = f"[{error_context.category.value}] {error_context.message}"
        
        if error_context.component:
            log_message = f"[{error_context.component}] {log_message}"
        
        if error_context.cluster_id:
            log_message += f" (cluster: {error_context.cluster_id})"
        
        if error_context.node_id:
            log_message += f" (node: {error_context.node_id})"
        
        if error_context.severity == ErrorSeverity.FATAL:
            logger.critical(log_message)
        elif error_context.severity == ErrorSeverity.HIGH:
            logger.error(log_message)
        elif error_context.severity == ErrorSeverity.MEDIUM:
            logger.warning(log_message)
        else:
            logger.info(log_message)
        
        if error_context.exception and error_context.severity in [ErrorSeverity.HIGH, ErrorSeverity.FATAL]:
            logger.exception(error_context.exception)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors encountered"""
        total_errors = len(self.error_history)
        
        errors_by_category = {}
        errors_by_severity = {}
        
        for error in self.error_history:
            # Count by category
            category = error.category.value
            errors_by_category[category] = errors_by_category.get(category, 0) + 1
            
            # Count by severity
            severity = error.severity.value
            errors_by_severity[severity] = errors_by_severity.get(severity, 0) + 1
        
        return {
            'total_errors': total_errors,
            'by_category': errors_by_category,
            'by_severity': errors_by_severity,
            'recent_errors': [
                {
                    'category': e.category.value,
                    'severity': e.severity.value,
                    'message': e.message
                }
                for e in self.error_history[-10:]  # Last 10 errors
            ]
        }
    
    def clear_history(self):
        """Clear error history"""
        self.error_history.clear()
        logger.info("Error history cleared")


# Global error handler instance
_error_handler = None


def get_error_handler() -> ErrorHandler:
    """Get the global error handler instance"""
    global _error_handler
    if _error_handler is None:
        _error_handler = ErrorHandler()
    return _error_handler
