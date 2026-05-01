"""
Operation Log Buffer - Buffers logs per operation
"""

import logging
import threading

logger = logging.getLogger()

_flush_lock = threading.Lock()


class OperationLogBuffer:
    """Buffers logs for a single operation and flushes atomically"""

    def __init__(self, operation_id: str):
        self.operation_id = operation_id
        self.buffer: list[tuple] = []  # (level, message)

    def info(self, msg: str):
        """Buffer an INFO level log"""
        self.buffer.append(("INFO", msg))

    def debug(self, msg: str):
        """Buffer a DEBUG level log"""
        self.buffer.append(("DEBUG", msg))

    def warning(self, msg: str):
        """Buffer a WARNING level log"""
        self.buffer.append(("WARNING", msg))

    def error(self, msg: str):
        """Buffer an ERROR level log"""
        self.buffer.append(("ERROR", msg))

    def flush(self):
        """Flush all buffered logs atomically to global logger"""
        with _flush_lock:
            if not self.buffer:
                return

            logger.info(f"=== OPERATION {self.operation_id} ===")

            for level, msg in self.buffer:
                if level == "INFO":
                    logger.info(msg)
                elif level == "DEBUG":
                    logger.debug(msg)
                elif level == "WARNING":
                    logger.warning(msg)
                elif level == "ERROR":
                    logger.error(msg)

            logger.info(f"=== END OPERATION {self.operation_id} ===")
            logger.info("")
