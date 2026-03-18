"""Structured logging utilities for sync operations."""

import logging
import time
from contextlib import contextmanager
from typing import Any, Generator

from ..repositories.log_repository import LogRepository
from ..models.db.sync_log import LogLevel, LogType

# Numeric ordering for level comparison
_LEVEL_ORDER: dict[LogLevel, int] = {
    LogLevel.DEBUG: 0,
    LogLevel.INFO: 1,
    LogLevel.WARNING: 2,
    LogLevel.ERROR: 3,
    LogLevel.CRITICAL: 4,
}


class SyncLogger:
    """
    Structured logger for sync operations.

    Logs to both:
    - Standard Python logging (console/file)
    - Database sync_log table (for detailed diagnostics)
    """

    def __init__(
        self,
        sync_run_id: str,
        log_repository: LogRepository | None = None,
        logger_name: str = "icecat_sync",
        db_log_level: LogLevel = LogLevel.ERROR,
    ):
        """
        Initialize sync logger.

        Args:
            sync_run_id: UUID of the current sync run
            log_repository: Optional repository for database logging
            logger_name: Name for the Python logger
            db_log_level: Minimum level for writing to sync_log table.
                          Default ERROR = only errors go to DB.
                          Set to INFO to log everything (verbose).
        """
        self.sync_run_id = sync_run_id
        self.log_repository = log_repository
        self.logger = logging.getLogger(logger_name)
        self.db_log_level = db_log_level

    def _log_to_db(
        self,
        log_type: LogType,
        message: str,
        level: LogLevel = LogLevel.INFO,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        api_endpoint: str | None = None,
        api_response_code: int | None = None,
        api_response_body: str | None = None,
        duration_ms: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log to database if repository is available."""
        if self.log_repository is None:
            return

        # Always log lifecycle events (START/END); filter the rest by level
        if log_type not in (LogType.START, LogType.END):
            if _LEVEL_ORDER.get(level, 0) < _LEVEL_ORDER.get(self.db_log_level, 0):
                return

        try:
            if log_type == LogType.START:
                self.log_repository.log_start(
                    self.sync_run_id, message, extra_data
                )
            elif log_type == LogType.PROGRESS:
                self.log_repository.log_progress(
                    self.sync_run_id, message, extra_data
                )
            elif log_type == LogType.API_CALL:
                self.log_repository.log_api_call(
                    sync_run_id=self.sync_run_id,
                    endpoint=api_endpoint or "",
                    response_code=api_response_code or 0,
                    response_body=api_response_body or "",
                    duration_ms=duration_ms or 0,
                    brand=brand,
                    mpn=mpn,
                    icecat_id=icecat_id,
                    extra_data=extra_data,
                )
            elif log_type == LogType.DB_WRITE:
                self.log_repository.log_db_write(
                    sync_run_id=self.sync_run_id,
                    message=message,
                    brand=brand,
                    mpn=mpn,
                    icecat_id=icecat_id,
                    duration_ms=duration_ms,
                    extra_data=extra_data,
                )
            elif log_type == LogType.ERROR:
                self.log_repository.log_error(
                    sync_run_id=self.sync_run_id,
                    message=message,
                    brand=brand,
                    mpn=mpn,
                    icecat_id=icecat_id,
                    extra_data=extra_data,
                    level=level,
                )
            elif log_type == LogType.END:
                self.log_repository.log_end(
                    self.sync_run_id, message, duration_ms, extra_data
                )
            # Commit on the log repo's own session
            self.log_repository.session.commit()
        except Exception as e:
            # Don't let logging failures break the sync
            self.logger.error(f"Failed to write log to database: {e}")
            try:
                self.log_repository.session.rollback()
            except Exception:
                pass

    def log_start(
        self,
        message: str,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log sync start event."""
        self.logger.info(f"[START] {message}")
        self._log_to_db(LogType.START, message, extra_data=extra_data)

    def log_progress(
        self,
        message: str,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log progress update."""
        self.logger.info(f"[PROGRESS] {message}")
        self._log_to_db(LogType.PROGRESS, message, extra_data=extra_data)

    def log_api_call(
        self,
        endpoint: str,
        response_code: int,
        response_body: str,
        duration_ms: int,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log an API call with full response data."""
        message = f"API call to {endpoint} returned {response_code} in {duration_ms}ms"

        if response_code >= 400:
            self.logger.warning(f"[API] {message}")
        else:
            self.logger.debug(f"[API] {message}")

        self._log_to_db(
            LogType.API_CALL,
            message,
            brand=brand,
            mpn=mpn,
            icecat_id=icecat_id,
            api_endpoint=endpoint,
            api_response_code=response_code,
            api_response_body=response_body,
            duration_ms=duration_ms,
            extra_data=extra_data,
        )

    def log_db_write(
        self,
        message: str,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        duration_ms: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log a database write operation."""
        self.logger.debug(f"[DB] {message}")
        self._log_to_db(
            LogType.DB_WRITE,
            message,
            brand=brand,
            mpn=mpn,
            icecat_id=icecat_id,
            duration_ms=duration_ms,
            extra_data=extra_data,
        )

    def log_error(
        self,
        message: str,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        extra_data: dict[str, Any] | None = None,
        level: LogLevel = LogLevel.ERROR,
    ) -> None:
        """Log an error."""
        if level == LogLevel.CRITICAL:
            self.logger.critical(f"[ERROR] {message}")
        elif level == LogLevel.WARNING:
            self.logger.warning(f"[WARN] {message}")
        else:
            self.logger.error(f"[ERROR] {message}")

        self._log_to_db(
            LogType.ERROR,
            message,
            level=level,
            brand=brand,
            mpn=mpn,
            icecat_id=icecat_id,
            extra_data=extra_data,
        )

    def log_end(
        self,
        message: str,
        duration_ms: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log sync end event."""
        self.logger.info(f"[END] {message}")
        self._log_to_db(LogType.END, message, duration_ms=duration_ms, extra_data=extra_data)

    @contextmanager
    def timed_operation(
        self,
        operation_name: str,
        brand: str | None = None,
        mpn: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        Context manager for timing operations.

        Example:
            with sync_logger.timed_operation("sync_product", brand="HP", mpn="ABC123") as ctx:
                # do work
                ctx["extra"] = {"items_synced": 5}
        """
        context: dict[str, Any] = {}
        start_time = time.perf_counter()

        try:
            yield context
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            self.log_db_write(
                f"Completed {operation_name} in {duration_ms}ms",
                brand=brand,
                mpn=mpn,
                duration_ms=duration_ms,
                extra_data=context.get("extra"),
            )
        except Exception as e:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            self.log_error(
                f"Failed {operation_name} after {duration_ms}ms: {str(e)}",
                brand=brand,
                mpn=mpn,
                extra_data={"error": str(e), "duration_ms": duration_ms},
            )
            raise


class ProgressTracker:
    """
    Track and report sync progress.

    Provides periodic progress updates for long-running operations.
    """

    def __init__(
        self,
        total: int,
        sync_logger: SyncLogger,
        report_interval: int = 100,
    ):
        """
        Initialize progress tracker.

        Args:
            total: Total number of items to process
            sync_logger: Logger for progress updates
            report_interval: Report every N items
        """
        self.total = total
        self.sync_logger = sync_logger
        self.report_interval = report_interval

        self.processed = 0
        self.succeeded = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = time.perf_counter()

    def increment_success(self) -> None:
        """Record a successful item."""
        self.processed += 1
        self.succeeded += 1
        self._check_report()

    def increment_failure(self) -> None:
        """Record a failed item."""
        self.processed += 1
        self.failed += 1
        self._check_report()

    def increment_skipped(self) -> None:
        """Record a skipped item."""
        self.processed += 1
        self.skipped += 1
        self._check_report()

    def _check_report(self) -> None:
        """Check if we should report progress."""
        if self.processed % self.report_interval == 0:
            self.report()

    def report(self) -> None:
        """Report current progress."""
        percentage = (self.processed / self.total * 100) if self.total > 0 else 0
        elapsed = time.perf_counter() - self.start_time

        if self.processed > 0:
            rate = self.processed / elapsed
            eta_seconds = (self.total - self.processed) / rate if rate > 0 else 0
            eta_str = self._format_duration(eta_seconds)
        else:
            eta_str = "calculating..."

        message = (
            f"Processed {self.processed}/{self.total} ({percentage:.1f}%) - "
            f"{self.succeeded} synced, {self.failed} failed, {self.skipped} skipped - "
            f"ETA: {eta_str}"
        )

        self.sync_logger.log_progress(
            message,
            extra_data={
                "processed": self.processed,
                "total": self.total,
                "succeeded": self.succeeded,
                "failed": self.failed,
                "skipped": self.skipped,
                "percentage": percentage,
                "elapsed_seconds": elapsed,
            },
        )

    def final_report(self) -> dict[str, Any]:
        """Generate final progress report."""
        elapsed = time.perf_counter() - self.start_time

        stats = {
            "total": self.total,
            "processed": self.processed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "skipped": self.skipped,
            "elapsed_seconds": elapsed,
            "rate_per_second": self.processed / elapsed if elapsed > 0 else 0,
        }

        message = (
            f"Completed {self.processed}/{self.total} items in {self._format_duration(elapsed)} - "
            f"{self.succeeded} synced, {self.failed} failed, {self.skipped} skipped"
        )

        self.sync_logger.log_progress(message, extra_data=stats)
        return stats

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format duration in human-readable form."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"


def setup_file_logging(
    log_file: str,
    level: int = logging.INFO,
    format_str: str | None = None,
) -> logging.Handler:
    """
    Set up file logging with rotation.

    Args:
        log_file: Path to log file
        level: Logging level
        format_str: Optional custom format string

    Returns:
        The configured file handler
    """
    from logging.handlers import RotatingFileHandler
    from pathlib import Path

    # Ensure log directory exists
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    if format_str is None:
        format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
    )
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(format_str))

    return handler
