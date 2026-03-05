"""Repository for sync logging operations."""

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from sqlalchemy import select, delete, and_
from sqlalchemy.orm import Session

from .base_repository import BaseRepository
from ..models.db.sync_log import SyncLog, LogLevel, LogType


class LogRepository(BaseRepository[SyncLog]):
    """
    Repository for sync log operations.

    Optimized for:
    - High-volume log insertion (async-safe)
    - Efficient log querying and filtering
    - Log cleanup for disk space management
    """

    def __init__(self, session: Session):
        super().__init__(session, SyncLog)

    def log_start(
        self,
        sync_run_id: str,
        message: str,
        extra_data: dict[str, Any] | None = None,
    ) -> SyncLog:
        """Log a sync start event."""
        log = SyncLog.create_start_log(sync_run_id, message, extra_data)
        return self.create(log)

    def log_progress(
        self,
        sync_run_id: str,
        message: str,
        extra_data: dict[str, Any] | None = None,
    ) -> SyncLog:
        """Log a progress update."""
        log = SyncLog.create_progress_log(sync_run_id, message, extra_data)
        return self.create(log)

    def log_api_call(
        self,
        sync_run_id: str,
        endpoint: str,
        response_code: int,
        response_body: str,
        duration_ms: int,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> SyncLog:
        """Log an API call with full response data."""
        log = SyncLog.create_api_log(
            sync_run_id=sync_run_id,
            endpoint=endpoint,
            response_code=response_code,
            response_body=response_body,
            duration_ms=duration_ms,
            brand=brand,
            mpn=mpn,
            icecat_id=icecat_id,
            extra_data=extra_data,
        )
        return self.create(log)

    def log_db_write(
        self,
        sync_run_id: str,
        message: str,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        duration_ms: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> SyncLog:
        """Log a database write operation."""
        log = SyncLog.create_db_log(
            sync_run_id=sync_run_id,
            message=message,
            brand=brand,
            mpn=mpn,
            icecat_id=icecat_id,
            duration_ms=duration_ms,
            extra_data=extra_data,
        )
        return self.create(log)

    def log_error(
        self,
        sync_run_id: str,
        message: str,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        extra_data: dict[str, Any] | None = None,
        level: LogLevel = LogLevel.ERROR,
    ) -> SyncLog:
        """Log an error."""
        log = SyncLog.create_error_log(
            sync_run_id=sync_run_id,
            message=message,
            brand=brand,
            mpn=mpn,
            icecat_id=icecat_id,
            extra_data=extra_data,
            level=level,
        )
        return self.create(log)

    def log_end(
        self,
        sync_run_id: str,
        message: str,
        duration_ms: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> SyncLog:
        """Log a sync end event."""
        log = SyncLog.create_end_log(
            sync_run_id=sync_run_id,
            message=message,
            duration_ms=duration_ms,
            extra_data=extra_data,
        )
        return self.create(log)

    def get_logs_by_run(
        self,
        sync_run_id: str,
        level: LogLevel | None = None,
        log_type: LogType | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Sequence[SyncLog]:
        """Get logs for a specific sync run with optional filtering."""
        conditions = [SyncLog.sync_run_id == sync_run_id]

        if level:
            conditions.append(SyncLog.log_level == level)
        if log_type:
            conditions.append(SyncLog.log_type == log_type)

        stmt = (
            select(SyncLog)
            .where(and_(*conditions))
            .order_by(SyncLog.created_at)
            .offset(offset)
        )
        if limit:
            stmt = stmt.limit(limit)

        return self.session.scalars(stmt).all()

    def get_error_logs_by_run(
        self,
        sync_run_id: str,
        limit: int | None = None,
    ) -> Sequence[SyncLog]:
        """Get error logs for a specific sync run."""
        return self.get_logs_by_run(
            sync_run_id=sync_run_id,
            log_type=LogType.ERROR,
            limit=limit,
        )

    def get_api_logs_by_product(
        self,
        brand: str,
        mpn: str,
        limit: int | None = None,
    ) -> Sequence[SyncLog]:
        """Get API call logs for a specific product."""
        stmt = (
            select(SyncLog)
            .where(
                and_(
                    SyncLog.brand == brand,
                    SyncLog.mpn == mpn,
                    SyncLog.log_type == LogType.API_CALL,
                )
            )
            .order_by(SyncLog.created_at.desc())
        )
        if limit:
            stmt = stmt.limit(limit)

        return self.session.scalars(stmt).all()

    def get_recent_errors(
        self, hours: int = 24, limit: int = 100
    ) -> Sequence[SyncLog]:
        """Get recent error logs."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        stmt = (
            select(SyncLog)
            .where(
                and_(
                    SyncLog.log_type == LogType.ERROR,
                    SyncLog.created_at >= cutoff,
                )
            )
            .order_by(SyncLog.created_at.desc())
            .limit(limit)
        )
        return self.session.scalars(stmt).all()

    def cleanup_old_logs(self, days: int = 30) -> int:
        """
        Delete logs older than specified days.

        Args:
            days: Delete logs older than this many days

        Returns:
            Number of logs deleted
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        stmt = delete(SyncLog).where(SyncLog.created_at < cutoff)
        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount

    def get_log_counts_by_level(self, sync_run_id: str) -> dict[str, int]:
        """Get count of logs by level for a run."""
        from sqlalchemy import func

        stmt = (
            select(SyncLog.log_level, func.count(SyncLog.id))
            .where(SyncLog.sync_run_id == sync_run_id)
            .group_by(SyncLog.log_level)
        )
        results = self.session.execute(stmt).all()
        return {level.value: count for level, count in results}

    def get_log_counts_by_type(self, sync_run_id: str) -> dict[str, int]:
        """Get count of logs by type for a run."""
        from sqlalchemy import func

        stmt = (
            select(SyncLog.log_type, func.count(SyncLog.id))
            .where(SyncLog.sync_run_id == sync_run_id)
            .group_by(SyncLog.log_type)
        )
        results = self.session.execute(stmt).all()
        return {log_type.value: count for log_type, count in results}

    def bulk_insert_logs(self, logs: list[SyncLog]) -> int:
        """Bulk insert logs for performance."""
        if not logs:
            return 0
        self.session.add_all(logs)
        self.session.flush()
        return len(logs)
