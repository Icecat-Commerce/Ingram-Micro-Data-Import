"""SyncLog model for activity logging during sync operations."""

from enum import Enum as PyEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.mysql import JSON, LONGTEXT

from .base import Base


class LogLevel(PyEnum):
    """Log severity levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogType(PyEnum):
    """Types of log entries."""

    START = "START"
    PROGRESS = "PROGRESS"
    API_CALL = "API_CALL"
    DB_WRITE = "DB_WRITE"
    ERROR = "ERROR"
    END = "END"


class SyncLog(Base):
    """
    Activity logging table for sync operations.

    Stores detailed logs of all sync activities including:
    - API calls with full request/response data
    - Database write operations
    - Progress updates
    - Errors and warnings

    This enables full debugging and audit trail for any sync issues.
    """

    __tablename__ = "sync_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Link to sync run
    sync_run_id = Column(
        String(36),
        nullable=False,
        comment="UUID for this sync run",
    )

    # Log metadata
    log_level = Column(
        Enum(LogLevel),
        default=LogLevel.INFO,
        nullable=False,
    )
    log_type = Column(
        Enum(LogType),
        nullable=False,
    )
    message = Column(
        Text,
        nullable=False,
    )

    # Product context (optional)
    brand = Column(String(255), nullable=True)
    mpn = Column(String(255), nullable=True)
    icecat_product_id = Column(Integer, nullable=True)

    # API call details (for API_CALL log type)
    api_endpoint = Column(
        String(500),
        nullable=True,
    )
    api_response_code = Column(
        Integer,
        nullable=True,
    )
    api_response_body = Column(
        LONGTEXT,
        nullable=True,
        comment="Full API response body",
    )

    # Performance metrics
    duration_ms = Column(
        Integer,
        nullable=True,
        comment="Operation duration in milliseconds",
    )

    # Additional structured data
    extra_data = Column(
        JSON,
        nullable=True,
        comment="Additional diagnostic data",
    )

    # Timestamp
    created_at = Column(
        DateTime,
        server_default=func.now(),
        nullable=False,
    )

    # Indexes for common queries
    __table_args__ = (
        Index("idx_sync_run", "sync_run_id"),
        Index("idx_log_level", "log_level"),
        Index("idx_log_type", "log_type"),
        Index("idx_created", "created_at"),
        Index("idx_brand_mpn", "brand", "mpn"),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_unicode_ci"},
    )

    def __repr__(self) -> str:
        return (
            f"<SyncLog(id={self.id}, run={self.sync_run_id[:8]}..., "
            f"level={self.log_level.value}, type={self.log_type.value})>"
        )

    @classmethod
    def create_start_log(
        cls,
        sync_run_id: str,
        message: str,
        extra_data: dict[str, Any] | None = None,
    ) -> "SyncLog":
        """Create a START log entry."""
        return cls(
            sync_run_id=sync_run_id,
            log_level=LogLevel.INFO,
            log_type=LogType.START,
            message=message,
            extra_data=extra_data,
        )

    @classmethod
    def create_progress_log(
        cls,
        sync_run_id: str,
        message: str,
        extra_data: dict[str, Any] | None = None,
    ) -> "SyncLog":
        """Create a PROGRESS log entry."""
        return cls(
            sync_run_id=sync_run_id,
            log_level=LogLevel.INFO,
            log_type=LogType.PROGRESS,
            message=message,
            extra_data=extra_data,
        )

    @classmethod
    def create_api_log(
        cls,
        sync_run_id: str,
        endpoint: str,
        response_code: int,
        response_body: str,
        duration_ms: int,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> "SyncLog":
        """Create an API_CALL log entry."""
        level = LogLevel.INFO if 200 <= response_code < 300 else LogLevel.WARNING
        return cls(
            sync_run_id=sync_run_id,
            log_level=level,
            log_type=LogType.API_CALL,
            message=f"API call to {endpoint} returned {response_code}",
            brand=brand,
            mpn=mpn,
            icecat_product_id=icecat_id,
            api_endpoint=endpoint,
            api_response_code=response_code,
            api_response_body=response_body,
            duration_ms=duration_ms,
            extra_data=extra_data,
        )

    @classmethod
    def create_db_log(
        cls,
        sync_run_id: str,
        message: str,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        duration_ms: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> "SyncLog":
        """Create a DB_WRITE log entry."""
        return cls(
            sync_run_id=sync_run_id,
            log_level=LogLevel.INFO,
            log_type=LogType.DB_WRITE,
            message=message,
            brand=brand,
            mpn=mpn,
            icecat_product_id=icecat_id,
            duration_ms=duration_ms,
            extra_data=extra_data,
        )

    @classmethod
    def create_error_log(
        cls,
        sync_run_id: str,
        message: str,
        brand: str | None = None,
        mpn: str | None = None,
        icecat_id: int | None = None,
        extra_data: dict[str, Any] | None = None,
        level: LogLevel = LogLevel.ERROR,
    ) -> "SyncLog":
        """Create an ERROR log entry."""
        return cls(
            sync_run_id=sync_run_id,
            log_level=level,
            log_type=LogType.ERROR,
            message=message,
            brand=brand,
            mpn=mpn,
            icecat_product_id=icecat_id,
            extra_data=extra_data,
        )

    @classmethod
    def create_end_log(
        cls,
        sync_run_id: str,
        message: str,
        duration_ms: int | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> "SyncLog":
        """Create an END log entry."""
        return cls(
            sync_run_id=sync_run_id,
            log_level=LogLevel.INFO,
            log_type=LogType.END,
            message=message,
            duration_ms=duration_ms,
            extra_data=extra_data,
        )
