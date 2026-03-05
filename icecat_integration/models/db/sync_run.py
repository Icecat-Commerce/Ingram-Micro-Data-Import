"""SyncRun model for tracking each sync execution."""

from datetime import datetime, timezone
from enum import Enum as PyEnum
from typing import Any
import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.mysql import JSON

from .base import Base


class RunStatus(PyEnum):
    """Possible states for a sync run."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


class SyncRun(Base):
    """
    Track each sync execution.

    This table maintains a record of each sync run including:
    - Start/end times
    - Progress statistics (products processed, created, updated, etc.)
    - Configuration snapshot used for the run
    - Final status and any error summaries

    Enables:
    - Progress monitoring during long-running syncs
    - Historical reporting on sync performance
    - Resume capability for interrupted syncs
    """

    __tablename__ = "sync_run"

    id = Column(
        String(36),
        primary_key=True,
        comment="UUID",
    )

    # Run state
    status = Column(
        Enum(RunStatus),
        default=RunStatus.RUNNING,
        nullable=False,
    )

    # Timestamps
    started_at = Column(
        DateTime,
        nullable=False,
    )
    ended_at = Column(
        DateTime,
        nullable=True,
    )

    # Progress counters
    total_products = Column(Integer, default=0, nullable=False)
    products_matched = Column(Integer, default=0, nullable=False)
    products_not_found = Column(Integer, default=0, nullable=False)
    products_created = Column(Integer, default=0, nullable=False)
    products_updated = Column(Integer, default=0, nullable=False)
    products_deleted = Column(Integer, default=0, nullable=False)
    products_errored = Column(Integer, default=0, nullable=False)

    # Batch tracking for resume capability
    current_batch = Column(Integer, default=0, nullable=False)
    total_batches = Column(Integer, default=0, nullable=False)

    # Context
    assortment_file = Column(
        String(500),
        nullable=True,
    )
    config_snapshot = Column(
        JSON,
        nullable=True,
        comment="Config used for this run",
    )

    # Error handling
    error_summary = Column(
        Text,
        nullable=True,
    )

    # Indexes
    __table_args__ = (
        Index("idx_status", "status"),
        Index("idx_started", "started_at"),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_unicode_ci"},
    )

    def __repr__(self) -> str:
        return (
            f"<SyncRun(id={self.id[:8]}..., status={self.status.value}, "
            f"total={self.total_products})>"
        )

    @classmethod
    def create_new(
        cls,
        assortment_file: str | None = None,
        config_snapshot: dict[str, Any] | None = None,
    ) -> "SyncRun":
        """Create a new sync run."""
        return cls(
            id=str(uuid.uuid4()),
            status=RunStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
            assortment_file=assortment_file,
            config_snapshot=config_snapshot,
        )

    def mark_completed(self) -> None:
        """Mark run as successfully completed."""
        self.status = RunStatus.COMPLETED
        self.ended_at = datetime.now(timezone.utc)

    def mark_failed(self, error_summary: str) -> None:
        """Mark run as failed."""
        self.status = RunStatus.FAILED
        self.ended_at = datetime.now(timezone.utc)
        self.error_summary = error_summary

    def mark_interrupted(self) -> None:
        """Mark run as interrupted (can be resumed)."""
        self.status = RunStatus.INTERRUPTED
        self.ended_at = datetime.now(timezone.utc)

    def increment_matched(self) -> None:
        """Increment matched counter."""
        self.products_matched += 1

    def increment_not_found(self) -> None:
        """Increment not found counter."""
        self.products_not_found += 1

    def increment_created(self) -> None:
        """Increment created counter."""
        self.products_created += 1

    def increment_updated(self) -> None:
        """Increment updated counter."""
        self.products_updated += 1

    def increment_deleted(self) -> None:
        """Increment deleted counter."""
        self.products_deleted += 1

    def increment_errored(self) -> None:
        """Increment error counter."""
        self.products_errored += 1

    def update_batch_progress(self, current: int, total: int) -> None:
        """Update batch progress for resume capability."""
        self.current_batch = current
        self.total_batches = total

    @property
    def products_processed(self) -> int:
        """Total products processed (success + failures)."""
        return (
            self.products_created
            + self.products_updated
            + self.products_not_found
            + self.products_errored
        )

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.products_processed == 0:
            return 0.0
        success = self.products_created + self.products_updated
        return (success / self.products_processed) * 100

    @property
    def duration_seconds(self) -> float | None:
        """Calculate run duration in seconds."""
        if self.ended_at is None:
            return (datetime.now(timezone.utc) - self.started_at).total_seconds()
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def progress_percentage(self) -> float:
        """Calculate progress as percentage."""
        if self.total_products == 0:
            return 0.0
        return (self.products_processed / self.total_products) * 100

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of the sync run."""
        return {
            "id": self.id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "total_products": self.total_products,
            "products_matched": self.products_matched,
            "products_not_found": self.products_not_found,
            "products_created": self.products_created,
            "products_updated": self.products_updated,
            "products_deleted": self.products_deleted,
            "products_errored": self.products_errored,
            "success_rate": f"{self.success_rate:.1f}%",
            "progress": f"{self.progress_percentage:.1f}%",
            "current_batch": self.current_batch,
            "total_batches": self.total_batches,
        }

    def can_resume(self) -> bool:
        """Check if this run can be resumed."""
        return self.status == RunStatus.INTERRUPTED
