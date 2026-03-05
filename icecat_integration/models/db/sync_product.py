"""SyncProduct model for tracking product synchronization state."""

from datetime import datetime, timezone
from enum import Enum as PyEnum

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import relationship

from .base import Base, TimestampMixin


class SyncStatus(PyEnum):
    """Possible sync states for a product."""

    PENDING = "pending"
    MATCHED = "matched"
    NOT_FOUND = "not_found"
    SYNCED = "synced"
    ERROR = "error"
    DELETED = "deleted"


class SyncProduct(Base, TimestampMixin):
    """
    Track product synchronization state.

    This table maintains the relationship between assortment items (Brand + MPN)
    and their corresponding Icecat product records. It tracks:
    - Which products have been matched to Icecat
    - Which products have been synced to the database
    - Error states and retry counts for failed syncs
    - Last modification dates for change detection
    """

    __tablename__ = "sync_product"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Assortment identifiers
    brand = Column(
        String(255),
        nullable=False,
        comment="Brand name from assortment",
    )
    mpn = Column(
        String(255),
        nullable=False,
        comment="MPN/VPN from assortment",
    )
    ean = Column(
        String(50),
        nullable=True,
        comment="EAN/UPC from assortment (if available)",
    )

    # Linked IDs (populated after matching/sync)
    icecat_product_id = Column(
        Integer,
        nullable=True,
        comment="Icecat Product ID (once matched)",
    )
    pimcore_product_id = Column(
        Integer,
        nullable=True,
        comment="FK to product table",
    )

    # Sync state
    status = Column(
        Enum(SyncStatus),
        default=SyncStatus.PENDING,
        nullable=False,
        comment="Current sync status",
    )

    # Timestamps for change detection
    last_icecat_modified = Column(
        DateTime,
        nullable=True,
        comment="Last modified date from Icecat",
    )
    last_sync_at = Column(
        DateTime,
        nullable=True,
        comment="Last successful sync timestamp",
    )

    # Error tracking
    error_message = Column(
        Text,
        nullable=True,
        comment="Last error if status=error",
    )
    retry_count = Column(
        Integer,
        default=0,
        nullable=False,
        comment="Number of retry attempts",
    )

    # Indexes for common queries
    __table_args__ = (
        Index("uk_brand_mpn", "brand", "mpn", unique=True),
        Index("idx_status", "status"),
        Index("idx_icecat_id", "icecat_product_id"),
        Index("idx_last_sync", "last_sync_at"),
        Index("idx_pimcore_id", "pimcore_product_id"),
        {"mysql_charset": "utf8mb4", "mysql_collate": "utf8mb4_unicode_ci"},
    )

    def __repr__(self) -> str:
        return (
            f"<SyncProduct(id={self.id}, brand='{self.brand}', "
            f"mpn='{self.mpn}', status={self.status.value})>"
        )

    def mark_matched(self, icecat_id: int, icecat_modified: datetime | None = None) -> None:
        """Mark product as matched to an Icecat product."""
        self.icecat_product_id = icecat_id
        self.status = SyncStatus.MATCHED
        self.last_icecat_modified = icecat_modified
        self.error_message = None

    def mark_synced(self, pimcore_id: int) -> None:
        """Mark product as successfully synced to database."""
        self.pimcore_product_id = pimcore_id
        self.status = SyncStatus.SYNCED
        self.last_sync_at = datetime.now(timezone.utc)
        self.error_message = None
        self.retry_count = 0

    def mark_not_found(self) -> None:
        """Mark product as not found in Icecat."""
        self.status = SyncStatus.NOT_FOUND
        self.icecat_product_id = None

    def mark_error(self, error_message: str) -> None:
        """Mark product as having an error."""
        self.status = SyncStatus.ERROR
        self.error_message = error_message
        self.retry_count += 1

    def mark_deleted(self) -> None:
        """Mark product as deleted (no longer in assortment)."""
        self.status = SyncStatus.DELETED

    def should_retry(self, max_retries: int = 3) -> bool:
        """Check if product should be retried."""
        return self.status == SyncStatus.ERROR and self.retry_count < max_retries

    def needs_update(self, icecat_modified: datetime | None) -> bool:
        """Check if product needs to be updated based on Icecat modification date."""
        if self.status != SyncStatus.SYNCED:
            return True
        if icecat_modified is None or self.last_icecat_modified is None:
            return True
        return icecat_modified > self.last_icecat_modified
