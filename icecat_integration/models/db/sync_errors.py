"""Sync errors database model for error retry mechanism.

- Logs all products with errors (with datetime)
- After normal processing, retry yesterday's problematic products
"""

from sqlalchemy import BigInteger, Boolean, Column, Index, Integer, String, Text, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class SyncErrors(Base):
    """
    Track products that failed during sync for retry mechanism.
    Products with errors are logged here and retried in subsequent runs.
    """

    __tablename__ = "sync_errors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(
        BigInteger,
        nullable=True,
        comment="Icecat Product ID (if known)",
    )
    brand = Column(
        String(100),
        nullable=True,
        comment="Brand name from assortment",
    )
    mpn = Column(
        String(100),
        nullable=True,
        comment="Manufacturer Part Number from assortment",
    )
    ean = Column(
        String(50),
        nullable=True,
        comment="EAN/UPC if available",
    )
    error_message = Column(
        Text,
        nullable=False,
        comment="Error message/description",
    )
    error_type = Column(
        String(50),
        nullable=True,
        comment="Error type classification (api_error, parse_error, db_error, etc.)",
    )
    error_datetime = Column(
        TIMESTAMP,
        nullable=False,
        server_default=func.current_timestamp(),
        comment="When the error occurred",
    )
    retry_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of retry attempts",
    )
    last_retry_at = Column(
        TIMESTAMP,
        nullable=True,
        comment="When the last retry was attempted",
    )
    resolved = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="True if error has been resolved",
    )
    resolved_at = Column(
        TIMESTAMP,
        nullable=True,
        comment="When the error was resolved",
    )
    sync_run_id = Column(
        String(36),
        nullable=True,
        comment="UUID of the sync run that caused the error",
    )
    sequence_number = Column(
        BigInteger,
        nullable=True,
        comment="Delta sequence number if applicable",
    )

    __table_args__ = (
        Index("idx_error_product", "product_id"),
        Index("idx_error_brand_mpn", "brand", "mpn"),
        Index("idx_error_resolved", "resolved"),
        Index("idx_error_datetime", "error_datetime"),
        Index("idx_error_retry", "resolved", "retry_count"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<SyncErrors(id={self.id}, brand={self.brand}, mpn={self.mpn}, resolved={self.resolved})>"
