"""Delta sync tracking database models.

- Delta_SYS_sequence: Track importer executions with sequence numbers
- Delta_SYS_product_sequence: Track which products processed per execution
- Delta_SYS_deletion_prodlocids: Products deleted in current delta run
- Delta_SYS_prodlocaleids_full: Products imported during FULL execution
"""

from sqlalchemy import Column, DateTime, Index, Integer, String, TIMESTAMP
from sqlalchemy.sql import func

from .base import Base


class DeltaSysSequence(Base):
    """
    Track importer execution sequences.
    Each row represents one sync run with a unique sequence number.
    """

    __tablename__ = "Delta_SYS_sequence"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sequencenumber = Column(
        Integer,
        nullable=False,
        unique=True,
        comment="Unique sequence number for this execution",
    )
    mode = Column(
        String(50),
        nullable=False,
        default="delta",
        comment="Run mode: 'full' or 'delta'",
    )
    starttime = Column(
        DateTime,
        nullable=False,
        comment="Execution start time",
    )
    endtime = Column(DateTime, nullable=False, comment="Execution completion time")
    exportstatus = Column(
        String(50),
        nullable=False,
        default="running",
        comment="Export status: running/completed/failed",
    )
    productcount = Column(
        Integer, nullable=False, default=0, comment="Count of products processed"
    )
    products_created = Column(
        Integer, nullable=False, default=0, comment="Count of products created"
    )
    products_updated = Column(
        Integer, nullable=False, default=0, comment="Count of products updated"
    )
    products_deleted = Column(
        Integer, nullable=False, default=0, comment="Count of products deleted"
    )
    products_errored = Column(
        Integer, nullable=False, default=0, comment="Count of products with errors"
    )

    __table_args__ = (
        Index("idx_sequence_number", "sequencenumber"),
        Index("idx_mode", "mode"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeltaSysSequence(seq={self.sequencenumber}, mode={self.mode})>"


class DeltaSysProductSequence(Base):
    """
    Track which products were processed in each execution.
    Links products to their processing sequence.
    """

    __tablename__ = "Delta_SYS_product_sequence"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sequencenumber = Column(
        Integer,
        nullable=False,
        comment="Reference to Delta_SYS_sequence",
    )
    productid = Column(
        Integer,
        nullable=False,
        comment="Icecat Product ID",
    )
    categoryid = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Icecat Category ID",
    )
    localeid = Column(
        Integer,
        nullable=False,
        comment="Locale/Language ID",
    )
    action = Column(
        String(10),
        nullable=False,
        comment="Action: create/update/delete/skip",
    )
    processed_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        comment="When this product was processed",
    )

    __table_args__ = (
        Index("idx_sequence", "sequencenumber"),
        Index("idx_product_locale", "productid", "localeid"),
        Index("idx_seq_product", "sequencenumber", "productid"),
        Index("idx_categoryid", "categoryid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeltaSysProductSequence(seq={self.sequencenumber}, pid={self.productid})>"


class DeltaSysDeletionProdLocIds(Base):
    """
    Track products deleted in the current delta run.
    Used to verify deletions and for audit trail.
    """

    __tablename__ = "Delta_SYS_deletion_prodlocids"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sequencenumber = Column(
        Integer,
        nullable=False,
        comment="Reference to Delta_SYS_sequence",
    )
    productid = Column(
        Integer,
        nullable=False,
        comment="Deleted Icecat Product ID",
    )
    localeid = Column(
        Integer,
        nullable=False,
        comment="Locale/Language ID",
    )
    deleted_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        comment="When this product was deleted",
    )
    reason = Column(
        String(100),
        nullable=True,
        comment="Reason for deletion (e.g., not in assortment)",
    )

    __table_args__ = (
        Index("idx_del_sequence", "sequencenumber"),
        Index("idx_del_product", "productid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeltaSysDeletionProdLocIds(pid={self.productid}, locale={self.localeid})>"


class DeltaSysProdLocaleIdsFull(Base):
    """
    Track products imported during FULL execution.
    Used to verify full sync completeness.
    """

    __tablename__ = "Delta_SYS_prodlocaleids_full"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sequencenumber = Column(
        Integer,
        nullable=False,
        comment="Reference to Delta_SYS_sequence for full run",
    )
    productid = Column(
        Integer,
        nullable=False,
        comment="Icecat Product ID",
    )
    localeid = Column(
        Integer,
        nullable=False,
        comment="Locale/Language ID",
    )
    imported_at = Column(
        TIMESTAMP,
        nullable=True,
        server_default=func.current_timestamp(),
        comment="When this product was imported",
    )
    was_created = Column(
        Integer,
        nullable=False,
        default=0,
        comment="1 if created new, 0 if updated existing",
    )

    __table_args__ = (
        Index("idx_full_sequence", "sequencenumber"),
        Index("idx_full_product_locale", "productid", "localeid"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return f"<DeltaSysProdLocaleIdsFull(pid={self.productid}, locale={self.localeid})>"
