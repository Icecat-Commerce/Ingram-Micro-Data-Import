"""Repository for delta sync tracking.

- Delta_SYS_sequence: Track importer executions with sequence numbers
- Delta_SYS_product_sequence: Track which products processed per execution
- Delta_SYS_deletion_prodlocids: Products deleted in current delta run
- Delta_SYS_prodlocaleids_full: Products imported during FULL execution
"""

from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from .base_repository import BaseRepository
from ..models.db.delta_sys_models import (
    DeltaSysSequence,
    DeltaSysProductSequence,
    DeltaSysDeletionProdLocIds,
    DeltaSysProdLocaleIdsFull,
)


class DeltaRepository(BaseRepository[DeltaSysSequence]):
    """
    Repository for managing delta sync tracking.

    Provides:
    - Sequence number generation for sync runs
    - Per-product action logging
    - Deletion tracking
    - Full sync product tracking
    """

    def __init__(self, session: Session):
        """Initialize with session."""
        super().__init__(session, DeltaSysSequence)

    def create_sequence(
        self,
        run_type: str = "delta",
    ) -> DeltaSysSequence:
        """
        Create a new sync sequence record.

        Args:
            run_type: Either 'full' or 'delta'

        Returns:
            Created DeltaSysSequence record with unique sequencenumber
        """
        # Get the next sequence number
        max_seq_stmt = select(func.max(DeltaSysSequence.sequencenumber))
        max_seq = self.session.execute(max_seq_stmt).scalar() or 0
        next_seq = max_seq + 1

        sequence = DeltaSysSequence(
            sequencenumber=next_seq,
            mode=run_type,
            starttime=datetime.now(),
            endtime=datetime.now(),
            exportstatus="running",
            productcount=0,
            products_created=0,
            products_updated=0,
            products_deleted=0,
            products_errored=0,
        )
        return self.create(sequence)

    def complete_sequence(
        self,
        sequence: DeltaSysSequence,
        products_processed: int = 0,
        products_created: int = 0,
        products_updated: int = 0,
        products_deleted: int = 0,
        products_errored: int = 0,
        status: str = "completed",
    ) -> DeltaSysSequence:
        """
        Mark a sequence as completed with final counts.

        Args:
            sequence: The DeltaSysSequence record to update
            products_processed: Total products processed
            products_created: New products created
            products_updated: Existing products updated
            products_deleted: Products deleted
            products_errored: Products with errors
            status: Final status (completed/failed)

        Returns:
            Updated DeltaSysSequence record
        """
        sequence.endtime = datetime.now()
        sequence.exportstatus = status
        sequence.productcount = products_processed
        sequence.products_created = products_created
        sequence.products_updated = products_updated
        sequence.products_deleted = products_deleted
        sequence.products_errored = products_errored
        return self.update(sequence)

    def fail_sequence(
        self,
        sequence: DeltaSysSequence,
        products_errored: int = 0,
    ) -> DeltaSysSequence:
        """
        Mark a sequence as failed.

        Args:
            sequence: The DeltaSysSequence record to update
            products_errored: Number of products that errored

        Returns:
            Updated DeltaSysSequence record
        """
        sequence.endtime = datetime.now()
        sequence.exportstatus = "failed"
        sequence.products_errored = products_errored
        return self.update(sequence)

    def log_product_action(
        self,
        sequence_number: int,
        product_id: int,
        locale_id: int,
        action: str,
        category_id: int = 0,
    ) -> DeltaSysProductSequence:
        """
        Log a product processing action.

        Args:
            sequence_number: Reference to Delta_SYS_sequence
            product_id: Icecat Product ID
            locale_id: Locale/Language ID
            action: Action taken (create/update/delete/skip)
            category_id: Icecat Category ID

        Returns:
            Created DeltaSysProductSequence record
        """
        product_seq = DeltaSysProductSequence(
            sequencenumber=sequence_number,
            productid=product_id,
            categoryid=category_id,
            localeid=locale_id,
            action=action,
        )
        self.session.add(product_seq)
        self.session.flush()
        return product_seq

    def log_deletion(
        self,
        sequence_number: int,
        product_id: int,
        locale_id: int,
        reason: str | None = None,
    ) -> DeltaSysDeletionProdLocIds:
        """
        Log a product deletion.

        Args:
            sequence_number: Reference to Delta_SYS_sequence
            product_id: Deleted Icecat Product ID
            locale_id: Locale/Language ID
            reason: Reason for deletion (e.g., 'not in assortment')

        Returns:
            Created DeltaSysDeletionProdLocIds record
        """
        deletion = DeltaSysDeletionProdLocIds(
            sequencenumber=sequence_number,
            productid=product_id,
            localeid=locale_id,
            reason=reason,
        )
        self.session.add(deletion)
        self.session.flush()
        return deletion

    def log_full_import(
        self,
        sequence_number: int,
        product_id: int,
        locale_id: int,
        was_created: bool = False,
    ) -> DeltaSysProdLocaleIdsFull:
        """
        Log a product import during full sync.

        Args:
            sequence_number: Reference to Delta_SYS_sequence for full run
            product_id: Icecat Product ID
            locale_id: Locale/Language ID
            was_created: True if product was newly created

        Returns:
            Created DeltaSysProdLocaleIdsFull record
        """
        full_import = DeltaSysProdLocaleIdsFull(
            sequencenumber=sequence_number,
            productid=product_id,
            localeid=locale_id,
            was_created=1 if was_created else 0,
        )
        self.session.add(full_import)
        self.session.flush()
        return full_import

    def get_latest_sequence(self) -> DeltaSysSequence | None:
        """
        Get the most recent sync sequence.

        Returns:
            Most recent DeltaSysSequence or None
        """
        stmt = select(DeltaSysSequence).order_by(
            DeltaSysSequence.sequencenumber.desc()
        ).limit(1)
        return self.session.scalars(stmt).first()

    def get_sequence_by_number(self, sequence_number: int) -> DeltaSysSequence | None:
        """
        Get a specific sequence by its number.

        Args:
            sequence_number: The sequence number to look up

        Returns:
            DeltaSysSequence or None
        """
        return self.get_one_by_filter(sequencenumber=sequence_number)

    def get_products_in_sequence(
        self,
        sequence_number: int,
        action: str | None = None,
    ) -> Sequence[DeltaSysProductSequence]:
        """
        Get all products processed in a sequence.

        Args:
            sequence_number: The sequence to query
            action: Optional filter by action type

        Returns:
            List of DeltaSysProductSequence records
        """
        stmt = select(DeltaSysProductSequence).where(
            DeltaSysProductSequence.sequencenumber == sequence_number
        )
        if action:
            stmt = stmt.where(DeltaSysProductSequence.action == action)

        return self.session.scalars(stmt).all()

    def get_deletions_in_sequence(
        self,
        sequence_number: int,
    ) -> Sequence[DeltaSysDeletionProdLocIds]:
        """
        Get all deletions in a sequence.

        Args:
            sequence_number: The sequence to query

        Returns:
            List of DeltaSysDeletionProdLocIds records
        """
        stmt = select(DeltaSysDeletionProdLocIds).where(
            DeltaSysDeletionProdLocIds.sequencenumber == sequence_number
        )
        return self.session.scalars(stmt).all()

    def get_sequence_summary(self, sequence_number: int) -> dict[str, Any]:
        """
        Get a summary of actions in a sequence.

        Args:
            sequence_number: The sequence to summarize

        Returns:
            Dictionary with counts by action type
        """
        stmt = select(
            DeltaSysProductSequence.action,
            func.count(DeltaSysProductSequence.id).label("count"),
        ).where(
            DeltaSysProductSequence.sequencenumber == sequence_number
        ).group_by(DeltaSysProductSequence.action)

        results = self.session.execute(stmt).all()

        summary = {
            "create": 0,
            "update": 0,
            "delete": 0,
            "skip": 0,
            "total": 0,
        }

        for action, count in results:
            if action in summary:
                summary[action] = count
            summary["total"] += count

        return summary
