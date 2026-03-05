"""Repository for sync error tracking and retry mechanism.

- Logs all products with errors (with datetime)
- After normal processing, retry yesterday's problematic products
"""

from datetime import datetime, timedelta
from typing import Any, Sequence

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from .base_repository import BaseRepository
from ..models.db.sync_errors import SyncErrors


class ErrorsRepository(BaseRepository[SyncErrors]):
    """
    Repository for managing sync errors and retry mechanism.

    Provides:
    - Error logging during sync operations
    - Query for errors eligible for retry (yesterday's unresolved)
    - Mark errors as resolved when retry succeeds
    """

    def __init__(self, session: Session):
        """Initialize with session."""
        super().__init__(session, SyncErrors)

    def create_error(
        self,
        error_message: str,
        error_type: str | None = None,
        product_id: int | None = None,
        brand: str | None = None,
        mpn: str | None = None,
        ean: str | None = None,
        sync_run_id: str | None = None,
        sequence_number: int | None = None,
    ) -> SyncErrors:
        """
        Create a new error record.

        Args:
            error_message: Description of the error
            error_type: Classification (api_error, parse_error, db_error, etc.)
            product_id: Icecat Product ID if known
            brand: Brand name from assortment
            mpn: Manufacturer Part Number
            ean: EAN/UPC if available
            sync_run_id: UUID of the sync run
            sequence_number: Delta sequence number if applicable

        Returns:
            Created SyncErrors record
        """
        error = SyncErrors(
            product_id=product_id,
            brand=brand,
            mpn=mpn,
            ean=ean,
            error_message=error_message,
            error_type=error_type,
            sync_run_id=sync_run_id,
            sequence_number=sequence_number,
            retry_count=0,
            resolved=False,
        )
        return self.create(error)

    def get_errors_for_retry(
        self,
        max_retry_count: int = 3,
        hours_ago: int = 24,
    ) -> Sequence[SyncErrors]:
        """
        Get unresolved errors from yesterday that are eligible for retry.

        After normal processing (after deletion step),
        retry yesterday's problematic products.

        Args:
            max_retry_count: Maximum number of retry attempts
            hours_ago: How many hours back to look for errors (default 24)

        Returns:
            List of SyncErrors eligible for retry
        """
        cutoff_time = datetime.now() - timedelta(hours=hours_ago)

        stmt = select(SyncErrors).where(
            and_(
                SyncErrors.resolved == False,
                SyncErrors.retry_count < max_retry_count,
                SyncErrors.error_datetime >= cutoff_time,
                # 404 "not_found" is a definitive response — never retry
                SyncErrors.error_type != "not_found",
            )
        ).order_by(SyncErrors.error_datetime.asc())

        return self.session.scalars(stmt).all()

    def get_unresolved_by_product(self, product_id: int) -> SyncErrors | None:
        """
        Get the most recent unresolved error for a specific product.

        Args:
            product_id: Icecat Product ID

        Returns:
            Most recent unresolved SyncErrors or None
        """
        stmt = select(SyncErrors).where(
            and_(
                SyncErrors.product_id == product_id,
                SyncErrors.resolved == False,
            )
        ).order_by(SyncErrors.error_datetime.desc()).limit(1)

        return self.session.scalars(stmt).first()

    def get_unresolved_by_brand_mpn(
        self,
        brand: str,
        mpn: str,
    ) -> SyncErrors | None:
        """
        Get the most recent unresolved error for a brand/MPN combination.

        Args:
            brand: Brand name
            mpn: Manufacturer Part Number

        Returns:
            Most recent unresolved SyncErrors or None
        """
        stmt = select(SyncErrors).where(
            and_(
                SyncErrors.brand == brand,
                SyncErrors.mpn == mpn,
                SyncErrors.resolved == False,
            )
        ).order_by(SyncErrors.error_datetime.desc()).limit(1)

        return self.session.scalars(stmt).first()

    def mark_resolved(
        self,
        error: SyncErrors,
        resolution_note: str | None = None,
    ) -> SyncErrors:
        """
        Mark an error as resolved.

        Args:
            error: The SyncErrors record to mark resolved
            resolution_note: Optional note about how it was resolved

        Returns:
            Updated SyncErrors record
        """
        error.resolved = True
        error.resolved_at = datetime.now()
        return self.update(error)

    def increment_retry_count(self, error: SyncErrors) -> SyncErrors:
        """
        Increment the retry count and update last_retry_at timestamp.

        Args:
            error: The SyncErrors record to update

        Returns:
            Updated SyncErrors record
        """
        error.retry_count += 1
        error.last_retry_at = datetime.now()
        return self.update(error)

    def get_error_summary(
        self,
        sync_run_id: str | None = None,
        hours_ago: int | None = None,
    ) -> dict[str, Any]:
        """
        Get error statistics for a sync run or time period.

        Args:
            sync_run_id: Filter by specific sync run
            hours_ago: Filter by time period

        Returns:
            Dictionary with error counts by type and status
        """
        from sqlalchemy import func

        base_query = select(
            SyncErrors.error_type,
            SyncErrors.resolved,
            func.count(SyncErrors.id).label("count"),
        ).group_by(SyncErrors.error_type, SyncErrors.resolved)

        conditions = []
        if sync_run_id:
            conditions.append(SyncErrors.sync_run_id == sync_run_id)
        if hours_ago:
            cutoff = datetime.now() - timedelta(hours=hours_ago)
            conditions.append(SyncErrors.error_datetime >= cutoff)

        if conditions:
            base_query = base_query.where(and_(*conditions))

        results = self.session.execute(base_query).all()

        summary = {
            "total": 0,
            "resolved": 0,
            "unresolved": 0,
            "by_type": {},
        }

        for error_type, resolved, count in results:
            summary["total"] += count
            if resolved:
                summary["resolved"] += count
            else:
                summary["unresolved"] += count

            type_key = error_type or "unknown"
            if type_key not in summary["by_type"]:
                summary["by_type"][type_key] = {"total": 0, "resolved": 0, "unresolved": 0}
            summary["by_type"][type_key]["total"] += count
            if resolved:
                summary["by_type"][type_key]["resolved"] += count
            else:
                summary["by_type"][type_key]["unresolved"] += count

        return summary
