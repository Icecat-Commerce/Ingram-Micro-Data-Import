"""Repository for sync tracking operations."""

from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import func, select, update, and_, or_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session

from .base_repository import BaseRepository
from ..models.db.sync_product import SyncProduct, SyncStatus
from ..models.db.sync_run import SyncRun, RunStatus


class SyncRepository(BaseRepository[SyncProduct]):
    """
    Repository for sync product tracking operations.

    Handles:
    - Sync product state management
    - Batch operations for large-scale processing
    - Status transitions and retry tracking
    """

    def __init__(self, session: Session):
        super().__init__(session, SyncProduct)

    def get_by_brand_mpn(self, brand: str, mpn: str) -> SyncProduct | None:
        """Get sync product by brand and MPN (unique key)."""
        stmt = select(SyncProduct).where(
            and_(SyncProduct.brand == brand, SyncProduct.mpn == mpn)
        )
        return self.session.scalars(stmt).first()

    def get_by_icecat_id(self, icecat_id: int) -> SyncProduct | None:
        """Get sync product by Icecat product ID."""
        return self.get_one_by_filter(icecat_product_id=icecat_id)

    def get_by_pimcore_id(self, pimcore_id: int) -> SyncProduct | None:
        """Get sync product by linked product ID."""
        return self.get_one_by_filter(pimcore_product_id=pimcore_id)

    def get_pending_products(
        self, limit: int | None = None, offset: int = 0
    ) -> Sequence[SyncProduct]:
        """Get products pending sync."""
        stmt = (
            select(SyncProduct)
            .where(SyncProduct.status == SyncStatus.PENDING)
            .offset(offset)
        )
        if limit:
            stmt = stmt.limit(limit)
        return self.session.scalars(stmt).all()

    def get_matched_products(
        self, limit: int | None = None, offset: int = 0
    ) -> Sequence[SyncProduct]:
        """Get products that have been matched but not yet synced."""
        stmt = (
            select(SyncProduct)
            .where(SyncProduct.status == SyncStatus.MATCHED)
            .offset(offset)
        )
        if limit:
            stmt = stmt.limit(limit)
        return self.session.scalars(stmt).all()

    def get_products_for_sync(
        self, mode: str = "delta", limit: int | None = None,
        offset: int = 0,
    ) -> Sequence[SyncProduct]:
        """
        Get products to sync with SQL-level OFFSET/LIMIT.

        Args:
            mode: 'full' returns all products, 'delta' returns only those needing sync
            limit: Maximum number of products to return (applied at SQL level)
            offset: Number of rows to skip (applied at SQL level)
        """
        if mode == "full":
            stmt = select(SyncProduct)
        else:
            stmt = select(SyncProduct).where(
                or_(
                    SyncProduct.status == SyncStatus.PENDING,
                    SyncProduct.status == SyncStatus.MATCHED,
                    and_(
                        SyncProduct.status == SyncStatus.ERROR,
                        SyncProduct.retry_count < 3,
                    ),
                )
            )
        # Deterministic ordering for parallel job slicing
        stmt = stmt.order_by(SyncProduct.id)
        if offset:
            stmt = stmt.offset(offset)
        if limit:
            stmt = stmt.limit(limit)
        return self.session.scalars(stmt).all()

    def count_products_for_sync(self, mode: str = "delta") -> int:
        """Count total products available for sync (without loading them)."""
        if mode == "full":
            stmt = select(func.count()).select_from(SyncProduct)
        else:
            stmt = select(func.count()).select_from(SyncProduct).where(
                or_(
                    SyncProduct.status == SyncStatus.PENDING,
                    SyncProduct.status == SyncStatus.MATCHED,
                    and_(
                        SyncProduct.status == SyncStatus.ERROR,
                        SyncProduct.retry_count < 3,
                    ),
                )
            )
        return self.session.scalar(stmt) or 0

    def get_error_products(
        self, limit: int | None = None, offset: int = 0
    ) -> Sequence[SyncProduct]:
        """Get products with errors."""
        stmt = (
            select(SyncProduct)
            .where(SyncProduct.status == SyncStatus.ERROR)
            .offset(offset)
        )
        if limit:
            stmt = stmt.limit(limit)
        return self.session.scalars(stmt).all()

    def get_synced_products_not_in_assortment(
        self, brand_mpn_pairs: set[tuple[str, str]]
    ) -> Sequence[SyncProduct]:
        """
        Find synced products that are no longer in the assortment.

        Args:
            brand_mpn_pairs: Set of (brand, mpn) tuples currently in assortment

        Returns:
            Products to be marked as deleted
        """
        # Get all synced products
        stmt = select(SyncProduct).where(
            SyncProduct.status.in_([SyncStatus.SYNCED, SyncStatus.MATCHED])
        )
        all_synced = self.session.scalars(stmt).all()

        # Filter to those not in assortment
        return [
            p for p in all_synced if (p.brand, p.mpn) not in brand_mpn_pairs
        ]

    def upsert_from_assortment(
        self, brand: str, mpn: str
    ) -> tuple[SyncProduct, bool]:
        """
        Insert or update sync product from assortment.

        Returns:
            Tuple of (SyncProduct, is_new)
        """
        existing = self.get_by_brand_mpn(brand, mpn)
        if existing:
            # If it was deleted, reactivate it
            if existing.status == SyncStatus.DELETED:
                existing.status = SyncStatus.PENDING
                existing.error_message = None
                existing.retry_count = 0
                self.session.flush()
            return existing, False

        # Create new
        new_product = SyncProduct(
            brand=brand,
            mpn=mpn,
            status=SyncStatus.PENDING,
        )
        self.create(new_product)
        return new_product, True

    def bulk_upsert_from_assortment(
        self, items: list[tuple[str, str]]
    ) -> tuple[int, int]:
        """
        Bulk insert/update from assortment items.

        Args:
            items: List of (brand, mpn) tuples

        Returns:
            Tuple of (new_count, existing_count)
        """
        new_count = 0
        existing_count = 0

        for brand, mpn in items:
            _, is_new = self.upsert_from_assortment(brand, mpn)
            if is_new:
                new_count += 1
            else:
                existing_count += 1

        return new_count, existing_count

    def mark_products_deleted(self, products: Sequence[SyncProduct]) -> int:
        """Mark products as deleted."""
        if not products:
            return 0

        ids = [p.id for p in products]
        stmt = (
            update(SyncProduct)
            .where(SyncProduct.id.in_(ids))
            .values(status=SyncStatus.DELETED)
        )
        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount

    def reset_error_products(self, max_retries: int = 3) -> int:
        """Reset error products that haven't exceeded max retries to pending."""
        stmt = (
            update(SyncProduct)
            .where(
                and_(
                    SyncProduct.status == SyncStatus.ERROR,
                    SyncProduct.retry_count < max_retries,
                )
            )
            .values(status=SyncStatus.PENDING)
        )
        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount

    def bulk_upsert_assortment(self, items: list) -> tuple[int, int]:
        """
        Bulk upsert from assortment items using ON DUPLICATE KEY UPDATE.

        Args:
            items: List of AssortmentItem objects (brand, mpn)

        Returns:
            Tuple of (new_count, existing_count) - approximate
        """
        if not items:
            return 0, 0

        records = [
            {
                "brand": item.brand,
                "mpn": item.mpn,
                "status": SyncStatus.PENDING,
                "retry_count": 0,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
            for item in items
        ]

        stmt = mysql_insert(SyncProduct).values(records)
        stmt = stmt.on_duplicate_key_update(
            updated_at=datetime.now(timezone.utc),
        )
        import time as _time
        for attempt in range(3):
            try:
                result = self.session.execute(stmt)
                self.session.flush()
                break
            except Exception as e:
                if "Lock wait timeout" in str(e) and attempt < 2:
                    self.session.rollback()
                    _time.sleep(5 * (attempt + 1))
                    continue
                raise

        # ON DUPLICATE KEY: rowcount = 1 for insert, 2 for update
        # Approximate: total items - (rows affected - items) = new count
        affected = result.rowcount
        new_count = max(0, 2 * len(records) - affected)
        existing_count = len(records) - new_count
        return new_count, existing_count

    def get_stale_products(self, run_started_at: datetime) -> Sequence[SyncProduct]:
        """
        Get synced/matched products not touched during current assortment load.

        Products whose updated_at is older than the run start time were not
        present in the assortment file (they would have been touched by the
        bulk_upsert_assortment step).
        """
        stmt = select(SyncProduct).where(
            and_(
                SyncProduct.status.in_([SyncStatus.SYNCED, SyncStatus.MATCHED]),
                SyncProduct.updated_at < run_started_at,
            )
        )
        return self.session.scalars(stmt).all()

    def get_status_counts(self) -> dict[str, int]:
        """Get count of products by status."""
        stmt = (
            select(SyncProduct.status, func.count(SyncProduct.id))
            .group_by(SyncProduct.status)
        )
        results = self.session.execute(stmt).all()
        return {status.value: count for status, count in results}


class SyncRunRepository(BaseRepository[SyncRun]):
    """
    Repository for sync run tracking operations.

    Handles:
    - Creating and updating sync runs
    - Finding resumable runs
    - Run statistics
    """

    def __init__(self, session: Session):
        super().__init__(session, SyncRun)

    def get_latest_run(self) -> SyncRun | None:
        """Get the most recent sync run."""
        stmt = select(SyncRun).order_by(SyncRun.started_at.desc()).limit(1)
        return self.session.scalars(stmt).first()

    def get_running_runs(self) -> Sequence[SyncRun]:
        """Get all currently running sync runs."""
        return self.get_by_filter(status=RunStatus.RUNNING)

    def get_resumable_runs(self) -> Sequence[SyncRun]:
        """Get all interrupted runs that can be resumed."""
        return self.get_by_filter(status=RunStatus.INTERRUPTED)

    def get_runs_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> Sequence[SyncRun]:
        """Get runs within a date range."""
        stmt = select(SyncRun).where(
            and_(
                SyncRun.started_at >= start_date,
                SyncRun.started_at <= end_date,
            )
        )
        return self.session.scalars(stmt).all()

    def create_run(
        self,
        assortment_file: str | None = None,
        config_snapshot: dict | None = None,
    ) -> SyncRun:
        """Create a new sync run."""
        run = SyncRun.create_new(
            assortment_file=assortment_file,
            config_snapshot=config_snapshot,
        )
        self.create(run)
        return run

    def mark_any_running_as_interrupted(self) -> int:
        """Mark any running runs as interrupted (for recovery)."""
        stmt = (
            update(SyncRun)
            .where(SyncRun.status == RunStatus.RUNNING)
            .values(
                status=RunStatus.INTERRUPTED,
                ended_at=datetime.now(timezone.utc),
            )
        )
        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount
