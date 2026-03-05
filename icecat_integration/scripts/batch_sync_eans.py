"""
Batch sync script for EAN-based product synchronization.

This script handles:
- Reading EANs from a text file
- Two-phase workflow: CREATE (new products) then UPDATE (existing)
- Detailed logging with run summaries
- Benchmarking metrics (products/sec, timing stats)
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from ..api import IcecatJsonDataFetchService
from ..config import AppConfig
from ..database.connection import DatabaseConnection, init_db
from ..mappers.product_mapper import ProductMapper
from ..repositories.product_repository import (
    ProductRepository,
    VendorRepository,
    CategoryRepository,
)

logger = logging.getLogger(__name__)


@dataclass
class SyncStats:
    """Statistics for a sync run."""

    # Counts
    total_eans: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    api_not_found: int = 0
    api_errors: int = 0
    parse_errors: int = 0
    db_errors: int = 0

    # Timing
    start_time: float = 0.0
    end_time: float = 0.0
    api_times_ms: list[int] = field(default_factory=list)
    db_times_ms: list[int] = field(default_factory=list)

    # Errors
    errors: list[tuple[str, str]] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time if self.end_time else 0.0

    @property
    def products_per_second(self) -> float:
        if self.duration_seconds > 0:
            processed = self.created + self.updated + self.skipped
            return processed / self.duration_seconds
        return 0.0

    @property
    def success_rate(self) -> float:
        total_attempted = self.created + self.updated + self.api_not_found + self.api_errors + self.parse_errors + self.db_errors
        if total_attempted > 0:
            return ((self.created + self.updated) / total_attempted) * 100
        return 0.0

    @property
    def avg_api_time_ms(self) -> float:
        return sum(self.api_times_ms) / len(self.api_times_ms) if self.api_times_ms else 0.0

    @property
    def avg_db_time_ms(self) -> float:
        return sum(self.db_times_ms) / len(self.db_times_ms) if self.db_times_ms else 0.0


@dataclass
class BatchSyncResult:
    """Result of a batch sync operation."""

    run_id: str
    phase: str
    stats: SyncStats
    started_at: datetime
    completed_at: datetime


class EANBatchSyncer:
    """
    Batch synchronizer for EAN-based product sync.

    Implements a two-phase approach:
    1. CREATE phase: Only insert new products (skip existing)
    2. UPDATE phase: Update all products (fetch fresh data)
    """

    def __init__(
        self,
        config: AppConfig,
        db_manager: DatabaseConnection | None = None,
        concurrency: int = 10,
    ):
        self.config = config
        self.db_manager = db_manager
        self.concurrency = concurrency
        self.mapper = ProductMapper()

    def _init_db(self) -> DatabaseConnection:
        """Initialize database if not already done."""
        if self.db_manager is None:
            self.db_manager = init_db(self.config.database)
        return self.db_manager

    def read_eans(self, ean_file: str | Path) -> list[str]:
        """Read EANs from a text file (one per line)."""
        ean_file = Path(ean_file)
        eans = []

        with open(ean_file, "r") as f:
            for line in f:
                ean = line.strip()
                if ean and not ean.startswith("#"):
                    eans.append(ean)

        return eans

    async def run_create_phase(
        self,
        eans: list[str],
        language: str = "EN",
    ) -> SyncStats:
        """
        CREATE phase: Insert new products only.

        Skips products that already exist in the database.
        """
        stats = SyncStats(total_eans=len(eans))
        stats.start_time = time.perf_counter()

        db_manager = self._init_db()
        api_service = IcecatJsonDataFetchService(self.config.icecat)

        logger.info(f"Starting CREATE phase for {len(eans)} EANs")

        with db_manager.session() as session:
            product_repo = ProductRepository(session)
            vendor_repo = VendorRepository(session)
            category_repo = CategoryRepository(session)

            # Process with semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.concurrency)

            async def process_ean(ean: str) -> None:
                async with semaphore:
                    await self._create_product(
                        ean=ean,
                        language=language,
                        api_service=api_service,
                        product_repo=product_repo,
                        vendor_repo=vendor_repo,
                        category_repo=category_repo,
                        session=session,
                        stats=stats,
                    )

            # Process all EANs concurrently
            tasks = [process_ean(ean) for ean in eans]
            await asyncio.gather(*tasks)

            session.commit()

        stats.end_time = time.perf_counter()
        return stats

    async def run_update_phase(
        self,
        eans: list[str],
        language: str = "EN",
    ) -> SyncStats:
        """
        UPDATE phase: Update all products.

        Fetches fresh data and updates all records.
        """
        stats = SyncStats(total_eans=len(eans))
        stats.start_time = time.perf_counter()

        db_manager = self._init_db()
        api_service = IcecatJsonDataFetchService(self.config.icecat)

        logger.info(f"Starting UPDATE phase for {len(eans)} EANs")

        with db_manager.session() as session:
            product_repo = ProductRepository(session)
            vendor_repo = VendorRepository(session)
            category_repo = CategoryRepository(session)

            semaphore = asyncio.Semaphore(self.concurrency)

            async def process_ean(ean: str) -> None:
                async with semaphore:
                    await self._update_product(
                        ean=ean,
                        language=language,
                        api_service=api_service,
                        product_repo=product_repo,
                        vendor_repo=vendor_repo,
                        category_repo=category_repo,
                        session=session,
                        stats=stats,
                    )

            tasks = [process_ean(ean) for ean in eans]
            await asyncio.gather(*tasks)

            session.commit()

        stats.end_time = time.perf_counter()
        return stats

    async def _create_product(
        self,
        ean: str,
        language: str,
        api_service: IcecatJsonDataFetchService,
        product_repo: ProductRepository,
        vendor_repo: VendorRepository,
        category_repo: CategoryRepository,
        session: Any,
        stats: SyncStats,
    ) -> None:
        """Create a single product (skip if exists)."""
        try:
            # Check if product already exists by EAN
            existing = product_repo.get_by_ean(ean)
            if existing:
                stats.skipped += 1
                return

            # Fetch from API
            api_start = time.perf_counter()
            result = await api_service.fetch_product_data_by_ean_async(ean, language)
            api_duration_ms = int((time.perf_counter() - api_start) * 1000)
            stats.api_times_ms.append(api_duration_ms)

            if not result.success:
                if "not found" in (result.error_message or "").lower():
                    stats.api_not_found += 1
                    stats.errors.append((ean, f"Not found in Icecat"))
                else:
                    stats.api_errors += 1
                    stats.errors.append((ean, f"API error: {result.error_message}"))
                return

            # Map the response
            try:
                mapped = self.mapper.map_product_response(result.data, language_id=1)
                if not mapped:
                    stats.parse_errors += 1
                    stats.errors.append((ean, "Failed to map product data"))
                    return
            except Exception as e:
                stats.parse_errors += 1
                stats.errors.append((ean, f"Parse error: {str(e)}"))
                return

            # Save to database
            db_start = time.perf_counter()
            try:
                # Ensure vendor exists
                if mapped.get("vendor"):
                    vendor_data = mapped["vendor"]
                    vendor_repo.get_or_create(
                        vendor_data["vendorid"],
                        vendor_data.get("name", "Unknown"),
                    )

                # Ensure category exists
                if mapped.get("category"):
                    cat_data = mapped["category"]
                    category_repo.get_or_create(
                        cat_data["categoryid"],
                        cat_data.get("categoryname", "Unknown"),
                    )

                # Create product with all related data
                # Attributes skipped: requires attributenames table to be populated first
                product_repo.sync_product_full(
                    product_data=mapped["product"],
                    descriptions=mapped.get("descriptions"),
                    marketing_info=mapped.get("marketing_info"),
                    features=mapped.get("features"),
                    media=mapped.get("media"),
                    attributes=None,  # Requires attributenames table
                )

                session.flush()
                db_duration_ms = int((time.perf_counter() - db_start) * 1000)
                stats.db_times_ms.append(db_duration_ms)
                stats.created += 1

            except Exception as e:
                session.rollback()
                stats.db_errors += 1
                stats.errors.append((ean, f"DB error: {str(e)}"))
                logger.error(f"Database error for EAN {ean}: {e}")

        except Exception as e:
            stats.api_errors += 1
            stats.errors.append((ean, f"Unexpected error: {str(e)}"))
            logger.error(f"Unexpected error for EAN {ean}: {e}")

    async def _update_product(
        self,
        ean: str,
        language: str,
        api_service: IcecatJsonDataFetchService,
        product_repo: ProductRepository,
        vendor_repo: VendorRepository,
        category_repo: CategoryRepository,
        session: Any,
        stats: SyncStats,
    ) -> None:
        """Update a single product (fetch fresh data)."""
        try:
            # Fetch from API
            api_start = time.perf_counter()
            result = await api_service.fetch_product_data_by_ean_async(ean, language)
            api_duration_ms = int((time.perf_counter() - api_start) * 1000)
            stats.api_times_ms.append(api_duration_ms)

            if not result.success:
                if "not found" in (result.error_message or "").lower():
                    stats.api_not_found += 1
                    stats.errors.append((ean, f"Not found in Icecat"))
                else:
                    stats.api_errors += 1
                    stats.errors.append((ean, f"API error: {result.error_message}"))
                return

            # Map the response
            try:
                mapped = self.mapper.map_product_response(result.data, language_id=1)
                if not mapped:
                    stats.parse_errors += 1
                    stats.errors.append((ean, "Failed to map product data"))
                    return
            except Exception as e:
                stats.parse_errors += 1
                stats.errors.append((ean, f"Parse error: {str(e)}"))
                return

            # Save to database (upsert)
            db_start = time.perf_counter()
            try:
                # Ensure vendor exists
                if mapped.get("vendor"):
                    vendor_data = mapped["vendor"]
                    vendor_repo.get_or_create(
                        vendor_data["vendorid"],
                        vendor_data.get("name", "Unknown"),
                    )

                # Ensure category exists
                if mapped.get("category"):
                    cat_data = mapped["category"]
                    category_repo.get_or_create(
                        cat_data["categoryid"],
                        cat_data.get("categoryname", "Unknown"),
                    )

                # Upsert product with all related data
                # Attributes skipped: requires attributenames table to be populated first
                product, is_new = product_repo.sync_product_full(
                    product_data=mapped["product"],
                    descriptions=mapped.get("descriptions"),
                    marketing_info=mapped.get("marketing_info"),
                    features=mapped.get("features"),
                    media=mapped.get("media"),
                    attributes=None,  # Requires attributenames table
                )

                session.flush()
                db_duration_ms = int((time.perf_counter() - db_start) * 1000)
                stats.db_times_ms.append(db_duration_ms)

                if is_new:
                    stats.created += 1
                else:
                    stats.updated += 1

            except Exception as e:
                session.rollback()
                stats.db_errors += 1
                stats.errors.append((ean, f"DB error: {str(e)}"))
                logger.error(f"Database error for EAN {ean}: {e}")

        except Exception as e:
            stats.api_errors += 1
            stats.errors.append((ean, f"Unexpected error: {str(e)}"))
            logger.error(f"Unexpected error for EAN {ean}: {e}")

    async def run_full_sync(
        self,
        eans: list[str],
        language: str = "EN",
    ) -> tuple[SyncStats, SyncStats]:
        """
        Run full sync: CREATE phase followed by UPDATE phase.

        Returns tuple of (create_stats, update_stats).
        """
        logger.info(f"Starting full sync for {len(eans)} EANs")

        # Phase 1: CREATE
        create_stats = await self.run_create_phase(eans, language)
        self._log_phase_summary("CREATE", create_stats)

        # Phase 2: UPDATE
        update_stats = await self.run_update_phase(eans, language)
        self._log_phase_summary("UPDATE", update_stats)

        # Log final summary
        self._log_final_summary(create_stats, update_stats)

        return create_stats, update_stats

    def _log_phase_summary(self, phase: str, stats: SyncStats) -> None:
        """Log summary for a single phase."""
        logger.info(f"""
{'='*60}
PHASE: {phase}
{'='*60}
Duration: {stats.duration_seconds:.1f}s
Products/second: {stats.products_per_second:.2f}

Results:
  Created: {stats.created}
  Updated: {stats.updated}
  Skipped (already exists): {stats.skipped}
  API not found: {stats.api_not_found}
  API errors: {stats.api_errors}
  Parse errors: {stats.parse_errors}
  DB errors: {stats.db_errors}

Timing:
  Avg API response: {stats.avg_api_time_ms:.0f}ms
  Avg DB write: {stats.avg_db_time_ms:.0f}ms
{'='*60}
""")

    def _log_final_summary(self, create_stats: SyncStats, update_stats: SyncStats) -> None:
        """Log final summary combining both phases."""
        total_duration = create_stats.duration_seconds + update_stats.duration_seconds
        total_created = create_stats.created + update_stats.created
        total_updated = create_stats.updated + update_stats.updated
        total_errors = (
            create_stats.api_not_found + create_stats.api_errors +
            create_stats.parse_errors + create_stats.db_errors +
            update_stats.api_not_found + update_stats.api_errors +
            update_stats.parse_errors + update_stats.db_errors
        )

        all_errors = create_stats.errors + update_stats.errors

        summary = f"""
{'='*60}
ICECAT SYNC RUN SUMMARY
{'='*60}
Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Duration: {total_duration:.1f} seconds

PHASE 1: CREATE
  New products created: {create_stats.created}
  Already existed (skipped): {create_stats.skipped}
  API not found: {create_stats.api_not_found}
  Errors: {create_stats.api_errors + create_stats.parse_errors + create_stats.db_errors}

PHASE 2: UPDATE
  Products updated: {update_stats.updated}
  Products created: {update_stats.created}
  API not found: {update_stats.api_not_found}
  Errors: {update_stats.api_errors + update_stats.parse_errors + update_stats.db_errors}

TOTALS
  Products processed: {create_stats.total_eans}
  Total created: {total_created}
  Total updated: {total_updated}
  Success rate: {((total_created + total_updated) / create_stats.total_eans * 100) if create_stats.total_eans > 0 else 0:.1f}%
  Products/second: {create_stats.total_eans / total_duration if total_duration > 0 else 0:.2f}

TIMING
  Avg API response: {(create_stats.avg_api_time_ms + update_stats.avg_api_time_ms) / 2:.0f}ms
  Avg DB write: {(create_stats.avg_db_time_ms + update_stats.avg_db_time_ms) / 2:.0f}ms
"""

        if all_errors:
            summary += f"\nERRORS ({len(all_errors)}):\n"
            for ean, error in all_errors[:20]:  # Show first 20 errors
                summary += f"  - EAN {ean}: {error}\n"
            if len(all_errors) > 20:
                summary += f"  ... and {len(all_errors) - 20} more errors\n"

        summary += f"{'='*60}"

        logger.info(summary)


def generate_summary_report(
    create_stats: SyncStats,
    update_stats: SyncStats,
    output_file: str | Path | None = None,
) -> str:
    """Generate a detailed summary report."""
    total_duration = create_stats.duration_seconds + update_stats.duration_seconds

    report = f"""
============================================================
ICECAT SYNC RUN SUMMARY
============================================================
Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Total Duration: {total_duration:.1f} seconds

PHASE 1: CREATE
  Total EANs: {create_stats.total_eans}
  New products created: {create_stats.created}
  Already existed (skipped): {create_stats.skipped}
  API not found: {create_stats.api_not_found}
  API errors: {create_stats.api_errors}
  Parse errors: {create_stats.parse_errors}
  DB errors: {create_stats.db_errors}
  Duration: {create_stats.duration_seconds:.1f}s
  Products/second: {create_stats.products_per_second:.2f}

PHASE 2: UPDATE
  Total EANs: {update_stats.total_eans}
  Products updated: {update_stats.updated}
  Products created: {update_stats.created}
  API not found: {update_stats.api_not_found}
  API errors: {update_stats.api_errors}
  Parse errors: {update_stats.parse_errors}
  DB errors: {update_stats.db_errors}
  Duration: {update_stats.duration_seconds:.1f}s
  Products/second: {update_stats.products_per_second:.2f}

PERFORMANCE METRICS
  Total products processed: {create_stats.total_eans}
  Overall success rate: {((create_stats.created + update_stats.updated) / create_stats.total_eans * 100) if create_stats.total_eans > 0 else 0:.1f}%
  Overall products/second: {create_stats.total_eans / total_duration if total_duration > 0 else 0:.2f}

  API Response Times:
    CREATE phase avg: {create_stats.avg_api_time_ms:.0f}ms
    UPDATE phase avg: {update_stats.avg_api_time_ms:.0f}ms
    CREATE phase min: {min(create_stats.api_times_ms) if create_stats.api_times_ms else 0}ms
    CREATE phase max: {max(create_stats.api_times_ms) if create_stats.api_times_ms else 0}ms

  DB Write Times:
    CREATE phase avg: {create_stats.avg_db_time_ms:.0f}ms
    UPDATE phase avg: {update_stats.avg_db_time_ms:.0f}ms
"""

    all_errors = create_stats.errors + update_stats.errors
    if all_errors:
        report += f"\nERRORS ({len(all_errors)}):\n"
        for ean, error in all_errors:
            report += f"  - EAN {ean}: {error}\n"

    report += "============================================================\n"

    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(report)
        logger.info(f"Summary report saved to: {output_path}")

    return report
