"""Sync orchestrator - main entry point for product synchronization."""

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from ..config import AppConfig
from ..database.connection import DatabaseConnection
from ..mappers.icecat_language_mapper import IcecatLanguageMapper
from ..models.db.sync_product import SyncProduct
from ..models.db.sync_run import SyncRun, RunStatus
from ..repositories.sync_repository import SyncRepository, SyncRunRepository
from ..repositories.log_repository import LogRepository
from ..repositories.delta_repository import DeltaRepository
from ..repositories.supplier_mapping_repository import SupplierMappingRepository
from ..utils.logging_utils import SyncLogger, ProgressTracker
from .assortment_reader import AssortmentReader
from .product_matcher import ProductMatcher
from .product_sync_service import ProductSyncService
from .batch_processor import BatchProcessor, GracefulShutdownHandler

logger = logging.getLogger(__name__)


@dataclass
class SyncRunResult:
    """Result of a complete sync run."""

    run_id: str
    status: str
    total_products: int
    products_matched: int
    products_not_found: int
    products_created: int
    products_updated: int
    products_deleted: int
    products_errored: int
    duration_seconds: float
    success_rate: float


class SyncOrchestrator:
    """
    Main orchestrator for product synchronization.

    Workflow:
    1. Create sync run record
    2. Read assortment file
    3. Update sync_product tracking table
    4. Match products with Icecat FrontOffice API
    5. Sync matched products to database
    6. Handle deletions (products not in assortment)
    7. Update run statistics
    """

    def __init__(
        self,
        config: AppConfig,
        db_manager: DatabaseConnection | None = None,
        delimiter: str | None = None,
        brand_column: str | None = None,
        mpn_column: str | None = None,
    ):
        """
        Initialize the orchestrator.

        Args:
            config: Application configuration
            db_manager: Optional database manager (created if not provided)
            delimiter: File delimiter (None = auto-detect)
            brand_column: Override brand column name
            mpn_column: Override MPN column name
        """
        self.config = config
        self.db_manager = db_manager

        # Assortment reader configuration
        self.delimiter = delimiter
        self.brand_column = brand_column
        self.mpn_column = mpn_column

        # Sync configuration
        self.batch_size = getattr(getattr(config, 'sync', config), 'batch_size', 100)
        self.max_concurrent = getattr(getattr(config, 'sync', config), 'concurrency', 40)

    async def prepare_assortment(
        self,
        assortment_file: str | Path,
        mode: str = "full",
    ) -> None:
        """Download + load assortment into sync_product table (Phases 1-3 only).

        Run this once before parallel sync jobs that use --skip-assortment.
        """
        assortment_file = Path(assortment_file)

        if self.db_manager is None:
            from ..database.connection import init_db
            self.db_manager = init_db(self.config.database)

        log_session = self.db_manager.get_session()

        try:
          with self.db_manager.session() as session:
            sync_repo = SyncRepository(session)
            run_repo = SyncRunRepository(session)
            log_repo = LogRepository(log_session)

            run_repo.mark_any_running_as_interrupted()
            sync_run = run_repo.create_run(
                assortment_file=str(assortment_file),
                config_snapshot=self._get_config_snapshot(),
            )
            session.commit()

            sync_logger = SyncLogger(sync_run_id=sync_run.id, log_repository=log_repo)
            reader = self._create_reader()

            # Phase 1: count rows
            t = time.perf_counter()
            row_count = reader.get_row_count(assortment_file)
            sync_logger.log_progress(
                f"[Phase 1/3] Assortment file: {row_count:,} rows ({time.perf_counter()-t:.1f}s)"
            )
            sync_run.total_products = row_count
            session.commit()

            # Phase 2: load into sync_product
            t = time.perf_counter()
            sync_logger.log_progress("[Phase 2/3] Loading assortment into sync tracking table...")
            new_count, existing_count = await self._update_sync_table(
                session, sync_repo, assortment_file, reader, sync_logger
            )
            dur = time.perf_counter() - t
            total = new_count + existing_count
            sync_run.total_products = total
            session.commit()
            sync_logger.log_progress(
                f"[Phase 2/3] Sync table updated: {new_count:,} new, {existing_count:,} existing "
                f"({dur:.1f}s, {total/dur:,.0f} items/s)"
            )

            # Phase 3: mark stale
            t = time.perf_counter()
            to_delete = sync_repo.get_stale_products(run_started_at=sync_run.started_at)
            sync_logger.log_progress(
                f"[Phase 3/3] Products to delete: {len(to_delete)} ({time.perf_counter()-t:.1f}s)"
            )

            sync_run.mark_completed()
            session.commit()

            logger.info(
                f"prepare-sync done: {total:,} products ready "
                f"({new_count:,} new, {existing_count:,} existing, {len(to_delete)} stale)"
            )
        finally:
            log_session.close()

    async def run_sync(
        self,
        assortment_file: str | Path,
        languages: list[str] | None = None,
        resume_run_id: str | None = None,
        mode: str = "delta",
        max_products: int | None = None,
        start_index: int = 0,
        source: str = "json",
        skip_assortment: bool = False,
    ) -> SyncRunResult:
        """
        Execute full sync workflow.

        Args:
            assortment_file: Path to CSV assortment file
            languages: List of language codes to sync (default: ['EN'])
            resume_run_id: Optional run ID to resume
            mode: 'delta' (daily, process changed products) or
                  'full' (weekend, compare all against database)
            max_products: Optional cap on number of products to process
            start_index: SQL OFFSET for parallel job slicing (default: 0)
            source: 'json' (9 API calls/product) or 'xml' (1 call with lang=INT)
            skip_assortment: Skip Phases 1-3 (use when prepare-sync already loaded data)

        Returns:
            SyncRunResult with final statistics
        """
        assortment_file = Path(assortment_file)
        # Default to all supported languages if not specified
        if languages is None:
            languages = [
                m.short_code for m in IcecatLanguageMapper.get_supported_languages()
            ]

        # Initialize database session
        if self.db_manager is None:
            from ..database.connection import init_db
            self.db_manager = init_db(self.config.database)

        # Separate session for logging so a failed sync transaction
        # never poisons the logger (and vice-versa).
        log_session = self.db_manager.get_session()

        try:
          with self.db_manager.session() as session:
            # Initialize repositories
            sync_repo = SyncRepository(session)
            run_repo = SyncRunRepository(session)
            log_repo = LogRepository(log_session)

            # Create or resume sync run
            if resume_run_id:
                sync_run = run_repo.get_by_id(resume_run_id)
                if not sync_run or not sync_run.can_resume():
                    raise ValueError(f"Cannot resume run {resume_run_id}")
                logger.info(f"Resuming sync run {resume_run_id}")
            else:
                # Mark any interrupted runs
                run_repo.mark_any_running_as_interrupted()

                # Create new run
                sync_run = run_repo.create_run(
                    assortment_file=str(assortment_file),
                    config_snapshot=self._get_config_snapshot(),
                )
                session.commit()
                logger.info(f"Created sync run {sync_run.id}")

            # Initialize sync logger
            sync_logger = SyncLogger(
                sync_run_id=sync_run.id,
                log_repository=log_repo,
            )

            try:
                logger.info(f"Starting sync in {mode.upper()} mode")
                result = await self._execute_sync(
                    session=session,
                    sync_run=sync_run,
                    sync_repo=sync_repo,
                    run_repo=run_repo,
                    sync_logger=sync_logger,
                    assortment_file=assortment_file,
                    languages=languages,
                    mode=mode,
                    max_products=max_products,
                    start_index=start_index,
                    source=source,
                    skip_assortment=skip_assortment,
                )
                return result

            except Exception as e:
                logger.error(f"Sync failed: {e}", exc_info=True)
                sync_run.mark_failed(str(e))
                session.commit()

                return SyncRunResult(
                    run_id=sync_run.id,
                    status="failed",
                    total_products=sync_run.total_products,
                    products_matched=sync_run.products_matched,
                    products_not_found=sync_run.products_not_found,
                    products_created=sync_run.products_created,
                    products_updated=sync_run.products_updated,
                    products_deleted=sync_run.products_deleted,
                    products_errored=sync_run.products_errored,
                    duration_seconds=sync_run.duration_seconds or 0,
                    success_rate=sync_run.success_rate,
                )
        finally:
            log_session.close()

    def _create_reader(self) -> AssortmentReader:
        """Create an AssortmentReader with configured delimiter and column overrides."""
        return AssortmentReader(
            delimiter=self.delimiter,
            brand_column=self.brand_column,
            mpn_column=self.mpn_column,
        )

    async def _execute_sync(
        self,
        session: Session,
        sync_run: SyncRun,
        sync_repo: SyncRepository,
        run_repo: SyncRunRepository,
        sync_logger: SyncLogger,
        assortment_file: Path,
        languages: list[str],
        mode: str = "delta",
        max_products: int | None = None,
        start_index: int = 0,
        source: str = "json",
        skip_assortment: bool = False,
    ) -> SyncRunResult:
        """Execute the actual sync workflow.

        Args:
            mode: 'delta' - only sync products needing sync (changed/new)
                  'full' - sync ALL products in assortment (weekend full run)
            source: 'json' (default, 9 API calls per product) or
                    'xml' (1 call with lang=INT, all locales)
        """
        start_time = time.perf_counter()

        # Initialize delta repository for tracking
        delta_repo = DeltaRepository(session)

        # Create delta sequence for this run
        delta_sequence = delta_repo.create_sequence(run_type=mode)
        session.commit()

        sync_logger.log_start(
            f"Starting sync from {assortment_file}",
            extra_data={
                "languages": languages,
                "mode": mode,
                "sequence_number": delta_sequence.sequencenumber,
            },
        )

        reader = self._create_reader()

        # ── Brand mapping (always loaded from DB, regardless of skip_assortment) ──
        # The assortment file uses distributor brand names (e.g. "COMPAQ", "HP SUPPL",
        # "HEWLETT PACKARD") which may differ from Icecat's canonical name (e.g. "HP").
        # The supplier_mapping table maps these aliases: {symbol_lower → icecat_name}.
        # If a brand has no mapping, the raw assortment name is sent as-is to the
        # Icecat API, which is case-insensitive for vendor names.
        phase_start = time.perf_counter()
        mapping_repo = SupplierMappingRepository(session)
        brand_map = mapping_repo.load_all_mappings()
        phase_dur = time.perf_counter() - phase_start
        if brand_map:
            sync_logger.log_progress(
                f"  Brand mapping: {len(brand_map):,} aliases loaded from DB ({phase_dur:.1f}s)"
            )
        else:
            sync_logger.log_progress(
                "  Brand mapping: no mappings in DB (run 'import-suppliers' first)"
            )

        if skip_assortment:
            # Phases 1-3 skipped — assortment already loaded by prepare-sync job.
            sync_run.total_products = sync_repo.count_products_for_sync(mode=mode)
            session.commit()
            sync_logger.log_progress(
                f"[Phase 1-3/7] SKIPPED (--skip-assortment): "
                f"{sync_run.total_products:,} products already in sync table"
            )
            to_delete = []
        else:
            # ── Phase 1: Count rows (fast line count, no parsing) ──
            phase_start = time.perf_counter()
            row_count = reader.get_row_count(assortment_file)
            phase_dur = time.perf_counter() - phase_start
            sync_logger.log_progress(
                f"[Phase 1/7] Assortment file: {row_count:,} rows ({phase_dur:.1f}s)"
            )

            sync_run.total_products = row_count
            session.commit()

            # ── Phase 2: Update sync_product tracking table ──
            phase_start = time.perf_counter()
            sync_logger.log_progress("[Phase 2/7] Loading assortment into sync tracking table...")
            new_count, existing_count = await self._update_sync_table(
                session, sync_repo, assortment_file, reader, sync_logger
            )
            phase_dur = time.perf_counter() - phase_start
            total_loaded = new_count + existing_count
            rate = total_loaded / phase_dur if phase_dur > 0 else 0
            sync_run.total_products = total_loaded
            session.commit()
            sync_logger.log_progress(
                f"[Phase 2/7] Sync table updated: {new_count:,} new, {existing_count:,} existing "
                f"({phase_dur:.1f}s, {rate:,.0f} items/s)"
            )

            # ── Phase 3: Identify products to delete (SQL-based, no memory load) ──
            phase_start = time.perf_counter()
            to_delete = sync_repo.get_stale_products(run_started_at=sync_run.started_at)
            phase_dur = time.perf_counter() - phase_start
            sync_logger.log_progress(
                f"[Phase 3/7] Products to delete: {len(to_delete)} ({phase_dur:.1f}s)"
            )

        # ── Phase 3.5: Prefilter against Icecat full index ──
        # Download the Icecat product index and mark products that don't
        # exist in Icecat as NOT_FOUND immediately, so we only call the
        # API for products that are actually available.
        phase_start = time.perf_counter()
        try:
            prefiltered = await self._prefilter_against_index(
                session, sync_repo, brand_map, sync_logger
            )
            phase_dur = time.perf_counter() - phase_start
            if prefiltered is not None:
                matched, skipped = prefiltered
                sync_logger.log_progress(
                    f"[Phase 3.5/7] Index prefilter: {matched:,} products exist in Icecat, "
                    f"{skipped:,} skipped (not in Icecat) ({phase_dur:.1f}s)"
                )
                session.commit()
        except Exception as e:
            phase_dur = time.perf_counter() - phase_start
            logger.warning(f"Index prefilter failed ({e}), will try all products via API")
            sync_logger.log_progress(
                f"[Phase 3.5/7] Index prefilter SKIPPED: {e} ({phase_dur:.1f}s)"
            )

        # ── Phase 4: Get products to sync ──
        # After prefilter, use delta mode to skip NOT_FOUND products
        # (they were already marked by the prefilter and don't need API calls)
        fetch_mode = "delta" if mode == "full" else mode
        phase_start = time.perf_counter()
        total_available = sync_repo.count_products_for_sync(mode=fetch_mode)
        products_to_sync = list(sync_repo.get_products_for_sync(
            mode=fetch_mode, offset=start_index, limit=max_products,
        ))

        mode_label = "FULL" if mode == "full" else "DELTA"
        slice_info = f"[{start_index:,}:{start_index + len(products_to_sync):,}] of {total_available:,}"
        sync_logger.log_progress(
            f"[Phase 4/7] {mode_label} MODE: {len(products_to_sync):,} products to process ({slice_info})"
        )

        # ── Phase 5: Match + Sync products (concurrent API, sequential DB) ──
        use_xml = source == "xml"

        # Build language pairs: list of (short_code, lang_id) for all requested languages
        multi_lang = len(languages) > 1
        lang_pairs = []
        for lang_code in languages:
            lid = IcecatLanguageMapper.map_to_icecat_lang_id(short_code=lang_code) or 1
            lang_pairs.append((lang_code, lid))
        lang_primary = languages[0]
        lang_id_primary = lang_pairs[0][1]

        source_label = "XML (lang=INT)" if use_xml else f"JSON ({len(lang_pairs)} languages)"
        sync_logger.log_progress(
            f"[Phase 5/7] Syncing {len(products_to_sync):,} products "
            f"(batch={self.batch_size}, concurrency={self.max_concurrent}, "
            f"source={source_label})..."
        )

        # Initialize API clients based on source
        matcher = None
        xml_fetch = None
        xml_parser = None

        if use_xml:
            from ..api import IcecatXmlProductFetchService
            from ..parsers import XmlProductParser
            xml_fetch = IcecatXmlProductFetchService(self.config.icecat)
            xml_parser = XmlProductParser()
        else:
            matcher = ProductMatcher(
                self.config.icecat,
                max_concurrent=self.max_concurrent,
                sync_logger=sync_logger,
            )

        sync_service = ProductSyncService(
            session=session,
            sync_logger=sync_logger,
            run_id=sync_run.id,
        )

        batch_processor = BatchProcessor(
            batch_size=self.batch_size,
            max_concurrent=self.max_concurrent,
        )

        progress = ProgressTracker(
            total=len(products_to_sync),
            sync_logger=sync_logger,
            report_interval=100,
        )

        from ..repositories.product_repository import ProductRepository
        product_repo = ProductRepository(session)

        phase_start = time.perf_counter()
        products_processed = 0

        with GracefulShutdownHandler(batch_processor):
            for batch_start in range(0, len(products_to_sync), self.batch_size):
                if batch_processor._shutdown_requested:
                    sync_run.mark_interrupted()
                    session.commit()
                    break

                batch = products_to_sync[batch_start:batch_start + self.batch_size]

                # Step 1: Fetch products from API (sequential, one at a time)
                # Icecat throttles at ~50 req/s. Sequential requests at ~20 req/s
                # are well within the safe limit and guarantee 100% accuracy.
                match_results = []
                for sp in batch:
                    mapped_brand = brand_map.get(sp.brand.lower(), sp.brand)
                    try:
                        if use_xml:
                            result = await xml_fetch.fetch_product_xml(mapped_brand, sp.mpn)
                            # Retry once on 429 (rate limit)
                            if result and result.status_code == 429:
                                await asyncio.sleep(2)
                                result = await xml_fetch.fetch_product_xml(mapped_brand, sp.mpn)
                        elif multi_lang:
                            lang_data = {}
                            for sc, lid in lang_pairs:
                                try:
                                    match = await matcher.match_product(mapped_brand, sp.mpn, sc)
                                    if match.found and match.icecat_data:
                                        lang_data[lid] = match.icecat_data
                                except Exception:
                                    pass
                            result = lang_data if lang_data else None
                        else:
                            result = await matcher.match_product(
                                mapped_brand, sp.mpn, lang_primary
                            )
                    except Exception as e:
                        logger.error(f"API error for {sp.brand}/{sp.mpn}: {e}")
                        result = None
                    match_results.append(result)

                # Step 2: Process results — accumulate successes for bulk DB write
                bulk_merged = []
                bulk_sp = []

                for sync_product, match_result in zip(batch, match_results):
                    if batch_processor._shutdown_requested:
                        break

                    try:
                        if match_result is None:
                            sync_product.mark_not_found()
                            sync_run.increment_not_found()
                            sync_run.increment_errored()
                            progress.increment_failure()
                            continue

                        if use_xml:
                            if not match_result.success:
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            merged = xml_parser.parse(match_result.xml_root)
                            if not merged:
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            if merged.get("vendor"):
                                sync_service._ensure_vendor(merged["vendor"])
                            if merged.get("category"):
                                sync_service._ensure_category(merged["category"])

                            icecat_id = merged["product"].get("productid")
                            if icecat_id:
                                sync_product.mark_matched(icecat_id)
                            sync_run.increment_matched()

                            bulk_merged.append(merged)
                            bulk_sp.append(sync_product)

                        elif multi_lang:
                            if not match_result:
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            primary_data = match_result.get(lang_id_primary) or next(iter(match_result.values()))
                            icecat_id = primary_data.get("data", {}).get("GeneralInfo", {}).get("IcecatId")
                            if icecat_id:
                                sync_product.mark_matched(icecat_id)
                            sync_run.increment_matched()

                            sync_result = sync_service.sync_multilang_product(
                                icecat_data_by_lang=match_result,
                                sync_product=sync_product,
                            )
                            if sync_result.success:
                                sync_run.increment_created() if sync_result.is_new else sync_run.increment_updated()
                                progress.increment_success()
                            else:
                                sync_run.increment_errored()
                                progress.increment_failure()

                        else:
                            if not match_result.found:
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            sync_product.mark_matched(match_result.icecat_id)
                            sync_run.increment_matched()

                            sync_result = await sync_service.sync_product(
                                icecat_data=match_result.icecat_data,
                                sync_product=sync_product,
                                language_id=lang_id_primary,
                            )
                            if sync_result.success:
                                sync_run.increment_created() if sync_result.is_new else sync_run.increment_updated()
                                progress.increment_success()
                            else:
                                sync_run.increment_errored()
                                progress.increment_failure()

                    except Exception as e:
                        logger.error(f"Error processing {sync_product.brand}/{sync_product.mpn}: {e}")
                        progress.increment_failure()
                        sync_run.increment_errored()

                # Step 3: Bulk write all accumulated XML products in one transaction
                if bulk_merged:
                    try:
                        product_repo.bulk_sync_many(bulk_merged, run_id=sync_run.id)
                        for sp in bulk_sp:
                            sp.mark_synced(sp.icecat_product_id)
                            sync_run.increment_created()
                            progress.increment_success()
                        logger.info(f"Bulk wrote {len(bulk_merged)} products")
                    except Exception as e:
                        logger.error(f"Bulk write failed ({len(bulk_merged)} products): {e}", exc_info=True)
                        for merged_item, sp in zip(bulk_merged, bulk_sp):
                            try:
                                sync_service.sync_from_merged_dict(merged=merged_item, sync_product=sp)
                                sync_run.increment_created()
                                progress.increment_success()
                            except Exception as e2:
                                sp.mark_error(str(e2))
                                sync_run.increment_errored()
                                progress.increment_failure()

                session.commit()
                products_processed += len(batch)

                if products_processed % max(1000, self.batch_size * 10) < self.batch_size:
                    elapsed = time.perf_counter() - phase_start
                    rate = products_processed / elapsed if elapsed > 0 else 0
                    eta = (len(products_to_sync) - products_processed) / rate if rate > 0 else 0
                    sync_logger.log_progress(
                        f"  Progress: {products_processed:,}/{len(products_to_sync):,} "
                        f"({rate:.0f}/s, ETA {eta / 60:.1f}m)"
                    )

        phase_dur = time.perf_counter() - phase_start
        rate = products_processed / phase_dur if phase_dur > 0 else 0
        sync_logger.log_progress(
            f"[Phase 5/7] Sync complete: {products_processed:,} products in {phase_dur:.1f}s ({rate:.0f}/s)"
        )

        # Close HTTP client pool (release connections)
        if matcher:
            await matcher.fetch_service.close()
        if xml_fetch:
            await xml_fetch.close()

        # ── Phase 6: Deactivate stale products (mark inactive) ──
        if to_delete and not batch_processor._shutdown_requested:
            phase_start = time.perf_counter()
            sync_logger.log_progress(f"[Phase 6/7] Deactivating {len(to_delete)} stale products (isactive=0)...")
            for product in to_delete:
                try:
                    sync_service.deactivate_product(product, reason="not_in_assortment")
                    sync_run.increment_deleted()
                except Exception as e:
                    logger.error(f"Error deactivating product: {e}")
            phase_dur = time.perf_counter() - phase_start
            sync_logger.log_progress(f"[Phase 6/7] Deactivation complete ({phase_dur:.1f}s)")
        else:
            sync_logger.log_progress("[Phase 6/7] No deactivations needed")

        # ── Phase 7: Retry ──
        sync_logger.log_progress("[Phase 7/7] Retry skipped")

        # ── Finalize ──
        progress.final_report()

        delta_repo.complete_sequence(
            sequence=delta_sequence,
            products_processed=sync_run.total_products,
            products_created=sync_run.products_created,
            products_updated=sync_run.products_updated,
            products_deleted=sync_run.products_deleted,
            products_errored=sync_run.products_errored,
            status="completed" if sync_run.status == RunStatus.RUNNING else "failed",
        )

        if sync_run.status == RunStatus.RUNNING:
            sync_run.mark_completed()

        session.commit()

        duration = time.perf_counter() - start_time
        sync_logger.log_end(
            f"Sync completed in {duration:.1f}s",
            duration_ms=int(duration * 1000),
            extra_data=sync_run.get_summary(),
        )

        return SyncRunResult(
            run_id=sync_run.id,
            status=sync_run.status.value,
            total_products=sync_run.total_products,
            products_matched=sync_run.products_matched,
            products_not_found=sync_run.products_not_found,
            products_created=sync_run.products_created,
            products_updated=sync_run.products_updated,
            products_deleted=sync_run.products_deleted,
            products_errored=sync_run.products_errored,
            duration_seconds=duration,
            success_rate=sync_run.success_rate,
        )

    async def _prefilter_against_index(
        self,
        session: Session,
        sync_repo: "SyncRepository",
        brand_map: dict[str, str],
        sync_logger: "SyncLogger",
    ) -> tuple[int, int] | None:
        """
        Download the Icecat full product index and mark products that
        don't exist in Icecat as NOT_FOUND before making any API calls.

        Returns (matched_count, skipped_count) or None if index unavailable.
        """
        import gzip
        import httpx
        from pathlib import Path

        index_url = "https://data.icecat.biz/export/level4/EN/files.index.csv.gz"
        index_path = Path("data/downloads/files.index.csv.gz")
        index_path.parent.mkdir(parents=True, exist_ok=True)

        # Build vendor ID → name mapping from DB
        from ..repositories.product_repository import VendorRepository
        vendor_repo = VendorRepository(session)
        result = session.execute(
            __import__("sqlalchemy").text("SELECT vendorid, LOWER(name) FROM vendor")
        )
        vendor_id_to_name = {r[0]: r[1] for r in result}

        # Download index file
        sync_logger.log_progress("  Downloading Icecat full index...")
        auth = (
            self.config.icecat.front_office_username,
            self.config.icecat.front_office_password,
        )
        async with httpx.AsyncClient(timeout=300) as client:
            async with client.stream("GET", index_url, auth=auth) as resp:
                resp.raise_for_status()
                with open(index_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        f.write(chunk)

        file_size_mb = index_path.stat().st_size / 1e6
        sync_logger.log_progress(f"  Index downloaded: {file_size_mb:.0f} MB")

        # Build lookup set: (vendor_name_lower, mpn_lower)
        icecat_products: set[tuple[str, str]] = set()
        line_count = 0

        with gzip.open(index_path, "rt", errors="replace") as f:
            next(f)  # skip header
            for line in f:
                line_count += 1
                fields = line.split("\t")
                if len(fields) < 9:
                    continue
                supplier_id = int(fields[4]) if fields[4].isdigit() else 0
                prod_id = fields[5].strip()
                m_prod_id = fields[7].strip()

                vendor_name = vendor_id_to_name.get(supplier_id, "")
                if vendor_name and prod_id:
                    icecat_products.add((vendor_name, prod_id.lower()))
                if vendor_name and m_prod_id and m_prod_id != prod_id:
                    icecat_products.add((vendor_name, m_prod_id.lower()))

        sync_logger.log_progress(
            f"  Index parsed: {line_count:,} products, "
            f"{len(icecat_products):,} unique (vendor, mpn) pairs"
        )

        # Match PENDING products against the index using lightweight SQL.
        # Reads only (id, brand, mpn) as raw tuples — no ORM objects in memory.
        from sqlalchemy import text as sa_text

        sync_logger.log_progress("  Matching assortment against index...")
        result = session.execute(sa_text(
            "SELECT id, brand, mpn FROM sync_product WHERE status = 'PENDING'"
        ))

        matched_ids = []
        total = 0
        for row_id, brand, mpn in result:
            total += 1
            brand_mapped = brand_map.get(brand.lower(), brand).lower()
            if (brand_mapped, mpn.lower()) in icecat_products:
                matched_ids.append(row_id)

        skipped = total - len(matched_ids)

        sync_logger.log_progress(
            f"  Prefilter result: {len(matched_ids):,} matched, {skipped:,} not in Icecat"
        )

        # Bulk SQL: mark all PENDING as NOT_FOUND
        session.execute(sa_text(
            "UPDATE sync_product SET status = 'NOT_FOUND' WHERE status = 'PENDING'"
        ))

        # Bulk SQL: reset matched products back to PENDING (in chunks)
        chunk_size = 10000
        for i in range(0, len(matched_ids), chunk_size):
            chunk = matched_ids[i:i + chunk_size]
            placeholders = ",".join(str(pid) for pid in chunk)
            session.execute(sa_text(
                f"UPDATE sync_product SET status = 'PENDING' WHERE id IN ({placeholders})"
            ))

        return len(matched_ids), skipped

    async def _update_sync_table(
        self,
        session: Session,
        sync_repo: SyncRepository,
        assortment_file: Path,
        reader: AssortmentReader,
        sync_logger: SyncLogger | None = None,
    ) -> tuple[int, int]:
        """Update sync_product table from assortment file using batch upserts."""
        new_count = 0
        existing_count = 0
        batch = []
        BATCH_SIZE = 5000

        for item in reader.read_csv(assortment_file):
            batch.append(item)

            if len(batch) >= BATCH_SIZE:
                n, e = sync_repo.bulk_upsert_assortment(batch)
                new_count += n
                existing_count += e
                batch = []
                session.commit()

                total = new_count + existing_count
                if total % 100_000 < BATCH_SIZE:
                    if sync_logger:
                        sync_logger.log_progress(
                            f"  Assortment loading: {total:,} items..."
                        )

        # Flush remaining
        if batch:
            n, e = sync_repo.bulk_upsert_assortment(batch)
            new_count += n
            existing_count += e
            session.commit()

        return new_count, existing_count

    async def _sync_single_product(
        self,
        sync_product: SyncProduct,
        matcher: ProductMatcher,
        sync_service: ProductSyncService,
        sync_run: SyncRun,
        session: Session,
        languages: list[str],
        delta_repo: DeltaRepository | None = None,
        delta_sequence: Any | None = None,
        mode: str = "delta",
        brand_map: dict[str, str] | None = None,
    ) -> bool:
        """
        Sync a single product through the full workflow.

        Supports both single-language and multi-language modes.
        When len(languages) > 1, fetches all languages and uses sync_multilang_product().

        Returns True on success, False on failure.
        """
        multi_lang = len(languages) > 1

        # Step 1: Match with Icecat (resolve brand via mapping)
        mapped_brand = sync_product.brand
        if brand_map:
            mapped_brand = brand_map.get(sync_product.brand.lower(), sync_product.brand)
            if mapped_brand != sync_product.brand:
                logger.debug(f"Brand mapped: {sync_product.brand} → {mapped_brand}")

        # Get primary language ID
        lang_id_primary = IcecatLanguageMapper.map_to_icecat_lang_id(short_code=languages[0]) or 1

        # First, check if product exists in primary language
        match_result = await matcher.match_product(
            mapped_brand,
            sync_product.mpn,
            languages[0],
        )

        if not match_result.found:
            sync_product.mark_not_found()
            sync_run.increment_not_found()
            # 404 is a definitive response — don't log to sync_errors (no retry)

            session.commit()
            return False

        # Update sync product with Icecat ID
        sync_product.mark_matched(match_result.icecat_id)
        sync_run.increment_matched()
        session.commit()

        # Step 2: Sync to database
        if multi_lang:
            # Fetch remaining languages
            icecat_data_by_lang: dict[int, dict[str, Any]] = {
                lang_id_primary: match_result.icecat_data,
            }
            for lang_code in languages[1:]:
                lid = IcecatLanguageMapper.map_to_icecat_lang_id(short_code=lang_code) or 1
                try:
                    lang_match = await matcher.match_product(mapped_brand, sync_product.mpn, lang_code)
                    if lang_match.found and lang_match.icecat_data:
                        icecat_data_by_lang[lid] = lang_match.icecat_data
                except Exception as e:
                    logger.debug(f"Lang {lang_code} failed for {sync_product.brand}/{sync_product.mpn}: {e}")

            sync_result = sync_service.sync_multilang_product(
                icecat_data_by_lang=icecat_data_by_lang,
                sync_product=sync_product,
            )
        else:
            sync_result = await sync_service.sync_product(
                icecat_data=match_result.icecat_data,
                sync_product=sync_product,
                language_id=lang_id_primary,
            )

        if sync_result.success:
            if sync_result.is_new:
                sync_run.increment_created()
            else:
                sync_run.increment_updated()

            if delta_repo and delta_sequence:
                try:
                    action = "create" if sync_result.is_new else "update"
                    delta_repo.log_product_action(
                        sequence_number=delta_sequence.sequencenumber,
                        product_id=sync_result.productid or 0,
                        locale_id=lang_id_primary,
                        action=action,
                        category_id=sync_result.categoryid or 0,
                    )

                    if mode == "full":
                        delta_repo.log_full_import(
                            sequence_number=delta_sequence.sequencenumber,
                            product_id=sync_result.productid or 0,
                            locale_id=lang_id_primary,
                            was_created=sync_result.is_new,
                        )
                except Exception as e:
                    logger.warning(f"Failed to log delta action: {e}")

            return True
        else:
            sync_run.increment_errored()
            return False

    async def _sync_single_product_xml(
        self,
        sync_product: SyncProduct,
        sync_service: ProductSyncService,
        sync_run: SyncRun,
        session: Session,
        brand_map: dict[str, str] | None = None,
    ) -> bool:
        """Sync a single product via XML endpoint (lang=INT, all locales in one call)."""
        from ..api import IcecatXmlProductFetchService
        from ..parsers import XmlProductParser

        mapped_brand = sync_product.brand
        if brand_map:
            mapped_brand = brand_map.get(sync_product.brand.lower(), sync_product.brand)

        xml_fetch = IcecatXmlProductFetchService(self.config.icecat)
        xml_parser = XmlProductParser()

        try:
            xml_result = await xml_fetch.fetch_product_xml(mapped_brand, sync_product.mpn)

            if not xml_result.success:
                sync_product.mark_not_found()
                sync_run.increment_not_found()
                session.commit()
                return False

            merged = xml_parser.parse(xml_result.xml_root)
            if not merged:
                sync_product.mark_not_found()
                sync_run.increment_not_found()
                session.commit()
                return False

            icecat_id = merged["product"].get("productid")
            if icecat_id:
                sync_product.mark_matched(icecat_id)
            sync_run.increment_matched()
            session.commit()

            sync_result = sync_service.sync_from_merged_dict(
                merged=merged,
                sync_product=sync_product,
            )

            if sync_result.success:
                if sync_result.is_new:
                    sync_run.increment_created()
                else:
                    sync_run.increment_updated()
                return True
            else:
                sync_run.increment_errored()
                return False

        except Exception as e:
            logger.error(f"XML sync error for {sync_product.brand}/{sync_product.mpn}: {e}")
            sync_run.increment_errored()
            return False
        finally:
            await xml_fetch.close()

    async def sync_single_product(
        self,
        brand: str,
        mpn: str,
        language: str = "EN",
        languages: list[str] | None = None,
        source: str = "json",
    ) -> SyncRunResult:
        """
        Sync a single product by Brand + MPN.

        Args:
            brand: Brand name
            mpn: Manufacturer part number
            language: Language code (used if languages is None)
            languages: List of language codes for multi-lang sync
            source: 'json' (default) or 'xml' (single call with lang=INT)

        Returns:
            SyncRunResult (with single product stats)
        """
        if languages is None:
            languages = [language]

        use_xml = source == "xml"

        if self.db_manager is None:
            from ..database.connection import init_db
            self.db_manager = init_db(self.config.database)

        log_session = self.db_manager.get_session()

        try:
          with self.db_manager.session() as session:
            sync_repo = SyncRepository(session)
            run_repo = SyncRunRepository(session)
            log_repo = LogRepository(log_session)

            # Create run
            sync_run = run_repo.create_run()
            sync_run.total_products = 1
            session.commit()

            sync_logger = SyncLogger(sync_run.id, log_repo)

            # Load brand mapping from DB
            mapping_repo = SupplierMappingRepository(session)
            brand_map = mapping_repo.load_all_mappings()

            # Get or create sync product
            sync_product, _ = sync_repo.upsert_from_assortment(brand, mpn)
            session.commit()

            sync_service = ProductSyncService(
                session=session,
                sync_logger=sync_logger,
                run_id=sync_run.id,
            )

            if use_xml:
                # XML path: single call with lang=INT
                success = await self._sync_single_product_xml(
                    sync_product=sync_product,
                    sync_service=sync_service,
                    sync_run=sync_run,
                    session=session,
                    brand_map=brand_map,
                )
            else:
                # JSON path: per-language calls
                matcher = ProductMatcher(
                    self.config.icecat,
                    sync_logger=sync_logger,
                )

                success = await self._sync_single_product(
                    sync_product=sync_product,
                    matcher=matcher,
                    sync_service=sync_service,
                    sync_run=sync_run,
                    session=session,
                    languages=languages,
                    brand_map=brand_map,
                )

            sync_run.mark_completed()
            session.commit()

            return SyncRunResult(
                run_id=sync_run.id,
                status="completed" if success else "failed",
                total_products=1,
                products_matched=sync_run.products_matched,
                products_not_found=sync_run.products_not_found,
                products_created=sync_run.products_created,
                products_updated=sync_run.products_updated,
                products_deleted=0,
                products_errored=sync_run.products_errored,
                duration_seconds=sync_run.duration_seconds or 0,
                success_rate=100.0 if success else 0.0,
            )
        finally:
            log_session.close()

    def _get_config_snapshot(self) -> dict[str, Any]:
        """Get configuration snapshot for run tracking."""
        return {
            "batch_size": self.batch_size,
            "max_concurrent": self.max_concurrent,
            "database_host": self.config.database.host,
        }
