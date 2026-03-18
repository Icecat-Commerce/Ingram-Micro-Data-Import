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

        # ── Phase 4: Get products to sync ──
        # SQL-level OFFSET/LIMIT for memory-efficient parallel job slicing:
        #   Job 1: --start-index 0      --max-products 385000
        #   Job 2: --start-index 385000  --max-products 385000
        #   Job 3: --start-index 770000  --max-products 385000
        #   Job 4: --start-index 1155000  (no max = process all remaining)
        phase_start = time.perf_counter()
        total_available = sync_repo.count_products_for_sync(mode=mode)
        products_to_sync = list(sync_repo.get_products_for_sync(
            mode=mode, offset=start_index, limit=max_products,
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

        # Single semaphore guards ALL API calls
        api_semaphore = asyncio.Semaphore(self.max_concurrent)

        phase_start = time.perf_counter()
        products_processed = 0

        with GracefulShutdownHandler(batch_processor):
            # Process in batches: concurrent API calls, sequential DB writes
            for batch_start in range(0, len(products_to_sync), self.batch_size):
                if batch_processor._shutdown_requested:
                    sync_run.mark_interrupted()
                    session.commit()
                    break

                batch = products_to_sync[batch_start:batch_start + self.batch_size]

                # Step 1: Concurrent API calls for the entire batch
                if use_xml:
                    # XML path: 1 call per product with lang=INT (all locales)
                    async def _fetch_xml(sp: SyncProduct):
                        async with api_semaphore:
                            try:
                                mapped_brand = brand_map.get(sp.brand.lower(), sp.brand)
                                if mapped_brand != sp.brand:
                                    logger.debug(f"Brand mapped: {sp.brand} → {mapped_brand}")
                                return await xml_fetch.fetch_product_xml(mapped_brand, sp.mpn)
                            except Exception as e:
                                logger.error(f"XML API error for {sp.brand}/{sp.mpn}: {e}")
                                return None

                    match_results = await asyncio.gather(
                        *[_fetch_xml(sp) for sp in batch],
                        return_exceptions=True,
                    )
                elif multi_lang:
                    async def _fetch_all_langs(sp: SyncProduct):
                        mapped_brand = brand_map.get(sp.brand.lower(), sp.brand)
                        if mapped_brand != sp.brand:
                            logger.debug(f"Brand mapped: {sp.brand} → {mapped_brand}")

                        async def _fetch_one_lang(short_code: str, lid: int):
                            async with api_semaphore:
                                try:
                                    match = await matcher.match_product(
                                        mapped_brand, sp.mpn, short_code
                                    )
                                    if match.found and match.icecat_data:
                                        return (lid, match.icecat_data)
                                except Exception as e:
                                    logger.debug(
                                        f"Lang {short_code} failed for {sp.brand}/{sp.mpn}: {e}"
                                    )
                                return None

                        try:
                            lang_results = await asyncio.gather(
                                *[_fetch_one_lang(sc, lid) for sc, lid in lang_pairs],
                                return_exceptions=True
                            )

                            results = {}
                            for result in lang_results:
                                if result and not isinstance(result, Exception):
                                    lid, data = result
                                    results[lid] = data

                            return results if results else None
                        except Exception as e:
                            logger.error(f"API error for {sp.brand}/{sp.mpn}: {e}")
                            return None

                    match_results = await asyncio.gather(
                        *[_fetch_all_langs(sp) for sp in batch],
                        return_exceptions=True,
                    )
                else:
                    # Single-language JSON: each API call guarded by semaphore
                    async def _fetch_one(sp: SyncProduct):
                        async with api_semaphore:
                            try:
                                # Resolve brand (see "Brand mapping" comment above)
                                mapped_brand = brand_map.get(sp.brand.lower(), sp.brand)
                                if mapped_brand != sp.brand:
                                    logger.debug(f"Brand mapped: {sp.brand} → {mapped_brand}")
                                return await matcher.match_product(
                                    mapped_brand, sp.mpn, lang_primary
                                )
                            except Exception as e:
                                logger.error(f"API error for {sp.brand}/{sp.mpn}: {e}")
                                return None

                    match_results = await asyncio.gather(
                        *[_fetch_one(sp) for sp in batch],
                        return_exceptions=True,
                    )

                # Step 2: Sequential DB writes for the batch results
                for sync_product, match_result in zip(batch, match_results):
                    if batch_processor._shutdown_requested:
                        break

                    try:
                        if isinstance(match_result, Exception) or match_result is None:
                            sync_product.mark_not_found()
                            sync_run.increment_not_found()
                            sync_run.increment_errored()
                            progress.increment_failure()
                            continue

                        if use_xml:
                            # XML result is an XmlFetchResult
                            if not match_result.success:
                                # Log first 10 failures for debugging
                                if sync_run.products_not_found < 10:
                                    logger.warning(
                                        f"XML NOT_FOUND {sync_product.brand}/{sync_product.mpn}: "
                                        f"error={match_result.error_message!r}, "
                                        f"status={match_result.status_code}"
                                    )
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            merged = xml_parser.parse(match_result.xml_root)
                            if not merged:
                                if sync_run.products_not_found < 10:
                                    logger.warning(
                                        f"XML PARSE_FAILED {sync_product.brand}/{sync_product.mpn}: "
                                        f"parser returned None"
                                    )
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            icecat_id = merged["product"].get("productid")
                            if icecat_id:
                                sync_product.mark_matched(icecat_id)
                            sync_run.increment_matched()

                            sync_result = sync_service.sync_from_merged_dict(
                                merged=merged,
                                sync_product=sync_product,
                            )
                        elif multi_lang:
                            # Multi-language result is a dict[int, dict]
                            if not match_result:
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            # Extract icecat_id from the primary language response
                            primary_data = match_result.get(lang_id_primary) or next(iter(match_result.values()))
                            icecat_id = primary_data.get("data", {}).get("GeneralInfo", {}).get("IcecatId")
                            if icecat_id:
                                sync_product.mark_matched(icecat_id)
                            sync_run.increment_matched()

                            # Sync multi-language to database
                            sync_result = sync_service.sync_multilang_product(
                                icecat_data_by_lang=match_result,
                                sync_product=sync_product,
                            )
                        else:
                            # Single-language result
                            if not match_result.found:
                                sync_product.mark_not_found()
                                sync_run.increment_not_found()
                                progress.increment_failure()
                                continue

                            # Matched - update sync product
                            sync_product.mark_matched(match_result.icecat_id)
                            sync_run.increment_matched()

                            # Sync to database
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

                            progress.increment_success()
                        else:
                            sync_run.increment_errored()
                            progress.increment_failure()

                    except Exception as e:
                        logger.error(f"Error processing {sync_product.brand}/{sync_product.mpn}: {e}")
                        progress.increment_failure()
                        sync_run.increment_errored()

                # Commit after each batch
                session.commit()
                products_processed += len(batch)

                # Log batch progress every 10 batches or every 1000 products
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
