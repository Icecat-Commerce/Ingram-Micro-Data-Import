"""Product sync service for synchronizing individual products to database."""

import logging
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from ..mappers.product_mapper import ProductMapper
from ..repositories.product_repository import (
    ProductRepository,
    VendorRepository,
    CategoryRepository,
)
from ..repositories.sync_repository import SyncRepository
from ..repositories.errors_repository import ErrorsRepository
from ..models.db.sync_product import SyncProduct
from ..utils.logging_utils import SyncLogger

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    """Result of syncing a single product."""

    success: bool
    is_new: bool
    product_id: int | None = None
    productid: int | None = None  # Icecat Product ID
    categoryid: int | None = None  # Icecat Category ID for delta tracking
    error_message: str | None = None
    duration_ms: int = 0


class ProductSyncService:
    """
    Sync individual products from Icecat data to database.

    Handles:
    - Product upsert (create if new, update if exists)
    - Related data sync using delete & recreate pattern
    - Vendor/category creation if needed
    - Sync tracking status updates
    """

    def __init__(
        self,
        session: Session,
        sync_logger: SyncLogger | None = None,
        default_language_id: int = 1,
        run_id: str | None = None,
    ):
        """
        Initialize the sync service.

        Args:
            session: SQLAlchemy database session
            sync_logger: Optional logger for sync operations
            default_language_id: Default Icecat language ID
            run_id: Sync run ID for audit trail and error tracking
        """
        self.session = session
        self.sync_logger = sync_logger
        self.run_id = run_id
        self.mapper = ProductMapper(default_language_id)

        # Initialize repositories
        self.product_repo = ProductRepository(session)
        self.vendor_repo = VendorRepository(session)
        self.category_repo = CategoryRepository(session)
        self.sync_repo = SyncRepository(session)
        self.errors_repo = ErrorsRepository(session)

    async def sync_product(
        self,
        icecat_data: dict[str, Any],
        sync_product: SyncProduct,
        language_id: int | None = None,
    ) -> SyncResult:
        """
        Sync a single product to the database.

        Steps:
        1. Map Icecat data to database format
        2. Create vendor if needed
        3. Create category if needed
        4. Upsert product (create if new, update if exists)
        5. Delete & recreate all related data
        6. Update sync tracking status

        Args:
            icecat_data: Raw Icecat API response data
            sync_product: SyncProduct tracking record
            language_id: Language ID for the data

        Returns:
            SyncResult with sync outcome
        """
        start_time = time.perf_counter()
        result = SyncResult(success=False, is_new=False)

        try:
            # Map the Icecat data
            mapped = self.mapper.map_product_response(icecat_data, language_id)
            if not mapped:
                result.error_message = "Failed to map Icecat data"
                self._update_sync_status_error(sync_product, result.error_message)
                return result

            product_data = mapped["product"]
            if not product_data.get("productid"):
                result.error_message = "Missing product ID in mapped data"
                self._update_sync_status_error(sync_product, result.error_message)
                return result

            # Ensure vendor exists
            vendor_data = mapped.get("vendor")
            if vendor_data:
                self._ensure_vendor(vendor_data)

            # Ensure category exists
            category_data = mapped.get("category")
            if category_data:
                self._ensure_category(category_data)

            # Sync the product with granular per-entity logging (Level 2)
            product, is_new = self._sync_product_with_logging(
                product_data=product_data,
                mapped=mapped,
                sync_product=sync_product,
            )

            # Commit the transaction
            self.session.commit()

            # Update sync tracking
            sync_product.mark_synced(product.productid)
            self.session.commit()

            # Update result
            result.success = True
            result.is_new = is_new
            result.product_id = product.productid
            result.productid = product.productid
            result.categoryid = product_data.get("categoryid")
            result.duration_ms = int((time.perf_counter() - start_time) * 1000)

            # Log overall success
            if self.sync_logger:
                action = "Created" if is_new else "Updated"
                self.sync_logger.log_db_write(
                    f"{action} product {product.productid} (all entities synced)",
                    brand=sync_product.brand,
                    mpn=sync_product.mpn,
                    icecat_id=sync_product.icecat_product_id,
                    duration_ms=result.duration_ms,
                    extra_data={
                        "product_id": product.productid,
                        "is_new": is_new,
                    },
                )

            logger.info(
                f"{'Created' if is_new else 'Updated'} product "
                f"{sync_product.brand}/{sync_product.mpn} -> {product.productid}"
            )

        except Exception as e:
            self.session.rollback()
            result.error_message = str(e)
            result.duration_ms = int((time.perf_counter() - start_time) * 1000)

            # Update sync status to error
            self._update_sync_status_error(sync_product, str(e))

            # Log error to sync_errors table for retry mechanism
            self._log_sync_error(
                sync_product=sync_product,
                error_message=str(e),
                error_type=self._classify_error(e),
            )

            logger.error(
                f"Error syncing {sync_product.brand}/{sync_product.mpn}: {e}",
                exc_info=True,
            )

            if self.sync_logger:
                self.sync_logger.log_error(
                    f"Failed to sync product: {e}",
                    brand=sync_product.brand,
                    mpn=sync_product.mpn,
                    icecat_id=sync_product.icecat_product_id,
                    extra_data={"error": str(e)},
                )

        return result

    def _ensure_vendor(self, vendor_data: dict[str, Any]) -> None:
        """Create vendor if it doesn't exist."""
        vendor_id = vendor_data.get("vendorid")
        vendor_name = vendor_data.get("name", "Unknown")
        logo_url = vendor_data.get("logourl")

        if vendor_id:
            self.vendor_repo.get_or_create(vendor_id, vendor_name, logo_url)

    def _ensure_category(self, category_data: dict[str, Any]) -> None:
        """Create category if it doesn't exist."""
        category_id = category_data.get("categoryid")
        category_name = category_data.get("categoryname", "Unknown")

        if category_id:
            self.category_repo.get_or_create(category_id, category_name)

    def _sync_product_with_logging(
        self,
        product_data: dict[str, Any],
        mapped: dict[str, Any],
        sync_product: SyncProduct,
    ) -> tuple[Any, bool]:
        """
        Sync product with granular per-entity logging (Activity Level 2).

        Instead of calling sync_product_full() as a black box, performs
        each entity sync individually with logging between each step.

        Args:
            product_data: Core product data dict
            mapped: Full mapped data dict with all entity lists
            sync_product: SyncProduct tracking record for log context

        Returns:
            Tuple of (Product, is_new)
        """
        log_ctx = {
            "brand": sync_product.brand,
            "mpn": sync_product.mpn,
            "icecat_id": sync_product.icecat_product_id,
        }

        # Step 1: Upsert product
        product, is_new = self.product_repo.upsert_product(product_data)
        if self.sync_logger:
            action = "Created" if is_new else "Updated"
            self.sync_logger.log_db_write(
                f"{action} product record {product.productid}",
                **log_ctx,
                extra_data={"entity": "product", "is_new": is_new},
            )

        pid = product.productid

        # Step 2: Sync descriptions
        descriptions = mapped.get("descriptions")
        if descriptions:
            rows = self.product_repo.sync_descriptions(pid, descriptions)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} descriptions",
                    **log_ctx,
                    extra_data={"entity": "descriptions", "rows": len(rows)},
                )

        # Step 3: Sync marketing info
        marketing_info = mapped.get("marketing_info")
        if marketing_info:
            rows = self.product_repo.sync_marketing_info(pid, marketing_info)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} marketing info",
                    **log_ctx,
                    extra_data={"entity": "marketing_info", "rows": len(rows)},
                )

        # Step 4: Sync features
        features = mapped.get("features")
        if features:
            rows = self.product_repo.sync_features(pid, features, run_id=self.run_id)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} features",
                    **log_ctx,
                    extra_data={"entity": "features", "rows": len(rows)},
                )

        # Step 5: Sync media
        media = mapped.get("media")
        if media:
            rows = self.product_repo.sync_media(pid, media, run_id=self.run_id)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} media",
                    **log_ctx,
                    extra_data={"entity": "media", "rows": len(rows)},
                )

        # Step 6: Sync attributes (non-searchable)
        attributes = mapped.get("attributes")
        if attributes:
            rows = self.product_repo.sync_attributes(pid, attributes, run_id=self.run_id)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} attributes",
                    **log_ctx,
                    extra_data={"entity": "attributes", "rows": len(rows)},
                )

        # Step 7: Sync search attributes (searchable)
        search_attributes = mapped.get("search_attributes")
        if search_attributes:
            rows = self.product_repo.sync_search_attributes(pid, search_attributes)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} search attributes",
                    **log_ctx,
                    extra_data={"entity": "search_attributes", "rows": len(rows)},
                )

        # Step 8: Sync thumbnails
        thumbnails = mapped.get("thumbnails")
        if thumbnails:
            rows = self.product_repo.sync_thumbnails(pid, thumbnails)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} thumbnails",
                    **log_ctx,
                    extra_data={"entity": "thumbnails", "rows": len(rows)},
                )

        # Step 9: Sync addons
        addons = mapped.get("addons")
        if addons:
            rows = self.product_repo.sync_addons(pid, addons, run_id=self.run_id)
            if self.sync_logger:
                self.sync_logger.log_db_write(
                    f"Synced {len(rows)} addons",
                    **log_ctx,
                    extra_data={"entity": "addons", "rows": len(rows)},
                )

        return product, is_new

    def _update_sync_status_error(
        self, sync_product: SyncProduct, error_message: str
    ) -> None:
        """Update sync product status to error."""
        try:
            sync_product.mark_error(error_message)
            self.session.commit()
        except Exception as e:
            logger.error(f"Failed to update sync status: {e}")
            self.session.rollback()

    def _log_sync_error(
        self,
        sync_product: SyncProduct,
        error_message: str,
        error_type: str | None = None,
    ) -> None:
        """
        Log error to sync_errors table for retry mechanism.

        Log error to sync_errors table for later retry.
        """
        try:
            self.errors_repo.create_error(
                error_message=error_message,
                error_type=error_type,
                product_id=sync_product.icecat_product_id,
                brand=sync_product.brand,
                mpn=sync_product.mpn,
                ean=sync_product.ean,
                sync_run_id=self.run_id,
            )
            self.session.commit()
        except Exception as e:
            logger.error(f"Failed to log sync error: {e}")
            self.session.rollback()

    def _classify_error(self, error: Exception) -> str:
        """Classify error type for tracking."""
        error_str = str(type(error).__name__).lower()

        if "timeout" in error_str or "connection" in error_str:
            return "api_error"
        elif "sql" in error_str or "database" in error_str or "integrity" in error_str:
            return "db_error"
        elif "parse" in error_str or "json" in error_str or "validation" in error_str:
            return "parse_error"
        elif "key" in error_str or "attribute" in error_str:
            return "mapping_error"
        else:
            return "unknown"

    def deactivate_product(
        self,
        sync_product: SyncProduct,
        reason: str | None = None,
    ) -> bool:
        """
        Mark a product as inactive (soft delete).

        Products no longer in assortment are marked isactive=0, NOT hard deleted.
        Ingram Micro decides when to actually remove products.

        Args:
            sync_product: SyncProduct tracking record
            reason: Reason for deactivation (e.g., 'not_in_assortment')

        Returns:
            True if deactivated successfully
        """
        try:
            if sync_product.pimcore_product_id:
                self.product_repo.deactivate_product(
                    sync_product.pimcore_product_id,
                    run_id=self.run_id,
                    reason=reason or "not_in_assortment",
                )

            sync_product.mark_deleted()
            self.session.commit()

            logger.info(f"Deactivated product {sync_product.brand}/{sync_product.mpn}")
            return True

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error deactivating product: {e}")
            return False

    def sync_multilang_product(
        self,
        icecat_data_by_lang: dict[int, dict[str, Any]],
        sync_product: SyncProduct,
    ) -> SyncResult:
        """
        Sync a product with data from multiple languages.

        Args:
            icecat_data_by_lang: Dict mapping language_id to Icecat response
            sync_product: SyncProduct tracking record

        Returns:
            SyncResult with sync outcome
        """
        from ..mappers.product_mapper import MultiLanguageProductMapper

        start_time = time.perf_counter()
        result = SyncResult(success=False, is_new=False)

        try:
            # Use multi-language mapper to aggregate data
            ml_mapper = MultiLanguageProductMapper()

            for lang_id, data in icecat_data_by_lang.items():
                ml_mapper.add_language_response(data, lang_id)

            merged = ml_mapper.get_merged_data()
            if not merged:
                result.error_message = "Failed to merge multi-language data"
                self._update_sync_status_error(sync_product, result.error_message)
                return result

            product_data = merged["product"]
            if not product_data.get("productid"):
                result.error_message = "Missing product ID in merged data"
                self._update_sync_status_error(sync_product, result.error_message)
                return result

            # Ensure vendor exists
            if merged.get("vendor"):
                self._ensure_vendor(merged["vendor"])

            # Ensure category exists
            if merged.get("category"):
                self._ensure_category(merged["category"])

            # Sync the product with granular per-entity logging (Level 2)
            product, is_new = self._sync_product_with_logging(
                product_data=product_data,
                mapped=merged,
                sync_product=sync_product,
            )

            self.session.commit()

            # Update sync tracking
            sync_product.mark_synced(product.productid)
            self.session.commit()

            result.success = True
            result.is_new = is_new
            result.product_id = product.productid
            result.productid = product.productid
            result.categoryid = product_data.get("categoryid")
            result.duration_ms = int((time.perf_counter() - start_time) * 1000)

            # Log overall success
            if self.sync_logger:
                action = "Created" if is_new else "Updated"
                self.sync_logger.log_db_write(
                    f"{action} multi-lang product {product.productid} ({len(icecat_data_by_lang)} languages)",
                    brand=sync_product.brand,
                    mpn=sync_product.mpn,
                    icecat_id=sync_product.icecat_product_id,
                    duration_ms=result.duration_ms,
                    extra_data={
                        "product_id": product.productid,
                        "is_new": is_new,
                        "languages": len(icecat_data_by_lang),
                    },
                )

            logger.info(
                f"{'Created' if is_new else 'Updated'} multi-lang product "
                f"{sync_product.brand}/{sync_product.mpn} with {len(icecat_data_by_lang)} languages"
            )

        except Exception as e:
            self.session.rollback()
            result.error_message = str(e)
            result.duration_ms = int((time.perf_counter() - start_time) * 1000)
            self._update_sync_status_error(sync_product, str(e))

            # Log error to sync_errors table for retry mechanism
            self._log_sync_error(
                sync_product=sync_product,
                error_message=str(e),
                error_type=self._classify_error(e),
            )

            logger.error(f"Error syncing multi-lang product: {e}", exc_info=True)

        return result
