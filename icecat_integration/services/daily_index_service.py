"""Daily Index Service for delta detection from Icecat daily index XML.

Downloads the Icecat daily index file, cross-references product IDs with
the sync_product table, and marks updated products as PENDING for re-sync.
"""

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from ..api.xml_data_service import IcecatXmlDataService
from ..config import IcecatConfig
from ..models.db.sync_product import SyncStatus
from ..repositories.sync_repository import SyncRepository

logger = logging.getLogger(__name__)


@dataclass
class DailyIndexResult:
    """Result of processing the daily index."""

    total_in_index: int
    products_in_assortment: int
    products_marked_pending: int
    products_already_pending: int
    parse_errors: int


class DailyIndexService:
    """
    Service for processing the Icecat daily index file.

    Downloads the daily index XML which lists all products updated in
    the last 24 hours, then cross-references with our sync_product table
    to mark updated products as PENDING for the next sync run.
    """

    def __init__(self, config: IcecatConfig, session: Session):
        self.xml_service = IcecatXmlDataService(config)
        self.sync_repo = SyncRepository(session)
        self.session = session

    async def update_from_daily_index(
        self, culture_id: str = "EN"
    ) -> DailyIndexResult:
        """
        Download daily index and mark updated products for re-sync.

        Steps:
        1. Download the daily index XML from Icecat
        2. Parse Product_ID and Updated timestamp from each entry
        3. Cross-reference with sync_product table
        4. Mark products needing update as PENDING

        Args:
            culture_id: Language/culture code for the index (default: EN)

        Returns:
            DailyIndexResult with processing statistics
        """
        result = DailyIndexResult(
            total_in_index=0,
            products_in_assortment=0,
            products_marked_pending=0,
            products_already_pending=0,
            parse_errors=0,
        )

        # Step 1: Download the daily index XML
        logger.info(f"Downloading daily index for culture: {culture_id}")
        root = await self.xml_service.download_daily_index_file_async(culture_id)
        if root is None:
            logger.error("Failed to download daily index file")
            return result

        # Step 2: Parse product entries from XML
        # The XML structure is: <ICECAT-interface><files.index><file Product_ID="..." Updated="..."/>
        files_index = root.find(".//files.index")
        if files_index is None:
            files_index = root
        entries = files_index.findall(".//file")
        result.total_in_index = len(entries)
        logger.info(f"Daily index contains {len(entries):,} products")

        # Step 3: Cross-reference with sync_product and mark for re-sync
        batch_count = 0
        COMMIT_BATCH = 1000

        for entry in entries:
            try:
                product_id_str = entry.get("Product_ID")
                updated_str = entry.get("Updated")

                if not product_id_str:
                    result.parse_errors += 1
                    continue

                product_id = int(product_id_str)

                # Look up in our sync_product table
                sync_product = self.sync_repo.get_by_icecat_id(product_id)
                if sync_product is None:
                    # Product not in our assortment, skip
                    continue

                result.products_in_assortment += 1

                # Already pending? No action needed
                if sync_product.status == SyncStatus.PENDING:
                    result.products_already_pending += 1
                    continue

                # Parse the Updated timestamp (format: "20260212120000")
                icecat_modified = None
                if updated_str:
                    try:
                        icecat_modified = datetime.strptime(updated_str, "%Y%m%d%H%M%S")
                    except ValueError:
                        logger.debug(f"Could not parse Updated timestamp: {updated_str}")

                # Check if the product needs an update
                if sync_product.needs_update(icecat_modified):
                    sync_product.status = SyncStatus.PENDING
                    if icecat_modified:
                        sync_product.last_icecat_modified = icecat_modified
                    result.products_marked_pending += 1

                batch_count += 1
                if batch_count >= COMMIT_BATCH:
                    self.session.commit()
                    batch_count = 0

            except (ValueError, TypeError) as e:
                result.parse_errors += 1
                logger.debug(f"Error processing daily index entry: {e}")
                continue

        # Final commit
        self.session.commit()

        logger.info(
            f"Daily index processed: {result.total_in_index:,} total, "
            f"{result.products_in_assortment:,} in assortment, "
            f"{result.products_marked_pending:,} marked pending, "
            f"{result.products_already_pending:,} already pending"
        )

        return result
