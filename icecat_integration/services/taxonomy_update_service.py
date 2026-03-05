"""Taxonomy update service - downloads and processes CategoryFeaturesList.xml.gz.

Populates: category, categoryMapping, categoryheader, categorydisplayattributes, attributenames.
Strategy: truncate all five tables, then stream-parse and bulk-insert in batches.
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from ..config import AppConfig
from ..database.connection import DatabaseConnection
from ..mappers.icecat_language_mapper import IcecatLanguageMapper
from ..parsers.category_features_parser import CategoryFeaturesParser
from ..repositories.taxonomy_repository import TaxonomyRepository

logger = logging.getLogger(__name__)

CATEGORY_FEATURES_URL = (
    "https://data.icecat.biz/export/freexml/refs/CategoryFeaturesList.xml.gz"
)

DEFAULT_BATCH_SIZE = 5000
# Maximum 32-bit signed integer
_INT_MAX = 2_147_483_647


@dataclass
class TaxonomyUpdateStats:
    """Statistics from a taxonomy update run."""

    categories_processed: int = 0
    feature_groups_processed: int = 0
    features_processed: int = 0
    categories_inserted: int = 0
    category_mappings_inserted: int = 0
    headers_inserted: int = 0
    display_attrs_inserted: int = 0
    attribute_names_inserted: int = 0
    download_seconds: float = 0.0
    parse_seconds: float = 0.0
    total_seconds: float = 0.0


class TaxonomyUpdateService:
    """
    Downloads and processes the Icecat CategoryFeaturesList.xml.gz
    to populate taxonomy tables.

    Usage:
        service = TaxonomyUpdateService(config, db_manager)
        stats = service.run()
    """

    def __init__(
        self,
        config: AppConfig,
        db_manager: DatabaseConnection,
        batch_size: int = DEFAULT_BATCH_SIZE,
        download_dir: str = "data/downloads",
    ):
        self.config = config
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.download_dir = Path(download_dir)
        self.supported_lang_ids = set(IcecatLanguageMapper.SUPPORTED_LANGUAGE_IDS)

    def run(
        self,
        skip_download: bool = False,
        file_path: str | None = None,
    ) -> TaxonomyUpdateStats:
        """
        Execute the full taxonomy update workflow.

        Args:
            skip_download: If True, use existing file instead of downloading.
            file_path: Path to an existing CategoryFeaturesList.xml.gz file.

        Returns:
            TaxonomyUpdateStats with counts and timings.
        """
        total_start = time.perf_counter()
        stats = TaxonomyUpdateStats()

        # Step 1: Locate or download the file
        if file_path:
            xml_gz_path = Path(file_path)
        elif not skip_download:
            dl_start = time.perf_counter()
            xml_gz_path = self._download_file()
            stats.download_seconds = time.perf_counter() - dl_start
        else:
            xml_gz_path = self.download_dir / "CategoryFeaturesList.xml.gz"

        if not xml_gz_path.exists():
            raise FileNotFoundError(f"Taxonomy file not found: {xml_gz_path}")

        size_gb = xml_gz_path.stat().st_size / (1024**3)
        logger.info(f"Using taxonomy file: {xml_gz_path} ({size_gb:.2f} GB)")

        # Step 2: Truncate existing taxonomy data
        with self.db_manager.session() as session:
            repo = TaxonomyRepository(session)
            counts = repo.truncate_all_taxonomy_tables()
            logger.info(f"Truncated taxonomy tables: {counts}")

        # Step 3: Parse and insert
        parse_start = time.perf_counter()
        self._parse_and_insert(xml_gz_path, stats)
        stats.parse_seconds = time.perf_counter() - parse_start

        stats.total_seconds = time.perf_counter() - total_start
        self._log_summary(stats)
        return stats

    def _download_file(self) -> Path:
        """Download CategoryFeaturesList.xml.gz with progress reporting."""
        self.download_dir.mkdir(parents=True, exist_ok=True)
        dest = self.download_dir / "CategoryFeaturesList.xml.gz"

        username = self.config.icecat.front_office_username
        password = self.config.icecat.front_office_password

        logger.info(f"Downloading taxonomy file from Icecat...")

        with httpx.stream(
            "GET",
            CATEGORY_FEATURES_URL,
            auth=(username, password),
            timeout=httpx.Timeout(600.0, connect=30.0),
            follow_redirects=True,
        ) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            last_report = 0

            with open(dest, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)

                    # Report every 100 MB
                    if total_size > 0 and downloaded - last_report >= 100 * 1024 * 1024:
                        pct = (downloaded / total_size) * 100
                        logger.info(
                            f"Download: {downloaded / (1024**3):.2f} / "
                            f"{total_size / (1024**3):.2f} GB ({pct:.1f}%)"
                        )
                        last_report = downloaded

        logger.info(f"Download complete: {dest} ({downloaded / (1024**3):.2f} GB)")
        return dest

    def _parse_and_insert(
        self, xml_gz_path: Path, stats: TaxonomyUpdateStats
    ) -> None:
        """Stream-parse the gzipped XML and insert records in batches."""
        # Batch accumulators
        category_batch: list[dict[str, Any]] = []
        category_mapping_batch: list[dict[str, Any]] = []
        header_batch: list[dict[str, Any]] = []
        display_attr_batch: list[dict[str, Any]] = []
        attr_names_batch: list[dict[str, Any]] = []

        parser = CategoryFeaturesParser(
            file_path=xml_gz_path,
            supported_lang_ids=self.supported_lang_ids,
        )

        for category_data in parser.iter_categories():
            stats.categories_processed += 1

            # --- category table records ---
            for lang_id, name in category_data.names.items():
                category_batch.append(
                    {
                        "categoryid": category_data.category_id,
                        "categoryname": name[:80],
                        "localeid": lang_id,
                        "isactive": True,
                    }
                )

            # --- categoryMapping table record (one per category) ---
            parent_id = category_data.parent_category_id or None
            category_mapping_batch.append(
                {
                    "categoryid": category_data.category_id,
                    "parentcategoryid": parent_id if parent_id and parent_id != 0 else None,
                    "isactive": True,
                    "ordernumber": 0,
                    "catlevel": 0,
                }
            )

            # --- categoryheader table records ---
            for cfg in category_data.feature_groups:
                stats.feature_groups_processed += 1
                for lang_id, group_name in cfg.names.items():
                    header_batch.append(
                        {
                            "categoryid": category_data.category_id,
                            "headerid": cfg.feature_group_id,
                            "headername": group_name[:200],
                            "localeid": lang_id,
                            "displayorder": min(cfg.order_number, _INT_MAX),
                            "isactive": True,
                        }
                    )

            # --- categorydisplayattributes + attributenames ---
            for feat in category_data.features:
                stats.features_processed += 1

                # Resolve headerid: CategoryFeatureGroup_ID -> FeatureGroup.ID
                resolved_header_id = category_data.cfg_id_to_header_id.get(
                    feat.category_feature_group_id,
                    feat.category_feature_group_id,  # fallback
                )

                for lang_id, feat_name in feat.names.items():
                    display_attr_batch.append(
                        {
                            "categoryid": category_data.category_id,
                            "attributeid": feat.feature_id,
                            "headerid": resolved_header_id,
                            "localeid": lang_id,
                            "displayorder": min(feat.order_number, _INT_MAX),
                            "isactive": True,
                            "issearchable": feat.is_searchable,
                        }
                    )
                    attr_names_batch.append(
                        {
                            "attributeid": feat.feature_id,
                            "name": feat_name[:110],
                            "localeid": lang_id,
                        }
                    )

            # Flush batches when they exceed threshold
            if len(category_batch) >= self.batch_size:
                stats.categories_inserted += self._flush_batch(
                    "category", category_batch
                )
            if len(category_mapping_batch) >= self.batch_size:
                stats.category_mappings_inserted += self._flush_batch(
                    "categoryMapping", category_mapping_batch
                )
            if len(header_batch) >= self.batch_size:
                stats.headers_inserted += self._flush_batch(
                    "categoryheader", header_batch
                )
            if len(display_attr_batch) >= self.batch_size:
                stats.display_attrs_inserted += self._flush_batch(
                    "categorydisplayattributes", display_attr_batch
                )
            if len(attr_names_batch) >= self.batch_size:
                stats.attribute_names_inserted += self._flush_batch(
                    "attributenames", attr_names_batch
                )

            # Progress logging every 500 categories
            if stats.categories_processed % 500 == 0:
                logger.info(
                    f"Progress: {stats.categories_processed} categories, "
                    f"{stats.features_processed} features, "
                    f"{stats.feature_groups_processed} feature groups"
                )

        # Final flush of remaining records
        if category_batch:
            stats.categories_inserted += self._flush_batch("category", category_batch)
        if category_mapping_batch:
            stats.category_mappings_inserted += self._flush_batch(
                "categoryMapping", category_mapping_batch
            )
        if header_batch:
            stats.headers_inserted += self._flush_batch("categoryheader", header_batch)
        if display_attr_batch:
            stats.display_attrs_inserted += self._flush_batch(
                "categorydisplayattributes", display_attr_batch
            )
        if attr_names_batch:
            stats.attribute_names_inserted += self._flush_batch(
                "attributenames", attr_names_batch
            )

    def _flush_batch(self, table_name: str, batch: list[dict[str, Any]]) -> int:
        """Flush a batch of records to the database and clear the list."""
        if not batch:
            return 0

        count = len(batch)
        with self.db_manager.session() as session:
            repo = TaxonomyRepository(session)
            repo.bulk_insert_for_table(table_name, batch)

        batch.clear()
        return count

    def _log_summary(self, stats: TaxonomyUpdateStats) -> None:
        """Log a summary of the taxonomy update."""
        logger.info("=" * 60)
        logger.info("TAXONOMY UPDATE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Categories processed: {stats.categories_processed}")
        logger.info(f"Feature groups processed: {stats.feature_groups_processed}")
        logger.info(f"Features processed: {stats.features_processed}")
        logger.info("---")
        logger.info(f"category rows inserted: {stats.categories_inserted}")
        logger.info(f"categoryMapping rows inserted: {stats.category_mappings_inserted}")
        logger.info(f"categoryheader rows inserted: {stats.headers_inserted}")
        logger.info(
            f"categorydisplayattributes rows inserted: {stats.display_attrs_inserted}"
        )
        logger.info(f"attributenames rows inserted: {stats.attribute_names_inserted}")
        logger.info("---")
        logger.info(f"Download time: {stats.download_seconds:.1f}s")
        logger.info(f"Parse + insert time: {stats.parse_seconds:.1f}s")
        logger.info(f"Total time: {stats.total_seconds:.1f}s")
