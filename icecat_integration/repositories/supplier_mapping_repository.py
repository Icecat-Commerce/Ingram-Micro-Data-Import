"""Repository for supplier brand mapping operations."""

import logging

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .base_repository import BaseRepository
from ..models.db.supplier_mapping import SupplierMapping

logger = logging.getLogger(__name__)


class SupplierMappingRepository(BaseRepository[SupplierMapping]):
    """Repository for supplier_mapping table — brand alias lookups."""

    def __init__(self, session: Session):
        super().__init__(session, SupplierMapping)

    def lookup_brand(self, symbol: str) -> str | None:
        """
        Lookup the Icecat canonical brand name for a distributor symbol.

        Checks symbols without a distributor_id first (global aliases),
        then falls back to any match.

        Args:
            symbol: Distributor brand name (case-insensitive)

        Returns:
            Icecat canonical name or None if not found
        """
        lower = symbol.lower()
        # Prefer global mappings (no distributor_id)
        stmt = (
            select(SupplierMapping.icecat_name)
            .where(SupplierMapping.symbol_lower == lower)
            .where(SupplierMapping.distributor_id.is_(None))
            .limit(1)
        )
        result = self.session.scalars(stmt).first()
        if result:
            return result

        # Fall back to any distributor-specific mapping
        stmt = (
            select(SupplierMapping.icecat_name)
            .where(SupplierMapping.symbol_lower == lower)
            .limit(1)
        )
        return self.session.scalars(stmt).first()

    def load_all_mappings(self) -> dict[str, str]:
        """
        Load all brand mappings into a dict for fast in-memory lookups.

        Prefers global mappings (distributor_id IS NULL) over distributor-specific ones.

        Returns:
            Dict of {symbol_lower: icecat_name}
        """
        # Load all mappings, global first so they take priority
        stmt = (
            select(SupplierMapping.symbol_lower, SupplierMapping.icecat_name)
            .order_by(SupplierMapping.distributor_id.asc())  # NULLs first (global mappings take priority)
        )
        rows = self.session.execute(stmt).all()

        mapping: dict[str, str] = {}
        for symbol_lower, icecat_name in rows:
            # First seen wins (global aliases loaded first)
            if symbol_lower not in mapping:
                mapping[symbol_lower] = icecat_name

        logger.info(f"Loaded {len(mapping):,} unique brand mappings from DB")
        return mapping

    def bulk_import(self, records: list[dict], batch_size: int = 5000) -> int:
        """
        Truncate supplier_mapping table and bulk insert all records.

        Args:
            records: List of dicts with keys: supplier_id, icecat_name, symbol, symbol_lower, distributor_id
            batch_size: Records per batch insert

        Returns:
            Total records inserted
        """
        # Truncate existing data
        self.session.execute(text("TRUNCATE TABLE supplier_mapping"))
        self.session.flush()

        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            count = self.bulk_insert(batch)
            total += count
            self.session.flush()

            if total % 10000 < batch_size:
                logger.info(f"  Supplier mapping import: {total:,} records...")

        logger.info(f"Imported {total:,} supplier mapping records")
        return total

    def get_all_for_supplier(self, supplier_id: int) -> list[SupplierMapping]:
        """Get all alias symbols for a supplier."""
        return self.get_by_filter(supplier_id=supplier_id)

    def get_mapping_count(self) -> int:
        """Get total number of mapping records."""
        return self.count()
