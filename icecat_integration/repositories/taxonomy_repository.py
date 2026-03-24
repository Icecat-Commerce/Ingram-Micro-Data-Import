"""Repository for taxonomy bulk operations (upsert + cleanup).

Strategy: UPSERT all records from the taxonomy file, then clean up stale
categories that are no longer present.  Tables are never truncated, so
existing data remains intact if the job crashes mid-import.
"""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# UPSERT SQL: INSERT ... ON DUPLICATE KEY UPDATE
# ---------------------------------------------------------------------------
_UPSERT_SQL = {
    "category": text(
        "INSERT INTO `category` (categoryid, categoryname, localeid, isactive) "
        "VALUES (:categoryid, :categoryname, :localeid, :isactive) "
        "ON DUPLICATE KEY UPDATE "
        "categoryname = VALUES(categoryname), isactive = VALUES(isactive), "
        "modified_date = CURRENT_TIMESTAMP"
    ),
    "categoryMapping": text(
        "INSERT INTO `categoryMapping` (categoryid, parentcategoryid, isactive, ordernumber, catlevel) "
        "VALUES (:categoryid, :parentcategoryid, :isactive, :ordernumber, :catlevel) "
        "ON DUPLICATE KEY UPDATE "
        "parentcategoryid = VALUES(parentcategoryid), isactive = VALUES(isactive), "
        "ordernumber = VALUES(ordernumber), catlevel = VALUES(catlevel), "
        "modified_date = CURRENT_TIMESTAMP"
    ),
    "categoryheader": text(
        "INSERT INTO `categoryheader` (categoryid, headerid, headername, localeid, displayorder, isactive) "
        "VALUES (:categoryid, :headerid, :headername, :localeid, :displayorder, :isactive) "
        "ON DUPLICATE KEY UPDATE "
        "headername = VALUES(headername), displayorder = VALUES(displayorder), "
        "isactive = VALUES(isactive), updated_at = CURRENT_TIMESTAMP"
    ),
    "categorydisplayattributes": text(
        "INSERT INTO `categorydisplayattributes` "
        "(categoryid, attributeid, headerid, localeid, displayorder, isactive, issearchable) "
        "VALUES (:categoryid, :attributeid, :headerid, :localeid, :displayorder, :isactive, :issearchable) "
        "ON DUPLICATE KEY UPDATE "
        "headerid = VALUES(headerid), displayorder = VALUES(displayorder), "
        "isactive = VALUES(isactive), issearchable = VALUES(issearchable), "
        "updated_at = CURRENT_TIMESTAMP"
    ),
    "attributenames": text(
        "INSERT INTO `attributenames` (attributeid, name, localeid) "
        "VALUES (:attributeid, :name, :localeid) "
        "ON DUPLICATE KEY UPDATE name = VALUES(name)"
    ),
}

TAXONOMY_TABLES = [
    "category", "categoryMapping", "categoryheader",
    "categorydisplayattributes", "attributenames",
]

# Unique keys required for UPSERT on tables that lack them
_REQUIRED_UNIQUE_KEYS = {
    "categoryheader": {
        "key_name": "uk_cat_header_locale",
        "columns": "(categoryid, headerid, localeid)",
    },
    "categorydisplayattributes": {
        "key_name": "uk_cat_disp_attr_locale",
        "columns": "(categoryid, attributeid, localeid)",
    },
}


class TaxonomyRepository:
    """
    Repository for bulk taxonomy table operations.

    Uses UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) so that tables are
    never emptied.  If the job crashes mid-import, existing data stays
    intact and downstream product sync continues to work.
    """

    # Tables with FKs that need checks disabled during writes
    _FK_SENSITIVE_TABLES = frozenset({"categoryMapping"})

    def __init__(self, session: Session):
        self.session = session

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------
    def ensure_unique_keys(self) -> None:
        """Add unique keys needed by UPSERT if they don't exist yet.

        categoryheader and categorydisplayattributes ship without a
        unique key in older schema versions.  This is idempotent.
        """
        for table_name, key_info in _REQUIRED_UNIQUE_KEYS.items():
            result = self.session.execute(
                text(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = :table_name "
                    "AND INDEX_NAME = :key_name"
                ),
                {"table_name": table_name, "key_name": key_info["key_name"]},
            )
            if result.scalar() > 0:
                logger.debug(
                    f"Unique key {key_info['key_name']} already exists on {table_name}"
                )
                continue

            logger.info(f"Adding unique key {key_info['key_name']} on {table_name}...")
            self.session.execute(
                text(
                    f"ALTER TABLE `{table_name}` "
                    f"ADD UNIQUE KEY `{key_info['key_name']}` {key_info['columns']}"
                )
            )
            logger.info(f"Added unique key {key_info['key_name']} on {table_name}")

    # ------------------------------------------------------------------
    # Bulk upsert
    # ------------------------------------------------------------------
    def bulk_upsert_for_table(
        self,
        table_name: str,
        records: list[dict[str, Any]],
    ) -> int:
        """
        Bulk upsert records using INSERT ... ON DUPLICATE KEY UPDATE.

        Existing rows are updated in-place; new rows are inserted.
        """
        if not records:
            return 0

        stmt = _UPSERT_SQL.get(table_name)
        if stmt is None:
            raise ValueError(f"Unknown taxonomy table: {table_name}")

        disable_fk = table_name in self._FK_SENSITIVE_TABLES
        if disable_fk:
            self.session.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        try:
            self.session.execute(stmt, records)
            self.session.flush()
        finally:
            if disable_fk:
                self.session.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        return len(records)

    # ------------------------------------------------------------------
    # Stale-data reporting (runs AFTER all upserts succeed)
    # ------------------------------------------------------------------
    def report_stale_categories(self, seen_categoryids: set[int]) -> dict[str, int]:
        """Log categories present in the DB but absent from the taxonomy file.

        Does NOT delete anything — just reports for manual review.
        """
        if not seen_categoryids:
            return {}

        ids_str = ",".join(str(cid) for cid in sorted(seen_categoryids))
        counts: dict[str, int] = {}

        for table_name in TAXONOMY_TABLES:
            if table_name == "attributenames":
                continue  # shared across categories, skip
            result = self.session.execute(
                text(
                    f"SELECT COUNT(*) FROM `{table_name}` "
                    f"WHERE categoryid NOT IN ({ids_str})"
                )
            )
            stale_count = result.scalar()
            counts[table_name] = stale_count
            if stale_count > 0:
                logger.warning(
                    f"{table_name}: {stale_count} rows reference categories "
                    f"not in the current taxonomy file (not deleted, log only)"
                )

        return counts
