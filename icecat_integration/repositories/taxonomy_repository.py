"""Repository for taxonomy bulk operations (truncate + insert)."""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Raw SQL INSERT statements for each table (much faster than ORM for bulk inserts)
_INSERT_SQL = {
    "category": text(
        "INSERT INTO `category` (categoryid, categoryname, localeid, isactive) "
        "VALUES (:categoryid, :categoryname, :localeid, :isactive)"
    ),
    "categoryMapping": text(
        "INSERT INTO `categoryMapping` (categoryid, parentcategoryid, isactive, ordernumber, catlevel) "
        "VALUES (:categoryid, :parentcategoryid, :isactive, :ordernumber, :catlevel)"
    ),
    "categoryheader": text(
        "INSERT INTO `categoryheader` (categoryid, headerid, headername, localeid, displayorder, isactive) "
        "VALUES (:categoryid, :headerid, :headername, :localeid, :displayorder, :isactive)"
    ),
    "categorydisplayattributes": text(
        "INSERT INTO `categorydisplayattributes` "
        "(categoryid, attributeid, headerid, localeid, displayorder, isactive, issearchable) "
        "VALUES (:categoryid, :attributeid, :headerid, :localeid, :displayorder, :isactive, :issearchable)"
    ),
    "attributenames": text(
        "INSERT IGNORE INTO `attributenames` (attributeid, name, localeid) "
        "VALUES (:attributeid, :name, :localeid)"
    ),
}

TAXONOMY_TABLES = [
    "category", "categoryMapping", "categoryheader",
    "categorydisplayattributes", "attributenames",
]


class TaxonomyRepository:
    """
    Repository for bulk taxonomy table operations.

    Uses raw SQL with executemany for performance on large datasets (~15M rows).
    """

    def __init__(self, session: Session):
        self.session = session

    def truncate_all_taxonomy_tables(self) -> dict[str, int]:
        """Delete all records from taxonomy tables.

        Temporarily disables FK checks because product.categoryid
        references categoryMapping. Safe here since we do a full refresh.
        """
        counts = {}
        self.session.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        try:
            for table_name in TAXONOMY_TABLES:
                result = self.session.execute(text(f"DELETE FROM `{table_name}`"))
                counts[table_name] = result.rowcount
                logger.info(f"Deleted {result.rowcount} rows from {table_name}")
            self.session.flush()
        finally:
            self.session.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
        return counts

    # Tables with self-referencing or external FKs that need checks disabled
    _FK_SENSITIVE_TABLES = frozenset({"categoryMapping"})

    def bulk_insert_for_table(
        self,
        table_name: str,
        records: list[dict[str, Any]],
    ) -> int:
        """
        Bulk insert records using executemany (raw SQL).

        Much faster than ORM insert().values() for large batches.
        Disables FK checks for categoryMapping (self-referencing FK on parentcategoryid).
        """
        if not records:
            return 0

        stmt = _INSERT_SQL.get(table_name)
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
