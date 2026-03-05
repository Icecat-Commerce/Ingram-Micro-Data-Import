"""Base repository with common CRUD operations."""

from typing import Any, Generic, TypeVar, Sequence

from sqlalchemy import select, delete, update
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert

from ..models.db.base import Base

T = TypeVar("T", bound=Base)


class BaseRepository(Generic[T]):
    """
    Base repository providing common database operations.

    Provides:
    - Basic CRUD operations (create, read, update, delete)
    - Bulk operations for high-volume data processing
    - Upsert (insert or update) functionality
    """

    def __init__(self, session: Session, model_class: type[T]):
        """
        Initialize repository with session and model class.

        Args:
            session: SQLAlchemy session for database operations
            model_class: The SQLAlchemy model class this repository manages
        """
        self.session = session
        self.model_class = model_class

    def get_by_id(self, id_value: Any) -> T | None:
        """Get a single record by primary key."""
        return self.session.get(self.model_class, id_value)

    def get_all(self, limit: int | None = None, offset: int = 0) -> Sequence[T]:
        """Get all records with optional pagination."""
        stmt = select(self.model_class).offset(offset)
        if limit:
            stmt = stmt.limit(limit)
        return self.session.scalars(stmt).all()

    def get_by_filter(self, **filters: Any) -> Sequence[T]:
        """Get records matching filter criteria."""
        stmt = select(self.model_class).filter_by(**filters)
        return self.session.scalars(stmt).all()

    def get_one_by_filter(self, **filters: Any) -> T | None:
        """Get a single record matching filter criteria."""
        stmt = select(self.model_class).filter_by(**filters)
        return self.session.scalars(stmt).first()

    def count(self, **filters: Any) -> int:
        """Count records matching filter criteria."""
        from sqlalchemy import func

        stmt = select(func.count()).select_from(self.model_class)
        if filters:
            stmt = stmt.filter_by(**filters)
        result = self.session.execute(stmt).scalar()
        return result or 0

    def create(self, entity: T) -> T:
        """Create a new record."""
        self.session.add(entity)
        self.session.flush()
        return entity

    def create_many(self, entities: list[T]) -> list[T]:
        """Create multiple records."""
        self.session.add_all(entities)
        self.session.flush()
        return entities

    def update(self, entity: T) -> T:
        """Update an existing record."""
        self.session.merge(entity)
        self.session.flush()
        return entity

    def delete(self, entity: T) -> None:
        """Delete a record."""
        self.session.delete(entity)
        self.session.flush()

    def delete_by_id(self, id_value: Any) -> bool:
        """Delete a record by primary key. Returns True if deleted."""
        entity = self.get_by_id(id_value)
        if entity:
            self.delete(entity)
            return True
        return False

    def delete_by_filter(self, **filters: Any) -> int:
        """Delete records matching filter criteria. Returns count deleted."""
        stmt = delete(self.model_class).filter_by(**filters)
        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount

    def bulk_insert(self, records: list[dict[str, Any]]) -> int:
        """
        Bulk insert records for high performance.

        Args:
            records: List of dictionaries with column values

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        stmt = mysql_insert(self.model_class).values(records)
        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount

    def bulk_upsert(
        self,
        records: list[dict[str, Any]],
        update_columns: list[str] | None = None,
    ) -> int:
        """
        Bulk insert or update records (ON DUPLICATE KEY UPDATE).

        Args:
            records: List of dictionaries with column values
            update_columns: Columns to update on conflict. If None, updates all.

        Returns:
            Number of records affected
        """
        if not records:
            return 0

        stmt = mysql_insert(self.model_class).values(records)

        # Determine which columns to update on conflict
        if update_columns is None:
            # Update all columns except primary key
            update_dict = {
                col.name: stmt.inserted[col.name]
                for col in self.model_class.__table__.columns
                if not col.primary_key
            }
        else:
            update_dict = {col: stmt.inserted[col] for col in update_columns}

        stmt = stmt.on_duplicate_key_update(**update_dict)
        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount

    def refresh(self, entity: T) -> T:
        """Refresh entity from database."""
        self.session.refresh(entity)
        return entity

    def commit(self) -> None:
        """Commit the current transaction."""
        self.session.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.session.rollback()
