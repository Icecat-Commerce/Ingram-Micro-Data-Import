"""Database connection management."""

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from ..config import DatabaseConfig
from ..models.db.base import Base

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection factory and session management."""

    def __init__(self, config: DatabaseConfig):
        self._config = config
        connect_args = {"ssl": {"ssl": True}} if config.ssl else {}
        self._engine = create_engine(
            config.connection_string,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_pre_ping=True,
            echo=False,
            connect_args=connect_args,
        )
        self._session_factory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
        )

    @property
    def engine(self):
        """Get the SQLAlchemy engine."""
        return self._engine

    def create_tables(self) -> list[str]:
        """Create missing tables (never alters or drops existing ones).

        Uses CREATE TABLE IF NOT EXISTS for every table defined in the
        ORM metadata. Tables that already exist are left untouched.

        Returns:
            List of table names that were newly created.
        """
        existing = set(inspect(self._engine).get_table_names())
        expected = set(Base.metadata.tables.keys())

        Base.metadata.create_all(self._engine, checkfirst=True)

        created = expected - existing
        skipped = expected & existing
        if created:
            logger.info("Created tables: %s", ", ".join(sorted(created)))
        if skipped:
            logger.info("Already existed (skipped): %s", ", ".join(sorted(skipped)))
        return sorted(created)

    def drop_tables(self) -> None:
        """Drop all tables in the database (disables FK checks first)."""
        with self._engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            conn.commit()
        Base.metadata.drop_all(self._engine)
        with self._engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
            conn.commit()

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope around a series of operations.

        Usage:
            with db.session() as session:
                session.add(obj)
                session.commit()
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self) -> Session:
        """
        Get a new session. Caller is responsible for closing.

        Usage:
            session = db.get_session()
            try:
                # do work
                session.commit()
            finally:
                session.close()
        """
        return self._session_factory()


# Global database connection instance (initialized in main)
_db_connection: DatabaseConnection | None = None


def init_db(config: DatabaseConfig) -> DatabaseConnection:
    """Initialize the global database connection."""
    global _db_connection
    _db_connection = DatabaseConnection(config)
    return _db_connection


def get_db() -> DatabaseConnection:
    """Get the global database connection."""
    if _db_connection is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db_connection


def get_db_session() -> Session:
    """Get a database session from the global connection."""
    return get_db().get_session()
