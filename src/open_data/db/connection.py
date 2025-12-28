"""
Database connection management for Open Data Platform.
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from open_data.config import settings
from open_data.db.models import Base

# Global engine instance
_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def get_engine(echo: bool = False) -> Engine:
    """
    Get or create the SQLAlchemy engine.

    Args:
        echo: If True, log all SQL statements.

    Returns:
        SQLAlchemy Engine instance.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            echo=echo,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    """Get the session factory."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _SessionLocal


def get_session() -> Generator[Session, None, None]:
    """
    Get a database session as a generator (for dependency injection).

    Yields:
        SQLAlchemy Session.
    """
    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Provide a transactional scope around a series of operations.

    Usage:
        with session_scope() as session:
            session.add(obj)
            # auto-commits on success, auto-rollbacks on exception
    """
    session_factory = get_session_factory()
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(drop_existing: bool = False) -> None:
    """
    Initialize the database schema.

    Note: For production, the init.sql script handles schema creation
    with partitioning. This function is mainly for testing or
    simple setups without partitioning.

    Args:
        drop_existing: If True, drop all tables before creating.
    """
    engine = get_engine()

    if drop_existing:
        Base.metadata.drop_all(bind=engine)

    Base.metadata.create_all(bind=engine)


def check_connection() -> bool:
    """
    Check if the database connection is working.

    Returns:
        True if connection is successful, False otherwise.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def get_table_stats() -> dict[str, int]:
    """
    Get row counts for main tables.

    Returns:
        Dictionary mapping table names to row counts.
    """
    engine = get_engine()
    tables = ["countries", "sources", "categories", "indicators", "observations"]
    stats = {}

    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            stats[table] = result.scalar() or 0

    return stats


def execute_raw_sql(sql: str) -> list[dict]:
    """
    Execute raw SQL and return results as list of dicts.

    Args:
        sql: SQL query to execute.

    Returns:
        List of dictionaries representing rows.
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]
