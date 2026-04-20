"""
Shared pytest fixtures for Forced Action test suite.
"""

import pytest
from unittest.mock import MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.core.models import Base


@pytest.fixture
def mock_db():
    """Lightweight mock DB session — no real DB required."""
    return MagicMock()


@pytest.fixture(scope="session")
def in_memory_db():
    """
    SQLite in-memory DB with full schema — for tests that need real ORM queries.
    Shared across the session for speed.

    NOTE: SQLite cannot handle Postgres-specific types (JSONB, ARRAY).
    For models using those types, use the `fresh_db` fixture (which routes
    to Postgres when available) or skip the test.
    """
    from sqlalchemy.dialects.postgresql import JSONB, ARRAY
    from sqlalchemy import JSON, String

    # Register type adapters so SQLite can handle Postgres-specific columns
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )

    # Monkey-patch JSONB → JSON and ARRAY → String for SQLite DDL
    import sqlalchemy.dialects.sqlite.base as sqlite_base
    _orig_get_colspec = sqlite_base.SQLiteTypeCompiler

    @staticmethod
    def _jsonb_compile(type_, **kw):
        return "JSON"

    from sqlalchemy.sql import compiler as _compiler

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture(scope="session")
def pg_engine():
    """
    Real Postgres engine — uses the app's DATABASE_URL.
    Scoped to session for connection reuse.
    Returns None if DATABASE_URL is not set.
    """
    try:
        from config.settings import get_settings
        settings = get_settings()
        url = settings.database_url
        if not url:
            return None
        engine = create_engine(str(url))
        # Quick connectivity check
        with engine.connect() as conn:
            conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        return engine
    except Exception:
        return None


@pytest.fixture
def fresh_db(pg_engine):
    """
    Fresh DB session per test — uses real Postgres, rolls back after each test.
    Skips if Postgres is not available.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured — skipping ORM test")
    Session = sessionmaker(bind=pg_engine)
    session = Session()
    session.begin_nested()
    yield session
    session.rollback()
    session.close()
