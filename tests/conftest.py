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
    """
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def fresh_db(in_memory_db):
    """
    Fresh DB session per test — rolls back after each test to keep isolation.
    """
    in_memory_db.begin_nested()
    yield in_memory_db
    in_memory_db.rollback()
