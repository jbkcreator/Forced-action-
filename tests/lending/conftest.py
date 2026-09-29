"""Shared fixtures for the lending tests.

The lending tables are created inside a transaction that is rolled back, so
nothing persists in the shared database.
"""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.lending.models import LENDING_SCHEMA, LendingCallDisposition


@pytest.fixture
def lending_db():
    """A session whose commits are savepoints inside one rolled-back transaction."""
    from config.settings import get_settings

    url = get_settings().database_url
    if not url:
        pytest.skip("requires a live Postgres DATABASE_URL")
    engine = create_engine(str(url), connect_args={"connect_timeout": 5})
    try:
        conn = engine.connect()
    except Exception:
        engine.dispose()
        pytest.skip("Postgres is not reachable")
    tx = conn.begin()
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LENDING_SCHEMA}"'))
    LendingCallDisposition.__table__.create(bind=conn, checkfirst=True)
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()
