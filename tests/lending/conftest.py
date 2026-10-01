"""Shared fixtures for the lending tests.

The lending tables are created inside a transaction that is rolled back, so
nothing persists in the shared database.
"""
from __future__ import annotations

import logging

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from migrations.apply_lending_call_dispositions_dialer import apply_to
from migrations.apply_lending_pr319_client_feedback import apply_to as apply_to_feedback
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
    apply_to(conn)  # DDL is transactional: the vendor-neutral migration is rolled back with the test
    apply_to_feedback(conn)
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


@pytest.fixture(autouse=True)
def _src_logs_reach_caplog():
    """config/logging.yaml sets the ``src`` logger to propagate=False, and any test that
    imports a module loading it would hide every later ``src.*`` record from caplog.
    Re-enable propagation (and DEBUG) for each test, then restore."""
    src = logging.getLogger("src")
    saved = (src.propagate, src.level)
    src.propagate, src.level = True, logging.NOTSET
    yield
    src.propagate, src.level = saved
