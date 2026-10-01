"""PR #319 client-feedback DDL: text consents, call-row columns (campaign id, consent check, recording status). Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pr319_client_feedback.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingTextConsent

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Tasks 2, 4 and 5 append their ALTER/UPDATE/INDEX statements here; each is idempotent.
STATEMENTS: tuple[str, ...] = (
    "ALTER TABLE lending.call_dispositions ADD COLUMN IF NOT EXISTS consent_checked_at timestamptz",
)


def apply_to(conn: Connection) -> None:
    """Run every step on ``conn`` (the caller owns the transaction, so tests can roll it back)."""
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LENDING_SCHEMA}"'))
    LendingTextConsent.__table__.create(bind=conn, checkfirst=True)
    for stmt in STATEMENTS:
        conn.execute(text(stmt))


def apply(engine: Engine | None = None) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn)
    logger.info("apply_lending_pr319_client_feedback complete.")


if __name__ == "__main__":
    apply()
