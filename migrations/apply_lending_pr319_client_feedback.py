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
    "ALTER TABLE lending.call_dispositions ADD COLUMN IF NOT EXISTS recording_status varchar(12)",
    "ALTER TABLE lending.call_dispositions ADD COLUMN IF NOT EXISTS recording_checked_at timestamptz",
    "UPDATE lending.call_dispositions SET recording_status = 'pending' "
    "WHERE recording_ref IS NOT NULL AND recording_status IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_lending_call_dispositions_recording_todo "
    "ON lending.call_dispositions (recording_checked_at) WHERE recording_status IN ('pending', 'forbidden')",
    "ALTER TABLE lending.call_dispositions ADD COLUMN IF NOT EXISTS dialer_campaign_id varchar(64)",
    "UPDATE lending.call_dispositions SET dialer_campaign_id = COALESCE(raw_event->'campaign'->>'id', raw_event->>'campaign_id', "
    "raw_event->'data'->'campaign'->>'id', raw_event->'data'->>'campaign_id') "
    "WHERE dialer_campaign_id IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_lending_call_dispositions_dialer_campaign_ended "
    "ON lending.call_dispositions (dialer_campaign_id, call_ended_at)",
)


def apply_to(conn: Connection) -> None:
    """Run every step on ``conn`` (the caller owns the transaction, so tests can roll it back)."""
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LENDING_SCHEMA}"'))
    LendingTextConsent.__table__.create(bind=conn, checkfirst=True)
    for stmt in STATEMENTS:
        conn.execute(text(stmt))


def apply(engine: Engine | None = None) -> None:
    engine = engine or create_engine(get_settings().lending_database_url or get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn)
    logger.info("apply_lending_pr319_client_feedback complete.")


if __name__ == "__main__":
    apply()
