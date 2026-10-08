"""WP-GL-9: text-back decision columns and the day-slot index on ``lending.missed_call_events``.

Adds decided_at / template_key / provider_message_id, and replaces the one-sendable-event-per-
phone-per-day unique index so only pending / sending / sent / send_unknown events hold the slot
(a skipped, dry-run or failed text frees it). Idempotent; run after
apply_lending_call_dispositions_dialer.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_gl9_text_back.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_text_back import SLOT_HOLDING_SQL
from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    table = f'"{schema}".missed_call_events'
    for ddl in (
        f'DROP INDEX IF EXISTS "{schema}".uq_lending_missed_call_phone_day',
        f"ALTER TABLE {table} ALTER COLUMN status TYPE varchar(24)",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS decided_at timestamptz",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS template_key varchar(20)",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS provider_message_id varchar(100)",
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_lending_missed_call_phone_day ON {table} (phone, event_date_et) "
        f"WHERE {SLOT_HOLDING_SQL}",
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_gl9_text_back complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
