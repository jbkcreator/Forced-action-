"""T-12 audio fallback: ``transcript_status`` on ``lending.call_dispositions``.

NULL = transcript still pending, ``done`` = fetched and extracted, ``missing`` = none within
30 minutes of the call end. The partial index serves the poller's pending query. Existing
answered calls older than the wait cap are expired by the poller on its first enabled tick
(never fetched). Idempotent; run after apply_lending_call_dispositions.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_call_transcript_status.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    table = f'"{schema}".call_dispositions'
    for ddl in (
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS transcript_status varchar(20)",
        f'CREATE INDEX IF NOT EXISTS ix_lending_call_dispositions_transcript_pending ON {table} (call_ended_at) '
        f"WHERE transcript_status IS NULL AND talk_duration_sec > 0",
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_call_transcript_status complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
