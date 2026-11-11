"""Add ``lending.call_dispositions.recording_view_url``.

The BatchDialer contact page (``https://app.batchdialer.com/contacts/view/{dialer_contact_id}``), where a contact's call
recordings play: BatchDialer's own recording link (``recording_ref``) answers 404 to an API key for every call. Filled
on ingest by ``src/lending/dispositions.py``; existing rows are filled by
``migrations/apply_lending_batchdialer_call_review_backfill.py``. Model: ``LendingCallDisposition`` in
``src/lending/models.py``. Idempotent. RUN BEFORE deploying the code: the call insert writes this column.

Usage:
    PYTHONPATH=. python migrations/apply_lending_recording_view_url.py
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
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text(f'ALTER TABLE "{schema}".call_dispositions '
                      "ADD COLUMN IF NOT EXISTS recording_view_url varchar(500)"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_recording_view_url complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
