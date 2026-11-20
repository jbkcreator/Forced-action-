"""``lending.loan_file_status_events`` (P1.2): one row per status change of a loan file (partner or LendingFlow).

``loan_files.status`` and ``status_at`` are overwritten by every change, so the nightly ledger export cannot read a
file's history from them. ``partners.record_status_change`` (the one writer of a file's status) and ``save_submission``
add a row in the same transaction as the change; the export reads these rows by time.

Idempotent. Existing partner files get one starting row (their current status, at its ``status_at``) when they have no
event yet, so they are not blank in the export. Run it BEFORE deploying: without the table a status change still
happens, but its event is not recorded (an error is logged).

Usage:
    PYTHONPATH=. python migrations/apply_lending_loan_file_status_events.py
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
    events, files = f'"{schema}".loan_file_status_events', f'"{schema}".loan_files'
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {events} (
            id           bigserial    PRIMARY KEY,
            loan_file_id bigint       NOT NULL REFERENCES {files} (id) ON DELETE CASCADE,
            status       varchar(20)  NOT NULL,
            reason       varchar(200),
            at           timestamptz  NOT NULL DEFAULT now()
        )"""))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_lending_loan_file_status_events_at ON {events} (at)"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_lending_loan_file_status_events_file ON {events} (loan_file_id, at)"))
    conn.execute(text(f"""
        INSERT INTO {events} (loan_file_id, status, reason, at)
        SELECT f.id, f.status, f.status_reason, f.status_at FROM {files} f
         WHERE f.partner_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM {events} e WHERE e.loan_file_id = f.id)"""))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_loan_file_status_events complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
