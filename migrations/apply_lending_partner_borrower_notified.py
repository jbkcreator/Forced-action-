"""Partner Door (P-1), Chunk 1 Addendum 1.1.1: borrower-contact record and withheld pre-qual letters.

``lending.partner_submissions.borrower_notified`` (default false) and ``borrower_consent_at`` record that the
borrower on a partner file has not been contacted until the ISA or Josh logs consent from a live first contact
(partner consent is never borrower consent). ``prequal_withheld_at`` marks a partner pre-qual letter that was not
sent because ``LENDING_PREQUAL_PDF_ENABLED`` was off; it is never sent later. Model: ``LendingPartnerSubmission``
in ``src/lending/models.py``. Idempotent; run after ``apply_lending_partner_door.py``.

DEPLOY ORDER: run this BEFORE deploying the code that uses it. ``partner_pipeline._after_routing`` reads
``prequal_withheld_at`` for every partner file.

Usage:
    PYTHONPATH=. python migrations/apply_lending_partner_borrower_notified.py
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
    table = f'"{schema}".partner_submissions'
    for ddl in (
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS prequal_withheld_at timestamptz",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS borrower_notified boolean NOT NULL DEFAULT false",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS borrower_consent_at timestamptz",
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_partner_borrower_notified complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
