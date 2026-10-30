"""Index ``lending.loan_files.borrower_phone``.

Every lending Slack post names the person next to its id (``src/lending/slack_names.py``), looking the name up by
phone across the lead records; partner files are one of those sources and were the only one with no index on the
phone. Model: ``LendingLoanFile`` in ``src/lending/models.py``. Idempotent; run after ``apply_lending_partner_door.py``.
Safe to run any time: the code works without it, only the lookup is slower on a large table.

Usage:
    PYTHONPATH=. python migrations/apply_lending_loan_files_borrower_phone_index.py
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
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_lending_loan_files_borrower_phone '
                      f'ON "{schema}".loan_files (borrower_phone)'))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_loan_files_borrower_phone_index complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
