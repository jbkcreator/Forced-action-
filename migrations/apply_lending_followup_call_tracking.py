"""14-day follow-up call tracking (Runbook § 4.1, Item 1): ``lending.call_dispositions.call_source``.

``batchdialer`` (default, every existing row) or ``ghl`` for calls made in GoHighLevel and imported by
``src.lending.ghl_call_import``. Dialer-only jobs act on ``batchdialer`` rows.

No view is created on purpose: apply_lending_call_dispositions_dialer.py re-applies column types on every run, and
PostgreSQL refuses to change the type of a column a view reads. The replacement rule is a query in
``src/lending/replacement_eligibility.py`` instead.

Idempotent; run after apply_lending_call_dispositions.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_followup_call_tracking.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_followup import CALL_SOURCE_BATCHDIALER
from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Checked first: even a no-op ADD COLUMN takes an exclusive lock on call_dispositions, which a test's rolled-back
# transaction would otherwise hold against the live table (the CDR poller writes it every 15 seconds).
_HAS_COLUMN = text("SELECT 1 FROM information_schema.columns WHERE table_schema = :schema "
                   "AND table_name = 'call_dispositions' AND column_name = 'call_source'")


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    if not conn.execute(_HAS_COLUMN, {"schema": schema}).scalar():
        conn.execute(text(f'ALTER TABLE "{schema}".call_dispositions ADD COLUMN call_source varchar(20) '
                          f"NOT NULL DEFAULT '{CALL_SOURCE_BATCHDIALER}'"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_followup_call_tracking complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
