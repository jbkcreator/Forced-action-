"""G3 (2026-11-03): ``lending.ghl_disposition_events``, GHL call outcomes posted by the disposition workflow and
held until src.lending.ghl_disposition_match attaches each to its imported GHL call. Idempotent; run after
apply_lending_call_dispositions.py (the table references call_dispositions).

The code is safe to deploy before this runs: the webhook answers 500 and the matcher logs and matches nothing.

Usage:
    PYTHONPATH=. python migrations/apply_lending_ghl_disposition_events.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingGhlDispositionEvent

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_HAS_TABLE = text("SELECT 1 FROM information_schema.tables WHERE table_schema = :schema "
                  "AND table_name = 'ghl_disposition_events'")


def apply_to(conn: Connection) -> None:
    """Run on ``conn`` (the caller owns the transaction, so tests can roll it back)."""
    if conn.execute(_HAS_TABLE, {"schema": LENDING_SCHEMA}).scalar():
        return
    # The foreign key briefly locks call_dispositions, which the CDR poller writes every few seconds.
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    LendingGhlDispositionEvent.__table__.create(bind=conn, checkfirst=True)


def apply(engine: Engine | None = None) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn)
    logger.info("apply_lending_ghl_disposition_events complete.")


if __name__ == "__main__":
    apply()
