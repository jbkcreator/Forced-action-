"""Opt-out sync to GoHighLevel: ``lending.opt_out_events.ghl_dnd_at``.

Set when the opt-out has been written to GHL as do-not-disturb; NULL means the
opt-out poller still has to (or must retry) the GHL update. Idempotent; run after
apply_lending_compliance.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_opt_out_ghl.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_STATEMENTS = [
    'ALTER TABLE "{t}".opt_out_events ADD COLUMN IF NOT EXISTS ghl_dnd_at timestamptz',
    'CREATE INDEX IF NOT EXISTS idx_lending_opt_out_events_ghl_pending ON "{t}".opt_out_events (id) '
    "WHERE ghl_dnd_at IS NULL AND phone_hash IS NOT NULL",
]


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    for statement in _STATEMENTS:
        conn.execute(text(statement.format(t=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_opt_out_ghl complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
