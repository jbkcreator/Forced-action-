"""Uncalled-lead alarms in working hours only (client list Nov 2, C1): allow ``resolved_reason = 'after_hours'``.

An alarm due outside 09:00-19:15 ET Mon-Fri is not sent; the worker closes the lead 'after_hours' and the 08:55
morning list shows it. This only widens ``ck_lending_uncalled_alarms_reason``; no row is touched. Kept apart from
apply_lending_uncalled_alarms.py on purpose: re-running that one closes every LendingFlow lead not picked up yet as
'preexisting'. Idempotent; run after apply_lending_uncalled_alarms.py and BEFORE deploying the worker change (the
worker writes 'after_hours').

Usage:
    PYTHONPATH=. python migrations/apply_lending_uncalled_alarms_after_hours.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ALTERS = [
    'ALTER TABLE "{t}".uncalled_alarms DROP CONSTRAINT IF EXISTS ck_lending_uncalled_alarms_reason',
    'ALTER TABLE "{t}".uncalled_alarms ADD CONSTRAINT ck_lending_uncalled_alarms_reason '
    "CHECK (resolved_reason IN ('connected', 'escalated', 'preexisting', 'after_hours'))",
]


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    if conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{schema}.uncalled_alarms"}).scalar() is None:
        logger.info("apply_lending_uncalled_alarms_after_hours: no uncalled_alarms table yet; run "
                    "apply_lending_uncalled_alarms.py first (it creates the table with this constraint).")
        return
    for stmt in _ALTERS:
        conn.execute(text(stmt.format(t=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_uncalled_alarms_after_hours complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
