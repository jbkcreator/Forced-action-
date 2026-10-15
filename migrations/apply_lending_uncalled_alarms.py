"""T-10: ``lending.uncalled_alarms``, the 2-minute / 5-minute uncalled-lead stopwatch per LendingFlow lead.

Also closes every LendingFlow lead that exists when this runs (``resolved_reason = 'preexisting'``), so switching
the alarms on never pages Josh about old or test leads. Re-run it right before setting
LENDING_UNCALLED_ALARMS_ENABLED=true. No foreign key to T-11's table; the backfill is skipped while that table
does not exist. Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema).

Usage:
    PYTHONPATH=. python migrations/apply_lending_uncalled_alarms.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingUncalledAlarm

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# create never alters an existing table; changes after the first apply go here (safe to re-run).
_ALTERS = [
    # delivery retries: attempt counters per channel, next retry time, retry claim time
    'ALTER TABLE "{t}".uncalled_alarms ADD COLUMN IF NOT EXISTS sms_120_attempts INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE "{t}".uncalled_alarms ADD COLUMN IF NOT EXISTS sms_300_attempts INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE "{t}".uncalled_alarms ADD COLUMN IF NOT EXISTS slack_300_attempts INTEGER NOT NULL DEFAULT 0',
    'ALTER TABLE "{t}".uncalled_alarms ADD COLUMN IF NOT EXISTS retry_at TIMESTAMPTZ',
    'ALTER TABLE "{t}".uncalled_alarms ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ',
]

_BACKFILL = """
    INSERT INTO "{t}".uncalled_alarms (lendingflow_lead_id, lead_uuid, phone, arrived_at, resolved_at, resolved_reason)
    SELECT id, lead_uuid::text, phone, received_at, now(), 'preexisting' FROM "{t}".lendingflow_leads
    ON CONFLICT (lendingflow_lead_id) DO NOTHING
"""


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingUncalledAlarm.__table__.create(translated, checkfirst=True)
    for stmt in _ALTERS:
        conn.execute(text(stmt.format(t=schema)))
    if conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{schema}.lendingflow_leads"}).scalar() is not None:
        closed = conn.execute(text(_BACKFILL.format(t=schema))).rowcount
        logger.info("apply_lending_uncalled_alarms: %d existing LendingFlow lead(s) marked preexisting.", closed)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_uncalled_alarms complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
