"""T-10: ``lending.uncalled_alarms``, the 2-minute / 5-minute uncalled-lead stopwatch per LendingFlow lead.

Website leads (``lending.web_leads``, client Q53) are watched too: ``web_lead_id`` is added next to
``lendingflow_lead_id``; the website leads that exist when that column is first added are closed as 'preexisting'
(once, so a later re-run never closes a lead the worker has not picked up yet).

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
    # website leads: a second lead source next to lendingflow_lead_id (exactly one of the two is set)
    'ALTER TABLE "{t}".uncalled_alarms ADD COLUMN IF NOT EXISTS web_lead_id INTEGER',
    'ALTER TABLE "{t}".uncalled_alarms ALTER COLUMN lendingflow_lead_id DROP NOT NULL',
    'CREATE UNIQUE INDEX IF NOT EXISTS uq_lending_uncalled_alarms_web_lead ON "{t}".uncalled_alarms (web_lead_id)',
    'ALTER TABLE "{t}".uncalled_alarms DROP CONSTRAINT IF EXISTS ck_lending_uncalled_alarms_one_source',
    'ALTER TABLE "{t}".uncalled_alarms ADD CONSTRAINT ck_lending_uncalled_alarms_one_source '
    'CHECK ((lendingflow_lead_id IS NULL) <> (web_lead_id IS NULL))',
]

_BACKFILL = """
    INSERT INTO "{t}".uncalled_alarms (lendingflow_lead_id, lead_uuid, phone, arrived_at, resolved_at, resolved_reason)
    SELECT id, lead_uuid::text, phone, received_at, now(), 'preexisting' FROM "{t}".lendingflow_leads
    ON CONFLICT (lendingflow_lead_id) DO NOTHING
"""

_BACKFILL_WEB = """
    INSERT INTO "{t}".uncalled_alarms (web_lead_id, lead_uuid, phone, arrived_at, resolved_at, resolved_reason)
    SELECT id, 'web-' || id, phone, received_at, now(), 'preexisting' FROM "{t}".web_leads
    ON CONFLICT (web_lead_id) DO NOTHING
"""


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    first_web_apply = conn.execute(text(  # also true when the table does not exist yet
        "SELECT NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = :s "
        "AND table_name = 'uncalled_alarms' AND column_name = 'web_lead_id')"), {"s": schema}).scalar()
    LendingUncalledAlarm.__table__.create(translated, checkfirst=True)
    for stmt in _ALTERS:
        conn.execute(text(stmt.format(t=schema)))
    if first_web_apply and conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{schema}.web_leads"}).scalar() is not None:
        closed = conn.execute(text(_BACKFILL_WEB.format(t=schema))).rowcount
        logger.info("apply_lending_uncalled_alarms: %d existing website lead(s) marked preexisting.", closed)
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
