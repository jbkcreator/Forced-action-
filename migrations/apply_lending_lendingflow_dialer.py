"""LendingFlow -> BatchDialer: dialer state on ``lending.lendingflow_leads`` + GHL note state on ``lending.call_dispositions``.

Leads already stored when the columns are added are marked ``preexisting`` (never loaded into the dialer); only leads
that arrive afterwards start ``pending``. Idempotent; run after apply_lending_lendingflow.py and
apply_lending_call_dispositions_dialer.py, and BEFORE deploying the code (the sweep reads these columns).

Usage:
    PYTHONPATH=. python migrations/apply_lending_lendingflow_dialer.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_STATUSES = ("pending", "loading", "loaded", "blocked", "unconfirmed", "remove_pending", "removed", "duplicate",
             "expired", "failed", "preexisting")

# dialer_status is added without a default first, so existing rows can be told apart and marked preexisting.
_ADD_STATUS = 'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_status varchar(20)'
_BACKFILL = 'UPDATE "{t}".lendingflow_leads SET dialer_status = \'preexisting\' WHERE dialer_status IS NULL'

_STATEMENTS: list[str] = [
    'ALTER TABLE "{t}".lendingflow_leads ALTER COLUMN dialer_status SET DEFAULT \'pending\'',
    'ALTER TABLE "{t}".lendingflow_leads ALTER COLUMN dialer_status SET NOT NULL',
    'ALTER TABLE "{t}".lendingflow_leads DROP CONSTRAINT IF EXISTS ck_lending_lendingflow_leads_dialer_status',
    'ALTER TABLE "{t}".lendingflow_leads ADD CONSTRAINT ck_lending_lendingflow_leads_dialer_status '
    "CHECK (dialer_status IN (" + ", ".join(f"'{s}'" for s in _STATUSES) + "))",
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_block_reason varchar(40)',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_attempts integer NOT NULL DEFAULT 0',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_failures integer NOT NULL DEFAULT 0',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_last_error varchar(200)',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_claimed_at timestamptz',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_next_attempt_at timestamptz',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_loaded_at timestamptz',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_removed_at timestamptz',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dialer_ghl_checked_at timestamptz',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS duplicate_of bigint '
    'REFERENCES "{t}".lendingflow_leads(id)',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS handled_in_ghl_at timestamptz',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS handled_in_ghl_reason varchar(30)',
    'CREATE INDEX IF NOT EXISTS idx_lending_lendingflow_leads_dialer_pending ON "{t}".lendingflow_leads (received_at) '
    "WHERE dialer_status = 'pending'",
    # DNC check before the GHL push. Existing leads (and every lead while the check is off) are 'skipped'.
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dnc_status varchar(12) NOT NULL DEFAULT \'skipped\'',
    'ALTER TABLE "{t}".lendingflow_leads DROP CONSTRAINT IF EXISTS ck_lending_lendingflow_leads_dnc_status',
    'ALTER TABLE "{t}".lendingflow_leads ADD CONSTRAINT ck_lending_lendingflow_leads_dnc_status '
    "CHECK (dnc_status IN ('pending', 'checking', 'clear', 'dnc', 'skipped'))",
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dnc_result varchar(30)',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dnc_claimed_at timestamptz',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS dnc_checked_at timestamptz',
]
_CALL_DISPOSITION_STATEMENTS: list[str] = [
    'ALTER TABLE "{t}".call_dispositions ADD COLUMN IF NOT EXISTS ghl_note_at timestamptz',
    'ALTER TABLE "{t}".call_dispositions ADD COLUMN IF NOT EXISTS ghl_note_disposition varchar(50)',
    'ALTER TABLE "{t}".call_dispositions ADD COLUMN IF NOT EXISTS ghl_note_attempts smallint NOT NULL DEFAULT 0',
]


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    """Also called at the end of apply_lending_lendingflow.apply_to, so the lead table never exists without these
    columns. call_dispositions is altered only when it exists."""
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text(_ADD_STATUS.format(t=schema)))
    conn.execute(text(_BACKFILL.format(t=schema)))
    for stmt in _STATEMENTS:
        conn.execute(text(stmt.format(t=schema)))
    if conn.execute(text("SELECT to_regclass(:t)"), {"t": f'"{schema}".call_dispositions'}).scalar() is not None:
        for stmt in _CALL_DISPOSITION_STATEMENTS:
            conn.execute(text(stmt.format(t=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_lendingflow_dialer complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
