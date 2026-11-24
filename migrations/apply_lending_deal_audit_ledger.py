"""R1 item 0.8 — the deal-level immutable audit ledger.

Creates ``lending.deal_audit_events``: one row per stage move, quote, rule version, field change, Cora draft, human edit
and approval on a loan file, each tagged ``cora`` / ``human:<id>`` / ``system:<source>`` and with the lender rule
snapshot in force.  Triggers refuse UPDATE, DELETE and TRUNCATE, so history cannot be changed from SQL or code.

Backfill (once, idempotent, actor ``system:backfill``, rule version NULL because none was recorded): a ``file_created``
row per existing loan file and a ``status_change`` row per existing ``loan_file_status_events`` row.

Idempotent; run after apply_lending_loan_state_machine.py and apply_lending_lender_rule_snapshots.py.
RUN BEFORE deploying the code: every loan-file write now records an event.

Usage:
    PYTHONPATH=. python migrations/apply_lending_deal_audit_ledger.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import CreateIndex

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingDealAuditEvent

logger = logging.getLogger(__name__)

_APPEND_ONLY_FUNCTION = """
CREATE OR REPLACE FUNCTION "{s}".deal_audit_events_append_only() RETURNS trigger AS $fn$
BEGIN
    RAISE EXCEPTION 'deal_audit_events is append-only';
END
$fn$ LANGUAGE plpgsql
"""

_BACKFILL_FILES = """
INSERT INTO "{s}".deal_audit_events (loan_file_id, event_type, actor, detail, dedupe_key, occurred_at)
SELECT f.id, 'file_created', 'system:backfill', jsonb_build_object('source', f.source), 'file:' || f.id, f.created_at
  FROM "{s}".loan_files f
ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
"""

_BACKFILL_STATUS = """
INSERT INTO "{s}".deal_audit_events (loan_file_id, event_type, actor, diff, detail, ref_table, ref_id, dedupe_key, occurred_at)
SELECT e.loan_file_id, 'status_change', 'system:backfill',
       jsonb_build_object('status', jsonb_build_object('before', NULL, 'after', e.status)),
       jsonb_build_object('reason', e.reason), 'loan_file_status_events', e.id::text, 'status_event:' || e.id, e.at
  FROM "{s}".loan_file_status_events e
ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
"""


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    table = LendingDealAuditEvent.__table__
    table.create(translated, checkfirst=True)
    for index in table.indexes:
        translated.execute(CreateIndex(index, if_not_exists=True))
    # A CHECK added after a first run would never reach an existing table; drop and re-add it.
    conn.exec_driver_sql(f'ALTER TABLE "{schema}".deal_audit_events DROP CONSTRAINT IF EXISTS ck_lending_deal_audit_events_actor')
    conn.exec_driver_sql(f'ALTER TABLE "{schema}".deal_audit_events ADD CONSTRAINT ck_lending_deal_audit_events_actor '
                         "CHECK (actor ~ '^(cora|human:[^ ]+|system:[^ ]+)$')")

    conn.exec_driver_sql(_APPEND_ONLY_FUNCTION.format(s=schema))
    conn.exec_driver_sql(f'DROP TRIGGER IF EXISTS deal_audit_events_no_change ON "{schema}".deal_audit_events')
    conn.exec_driver_sql(f'CREATE TRIGGER deal_audit_events_no_change BEFORE UPDATE OR DELETE ON "{schema}".deal_audit_events '
                         f'FOR EACH ROW EXECUTE FUNCTION "{schema}".deal_audit_events_append_only()')
    conn.exec_driver_sql(f'DROP TRIGGER IF EXISTS deal_audit_events_no_truncate ON "{schema}".deal_audit_events')
    conn.exec_driver_sql(f'CREATE TRIGGER deal_audit_events_no_truncate BEFORE TRUNCATE ON "{schema}".deal_audit_events '
                         f'FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".deal_audit_events_append_only()')

    # Backfill once: only while the ledger is still empty. Live writers use their own keys, so a re-run after
    # deployment would otherwise add a second copy of activity the live code already recorded.
    if not conn.execute(text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".deal_audit_events)')).scalar():
        conn.execute(text(_BACKFILL_FILES.format(s=schema)))
        conn.execute(text(_BACKFILL_STATUS.format(s=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        apply_to(conn, schema)
    logger.info("apply_lending_deal_audit_ledger complete (schema=%s).", schema)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    apply()
