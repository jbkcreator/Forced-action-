"""Nightly PropertyRadar calling-list pipeline: the columns and checks it needs on the calling-list tables.

* ``lending.pr_list_pulls``: ``state`` (FL / GA; existing rows are Florida) and ``run_kind`` (manual / nightly),
  so the nightly export cap counts what the night already spent.
* ``lending.pr_list_records``: ``property_id`` / ``match_method`` / ``match_confidence`` / ``linked_at``, the link to
  a Forced Action property.
* Both tables accept the new ``investor_purchase`` list (the list_key checks are replaced).
* ``public.enrichment_usage_logs.trace_type``: the Tracerfy mode the skip-trace ledger reads; the trace code
  already writes it.

Idempotent; run after apply_lending_pr_calling_lists.py and BEFORE deploying the nightly code.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pr_list_nightly.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_LIST_KEYS = "list_key IN ('verified_maturity', 'cash_buyer', 'investor_purchase', 'wholesaler')"


def _statements(schema: str) -> list[str]:
    pulls, records = f'"{schema}".pr_list_pulls', f'"{schema}".pr_list_records'
    return [
        f"ALTER TABLE {pulls} ADD COLUMN IF NOT EXISTS state varchar(2) NOT NULL DEFAULT 'FL'",
        f"ALTER TABLE {pulls} ADD COLUMN IF NOT EXISTS run_kind varchar(10) NOT NULL DEFAULT 'manual'",
        f"ALTER TABLE {pulls} DROP CONSTRAINT IF EXISTS ck_lending_pr_list_pulls_run_kind",
        f"ALTER TABLE {pulls} ADD CONSTRAINT ck_lending_pr_list_pulls_run_kind CHECK (run_kind IN ('manual', 'nightly'))",
        f"ALTER TABLE {pulls} DROP CONSTRAINT IF EXISTS ck_lending_pr_list_pulls_list",
        f"ALTER TABLE {pulls} ADD CONSTRAINT ck_lending_pr_list_pulls_list CHECK ({_LIST_KEYS})",
        f"CREATE INDEX IF NOT EXISTS idx_lending_pr_list_pulls_started ON {pulls} (run_kind, started_at)",
        f"ALTER TABLE {records} ADD COLUMN IF NOT EXISTS property_id integer",
        f"ALTER TABLE {records} ADD COLUMN IF NOT EXISTS match_method varchar(30)",
        f"ALTER TABLE {records} ADD COLUMN IF NOT EXISTS match_confidence numeric(4, 3)",
        f"ALTER TABLE {records} ADD COLUMN IF NOT EXISTS linked_at timestamptz",
        f"ALTER TABLE {records} DROP CONSTRAINT IF EXISTS ck_lending_pr_list_records_list",
        f"ALTER TABLE {records} ADD CONSTRAINT ck_lending_pr_list_records_list CHECK ({_LIST_KEYS})",
        f"CREATE INDEX IF NOT EXISTS idx_lending_pr_list_records_unlinked ON {records} (list_key) WHERE linked_at IS NULL",
        f"CREATE INDEX IF NOT EXISTS idx_lending_pr_list_records_first_seen ON {records} (first_seen_at)",
    ]


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA, *, usage_log_table: str = "enrichment_usage_logs") -> None:
    """``schema`` / ``usage_log_table`` are overridable so tests never touch the shared tables."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        for statement in _statements(schema):
            conn.execute(text(statement))
        conn.execute(text(f"ALTER TABLE {usage_log_table} ADD COLUMN IF NOT EXISTS trace_type varchar(20)"))
    logger.info("apply_lending_pr_list_nightly complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
