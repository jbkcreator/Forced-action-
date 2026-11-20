"""P2.1 Speed-to-Lead: ``lending.speed_to_lead`` (one row per new lead: intro-text and call-cascade timings, plus the
call queue columns ``queued_at`` / ``queue_reason`` / ``released_at``), ``lending.speed_to_lead_notices`` (queue
notices posted once per day) and the website form's AI-call consent box on ``lending.web_leads``
(``ai_call_consent``, ``ai_call_consent_text``, ``ai_call_consent_text_matches``; existing rows get false / NULL, i.e.
no AI-call consent).

Idempotent. Run it BEFORE deploying the code: the LendingFlow follow-up and the website-lead delivery write the table,
the website form inserts the new columns, and the uncalled-alarm worker reads the queue. Until it exists the
speed-to-lead step logs an error and does nothing; lead delivery is never affected.

Usage:
    PYTHONPATH=. python migrations/apply_lending_speed_to_lead.py
"""
from __future__ import annotations

import logging
from typing import cast

from sqlalchemy import Table, create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingSpeedToLead, LendingSpeedToLeadNotice

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


_HAS_COLUMN = text("SELECT 1 FROM information_schema.columns "
                   "WHERE table_schema = :schema AND table_name = :table AND column_name = :column")
# Checked first: even ADD COLUMN IF NOT EXISTS takes a lock on the live table. A constant default is metadata-only.
_NEW_COLUMNS = (
    ("web_leads", "ai_call_consent", "boolean NOT NULL DEFAULT false"),
    ("web_leads", "ai_call_consent_text", "text"),
    ("web_leads", "ai_call_consent_text_matches", "boolean"),
    ("speed_to_lead", "queued_at", "timestamptz"),
    ("speed_to_lead", "queue_reason", "varchar(60)"),
    ("speed_to_lead", "released_at", "timestamptz"),
    ("speed_to_lead", "recheck_at", "timestamptz"),
    ("speed_to_lead", "alerted_at", "timestamptz"),
)
_INDEXES = (
    "CREATE INDEX IF NOT EXISTS ix_speed_to_lead_queued ON \"{schema}\".speed_to_lead (received_at) "
    "WHERE call_status = 'queued'",
    "CREATE INDEX IF NOT EXISTS ix_speed_to_lead_call_tagged_at ON \"{schema}\".speed_to_lead (call_tagged_at)",
)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    for model in (LendingSpeedToLead, LendingSpeedToLeadNotice):
        cast(Table, model.__table__).create(translated, checkfirst=True)
    for table, column, definition in _NEW_COLUMNS:
        if not conn.execute(_HAS_COLUMN, {"schema": schema, "table": table, "column": column}).scalar():
            conn.execute(text(f'ALTER TABLE "{schema}".{table} ADD COLUMN IF NOT EXISTS {column} {definition}'))
    for sql in _INDEXES:
        conn.execute(text(sql.format(schema=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_speed_to_lead complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
