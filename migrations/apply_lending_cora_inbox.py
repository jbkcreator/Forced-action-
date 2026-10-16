"""``lending.cora_inbox``: events Cora raises with the operator in her main thread before they go to a team
channel. Rows are added by ``src.lending.cora_inbox``.

Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema).

Usage:
    PYTHONPATH=. python migrations/apply_lending_cora_inbox.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingCoraInboxItem

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Each later change is checked before it runs: even a no-op ALTER or DROP INDEX takes an exclusive table lock,
# which a test's rolled-back transaction would otherwise hold against the shared database for the whole test.
_HAS_COLUMN = text("SELECT 1 FROM information_schema.columns WHERE table_schema = :schema "
                   "AND table_name = 'cora_inbox' AND column_name = :column")
_HAS_INDEX = text("SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :index")
# Columns added after the table first shipped; a table created above already has them.
_LATER_COLUMNS = (("channel_posted_at", "timestamptz"), ("routed_channel", "varchar(64)"), ("decision_note", "text"))


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingCoraInboxItem.__table__.create(translated, checkfirst=True)

    for column, ddl in _LATER_COLUMNS:
        if not conn.execute(_HAS_COLUMN, {"schema": schema, "column": column}).scalar():
            conn.execute(text(f'ALTER TABLE "{schema}".cora_inbox ADD COLUMN {column} {ddl}'))

    # One digest post lists many items, so the post no longer identifies a single row.
    if conn.execute(_HAS_INDEX, {"schema": schema, "index": "uq_cora_inbox_surfaced_post"}).scalar():
        conn.execute(text(f'DROP INDEX "{schema}".uq_cora_inbox_surfaced_post'))
    if not conn.execute(_HAS_INDEX, {"schema": schema, "index": "ix_cora_inbox_surfaced_post"}).scalar():
        conn.execute(text(f'CREATE INDEX ix_cora_inbox_surfaced_post ON "{schema}".cora_inbox '
                          "(surfaced_channel, surfaced_ts) WHERE surfaced_ts IS NOT NULL"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_cora_inbox complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
