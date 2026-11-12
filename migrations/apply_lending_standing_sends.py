"""P1.4 Standing Sends Tracker: ``lending.card_edits`` (before/after of every approval-card Revise) and
``lending.template_graduations`` (template versions graduated to standing auto-send).

Idempotent; run after apply_lending_cora_agent_core.py (creates ``lending.pending_actions``). Run it BEFORE
deploying the code: Cora's revise handler and sweep write these tables.

Usage:
    PYTHONPATH=. python migrations/apply_lending_standing_sends.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingCardEdit, LendingTemplateGraduation

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# Indexes on pending_actions for the sweep's incremental lookups (decisions / revisions since the previous run) and the
# per-template counts. Each is checked first: even CREATE INDEX IF NOT EXISTS takes a lock on the live table.
_HAS_INDEX = text("SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :index")
_PENDING_ACTION_INDEXES = (
    ("ix_pending_actions_decided_at", "(decided_at)"),
    ("ix_pending_actions_updated_at", "(updated_at)"),
    ("ix_pending_actions_template_version",
     "((payload->>'template_version')) WHERE payload ? 'template_version'"),
)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingCardEdit.__table__.create(translated, checkfirst=True)
    LendingTemplateGraduation.__table__.create(translated, checkfirst=True)
    for index, definition in _PENDING_ACTION_INDEXES:
        if not conn.execute(_HAS_INDEX, {"schema": schema, "index": index}).scalar():
            conn.execute(text(f'CREATE INDEX {index} ON "{schema}".pending_actions {definition}'))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_standing_sends complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
