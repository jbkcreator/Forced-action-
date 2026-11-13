"""P2.4 closing blocker card and document chase tables: ``lending.ghl_opportunity_stage`` (each GHL opportunity's
current stage and since when, backfilled once from ``lending.ghl_stage_events``), ``doc_chases``, ``doc_chase_items``,
``doc_chase_uploads``, ``doc_chase_touches`` and ``closing_blocker_cards``.

Idempotent; run after apply_lending_ghl_stage_events.py. RUN BEFORE deploying the code: the stage webhook writes
``ghl_opportunity_stage``.

Usage:
    PYTHONPATH=. python migrations/apply_lending_doc_chase.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import (
    LENDING_SCHEMA,
    LendingClosingBlockerCard,
    LendingDocChase,
    LendingDocChaseItem,
    LendingDocChaseTouch,
    LendingDocChaseUpload,
    LendingGhlOpportunityStage,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = (LendingGhlOpportunityStage, LendingDocChase, LendingDocChaseItem, LendingDocChaseUpload,
          LendingDocChaseTouch, LendingClosingBlockerCard)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    for model in TABLES:
        model.__table__.create(translated, checkfirst=True)
    # Deals that moved before this table existed: their latest stage entry. Never overwrites a live row.
    conn.execute(text(
        f'INSERT INTO "{schema}".ghl_opportunity_stage (ghl_opportunity_id, stage_name, stage_key, entered_at, phone) '
        f'SELECT DISTINCT ON (ghl_opportunity_id) ghl_opportunity_id, stage_name, stage_key, event_at, phone '
        f'FROM "{schema}".ghl_stage_events ORDER BY ghl_opportunity_id, event_at DESC '
        "ON CONFLICT (ghl_opportunity_id) DO NOTHING"))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_doc_chase complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
