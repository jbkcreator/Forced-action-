"""P1.4 Standing Sends Tracker: ``lending.card_edits`` (before/after of every approval-card Revise) and
``lending.template_graduations`` (template versions graduated to standing auto-send).

Idempotent; run after apply_lending_cora_agent_core.py (creates ``lending.pending_actions``). Run it BEFORE
deploying the code: Cora's revise handler and sweep write these tables.

Usage:
    PYTHONPATH=. python migrations/apply_lending_standing_sends.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingCardEdit, LendingTemplateGraduation

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingCardEdit.__table__.create(translated, checkfirst=True)
    LendingTemplateGraduation.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_standing_sends complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
