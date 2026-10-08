"""lending.ghl_stage_events: GHL pipeline stage entries from the workflow webhook (scoreboard "showed"). Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_lending_ghl_stage_events.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingGhlStageEvent

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection) -> None:
    """Run on ``conn`` (the caller owns the transaction, so tests can roll it back)."""
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LENDING_SCHEMA}"'))
    LendingGhlStageEvent.__table__.create(bind=conn, checkfirst=True)


def apply(engine: Engine | None = None) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn)
    logger.info("apply_lending_ghl_stage_events complete.")


if __name__ == "__main__":
    apply()
