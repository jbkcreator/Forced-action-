"""P2.1 Speed-to-Lead: ``lending.speed_to_lead`` (one row per new lead: intro-text and call-cascade timings).

Idempotent. Run it BEFORE deploying the code: the LendingFlow follow-up and the website-lead delivery write this
table. Until it exists the speed-to-lead step logs an error and does nothing; lead delivery is never affected.

Usage:
    PYTHONPATH=. python migrations/apply_lending_speed_to_lead.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingSpeedToLead

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingSpeedToLead.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_speed_to_lead complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
