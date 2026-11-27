"""``lending.clay_enrichments`` (R1 1.3): one Clay lookup per morning-list item and field (email / llc_owner).
Rows are written by ``src.lending.morning_list.clay``; the morning list reads the found values.

Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema). Run it before setting
LENDING_CLAY_DAILY_CAP above 0 (until then Clay is never called and the list skips Clay results quietly).

Usage:
    PYTHONPATH=. python migrations/apply_lending_clay_enrichments.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingClayEnrichment

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingClayEnrichment.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_clay_enrichments complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
