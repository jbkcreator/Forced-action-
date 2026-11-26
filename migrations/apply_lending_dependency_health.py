"""R1 0.6 dependency fallbacks: ``lending.dependency_health`` (when each outside service or feed last worked),
``lending.dependency_alerts`` (one row per alert episode) and ``lending.exhausted_phones`` (phones that reached the
total attempt cap; never reloaded from a list until a new inbound). Idempotent; run BEFORE deploying the code (the
feeds write health rows, the list gates read exhausted marks).

Usage:
    PYTHONPATH=. python migrations/apply_lending_dependency_health.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import (LENDING_SCHEMA, LendingDependencyAlert, LendingDependencyHealth,
                                LendingExhaustedPhone)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MODELS = (LendingDependencyHealth, LendingDependencyAlert, LendingExhaustedPhone)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    for model in MODELS:
        model.__table__.create(translated, checkfirst=True)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_dependency_health complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
