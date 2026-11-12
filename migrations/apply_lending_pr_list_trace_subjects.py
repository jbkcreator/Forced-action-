"""Create ``lending.pr_list_trace_subjects``: who Tracerfy is sent for each name-only person on a calling list.

Idempotent (``checkfirst``); run after apply_lending_pr_calling_lists.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pr_list_trace_subjects.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingPrListTraceSubject

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingPrListTraceSubject.__table__.create(translated, checkfirst=True)
    logger.info("apply_lending_pr_list_trace_subjects complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
