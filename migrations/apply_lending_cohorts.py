"""90-day cohort tracking (Runbook § 4.6, Item 6): lending.lendingflow_batches and lending.lendingflow_replacements.

* ``lendingflow_batches``: one row per LendingFlow purchase (name, purchase date, lead count). Delivered leads fill
  the batches in arrival order (src/lending/cohort_report.py).
* ``lendingflow_replacements``: one replacement request per LendingFlow lead, ``requested`` -> ``credited`` /
  ``denied``.

New tables only; nothing on call_dispositions. Idempotent; run after apply_lending_lendingflow.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_cohorts.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingFlowBatch, LendingFlowReplacement

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection) -> None:
    """Run on ``conn`` (the caller owns the transaction, so tests can roll it back)."""
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LENDING_SCHEMA}"'))
    LendingFlowBatch.__table__.create(bind=conn, checkfirst=True)
    LendingFlowReplacement.__table__.create(bind=conn, checkfirst=True)


def apply(engine: Engine | None = None) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn)
    logger.info("apply_lending_cohorts complete.")


if __name__ == "__main__":
    apply()
