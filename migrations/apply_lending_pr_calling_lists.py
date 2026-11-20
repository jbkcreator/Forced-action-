"""Create ``lending.pr_list_pulls`` and ``lending.pr_list_records``: PropertyRadar calling lists.

Verified maturity, Cash buyers and Wholesalers (config/lending_calling_lists.py), one row per list
and property, apart from the maturity pipeline's public.property_radar_records. Idempotent
(``checkfirst``); run after apply_lending_compliance.py, which creates the ``lending`` schema.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pr_calling_lists.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingPrListPull, LendingPrListRecord

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_WIDENED_COLUMNS: dict[str, int] = {
    "loan_type": 30, "property_type": 20, "owner_mailing_state": 20, "last_transfer_type": 40,
}


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    """``schema`` is overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingPrListPull.__table__.create(translated, checkfirst=True)
        LendingPrListRecord.__table__.create(translated, checkfirst=True)
        # Widened after the first live pull: PropertyRadar loan types run past 10 characters
        # ("Non-Conforming"). Re-running is a no-op once the columns are this wide.
        for column, width in _WIDENED_COLUMNS.items():
            conn.execute(text(f'ALTER TABLE "{schema}".pr_list_records ALTER COLUMN {column} TYPE varchar({width})'))
    logger.info("apply_lending_pr_calling_lists complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
