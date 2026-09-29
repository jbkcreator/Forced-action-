"""Create the ``lending`` schema and Wave 0 compliance-floor tables.

Tables: suppression_list, contacts, opt_out_events, load_exclusions
(src/core/lending_models.py). Then backfills lending.suppression_list from FA
opt-outs + Tracerfy litigators via reconcile_suppression. Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_lending_compliance.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.core.lending_models import LENDING_SCHEMA, LendingBase

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# create_all never alters an existing table; columns added after first apply go here.
_ALTERS = [
    "ALTER TABLE \"{t}\".suppression_list ALTER COLUMN created_at SET DEFAULT now()",
    "ALTER TABLE \"{t}\".contacts ALTER COLUMN created_at SET DEFAULT now()",
    "ALTER TABLE \"{t}\".opt_out_events ALTER COLUMN received_at SET DEFAULT now()",
    "ALTER TABLE \"{t}\".load_exclusions ALTER COLUMN created_at SET DEFAULT now()",
    "ALTER TABLE \"{t}\".contacts ADD COLUMN IF NOT EXISTS line_type varchar(20)",
]


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA, source_schema: str = "public") -> None:
    """``schema`` / ``source_schema`` are overridable so tests never touch the shared schema."""
    from src.services.lending_compliance import reconcile_suppression

    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingBase.metadata.create_all(translated, checkfirst=True)
        for stmt in _ALTERS:
            conn.execute(text(stmt.format(t=schema)))
        added = reconcile_suppression(Session(bind=conn), source_schema=source_schema, target_schema=schema)
    logger.info("apply_lending_compliance complete (schema=%s, suppression rows added=%d).", schema, added)


if __name__ == "__main__":
    apply()
