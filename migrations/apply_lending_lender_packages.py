"""Secondary lender packages: ``lending.lender_packages`` and the quarantine / document-type columns on both upload
tables (``doc_chase_uploads``, ``partner_file_uploads``).

Idempotent. RUN BEFORE deploying the code: the upload routes write the quarantine columns.

Usage:
    PYTHONPATH=. python migrations/apply_lending_lender_packages.py
"""
from __future__ import annotations

import logging

from sqlalchemy import Table, create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import CreateIndex

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingLenderPackage

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = (LendingLenderPackage,)
# Columns this script adds to tables other migrations create, with their DDL types.
ADDED_COLUMNS: dict[str, dict[str, str]] = {
    "lender_packages": {"overrides": "jsonb NOT NULL DEFAULT '{}'::jsonb", "deal_facts_path": "varchar(300)",
                        "card_channel": "varchar(40)", "card_ts": "varchar(40)"},
    "doc_chase_uploads": {"quarantine_status": "varchar(20)", "quarantine_reason": "varchar(300)"},
    "partner_file_uploads": {"quarantine_status": "varchar(20)", "quarantine_reason": "varchar(300)",
                             "item_code": "varchar(40)"},
}


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    for model in TABLES:
        model.__table__.create(translated, checkfirst=True)
    for model in TABLES:
        table: Table = model.__table__  # type: ignore[assignment]  # declarative tables are Table at runtime
        for index in sorted(table.indexes, key=lambda i: str(i.name or "")):
            translated.execute(CreateIndex(index, if_not_exists=True))
    for table, columns in ADDED_COLUMNS.items():
        added = ", ".join(f"ADD COLUMN IF NOT EXISTS {name} {ddl}" for name, ddl in columns.items())
        conn.execute(text(f'ALTER TABLE "{schema}".{table} {added}'))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        apply_to(conn, schema)
    logger.info("apply_lending_lender_packages complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
