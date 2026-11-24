"""R1 0.2 document access tables: ``lending.doc_view_links`` (15-minute view links, token hashes only) and
``lending.staff_api_tokens`` (personal staff API tokens, hashes only, 90 days).

Idempotent; run after apply_lending_doc_chase.py and BEFORE apply_lending_caller_db_role.py (so the role migration
can revoke the new document table). RUN BEFORE deploying the code.

Usage:
    PYTHONPATH=. python migrations/apply_lending_document_access.py
"""
from __future__ import annotations

import logging

from sqlalchemy import Table, create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import CreateIndex

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingDocViewLink, LendingStaffApiToken
from src.lending.security.rbac import revoke_from_caller_role

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TABLES = (LendingStaffApiToken, LendingDocViewLink)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    for model in TABLES:
        model.__table__.create(translated, checkfirst=True)
    for model in TABLES:
        table: Table = model.__table__  # type: ignore[assignment]  # declarative tables are Table at runtime
        for index in sorted(table.indexes, key=lambda i: str(i.name or "")):
            translated.execute(CreateIndex(index, if_not_exists=True))
    # Both tables hold token hashes: closed to callers at once when lending_caller already exists.
    revoke_from_caller_role(conn, schema, tuple(model.__tablename__ for model in TABLES))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_document_access complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
