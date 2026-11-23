"""``lending.sunbiz_llc_contacts`` (T3): the person traced for each new Florida real estate LLC, with the phones and
emails Tracerfy found (written by ``src.tasks.sunbiz_llc_trace``, read by the cold-email step). Idempotent; run after
apply_lending_compliance.py (creates the ``lending`` schema), before deploying the task.

Usage:
    PYTHONPATH=. python migrations/apply_lending_sunbiz_llc_contacts.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "{schema}".sunbiz_llc_contacts (
            document_number varchar(12)  PRIMARY KEY,
            entity_name     varchar(200) NOT NULL,
            subject_role    varchar(10)  NOT NULL CHECK (subject_role IN ('officer', 'agent')),
            subject_name    varchar(80)  NOT NULL,
            phones          varchar[]    NOT NULL DEFAULT '{{}}',
            emails          varchar[]    NOT NULL DEFAULT '{{}}',
            traced_at       timestamptz  NOT NULL DEFAULT now()
        )"""))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_sunbiz_llc_contacts complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
