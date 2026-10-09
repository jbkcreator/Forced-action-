"""T-11: ``lending.lendingflow_leads`` + ``lending.lead_consent_certificates``.

LendingFlow leads (deduped on vendor id and on phone+email hash) and their append-only consent
evidence. Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema and
``lending.contacts``). Separate from T-14's migration on purpose.

Usage:
    PYTHONPATH=. python migrations/apply_lending_lendingflow.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingLeadConsentCertificate, LendingLendingFlowLead

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# create never alters an existing table; columns added after first apply go here.
_ALTERS: list[str] = [
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS loan_amount_range varchar(40)',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS loan_amount_min bigint',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS loan_amount_max bigint',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS lead_source_campaign varchar(100)',
    'ALTER TABLE "{t}".lendingflow_leads ADD COLUMN IF NOT EXISTS submitted_at timestamptz',
]


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingLendingFlowLead.__table__.create(translated, checkfirst=True)
    LendingLeadConsentCertificate.__table__.create(translated, checkfirst=True)
    for stmt in _ALTERS:
        conn.execute(text(stmt.format(t=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_lendingflow complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
