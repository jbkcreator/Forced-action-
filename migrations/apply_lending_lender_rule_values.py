"""T-05: ``lending.lender_rule_values`` (one row per lender key and field, with a status) and its seed.

The seed copies every parameter of every lender in ``config/lender_matrix.py`` as one row: value plus
status (confirmed / placeholder / confirm_at_submission). Existing rows are never overwritten, so a value
Josh has edited survives a re-run. Josh's edits are plain row updates; see docs/lending/lender-rules-runbook.md.

Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema).

Usage:
    PYTHONPATH=. python migrations/apply_lending_lender_rule_values.py
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lender_matrix import LENDER_MATRIX, PARAMETER_FIELDS, field_status_of
from config.settings import get_settings
from src.lending.lender_rules_store import encode_value
from src.lending.models import LENDING_SCHEMA, LendingLenderRuleValue

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_SEED_SQL = """
    INSERT INTO "{schema}".lender_rule_values (lender_key, field, value, status, note, updated_by)
    VALUES (:lender_key, :field, CAST(:value AS jsonb), :status, 'seeded from config/lender_matrix.py', 'seed')
    ON CONFLICT (lender_key, field) DO NOTHING
"""


def seed_rows() -> list[dict]:
    return [
        {
            "lender_key": rules.key,
            "field": name,
            "value": json.dumps(encode_value(getattr(rules, name))),
            "status": field_status_of(rules, name).value,
        }
        for rules in LENDER_MATRIX
        for name in PARAMETER_FIELDS
    ]


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> int:
    """Create the table and insert the missing seed rows. Returns rows inserted. Does not commit."""
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingLenderRuleValue.__table__.create(translated, checkfirst=True)
    inserted = 0
    for row in seed_rows():
        inserted += conn.execute(text(_SEED_SQL.format(schema=schema)), row).rowcount
    return inserted


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        inserted = apply_to(conn, schema)
    logger.info("apply_lending_lender_rule_values complete (schema=%s, seed rows inserted=%d).", schema, inserted)


if __name__ == "__main__":
    apply()
