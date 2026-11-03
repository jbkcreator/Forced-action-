"""Client list Nov 2 (Q1): put Backflip on the same safe placeholder set as the other four lenders in
``lending.lender_rule_values`` (read when LENDING_RULES_FROM_DB=true).

apply_lending_lender_rule_values.py never overwrites a row, so a database seeded before this change still holds
Backflip's Oct 1 values (flip: floor 640, ARV 75%, $100K min, ~$85K min purchase; ground-up: $500K min,
3+ projects, 7.75% spread). This resets each Backflip row that still carries the seed's value
(``updated_by = 'seed'``) to the current config seed. A row anyone has edited is left alone and logged.

Idempotent; run after apply_lending_lender_rule_values.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_lender_rules_safe_set.py
"""
from __future__ import annotations

import logging

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from migrations.apply_lending_lender_rule_values import seed_rows
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BACKFLIP_KEYS = ("generic_backflip_flip", "generic_backflip_construction")

_RESET_SQL = """
    UPDATE "{schema}".lender_rule_values
       SET value = CAST(:value AS jsonb), status = :status, updated_at = now(),
           note = 'reset to the safe placeholder set (client list Nov 2, Q1)'
     WHERE lender_key = :lender_key AND field = :field AND updated_by = 'seed'
       AND (value IS DISTINCT FROM CAST(:value AS jsonb) OR status IS DISTINCT FROM :status)
"""

_EDITED_SQL = """
    SELECT lender_key, field FROM "{schema}".lender_rule_values
     WHERE lender_key IN :keys AND updated_by IS DISTINCT FROM 'seed' ORDER BY lender_key, field
"""


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> int:
    """Reset the seed-owned Backflip rows. Returns rows changed. Does not commit."""
    if conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{schema}.lender_rule_values"}).scalar() is None:
        logger.info("apply_lending_lender_rules_safe_set: no lender_rule_values table; nothing to reset.")
        return 0
    changed = 0
    for row in seed_rows():
        if row["lender_key"] in BACKFLIP_KEYS:
            changed += conn.execute(text(_RESET_SQL.format(schema=schema)), row).rowcount
    edited = conn.execute(text(_EDITED_SQL.format(schema=schema)).bindparams(bindparam("keys", expanding=True)),
                          {"keys": list(BACKFLIP_KEYS)}).all()
    for lender_key, field in edited:
        logger.warning("apply_lending_lender_rules_safe_set: %s.%s was edited by hand; left as is", lender_key, field)
    return changed


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        changed = apply_to(conn, schema)
    logger.info("apply_lending_lender_rules_safe_set complete (schema=%s, rows reset=%d).", schema, changed)


if __name__ == "__main__":
    apply()
