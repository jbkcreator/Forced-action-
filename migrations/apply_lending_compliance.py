"""Create the ``lending`` schema and Wave 0 compliance-floor tables.

Tables: suppression_list, contacts, opt_out_events, load_exclusions
(src/core/lending_models.py). Idempotent — safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_lending_compliance.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings
from src.core.lending_models import LENDING_SCHEMA, LendingBase

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA, source_schema: str = "public") -> None:
    """``schema`` / ``source_schema`` are overridable so tests never touch the shared schema."""
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
        LendingBase.metadata.create_all(translated, checkfirst=True)
        _ensure_timestamp_defaults(conn, schema)
        _backfill(conn, schema, source_schema)
    logger.info("apply_lending_compliance complete (schema=%s).", schema)


# Order matters: an earlier reason wins on a unique-key conflict.
# National DNC is deliberately not backfilled — it is re-verified every 31 days
# (spec §3.2); the Tracerfy-derived sms_opt_outs rows are national-DNC hits, not opt-outs.
_BACKFILL = [
    """
    INSERT INTO "{t}".suppression_list (phone, reason, source_channel, created_at)
    SELECT phone, 'OPT_OUT', 'backfill:sms_opt_outs', now()
    FROM "{s}".sms_opt_outs
    WHERE source <> 'tracerfy_dnc_refresh'
    ON CONFLICT (phone) DO NOTHING
    """,
    """
    INSERT INTO "{t}".suppression_list (email, reason, source_channel, created_at)
    SELECT lower(email), 'OPT_OUT', 'backfill:email_opt_outs', now()
    FROM "{s}".email_opt_outs
    ON CONFLICT (email) DO NOTHING
    """,
    """
    INSERT INTO "{t}".suppression_list (phone, reason, source_channel, created_at)
    SELECT phone, 'LITIGATOR', 'backfill:dnc_phone_checks', now()
    FROM "{s}".dnc_phone_checks
    WHERE litigator
    ON CONFLICT (phone) DO NOTHING
    """,
]


_TIMESTAMP_DEFAULTS = [
    ("suppression_list", "created_at"),
    ("contacts", "created_at"),
    ("opt_out_events", "received_at"),
    ("load_exclusions", "created_at"),
]


def _ensure_timestamp_defaults(conn, schema: str) -> None:
    """Raw-SQL writers (project rule) rely on DB defaults, not ORM ones."""
    for table, column in _TIMESTAMP_DEFAULTS:
        conn.execute(text(f'ALTER TABLE "{schema}".{table} ALTER COLUMN {column} SET DEFAULT now()'))


def _backfill(conn, schema: str, source_schema: str) -> None:
    for stmt in _BACKFILL:
        conn.execute(text(stmt.format(t=schema, s=source_schema)))


if __name__ == "__main__":
    apply()
