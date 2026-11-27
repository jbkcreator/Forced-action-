"""R1 0.6 dependency fallbacks: ``lending.dependency_health`` (when each outside service or feed last worked),
``lending.dependency_alerts`` (one row per alert episode, ``unposted`` = the post Slack has not accepted) and ``lending.exhausted_phones`` (phones that reached the
total attempt cap; never reloaded from a list until a new inbound), plus ``lending.instantly_replies.ghl_handoff_pending_at``
(a Deal reply's GHL handoff held through a dependency pause; added only when that table exists). Idempotent; run BEFORE
deploying the code (the feeds write health rows, the list gates read exhausted marks, the reply sweep reads the column).

Usage:
    PYTHONPATH=. python migrations/apply_lending_dependency_health.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import (LENDING_SCHEMA, LendingDependencyAlert, LendingDependencyHealth,
                                LendingExhaustedPhone)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MODELS = (LendingDependencyHealth, LendingDependencyAlert, LendingExhaustedPhone)
_ALERT_UNPOSTED_DDL = "ALTER TABLE {schema}.dependency_alerts ADD COLUMN IF NOT EXISTS unposted VARCHAR(12)"
_REPLY_HANDOFF_DDL = (
    "ALTER TABLE {schema}.instantly_replies ADD COLUMN IF NOT EXISTS ghl_handoff_pending_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS ix_instantly_replies_handoff_pending ON {schema}.instantly_replies "
    "(ghl_handoff_pending_at) WHERE ghl_handoff_pending_at IS NOT NULL",
)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    for model in MODELS:
        model.__table__.create(translated, checkfirst=True)
    conn.execute(text(_ALERT_UNPOSTED_DDL.format(schema=schema)))
    if conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{schema}.instantly_replies"}).scalar() is not None:
        for statement in _REPLY_HANDOFF_DDL:
            conn.execute(text(statement.format(schema=schema)))
    else:
        logger.warning("%s.instantly_replies does not exist yet; re-run after apply_lending_instantly_replies.py", schema)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_dependency_health complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
