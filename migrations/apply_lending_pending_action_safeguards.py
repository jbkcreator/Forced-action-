"""Send safeguards on ``lending.pending_actions``.

Adds revision history (``revisions``), the recipient used by the send-time opt-out check
(``recipient_phone`` / ``recipient_email``), lead references (``contact_ref`` / ``deal_ref``),
a unique ``idempotency_key``, ``expires_at``, and ``revised_by`` (split from ``decided_by``).
Widens the status check with ``blocked`` and ``expired``, and moves the open-revision index to
``revised_by``. DDL owned by ``packages/agent_core/schema.py``. Idempotent; run after
apply_lending_cora_agent_core.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_pending_action_safeguards.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from packages.agent_core.schema import apply_safeguards
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    apply_safeguards(conn, schema)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_pending_action_safeguards complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
