"""Cora agent core tables in schema ``lending``: ``agent_halt_state`` (kill switch) and
``pending_actions`` (external sends held for approval).

The DDL is owned by ``packages/agent_core/schema.py`` so the library and this database stay in
step; the matching models are ``LendingAgentHaltState`` / ``LendingPendingAction`` in
``src/lending/models.py``. Idempotent. Run apply_lending_pending_action_safeguards.py after it.

Usage:
    PYTHONPATH=. python migrations/apply_lending_cora_agent_core.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from packages.agent_core.schema import apply_base
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    apply_base(conn, schema)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_cora_agent_core complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
