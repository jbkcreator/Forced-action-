"""Cora's standing-rule memory: ``lending.agent_memory``, exactly as the CORA-CORE specification
gives it (memory_id, category, rule_text, source_thread_ts, is_active, created_at).

DDL owned by ``packages/agent_core/schema.py``; model ``LendingAgentMemory`` in
``src/lending/models.py``. Needs PostgreSQL 13+ for gen_random_uuid(). Idempotent.

Usage:
    PYTHONPATH=. python migrations/apply_lending_agent_memory.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from packages.agent_core.schema import apply_memory
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    apply_memory(conn, schema)


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_agent_memory complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
