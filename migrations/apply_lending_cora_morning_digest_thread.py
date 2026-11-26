"""R1 1.3: ``lending.cora_morning_digests.thread_pending``, David's morning-list thread replies Slack has not
accepted yet (JSON list of Block Kit block lists; NULL once all are posted). Cora's sweep posts them in order under
the digest, so a refused reply is retried without posting the digest or an accepted reply twice.

Idempotent; run after apply_lending_cora_morning_digests.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_cora_morning_digest_thread.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = "ALTER TABLE lending.cora_morning_digests ADD COLUMN IF NOT EXISTS thread_pending TEXT"


def apply(engine: Engine | None = None) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text(_DDL))
    logger.info("apply_lending_cora_morning_digest_thread complete.")


if __name__ == "__main__":
    apply()
