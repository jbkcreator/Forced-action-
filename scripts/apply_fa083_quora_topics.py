"""Auto-converted from alembic migration `fa083_quora_topics` (revision fa083_quora_topics).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa083_quora_topics.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE quora_topics (
    id SERIAL NOT NULL, 
    keyword TEXT NOT NULL, 
    is_active BOOLEAN DEFAULT 'true' NOT NULL, 
    last_run_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_quora_topics_keyword UNIQUE (keyword)
);

CREATE INDEX idx_quora_topics_active_last_run ON quora_topics (is_active, last_run_at);

CREATE TABLE quora_settings (
    id SERIAL NOT NULL, 
    cooldown_days INTEGER DEFAULT '1' NOT NULL, 
    PRIMARY KEY (id)
);

INSERT INTO quora_settings (id, cooldown_days) VALUES (1, 1);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa083_quora_topics")


if __name__ == "__main__":
    main()
