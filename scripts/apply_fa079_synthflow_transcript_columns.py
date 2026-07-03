"""Auto-converted from alembic migration `fa079_synthflow_transcript_columns` (revision fa079_synthflow_transcript_columns).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa079_synthflow_transcript_columns.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE synthflow_calls ADD COLUMN call_id VARCHAR(100);

ALTER TABLE synthflow_calls ADD COLUMN transcript_text TEXT;

ALTER TABLE synthflow_calls ADD COLUMN recording_url VARCHAR(500);

ALTER TABLE synthflow_calls ADD COLUMN duration_seconds INTEGER;

ALTER TABLE synthflow_calls ADD CONSTRAINT uq_synthflow_calls_call_id UNIQUE (call_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa079_synthflow_transcript_columns")


if __name__ == "__main__":
    main()
