"""Auto-converted from alembic migration `fa048_lifecycle_suppressions` (revision fa048_lifecycle_suppressions).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa048_lifecycle_suppressions.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE lifecycle_suppressions (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    reason VARCHAR(40) NOT NULL, 
    source VARCHAR(40) NOT NULL, 
    source_id VARCHAR(100), 
    paused_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    is_active BOOLEAN DEFAULT true NOT NULL, 
    created_by VARCHAR(100), 
    notes VARCHAR(255), 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_lifecycle_suppression_active_sub ON lifecycle_suppressions (subscriber_id, is_active);

CREATE INDEX idx_lifecycle_suppression_reason ON lifecycle_suppressions (reason);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa048_lifecycle_suppressions")


if __name__ == "__main__":
    main()
