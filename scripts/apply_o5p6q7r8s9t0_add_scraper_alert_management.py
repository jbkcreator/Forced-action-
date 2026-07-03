"""Auto-converted from alembic migration `o5p6q7r8s9t0_add_scraper_alert_management` (revision o5p6q7r8s9t0).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_o5p6q7r8s9t0_add_scraper_alert_management.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE scraper_run_stats ADD COLUMN error_type VARCHAR(20);

UPDATE scraper_run_stats
        SET error_type = CASE
            WHEN run_success = TRUE  THEN 'none'
            ELSE 'scraper_error'
        END
        WHERE error_type IS NULL;

CREATE TABLE scraper_alert_log (
    id SERIAL NOT NULL, 
    source_type VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) DEFAULT 'hillsborough' NOT NULL, 
    alert_type VARCHAR(50) NOT NULL, 
    alerted_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    PRIMARY KEY (id)
);

CREATE INDEX idx_scraper_alert_log_lookup ON scraper_alert_log (source_type, county_id, alert_type, alerted_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied o5p6q7r8s9t0_add_scraper_alert_management")


if __name__ == "__main__":
    main()
