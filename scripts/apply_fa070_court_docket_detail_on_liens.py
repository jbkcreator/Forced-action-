"""Auto-converted from alembic migration `fa070_court_docket_detail_on_liens` (revision fa070).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa070_court_docket_detail_on_liens.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE legal_and_liens ADD COLUMN mailing_address TEXT;

ALTER TABLE legal_and_liens ADD COLUMN docket_detail JSONB;

ALTER TABLE legal_and_liens ADD COLUMN balance_due NUMERIC(12, 2);

ALTER TABLE legal_and_liens ADD COLUMN docket_status VARCHAR(30);

CREATE INDEX idx_legal_docket_status ON legal_and_liens (docket_status);

ALTER TABLE legal_and_liens ADD CONSTRAINT check_legal_docket_status CHECK (docket_status IS NULL OR docket_status IN ('ok','case_number_missing','not_found','blocked','error'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa070_court_docket_detail_on_liens")


if __name__ == "__main__":
    main()
