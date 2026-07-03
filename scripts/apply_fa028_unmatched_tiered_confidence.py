"""Auto-converted from alembic migration `fa028_unmatched_tiered_confidence` (revision fa028_unmatched_tiered_confidence).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa028_unmatched_tiered_confidence.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE unmatched_records ADD COLUMN match_confidence NUMERIC(4, 3);

ALTER TABLE unmatched_records ADD COLUMN match_method VARCHAR(30);

ALTER TABLE unmatched_records ADD COLUMN candidate_property_id INTEGER;

ALTER TABLE unmatched_records ADD CONSTRAINT fk_unmatched_candidate_property FOREIGN KEY(candidate_property_id) REFERENCES properties (id);

CREATE INDEX ix_unmatched_candidate_property ON unmatched_records (candidate_property_id);

ALTER TABLE unmatched_records DROP CONSTRAINT IF EXISTS check_unmatched_match_status;

ALTER TABLE unmatched_records ADD CONSTRAINT check_unmatched_match_status CHECK (match_status IN ('unmatched','matched','skipped','pending_review'));

ALTER TABLE unmatched_records ADD CONSTRAINT check_unmatched_match_method CHECK (match_method IN ('address','owner_name','legal_desc','parcel_id') OR match_method IS NULL);

ALTER TABLE legal_and_liens ALTER COLUMN match_confidence TYPE NUMERIC(4,3) USING CASE WHEN match_confidence IS NULL THEN NULL      ELSE ROUND(match_confidence / 100.0, 3) END;

ALTER TABLE deeds ADD COLUMN match_confidence NUMERIC(4, 3);

ALTER TABLE deeds ADD COLUMN match_method VARCHAR(30);

ALTER TABLE legal_proceedings ADD COLUMN match_confidence NUMERIC(4, 3);

ALTER TABLE legal_proceedings ADD COLUMN match_method VARCHAR(30);

ALTER TABLE code_violations ADD COLUMN match_confidence NUMERIC(4, 3);

ALTER TABLE code_violations ADD COLUMN match_method VARCHAR(30);

ALTER TABLE foreclosures ADD COLUMN match_confidence NUMERIC(4, 3);

ALTER TABLE foreclosures ADD COLUMN match_method VARCHAR(30);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa028_unmatched_tiered_confidence")


if __name__ == "__main__":
    main()
