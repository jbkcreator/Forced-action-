"""Auto-converted from alembic migration `x8y9z0a1b2c3_fa005_scoring_indexes` (revision fa005_scoring_indexes).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_x8y9z0a1b2c3_fa005_scoring_indexes.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE INDEX IF NOT EXISTS idx_score_property_date ON distress_scores (property_id, score_date DESC);

CREATE INDEX IF NOT EXISTS idx_cv_pid_date_added ON code_violations (property_id, date_added);

CREATE INDEX IF NOT EXISTS idx_lal_pid_date_added ON legal_and_liens (property_id, date_added);

CREATE INDEX IF NOT EXISTS idx_deeds_pid_date_added ON deeds (property_id, date_added);

CREATE INDEX IF NOT EXISTS idx_lp_pid_date_added ON legal_proceedings (property_id, date_added);

CREATE INDEX IF NOT EXISTS idx_td_pid_date_added ON tax_delinquencies (property_id, date_added);

CREATE INDEX IF NOT EXISTS idx_fc_pid_date_added ON foreclosures (property_id, date_added);

CREATE INDEX IF NOT EXISTS idx_bp_pid_date_added ON building_permits (property_id, date_added);

CREATE INDEX IF NOT EXISTS idx_inc_pid_date_added ON incidents (property_id, date_added);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied x8y9z0a1b2c3_fa005_scoring_indexes")


if __name__ == "__main__":
    main()
