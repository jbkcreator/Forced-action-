"""Auto-converted from alembic migration `fa013_referral_team_broken_audit` (revision fa013_referral_team_broken_audit).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa013_referral_team_broken_audit.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE referral_teams ADD COLUMN broken_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE referral_teams ADD COLUMN broken_reason VARCHAR(32);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa013_referral_team_broken_audit")


if __name__ == "__main__":
    main()
