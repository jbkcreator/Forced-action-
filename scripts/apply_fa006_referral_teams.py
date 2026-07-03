"""Auto-converted from alembic migration `fa006_referral_teams` (revision fa006_referral_teams).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa006_referral_teams.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE referral_teams (
    id SERIAL NOT NULL, 
    lead_subscriber_id INTEGER NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    vertical VARCHAR(50) NOT NULL, 
    member_subscriber_ids INTEGER[] NOT NULL, 
    shared_zips VARCHAR(10)[], 
    status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    unlocked_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(lead_subscriber_id) REFERENCES subscribers (id), 
    CONSTRAINT check_referral_team_status CHECK (status IN ('active', 'broken'))
);

CREATE INDEX ix_referral_teams_lead_subscriber_id ON referral_teams (lead_subscriber_id);

CREATE INDEX idx_referral_team_county_vertical ON referral_teams (county_id, vertical);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa006_referral_teams")


if __name__ == "__main__":
    main()
