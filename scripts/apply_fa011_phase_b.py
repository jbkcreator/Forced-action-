"""Auto-converted from alembic migration `fa011_phase_b` (revision fa011_phase_b).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa011_phase_b.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE partner_subscriptions (
    id SERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    max_zips INTEGER DEFAULT '5' NOT NULL, 
    activated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW() NOT NULL, 
    deactivated_at TIMESTAMP WITHOUT TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT fk_partner_sub FOREIGN KEY(subscriber_id) REFERENCES subscribers (id), 
    CONSTRAINT uq_partner_sub UNIQUE (subscriber_id)
);

CREATE INDEX idx_partner_sub ON partner_subscriptions (subscriber_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa011_phase_b")


if __name__ == "__main__":
    main()
