"""Auto-converted from alembic migration `u5v6w7x8y9z0_add_sent_lead_refund_columns` (revision fa002_refund_cols).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_u5v6w7x8y9z0_add_sent_lead_refund_columns.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE sent_leads ADD COLUMN stripe_payment_intent_id VARCHAR(100);

ALTER TABLE sent_leads ADD COLUMN refunded_at TIMESTAMP WITHOUT TIME ZONE;

ALTER TABLE sent_leads ADD COLUMN refund_reason VARCHAR(255);

ALTER TABLE sent_leads ADD COLUMN stripe_refund_id VARCHAR(100);

CREATE INDEX idx_sent_leads_pi_id ON sent_leads (stripe_payment_intent_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied u5v6w7x8y9z0_add_sent_lead_refund_columns")


if __name__ == "__main__":
    main()
