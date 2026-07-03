"""Auto-converted from alembic migration `fa109_lead_purchase_amount_cents` (revision fa109_lead_purchase_amount_cents).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa109_lead_purchase_amount_cents.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE sent_leads ADD COLUMN amount_cents INTEGER;

ALTER TABLE lead_pack_purchases ADD COLUMN amount_cents INTEGER;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa109_lead_purchase_amount_cents")


if __name__ == "__main__":
    main()
