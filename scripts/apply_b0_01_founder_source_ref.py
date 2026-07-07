"""B0-01 — add source_ref idempotency key to deal_outcomes.

Bulk imports (founder portfolio, later connectors) upsert on source_ref =
hash(parcel/address | deal_date | amount). Subscriber-tap rows leave it NULL;
a PARTIAL unique index constrains only non-NULL values so those NULLs never
collide. See ADR 0026, CONTEXT.md § Founder Portfolio Import.

Idempotent. Usage:
    PYTHONPATH=. python scripts/apply_b0_01_founder_source_ref.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE deal_outcomes ADD COLUMN IF NOT EXISTS source_ref TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_outcomes_source_ref
    ON deal_outcomes (source_ref)
    WHERE source_ref IS NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied b0_01_founder_source_ref")


if __name__ == "__main__":
    main()
