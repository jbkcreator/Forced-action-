"""B0-02 — Outcome Sanity Filter: add buyer_could_not_act_reason to score_feedback.

A death caused by a Buyer-Capacity Failure (low_fico / no_capital) is written
with realized_outcome NULL (the shield — excluded from every rate/training
consumer via their existing `realized_outcome IS NOT NULL` guard) and its raw
reason preserved here. The buyer_could_not_act tag is derived (this column
IS NOT NULL). Free text, no CHECK — canonical set enforced in the service.
See ADR 0027, CONTEXT.md § Outcome Sanity Filter.

Idempotent. Usage:
    PYTHONPATH=. python scripts/apply_b0_02_sanity_filter.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE score_feedback
    ADD COLUMN IF NOT EXISTS buyer_could_not_act_reason TEXT;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied b0_02_sanity_filter")


if __name__ == "__main__":
    main()
