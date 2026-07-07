"""Data repair — null out NaN-string corruption in tax_deed_auctions.

BaseLoader.parse_amount() was called as `self.parse_amount(str(raw.get(...)))`
in the tax-deed loader. When the source cell was missing (pandas NaN),
`str(nan)` produced the literal string "nan", which the old guard
(`pd.isna(x) or not x`) failed to catch — `float("nan")` succeeds in Python,
so the corrupted value was written straight into the Numeric column as NaN
instead of NULL. `sold_to` had the same defect via `.strip() or None` (a
non-empty "nan" string is truthy). Both are fixed at the source now
(src/loaders/base.py, src/loaders/tax_deed.py) — this repairs the rows
already written before the fix landed.

Idempotent — re-running finds nothing left to update.

Usage:
    PYTHONPATH=. python migrations/apply_cde04_tax_deed_nan_repair.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "UPDATE tax_deed_auctions SET sold_amount = NULL WHERE sold_amount::text = 'NaN';",
    "UPDATE tax_deed_auctions SET opening_bid = NULL WHERE opening_bid::text = 'NaN';",
    "UPDATE tax_deed_auctions SET sold_to = NULL WHERE sold_to = 'nan';",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            result = conn.execute(text(stmt))
            logger.info("Repair step %d/%d — %d rows affected", i, len(DDL), result.rowcount)

    logger.info("cde04_tax_deed_nan_repair complete.")


if __name__ == "__main__":
    main()
