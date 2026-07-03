"""Auto-converted from alembic migration `fa082_drop_pitch_generated_status` (revision fa082_drop_pitch_generated_status).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa082_drop_pitch_generated_status.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE dfy_lite_orders DROP CONSTRAINT ck_dfy_lite_status;

ALTER TABLE dfy_lite_orders ADD CONSTRAINT ck_dfy_lite_status CHECK (
            status IN (
                'Order_Received', 'Signal_Compiled',
                'Needs_Review', 'Delivered',
                'Signal_Failed', 'Pitch_Failed', 'Cancelled'
            )
        );
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa082_drop_pitch_generated_status")


if __name__ == "__main__":
    main()
