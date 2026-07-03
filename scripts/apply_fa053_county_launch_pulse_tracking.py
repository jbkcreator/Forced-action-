"""Auto-converted from alembic migration `fa053_county_launch_pulse_tracking` (revision fa053).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa053_county_launch_pulse_tracking.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE expansion_candidates ADD COLUMN revenue_pulse_sent_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE county_launch_audit DROP CONSTRAINT ck_county_launch_audit_event;

ALTER TABLE county_launch_audit ADD CONSTRAINT ck_county_launch_audit_event CHECK (event_type IN ('evaluated','posted','approved','rejected','launch_started','launch_aborted_gate_red','launched','cooldown_skipped','waitlist_notified','revenue_pulse_sent'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa053_county_launch_pulse_tracking")


if __name__ == "__main__":
    main()
