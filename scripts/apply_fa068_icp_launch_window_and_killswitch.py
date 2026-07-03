"""Auto-converted from alembic migration `fa068_icp_launch_window_and_killswitch` (revision fa068).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa068_icp_launch_window_and_killswitch.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE expansion_icp_channels ADD COLUMN launch_started_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE expansion_icp_channels ADD COLUMN launch_ends_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE expansion_icp_channels ADD COLUMN killswitch_decision VARCHAR(10);

ALTER TABLE expansion_icp_channels ADD COLUMN killswitch_reason TEXT;

ALTER TABLE expansion_icp_channels ADD COLUMN killswitch_decided_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE expansion_icp_channels ADD COLUMN killswitch_decided_by VARCHAR(100);

ALTER TABLE expansion_icp_channels ADD CONSTRAINT ck_expansion_icp_killswitch_decision CHECK (killswitch_decision IS NULL OR killswitch_decision IN ('keep','adjust','kill'));

ALTER TABLE icp_channel_launch_audit DROP CONSTRAINT ck_icp_audit_event_type;

ALTER TABLE icp_channel_launch_audit ADD CONSTRAINT ck_icp_audit_event_type CHECK (event_type IN ('activated','paused','killed','force_activated','config_updated','gate_evaluated','created','killswitch_keep','killswitch_adjust','killswitch_kill'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa068_icp_launch_window_and_killswitch")


if __name__ == "__main__":
    main()
