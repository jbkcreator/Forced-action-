"""Auto-converted from alembic migration `fa099_broker_transitions` (revision fa099_broker_transitions).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa099_broker_transitions.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE IF NOT EXISTS broker_transitions (
            transition_id   UUID        NOT NULL DEFAULT generate_uuidv7()
                                        CONSTRAINT broker_transitions_pkey PRIMARY KEY,
            lane_id         UUID        NOT NULL
                                        REFERENCES lanes(lane_id),
            prospect_id     UUID        NOT NULL
                                        REFERENCES prospects(prospect_id),
            broker_id       UUID        NOT NULL
                                        REFERENCES brokers(broker_id),
            from_state      VARCHAR(50) NOT NULL,
            to_state        VARCHAR(50) NOT NULL
                                        CONSTRAINT ck_bt_to_state
                                        CHECK (to_state IN ('unassigned', 'assigned', 'working', 'quoted', 'committed', 'closed_won', 'closed_lost')),
            reason_code     VARCHAR(100) NOT NULL,
            actor           VARCHAR(255) NOT NULL,
            occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

CREATE INDEX IF NOT EXISTS idx_bt_lane_id ON broker_transitions (lane_id);

CREATE INDEX IF NOT EXISTS idx_bt_prospect_id ON broker_transitions (prospect_id);

CREATE INDEX IF NOT EXISTS idx_bt_broker_id ON broker_transitions (broker_id);

CREATE INDEX IF NOT EXISTS idx_bt_lane_occurred ON broker_transitions (lane_id, occurred_at DESC);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa099_broker_transitions")


if __name__ == "__main__":
    main()
