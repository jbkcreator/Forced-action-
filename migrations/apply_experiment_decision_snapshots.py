"""Create experiment_decision_snapshots (LEARN-v2.2 Layer 1, Step 3).

Immutable record of what was known and chosen the moment an opportunity
was assigned to an Agent Lane experiment arm — what Layer 2's attribution
join and Layer 4's Golden CLOSE chains both walk backward from. See
src.core.models.ExperimentDecisionSnapshot and
src/services/agent_lane_experiment_engine.py's record_decision_snapshot().

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_experiment_decision_snapshots.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS experiment_decision_snapshots (
        id                      SERIAL PRIMARY KEY,
        test_id                 INTEGER      NOT NULL REFERENCES agent_lane_experiments(id),
        opportunity_thread_id   VARCHAR(20)  NOT NULL,
        assigned_variant        VARCHAR(10)  NOT NULL,
        message_angle           VARCHAR(100),
        offer                   VARCHAR(60),
        buyer_type               VARCHAR(30),
        target_characteristics  JSONB,
        chosen_action           VARCHAR(60),
        leading_alternative     VARCHAR(60),
        created_at              TIMESTAMP    NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_experiment_decision_snapshot UNIQUE (test_id, opportunity_thread_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_experiment_decision_snapshots_test_id "
    "ON experiment_decision_snapshots (test_id)",
    "CREATE INDEX IF NOT EXISTS ix_experiment_decision_snapshots_thread_id "
    "ON experiment_decision_snapshots (opportunity_thread_id)",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("experiment_decision_snapshots migration complete.")


if __name__ == "__main__":
    main()
