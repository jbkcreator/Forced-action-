"""Create Agent Lane's own experiment tables, separate from Lifecycle's ab_tests/ab_assignments.

Agent Lane (Cora cold outbound, REVINT price-band tests, Hunter's vertical
autopilot, LEARN) and Lifecycle (post-customer/subscriber messaging) are
two different engines — pre- vs post-customer outreach. REVINT-v2.2
originally extended ab_tests/ab_assignments directly (migrations/
apply_price_assignment.py, apply_ab_assignment_thread_id.py) because that
table's statistical machinery (deterministic hashing, traffic-cap guardrail,
z-test verdict) was already proven — but doing so coupled Agent Lane's
schema and blast radius to Lifecycle's (e.g. ab_rollback_check walks every
active AbTest with no name filter, so a row inserted there is already
subject to Lifecycle's own rollback math). This migration gives Agent Lane
its own tables with the same field shape; migrations/apply_agent_lane_
experiment_separation_cleanup.py removes the now-redundant columns from
ab_tests/ab_assignments once the code redirect (src/services/
agent_lane_experiment_engine.py) is verified live.

Also repoints price_assignments.ab_assignment_id -> experiment_assignment_id,
FK'd to agent_lane_experiment_assignments instead of ab_assignments. Safe:
PRICE_BAND_TESTING_ENABLED has been False throughout, so every existing
price_assignments row has this column NULL.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_agent_lane_experiments.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS agent_lane_experiments (
        id                      SERIAL PRIMARY KEY,
        test_name               VARCHAR(100) NOT NULL UNIQUE,
        hypothesis              TEXT,
        audience                VARCHAR(100),
        offer                   VARCHAR(60),
        variant_a               JSONB       NOT NULL,
        variant_b               JSONB       NOT NULL,
        control_price_cents     INTEGER,
        test_price_cents        INTEGER,
        traffic_pct             INTEGER      NOT NULL DEFAULT 10,
        min_sample              INTEGER,
        success_metric          VARCHAR(60),
        status                  VARCHAR(20)  NOT NULL DEFAULT 'active',
        verdict                 VARCHAR(20),
        started_at              TIMESTAMP    NOT NULL DEFAULT NOW(),
        ended_at                TIMESTAMP,
        winner                  VARCHAR(10),
        CONSTRAINT check_agent_lane_experiment_status
            CHECK (status IN ('active', 'completed', 'rolled_back')),
        CONSTRAINT check_agent_lane_experiment_traffic_pct
            CHECK (traffic_pct BETWEEN 1 AND 100),
        CONSTRAINT check_agent_lane_experiment_verdict
            CHECK (verdict IS NULL OR verdict IN ('control_wins', 'test_wins', 'inconclusive'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_lane_experiment_assignments (
        id                      SERIAL PRIMARY KEY,
        test_id                 INTEGER      NOT NULL REFERENCES agent_lane_experiments(id),
        opportunity_thread_id   VARCHAR(20)  NOT NULL,
        variant                 VARCHAR(10)  NOT NULL,
        outcome                 VARCHAR(30),
        outcome_at              TIMESTAMPTZ,
        created_at              TIMESTAMP    NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_agent_lane_experiment_assignment UNIQUE (test_id, opportunity_thread_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_agent_lane_experiment_assignments_test_id "
    "ON agent_lane_experiment_assignments (test_id)",
    "CREATE INDEX IF NOT EXISTS ix_agent_lane_experiment_assignments_thread_id "
    "ON agent_lane_experiment_assignments (opportunity_thread_id)",

    # Repoint price_assignments at the new assignment table.
    "ALTER TABLE price_assignments RENAME COLUMN ab_assignment_id TO experiment_assignment_id",
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'price_assignments_ab_assignment_id_fkey'
        ) THEN
            ALTER TABLE price_assignments DROP CONSTRAINT price_assignments_ab_assignment_id_fkey;
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'price_assignments_experiment_assignment_id_fkey'
        ) THEN
            ALTER TABLE price_assignments
                ADD CONSTRAINT price_assignments_experiment_assignment_id_fkey
                FOREIGN KEY (experiment_assignment_id) REFERENCES agent_lane_experiment_assignments(id);
        END IF;
    END $$;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("agent_lane_experiments migration complete.")


if __name__ == "__main__":
    main()
