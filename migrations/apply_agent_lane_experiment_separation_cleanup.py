"""Remove REVINT-v2.2's price-band columns from Lifecycle's ab_tests/ab_assignments.

Corrective migration: REVINT-v2.2 originally extended Lifecycle's AbTest/
AbAssignment in place (migrations/apply_price_assignment.py's ab_tests
columns, migrations/apply_ab_assignment_thread_id.py's ab_assignments
columns/constraint) to run cold, pre-customer price-band tests. Agent Lane
and Lifecycle are separate engines (pre- vs post-customer outreach); those
columns have been redirected to the new agent_lane_experiments/
agent_lane_experiment_assignments tables (migrations/apply_agent_lane_
experiments.py, src/services/agent_lane_experiment_engine.py).

DO NOT RUN until that redirect is merged/deployed and confirmed live —
running this before the code stops reading/writing these columns will
break get_price_variant()/assign_variant_by_thread() against the live DB.

Safe to run once confirmed: PRICE_BAND_TESTING_ENABLED has been False
throughout REVINT-v2.2's history, so no ab_assignments row has ever been
written with opportunity_thread_id set (verify with the COUNT query in
verify_before_running() below before running DDL) — restoring
subscriber_id NOT NULL cannot violate any existing row.

Idempotent — IF EXISTS guards throughout. Usage:
    PYTHONPATH=. python migrations/apply_agent_lane_experiment_separation_cleanup.py --verify-only
    PYTHONPATH=. python migrations/apply_agent_lane_experiment_separation_cleanup.py
"""
from __future__ import annotations

import logging
import sys

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    # ab_tests: drop REVINT's price-experiment metadata columns + verdict check.
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'ab_tests' AND constraint_name = 'check_ab_test_verdict'
        ) THEN
            ALTER TABLE ab_tests DROP CONSTRAINT check_ab_test_verdict;
        END IF;
    END $$;
    """,
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS hypothesis",
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS offer",
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS audience",
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS control_price_cents",
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS test_price_cents",
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS min_sample",
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS success_metric",
    "ALTER TABLE ab_tests DROP COLUMN IF EXISTS verdict",

    # ab_assignments: drop the thread-keyed path + XOR constraint, restore subscriber_id NOT NULL.
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'check_ab_assignment_key_xor'
        ) THEN
            ALTER TABLE ab_assignments DROP CONSTRAINT check_ab_assignment_key_xor;
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'uq_ab_assignment_thread'
        ) THEN
            ALTER TABLE ab_assignments DROP CONSTRAINT uq_ab_assignment_thread;
        END IF;
    END $$;
    """,
    "ALTER TABLE ab_assignments DROP COLUMN IF EXISTS opportunity_thread_id",
    "ALTER TABLE ab_assignments ALTER COLUMN subscriber_id SET NOT NULL",
]


def verify_before_running(conn) -> bool:
    """Returns True iff it's safe to restore subscriber_id NOT NULL — i.e. no
    row exists with subscriber_id NULL (which would only happen if
    assign_variant_by_thread() had ever actually run with the flag on)."""
    row = conn.execute(
        text("SELECT COUNT(*) AS n FROM ab_assignments WHERE subscriber_id IS NULL")
    ).fetchone()
    n = row.n
    if n:
        logger.error(
            "ABORT: %d ab_assignments row(s) have subscriber_id IS NULL — "
            "these were written via the thread-keyed path and must be migrated "
            "to agent_lane_experiment_assignments before this cleanup can run.",
            n,
        )
        return False
    logger.info("verify_before_running: 0 rows with subscriber_id IS NULL — safe to proceed.")
    return True


def main() -> None:
    verify_only = "--verify-only" in sys.argv
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        if not verify_before_running(conn):
            sys.exit(1)
        if verify_only:
            logger.info("--verify-only: not running DDL.")
            return
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("agent_lane_experiment_separation_cleanup complete.")


if __name__ == "__main__":
    main()
