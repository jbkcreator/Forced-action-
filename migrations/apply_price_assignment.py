"""Create price_assignments table.

SUPERSEDED IN PART: this originally also added price-experiment metadata
columns directly to ab_tests (Lifecycle's table). That coupled Agent Lane's
price-band tests to Lifecycle's schema/blast radius — see
migrations/apply_agent_lane_experiments.py and
migrations/apply_agent_lane_experiment_separation_cleanup.py, which create
Agent Lane's own agent_lane_experiments table and remove these columns from
ab_tests instead. Only the price_assignments table creation remains here.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_price_assignment.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    # New price_assignments table. experiment_assignment_id's FK target
    # (agent_lane_experiment_assignments) is added by
    # migrations/apply_agent_lane_experiments.py, which also renames this
    # column from its original ab_assignment_id.
    """
    CREATE TABLE IF NOT EXISTS price_assignments (
        id                      SERIAL PRIMARY KEY,
        opportunity_thread_id   VARCHAR(30)  NOT NULL,
        offer                   VARCHAR(60)  NOT NULL,
        assigned_price_cents    INTEGER      NOT NULL,
        currency                VARCHAR(3)   NOT NULL DEFAULT 'usd',
        experiment_assignment_id INTEGER,
        price_band_floor_cents  INTEGER      NOT NULL,
        price_band_ceiling_cents INTEGER     NOT NULL,
        band_validated          BOOLEAN      NOT NULL DEFAULT FALSE,
        assigned_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        status                  VARCHAR(20)  NOT NULL DEFAULT 'active',
        CONSTRAINT check_price_assignment_status
            CHECK (status IN ('active', 'superseded', 'expired'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_price_assignments_thread_offer_status "
    "ON price_assignments (opportunity_thread_id, offer, status)",
    "CREATE INDEX IF NOT EXISTS ix_price_assignments_opportunity_thread_id "
    "ON price_assignments (opportunity_thread_id)",
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("apply_price_assignment: done.")


if __name__ == "__main__":
    main()
