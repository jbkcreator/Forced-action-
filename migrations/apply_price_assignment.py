"""Create price_assignments table and add price-experiment metadata columns to ab_tests.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_price_assignment.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    # New price_assignments table
    """
    CREATE TABLE IF NOT EXISTS price_assignments (
        id                      SERIAL PRIMARY KEY,
        opportunity_thread_id   VARCHAR(30)  NOT NULL,
        offer                   VARCHAR(60)  NOT NULL,
        assigned_price_cents    INTEGER      NOT NULL,
        currency                VARCHAR(3)   NOT NULL DEFAULT 'usd',
        ab_assignment_id        INTEGER      REFERENCES ab_assignments(id),
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

    # Experiment metadata columns on ab_tests
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS hypothesis TEXT",
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS offer VARCHAR(60)",
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS audience VARCHAR(100)",
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS control_price_cents INTEGER",
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS test_price_cents INTEGER",
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS min_sample INTEGER",
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS success_metric VARCHAR(60)",
    "ALTER TABLE ab_tests ADD COLUMN IF NOT EXISTS verdict VARCHAR(20)",

    # Verdict check constraint (safe to run repeatedly via DO block)
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'ab_tests'
              AND constraint_name = 'check_ab_test_verdict'
        ) THEN
            ALTER TABLE ab_tests ADD CONSTRAINT check_ab_test_verdict
                CHECK (verdict IS NULL OR verdict IN ('control_wins', 'test_wins', 'inconclusive'));
        END IF;
    END$$
    """,
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("apply_price_assignment: done.")


if __name__ == "__main__":
    main()
