"""Add ab_assignments.outcome_at (Task 4.1 retention_v1 time-windowed holdout).

Lets holdout_verdict enforce a conversion window ("any paid action within
N days") by comparing outcome_at - created_at, instead of treating any
eventual outcome as an unbounded conversion. Additive/nullable — every
existing AbAssignment consumer (bundle pricing, attribution rollout, annual
signup) is unaffected; only ab_engine.record_outcome populates it going
forward.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_ab_assignment_outcome_at.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    "ALTER TABLE ab_assignments "
    "ADD COLUMN IF NOT EXISTS outcome_at TIMESTAMP WITH TIME ZONE",
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("ab_assignments.outcome_at applied.")


if __name__ == "__main__":
    main()
