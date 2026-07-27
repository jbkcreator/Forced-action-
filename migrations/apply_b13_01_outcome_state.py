"""T-B13-01 — add the one-tap buyer-outcome columns to deal_outcomes.

Adds outcome_state (closed/dead/pending), dead_reason, and reason_fault_class
(lead_fault/buyer_neutral) so the delivered-lead card can capture a buyer's
outcome tap plus a required reason on 'dead'. All additive/nullable — existing
DealOutcome writers (deal-capture legacy path, CDE label-layer, founder import)
are unaffected; they leave outcome_state NULL.

Guards:
- outcome_state ∈ {closed,dead,pending} or NULL
- reason_fault_class ∈ {lead_fault,buyer_neutral} or NULL
- dead requires a dead_reason (a 'dead' row must carry a reason)
Plus an index on reason_fault_class (the dispatcher routes lead_fault → retune).

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_b13_01_outcome_state.py
"""

from sqlalchemy import text
from src.core.database import Database

COLUMNS = [
    "ALTER TABLE deal_outcomes ADD COLUMN IF NOT EXISTS outcome_state VARCHAR(10)",
    "ALTER TABLE deal_outcomes ADD COLUMN IF NOT EXISTS dead_reason VARCHAR(30)",
    "ALTER TABLE deal_outcomes ADD COLUMN IF NOT EXISTS reason_fault_class VARCHAR(15)",
]

CONSTRAINTS = [
    (
        "ck_deal_outcomes_outcome_state",
        "outcome_state IS NULL OR outcome_state IN ('closed','dead','pending')",
    ),
    (
        "ck_deal_outcomes_reason_fault_class",
        "reason_fault_class IS NULL OR reason_fault_class IN ('lead_fault','buyer_neutral')",
    ),
    (
        "ck_deal_outcomes_dead_requires_reason",
        "outcome_state <> 'dead' OR dead_reason IS NOT NULL",
    ),
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_deal_outcomes_fault_class "
    "ON deal_outcomes (reason_fault_class)",
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in COLUMNS:
            s.execute(text(stmt))
        # ADD CONSTRAINT has no IF NOT EXISTS — drop-then-add keeps it idempotent.
        for name, expr in CONSTRAINTS:
            s.execute(text(f"ALTER TABLE deal_outcomes DROP CONSTRAINT IF EXISTS {name}"))
            s.execute(text(f"ALTER TABLE deal_outcomes ADD CONSTRAINT {name} CHECK ({expr})"))
        for stmt in INDEXES:
            s.execute(text(stmt))
    print("deal_outcomes outcome_state / dead_reason / reason_fault_class applied.")


if __name__ == "__main__":
    main()
