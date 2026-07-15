"""Add 'holdout_result' to learning_cards.check_card_type (Task 4.1).

cora_holdout_check.py needs its own card_type, not 'ab_result' — that value
is already written daily by ab_rollback_check.py/cora_attribution_rollback_
check.py, and learning_cards has a table-wide UNIQUE(card_date, card_type):
sharing a type risks two unrelated jobs' results overwriting each other the
same day.

Postgres has no ALTER CHECK CONSTRAINT — drop and recreate. Idempotent
(DROP IF EXISTS + recreate is safe to rerun).

Includes 'kill_switch_scorecard', 'win_autopsy', 'conversion_tier_report' —
already live in the DB's check_card_type constraint (confirmed via
pg_get_constraintdef) but missing from models.py's CheckConstraint string
(pre-existing drift, unrelated to Task 4.1). This migration is the one
place both need to agree, so it restates the full real set rather than
just appending 'holdout_result' on top of the stale model string.

Usage:
    PYTHONPATH=. python migrations/apply_learning_card_holdout_result.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    "ALTER TABLE learning_cards DROP CONSTRAINT IF EXISTS check_card_type",
    """
    ALTER TABLE learning_cards ADD CONSTRAINT check_card_type CHECK (
        card_type IN (
            'message_perf', 'deal_pattern', 'ab_result',
            'churn_signal', 'pricing_test', 'general',
            'autonomy_summary', 'kill_switch_scorecard',
            'win_autopsy', 'conversion_tier_report',
            'holdout_result'
        )
    )
    """,
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
    print("learning_cards.check_card_type: 'holdout_result' added.")


if __name__ == "__main__":
    main()
