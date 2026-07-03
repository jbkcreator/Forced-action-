"""add conversion_tier_report to learning_cards check_card_type

Revision ID: fa086_learning_cards_conversion_type
Revises: fa085_win_story_approval_gate
Create Date: 2026-06-22
"""

from alembic import op

revision = "fa086_learning_cards_conversion_type"
down_revision = "fa085_win_story_approval_gate"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE learning_cards DROP CONSTRAINT IF EXISTS check_card_type")
    op.execute("""
        ALTER TABLE learning_cards ADD CONSTRAINT check_card_type CHECK (
            card_type = ANY(ARRAY[
                'message_perf',
                'deal_pattern',
                'ab_result',
                'churn_signal',
                'pricing_test',
                'general',
                'autonomy_summary',
                'kill_switch_scorecard',
                'win_autopsy',
                'conversion_tier_report'
            ])
        )
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE learning_cards DROP CONSTRAINT IF EXISTS check_card_type")
    op.execute("""
        ALTER TABLE learning_cards ADD CONSTRAINT check_card_type CHECK (
            card_type = ANY(ARRAY[
                'message_perf',
                'deal_pattern',
                'ab_result',
                'churn_signal',
                'pricing_test',
                'general',
                'autonomy_summary',
                'kill_switch_scorecard',
                'win_autopsy'
            ])
        )
    """)
