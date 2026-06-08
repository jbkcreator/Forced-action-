"""fa074 - add win_autopsy learning card type

Revision ID: fa074_win_autopsy_card
Revises: fa073_contact_freshness
Create Date: 2026-06-08
"""
from typing import Sequence, Union

from alembic import op

revision: str = "fa074_win_autopsy_card"
down_revision: Union[str, Sequence[str], None] = "fa073_contact_freshness"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("check_card_type", "learning_cards", type_="check")
    op.create_check_constraint(
        "check_card_type",
        "learning_cards",
        "card_type IN ("
        "'message_perf','deal_pattern','ab_result',"
        "'churn_signal','pricing_test','general',"
        "'autonomy_summary','kill_switch_scorecard','win_autopsy'"
        ")",
    )


def downgrade() -> None:
    op.drop_constraint("check_card_type", "learning_cards", type_="check")
    op.create_check_constraint(
        "check_card_type",
        "learning_cards",
        "card_type IN ("
        "'message_perf','deal_pattern','ab_result',"
        "'churn_signal','pricing_test','general',"
        "'autonomy_summary','kill_switch_scorecard'"
        ")",
    )
