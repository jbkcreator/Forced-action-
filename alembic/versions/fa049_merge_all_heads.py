"""fa049 — merge all outstanding heads into a single linear tip

Merges:
  - 2750ffc7086f  (merge vendor cost monitor with main)
  - fa032_dbpr_company_name
  - fa034_learning_card_type_date_index
  - fa048_subscriber_revenue_synthflow
  - fa005_concierge_chat

Revision ID: fa049_merge_all_heads
Revises:     2750ffc7086f,
             fa032_dbpr_company_name,
             fa034_learning_card_type_date_index,
             fa048_subscriber_revenue_synthflow,
             fa005_concierge_chat
Create Date: 2026-05-28
"""

from alembic import op

revision = "fa049_merge_all_heads"
down_revision = (
    "2750ffc7086f",
    "fa032_dbpr_company_name",
    "fa034_learning_card_type_date_index",
    "fa048_subscriber_revenue_synthflow",
    "fa005_concierge_chat",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
