"""fa058 — White-label: intended_plan_tier column

Stores the plan a client selected during the signup wizard (intent only).
Distinct from plan_tier, which is only set when a real Stripe subscription
is created. Drives Subscribe pre-selection in the Billing UI.

Revision ID: fa058
Revises: fa057
"""

from alembic import op
import sqlalchemy as sa

revision: str = "fa058"
down_revision: str = "fa057"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "white_label_clients",
        sa.Column("intended_plan_tier", sa.String(20), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("white_label_clients", "intended_plan_tier")
