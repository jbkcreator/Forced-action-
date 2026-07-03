"""add_bundle_ab_variant

Stage 5 — adds `ab_variant` column to `bundle_purchases` so each purchase
can be attributed to the A/B pricing variant Cora dispatched. NULL means
no test was active when the purchase was created (e.g. organic
SMS-command purchase).

Revision ID: fa005_bundle_ab_variant
Revises:     fa004_premium_purchases
Create Date: 2026-04-30
"""

import sqlalchemy as sa
from alembic import op


revision = "fa005_bundle_ab_variant"
down_revision = "fa004_premium_purchases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "bundle_purchases",
        sa.Column("ab_variant", sa.String(length=8), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("bundle_purchases", "ab_variant")
