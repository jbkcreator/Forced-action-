"""Add premium_purchases.output_ref_expires_at — column existed on the ORM
model (added 2026-06-25, commit bd8a426) with no matching migration, so any
full-table ORM SELECT against premium_purchases (e.g.
_resolve_premium_purchase_from_charge, called by every charge.refunded
webhook) has been erroring with UndefinedColumn.

Revision ID: fa112_premium_purchases_output_ref_expires_at
Revises:     fa111_algorithmic_variance_log
Create Date: 2026-07-02
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa112_premium_purchases_output_ref_expires_at"
down_revision = "fa111_algorithmic_variance_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "premium_purchases",
        sa.Column("output_ref_expires_at", sa.DateTime(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("premium_purchases", "output_ref_expires_at")
