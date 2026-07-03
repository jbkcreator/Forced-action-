"""add_broken_at_broken_reason_to_referral_teams

Phase 1 referral revocation — audit columns that record when and why a team
was broken (dispute | refund | churn).

Revision ID: fa013_referral_team_broken_audit
Revises:     fa012_stage6_subscriber_payment_recovery_fields
Create Date: 2026-05-12
"""

import sqlalchemy as sa
from alembic import op

revision = "fa013_referral_team_broken_audit"
down_revision = "fa012_stage6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "referral_teams",
        sa.Column("broken_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "referral_teams",
        sa.Column("broken_reason", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("referral_teams", "broken_reason")
    op.drop_column("referral_teams", "broken_at")
