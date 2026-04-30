"""add_referral_teams

Stage 5 — referral team mechanic. Three confirmed referrals in the same
county + vertical unlock a Shared ZIP View for the trio.

Revision ID: fa006_referral_teams
Revises:     fa005_bundle_ab_variant
Create Date: 2026-04-30
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "fa006_referral_teams"
down_revision = "fa005_bundle_ab_variant"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "referral_teams",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("lead_subscriber_id", sa.Integer(), nullable=False),
        sa.Column("county_id", sa.String(length=50), nullable=False),
        sa.Column("vertical", sa.String(length=50), nullable=False),
        sa.Column("member_subscriber_ids", postgresql.ARRAY(sa.Integer()), nullable=False),
        sa.Column("shared_zips", postgresql.ARRAY(sa.String(length=10)), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        sa.Column("unlocked_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["lead_subscriber_id"], ["subscribers.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("status IN ('active', 'broken')", name="check_referral_team_status"),
    )
    op.create_index("ix_referral_teams_lead_subscriber_id", "referral_teams", ["lead_subscriber_id"])
    op.create_index("idx_referral_team_county_vertical", "referral_teams", ["county_id", "vertical"])


def downgrade() -> None:
    op.drop_index("idx_referral_team_county_vertical", table_name="referral_teams")
    op.drop_index("ix_referral_teams_lead_subscriber_id", table_name="referral_teams")
    op.drop_table("referral_teams")
