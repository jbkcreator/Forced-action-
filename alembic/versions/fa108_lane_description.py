"""Add lane_description columns to lanes table.

Stores a Haiku-generated distress summary per lane.
description_tier tracks the intent_tier at generation time — tier change
signals the description is outdated and needs regeneration.

Revision ID: fa108_lane_description
Revises:     fa107_lender_rejected_state
Create Date: 2026-06-30
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa108_lane_description"
down_revision = "fa107_lender_rejected_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("lanes", sa.Column("lane_description", sa.Text(), nullable=True))
    op.add_column("lanes", sa.Column("description_tier", sa.String(50), nullable=True))
    op.add_column("lanes", sa.Column("description_generated_at", sa.TIMESTAMP(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("lanes", "description_generated_at")
    op.drop_column("lanes", "description_tier")
    op.drop_column("lanes", "lane_description")
