"""fa106_lanes_property_anchor

Remove prospect_id from lanes — lanes are Financing Intent only.
property_id becomes the sole mandatory FK (NOT NULL).
Replaces the partial unique index with a full unique constraint.

Revision ID: fa106_lanes_property_anchor
Revises:     fa105_drop_prospect_id_denorm
Create Date: 2026-06-30
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "fa106_lanes_property_anchor"
down_revision = "fa105_drop_prospect_id_denorm"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("idx_lanes_prospect_id", table_name="lanes", if_exists=True)
    op.drop_index("uq_lanes_property_lane_type", table_name="lanes", if_exists=True)
    op.drop_column("lanes", "prospect_id")
    op.alter_column("lanes", "property_id", nullable=False)
    op.create_index(
        "uq_lanes_property_lane_type",
        "lanes",
        ["property_id", "lane_type"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_lanes_property_lane_type", table_name="lanes")
    op.alter_column("lanes", "property_id", nullable=True)
    op.add_column(
        "lanes",
        sa.Column(
            "prospect_id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            sa.ForeignKey("prospects.prospect_id"),
            nullable=True,
        ),
    )
    op.create_index("idx_lanes_prospect_id", "lanes", ["prospect_id"])
