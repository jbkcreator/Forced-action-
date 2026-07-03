"""fa104_lanes_property_id

Make lanes.prospect_id nullable and add lanes.property_id (FK to properties)
so the Loan Lane pool can be seeded directly from financing_intent_scores
without requiring a CDS prospect row.

Adds a partial unique constraint on (property_id, lane_type) WHERE
property_id IS NOT NULL to enforce one-lane-per-property.

Revision ID: fa104_lanes_property_id
Revises:     fa099_broker_transitions
Create Date: 2026-06-30
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "fa104_lanes_property_id"
down_revision = "fa099_broker_transitions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("lanes", "prospect_id", nullable=True)

    op.add_column(
        "lanes",
        sa.Column("property_id", sa.Integer, sa.ForeignKey("properties.id", name="fk_lanes_property_id"), nullable=True),
    )
    op.create_index(
        "ix_lanes_property_id",
        "lanes",
        ["property_id"],
        postgresql_where=sa.text("property_id IS NOT NULL"),
    )
    op.create_index(
        "uq_lanes_property_lane_type",
        "lanes",
        ["property_id", "lane_type"],
        unique=True,
        postgresql_where=sa.text("property_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_lanes_property_lane_type", table_name="lanes")
    op.drop_index("ix_lanes_property_id", table_name="lanes")
    op.drop_column("lanes", "property_id")
    op.alter_column("lanes", "prospect_id", nullable=False)
