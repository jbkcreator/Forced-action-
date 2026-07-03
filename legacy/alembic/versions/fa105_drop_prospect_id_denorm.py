"""fa105_drop_prospect_id_denorm

Drop denormalized prospect_id columns from broker_transitions and
commission_ledger. Both tables carry lane_id which already links back
to the lane (and from there to prospect_id or property_id), making the
direct column redundant and a maintenance liability for property-only lanes.

Revision ID: fa105_drop_prospect_id_denorm
Revises:     fa104_lanes_property_id
Create Date: 2026-06-30
"""

from __future__ import annotations

from alembic import op


revision = "fa105_drop_prospect_id_denorm"
down_revision = "fa104_lanes_property_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("idx_bt_prospect_id", table_name="broker_transitions", if_exists=True)
    op.drop_column("broker_transitions", "prospect_id")
    op.drop_column("commission_ledger", "prospect_id")


def downgrade() -> None:
    import sqlalchemy as sa
    op.add_column(
        "broker_transitions",
        sa.Column("prospect_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "commission_ledger",
        sa.Column("prospect_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=True),
    )
