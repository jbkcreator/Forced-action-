"""add phone_deliverability_snapshots

Daily snapshot of phone-deliverability quality across Gold+ leads. Populated
by src/tasks/phone_deliverability_sampler.py — samples N Gold+ contacts,
classifies each phone as mobile/landline/voip/unknown via cached
Owner.phone_metadata when present, falls back to Telnyx Number Lookup
otherwise. mobile_pct is the headline SMS-deliverability metric.

Revision ID: fa013_phone_deliv
Revises:     fa012_stage6
Create Date: 2026-05-11
"""

import sqlalchemy as sa
from alembic import op


revision = "fa013_phone_deliv"
down_revision = "fa012_stage6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "phone_deliverability_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("tier_filter", sa.String(40), nullable=False, server_default="gold_plus"),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("lookups_cached", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lookups_attempted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lookups_succeeded", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("mobile_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("voip_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("landline_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unknown_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("no_phone_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("mobile_pct", sa.Numeric(5, 2), nullable=True),
        sa.Column("vendor", sa.String(20), nullable=False, server_default="telnyx"),
        sa.Column("cost_cents", sa.Integer(), nullable=True, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "snapshot_date", "county_id", "tier_filter",
            name="uq_phone_deliv_snapshot_day",
        ),
    )
    op.create_index(
        "ix_phone_deliverability_snapshots_snapshot_date",
        "phone_deliverability_snapshots",
        ["snapshot_date"],
    )
    op.create_index(
        "ix_phone_deliverability_snapshots_county_id",
        "phone_deliverability_snapshots",
        ["county_id"],
    )
    op.create_index(
        "idx_phone_deliv_date_county",
        "phone_deliverability_snapshots",
        ["snapshot_date", "county_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_phone_deliv_date_county", table_name="phone_deliverability_snapshots")
    op.drop_index("ix_phone_deliverability_snapshots_county_id", table_name="phone_deliverability_snapshots")
    op.drop_index("ix_phone_deliverability_snapshots_snapshot_date", table_name="phone_deliverability_snapshots")
    op.drop_table("phone_deliverability_snapshots")
