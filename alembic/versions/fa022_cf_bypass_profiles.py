"""add cf_bypass_profiles

Per-county Cloudflare-bypass session metadata. Profile FILES live on the
scraping host's local disk under data/cf_session/edge_profile_<name>/ -
this table tracks metadata (status, last_warmed_at, last_validated_at,
failure reasons) plus an optional zipped backup blob so a fresh host can
restore a known-good profile without re-warming from scratch.

Revision ID: fa022_cf_bypass_profiles
Revises:     fa021
Create Date: 2026-05-15
"""

import sqlalchemy as sa
from alembic import op


revision = "fa022_cf_bypass_profiles"
down_revision = "fa021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cf_bypass_profiles",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("profile_name", sa.String(80), nullable=False, unique=True),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("portal_url", sa.Text(), nullable=False),
        sa.Column(
            "status", sa.String(20),
            nullable=False, server_default="unwarmed",
        ),
        sa.Column("last_warmed_at",      sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_validated_at",   sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_at",     sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_reason", sa.Text(), nullable=True),
        sa.Column("profile_dir_path",    sa.Text(), nullable=False),
        sa.Column(
            "validation_ttl_minutes", sa.Integer(),
            nullable=False, server_default="540",
        ),
        sa.Column("profile_blob",       sa.LargeBinary(), nullable=True),
        sa.Column("profile_blob_size",  sa.Integer(),     nullable=True),
        sa.Column("profile_blob_at",    sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "status IN ('unwarmed', 'ready', 'warming', 'expired', 'failed')",
            name="check_cf_profile_status",
        ),
    )
    op.create_index("ix_cf_bypass_profiles_county", "cf_bypass_profiles", ["county_id"])
    op.create_index("ix_cf_bypass_profiles_status", "cf_bypass_profiles", ["status"])


def downgrade() -> None:
    op.drop_index("ix_cf_bypass_profiles_status", table_name="cf_bypass_profiles")
    op.drop_index("ix_cf_bypass_profiles_county", table_name="cf_bypass_profiles")
    op.drop_table("cf_bypass_profiles")
