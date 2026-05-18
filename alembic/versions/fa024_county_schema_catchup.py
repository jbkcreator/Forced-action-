"""fa024 — county schema catch-up

Applies all county-management DDL that was built on the scaling/county branch
and is NOT yet present in prod after the fa022 stamp:

  county_sources:
    - url: DROP NOT NULL
    - ADD scrape_mode VARCHAR(32) NOT NULL DEFAULT 'ai_only' + CHECK
    - ADD playwright_code TEXT
    - ADD playwright_code_version VARCHAR(32)
    - ADD playwright_code_approved BOOLEAN NOT NULL DEFAULT FALSE

  county_column_mappings:
    - ADD sample_rows JSONB
    - ADD reject_feedback TEXT
    - ADD mapped_by VARCHAR(10) DEFAULT 'llm'
    - ADD post_processors JSONB
    - ADD value_maps JSONB
    - ADD row_routing JSONB

  CREATE TABLE playwright_code_history
  CREATE TABLE cf_bypass_profiles

  sms_opt_outs.source: SET DEFAULT 'inbound_sms'

Revision ID: fa024_county_catchup
Revises:     fa023_human_close
Create Date: 2026-05-18
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from alembic import op

revision = "fa024_county_catchup"
down_revision = "fa023_human_close"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── county_sources: make url nullable ────────────────────────────────────
    op.alter_column("county_sources", "url", existing_type=sa.Text(), nullable=True)

    # ── county_sources: scrape-mode + playwright columns ─────────────────────
    op.add_column(
        "county_sources",
        sa.Column(
            "scrape_mode",
            sa.String(32),
            nullable=False,
            server_default="ai_only",
        ),
    )
    op.create_check_constraint(
        "check_county_sources_scrape_mode",
        "county_sources",
        "scrape_mode IN ('ai_only','playwright_only','playwright_then_ai','static_download','api')",
    )
    op.add_column(
        "county_sources",
        sa.Column("playwright_code", sa.Text(), nullable=True),
    )
    op.add_column(
        "county_sources",
        sa.Column("playwright_code_version", sa.String(32), nullable=True),
    )
    op.add_column(
        "county_sources",
        sa.Column(
            "playwright_code_approved",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )

    # ── county_column_mappings: new optional columns ──────────────────────────
    op.add_column(
        "county_column_mappings",
        sa.Column("sample_rows", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column("reject_feedback", sa.Text(), nullable=True),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column("mapped_by", sa.String(10), nullable=True, server_default="llm"),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column("post_processors", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column("value_maps", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column("row_routing", postgresql.JSONB(), nullable=True),
    )

    # ── playwright_code_history ───────────────────────────────────────────────
    op.create_table(
        "playwright_code_history",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "source_id",
            sa.Integer(),
            sa.ForeignKey("county_sources.id"),
            nullable=False,
        ),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("code", sa.Text(), nullable=True),
        sa.Column("prompt_version", sa.String(20), nullable=True),
        sa.Column("reason", sa.String(40), nullable=False),
        sa.Column(
            "is_approved",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "generated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "idx_pwc_history_source_generated",
        "playwright_code_history",
        ["source_id", "generated_at"],
    )
    op.create_index(
        "ix_playwright_code_history_county_id",
        "playwright_code_history",
        ["county_id"],
    )
    op.create_index(
        "ix_playwright_code_history_source_id",
        "playwright_code_history",
        ["source_id"],
    )

    # ── cf_bypass_profiles ────────────────────────────────────────────────────
    op.create_table(
        "cf_bypass_profiles",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("profile_name", sa.String(80), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("portal_url", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="unwarmed",
        ),
        sa.Column("last_warmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_reason", sa.Text(), nullable=True),
        sa.Column("profile_dir_path", sa.Text(), nullable=False),
        sa.Column(
            "validation_ttl_minutes",
            sa.Integer(),
            nullable=False,
            server_default="540",
        ),
        sa.Column("profile_blob", sa.LargeBinary(), nullable=True),
        sa.Column("profile_blob_size", sa.Integer(), nullable=True),
        sa.Column("profile_blob_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("profile_name", name="uq_cf_bypass_profile_name"),
        sa.CheckConstraint(
            "status IN ('unwarmed','ready','warming','expired','failed')",
            name="check_cf_profile_status",
        ),
    )
    op.create_index(
        "ix_cf_bypass_profiles_county_id",
        "cf_bypass_profiles",
        ["county_id"],
    )
    op.create_index(
        "ix_cf_bypass_profiles_status_lookup",
        "cf_bypass_profiles",
        ["status"],
    )

    # ── sms_opt_outs.source: set default ─────────────────────────────────────
    op.alter_column(
        "sms_opt_outs",
        "source",
        existing_type=sa.String(),
        nullable=False,
        server_default="inbound_sms",
    )


def downgrade() -> None:
    op.alter_column(
        "sms_opt_outs", "source", existing_type=sa.String(), server_default=None
    )
    op.drop_index("ix_cf_bypass_profiles_status_lookup", table_name="cf_bypass_profiles")
    op.drop_index("ix_cf_bypass_profiles_county_id", table_name="cf_bypass_profiles")
    op.drop_table("cf_bypass_profiles")
    op.drop_index("ix_playwright_code_history_source_id", table_name="playwright_code_history")
    op.drop_index("ix_playwright_code_history_county_id", table_name="playwright_code_history")
    op.drop_index("idx_pwc_history_source_generated", table_name="playwright_code_history")
    op.drop_table("playwright_code_history")
    for col in ("row_routing", "value_maps", "post_processors", "mapped_by", "reject_feedback", "sample_rows"):
        op.drop_column("county_column_mappings", col)
    op.drop_column("county_sources", "playwright_code_approved")
    op.drop_column("county_sources", "playwright_code_version")
    op.drop_column("county_sources", "playwright_code")
    op.drop_constraint("check_county_sources_scrape_mode", "county_sources", type_="check")
    op.drop_column("county_sources", "scrape_mode")
    op.alter_column("county_sources", "url", existing_type=sa.Text(), nullable=False)
