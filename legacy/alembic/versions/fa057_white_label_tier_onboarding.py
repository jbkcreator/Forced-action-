"""fa057 — Stage 12 White-label Tier Onboarding

Creates four new tables:
  white_label_clients              — B2B company accounts
  white_label_users                — team members per company
  white_label_api_keys             — programmatic access keys
  white_label_contractor_enrichments — Clay enrichment cache

Revision ID: fa057
Revises: fa056
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa057"
down_revision: str = "fa056"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── white_label_clients ──────────────────────────────────────────────────
    op.create_table(
        "white_label_clients",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("company_name", sa.String(255), nullable=False),
        sa.Column("company_slug", sa.String(100), nullable=False, unique=True),
        sa.Column("display_name", sa.String(255), nullable=True),
        sa.Column("admin_email", sa.String(255), nullable=False),
        sa.Column("admin_name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="pending_verification"),
        sa.Column("stripe_customer_id", sa.String(100), nullable=True, unique=True),
        sa.Column("stripe_subscription_id", sa.String(100), nullable=True, unique=True),
        sa.Column("plan_tier", sa.String(20), nullable=True),
        sa.Column("plan_price_cents", sa.Integer, nullable=True),
        sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("logo_url", sa.String(500), nullable=True),
        sa.Column("primary_color", sa.String(7), nullable=True),
        sa.Column("secondary_color", sa.String(7), nullable=True),
        sa.Column("counties_enabled", JSONB, nullable=True),
        sa.Column("verticals_enabled", JSONB, nullable=True),
        sa.Column("api_enabled", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("api_requests_per_day", sa.Integer, nullable=False, server_default="10000"),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("churned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('pending_verification','active','suspended','churned')",
            name="check_wl_client_status",
        ),
        sa.CheckConstraint(
            "plan_tier IS NULL OR plan_tier IN ('standard','premium')",
            name="check_wl_client_plan_tier",
        ),
    )
    op.create_index("idx_wl_client_slug",         "white_label_clients", ["company_slug"], unique=True)
    op.create_index("idx_wl_client_admin_email",  "white_label_clients", ["admin_email"])
    op.create_index("idx_wl_client_status",        "white_label_clients", ["status"])
    op.create_index("idx_wl_client_stripe_cid",   "white_label_clients", ["stripe_customer_id"])

    # ── white_label_users ────────────────────────────────────────────────────
    op.create_table(
        "white_label_users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("client_id", sa.Integer, sa.ForeignKey("white_label_clients.id", ondelete="CASCADE"), nullable=False),
        sa.Column("email", sa.String(255), nullable=False, unique=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("role", sa.String(20), nullable=False, server_default="member"),
        sa.Column("password_hash", sa.String(255), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("email_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reset_token", sa.String(64), nullable=True),
        sa.Column("reset_token_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("invited_by_id", sa.Integer, sa.ForeignKey("white_label_users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("role IN ('admin','member')", name="check_wl_user_role"),
    )
    op.create_index("idx_wl_user_client_id",     "white_label_users", ["client_id"])
    op.create_index("idx_wl_user_email",          "white_label_users", ["email"], unique=True)
    op.create_index("idx_wl_user_reset_token",   "white_label_users", ["reset_token"])
    op.create_index("idx_wl_user_client_email",  "white_label_users", ["client_id", "email"])

    # ── white_label_api_keys ─────────────────────────────────────────────────
    op.create_table(
        "white_label_api_keys",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("client_id", sa.Integer, sa.ForeignKey("white_label_clients.id", ondelete="CASCADE"), nullable=False),
        sa.Column("key_prefix", sa.String(12), nullable=False),
        sa.Column("key_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("label", sa.String(100), nullable=False, server_default="Default"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_by", sa.Integer, sa.ForeignKey("white_label_users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("requests_today", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_requests", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_wl_api_key_prefix",       "white_label_api_keys", ["key_prefix"])
    op.create_index("idx_wl_api_key_hash",          "white_label_api_keys", ["key_hash"], unique=True)
    op.create_index("idx_wl_api_key_client_active", "white_label_api_keys", ["client_id", "is_active"])

    # ── white_label_contractor_enrichments ───────────────────────────────────
    op.create_table(
        "white_label_contractor_enrichments",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("client_id", sa.Integer, sa.ForeignKey("white_label_clients.id", ondelete="CASCADE"), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("vertical", sa.String(50), nullable=False),
        sa.Column("clay_run_id", sa.String(100), nullable=True),
        sa.Column("data", JSONB, nullable=True),
        sa.Column("enriched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("client_id", "county_id", "vertical", name="uq_wl_contractor_enrichment"),
    )
    op.create_index("idx_wl_enrichment_client_county", "white_label_contractor_enrichments", ["client_id", "county_id"])


def downgrade() -> None:
    op.drop_table("white_label_contractor_enrichments")
    op.drop_table("white_label_api_keys")
    op.drop_table("white_label_users")
    op.drop_table("white_label_clients")
