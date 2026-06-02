"""fa067_supplier_intelligence

Supplier Intelligence Foundation — Phase 1 schema.

Creates four tables:
  1. supplier_accounts          — one row per supplier company
  2. supplier_subscriptions     — Stripe subscription per account
  3. supplier_reports           — generated report with section JSONB
  4. supplier_report_exports    — PDF/CSV export audit

Phase 1: foundation/shell. Advanced analytics (deal benchmarks, recommendations)
return "insufficient_data" at the application layer until deal outcome volume
meets the thresholds in config/supplier_intel_config.py.

Revision ID: fa067
Revises:     fa066
Create Date: 2026-06-02
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa067"
down_revision: Union[str, Sequence[str], None] = "fa066"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── supplier_accounts ──────────────────────────────────────────────────
    op.create_table(
        "supplier_accounts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("company_name", sa.String(255), nullable=False),
        sa.Column("contact_name", sa.String(255), nullable=True),
        sa.Column("contact_email", sa.String(255), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("counties", JSONB, nullable=True),   # list of county_id strings
        sa.Column("verticals", JSONB, nullable=True),  # list of vertical codes
        sa.Column("access_token", sa.String(36), nullable=False, unique=True),
        sa.Column("stripe_customer_id", sa.String(100), nullable=True, unique=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "status IN ('active','suspended','canceled')",
            name="ck_supplier_accounts_status",
        ),
    )
    op.create_index("idx_supplier_accounts_email", "supplier_accounts", ["contact_email"])
    op.create_index("idx_supplier_accounts_status", "supplier_accounts", ["status"])

    # ── supplier_subscriptions ─────────────────────────────────────────────
    op.create_table(
        "supplier_subscriptions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "account_id", sa.Integer,
            sa.ForeignKey("supplier_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("plan_tier", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="trialing"),
        sa.Column("stripe_subscription_id", sa.String(100), nullable=True, unique=True),
        sa.Column("stripe_price_id", sa.String(100), nullable=True),
        sa.Column("price_cents", sa.Integer, nullable=True),
        sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "status IN ('trialing','active','past_due','canceled')",
            name="ck_supplier_subscriptions_status",
        ),
        sa.CheckConstraint(
            "plan_tier IN ('foundation','standard','premium')",
            name="ck_supplier_subscriptions_tier",
        ),
    )
    op.create_index("idx_supplier_subscriptions_account", "supplier_subscriptions", ["account_id"])
    op.create_index("idx_supplier_subscriptions_status", "supplier_subscriptions", ["status"])

    # ── supplier_reports ───────────────────────────────────────────────────
    op.create_table(
        "supplier_reports",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "account_id", sa.Integer,
            sa.ForeignKey("supplier_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("report_period_start", sa.Date, nullable=True),
        sa.Column("report_period_end", sa.Date, nullable=True),
        sa.Column("sections_json", JSONB, nullable=True),              # per-section data/N/A
        sa.Column("data_readiness_snapshot", JSONB, nullable=True),   # thresholds vs current counts
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "status IN ('pending','generated','failed','exported')",
            name="ck_supplier_reports_status",
        ),
    )
    op.create_index("idx_supplier_reports_account_date",
                    "supplier_reports", ["account_id", "created_at"])
    op.create_index("idx_supplier_reports_status", "supplier_reports", ["status"])

    # ── supplier_report_exports ────────────────────────────────────────────
    op.create_table(
        "supplier_report_exports",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "report_id", sa.Integer,
            sa.ForeignKey("supplier_reports.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("format", sa.String(10), nullable=False),
        sa.Column("file_path", sa.Text, nullable=True),
        sa.Column("exported_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("emailed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "format IN ('pdf','csv')",
            name="ck_supplier_report_exports_format",
        ),
    )
    op.create_index("idx_supplier_report_exports_report",
                    "supplier_report_exports", ["report_id"])


def downgrade() -> None:
    op.drop_index("idx_supplier_report_exports_report", table_name="supplier_report_exports")
    op.drop_table("supplier_report_exports")

    op.drop_index("idx_supplier_reports_status", table_name="supplier_reports")
    op.drop_index("idx_supplier_reports_account_date", table_name="supplier_reports")
    op.drop_table("supplier_reports")

    op.drop_index("idx_supplier_subscriptions_status", table_name="supplier_subscriptions")
    op.drop_index("idx_supplier_subscriptions_account", table_name="supplier_subscriptions")
    op.drop_table("supplier_subscriptions")

    op.drop_index("idx_supplier_accounts_status", table_name="supplier_accounts")
    op.drop_index("idx_supplier_accounts_email", table_name="supplier_accounts")
    op.drop_table("supplier_accounts")
