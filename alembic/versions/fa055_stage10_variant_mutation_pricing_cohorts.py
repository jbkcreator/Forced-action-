"""fa055_stage10_variant_mutation_pricing_cohorts

Stage 10: A/B + Self-Healing schema additions.

  1. `message_variant_tests` — 3-slot (a/b/c) variant management per sequence.
     Stores send counts, conversion counts, slot status, and proving-cycle state.
     Replaces ad-hoc binary AbTest usage for message mutation.

  2. `variant_retirement_log` — idempotent audit record for every slot retirement,
     replacement, reversion, or promotion. idempotency_key UNIQUE prevents
     double-retirements on retry.

  3. `pricing_cohorts` — per-trade and per-county pricing overrides. Activated
     only after 6+ weeks of deal data and constrained within guardrail bounds.
     UNIQUE on (county_id, trade_vertical, price_type) with active status
     enforced at the application layer (only one active row per tuple at a time).

Revision ID: fa055
Revises:     fa054
Create Date: 2026-05-30
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa055"
down_revision: Union[str, Sequence[str], None] = "fa054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── message_variant_tests ─────────────────────────────────────────────
    op.create_table(
        "message_variant_tests",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("sequence_name", sa.String(100), nullable=False, unique=True),
        sa.Column("segment", sa.String(50), nullable=True),
        sa.Column("traffic_pct", sa.Integer, nullable=False, server_default="10"),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),

        # Slot A — control / baseline
        sa.Column("slot_a_body", sa.Text, nullable=False),
        sa.Column("slot_a_sends", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_a_conversions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_a_replies", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_a_status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("slot_a_retired_at", sa.DateTime(timezone=True), nullable=True),

        # Slot B
        sa.Column("slot_b_body", sa.Text, nullable=False),
        sa.Column("slot_b_sends", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_b_conversions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_b_replies", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_b_status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("slot_b_retired_at", sa.DateTime(timezone=True), nullable=True),

        # Slot C
        sa.Column("slot_c_body", sa.Text, nullable=False),
        sa.Column("slot_c_sends", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_c_conversions", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_c_replies", sa.Integer, nullable=False, server_default="0"),
        sa.Column("slot_c_status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("slot_c_retired_at", sa.DateTime(timezone=True), nullable=True),

        # Proving-cycle tracking — which slot just received a replacement body.
        sa.Column("proving_slot", sa.String(5), nullable=True),
        sa.Column("proving_baseline_conv_rate", sa.Numeric(8, 6), nullable=True),
        sa.Column("proving_started_at", sa.DateTime(timezone=True), nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),

        sa.CheckConstraint(
            "status IN ('active','paused','completed')",
            name="check_mvt_status",
        ),
        sa.CheckConstraint(
            "traffic_pct BETWEEN 1 AND 10",
            name="check_mvt_traffic_cap",
        ),
        sa.CheckConstraint(
            "slot_a_status IN ('active','retired') AND "
            "slot_b_status IN ('active','retired') AND "
            "slot_c_status IN ('active','retired')",
            name="check_mvt_slot_statuses",
        ),
        sa.CheckConstraint(
            "proving_slot IS NULL OR proving_slot IN ('a','b','c')",
            name="check_mvt_proving_slot",
        ),
    )
    op.create_index("idx_mvt_status", "message_variant_tests", ["status"])
    op.create_index("idx_mvt_sequence_name", "message_variant_tests", ["sequence_name"])

    # ── variant_retirement_log ────────────────────────────────────────────
    op.create_table(
        "variant_retirement_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("test_id", sa.Integer,
                  sa.ForeignKey("message_variant_tests.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("action", sa.String(30), nullable=False),
        sa.Column("slot", sa.String(5), nullable=False),
        sa.Column("old_body", sa.Text, nullable=True),
        sa.Column("new_body", sa.Text, nullable=True),
        sa.Column("old_conversion_rate", sa.Numeric(8, 6), nullable=True),
        sa.Column("new_conversion_rate", sa.Numeric(8, 6), nullable=True),
        sa.Column("reason", sa.Text, nullable=True),
        # Prevents double-retirements on job retry.
        sa.Column("idempotency_key", sa.String(120), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
        sa.CheckConstraint(
            "action IN ('retired','replaced','reverted','promoted','rollback')",
            name="check_vrl_action",
        ),
        sa.CheckConstraint(
            "slot IN ('a','b','c')",
            name="check_vrl_slot",
        ),
    )
    op.create_index("idx_vrl_test_id", "variant_retirement_log", ["test_id"])
    op.create_index("idx_vrl_idempotency_key", "variant_retirement_log", ["idempotency_key"])

    # ── pricing_cohorts ───────────────────────────────────────────────────
    op.create_table(
        "pricing_cohorts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("trade_vertical", sa.String(50), nullable=False),
        sa.Column("price_type", sa.String(30), nullable=False),
        sa.Column("base_price_cents", sa.Integer, nullable=False),
        sa.Column("adjusted_price_cents", sa.Integer, nullable=False),
        sa.Column("adjustment_pct", sa.Numeric(6, 2), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("activation_reason", sa.Text, nullable=True),
        sa.Column("rollback_reason", sa.Text, nullable=True),
        sa.Column("deal_weeks", sa.Integer, nullable=True),
        sa.Column("deal_count", sa.Integer, nullable=True),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rolled_back_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
        sa.CheckConstraint(
            "status IN ('pending','active','rolled_back')",
            name="check_pc_status",
        ),
        sa.CheckConstraint(
            "adjustment_pct BETWEEN -25 AND 25",
            name="check_pc_adjustment_bounds",
        ),
    )
    op.create_index(
        "idx_pc_county_vertical_type",
        "pricing_cohorts",
        ["county_id", "trade_vertical", "price_type"],
    )
    op.create_index("idx_pc_status", "pricing_cohorts", ["status"])

    # Partial index for the common lookup: active cohort for a (county, vertical, type).
    op.execute(sa.text("""
        CREATE UNIQUE INDEX idx_pc_active_unique
            ON pricing_cohorts(county_id, trade_vertical, price_type)
            WHERE status = 'active'
    """))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS idx_pc_active_unique"))
    op.drop_index("idx_pc_status", table_name="pricing_cohorts")
    op.drop_index("idx_pc_county_vertical_type", table_name="pricing_cohorts")
    op.drop_table("pricing_cohorts")

    op.drop_index("idx_vrl_idempotency_key", table_name="variant_retirement_log")
    op.drop_index("idx_vrl_test_id", table_name="variant_retirement_log")
    op.drop_table("variant_retirement_log")

    op.drop_index("idx_mvt_sequence_name", table_name="message_variant_tests")
    op.drop_index("idx_mvt_status", table_name="message_variant_tests")
    op.drop_table("message_variant_tests")
