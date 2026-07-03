"""fa034_cora_incident

Adds the Cora self-healing primitives:

  1. `cora_incident` table — durable record of every metric breach Cora
     detects, the action she takes, and when it resolves.

  2. Eight nullable metric columns on `platform_daily_stats` — one row per
     metric per (run_date, county_id), used by the hourly self-healing job
     to compute 7-day rolling baselines without a new table.

Source of truth: FA-2B-v9-FINAL — Self-Healing Ops, Kill-Switch Discipline,
Q44, Q52. Plan: ~/.claude/plans/the-bronze-at-2-34-vs-ultra-platinum-at-happy-pretzel.md

Production note: the new table is empty until the cora_self_healing job
starts (gated by CORA_SELF_HEALING_ENABLED env var, default false), so this
migration is a pure additive change. Adding nullable columns to
platform_daily_stats is also lock-light on Postgres. Safe to apply during
normal hours.

Revision ID: fa034_cora_incident
Revises:     b4c8ae3057fa
Create Date: 2026-05-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "fa034_cora_incident"
down_revision: Union[str, Sequence[str], None] = "b4c8ae3057fa"
branch_labels = None
depends_on = None


# Metric columns added to platform_daily_stats so the self-healing job can
# read a 7-day rolling average per metric without a new table.
_BASELINE_METRIC_COLUMNS = [
    "sms_reply_rate",
    "offer_acceptance_rate",
    "first_payment_rate",
    "saved_card_rate",
    "wallet_adoption",
    "lock_conversion",
    "retention_30d",
]


def upgrade() -> None:
    # ── cora_incident ────────────────────────────────────────────────────
    op.create_table(
        "cora_incident",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("metric_name", sa.String(64), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=True),
        sa.Column("feature_name", sa.String(64), nullable=True),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("observed_value", sa.Numeric(10, 4), nullable=False),
        sa.Column("threshold_value", sa.Numeric(10, 4), nullable=False),
        sa.Column("baseline_value", sa.Numeric(10, 4), nullable=True),
        sa.Column("breach_started", sa.DateTime(timezone=True), nullable=False),
        sa.Column("breach_resolved", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_hours", sa.Integer, nullable=True),
        sa.Column(
            "action_taken", sa.String(32),
            nullable=False, server_default="no_op",
        ),
        sa.Column("action_details", JSONB, nullable=True),
        sa.Column("decision_id", UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "severity IN ('yellow','red')", name="check_cora_incident_severity",
        ),
        sa.CheckConstraint(
            "action_taken IN ('no_op','fallback_enabled','auto_paused',"
            "'human_escalated','feature_killed','resolved')",
            name="check_cora_incident_action",
        ),
    )

    # Partial index for the hot-path lookup: "do we have an open incident
    # for this (metric, county, feature) tuple?" Used every hour by the
    # self-healing job.
    op.execute(sa.text("""
        CREATE INDEX idx_cora_incident_metric_open
            ON cora_incident(metric_name, county_id, feature_name)
            WHERE breach_resolved IS NULL
    """))

    # Time-ordered scan for unresolved incidents (severity-prioritised) —
    # used by Revenue Pulse to pick the latest critical incident.
    op.execute(sa.text("""
        CREATE INDEX idx_cora_incident_unresolved
            ON cora_incident(severity, breach_started)
            WHERE breach_resolved IS NULL
    """))

    op.create_index(
        "idx_cora_incident_breach_started",
        "cora_incident", ["breach_started"],
        postgresql_using="btree",
    )

    # ── platform_daily_stats baseline columns ────────────────────────────
    # Nullable because historical rows pre-deploy won't have these. The
    # baseline helper treats NULL as "no data" and falls back to threshold-
    # only comparison.
    for col in _BASELINE_METRIC_COLUMNS:
        op.add_column(
            "platform_daily_stats",
            sa.Column(col, sa.Numeric(6, 4), nullable=True),
        )
    # cac_paid_channels uses a wider numeric form (dollars).
    op.add_column(
        "platform_daily_stats",
        sa.Column("cac_paid_channels", sa.Numeric(10, 2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("platform_daily_stats", "cac_paid_channels")
    for col in reversed(_BASELINE_METRIC_COLUMNS):
        op.drop_column("platform_daily_stats", col)

    op.drop_index("idx_cora_incident_breach_started", table_name="cora_incident")
    op.execute(sa.text("DROP INDEX IF EXISTS idx_cora_incident_unresolved"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_cora_incident_metric_open"))
    op.drop_table("cora_incident")
