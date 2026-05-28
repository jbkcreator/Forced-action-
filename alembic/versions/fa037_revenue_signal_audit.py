"""fa037_revenue_signal_audit

Completes the Revenue Signal Score per-subscriber feature:

  1. Extends `user_segments` with 5 explainability + freshness columns:
       - revenue_signal_band         (low / medium / high / very_high)
       - revenue_signal_breakdown    (JSONB — per-component contributions)
       - revenue_signal_updated_at   (when the score itself last changed)
       - last_significant_action_at  (when a significant action last fired)
       - revenue_signal_last_action  (action label of the latest update)

  2. Creates the append-only `revenue_signal_score_events` audit table —
     one row per score update. Captures old/new/delta, the action that
     triggered the change, and arbitrary metadata. Indexed for the admin
     subscriber-detail lookup (subscriber_id, created_at DESC).

The existing `user_segments.revenue_signal_score INT DEFAULT 0` column
stays unchanged — back-compat with the 9 existing callsites that update
it via `reclassify_safe`. The new columns are nullable so existing rows
remain valid until the next score-update event fills them in.

Source of truth: FA-2B-v9-FINAL §Revenue Signal Score (completion).
Plan: ~/.claude/plans/the-bronze-at-2-34-vs-ultra-platinum-at-happy-pretzel.md

Production note: purely additive — 5 nullable cols + new table + indexes.
Safe during business hours. No backfill — audit history starts from
deploy time.

Revision ID: fa037_revenue_signal_audit
Revises:     fa036_cora_autonomy_scorecard
Create Date: 2026-05-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa037_revenue_signal_audit"
down_revision: Union[str, Sequence[str], None] = "fa036_cora_autonomy_scorecard"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── user_segments: 5 new explainability / freshness columns ─────────
    op.add_column(
        "user_segments",
        sa.Column("revenue_signal_band", sa.String(20), nullable=True),
    )
    op.add_column(
        "user_segments",
        sa.Column("revenue_signal_breakdown", JSONB, nullable=True),
    )
    op.add_column(
        "user_segments",
        sa.Column("revenue_signal_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "user_segments",
        sa.Column("last_significant_action_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "user_segments",
        sa.Column("revenue_signal_last_action", sa.String(80), nullable=True),
    )
    op.create_check_constraint(
        "check_revenue_signal_band",
        "user_segments",
        "revenue_signal_band IS NULL OR "
        "revenue_signal_band IN ('low', 'medium', 'high', 'very_high')",
    )

    # ── revenue_signal_score_events: append-only audit trail ───────────
    op.create_table(
        "revenue_signal_score_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "subscriber_id", sa.Integer,
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("action_type", sa.String(80), nullable=True),
        sa.Column("old_score", sa.Integer, nullable=True),
        sa.Column("new_score", sa.Integer, nullable=False),
        sa.Column("delta", sa.Integer, nullable=False),
        sa.Column("band", sa.String(20), nullable=True),
        sa.Column("breakdown", JSONB, nullable=True),
        sa.Column("metadata", JSONB, nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
    )
    op.create_index(
        "idx_rss_events_sub_time",
        "revenue_signal_score_events",
        ["subscriber_id", sa.text("created_at DESC")],
    )
    op.create_index(
        "idx_rss_events_action",
        "revenue_signal_score_events",
        ["action_type"],
    )
    op.create_index(
        "idx_rss_events_created",
        "revenue_signal_score_events",
        ["created_at"],
    )


def downgrade() -> None:
    # Drop the audit table + indexes first.
    op.drop_index("idx_rss_events_created", table_name="revenue_signal_score_events")
    op.drop_index("idx_rss_events_action", table_name="revenue_signal_score_events")
    op.drop_index("idx_rss_events_sub_time", table_name="revenue_signal_score_events")
    op.drop_table("revenue_signal_score_events")

    # Then peel off the user_segments additions.
    op.drop_constraint("check_revenue_signal_band", "user_segments", type_="check")
    op.drop_column("user_segments", "revenue_signal_last_action")
    op.drop_column("user_segments", "last_significant_action_at")
    op.drop_column("user_segments", "revenue_signal_updated_at")
    op.drop_column("user_segments", "revenue_signal_breakdown")
    op.drop_column("user_segments", "revenue_signal_band")
