"""fa_s5 — revenue_leak_log and win_story_assets tables

Sprint S5: Enhancement Workflows & Self-Growing Loops.
  - revenue_leak_log: nightly per-county Gold+ lead leak aggregate
  - win_story_assets: sanitised proof statements from lead-pack deliveries/loan events

NOTE: Alembic CLI is unusable in this tree (divergent multi-head history).
Apply with the idempotent companion script:

    PYTHONPATH=. python scripts/apply_fa_s5_enhancement_loops.py

Revision ID: fa_s5_enhancement_loops
Revises: fa_s1_financing_intent_scores
Create Date: 2026-06-18
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa_s5_enhancement_loops"
down_revision: Union[str, Sequence[str], None] = "fa_s1_financing_intent_scores"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "revenue_leak_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("log_date", sa.Date(), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("total_leads_leaked", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("estimated_dollar_value", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("vertical_breakdown", JSONB(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.UniqueConstraint("log_date", "county_id", name="uq_revenue_leak_day_county"),
    )
    op.create_index("idx_revenue_leak_date", "revenue_leak_log", ["log_date"])
    op.create_index("idx_revenue_leak_county", "revenue_leak_log", ["county_id"])

    op.create_table(
        "win_story_assets",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("proof_text", sa.Text(), nullable=False),
        sa.Column("amount_range", sa.String(60), nullable=True),
        sa.Column("is_public", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "event_type IN ('lead_pack', 'loan_funded')",
            name="ck_win_story_event_type",
        ),
    )
    op.create_index(
        "idx_win_story_public_created", "win_story_assets", ["is_public", "created_at"]
    )
    op.create_index("idx_win_story_county", "win_story_assets", ["county_id"])


def downgrade() -> None:
    op.drop_table("win_story_assets")
    op.drop_table("revenue_leak_log")
