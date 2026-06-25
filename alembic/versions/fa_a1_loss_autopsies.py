"""fa_a1 — loss_autopsies table (Phase 3 A1: Loss Autopsy Engine)

Captures structured failure retrospectives whenever a lead is marked
closed_lost, declined, or ghosts past the 24-hour human-close SLA.
Claude parses multi-source context and classifies the loss reason.

NOTE: Alembic CLI is unusable in this tree (divergent multi-head history).
Apply with the idempotent companion script:

    PYTHONPATH=. python scripts/apply_fa_a1_loss_autopsies.py

Revision ID: fa_a1_loss_autopsies
Revises: fa_s5_enhancement_loops
Create Date: 2026-06-24
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "fa_a1_loss_autopsies"
down_revision: Union[str, Sequence[str], None] = "fa_s5_enhancement_loops"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "loss_autopsies",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "prospect_id",
            UUID(as_uuid=True),
            sa.ForeignKey("prospects.prospect_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "deal_outcome_id",
            sa.Integer(),
            sa.ForeignKey("deal_outcomes.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("trigger_reason", sa.String(50), nullable=False),
        sa.Column("primary_rejection_reason", sa.Text(), nullable=True),
        sa.Column("competitor_rate_delta", sa.Numeric(8, 4), nullable=True),
        sa.Column("underwriting_blocker", sa.Text(), nullable=True),
        sa.Column("cora_behavior_adjustment", sa.Text(), nullable=True),
        sa.Column(
            "raw_context",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "model_response",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("claude_cost_usd", sa.Numeric(10, 6), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "trigger_reason IN ('CLOSED_LOST','DECLINED','GHOSTED_SLA')",
            name="ck_loss_autopsy_trigger",
        ),
    )

    op.create_index("idx_loss_autopsies_property_id", "loss_autopsies", ["property_id"])
    op.create_index("idx_loss_autopsies_deal_outcome_id", "loss_autopsies", ["deal_outcome_id"])
    op.create_index("idx_loss_autopsies_trigger_reason", "loss_autopsies", ["trigger_reason"])
    op.create_index(
        "idx_loss_autopsies_created_at",
        "loss_autopsies",
        ["created_at"],
        postgresql_ops={"created_at": "DESC"},
    )
    # Idempotency: one autopsy per deal outcome
    op.execute("""
        CREATE UNIQUE INDEX idx_loss_autopsies_deal_outcome_once
            ON loss_autopsies(deal_outcome_id)
            WHERE deal_outcome_id IS NOT NULL
    """)


def downgrade() -> None:
    op.drop_index("idx_loss_autopsies_deal_outcome_once", table_name="loss_autopsies")
    op.drop_index("idx_loss_autopsies_created_at", table_name="loss_autopsies")
    op.drop_index("idx_loss_autopsies_trigger_reason", table_name="loss_autopsies")
    op.drop_index("idx_loss_autopsies_deal_outcome_id", table_name="loss_autopsies")
    op.drop_index("idx_loss_autopsies_property_id", table_name="loss_autopsies")
    op.drop_table("loss_autopsies")
