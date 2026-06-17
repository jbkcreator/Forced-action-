"""closer_calls table — Closer Cockpit (Sprint S1b)

Revision ID: fa080_closer_calls
Revises: fa079_synthflow_transcript_columns

NOTE: The Alembic tree is multi-head — this file is written for the record /
lineage. Apply the DDL via `scripts/apply_closer_calls_ddl.py` (idempotent,
checkfirst) rather than the alembic CLI.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa080_closer_calls"
down_revision: Union[str, None] = "fa079_synthflow_transcript_columns"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "closer_calls",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("aircall_call_id", sa.String(40), nullable=False),
        sa.Column("subscriber_id", sa.Integer, sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("escalation_id", sa.Integer, sa.ForeignKey("human_close_escalations.id"), nullable=True),
        sa.Column("closer_aircall_user_id", sa.String(40)),
        sa.Column("closer_name", sa.String(120)),
        sa.Column("direction", sa.String(12)),
        sa.Column("dialed_e164", sa.String(20)),
        sa.Column("duration_sec", sa.Integer),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("transcript_text", sa.Text),
        sa.Column("transcript_fetched_at", sa.DateTime(timezone=True)),
        sa.Column("sentiment", sa.String(12)),
        sa.Column("topics", JSONB),
        sa.Column("objections", JSONB),
        sa.Column("objection_resolved", sa.String(12)),
        sa.Column("call_outcome", sa.String(30)),
        sa.Column("follow_ups", JSONB),
        sa.Column("tagged_at", sa.DateTime(timezone=True)),
        sa.Column("objection_type", sa.String(40)),
        sa.Column("pitch_variant", sa.String(40)),
        sa.Column("lead_quality_rating", sa.Integer),
        sa.Column("feedback_by", sa.String(120)),
        sa.Column("feedback_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "lead_quality_rating IS NULL OR (lead_quality_rating BETWEEN 1 AND 5)",
            name="ck_closer_calls_lead_quality",
        ),
        sa.CheckConstraint(
            "call_outcome IS NULL OR call_outcome IN "
            "('committed','callback_scheduled','undecided','declined','no_meaningful_conversation')",
            name="ck_closer_calls_outcome",
        ),
        sa.CheckConstraint(
            "objection_resolved IS NULL OR objection_resolved IN ('resolved','unresolved','none')",
            name="ck_closer_calls_obj_resolved",
        ),
        sa.CheckConstraint(
            "sentiment IS NULL OR sentiment IN ('positive','neutral','negative','mixed')",
            name="ck_closer_calls_sentiment",
        ),
    )
    op.create_unique_constraint("uq_closer_calls_aircall_id", "closer_calls", ["aircall_call_id"])
    op.create_index("idx_closer_calls_subscriber", "closer_calls", ["subscriber_id"])
    op.create_index("idx_closer_calls_closer_started", "closer_calls", ["closer_aircall_user_id", "started_at"])
    op.create_index("idx_closer_calls_tagged_at", "closer_calls", ["tagged_at"])


def downgrade() -> None:
    op.drop_table("closer_calls")
