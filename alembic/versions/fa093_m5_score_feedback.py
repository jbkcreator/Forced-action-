"""M5 CDS Feedback — score_feedback table (spec §4.4)

Revision ID: fa093_m5_score_feedback
Revises: fa092_m12_event_failures
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

revision: str = "fa093_m5_score_feedback"
down_revision: Union[str, Sequence[str]] = "fa092_m12_event_failures"
branch_labels = None
depends_on = None

_TIER_CHECK = (
    "predicted_tier IN ('Bronze','Silver','Gold','Platinum','Ultra','sub_grade')"
)
_OUTCOME_CHECK = (
    "realized_outcome IS NULL OR "
    "realized_outcome IN ('contacted','converted','funded','dead')"
)


def upgrade() -> None:
    op.create_table(
        "score_feedback",
        sa.Column(
            "score_id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("generate_uuidv7()"),
        ),
        sa.Column(
            "prospect_id",
            PG_UUID(as_uuid=True),
            sa.ForeignKey("prospects.prospect_id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("closer_call_id", sa.BigInteger(), nullable=True),
        sa.Column("predicted_tier", sa.String(), nullable=False),
        sa.Column("predicted_rate", sa.Numeric(6, 4), nullable=True),
        sa.Column("realized_outcome", sa.String(), nullable=True),
        sa.Column("delta", sa.Numeric(8, 4), nullable=True),
        sa.Column(
            "scored_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("prospect_id", name="uq_score_feedback_prospect_id"),
        sa.CheckConstraint(_TIER_CHECK, name="ck_score_feedback_predicted_tier"),
        sa.CheckConstraint(_OUTCOME_CHECK, name="ck_score_feedback_realized_outcome"),
    )
    op.create_index("idx_score_feedback_prospect_id", "score_feedback", ["prospect_id"])
    op.create_index("idx_score_feedback_predicted_tier", "score_feedback", ["predicted_tier"])
    op.create_index(
        "idx_score_feedback_closer_call_id",
        "score_feedback",
        ["closer_call_id"],
        postgresql_where=sa.text("closer_call_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_score_feedback_closer_call_id", table_name="score_feedback")
    op.drop_index("idx_score_feedback_predicted_tier", table_name="score_feedback")
    op.drop_index("idx_score_feedback_prospect_id", table_name="score_feedback")
    op.drop_table("score_feedback")
