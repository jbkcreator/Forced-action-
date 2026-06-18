"""quora_questions table — Quora organic answer pipeline

Revision ID: fa081_quora_questions
Revises: fa080_closer_calls, fa080_dfy_lite_orders
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

revision: str = "fa081_quora_questions"
down_revision: Union[str, Sequence[str]] = ("fa080_closer_calls", "fa080_dfy_lite_orders")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "quora_questions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),

        # Quora identity
        sa.Column("qid", sa.BigInteger(), nullable=True),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),

        # Scraped signals
        sa.Column("answer_count", sa.Integer(), nullable=True),
        sa.Column("follower_count", sa.Integer(), nullable=True),
        sa.Column("view_count", sa.Integer(), nullable=True),
        sa.Column("is_locked", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_sensitive", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("topics", ARRAY(sa.Text()), nullable=True),
        sa.Column("created_time", sa.DateTime(timezone=True), nullable=True),

        # Deterministic scoring
        sa.Column("deterministic_score", sa.Integer(), nullable=True),
        sa.Column("deterministic_reasons", ARRAY(sa.Text()), nullable=True),

        # Cora classification
        sa.Column("matched_keyword", sa.Text(), nullable=True),
        sa.Column("cora_decision_id", sa.String(36), nullable=True),
        sa.Column("intent_lane", sa.String(60), nullable=True),
        sa.Column("recommended_action", sa.String(40), nullable=True),
        sa.Column("priority_score", sa.Integer(), nullable=True),
        sa.Column("risk_level", sa.String(20), nullable=True),
        sa.Column("cora_classification", JSONB(), nullable=True),

        # Answer workflow
        sa.Column("answer_draft", JSONB(), nullable=True),
        sa.Column("answer_status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("quora_answer_id", sa.Text(), nullable=True),

        # Housekeeping
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("last_classified_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),

        sa.CheckConstraint(
            "answer_status IN ('pending','drafted','skipped','published','failed')",
            name="check_quora_answer_status",
        ),
    )

    op.create_index("idx_quora_questions_qid", "quora_questions", ["qid"], unique=True)
    op.create_index("idx_quora_questions_keyword", "quora_questions", ["matched_keyword"])
    op.create_index("idx_quora_questions_recommended_action", "quora_questions", ["recommended_action"])
    op.create_index("idx_quora_questions_answer_status", "quora_questions", ["answer_status"])
    op.create_index("idx_quora_questions_action_priority", "quora_questions",
                    ["recommended_action", "priority_score"])


def downgrade() -> None:
    op.drop_table("quora_questions")
