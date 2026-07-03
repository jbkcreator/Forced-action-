"""fa_s1 — financing_intent_scores table

Sprint S1 financing-intent scoring lane. Stores per-property daily scoring
output from src/services/financing_intent_engine.py: score, tier, recommended
loan product, per-signal flags/scores/details, source row IDs, and excluded
signal reasons.

NOTE: the alembic CLI is unusable in this tree (divergent multi-head history).
Apply with the idempotent companion script:

    PYTHONPATH=. python scripts/apply_fa_s1_financing_intent_scores.py

This file is the schema-of-record; the script performs the same DDL.

Revision ID: fa_s1_financing_intent_scores
Revises: fa078_contactability_detail
Create Date: 2026-06-16
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa_s1_financing_intent_scores"
down_revision: Union[str, Sequence[str], None] = "fa078_contactability_detail"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "financing_intent_scores",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=True),
        sa.Column("score_date", sa.Date(), nullable=False),
        sa.Column("financing_intent_score", sa.Numeric(5, 2), nullable=False),
        sa.Column("intent_tier", sa.String(20), nullable=False),
        sa.Column("recommended_product", sa.String(50), nullable=True),
        sa.Column("signal_flags", JSONB(), nullable=False, server_default="{}"),
        sa.Column("signal_scores", JSONB(), nullable=False, server_default="{}"),
        sa.Column("signal_details", JSONB(), nullable=False, server_default="{}"),
        sa.Column("source_ids", JSONB(), nullable=False, server_default="{}"),
        sa.Column("excluded_reasons", JSONB(), nullable=False, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.UniqueConstraint("property_id", "score_date", name="uq_fis_property_date"),
        sa.CheckConstraint(
            "intent_tier IN ('high', 'medium', 'low')",
            name="ck_fis_intent_tier",
        ),
    )
    op.create_index("idx_fis_property_id", "financing_intent_scores", ["property_id"])
    op.create_index("idx_fis_county_id",   "financing_intent_scores", ["county_id"])
    op.create_index("idx_fis_score_date",  "financing_intent_scores", ["score_date"])
    op.create_index("idx_fis_intent_tier", "financing_intent_scores", ["intent_tier"])
    op.create_index(
        "idx_fis_score_desc",
        "financing_intent_scores",
        [sa.text("financing_intent_score DESC")],
    )


def downgrade() -> None:
    op.drop_table("financing_intent_scores")
