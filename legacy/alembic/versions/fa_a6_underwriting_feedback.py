"""fa_a6 — underwriting_feedback table (Sprint 4.6)

Per-property audit log of broker underwriting decline reasons.
Each row drives nudges into scoring_weight_overrides (A3) so the CDS
prioritization engine de-values future properties carrying similar
risk variables.

NOTE: Alembic CLI is unusable in this tree (divergent multi-head history).
Apply with the idempotent companion script:

    PYTHONPATH=. python scripts/apply_fa_a6_underwriting_feedback.py

Revision ID: fa_a6_underwriting_feedback
Revises: fa_a5_pre_decision_snapshots
Create Date: 2026-06-25
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa_a6_underwriting_feedback"
down_revision: Union[str, Sequence[str], None] = "fa_a5_pre_decision_snapshots"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_VALID_CODES = (
    "ltv_too_high",
    "structural_damage",
    "commercial_zoning",
    "title_defect",
    "flood_zone",
    "environmental_hazard",
    "deferred_maintenance",
    "unpermitted_additions",
    "tenant_occupied",
    "market_saturation",
)


def upgrade() -> None:
    op.create_table(
        "underwriting_feedback",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "property_id",
            sa.Integer(),
            sa.ForeignKey("properties.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("reason_code", sa.String(60), nullable=False),
        sa.Column("reason_detail", sa.Text(), nullable=True),
        sa.Column("lender_id", sa.String(80), nullable=True),
        sa.Column("loan_amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("submitted_by", sa.String(80), nullable=False),
        sa.Column(
            "submitted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            f"reason_code IN ({', '.join(repr(c) for c in _VALID_CODES)})",
            name="ck_uw_feedback_reason_code",
        ),
    )
    op.create_index(
        "idx_uw_feedback_property_id",
        "underwriting_feedback",
        ["property_id"],
    )
    op.create_index(
        "idx_uw_feedback_submitted_at",
        "underwriting_feedback",
        ["submitted_at"],
        postgresql_ops={"submitted_at": "DESC"},
    )
    op.create_index(
        "idx_uw_feedback_reason_code",
        "underwriting_feedback",
        ["reason_code"],
    )


def downgrade() -> None:
    op.drop_index("idx_uw_feedback_reason_code", table_name="underwriting_feedback")
    op.drop_index("idx_uw_feedback_submitted_at", table_name="underwriting_feedback")
    op.drop_index("idx_uw_feedback_property_id", table_name="underwriting_feedback")
    op.drop_table("underwriting_feedback")
