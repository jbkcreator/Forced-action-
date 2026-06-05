"""fa071 - add structured Cora override reason code

Revision ID: fa071_agent_decision_override_reason_code
Revises: fa_tracerfy_source
Create Date: 2026-06-04
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "fa071_agent_decision_override_reason_code"
down_revision: Union[str, Sequence[str], None] = "fa_tracerfy_source"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


VALUES = (
    "factual_error",
    "compliance_risk",
    "wrong_audience",
    "bad_timing",
    "low_lead_quality",
    "offer_mismatch",
    "tone_or_brand_risk",
    "duplicate_or_redundant",
    "customer_context_missing",
    "operator_strategy",
    "other",
)


def upgrade() -> None:
    op.add_column(
        "agent_decisions",
        sa.Column("override_reason_code", sa.String(length=40), nullable=True),
    )
    op.create_check_constraint(
        "check_agent_override_reason_code",
        "agent_decisions",
        "override_reason_code IS NULL OR override_reason_code IN "
        f"({', '.join(repr(value) for value in VALUES)})",
    )
    op.create_index(
        "idx_agent_decisions_override_reason_code",
        "agent_decisions",
        ["override_reason_code"],
    )


def downgrade() -> None:
    op.drop_index(
        "idx_agent_decisions_override_reason_code",
        table_name="agent_decisions",
    )
    op.drop_constraint(
        "check_agent_override_reason_code",
        "agent_decisions",
        type_="check",
    )
    op.drop_column("agent_decisions", "override_reason_code")
