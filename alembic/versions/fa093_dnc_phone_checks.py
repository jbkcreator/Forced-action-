"""Universal DNC phone freshness checks

Revision ID: fa093_dnc_phone_checks
Revises: fa092_m12_event_failures
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa093_dnc_phone_checks"
down_revision: Union[str, Sequence[str]] = "fa092_m12_event_failures"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dnc_phone_checks",
        sa.Column("phone", sa.String(20), nullable=False),
        sa.Column("national_dnc", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("litigator", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("source", sa.String(40), nullable=False, server_default="tracerfy_dnc_refresh"),
        sa.Column("raw_result", JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("phone", name="pk_dnc_phone_checks"),
    )
    op.create_index("idx_dnc_phone_checks_checked_at", "dnc_phone_checks", ["checked_at"])
    op.create_index(
        "idx_dnc_phone_checks_clean_fresh",
        "dnc_phone_checks",
        ["checked_at"],
        postgresql_where=sa.text("national_dnc = false AND litigator = false"),
    )


def downgrade() -> None:
    op.drop_index("idx_dnc_phone_checks_clean_fresh", table_name="dnc_phone_checks")
    op.drop_index("idx_dnc_phone_checks_checked_at", table_name="dnc_phone_checks")
    op.drop_table("dnc_phone_checks")
