"""fa070_court_docket_detail_on_liens

Adds the 4 court-docket detail columns to legal_and_liens (judgments), mirroring
fa069 on legal_proceedings. legal_and_liens already has
court_docket_parties/events/scraped_at.

Applied operationally via scripts/apply_fa070_ddl.py (Alembic CLI unusable on the
multi-head tree). This file is the migration of record.

Revision ID: fa070
Revises:     fa069
Create Date: 2026-06-09
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "fa070"
down_revision: Union[str, Sequence[str], None] = "fa069"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("legal_and_liens", sa.Column("mailing_address", sa.Text, nullable=True))
    op.add_column("legal_and_liens", sa.Column("docket_detail", postgresql.JSONB, nullable=True))
    op.add_column("legal_and_liens", sa.Column("balance_due", sa.Numeric(12, 2), nullable=True))
    op.add_column("legal_and_liens", sa.Column("docket_status", sa.String(30), nullable=True))
    op.create_index("idx_legal_docket_status", "legal_and_liens", ["docket_status"])
    op.create_check_constraint(
        "check_legal_docket_status", "legal_and_liens",
        "docket_status IS NULL OR docket_status IN "
        "('ok','case_number_missing','not_found','blocked','error')",
    )


def downgrade() -> None:
    op.drop_constraint("check_legal_docket_status", "legal_and_liens", type_="check")
    op.drop_index("idx_legal_docket_status", table_name="legal_and_liens")
    op.drop_column("legal_and_liens", "docket_status")
    op.drop_column("legal_and_liens", "balance_due")
    op.drop_column("legal_and_liens", "docket_detail")
    op.drop_column("legal_and_liens", "mailing_address")
