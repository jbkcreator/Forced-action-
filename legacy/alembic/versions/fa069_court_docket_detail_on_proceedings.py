"""fa069_court_docket_detail_on_proceedings

Adds Stage-2 court-docket detail columns to legal_proceedings, populated by the
per-engine detail extractor (Pinellas Eviction/Probate/Divorce). Detail is the
structured scrape_case() payload promoted onto the source row (JSONB-on-row),
not normalized court_* tables — see docs/adr/0012-court-docket-jsonb-on-row.md.

  mailing_address  — promoted party mailing address (Defendant/Petitioner/PR)
  docket_detail    — full scrape_case() dict (header/parties/events/documents/financial)
  balance_due      — Financial section Balance Due
  docket_status    — ok | case_number_missing | not_found | blocked | error

NOTE: applied operationally via scripts/apply_fa069_ddl.py (Alembic CLI is
unusable on this multi-head tree). This file is the migration of record.

Revision ID: fa069
Revises:     fa068
Create Date: 2026-06-08
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "fa069"
down_revision: Union[str, Sequence[str], None] = "fa068"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("legal_proceedings", sa.Column("mailing_address", sa.Text, nullable=True))
    op.add_column("legal_proceedings", sa.Column("docket_detail", postgresql.JSONB, nullable=True))
    op.add_column("legal_proceedings", sa.Column("balance_due", sa.Numeric(12, 2), nullable=True))
    op.add_column("legal_proceedings", sa.Column("docket_status", sa.String(30), nullable=True))

    op.create_index("idx_proceeding_docket_status", "legal_proceedings", ["docket_status"])

    op.create_check_constraint(
        "check_proceeding_docket_status",
        "legal_proceedings",
        "docket_status IS NULL OR docket_status IN "
        "('ok','case_number_missing','not_found','blocked','error')",
    )


def downgrade() -> None:
    op.drop_constraint("check_proceeding_docket_status", "legal_proceedings", type_="check")
    op.drop_index("idx_proceeding_docket_status", table_name="legal_proceedings")
    op.drop_column("legal_proceedings", "docket_status")
    op.drop_column("legal_proceedings", "balance_due")
    op.drop_column("legal_proceedings", "docket_detail")
    op.drop_column("legal_proceedings", "mailing_address")
