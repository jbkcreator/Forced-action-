"""fa011_column_mapping_extras

Add sample_rows, reject_feedback, mapped_by to county_column_mappings.

Revision ID: fa011_column_mapping_extras
Revises:     2ad0c9f6597c
Create Date: 2026-05-06
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "fa011_column_mapping_extras"
down_revision: Union[str, None] = "2ad0c9f6597c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "county_column_mappings",
        sa.Column("sample_rows", postgresql.JSONB(), nullable=True,
                  comment="First 3-5 rows of source data — shown in admin mapping UI"),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column("reject_feedback", sa.Text(), nullable=True,
                  comment="Admin feedback on why the mapping was rejected — passed to LLM on re-map"),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column("mapped_by", sa.String(10), server_default="llm", nullable=True,
                  comment="Who produced this mapping: llm or human"),
    )


def downgrade() -> None:
    op.drop_column("county_column_mappings", "sample_rows")
    op.drop_column("county_column_mappings", "reject_feedback")
    op.drop_column("county_column_mappings", "mapped_by")
