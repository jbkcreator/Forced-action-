"""unique constraint on unmatched_records instrument_number source_type county_id

Revision ID: 1dfb55b61ac6
Revises: i9j0k1l2m3n4
Create Date: 2026-04-01 11:57:29.482097

Adds a partial unique index on (instrument_number, source_type, county_id)
WHERE instrument_number IS NOT NULL.

NULLs (evictions, bankruptcies, probate) are excluded by the WHERE clause
so they continue to insert freely. Records with the same instrument number
across different source_types or counties are still allowed.
"""
from typing import Sequence, Union

from alembic import op

revision: str = '1dfb55b61ac6'
down_revision: Union[str, Sequence[str], None] = 'i9j0k1l2m3n4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "uq_unmatched_instrument_source_county",
        "unmatched_records",
        ["instrument_number", "source_type", "county_id"],
        unique=True,
        postgresql_where="instrument_number IS NOT NULL",
    )


def downgrade() -> None:
    op.drop_index(
        "uq_unmatched_instrument_source_county",
        table_name="unmatched_records",
    )
