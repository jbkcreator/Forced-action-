"""fa028 — add defendant column to foreclosures

Persists the borrower/defendant name from lis pendens filings so the
BatchData skip-trace path can target the actual homeowner by name
(parallel to the probate heir override already in src/services/skip_trace.py).

The lis pendens loader already extracts the defendant from the Grantee
column via _extract_owner_candidates(); previously it was used only for
property matching and discarded. After this migration the loader will
persist it.

Revision ID: fa028_foreclosures_defendant
Revises:     92ee22387084
Create Date: 2026-05-21
"""

import sqlalchemy as sa
from alembic import op

revision = "fa028_foreclosures_defendant"
down_revision = "92ee22387084"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "foreclosures",
        sa.Column("defendant", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_foreclosure_defendant",
        "foreclosures",
        ["defendant"],
    )


def downgrade() -> None:
    op.drop_index("idx_foreclosure_defendant", table_name="foreclosures")
    op.drop_column("foreclosures", "defendant")
