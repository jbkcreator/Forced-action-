"""fa031 — skip trace waterfall: confidence column + expand source constraint

Changes:
  1. enriched_contacts.confidence NUMERIC(4,3) — quality score from waterfall
  2. check_enriched_source constraint expanded to include 'pdl' as valid source

Revision ID: fa031_skip_trace_waterfall
Revises:     fa030_merge_foreclosures_defendant
Create Date: 2026-05-25
"""

import sqlalchemy as sa
from alembic import op

revision = "fa031_skip_trace_waterfall"
down_revision = "fa030_merge_foreclosures_defendant"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "enriched_contacts",
        sa.Column("confidence", sa.Numeric(4, 3), nullable=True),
    )

    # Expand source check constraint to include 'pdl'
    op.execute("ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source")
    op.execute(
        "ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_source "
        "CHECK (source IN ('batch_skip_tracing', 'idi', 'pdl'))"
    )


def downgrade() -> None:
    op.drop_column("enriched_contacts", "confidence")

    op.execute("ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source")
    op.execute(
        "ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_source "
        "CHECK (source IN ('batch_skip_tracing', 'idi'))"
    )
