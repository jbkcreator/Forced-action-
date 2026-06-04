"""fa_tracerfy — expand check_enriched_source to include 'tracerfy'

Changes:
  1. check_enriched_source constraint expanded to include 'tracerfy' as valid source

Revision ID: fa_tracerfy_source
Revises:     fa031_skip_trace_waterfall
Create Date: 2026-06-03
"""

import sqlalchemy as sa
from alembic import op

revision = "fa_tracerfy_source"
down_revision = "fa068"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source")
    op.execute(
        "ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_source "
        "CHECK (source IN ('batch_skip_tracing', 'idi', 'pdl', 'tracerfy'))"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source")
    op.execute(
        "ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_source "
        "CHECK (source IN ('batch_skip_tracing', 'idi', 'pdl'))"
    )
