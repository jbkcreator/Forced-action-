"""fa093 — A2 Lead Confidence gating: distress_scores.is_guess_lead + lead_confidence.

Adds the two A2 columns and a partial index supporting the sellable-lead read
path (WHERE NOT is_guess_lead). See tasks/A2-implementation-plan.md.

NOTE: this repo's alembic tree is multi-head and the CLI is unusable; the DDL is
actually applied via scripts/apply_fa093_a2_lead_confidence.py. This file is kept
for the migration record and mirrors that DDL exactly.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "fa093_a2_lead_confidence"
down_revision: Union[str, Sequence[str]] = "fa085_m10_lead_delivery"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE distress_scores ADD COLUMN IF NOT EXISTS lead_confidence NUMERIC(4, 3)")
    op.execute(
        "ALTER TABLE distress_scores "
        "ADD COLUMN IF NOT EXISTS is_guess_lead BOOLEAN NOT NULL DEFAULT FALSE"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_score_sellable "
        "ON distress_scores (final_cds_score) WHERE is_guess_lead = false"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_score_sellable")
    op.execute("ALTER TABLE distress_scores DROP COLUMN IF EXISTS is_guess_lead")
    op.execute("ALTER TABLE distress_scores DROP COLUMN IF EXISTS lead_confidence")
