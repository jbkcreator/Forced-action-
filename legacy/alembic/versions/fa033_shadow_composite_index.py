"""fa033_shadow_composite_index

Adds the composite `(property_id, score_date DESC)` index to the
`distress_scores_shadow` table that the Stage E shadow-rescore phase uses.

Mirrors fa005_scoring_indexes which added the same index to the live
`distress_scores` table. The shadow-table migration (fa032) shipped with
only the single-column indexes from the SQLAlchemy `__table_args__` block
on `DistressScore` — but the engine's hot-path queries (`save_score_to_database`,
`_persist_score_batch`) all hit `WHERE property_id = :pid AND score_date >= :start
AND score_date < :end` and `WHERE property_id = :pid ORDER BY score_date DESC
LIMIT 1`. Both query shapes need the composite (property_id, score_date DESC)
to avoid a sequential scan per write.

Without this index, a shadow rescore over hundreds of thousands of properties
falls off a cliff once the shadow table grows past a few thousand rows.

Production note:
  For zero-lock production deployment on large tables, create the index
  manually first with CONCURRENTLY, then run `alembic upgrade head` —
  the IF NOT EXISTS guard makes the migration a no-op:

    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shadow_score_property_date
        ON distress_scores_shadow (property_id, score_date DESC);

Revision ID: fa033_shadow_composite_index
Revises:     fa032_distress_scores_shadow
Create Date: 2026-05-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "fa033_shadow_composite_index"
down_revision: Union[str, Sequence[str], None] = "fa032_distress_scores_shadow"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_shadow_score_property_date "
        "ON distress_scores_shadow (property_id, score_date DESC)"
    ))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS idx_shadow_score_property_date"))
