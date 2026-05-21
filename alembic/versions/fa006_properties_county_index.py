"""fa006_properties_county_index

Adds index on properties(county_id) to support:
  - _collect_changed_property_ids() county filter JOIN
  - get_sample_property_ids() county-stratified sampling
  - Any feed/query that filters the 522k-row properties table by county

Production note:
  For zero-lock deployment on large tables, create manually first:

    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_properties_county_id
        ON properties (county_id);

  then run alembic upgrade head — the IF NOT EXISTS guard makes it a no-op.

Revision ID: fa006_properties_county_index
Revises:     fa005_scoring_indexes
Create Date: 2026-05-21
"""

import sqlalchemy as sa
from alembic import op

revision = "fa006_properties_county_index"
down_revision = "fa005_scoring_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_properties_county_id "
        "ON properties (county_id)"
    ))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS idx_properties_county_id"))
