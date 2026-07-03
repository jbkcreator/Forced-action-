"""fa030 — merge two parallel heads after feat/extend-enrichment-filing-types

Joins:
  - fa029_add_normalized_address       (Pinellas address-normalization chain)
  - fa028_foreclosures_defendant       (Foreclosure.defendant column for skip-trace)

Both descend from 92ee22387084 via different parents; alembic upgrade head
refuses with "multiple heads" until they're merged. This is a no-op merge
revision — no DDL, just a single head for the schema graph.

Revision ID: fa030_merge_foreclosures_defendant
Revises:     fa029_add_normalized_address, fa028_foreclosures_defendant
Create Date: 2026-05-21
"""

from alembic import op  # noqa: F401  (kept for symmetry with sibling migrations)

revision = "fa030_merge_foreclosures_defendant"
down_revision = ("fa029_add_normalized_address", "fa028_foreclosures_defendant")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
