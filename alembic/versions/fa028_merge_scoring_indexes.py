"""fa028_merge_scoring_indexes

Merge point: joins the scoring-index branch (fa005 + fa006) back into the
main migration chain.

Revision ID: fa028_merge_scoring_indexes
Revises:     92ee22387084, fa006_properties_county_index
Create Date: 2026-05-21
"""

from alembic import op

revision = "fa028_merge_scoring_indexes"
down_revision = ("92ee22387084", "fa006_properties_county_index")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
