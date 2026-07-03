"""add description to building permits

Revision ID: fa077_add_description_to_building_permits
Revises: fa076_widen_unmatched_match_method
Create Date: 2026-06-10

Adds description TEXT column to building_permits.
Pinellas County does not use a roofing-specific permit_type — roofing jobs
are identified via the Description field (e.g. "Reroof Metal", "Shingle
and/or Flat", "Tile"). Capturing and indexing this column allows the
roofing_permit_engine classifier to match Pinellas roofing permits.
"""
from alembic import op
import sqlalchemy as sa

revision = 'fa077_add_description_to_building_permits'
down_revision = 'fa076_widen_unmatched_match_method'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'building_permits',
        sa.Column('description', sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('building_permits', 'description')
