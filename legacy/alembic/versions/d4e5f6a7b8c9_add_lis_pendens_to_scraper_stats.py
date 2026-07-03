"""add_lis_pendens_to_scraper_stats_source_type

Revision ID: d4e5f6a7b8c9
Revises: c2d3e4f5a6b7
Create Date: 2026-03-16 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, Sequence[str], None] = 'c2d3e4f5a6b7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Expand scraper_run_stats.source_type check constraint to include 'lis_pendens'.

    PostgreSQL does not support ALTER CONSTRAINT, so we drop the old constraint
    and recreate it with the new value added.
    """
    op.drop_constraint('check_run_stats_source_type', 'scraper_run_stats', type_='check')
    op.create_check_constraint(
        'check_run_stats_source_type',
        'scraper_run_stats',
        "source_type IN ("
        "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl',"
        "'judgments', 'deeds', 'evictions', 'probate', 'bankruptcy',"
        "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
        "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
        "'lis_pendens'"
        ")",
    )


def downgrade() -> None:
    """Revert to the original constraint without 'lis_pendens'."""
    op.drop_constraint('check_run_stats_source_type', 'scraper_run_stats', type_='check')
    op.create_check_constraint(
        'check_run_stats_source_type',
        'scraper_run_stats',
        "source_type IN ("
        "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl',"
        "'judgments', 'deeds', 'evictions', 'probate', 'bankruptcy',"
        "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
        "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents'"
        ")",
    )
