"""add_scraper_run_stats_table

Revision ID: a2b3c4d5e6f7
Revises: ebf68e5bc028
Create Date: 2026-03-11 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a2b3c4d5e6f7'
down_revision: Union[str, Sequence[str], None] = 'ebf68e5bc028'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create scraper_run_stats table for daily per-source telemetry."""
    op.create_table(
        'scraper_run_stats',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column('run_date', sa.Date(), nullable=False),
        sa.Column('source_type', sa.String(length=50), nullable=False),
        sa.Column('county_id', sa.String(length=50), nullable=False, server_default='hillsborough'),
        sa.Column('total_scraped', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('matched', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('unmatched', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('skipped', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('scored', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('run_success', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('duration_seconds', sa.Numeric(10, 2), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.UniqueConstraint('run_date', 'source_type', 'county_id', name='uq_scraper_run_stats'),
        sa.CheckConstraint(
            "source_type IN ("
            "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl',"
            "'judgments', 'deeds', 'evictions', 'probate', 'bankruptcy',"
            "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
            "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents'"
            ")",
            name='check_run_stats_source_type',
        ),
    )
    op.create_index('idx_run_stats_run_date', 'scraper_run_stats', ['run_date'], unique=False)
    op.create_index('idx_run_stats_source_type', 'scraper_run_stats', ['source_type'], unique=False)
    op.create_index('idx_run_stats_county_id', 'scraper_run_stats', ['county_id'], unique=False)
    op.create_index('idx_run_stats_date_source', 'scraper_run_stats', ['run_date', 'source_type'], unique=False)


def downgrade() -> None:
    """Drop scraper_run_stats table."""
    op.drop_index('idx_run_stats_date_source', table_name='scraper_run_stats')
    op.drop_index('idx_run_stats_county_id', table_name='scraper_run_stats')
    op.drop_index('idx_run_stats_source_type', table_name='scraper_run_stats')
    op.drop_index('idx_run_stats_run_date', table_name='scraper_run_stats')
    op.drop_table('scraper_run_stats')
