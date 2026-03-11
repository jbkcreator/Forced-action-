"""add_platform_daily_stats_table

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-03-11 00:01:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3c4d5e6f7a8'
down_revision: Union[str, Sequence[str], None] = 'a2b3c4d5e6f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create platform_daily_stats table for daily platform-level health metrics."""
    op.create_table(
        'platform_daily_stats',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column('run_date', sa.Date(), nullable=False),
        sa.Column('county_id', sa.String(length=50), nullable=False, server_default='hillsborough'),

        # Signal pipeline
        sa.Column('signals_scraped', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('signals_matched', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('signals_skipped', sa.Integer(), nullable=False, server_default='0'),

        # CDS scoring
        sa.Column('properties_scored', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('properties_with_signals', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('score_runs_total', sa.Integer(), nullable=False, server_default='0'),

        # Lead output
        sa.Column('leads_new', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('leads_updated', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('leads_unchanged', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('leads_qualified', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('leads_upgraded', sa.Integer(), nullable=False, server_default='0'),

        # Tier snapshot
        sa.Column('tier_ultra_platinum', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('tier_platinum', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('tier_gold', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('tier_silver', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('tier_bronze', sa.Integer(), nullable=False, server_default='0'),

        # Audit
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),

        sa.UniqueConstraint('run_date', 'county_id', name='uq_platform_daily_stats'),
    )
    op.create_index('idx_platform_stats_date', 'platform_daily_stats', ['run_date'], unique=False)
    op.create_index('idx_platform_stats_county_id', 'platform_daily_stats', ['county_id'], unique=False)


def downgrade() -> None:
    """Drop platform_daily_stats table."""
    op.drop_index('idx_platform_stats_county_id', table_name='platform_daily_stats')
    op.drop_index('idx_platform_stats_date', table_name='platform_daily_stats')
    op.drop_table('platform_daily_stats')
