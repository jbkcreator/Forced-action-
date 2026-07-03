"""add_scraper_alert_management

Adds error_type column to scraper_run_stats and creates the scraper_alert_log
table used for alert deduplication (cooldown window).

Revision ID: o5p6q7r8s9t0
Revises: n4o5p6q7r8s9
Create Date: 2026-04-21
"""

from alembic import op
import sqlalchemy as sa

revision = 'o5p6q7r8s9t0'
down_revision = 'n4o5p6q7r8s9'
branch_labels = None
depends_on = None


def upgrade():
    # Add error_type to scraper_run_stats
    op.add_column(
        'scraper_run_stats',
        sa.Column('error_type', sa.String(20), nullable=True),
    )

    # Backfill: derive error_type from existing run_success rows
    op.execute("""
        UPDATE scraper_run_stats
        SET error_type = CASE
            WHEN run_success = TRUE  THEN 'none'
            ELSE 'scraper_error'
        END
        WHERE error_type IS NULL
    """)

    # Create scraper_alert_log table
    op.create_table(
        'scraper_alert_log',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('source_type', sa.String(50), nullable=False),
        sa.Column('county_id', sa.String(50), nullable=False, server_default='hillsborough'),
        sa.Column('alert_type', sa.String(50), nullable=False),
        sa.Column('alerted_at', sa.DateTime(timezone=True), nullable=False),
    )

    op.create_index(
        'idx_scraper_alert_log_lookup',
        'scraper_alert_log',
        ['source_type', 'county_id', 'alert_type', 'alerted_at'],
    )


def downgrade():
    op.drop_index('idx_scraper_alert_log_lookup', table_name='scraper_alert_log')
    op.drop_table('scraper_alert_log')
    op.drop_column('scraper_run_stats', 'error_type')
