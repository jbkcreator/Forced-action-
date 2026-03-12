"""add_unmatched_records_table

Revision ID: c2d3e4f5a6b7
Revises: b3c4d5e6f7a8
Create Date: 2026-03-12

"""
from alembic import op
import sqlalchemy as sa

revision = 'c2d3e4f5a6b7'
down_revision = 'b3c4d5e6f7a8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'unmatched_records',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('source_type', sa.String(50), nullable=False),
        sa.Column('county_id', sa.String(50), nullable=False, server_default='hillsborough'),
        sa.Column('raw_data', sa.JSON(), nullable=False),
        sa.Column('instrument_number', sa.String(100), nullable=True),
        sa.Column('grantor', sa.Text(), nullable=True),
        sa.Column('address_string', sa.Text(), nullable=True),
        sa.Column('match_status', sa.String(20), nullable=False, server_default='unmatched'),
        sa.Column('match_attempted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('matched_property_id', sa.Integer(), nullable=True),
        sa.Column('date_added', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['matched_property_id'], ['properties.id'], name='fk_unmatched_property'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_unmatched_records_source_type', 'unmatched_records', ['source_type'])
    op.create_index('ix_unmatched_records_county_id', 'unmatched_records', ['county_id'])
    op.create_index('ix_unmatched_records_match_status', 'unmatched_records', ['match_status'])
    op.create_index('ix_unmatched_records_instrument_number', 'unmatched_records', ['instrument_number'])
    op.create_index('ix_unmatched_source_status', 'unmatched_records', ['source_type', 'match_status'])


def downgrade() -> None:
    op.drop_index('ix_unmatched_source_status', table_name='unmatched_records')
    op.drop_index('ix_unmatched_records_instrument_number', table_name='unmatched_records')
    op.drop_index('ix_unmatched_records_match_status', table_name='unmatched_records')
    op.drop_index('ix_unmatched_records_county_id', table_name='unmatched_records')
    op.drop_index('ix_unmatched_records_source_type', table_name='unmatched_records')
    op.drop_table('unmatched_records')
