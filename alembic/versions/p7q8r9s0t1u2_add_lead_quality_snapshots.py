"""add_lead_quality_snapshots

Revision ID: p7q8r9s0t1u2
Revises: 2d2dd1371479
Create Date: 2026-04-27 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'p7q8r9s0t1u2'
down_revision: Union[str, None] = 's3t4u5v6w7x8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'lead_quality_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('property_id', sa.Integer(), nullable=False),
        sa.Column('subscriber_id', sa.Integer(), nullable=False),
        sa.Column('county_id', sa.String(length=50), nullable=False),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('snapshot_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('score_at_send', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('tier_at_send', sa.String(length=20), nullable=True),
        sa.Column('signals_at_send', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('score_at_snapshot', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('tier_at_snapshot', sa.String(length=20), nullable=True),
        sa.Column('still_gold_plus', sa.Boolean(), nullable=False),
        sa.Column('has_deed_transfer', sa.Boolean(), nullable=False),
        sa.Column('has_resolved_signals', sa.Boolean(), nullable=False),
        sa.Column('outcome', sa.String(length=20), nullable=False),
        sa.ForeignKeyConstraint(['property_id'], ['properties.id']),
        sa.ForeignKeyConstraint(['subscriber_id'], ['subscribers.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('property_id', 'subscriber_id', 'sent_at',
                            name='uq_lead_quality_snapshot'),
    )
    op.create_index('idx_lqs_snapshot_at', 'lead_quality_snapshots', ['snapshot_at'])
    op.create_index('idx_lqs_outcome', 'lead_quality_snapshots', ['outcome'])
    op.create_index(op.f('ix_lead_quality_snapshots_property_id'),
                    'lead_quality_snapshots', ['property_id'])
    op.create_index(op.f('ix_lead_quality_snapshots_subscriber_id'),
                    'lead_quality_snapshots', ['subscriber_id'])


def downgrade() -> None:
    op.drop_index(op.f('ix_lead_quality_snapshots_subscriber_id'),
                  table_name='lead_quality_snapshots')
    op.drop_index(op.f('ix_lead_quality_snapshots_property_id'),
                  table_name='lead_quality_snapshots')
    op.drop_index('idx_lqs_outcome', table_name='lead_quality_snapshots')
    op.drop_index('idx_lqs_snapshot_at', table_name='lead_quality_snapshots')
    op.drop_table('lead_quality_snapshots')
