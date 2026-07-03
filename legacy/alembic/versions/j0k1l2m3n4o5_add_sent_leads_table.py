"""add_sent_leads_table

Revision ID: j0k1l2m3n4o5
Revises: i9j0k1l2m3n4
Create Date: 2026-04-02 00:00:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = 'j0k1l2m3n4o5'
down_revision: Union[str, None] = '1dfb55b61ac6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'sent_leads',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column('subscriber_id', sa.Integer(), nullable=False),
        sa.Column('property_id', sa.Integer(), nullable=False),
        sa.Column('sent_at', sa.DateTime(), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['subscriber_id'], ['subscribers.id']),
        sa.ForeignKeyConstraint(['property_id'], ['properties.id']),
        sa.UniqueConstraint('subscriber_id', 'property_id', name='uq_sent_lead'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'idx_sent_lead_subscriber_sent_at',
        'sent_leads',
        ['subscriber_id', 'sent_at'],
    )


def downgrade() -> None:
    op.drop_index('idx_sent_lead_subscriber_sent_at', table_name='sent_leads')
    op.drop_table('sent_leads')
