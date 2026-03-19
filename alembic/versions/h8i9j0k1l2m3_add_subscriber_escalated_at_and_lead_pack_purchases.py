"""add_subscriber_escalated_at_and_lead_pack_purchases

Revision ID: h8i9j0k1l2m3
Revises: g7h8i9j0k1l2
Create Date: 2026-03-19 00:00:00.000000

Two changes:
  1. Add escalated_at column to subscribers (tracks when 6-month founding
     rate lock expires and the subscription is moved to regular pricing).
  2. Create lead_pack_purchases table for $99 lead pack enforcement
     (5 leads, 72-hour exclusivity per ZIP+vertical).
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ARRAY


revision: str = 'h8i9j0k1l2m3'
down_revision: Union[str, None] = 'g7h8i9j0k1l2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. subscribers.escalated_at
    op.add_column(
        'subscribers',
        sa.Column('escalated_at', sa.DateTime(), nullable=True),
    )

    # 2. lead_pack_purchases table
    op.create_table(
        'lead_pack_purchases',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('subscriber_id', sa.Integer(), nullable=False),
        sa.Column('zip_code', sa.String(10), nullable=False),
        sa.Column('vertical', sa.String(50), nullable=False),
        sa.Column('county_id', sa.String(50), nullable=False, server_default='hillsborough'),
        sa.Column('stripe_payment_intent_id', sa.String(100), nullable=False),
        sa.Column('status', sa.String(20), nullable=False, server_default='pending'),
        sa.Column('purchased_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('delivered_at', sa.DateTime(), nullable=True),
        sa.Column('exclusive_until', sa.DateTime(), nullable=True),
        sa.Column('lead_ids', ARRAY(sa.Integer()), nullable=True),
        sa.ForeignKeyConstraint(['subscriber_id'], ['subscribers.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('stripe_payment_intent_id', name='uq_lead_pack_payment_intent'),
        sa.CheckConstraint(
            "status IN ('pending', 'delivered', 'expired')",
            name='check_lead_pack_status',
        ),
    )
    op.create_index('idx_lead_pack_zip_vertical', 'lead_pack_purchases', ['zip_code', 'vertical'])
    op.create_index('idx_lead_pack_exclusive_until', 'lead_pack_purchases', ['exclusive_until'])
    op.create_index(
        'idx_lead_pack_subscriber', 'lead_pack_purchases', ['subscriber_id']
    )


def downgrade() -> None:
    op.drop_table('lead_pack_purchases')
    op.drop_column('subscribers', 'escalated_at')
