"""add_bundle_purchases

Revision ID: p6q7r8s9t0u1
Revises: o5p6q7r8s9t0
Create Date: 2026-04-22
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'p6q7r8s9t0u1'
down_revision = 'o5p6q7r8s9t0'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'bundle_purchases',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('subscriber_id', sa.Integer(), nullable=False),
        sa.Column('bundle_type', sa.String(30), nullable=False),
        sa.Column('stripe_payment_intent_id', sa.String(100), nullable=False),
        sa.Column('status', sa.String(20), nullable=False, server_default='pending'),
        sa.Column('zip_code', sa.String(10), nullable=True),
        sa.Column('vertical', sa.String(50), nullable=True),
        sa.Column('county_id', sa.String(50), nullable=False, server_default='hillsborough'),
        sa.Column('credits_awarded', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('lead_ids', postgresql.ARRAY(sa.Integer()), nullable=True),
        sa.Column('purchased_at', sa.DateTime(), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['subscriber_id'], ['subscribers.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint(
            "bundle_type IN ('weekend', 'storm', 'zip_booster', 'monthly_reload')",
            name='check_bundle_type',
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'active', 'expired', 'cancelled')",
            name='check_bundle_status',
        ),
    )
    op.create_index('idx_bundle_purchase_subscriber', 'bundle_purchases', ['subscriber_id'])
    op.create_index('idx_bundle_purchase_intent', 'bundle_purchases', ['stripe_payment_intent_id'], unique=True)
    op.create_index('idx_bundle_type_status', 'bundle_purchases', ['bundle_type', 'status'])


def downgrade():
    op.drop_index('idx_bundle_type_status', table_name='bundle_purchases')
    op.drop_index('idx_bundle_purchase_intent', table_name='bundle_purchases')
    op.drop_index('idx_bundle_purchase_subscriber', table_name='bundle_purchases')
    op.drop_table('bundle_purchases')
