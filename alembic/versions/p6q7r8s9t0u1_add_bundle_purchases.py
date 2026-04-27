"""add_bundle_purchases

Revision ID: p6q7r8s9t0u1
Revises: 2d2dd1371479
Create Date: 2026-04-22

Restores missing migration file. Table was created directly; file needed for chain integrity.
Table already exists in DB — upgrade() is a no-op wrapped in try/except.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = 'p6q7r8s9t0u1'
down_revision = '2d2dd1371479'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    result = conn.execute(sa.text(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='bundle_purchases')"
    ))
    if result.scalar():
        return  # table already exists
    op.create_table(
        'bundle_purchases',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('subscriber_id', sa.Integer(), nullable=False),
        sa.Column('bundle_type', sa.String(length=30), nullable=False),
        sa.Column('stripe_payment_intent_id', sa.String(length=100), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('zip_code', sa.String(length=10), nullable=True),
        sa.Column('vertical', sa.String(length=50), nullable=True),
        sa.Column('county_id', sa.String(length=50), nullable=False),
        sa.Column('credits_awarded', sa.Integer(), nullable=False),
        sa.Column('lead_ids', postgresql.ARRAY(sa.Integer()), nullable=True),
        sa.Column('purchased_at', sa.DateTime(), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['subscriber_id'], ['subscribers.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('stripe_payment_intent_id'),
    )


def downgrade() -> None:
    pass
