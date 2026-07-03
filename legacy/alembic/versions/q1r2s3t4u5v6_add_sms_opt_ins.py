"""add_sms_opt_ins

Revision ID: q1r2s3t4u5v6
Revises: p6q7r8s9t0u1
Create Date: 2026-04-22

TCPA double opt-in records — explicit consent tracking per phone number.
Required by Item 40 (TCPA compliance).
"""
from alembic import op
import sqlalchemy as sa

revision = 'q1r2s3t4u5v6'
down_revision = 'p6q7r8s9t0u1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'sms_opt_ins',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('phone', sa.String(length=20), nullable=False),
        sa.Column('subscriber_id', sa.Integer(), nullable=True),
        sa.Column('keyword_used', sa.String(length=20), nullable=True),
        sa.Column('source', sa.String(length=30), nullable=False),
        sa.Column('opt_in_message', sa.Text(), nullable=True),
        sa.Column('opted_in_at', sa.DateTime(), nullable=False),
        sa.Column('ip_address', sa.String(length=50), nullable=True),
        sa.CheckConstraint(
            "source IN ('double_opt_in', 'manual', 'import', 'widget')",
            name='check_opt_in_source',
        ),
        sa.ForeignKeyConstraint(['subscriber_id'], ['subscribers.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('phone'),
    )
    op.create_index('idx_sms_opt_in_phone', 'sms_opt_ins', ['phone'])
    op.create_index('idx_sms_opt_in_subscriber', 'sms_opt_ins', ['subscriber_id'])


def downgrade() -> None:
    op.drop_index('idx_sms_opt_in_subscriber', table_name='sms_opt_ins')
    op.drop_index('idx_sms_opt_in_phone', table_name='sms_opt_ins')
    op.drop_table('sms_opt_ins')
