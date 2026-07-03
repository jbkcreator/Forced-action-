"""fa011_phase_b

Phase B schema additions:
  - new table: partner_subscriptions

Revision ID: fa011_phase_b
Revises:     fa010_phase_a_missing_flows
Create Date: 2026-05-06
"""
import sqlalchemy as sa
from alembic import op

revision = 'fa011_phase_b'
down_revision = 'fa010_phase_a_missing_flows'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'partner_subscriptions',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('subscriber_id', sa.Integer(), nullable=False),
        sa.Column('max_zips', sa.Integer(), nullable=False, server_default='5'),
        sa.Column('activated_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('deactivated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['subscriber_id'], ['subscribers.id'], name='fk_partner_sub'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('subscriber_id', name='uq_partner_sub'),
    )
    op.create_index('idx_partner_sub', 'partner_subscriptions', ['subscriber_id'])


def downgrade() -> None:
    op.drop_index('idx_partner_sub', table_name='partner_subscriptions')
    op.drop_table('partner_subscriptions')
