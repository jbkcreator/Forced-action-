"""fa010_phase_a_missing_flows

Phase A schema additions:
  - subscribers: lock_candidate_zip, lock_candidate_at, ap_lite_candidate_at,
                 paused_at, pause_resume_at, escalation_routed_at, escalation_channel
  - wallet_transactions: zip_code column + composite index
  - new table: manual_action_log
  - new table: human_close_escalations

Revision ID: fa010_phase_a_missing_flows
Revises:     fa009_annual_lock_tier
Create Date: 2026-05-06
"""
import sqlalchemy as sa
from alembic import op

revision = 'fa010_phase_a_missing_flows'
down_revision = 'fa009_annual_lock_tier'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── subscribers: new nullable columns ────────────────────────────────────
    op.add_column('subscribers', sa.Column('lock_candidate_zip', sa.String(10), nullable=True))
    op.add_column('subscribers', sa.Column('lock_candidate_at', sa.DateTime(), nullable=True))
    op.add_column('subscribers', sa.Column('ap_lite_candidate_at', sa.DateTime(), nullable=True))
    op.add_column('subscribers', sa.Column('paused_at', sa.DateTime(), nullable=True))
    op.add_column('subscribers', sa.Column('pause_resume_at', sa.DateTime(), nullable=True))
    op.add_column('subscribers', sa.Column('escalation_routed_at', sa.DateTime(), nullable=True))
    op.add_column('subscribers', sa.Column('escalation_channel', sa.String(20), nullable=True))

    op.create_index(
        'idx_sub_lock_candidate',
        'subscribers',
        ['lock_candidate_at'],
        postgresql_where=sa.text('lock_candidate_at IS NOT NULL'),
    )
    op.create_index(
        'idx_sub_paused',
        'subscribers',
        ['paused_at'],
        postgresql_where=sa.text('paused_at IS NOT NULL'),
    )

    # ── wallet_transactions: zip_code + composite index ───────────────────────
    op.add_column('wallet_transactions', sa.Column('zip_code', sa.String(10), nullable=True))
    op.create_index(
        'idx_wallet_txn_sub_zip_created',
        'wallet_transactions',
        ['subscriber_id', 'zip_code', 'created_at'],
    )

    # ── manual_action_log ─────────────────────────────────────────────────────
    op.create_table(
        'manual_action_log',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('subscriber_id', sa.Integer(), sa.ForeignKey('subscribers.id'), nullable=False),
        sa.Column('action_type', sa.String(40), nullable=False),
        sa.Column('week_start', sa.Date(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False,
                  server_default=sa.text('NOW()')),
    )
    op.create_index('idx_mal_sub_week', 'manual_action_log', ['subscriber_id', 'week_start'])
    op.create_index('idx_mal_created', 'manual_action_log', ['created_at'])

    # ── human_close_escalations ───────────────────────────────────────────────
    op.create_table(
        'human_close_escalations',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('subscriber_id', sa.Integer(), sa.ForeignKey('subscribers.id'), nullable=False),
        sa.Column('decision_id', sa.String(40), nullable=False),
        sa.Column('revenue_signal_score', sa.Integer(), nullable=False),
        sa.Column('interactions_count', sa.Integer(), nullable=False),
        sa.Column('target_tier', sa.String(20), nullable=False),
        sa.Column('channel', sa.String(20), nullable=False),
        sa.Column('routed_at', sa.DateTime(), nullable=False,
                  server_default=sa.text('NOW()')),
        sa.Column('closer_assigned', sa.String(80), nullable=True),
        sa.Column('outcome', sa.String(20), nullable=True),
        sa.Column('outcome_at', sa.DateTime(), nullable=True),
        sa.Column('context_json', sa.dialects.postgresql.JSONB(), nullable=True),
        sa.UniqueConstraint('subscriber_id', 'decision_id', name='uq_hce_sub_decision'),
        sa.CheckConstraint(
            "channel IN ('slack', 'ghl', 'sms', 'email')",
            name='check_hce_channel',
        ),
        sa.CheckConstraint(
            "outcome IN ('won', 'lost', 'no_response', 'rescheduled') OR outcome IS NULL",
            name='check_hce_outcome',
        ),
    )
    op.create_index('idx_hce_routed', 'human_close_escalations', ['routed_at'])
    op.create_index('idx_hce_open', 'human_close_escalations', ['outcome', 'routed_at'])


def downgrade() -> None:
    op.drop_table('human_close_escalations')
    op.drop_table('manual_action_log')

    op.drop_index('idx_wallet_txn_sub_zip_created', 'wallet_transactions')
    op.drop_column('wallet_transactions', 'zip_code')

    op.drop_index('idx_sub_paused', 'subscribers')
    op.drop_index('idx_sub_lock_candidate', 'subscribers')
    op.drop_column('subscribers', 'escalation_channel')
    op.drop_column('subscribers', 'escalation_routed_at')
    op.drop_column('subscribers', 'pause_resume_at')
    op.drop_column('subscribers', 'paused_at')
    op.drop_column('subscribers', 'ap_lite_candidate_at')
    op.drop_column('subscribers', 'lock_candidate_at')
    op.drop_column('subscribers', 'lock_candidate_zip')
