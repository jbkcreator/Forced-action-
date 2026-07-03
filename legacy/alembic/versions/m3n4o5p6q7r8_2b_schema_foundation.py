"""2b_schema_foundation

Phase 2B Day 1 — adds 4 new Subscriber fields, widens tier/status CHECK constraints,
and creates 9 new tables:
  wallet_balances, wallet_transactions, user_segments, message_outcomes,
  deal_outcomes, learning_cards, referral_events, ab_tests, ab_assignments

Revision ID: m3n4o5p6q7r8
Revises:     l2m3n4o5p6q7
Create Date: 2026-04-20
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = 'm3n4o5p6q7r8'
down_revision = 'l2m3n4o5p6q7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. Drop old CHECK constraints on subscribers ──────────────────────────
    op.drop_constraint('check_subscriber_tier',   'subscribers', type_='check')
    op.drop_constraint('check_subscriber_status', 'subscribers', type_='check')

    # ── 2. Add new Subscriber columns ────────────────────────────────────────
    op.add_column('subscribers', sa.Column('has_saved_card',            sa.Boolean(),     nullable=False, server_default='false'))
    op.add_column('subscribers', sa.Column('stripe_payment_method_id',  sa.String(100),   nullable=True))
    op.add_column('subscribers', sa.Column('referral_code',             sa.String(20),    nullable=True))
    op.add_column('subscribers', sa.Column('auto_mode_enabled',         sa.Boolean(),     nullable=False, server_default='false'))

    op.create_index('idx_subscriber_referral_code', 'subscribers', ['referral_code'], unique=True,
                    postgresql_where=sa.text("referral_code IS NOT NULL"))

    # ── 3. Add widened CHECK constraints ─────────────────────────────────────
    op.create_check_constraint(
        'check_subscriber_tier',
        'subscribers',
        "tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner')",
    )
    op.create_check_constraint(
        'check_subscriber_status',
        'subscribers',
        "status IN ('active', 'grace', 'churned', 'cancelled', 'paused')",
    )

    # ── 4. wallet_balances ────────────────────────────────────────────────────
    op.create_table(
        'wallet_balances',
        sa.Column('id',                  sa.Integer(),   primary_key=True, autoincrement=True),
        sa.Column('subscriber_id',       sa.Integer(),   sa.ForeignKey('subscribers.id'), nullable=False),
        sa.Column('wallet_tier',         sa.String(20),  nullable=False),
        sa.Column('credits_remaining',   sa.Integer(),   nullable=False, server_default='0'),
        sa.Column('credits_used_total',  sa.Integer(),   nullable=False, server_default='0'),
        sa.Column('auto_reload_enabled', sa.Boolean(),   nullable=False, server_default='true'),
        sa.Column('last_reload_at',      sa.DateTime(),  nullable=True),
        sa.Column('created_at',          sa.DateTime(),  nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at',          sa.DateTime(),  nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("wallet_tier IN ('starter_wallet', 'growth', 'power')", name='check_wallet_tier'),
        sa.UniqueConstraint('subscriber_id', name='uq_wallet_balance_subscriber'),
    )
    op.create_index('idx_wallet_balance_subscriber', 'wallet_balances', ['subscriber_id'])

    # ── 5. wallet_transactions ────────────────────────────────────────────────
    op.create_table(
        'wallet_transactions',
        sa.Column('id',               sa.Integer(),    primary_key=True, autoincrement=True),
        sa.Column('subscriber_id',    sa.Integer(),    sa.ForeignKey('subscribers.id'),     nullable=False),
        sa.Column('wallet_id',        sa.Integer(),    sa.ForeignKey('wallet_balances.id'), nullable=False),
        sa.Column('txn_type',         sa.String(20),   nullable=False),
        sa.Column('amount',           sa.Integer(),    nullable=False),
        sa.Column('balance_after',    sa.Integer(),    nullable=False),
        sa.Column('description',      sa.String(255),  nullable=True),
        sa.Column('stripe_charge_id', sa.String(100),  nullable=True),
        sa.Column('created_at',       sa.DateTime(),   nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("txn_type IN ('credit', 'debit', 'reload', 'bonus', 'refund')", name='check_wallet_txn_type'),
    )
    op.create_index('idx_wallet_txn_subscriber',    'wallet_transactions', ['subscriber_id'])
    op.create_index('idx_wallet_txn_wallet',        'wallet_transactions', ['wallet_id'])
    op.create_index('idx_wallet_txn_charge',        'wallet_transactions', ['stripe_charge_id'])
    op.create_index('idx_wallet_txn_sub_created',   'wallet_transactions', ['subscriber_id', 'created_at'])

    # ── 6. user_segments ──────────────────────────────────────────────────────
    op.create_table(
        'user_segments',
        sa.Column('id',                   sa.Integer(),    primary_key=True, autoincrement=True),
        sa.Column('subscriber_id',        sa.Integer(),    sa.ForeignKey('subscribers.id'), nullable=False),
        sa.Column('segment',              sa.String(30),   nullable=False),
        sa.Column('revenue_signal_score', sa.Integer(),    nullable=True,  server_default='0'),
        sa.Column('last_classified_at',   sa.DateTime(),   nullable=False, server_default=sa.func.now()),
        sa.Column('classification_reason',sa.String(255),  nullable=True),
        sa.Column('created_at',           sa.DateTime(),   nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at',           sa.DateTime(),   nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "segment IN ('new', 'browsing', 'engaged', 'wallet_active', 'high_intent', 'lock_candidate', 'at_risk', 'churned')",
            name='check_user_segment',
        ),
        sa.UniqueConstraint('subscriber_id', name='uq_user_segment_subscriber'),
    )
    op.create_index('idx_user_segment_subscriber', 'user_segments', ['subscriber_id'])

    # ── 7. message_outcomes ───────────────────────────────────────────────────
    op.create_table(
        'message_outcomes',
        sa.Column('id',                    sa.Integer(),       primary_key=True, autoincrement=True),
        sa.Column('subscriber_id',         sa.Integer(),       sa.ForeignKey('subscribers.id'), nullable=True),
        sa.Column('message_type',          sa.String(20),      nullable=False),
        sa.Column('template_id',           sa.String(100),     nullable=True),
        sa.Column('variant_id',            sa.String(100),     nullable=True),
        sa.Column('channel',               sa.String(50),      nullable=True),
        sa.Column('sent_at',               sa.DateTime(),      nullable=False, server_default=sa.func.now()),
        sa.Column('delivered_at',          sa.DateTime(),      nullable=True),
        sa.Column('opened_at',             sa.DateTime(),      nullable=True),
        sa.Column('clicked_at',            sa.DateTime(),      nullable=True),
        sa.Column('replied_at',            sa.DateTime(),      nullable=True),
        sa.Column('conversion_type',       sa.String(30),      nullable=True),
        sa.Column('conversion_within_4h',  sa.Boolean(),       server_default='false'),
        sa.Column('conversion_within_24h', sa.Boolean(),       server_default='false'),
        sa.Column('conversion_within_48h', sa.Boolean(),       server_default='false'),
        sa.Column('revenue_attributed',    sa.Numeric(10, 2),  nullable=True),
        sa.Column('created_at',            sa.DateTime(),      nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("message_type IN ('sms', 'email', 'voice')", name='check_message_type'),
    )
    op.create_index('idx_message_outcome_subscriber', 'message_outcomes', ['subscriber_id'])
    op.create_index('idx_message_outcome_sub_sent',   'message_outcomes', ['subscriber_id', 'sent_at'])
    op.create_index('idx_message_outcome_variant',    'message_outcomes', ['variant_id'])

    # ── 8. deal_outcomes ──────────────────────────────────────────────────────
    op.create_table(
        'deal_outcomes',
        sa.Column('id',               sa.Integer(),       primary_key=True, autoincrement=True),
        sa.Column('subscriber_id',    sa.Integer(),       sa.ForeignKey('subscribers.id'),  nullable=False),
        sa.Column('property_id',      sa.Integer(),       sa.ForeignKey('properties.id'),   nullable=True),
        sa.Column('deal_size_bucket', sa.String(20),      nullable=True),
        sa.Column('deal_amount',      sa.Numeric(12, 2),  nullable=True),
        sa.Column('deal_date',        sa.Date(),          nullable=True),
        sa.Column('lead_source',      sa.String(50),      nullable=True),
        sa.Column('days_to_close',    sa.Integer(),       nullable=True),
        sa.Column('created_at',       sa.DateTime(),      nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "deal_size_bucket IN ('5_10k', '10_25k', '25k_plus', 'skip')",
            name='check_deal_size_bucket',
        ),
    )
    op.create_index('idx_deal_outcome_subscriber',       'deal_outcomes', ['subscriber_id'])
    op.create_index('idx_deal_outcome_property',         'deal_outcomes', ['property_id'])
    op.create_index('idx_deal_outcome_sub_date',         'deal_outcomes', ['subscriber_id', 'deal_date'])

    # ── 9. learning_cards ─────────────────────────────────────────────────────
    op.create_table(
        'learning_cards',
        sa.Column('id',           sa.Integer(),   primary_key=True, autoincrement=True),
        sa.Column('card_date',    sa.Date(),      nullable=False),
        sa.Column('card_type',    sa.String(30),  nullable=False),
        sa.Column('summary_text', sa.Text(),      nullable=False),
        sa.Column('data_json',    JSONB,          nullable=True),
        sa.Column('action_taken', sa.String(255), nullable=True),
        sa.Column('created_at',   sa.DateTime(),  nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "card_type IN ('message_perf', 'deal_pattern', 'ab_result', 'churn_signal', 'pricing_test', 'general')",
            name='check_learning_card_type',
        ),
        sa.UniqueConstraint('card_date', 'card_type', name='uq_learning_card_date_type'),
    )
    op.create_index('idx_learning_card_date', 'learning_cards', ['card_date'])

    # ── 10. referral_events ───────────────────────────────────────────────────
    op.create_table(
        'referral_events',
        sa.Column('id',                     sa.Integer(),   primary_key=True, autoincrement=True),
        sa.Column('referrer_subscriber_id', sa.Integer(),   sa.ForeignKey('subscribers.id'), nullable=False),
        sa.Column('referee_subscriber_id',  sa.Integer(),   sa.ForeignKey('subscribers.id'), nullable=True),
        sa.Column('referral_code',          sa.String(20),  nullable=False),
        sa.Column('status',                 sa.String(20),  nullable=False, server_default='pending'),
        sa.Column('reward_type',            sa.String(30),  nullable=True),
        sa.Column('reward_value',           sa.String(50),  nullable=True),
        sa.Column('created_at',             sa.DateTime(),  nullable=False, server_default=sa.func.now()),
        sa.Column('confirmed_at',           sa.DateTime(),  nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'confirmed', 'rewarded', 'expired')",
            name='check_referral_status',
        ),
    )
    op.create_index('idx_referral_referrer',        'referral_events', ['referrer_subscriber_id'])
    op.create_index('idx_referral_referee',         'referral_events', ['referee_subscriber_id'])
    op.create_index('idx_referral_code',            'referral_events', ['referral_code'])
    op.create_index('idx_referral_referrer_status', 'referral_events', ['referrer_subscriber_id', 'status'])

    # ── 11. ab_tests ──────────────────────────────────────────────────────────
    op.create_table(
        'ab_tests',
        sa.Column('id',          sa.Integer(),    primary_key=True, autoincrement=True),
        sa.Column('test_name',   sa.String(100),  nullable=False),
        sa.Column('segment',     sa.String(30),   nullable=True),
        sa.Column('variant_a',   JSONB,           nullable=False),
        sa.Column('variant_b',   JSONB,           nullable=False),
        sa.Column('traffic_pct', sa.Integer(),    nullable=False, server_default='10'),
        sa.Column('status',      sa.String(20),   nullable=False, server_default='active'),
        sa.Column('started_at',  sa.DateTime(),   nullable=False, server_default=sa.func.now()),
        sa.Column('ended_at',    sa.DateTime(),   nullable=True),
        sa.Column('winner',      sa.String(10),   nullable=True),
        sa.CheckConstraint("status IN ('active', 'completed', 'rolled_back')", name='check_ab_test_status'),
        sa.CheckConstraint('traffic_pct BETWEEN 1 AND 100',                    name='check_ab_traffic_pct'),
        sa.UniqueConstraint('test_name', name='uq_ab_test_name'),
    )

    # ── 12. ab_assignments ────────────────────────────────────────────────────
    op.create_table(
        'ab_assignments',
        sa.Column('id',            sa.Integer(),   primary_key=True, autoincrement=True),
        sa.Column('test_id',       sa.Integer(),   sa.ForeignKey('ab_tests.id'),      nullable=False),
        sa.Column('subscriber_id', sa.Integer(),   sa.ForeignKey('subscribers.id'),   nullable=False),
        sa.Column('variant',       sa.String(10),  nullable=False),
        sa.Column('outcome',       sa.String(30),  nullable=True),
        sa.Column('created_at',    sa.DateTime(),  nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('test_id', 'subscriber_id', name='uq_ab_assignment'),
    )
    op.create_index('idx_ab_assignment_test',       'ab_assignments', ['test_id'])
    op.create_index('idx_ab_assignment_subscriber', 'ab_assignments', ['subscriber_id'])


def downgrade() -> None:
    op.drop_table('ab_assignments')
    op.drop_table('ab_tests')
    op.drop_table('referral_events')
    op.drop_table('learning_cards')
    op.drop_table('deal_outcomes')
    op.drop_table('message_outcomes')
    op.drop_table('user_segments')
    op.drop_table('wallet_transactions')
    op.drop_table('wallet_balances')

    op.drop_constraint('check_subscriber_tier',   'subscribers', type_='check')
    op.drop_constraint('check_subscriber_status', 'subscribers', type_='check')

    op.drop_index('idx_subscriber_referral_code', table_name='subscribers')
    op.drop_column('subscribers', 'auto_mode_enabled')
    op.drop_column('subscribers', 'referral_code')
    op.drop_column('subscribers', 'stripe_payment_method_id')
    op.drop_column('subscribers', 'has_saved_card')

    op.create_check_constraint(
        'check_subscriber_tier',
        'subscribers',
        "tier IN ('starter', 'pro', 'dominator')",
    )
    op.create_check_constraint(
        'check_subscriber_status',
        'subscribers',
        "status IN ('active', 'grace', 'churned', 'cancelled')",
    )
