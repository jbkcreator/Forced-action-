"""2b_schema_foundation

Phase 2B schema foundation:
- Add 4 new columns to subscribers (has_saved_card, stripe_payment_method_id,
  referral_code, auto_mode_enabled)
- Widen tier CHECK constraint to include free/data_only/autopilot_lite/autopilot_pro/partner
- Widen status CHECK constraint to include paused
- Create 9 new tables: wallet_balances, wallet_transactions, user_segments,
  message_outcomes, deal_outcomes, learning_cards, referral_events, ab_tests,
  ab_assignments

Revision ID: m3n4o5p6q7r8
Revises: l2m3n4o5p6q7
Create Date: 2026-04-20 12:00:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from alembic import op


revision: str = 'm3n4o5p6q7r8'
down_revision: Union[str, None] = 'l2m3n4o5p6q7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── 1. Drop old CHECK constraints on subscribers ──────────────────────
    op.drop_constraint("check_subscriber_tier", "subscribers", type_="check")
    op.drop_constraint("check_subscriber_status", "subscribers", type_="check")

    # ── 2. Add new columns to subscribers ─────────────────────────────────
    op.add_column("subscribers", sa.Column("has_saved_card", sa.Boolean(), nullable=False, server_default=sa.text("false")))
    op.add_column("subscribers", sa.Column("stripe_payment_method_id", sa.String(100), nullable=True))
    op.add_column("subscribers", sa.Column("referral_code", sa.String(20), nullable=True))
    op.add_column("subscribers", sa.Column("auto_mode_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")))

    op.create_unique_constraint("uq_subscriber_referral_code", "subscribers", ["referral_code"])
    op.create_index("idx_subscriber_referral_code", "subscribers", ["referral_code"])

    # ── 3. Add widened CHECK constraints ──────────────────────────────────
    op.create_check_constraint(
        "check_subscriber_tier",
        "subscribers",
        "tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner')",
    )
    op.create_check_constraint(
        "check_subscriber_status",
        "subscribers",
        "status IN ('active', 'grace', 'churned', 'cancelled', 'paused')",
    )

    # ── 4. Create wallet_balances ─────────────────────────────────────────
    op.create_table(
        "wallet_balances",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False, unique=True),
        sa.Column("wallet_tier", sa.String(20), nullable=False),
        sa.Column("credits_remaining", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("credits_used_total", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("auto_reload_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("last_reload_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("wallet_tier IN ('starter_wallet', 'growth', 'power')", name="check_wallet_tier"),
    )
    op.create_index("idx_wallet_balance_subscriber", "wallet_balances", ["subscriber_id"])

    # ── 5. Create wallet_transactions ─────────────────────────────────────
    op.create_table(
        "wallet_transactions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("wallet_id", sa.Integer(), sa.ForeignKey("wallet_balances.id"), nullable=False),
        sa.Column("txn_type", sa.String(20), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("balance_after", sa.Integer(), nullable=False),
        sa.Column("description", sa.String(255), nullable=True),
        sa.Column("stripe_charge_id", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("txn_type IN ('credit', 'debit', 'reload', 'bonus', 'refund')", name="check_txn_type"),
    )
    op.create_index("idx_wallet_txn_subscriber", "wallet_transactions", ["subscriber_id"])
    op.create_index("idx_wallet_txn_wallet", "wallet_transactions", ["wallet_id"])
    op.create_index("idx_wallet_txn_stripe_charge", "wallet_transactions", ["stripe_charge_id"])
    op.create_index("idx_wallet_txn_sub_created", "wallet_transactions", ["subscriber_id", "created_at"])

    # ── 6. Create user_segments ───────────────────────────────────────────
    op.create_table(
        "user_segments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False, unique=True),
        sa.Column("segment", sa.String(30), nullable=False),
        sa.Column("revenue_signal_score", sa.Integer(), nullable=True, server_default=sa.text("0")),
        sa.Column("last_classified_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("classification_reason", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "segment IN ('new', 'browsing', 'engaged', 'wallet_active', 'high_intent', 'lock_candidate', 'at_risk', 'churned')",
            name="check_user_segment",
        ),
    )
    op.create_index("idx_user_segment_subscriber", "user_segments", ["subscriber_id"])

    # ── 7. Create message_outcomes ────────────────────────────────────────
    op.create_table(
        "message_outcomes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True),
        sa.Column("message_type", sa.String(20), nullable=False),
        sa.Column("template_id", sa.String(100), nullable=True),
        sa.Column("variant_id", sa.String(100), nullable=True),
        sa.Column("channel", sa.String(50), nullable=True),
        sa.Column("sent_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("delivered_at", sa.DateTime(), nullable=True),
        sa.Column("opened_at", sa.DateTime(), nullable=True),
        sa.Column("clicked_at", sa.DateTime(), nullable=True),
        sa.Column("replied_at", sa.DateTime(), nullable=True),
        sa.Column("conversion_type", sa.String(30), nullable=True),
        sa.Column("conversion_within_4h", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("conversion_within_24h", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("conversion_within_48h", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("revenue_attributed", sa.Numeric(10, 2), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("message_type IN ('sms', 'email', 'voice')", name="check_message_type"),
    )
    op.create_index("idx_msg_outcome_subscriber", "message_outcomes", ["subscriber_id"])
    op.create_index("idx_msg_outcome_variant", "message_outcomes", ["variant_id"])
    op.create_index("idx_msg_outcome_sub_sent", "message_outcomes", ["subscriber_id", "sent_at"])

    # ── 8. Create deal_outcomes ───────────────────────────────────────────
    op.create_table(
        "deal_outcomes",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=True),
        sa.Column("deal_size_bucket", sa.String(20), nullable=True),
        sa.Column("deal_amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("deal_date", sa.Date(), nullable=True),
        sa.Column("lead_source", sa.String(50), nullable=True),
        sa.Column("days_to_close", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("deal_size_bucket IN ('5_10k', '10_25k', '25k_plus', 'skip')", name="check_deal_size_bucket"),
    )
    op.create_index("idx_deal_outcome_subscriber", "deal_outcomes", ["subscriber_id"])
    op.create_index("idx_deal_outcome_property", "deal_outcomes", ["property_id"])
    op.create_index("idx_deal_outcome_sub_date", "deal_outcomes", ["subscriber_id", "deal_date"])

    # ── 9. Create learning_cards ──────────────────────────────────────────
    op.create_table(
        "learning_cards",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("card_date", sa.Date(), nullable=False),
        sa.Column("card_type", sa.String(30), nullable=False),
        sa.Column("summary_text", sa.Text(), nullable=False),
        sa.Column("data_json", postgresql.JSONB(), nullable=True),
        sa.Column("action_taken", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "card_type IN ('message_perf', 'deal_pattern', 'ab_result', 'churn_signal', 'pricing_test', 'general')",
            name="check_card_type",
        ),
        sa.UniqueConstraint("card_date", "card_type", name="uq_learning_card_date_type"),
    )
    op.create_index("idx_learning_card_date", "learning_cards", ["card_date"])

    # ── 10. Create referral_events ────────────────────────────────────────
    op.create_table(
        "referral_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("referrer_subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("referee_subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True),
        sa.Column("referral_code", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("reward_type", sa.String(30), nullable=True),
        sa.Column("reward_value", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("confirmed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint("status IN ('pending', 'confirmed', 'rewarded', 'expired')", name="check_referral_status"),
    )
    op.create_index("idx_referral_referrer", "referral_events", ["referrer_subscriber_id"])
    op.create_index("idx_referral_referee", "referral_events", ["referee_subscriber_id"])
    op.create_index("idx_referral_code", "referral_events", ["referral_code"])
    op.create_index("idx_referral_referrer_status", "referral_events", ["referrer_subscriber_id", "status"])

    # ── 11. Create ab_tests ───────────────────────────────────────────────
    op.create_table(
        "ab_tests",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("test_name", sa.String(100), nullable=False, unique=True),
        sa.Column("segment", sa.String(30), nullable=True),
        sa.Column("variant_a", postgresql.JSONB(), nullable=False),
        sa.Column("variant_b", postgresql.JSONB(), nullable=False),
        sa.Column("traffic_pct", sa.Integer(), nullable=False, server_default=sa.text("10")),
        sa.Column("status", sa.String(20), nullable=False, server_default=sa.text("'active'")),
        sa.Column("started_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("winner", sa.String(10), nullable=True),
        sa.CheckConstraint("status IN ('active', 'completed', 'rolled_back')", name="check_ab_test_status"),
        sa.CheckConstraint("traffic_pct BETWEEN 1 AND 100", name="check_ab_traffic_pct"),
    )

    # ── 12. Create ab_assignments ─────────────────────────────────────────
    op.create_table(
        "ab_assignments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("test_id", sa.Integer(), sa.ForeignKey("ab_tests.id"), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("variant", sa.String(10), nullable=False),
        sa.Column("outcome", sa.String(30), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("test_id", "subscriber_id", name="uq_ab_assignment"),
    )
    op.create_index("idx_ab_assignment_test", "ab_assignments", ["test_id"])
    op.create_index("idx_ab_assignment_subscriber", "ab_assignments", ["subscriber_id"])


def downgrade() -> None:
    # Drop new tables in reverse FK order
    op.drop_table("ab_assignments")
    op.drop_table("ab_tests")
    op.drop_table("referral_events")
    op.drop_table("learning_cards")
    op.drop_table("deal_outcomes")
    op.drop_table("message_outcomes")
    op.drop_table("user_segments")
    op.drop_table("wallet_transactions")
    op.drop_table("wallet_balances")

    # Drop new subscriber columns
    op.drop_index("idx_subscriber_referral_code", table_name="subscribers")
    op.drop_constraint("uq_subscriber_referral_code", "subscribers", type_="unique")
    op.drop_column("subscribers", "auto_mode_enabled")
    op.drop_column("subscribers", "referral_code")
    op.drop_column("subscribers", "stripe_payment_method_id")
    op.drop_column("subscribers", "has_saved_card")

    # Restore original CHECK constraints
    op.drop_constraint("check_subscriber_tier", "subscribers", type_="check")
    op.drop_constraint("check_subscriber_status", "subscribers", type_="check")
    op.create_check_constraint(
        "check_subscriber_tier",
        "subscribers",
        "tier IN ('starter', 'pro', 'dominator')",
    )
    op.create_check_constraint(
        "check_subscriber_status",
        "subscribers",
        "status IN ('active', 'grace', 'churned', 'cancelled')",
    )
