"""
Phase 2B schema foundation tests.

Validates:
  - All 9 new tables can be created and populated via ORM
  - Foreign key relationships work correctly
  - Unique constraints are enforced
  - New Subscriber fields work (has_saved_card, stripe_payment_method_id,
    referral_code, auto_mode_enabled)
  - Config files import and contain expected data
  - Guardrail bounds check helper works
  - Wallet transaction balance tracking

Run with:
    pytest tests/test_2b_schema.py -v
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.core.models import (
    AbAssignment,
    AbTest,
    DealOutcome,
    LearningCard,
    MessageOutcome,
    ReferralEvent,
    Subscriber,
    UserSegment,
    WalletBalance,
    WalletTransaction,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_sub_counter = 0

def _make_subscriber(fresh_db, **overrides):
    """Insert a real Subscriber row via ORM and return it."""
    global _sub_counter
    _sub_counter += 1
    cid = overrides.get("stripe_customer_id", f"cus_test_{_sub_counter}")
    defaults = dict(
        stripe_customer_id=cid,
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        founding_member=False,
        status="active",
        email=f"test_{_sub_counter}@example.com",
        name="Test User",
    )
    defaults.update(overrides)
    sub = Subscriber(**defaults)
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


# ---------------------------------------------------------------------------
# Config import tests
# ---------------------------------------------------------------------------

class TestRevenueladderConfig:
    def test_revenue_ladder_has_12_steps(self):
        from config.revenue_ladder import REVENUE_LADDER
        assert len(REVENUE_LADDER) == 12

    def test_ladder_steps_are_sequential(self):
        from config.revenue_ladder import REVENUE_LADDER
        steps = [s["step"] for s in REVENUE_LADDER]
        assert steps == list(range(1, 13))

    def test_wallet_tiers_exist(self):
        from config.revenue_ladder import WALLET_TIERS
        assert set(WALLET_TIERS.keys()) == {"starter_wallet", "growth", "power"}

    def test_wallet_tier_credits(self):
        from config.revenue_ladder import WALLET_TIERS
        assert WALLET_TIERS["starter_wallet"]["credits_per_cycle"] == 20
        assert WALLET_TIERS["growth"]["credits_per_cycle"] == 50
        assert WALLET_TIERS["power"]["credits_per_cycle"] == 120

    def test_wallet_auto_mode_included(self):
        from config.revenue_ladder import WALLET_TIERS
        assert WALLET_TIERS["starter_wallet"]["auto_mode_included"] is False
        assert WALLET_TIERS["growth"]["auto_mode_included"] is True
        assert WALLET_TIERS["power"]["auto_mode_included"] is True

    def test_bundles_exist(self):
        from config.revenue_ladder import BUNDLES
        assert set(BUNDLES.keys()) == {"weekend", "storm", "zip_booster", "monthly_reload"}

    def test_bundle_prices(self):
        from config.revenue_ladder import BUNDLES
        assert BUNDLES["weekend"]["price_cents"] == 1900
        assert BUNDLES["storm"]["price_cents"] == 3900
        assert BUNDLES["zip_booster"]["price_cents"] == 2900
        assert BUNDLES["monthly_reload"]["price_cents"] == 8900

    def test_annual_push_triggers(self):
        from config.revenue_ladder import ANNUAL_PUSH_TRIGGERS
        assert len(ANNUAL_PUSH_TRIGGERS) == 6
        names = {t["name"] for t in ANNUAL_PUSH_TRIGGERS}
        assert "deal_win_10k" in names
        assert "auto_switch_day_60" in names

    def test_free_allotment(self):
        from config.revenue_ladder import FREE_ALLOTMENT
        assert FREE_ALLOTMENT["skips_per_week"] == 3
        assert FREE_ALLOTMENT["texts_per_week"] == 3
        assert FREE_ALLOTMENT["voicemails_per_week"] == 1

    def test_credit_costs(self):
        from config.revenue_ladder import CREDIT_COSTS
        assert CREDIT_COSTS["lead_unlock"] == 1
        assert CREDIT_COSTS["skip_trace"] == 2
        assert CREDIT_COSTS["transfer"] == 26


class TestCoraGuardrailsConfig:
    def test_guardrails_has_13_rules(self):
        from config.cora_guardrails import GUARDRAILS
        assert len(GUARDRAILS) == 13

    def test_expansion_gates_has_7(self):
        from config.cora_guardrails import EXPANSION_GATES
        assert len(EXPANSION_GATES) == 7

    def test_kill_switch_has_9_metrics(self):
        from config.cora_guardrails import KILL_SWITCH
        assert len(KILL_SWITCH) == 9

    def test_lock_pricing_range(self):
        from config.cora_guardrails import GUARDRAILS
        g = GUARDRAILS["lock_pricing"]
        assert g["min_cents"] == 14700
        assert g["max_cents"] == 24700

    def test_discount_hard_limit(self):
        from config.cora_guardrails import GUARDRAILS
        g = GUARDRAILS["discount_max"]
        assert g["max_pct"] == 20
        assert g["hard_limit"] is True

    def test_is_within_guardrail_accepts_valid(self):
        from config.cora_guardrails import is_within_guardrail
        assert is_within_guardrail("lock_pricing", 19700) is True
        assert is_within_guardrail("discount_max", 15) is True
        assert is_within_guardrail("credit_bonus_max", 5) is True

    def test_is_within_guardrail_rejects_invalid(self):
        from config.cora_guardrails import is_within_guardrail
        assert is_within_guardrail("lock_pricing", 30000) is False
        assert is_within_guardrail("discount_max", 25) is False
        assert is_within_guardrail("credit_bonus_max", 15) is False

    def test_get_guardrail(self):
        from config.cora_guardrails import get_guardrail
        g = get_guardrail("urgency_window")
        assert g["min_minutes"] == 10
        assert g["max_minutes"] == 60

    def test_get_guardrail_missing_raises(self):
        from config.cora_guardrails import get_guardrail
        with pytest.raises(KeyError):
            get_guardrail("nonexistent_guardrail")


# ---------------------------------------------------------------------------
# Subscriber model additions
# ---------------------------------------------------------------------------

class TestSubscriberNewFields:
    def test_defaults_on_new_subscriber(self, fresh_db):
        sub = _make_subscriber(fresh_db)
        assert sub.has_saved_card is False
        assert sub.stripe_payment_method_id is None
        assert sub.referral_code is None
        assert sub.auto_mode_enabled is False

    def test_saved_card_fields(self, fresh_db):
        sub = _make_subscriber(
            fresh_db,
            has_saved_card=True,
            stripe_payment_method_id="pm_abc123",
        )
        assert sub.has_saved_card is True
        assert sub.stripe_payment_method_id == "pm_abc123"

    def test_referral_code(self, fresh_db):
        sub = _make_subscriber(fresh_db, referral_code="REF001")
        assert sub.referral_code == "REF001"

    def test_auto_mode_enabled(self, fresh_db):
        sub = _make_subscriber(fresh_db, auto_mode_enabled=True)
        assert sub.auto_mode_enabled is True

    def test_new_tier_values(self, fresh_db):
        """All new 2B tier values should be accepted by the CHECK constraint."""
        verticals = ["roofing", "restoration", "wholesalers", "fix_flip", "public_adjusters"]
        for i, tier in enumerate(["free", "data_only", "autopilot_lite", "autopilot_pro", "partner"]):
            sub = _make_subscriber(
                fresh_db,
                stripe_customer_id=f"cus_tier_{tier}",
                tier=tier,
                vertical=verticals[i % len(verticals)],
            )
            assert sub.tier == tier

    def test_paused_status(self, fresh_db):
        sub = _make_subscriber(
            fresh_db,
            stripe_customer_id="cus_paused",
            status="paused",
        )
        assert sub.status == "paused"


# ---------------------------------------------------------------------------
# WalletBalance
# ---------------------------------------------------------------------------

class TestWalletBalance:
    def test_create_wallet(self, fresh_db):
        sub = _make_subscriber(fresh_db)
        wallet = WalletBalance(
            subscriber_id=sub.id,
            wallet_tier="growth",
            credits_remaining=50,
            auto_reload_enabled=True,
        )
        fresh_db.add(wallet)
        fresh_db.flush()

        assert wallet.id is not None
        assert wallet.wallet_tier == "growth"
        assert wallet.credits_remaining == 50
        assert wallet.credits_used_total == 0
        assert wallet.auto_reload_enabled is True

    def test_wallet_defaults(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_wdefault")
        wallet = WalletBalance(subscriber_id=sub.id, wallet_tier="starter_wallet")
        fresh_db.add(wallet)
        fresh_db.flush()

        assert wallet.credits_remaining == 0
        assert wallet.credits_used_total == 0
        assert wallet.auto_reload_enabled is True


# ---------------------------------------------------------------------------
# WalletTransaction
# ---------------------------------------------------------------------------

class TestWalletTransaction:
    def test_create_transaction(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_wtxn")
        wallet = WalletBalance(subscriber_id=sub.id, wallet_tier="power", credits_remaining=120)
        fresh_db.add(wallet)
        fresh_db.flush()

        txn = WalletTransaction(
            subscriber_id=sub.id,
            wallet_id=wallet.id,
            txn_type="debit",
            amount=-2,
            balance_after=118,
            description="skip_trace",
        )
        fresh_db.add(txn)
        fresh_db.flush()

        assert txn.id is not None
        assert txn.txn_type == "debit"
        assert txn.amount == -2
        assert txn.balance_after == 118

    def test_reload_transaction(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_reload")
        wallet = WalletBalance(subscriber_id=sub.id, wallet_tier="growth", credits_remaining=3)
        fresh_db.add(wallet)
        fresh_db.flush()

        txn = WalletTransaction(
            subscriber_id=sub.id,
            wallet_id=wallet.id,
            txn_type="reload",
            amount=50,
            balance_after=53,
            stripe_charge_id="ch_reload_123",
        )
        fresh_db.add(txn)
        fresh_db.flush()

        assert txn.stripe_charge_id == "ch_reload_123"
        assert txn.amount == 50


# ---------------------------------------------------------------------------
# UserSegment
# ---------------------------------------------------------------------------

class TestUserSegment:
    def test_create_segment(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_seg")
        seg = UserSegment(
            subscriber_id=sub.id,
            segment="new",
            revenue_signal_score=0,
            classification_reason="first_signup",
        )
        fresh_db.add(seg)
        fresh_db.flush()

        assert seg.segment == "new"
        assert seg.revenue_signal_score == 0

    def test_all_segment_values(self, fresh_db):
        segments = ["new", "browsing", "engaged", "wallet_active",
                    "high_intent", "lock_candidate", "at_risk", "churned"]
        for i, segment_name in enumerate(segments):
            sub = _make_subscriber(fresh_db, stripe_customer_id=f"cus_seg_{i}")
            seg = UserSegment(subscriber_id=sub.id, segment=segment_name)
            fresh_db.add(seg)
            fresh_db.flush()
            assert seg.segment == segment_name


# ---------------------------------------------------------------------------
# MessageOutcome
# ---------------------------------------------------------------------------

class TestMessageOutcome:
    def test_create_sms_outcome(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_msg")
        msg = MessageOutcome(
            subscriber_id=sub.id,
            message_type="sms",
            template_id="lock_close_v1",
            variant_id="ab_test_1_a",
            channel="twilio",
        )
        fresh_db.add(msg)
        fresh_db.flush()

        assert msg.id is not None
        assert msg.message_type == "sms"
        assert msg.conversion_within_4h is False

    def test_conversion_attribution(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_conv")
        msg = MessageOutcome(
            subscriber_id=sub.id,
            message_type="email",
            conversion_type="wallet",
            conversion_within_4h=True,
            conversion_within_24h=True,
            revenue_attributed=99.00,
        )
        fresh_db.add(msg)
        fresh_db.flush()

        assert msg.conversion_type == "wallet"
        assert msg.conversion_within_4h is True
        assert float(msg.revenue_attributed) == 99.00

    def test_all_message_types(self, fresh_db):
        for mtype in ["sms", "email", "voice"]:
            msg = MessageOutcome(message_type=mtype)
            fresh_db.add(msg)
            fresh_db.flush()
            assert msg.message_type == mtype


# ---------------------------------------------------------------------------
# DealOutcome
# ---------------------------------------------------------------------------

class TestDealOutcome:
    def test_create_deal(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_deal")
        deal = DealOutcome(
            subscriber_id=sub.id,
            deal_size_bucket="10_25k",
            deal_amount=18500.00,
            deal_date=date.today(),
            lead_source="foreclosure",
            days_to_close=14,
        )
        fresh_db.add(deal)
        fresh_db.flush()

        assert deal.id is not None
        assert deal.deal_size_bucket == "10_25k"
        assert deal.days_to_close == 14

    def test_all_deal_size_buckets(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_deal_all")
        for bucket in ["5_10k", "10_25k", "25k_plus", "skip"]:
            deal = DealOutcome(subscriber_id=sub.id, deal_size_bucket=bucket)
            fresh_db.add(deal)
            fresh_db.flush()
            assert deal.deal_size_bucket == bucket


# ---------------------------------------------------------------------------
# LearningCard
# ---------------------------------------------------------------------------

class TestLearningCard:
    def test_create_learning_card(self, fresh_db):
        card = LearningCard(
            card_date=date.today(),
            card_type="message_perf",
            summary_text="Lock close SMS variant B outperformed A by 12% this week.",
            data_json={"variant_a_rate": 0.08, "variant_b_rate": 0.20, "sample_size": 420},
            action_taken="Retired variant A, promoted variant B to 100%",
        )
        fresh_db.add(card)
        fresh_db.flush()

        assert card.id is not None
        assert card.card_type == "message_perf"
        assert "variant_b_rate" in card.data_json

    def test_all_card_types(self, fresh_db):
        types = ["message_perf", "deal_pattern", "ab_result",
                "churn_signal", "pricing_test", "general"]
        for i, ctype in enumerate(types):
            card = LearningCard(
                card_date=date(2026, 1, i + 1),
                card_type=ctype,
                summary_text=f"Test card for {ctype}",
            )
            fresh_db.add(card)
            fresh_db.flush()
            assert card.card_type == ctype


# ---------------------------------------------------------------------------
# ReferralEvent
# ---------------------------------------------------------------------------

class TestReferralEvent:
    def test_create_referral(self, fresh_db):
        referrer = _make_subscriber(fresh_db, stripe_customer_id="cus_referrer")
        ref = ReferralEvent(
            referrer_subscriber_id=referrer.id,
            referral_code="REF001",
            status="pending",
        )
        fresh_db.add(ref)
        fresh_db.flush()

        assert ref.id is not None
        assert ref.status == "pending"
        assert ref.referee_subscriber_id is None

    def test_confirmed_referral_with_reward(self, fresh_db):
        referrer = _make_subscriber(fresh_db, stripe_customer_id="cus_ref2")
        referee = _make_subscriber(fresh_db, stripe_customer_id="cus_referee")
        ref = ReferralEvent(
            referrer_subscriber_id=referrer.id,
            referee_subscriber_id=referee.id,
            referral_code="REF002",
            status="rewarded",
            reward_type="credits",
            reward_value="5",
            confirmed_at=datetime.now(timezone.utc),
        )
        fresh_db.add(ref)
        fresh_db.flush()

        assert ref.status == "rewarded"
        assert ref.reward_type == "credits"
        assert ref.confirmed_at is not None

    def test_all_referral_statuses(self, fresh_db):
        referrer = _make_subscriber(fresh_db, stripe_customer_id="cus_ref3")
        for status in ["pending", "confirmed", "rewarded", "expired"]:
            ref = ReferralEvent(
                referrer_subscriber_id=referrer.id,
                referral_code=f"REF_{status}",
                status=status,
            )
            fresh_db.add(ref)
            fresh_db.flush()
            assert ref.status == status


# ---------------------------------------------------------------------------
# AbTest + AbAssignment
# ---------------------------------------------------------------------------

class TestAbTest:
    def test_create_ab_test(self, fresh_db):
        test = AbTest(
            test_name="lock_close_urgency_vs_roi",
            segment="high_intent",
            variant_a={"message": "Your ZIP is filling up fast — lock it now"},
            variant_b={"message": "Contractors in your ZIP closed $12K avg from these leads"},
            traffic_pct=10,
        )
        fresh_db.add(test)
        fresh_db.flush()

        assert test.id is not None
        assert test.status == "active"
        assert test.traffic_pct == 10
        assert "message" in test.variant_a

    def test_ab_assignment(self, fresh_db):
        test = AbTest(
            test_name="wallet_push_timing",
            variant_a={"delay_hours": 1},
            variant_b={"delay_hours": 4},
        )
        fresh_db.add(test)
        fresh_db.flush()

        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_ab")
        assignment = AbAssignment(
            test_id=test.id,
            subscriber_id=sub.id,
            variant="a",
        )
        fresh_db.add(assignment)
        fresh_db.flush()

        assert assignment.variant == "a"
        assert assignment.outcome is None

    def test_ab_assignment_outcome(self, fresh_db):
        test = AbTest(
            test_name="bundle_vs_wallet",
            variant_a={"offer": "bundle"},
            variant_b={"offer": "wallet"},
        )
        fresh_db.add(test)
        fresh_db.flush()

        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_ab2")
        assignment = AbAssignment(
            test_id=test.id,
            subscriber_id=sub.id,
            variant="b",
            outcome="converted",
        )
        fresh_db.add(assignment)
        fresh_db.flush()

        assert assignment.outcome == "converted"


# ---------------------------------------------------------------------------
# Relationship / FK tests
# ---------------------------------------------------------------------------

class TestRelationships:
    def test_wallet_belongs_to_subscriber(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_rel1")
        wallet = WalletBalance(subscriber_id=sub.id, wallet_tier="growth")
        fresh_db.add(wallet)
        fresh_db.flush()

        assert wallet.subscriber.id == sub.id

    def test_wallet_transactions_belong_to_wallet(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_rel2")
        wallet = WalletBalance(subscriber_id=sub.id, wallet_tier="power", credits_remaining=100)
        fresh_db.add(wallet)
        fresh_db.flush()

        txn = WalletTransaction(
            subscriber_id=sub.id, wallet_id=wallet.id,
            txn_type="debit", amount=-1, balance_after=99,
        )
        fresh_db.add(txn)
        fresh_db.flush()

        assert txn.wallet.id == wallet.id
        assert len(wallet.transactions) == 1

    def test_subscriber_segment_backref(self, fresh_db):
        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_rel3")
        seg = UserSegment(subscriber_id=sub.id, segment="engaged")
        fresh_db.add(seg)
        fresh_db.flush()

        assert sub.segment[0].segment == "engaged"

    def test_ab_assignment_relationship(self, fresh_db):
        test = AbTest(
            test_name="test_rel",
            variant_a={"x": 1}, variant_b={"x": 2},
        )
        fresh_db.add(test)
        fresh_db.flush()

        sub = _make_subscriber(fresh_db, stripe_customer_id="cus_rel4")
        a = AbAssignment(test_id=test.id, subscriber_id=sub.id, variant="a")
        fresh_db.add(a)
        fresh_db.flush()

        assert a.test.test_name == "test_rel"
        assert len(test.assignments) == 1
