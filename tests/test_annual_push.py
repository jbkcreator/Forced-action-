"""
Annual Push task tests — Item 6.

Unit tests: mock DB; no Stripe or email calls.

Run:
    pytest tests/test_annual_push.py -v
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.tasks.annual_push import _check_triggers, _push_annual_offer, switch_to_annual


# ============================================================================
# Helpers
# ============================================================================


def _make_sub(
    tier="starter",
    status="active",
    founding=False,
    account_age_days=15,
    email="user@example.com",
    sub_id=1,
):
    sub = MagicMock()
    sub.id = sub_id
    sub.tier = tier
    sub.status = status
    sub.founding_member = founding
    sub.email = email
    sub.event_feed_uuid = "test-uuid"
    sub.stripe_subscription_id = "sub_test"
    sub.name = "Test User"
    now = datetime.now(timezone.utc)
    sub.created_at = now - timedelta(days=account_age_days)
    return sub


def _make_db(
    deal_count=0,
    total_debits=0,
    big_deal=None,
    recent_offer=None,
    auto_switch_last_row=None,  # (template_id, sent_at_datetime) | None
):
    """Build a mock DB whose .execute() supports both .first() (auto-switch
    lookup) and .scalar_one_or_none() (suppression + per-trigger checks).

    Each execute() returns a fresh result where:
      - .first() returns the configured auto_switch_last_row (one row max)
      - .scalar_one_or_none() consumes the next value from the scalar queue
        in order: recent_offer → deal_count → total_debits → big_deal

    Auto-switch queries use .first() and DON'T consume from the scalar queue,
    so callers at age<60 (no auto_switch call) and age>=60 (one auto_switch
    call) both work with the same fixture.
    """
    db = MagicMock()
    scalar_queue = [recent_offer, deal_count, total_debits, big_deal]
    scalar_idx = [0]

    def execute_side_effect(stmt):
        result = MagicMock()
        result.first.return_value = auto_switch_last_row
        idx = scalar_idx[0]
        if idx < len(scalar_queue):
            result.scalar_one_or_none.return_value = scalar_queue[idx]
            scalar_idx[0] += 1
        else:
            result.scalar_one_or_none.return_value = None
        return result

    db.execute.side_effect = execute_side_effect
    return db


# ============================================================================
# _check_triggers()
# ============================================================================


class TestCheckTriggersUnit:
    def test_already_annual_returns_empty(self):
        sub = _make_sub(tier="annual_lock")
        db = MagicMock()
        assert _check_triggers(sub, db) == []

    def test_day_7_founding_member(self):
        sub = _make_sub(founding=True, account_age_days=7)
        db = _make_db(deal_count=0, total_debits=0, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert "charter_day_7" in triggers

    def test_day_7_non_founding_no_charter_trigger(self):
        sub = _make_sub(founding=False, account_age_days=7)
        db = _make_db(deal_count=0, total_debits=0, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert "charter_day_7" not in triggers

    def test_day_10_triggers(self):
        sub = _make_sub(account_age_days=10)
        db = _make_db(deal_count=0, total_debits=0, big_deal=None)
        assert "day_10_14" in _check_triggers(sub, db)

    def test_day_14_triggers(self):
        sub = _make_sub(account_age_days=14)
        db = _make_db(deal_count=0, total_debits=0, big_deal=None)
        assert "day_10_14" in _check_triggers(sub, db)

    def test_day_9_no_day_10_14(self):
        sub = _make_sub(account_age_days=9)
        db = _make_db(deal_count=0, total_debits=0, big_deal=None)
        assert "day_10_14" not in _check_triggers(sub, db)

    def test_two_deals_triggers(self):
        sub = _make_sub(account_age_days=30)
        db = _make_db(deal_count=2, total_debits=0, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert "two_deals" in triggers

    def test_one_deal_no_trigger(self):
        sub = _make_sub(account_age_days=30)
        db = _make_db(deal_count=1, total_debits=0, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert "two_deals" not in triggers

    def test_spend_250_triggers(self):
        sub = _make_sub(account_age_days=30)
        # debit total * 2.5 >= 250 → total_debits >= 100
        db = _make_db(deal_count=0, total_debits=100, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert "spend_250" in triggers

    def test_big_deal_triggers(self):
        sub = _make_sub(account_age_days=30)
        db = _make_db(deal_count=0, total_debits=0, big_deal=999)  # any non-None = found
        triggers = _check_triggers(sub, db)
        assert "deal_win_10k" in triggers

    def test_day_60_auto_switch(self):
        sub = _make_sub(account_age_days=60)
        db = _make_db(deal_count=0, total_debits=0, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert "auto_switch_day_60" in triggers

    def test_day_61_still_fires_auto_switch(self):
        """Fix for the exact-day == 60 bug. Day 61, 90, 180 all eligible."""
        for age in (61, 65, 90, 180):
            sub = _make_sub(account_age_days=age, sub_id=age)
            db = _make_db()
            triggers = _check_triggers(sub, db)
            assert "auto_switch_day_60" in triggers, f"missing at age={age}"

    def test_day_59_does_not_fire_auto_switch(self):
        sub = _make_sub(account_age_days=59)
        db = _make_db()
        triggers = _check_triggers(sub, db)
        assert "auto_switch_day_60" not in triggers

    def test_auto_switch_r1_fires_after_3_days(self):
        """After Day-60 base offer was sent 3 days ago, r1 reminder fires."""
        sub = _make_sub(account_age_days=63)
        last = datetime.now(timezone.utc) - timedelta(days=3)
        db = _make_db(auto_switch_last_row=("annual_offer_auto_switch_day_60", last))
        triggers = _check_triggers(sub, db)
        assert "auto_switch_day_60_r1" in triggers

    def test_auto_switch_r1_not_yet_due(self):
        """Base offer sent 1 day ago; gap is 3 days → no reminder yet."""
        sub = _make_sub(account_age_days=61)
        last = datetime.now(timezone.utc) - timedelta(days=1)
        db = _make_db(auto_switch_last_row=("annual_offer_auto_switch_day_60", last))
        triggers = _check_triggers(sub, db)
        assert not any(t.startswith("auto_switch_day_60") for t in triggers)

    def test_auto_switch_r2_fires_7_days_after_r1(self):
        sub = _make_sub(account_age_days=70)
        last = datetime.now(timezone.utc) - timedelta(days=7)
        db = _make_db(auto_switch_last_row=("annual_offer_auto_switch_day_60_r1", last))
        triggers = _check_triggers(sub, db)
        assert "auto_switch_day_60_r2" in triggers

    def test_auto_switch_complete_after_r2(self):
        """Once r2 has been sent, sequence is permanently complete."""
        sub = _make_sub(account_age_days=120)
        last = datetime.now(timezone.utc) - timedelta(days=40)
        db = _make_db(auto_switch_last_row=("annual_offer_auto_switch_day_60_r2", last))
        triggers = _check_triggers(sub, db)
        assert not any(t.startswith("auto_switch_day_60") for t in triggers)

    def test_auto_switch_priority_over_other_triggers(self):
        """If a Day-60+ user also matches deal_win_10k, auto_switch is first
        in the returned list (so the cron's `triggers[0]` picks it)."""
        sub = _make_sub(founding=True, account_age_days=60)
        db = _make_db(deal_count=2, total_debits=100, big_deal=999)
        triggers = _check_triggers(sub, db)
        assert triggers[0] == "auto_switch_day_60"

    def test_no_triggers_on_day_5(self):
        sub = _make_sub(account_age_days=5)
        db = _make_db(deal_count=0, total_debits=0, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert triggers == []

    def test_multiple_triggers_can_fire(self):
        sub = _make_sub(founding=True, account_age_days=7)
        db = _make_db(deal_count=2, total_debits=100, big_deal=None)
        triggers = _check_triggers(sub, db)
        assert len(triggers) >= 2

    # Stage 5 — 30-day duplicate-offer suppression
    def test_recent_offer_suppresses_all_triggers(self):
        sub = _make_sub(founding=True, account_age_days=7)
        # recent_offer non-None means a MessageOutcome was found in last 30d
        db = _make_db(deal_count=2, total_debits=100, big_deal=999, recent_offer=42)
        triggers = _check_triggers(sub, db)
        assert triggers == []


# ============================================================================
# _push_annual_offer()
# ============================================================================


class TestPushAnnualOfferUnit:
    def test_returns_false_when_no_email(self):
        sub = _make_sub(email=None)
        db = MagicMock()
        result = _push_annual_offer(sub, "day_10_14", db)
        assert result is False

    def test_returns_true_on_email_success(self):
        sub = _make_sub(email="user@example.com")
        db = MagicMock()
        with patch("src.services.email.send_email") as mock_email:
            mock_email.return_value = None
            result = _push_annual_offer(sub, "day_10_14", db)
        assert result is True

    def test_returns_false_on_email_failure(self):
        sub = _make_sub(email="user@example.com")
        db = MagicMock()
        with patch("src.services.email.send_email", side_effect=Exception("SMTP error")):
            result = _push_annual_offer(sub, "day_10_14", db)
        assert result is False

    def test_email_subject_mentions_year(self):
        sub = _make_sub(email="user@example.com")
        db = MagicMock()
        captured = {}

        def capture_email(to, subject, body_text):
            captured["subject"] = subject
            captured["body"] = body_text

        with patch("src.services.email.send_email", side_effect=capture_email):
            _push_annual_offer(sub, "charter_day_7", db)

        assert "year" in captured["subject"].lower() or "annual" in captured["subject"].lower()


# ============================================================================
# switch_to_annual()
# ============================================================================


class TestSwitchToAnnualUnit:
    def test_returns_false_when_subscriber_not_found(self):
        db = MagicMock()
        db.get.return_value = None
        assert switch_to_annual(999, db) is False

    def test_returns_false_when_no_subscription(self):
        sub = _make_sub()
        sub.stripe_subscription_id = None
        db = MagicMock()
        db.get.return_value = sub
        assert switch_to_annual(1, db) is False

    def test_returns_false_when_no_price_configured(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        with patch("src.tasks.annual_push.settings") as mock_settings:
            mock_settings.active_stripe_price.return_value = None
            assert switch_to_annual(1, db) is False

    def test_switches_tier_to_annual_lock_on_success(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        with patch("src.tasks.annual_push.settings") as mock_settings, \
             patch("src.services.stripe_service.switch_subscription_plan") as mock_switch:
            mock_settings.active_stripe_price.return_value = "price_annual_test"
            mock_switch.return_value = {}
            result = switch_to_annual(1, db)
        assert result is True
        assert sub.tier == "annual_lock"

    def test_returns_false_on_stripe_error(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        with patch("src.tasks.annual_push.settings") as mock_settings, \
             patch("src.services.stripe_service.switch_subscription_plan") as mock_switch:
            mock_settings.active_stripe_price.return_value = "price_annual_test"
            mock_switch.side_effect = Exception("Stripe error")
            result = switch_to_annual(1, db)
        assert result is False

    def test_stripe_failure_does_not_set_tier(self):
        """Critical: a Stripe error must NOT leave the subscriber on
        annual_lock tier — that would lie about their billing state."""
        sub = _make_sub(tier="starter")
        db = MagicMock()
        db.get.return_value = sub
        with patch("src.tasks.annual_push.settings") as mock_settings, \
             patch("src.services.stripe_service.switch_subscription_plan") as mock_switch:
            mock_settings.active_stripe_price.return_value = "price_annual_test"
            mock_switch.side_effect = Exception("network")
            switch_to_annual(1, db)
        assert sub.tier == "starter", "tier must not flip on Stripe failure"


# ============================================================================
# SMS YEARLY handler (acceptance path)
# ============================================================================


class TestSmsYearlyAcceptance:
    def test_yearly_invokes_switch_to_annual(self):
        from src.services.sms_commands import _handle_yearly
        sub = _make_sub(tier="starter")
        db = MagicMock()
        with patch("src.services.stripe_service.can_switch_subscription", return_value=(True, None)), \
             patch("src.tasks.annual_push.switch_to_annual", return_value=True) as mock_switch:
            reply = _handle_yearly(sub, db)
            mock_switch.assert_called_once_with(sub.id, db)
        assert "Locked in" in reply or "annual plan" in reply.lower()

    def test_yearly_when_already_annual(self):
        from src.services.sms_commands import _handle_yearly
        sub = _make_sub(tier="annual_lock")
        db = MagicMock()
        reply = _handle_yearly(sub, db)
        assert "already" in reply.lower()

    def test_yearly_blocked_billing_status(self):
        from src.services.sms_commands import _handle_yearly
        sub = _make_sub(tier="starter")
        db = MagicMock()
        with patch("src.services.stripe_service.can_switch_subscription", return_value=(False, "grace")):
            reply = _handle_yearly(sub, db)
        assert "blocked" in reply.lower()
        assert "grace" in reply.lower()

    def test_yearly_stripe_failure_returns_manual_route(self):
        from src.services.sms_commands import _handle_yearly
        sub = _make_sub(tier="starter")
        db = MagicMock()
        with patch("src.services.stripe_service.can_switch_subscription", return_value=(True, None)), \
             patch("src.tasks.annual_push.switch_to_annual", return_value=False):
            reply = _handle_yearly(sub, db)
        assert "dashboard" in reply.lower() or "try again" in reply.lower()
