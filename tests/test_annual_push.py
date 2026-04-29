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


def _make_db(deal_count=0, total_debits=0, big_deal=None):
    db = MagicMock()
    results = [deal_count, total_debits, big_deal]
    call_count = [0]

    def side_effect(stmt):
        idx = call_count[0]
        call_count[0] += 1
        result = MagicMock()
        result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
        return result

    db.execute.side_effect = side_effect
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
