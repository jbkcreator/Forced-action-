"""
Proactive Save task tests — Item 7.

Unit tests: mock DB; no email or Stripe calls.

Run:
    pytest tests/test_proactive_save.py -v
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.tasks.proactive_save import (
    _identify_risk,
    _send_save_offer,
    downgrade_to_data_only,
)


# ============================================================================
# Helpers
# ============================================================================


def _make_sub(
    tier="starter",
    status="active",
    email="user@example.com",
    sub_id=1,
    event_feed_uuid="test-uuid",
    grace_expires_at=None,
    created_days_ago=30,
    stripe_subscription_id="sub_test",
):
    sub = MagicMock()
    sub.id = sub_id
    sub.tier = tier
    sub.status = status
    sub.email = email
    sub.event_feed_uuid = event_feed_uuid
    sub.grace_expires_at = grace_expires_at
    sub.stripe_subscription_id = stripe_subscription_id
    sub.name = "Test User"
    now = datetime.now(timezone.utc)
    sub.created_at = now - timedelta(days=created_days_ago)
    return sub


def _make_db(last_txn_days_ago=None):
    db = MagicMock()
    if last_txn_days_ago is None:
        db.execute.return_value.scalar_one_or_none.return_value = None
    else:
        txn_time = datetime.now(timezone.utc) - timedelta(days=last_txn_days_ago)
        db.execute.return_value.scalar_one_or_none.return_value = txn_time
    return db


# ============================================================================
# _identify_risk()
# ============================================================================


class TestIdentifyRiskUnit:
    def test_data_only_tier_skipped(self):
        sub = _make_sub(tier="data_only")
        db = _make_db()
        assert _identify_risk(sub, db) is None

    def test_free_tier_skipped(self):
        sub = _make_sub(tier="free")
        db = _make_db()
        assert _identify_risk(sub, db) is None

    def test_inactivity_5_days_triggers(self):
        sub = _make_sub(tier="starter")
        db = _make_db(last_txn_days_ago=5)
        assert _identify_risk(sub, db) == "inactivity"

    def test_inactivity_7_days_triggers(self):
        sub = _make_sub(tier="starter")
        db = _make_db(last_txn_days_ago=7)
        assert _identify_risk(sub, db) == "inactivity"

    def test_inactivity_4_days_no_trigger(self):
        sub = _make_sub(tier="starter")
        db = _make_db(last_txn_days_ago=4)
        assert _identify_risk(sub, db) is None

    def test_inactivity_8_days_no_trigger(self):
        sub = _make_sub(tier="starter")
        db = _make_db(last_txn_days_ago=8)
        assert _identify_risk(sub, db) is None

    def test_payment_failure_day5_triggers(self):
        now = datetime.now(timezone.utc)
        # grace expires in 2 days, so entered 5+ days ago
        with patch("src.tasks.proactive_save.settings") as mock_settings:
            mock_settings.grace_period_hours = 168  # 7 days grace
            expires = now + timedelta(hours=48)  # expires in 2 days = entered 5 days ago
            sub = _make_sub(tier="starter", status="grace", grace_expires_at=expires)
            db = _make_db(last_txn_days_ago=10)
            result = _identify_risk(sub, db)
        assert result == "payment_failure_day5"

    def test_grace_day_2_no_trigger(self):
        now = datetime.now(timezone.utc)
        with patch("src.tasks.proactive_save.settings") as mock_settings:
            mock_settings.grace_period_hours = 168
            expires = now + timedelta(hours=120)  # 5 days left = 2 days in grace
            sub = _make_sub(tier="starter", status="grace", grace_expires_at=expires)
            db = _make_db(last_txn_days_ago=10)
            result = _identify_risk(sub, db)
        assert result != "payment_failure_day5"

    def test_no_wallet_txns_uses_account_creation_date(self):
        # No wallet transactions → uses sub.created_at
        sub = _make_sub(tier="pro", created_days_ago=6)
        db = _make_db(last_txn_days_ago=None)  # None → no txn → use created_at
        result = _identify_risk(sub, db)
        assert result == "inactivity"


# ============================================================================
# _send_save_offer()
# ============================================================================


class TestSendSaveOfferUnit:
    def test_returns_false_when_no_email(self):
        sub = _make_sub(email=None)
        assert _send_save_offer(sub, "inactivity") is False

    def test_returns_true_on_email_success(self):
        sub = _make_sub(email="user@example.com")
        with patch("src.services.email.send_email") as mock_email:
            mock_email.return_value = None
            with patch("src.tasks.proactive_save.settings") as mock_settings:
                mock_settings.app_base_url = "https://app.example.com"
                result = _send_save_offer(sub, "inactivity")
        assert result is True

    def test_returns_false_on_email_exception(self):
        sub = _make_sub(email="user@example.com")
        with patch("src.services.email.send_email", side_effect=Exception("fail")):
            with patch("src.tasks.proactive_save.settings") as mock_settings:
                mock_settings.app_base_url = "https://app.example.com"
                result = _send_save_offer(sub, "inactivity")
        assert result is False

    def test_inactivity_email_mentions_active(self):
        sub = _make_sub(email="user@example.com")
        captured = {}

        with patch("src.services.email.send_email", side_effect=lambda **kw: captured.update(kw)), \
             patch("src.tasks.proactive_save.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.com"
            _send_save_offer(sub, "inactivity")

        assert "active" in captured.get("body_text", "").lower() or "busy" in captured.get("body_text", "").lower()

    def test_payment_failure_email_subject_mentions_data(self):
        sub = _make_sub(email="user@example.com")
        captured = {}

        with patch("src.services.email.send_email", side_effect=lambda **kw: captured.update(kw)), \
             patch("src.tasks.proactive_save.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.com"
            _send_save_offer(sub, "payment_failure_day5")

        subject = captured.get("subject", "")
        assert "data" in subject.lower()


# ============================================================================
# downgrade_to_data_only()
# ============================================================================


class TestDowngradeToDataOnlyUnit:
    def test_returns_false_when_subscriber_not_found(self):
        db = MagicMock()
        db.get.return_value = None
        assert downgrade_to_data_only(999, db) is False

    def test_returns_false_when_no_subscription(self):
        sub = _make_sub()
        sub.stripe_subscription_id = None
        db = MagicMock()
        db.get.return_value = sub
        assert downgrade_to_data_only(1, db) is False

    def test_returns_false_when_price_not_configured(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        with patch("src.tasks.proactive_save.settings") as mock_settings:
            mock_settings.active_stripe_price.return_value = None
            assert downgrade_to_data_only(1, db) is False

    def test_sets_tier_to_data_only_on_success(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        with patch("src.tasks.proactive_save.settings") as mock_settings, \
             patch("src.services.stripe_service.switch_subscription_plan") as mock_switch:
            mock_settings.active_stripe_price.return_value = "price_data_only_test"
            mock_switch.return_value = {}
            result = downgrade_to_data_only(1, db)
        assert result is True
        assert sub.tier == "data_only"

    def test_returns_false_on_stripe_error(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        with patch("src.tasks.proactive_save.settings") as mock_settings, \
             patch("src.services.stripe_service.switch_subscription_plan") as mock_switch:
            mock_settings.active_stripe_price.return_value = "price_data_only_test"
            mock_switch.side_effect = Exception("Stripe error")
            assert downgrade_to_data_only(1, db) is False
