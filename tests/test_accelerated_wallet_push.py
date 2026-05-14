"""Unit tests for the Accelerated Wallet Push detector + activator (fa016).

Detector is `wallet_engine.accelerated_push_eligible`. The tests stub out the
Subscriber / WalletBalance / WalletTransaction / PremiumPurchase queries via
mock_db.execute() — same pattern as test_wallet_engine.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.core.models import WalletBalance


def _set_feature_flag(enabled: bool):
    """Return a context-manager-friendly patch for the runtime flag."""
    from config import settings as _s
    return patch.object(_s.settings, "accelerated_wallet_push_enabled", enabled)


def _mk_sub(*, has_saved_card=True, opt_out=False, pm_id="pm_test"):
    sub = MagicMock()
    sub.id = 1
    sub.has_saved_card = has_saved_card
    sub.stripe_payment_method_id = pm_id
    sub.wallet_opt_out = opt_out
    sub.missed_lead_count = 2
    return sub


class TestAcceleratedPushEligibleDetector:
    def test_disabled_feature_returns_none(self, mock_db):
        from src.services.wallet_engine import accelerated_push_eligible
        with _set_feature_flag(False):
            assert accelerated_push_eligible(1, mock_db) is None

    def test_no_subscriber_returns_none(self, mock_db):
        from src.services.wallet_engine import accelerated_push_eligible
        mock_db.get.return_value = None
        with _set_feature_flag(True):
            assert accelerated_push_eligible(1, mock_db) is None

    def test_wallet_opt_out_returns_none(self, mock_db):
        from src.services.wallet_engine import accelerated_push_eligible
        mock_db.get.return_value = _mk_sub(opt_out=True)
        with _set_feature_flag(True):
            assert accelerated_push_eligible(1, mock_db) is None

    def test_no_saved_card_returns_none(self, mock_db):
        from src.services.wallet_engine import accelerated_push_eligible
        mock_db.get.return_value = _mk_sub(has_saved_card=False, pm_id=None)
        with _set_feature_flag(True):
            assert accelerated_push_eligible(1, mock_db) is None

    def test_existing_wallet_returns_none(self, mock_db):
        from src.services.wallet_engine import accelerated_push_eligible
        mock_db.get.return_value = _mk_sub()
        existing = WalletBalance(subscriber_id=1, wallet_tier="starter_wallet", credits_remaining=5)
        mock_db.execute.return_value.scalar_one_or_none.return_value = existing
        with _set_feature_flag(True):
            assert accelerated_push_eligible(1, mock_db) is None

    def test_eligible_returns_offer_when_debit_exists(self, mock_db):
        """Saved card + wallet absent + at least one debit → eligible."""
        from src.services.wallet_engine import accelerated_push_eligible
        mock_db.get.return_value = _mk_sub()

        # First scalar_one_or_none returns None (no WalletBalance), then
        # scalar() returns 1 for debit count.
        def execute_side_effect(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = None
            mock_result.scalar.return_value = 1   # debit exists
            return mock_result
        mock_db.execute.side_effect = execute_side_effect

        with _set_feature_flag(True), \
             patch("src.core.redis_client.redis_available", return_value=False):
            result = accelerated_push_eligible(1, mock_db)
        assert result is not None
        assert result["tier"] == "starter_wallet"
        assert result["credits_in_offer"] == 20
        assert result["price_cents"] == 4900
        assert result["reason"] == "saved_card_paid_intent"

    def test_redis_dedupe_skips_same_day(self, mock_db):
        """If the dedupe key is already set, the detector returns None even when
        all other conditions hold."""
        from src.services.wallet_engine import accelerated_push_eligible
        mock_db.get.return_value = _mk_sub()
        def execute_side_effect(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar_one_or_none.return_value = None
            mock_result.scalar.return_value = 1
            return mock_result
        mock_db.execute.side_effect = execute_side_effect

        with _set_feature_flag(True), \
             patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.rget", return_value="1"):
            assert accelerated_push_eligible(1, mock_db) is None


class TestActivateViaSavedCard:
    def test_raises_when_no_subscriber(self, mock_db):
        from src.services.wallet_engine import activate_via_saved_card
        mock_db.get.return_value = None
        with pytest.raises(ValueError) as exc:
            activate_via_saved_card(1, "starter_wallet", mock_db, offer_id=99)
        assert "no_subscriber" in str(exc.value)

    def test_raises_when_no_saved_card(self, mock_db):
        from src.services.wallet_engine import activate_via_saved_card
        sub = MagicMock()
        sub.has_saved_card = False
        sub.stripe_payment_method_id = None
        mock_db.get.return_value = sub
        with pytest.raises(ValueError) as exc:
            activate_via_saved_card(1, "starter_wallet", mock_db, offer_id=99)
        assert "no_saved_card" in str(exc.value)

    def test_raises_when_no_stripe_customer(self, mock_db):
        from src.services.wallet_engine import activate_via_saved_card
        sub = MagicMock()
        sub.has_saved_card = True
        sub.stripe_payment_method_id = "pm_test"
        sub.stripe_customer_id = None
        mock_db.get.return_value = sub
        with pytest.raises(ValueError) as exc:
            activate_via_saved_card(1, "starter_wallet", mock_db, offer_id=99)
        assert "no_stripe_customer" in str(exc.value)

    def test_delegates_to_stripe_service(self, mock_db):
        from src.services.wallet_engine import activate_via_saved_card
        sub = MagicMock()
        sub.has_saved_card = True
        sub.stripe_payment_method_id = "pm_test"
        sub.stripe_customer_id = "cus_test"
        mock_db.get.return_value = sub
        with patch(
            "src.services.stripe_service.create_subscription_off_saved_pm",
            return_value={"subscription_id": "sub_123", "status": "incomplete"},
        ) as p:
            result = activate_via_saved_card(1, "starter_wallet", mock_db, offer_id=99)
        p.assert_called_once()
        assert result["subscription_id"] == "sub_123"


class TestEnrollmentTriggersExtended:
    def test_eight_dollar_day_trigger(self, mock_db):
        """≥ 4 credits debited today → trigger fires for non-saved-card user."""
        from src.services.wallet_engine import check_enrollment_triggers
        sub = MagicMock()
        sub.has_saved_card = False
        sub.wallet_opt_out = False
        mock_db.get.return_value = sub

        # Sequence:
        #  1. select WalletBalance         → None
        #  2. count unlocks_24h            → 0
        #  3. count total_unlocks          → 0
        #  4. sum today_credits            → -4 (debits sum negative)
        # then return without checking repeat_zip.
        results = [None, 0, 0, -4]
        def execute_side_effect(*args, **kwargs):
            mock_result = MagicMock()
            v = results.pop(0)
            mock_result.scalar_one_or_none.return_value = v if v is None else None
            mock_result.scalar.return_value = v
            mock_result.first.return_value = None
            return mock_result
        mock_db.execute.side_effect = execute_side_effect
        assert check_enrollment_triggers(1, mock_db) == "starter_wallet"

    def test_repeat_zip_48h_trigger(self, mock_db):
        """A ZIP touched ≥ 2 times in last 48h → trigger fires."""
        from src.services.wallet_engine import check_enrollment_triggers
        sub = MagicMock()
        sub.has_saved_card = False
        sub.wallet_opt_out = False
        mock_db.get.return_value = sub
        results = [None, 0, 0, 0, ("33647", 2)]  # last is .first() return
        idx = {"i": 0}
        def execute_side_effect(*args, **kwargs):
            mock_result = MagicMock()
            v = results[idx["i"]] if idx["i"] < len(results) else None
            idx["i"] += 1
            mock_result.scalar_one_or_none.return_value = v if v is None else None
            mock_result.scalar.return_value = v if isinstance(v, int) else 0
            mock_result.first.return_value = v if isinstance(v, tuple) else None
            return mock_result
        mock_db.execute.side_effect = execute_side_effect
        assert check_enrollment_triggers(1, mock_db) == "starter_wallet"
