"""
Payment Sheet service tests — Item 26.

All unit tests — Stripe API and DB are mocked.

Run:
    pytest tests/test_payment_sheet.py -v
"""
from unittest.mock import MagicMock, patch

import pytest

from src.services.payment_sheet import create_payment_intent, get_publishable_key


# ============================================================================
# Helpers
# ============================================================================


def _mock_sub(sub_id=1, stripe_customer_id="cus_test", pm_id=None):
    sub = MagicMock()
    sub.id = sub_id
    sub.stripe_customer_id = stripe_customer_id
    sub.stripe_payment_method_id = pm_id
    return sub


def _mock_db(sub=None):
    db = MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = sub
    return db


def _mock_pi(pi_id="pi_test123", client_secret="pi_test123_secret"):
    pi = MagicMock()
    pi.id = pi_id
    pi.client_secret = client_secret
    return pi


# ============================================================================
# create_payment_intent()
# ============================================================================


class TestCreatePaymentIntentUnit:
    def test_raises_runtime_error_when_stripe_not_configured(self):
        db = _mock_db(sub=_mock_sub())
        with patch("src.services.payment_sheet.settings") as mock_settings:
            mock_settings.active_stripe_secret_key = None
            with pytest.raises(RuntimeError, match="Stripe not configured"):
                create_payment_intent(1, 4900, "test", False, db)

    def test_raises_value_error_when_subscriber_not_found(self):
        db = _mock_db(sub=None)
        with patch("src.services.payment_sheet.settings") as mock_settings:
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_publishable_key = "pk_test"
            with patch("src.services.payment_sheet.stripe") as mock_stripe:
                with pytest.raises(ValueError, match="not found"):
                    create_payment_intent(999, 4900, "test", False, db)

    def test_creates_payment_intent_basic(self):
        sub = _mock_sub()
        db = _mock_db(sub=sub)
        pi = _mock_pi()

        with patch("src.services.payment_sheet.settings") as mock_settings, \
             patch("src.services.payment_sheet.stripe") as mock_stripe:
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_publishable_key = "pk_test"
            mock_stripe.PaymentIntent.create.return_value = pi

            result = create_payment_intent(1, 4900, "Top-up", False, db)

        assert result["payment_intent_id"] == "pi_test123"
        assert result["client_secret"] == "pi_test123_secret"
        assert result["amount"] == 4900
        assert result["save_card"] is False
        assert result["publishable_key"] == "pk_test"

    def test_save_card_sets_setup_future_usage(self):
        sub = _mock_sub()
        db = _mock_db(sub=sub)
        pi = _mock_pi()
        captured_params = {}

        def capture(**kwargs):
            captured_params.update(kwargs)
            return pi

        with patch("src.services.payment_sheet.settings") as mock_settings, \
             patch("src.services.payment_sheet.stripe") as mock_stripe:
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_publishable_key = "pk_test"
            mock_stripe.PaymentIntent.create.side_effect = capture

            create_payment_intent(1, 9900, "Unlock", True, db)

        assert captured_params.get("setup_future_usage") == "off_session"

    def test_customer_id_attached_when_present(self):
        sub = _mock_sub(stripe_customer_id="cus_abc")
        db = _mock_db(sub=sub)
        pi = _mock_pi()
        captured_params = {}

        with patch("src.services.payment_sheet.settings") as mock_settings, \
             patch("src.services.payment_sheet.stripe") as mock_stripe:
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_publishable_key = "pk_test"

            def capture(**kwargs):
                captured_params.update(kwargs)
                return pi

            mock_stripe.PaymentIntent.create.side_effect = capture
            create_payment_intent(1, 4900, "test", False, db)

        assert captured_params.get("customer") == "cus_abc"

    def test_no_customer_id_when_none(self):
        sub = _mock_sub(stripe_customer_id=None)
        db = _mock_db(sub=sub)
        pi = _mock_pi()
        captured_params = {}

        with patch("src.services.payment_sheet.settings") as mock_settings, \
             patch("src.services.payment_sheet.stripe") as mock_stripe:
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_publishable_key = "pk_test"

            def capture(**kwargs):
                captured_params.update(kwargs)
                return pi

            mock_stripe.PaymentIntent.create.side_effect = capture
            create_payment_intent(1, 4900, "test", False, db)

        assert "customer" not in captured_params

    def test_metadata_merged_into_params(self):
        sub = _mock_sub()
        db = _mock_db(sub=sub)
        pi = _mock_pi()
        captured_params = {}

        with patch("src.services.payment_sheet.settings") as mock_settings, \
             patch("src.services.payment_sheet.stripe") as mock_stripe:
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_publishable_key = "pk_test"

            def capture(**kwargs):
                captured_params.update(kwargs)
                return pi

            mock_stripe.PaymentIntent.create.side_effect = capture
            create_payment_intent(1, 4900, "test", False, db, metadata={"product": "bundle"})

        assert captured_params["metadata"]["product"] == "bundle"
        assert captured_params["metadata"]["subscriber_id"] == "1"

    def test_stripe_error_propagates(self):
        import stripe as stripe_lib
        sub = _mock_sub()
        db = _mock_db(sub=sub)

        with patch("src.services.payment_sheet.settings") as mock_settings, \
             patch("src.services.payment_sheet.stripe") as mock_stripe:
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_publishable_key = "pk_test"
            mock_stripe.error.StripeError = stripe_lib.error.StripeError
            mock_stripe.PaymentIntent.create.side_effect = stripe_lib.error.StripeError("fail")

            with pytest.raises(stripe_lib.error.StripeError):
                create_payment_intent(1, 4900, "test", False, db)


# ============================================================================
# get_publishable_key()
# ============================================================================


class TestGetPublishableKeyUnit:
    def test_returns_key_from_settings(self):
        with patch("src.services.payment_sheet.settings") as mock_settings:
            mock_settings.active_stripe_publishable_key = "pk_test_abc"
            assert get_publishable_key() == "pk_test_abc"

    def test_returns_none_when_not_configured(self):
        with patch("src.services.payment_sheet.settings") as mock_settings:
            mock_settings.active_stripe_publishable_key = None
            assert get_publishable_key() is None
