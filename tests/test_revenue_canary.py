from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text

from src.services.revenue_canary import (
    CHECK_NAMES,
    CanaryResult,
    run_all_checks,
)


# ── Stripe-key safety gate ────────────────────────────────────────────────

def test_require_stripe_test_key_rejects_live_key():
    from src.services.revenue_canary import _require_stripe_test_key
    fake_settings = MagicMock()
    fake_settings.stripe_test_secret_key.get_secret_value.return_value = "sk_live_oops"
    with patch("src.services.revenue_canary.get_settings", return_value=fake_settings):
        with pytest.raises(RuntimeError, match="sk_test"):
            _require_stripe_test_key()


def test_require_stripe_test_key_accepts_test_key():
    from src.services.revenue_canary import _require_stripe_test_key
    fake_settings = MagicMock()
    fake_settings.stripe_test_secret_key.get_secret_value.return_value = "sk_test_abc123"
    with patch("src.services.revenue_canary.get_settings", return_value=fake_settings):
        assert _require_stripe_test_key() == "sk_test_abc123"


# ── checkout ──────────────────────────────────────────────────────────────

def test_check_checkout_ok_on_succeeded_payment_intent():
    from src.services.revenue_canary import _check_checkout
    fake_pi = {"id": "pi_canary_1", "status": "succeeded"}
    with patch("src.services.revenue_canary._require_stripe_test_key", return_value="sk_test_x"), \
         patch("stripe.PaymentIntent.create", return_value=fake_pi):
        result = _check_checkout()
    assert result.name == "checkout"
    assert result.ok is True
    assert "pi_canary_1" in result.detail


def test_check_checkout_fails_on_non_succeeded_status():
    from src.services.revenue_canary import _check_checkout
    fake_pi = {"id": "pi_canary_2", "status": "requires_action"}
    with patch("src.services.revenue_canary._require_stripe_test_key", return_value="sk_test_x"), \
         patch("stripe.PaymentIntent.create", return_value=fake_pi):
        result = _check_checkout()
    assert result.ok is False


def test_check_checkout_fails_on_exception():
    from src.services.revenue_canary import _check_checkout
    with patch("src.services.revenue_canary._require_stripe_test_key", return_value="sk_test_x"), \
         patch("stripe.PaymentIntent.create", side_effect=RuntimeError("stripe down")):
        result = _check_checkout()
    assert result.ok is False
    assert "stripe down" in result.detail


def test_check_checkout_force_fail_short_circuits():
    from src.services.revenue_canary import _check_checkout
    with patch("stripe.PaymentIntent.create") as create:
        result = _check_checkout(force_fail=True)
    assert result.ok is False
    assert "--kill-canary" in result.detail
    create.assert_not_called()


# ── payment_link ──────────────────────────────────────────────────────────

def test_check_payment_link_ok_when_session_has_url():
    from src.services.revenue_canary import _check_payment_link
    fake_session_dict = {"id": "cs_canary_1", "url": "https://checkout.stripe.com/pay/cs_canary_1"}
    with patch("src.services.revenue_canary._require_stripe_test_key", return_value="sk_test_x"), \
         patch("stripe.checkout.Session.create", return_value=fake_session_dict):
        result = _check_payment_link()
    assert result.ok is True


def test_check_payment_link_fails_when_url_missing():
    from src.services.revenue_canary import _check_payment_link
    fake_session_dict = {"id": "cs_canary_2", "url": None}
    with patch("src.services.revenue_canary._require_stripe_test_key", return_value="sk_test_x"), \
         patch("stripe.checkout.Session.create", return_value=fake_session_dict):
        result = _check_payment_link()
    assert result.ok is False


# ── entitlement / delivery (real DB round trip) ─────────────────────────────

def test_check_entitlement_round_trips_probe_log(fresh_db):
    from src.services.revenue_canary import _check_entitlement
    result = _check_entitlement(fresh_db)
    assert result.ok is True
    row = fresh_db.execute(text(
        "SELECT probe_value FROM revenue_canary_probe_log WHERE check_name = 'entitlement'"
    )).fetchone()
    assert row is not None


def test_check_delivery_round_trips_probe_log(fresh_db):
    from src.services.revenue_canary import _check_delivery
    result = _check_delivery(fresh_db)
    assert result.ok is True
    row = fresh_db.execute(text(
        "SELECT probe_value FROM revenue_canary_probe_log WHERE check_name = 'delivery'"
    )).fetchone()
    assert row is not None


def test_entitlement_and_delivery_never_touch_platform_revenue_ledger(fresh_db):
    """Regression guard for this plan's core safety constraint."""
    from src.services.revenue_canary import _check_entitlement, _check_delivery
    before = fresh_db.execute(text("SELECT COUNT(*) AS c FROM platform_revenue_ledger")).fetchone().c
    _check_entitlement(fresh_db)
    _check_delivery(fresh_db)
    after = fresh_db.execute(text("SELECT COUNT(*) AS c FROM platform_revenue_ledger")).fetchone().c
    assert after == before


# ── mail ──────────────────────────────────────────────────────────────────

def test_check_mail_ok_on_successful_send():
    from src.services.revenue_canary import _check_mail
    fake_settings = MagicMock()
    fake_settings.smtp_host = "smtp.example.com"
    fake_settings.smtp_user = "user"
    fake_settings.smtp_pass.get_secret_value.return_value = "pass"
    fake_settings.canary_mail_to = "canary@example.com"
    fake_settings.email_from = "noreply@example.com"
    fake_settings.smtp_port = 587
    with patch("src.services.revenue_canary.get_settings", return_value=fake_settings), \
         patch("smtplib.SMTP") as mock_smtp:
        mock_conn = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_conn
        result = _check_mail()
    assert result.ok is True
    mock_conn.sendmail.assert_called_once()


def test_check_mail_fails_when_not_configured():
    from src.services.revenue_canary import _check_mail
    fake_settings = MagicMock()
    fake_settings.smtp_host = None
    with patch("src.services.revenue_canary.get_settings", return_value=fake_settings):
        result = _check_mail()
    assert result.ok is False
    assert "not configured" in result.detail


# ── model_api ─────────────────────────────────────────────────────────────

def test_check_model_api_ok_on_response():
    from src.services.revenue_canary import _check_model_api
    with patch("src.services.claude_router.call_claude", return_value="OK") as call:
        result = _check_model_api()
    assert result.ok is True
    call.assert_called_once()
    assert call.call_args.kwargs["force_tier"] == "haiku"
    assert call.call_args.kwargs["db"] is None


def test_check_model_api_fails_on_exception():
    from src.services.revenue_canary import _check_model_api
    with patch("src.services.claude_router.call_claude", side_effect=RuntimeError("api down")):
        result = _check_model_api()
    assert result.ok is False


# ── run_all_checks / kill switch ────────────────────────────────────────────

def test_run_all_checks_returns_six_results(fresh_db):
    with patch("src.services.revenue_canary._check_checkout") as checkout, \
         patch("src.services.revenue_canary._check_payment_link") as pl, \
         patch("src.services.revenue_canary._check_mail") as mail, \
         patch("src.services.revenue_canary._check_model_api") as model:
        checkout.return_value = CanaryResult("checkout", True, "ok", 1)
        pl.return_value = CanaryResult("payment_link", True, "ok", 1)
        mail.return_value = CanaryResult("mail", True, "ok", 1)
        model.return_value = CanaryResult("model_api", True, "ok", 1)
        results = run_all_checks(fresh_db)
    assert {r.name for r in results} == set(CHECK_NAMES)


def test_run_all_checks_kill_forces_named_check_to_fail(fresh_db):
    with patch("src.services.revenue_canary._check_payment_link") as pl, \
         patch("src.services.revenue_canary._check_mail") as mail, \
         patch("src.services.revenue_canary._check_model_api") as model:
        pl.return_value = CanaryResult("payment_link", True, "ok", 1)
        mail.return_value = CanaryResult("mail", True, "ok", 1)
        model.return_value = CanaryResult("model_api", True, "ok", 1)
        results = run_all_checks(fresh_db, kill="checkout")
    checkout_result = next(r for r in results if r.name == "checkout")
    assert checkout_result.ok is False
    assert "--kill-canary" in checkout_result.detail
    # everything else still ran and passed
    assert all(r.ok for r in results if r.name != "checkout")


def test_run_all_checks_rejects_unknown_kill_name(fresh_db):
    with pytest.raises(ValueError, match="Unknown canary check"):
        run_all_checks(fresh_db, kill="not_a_real_check")
