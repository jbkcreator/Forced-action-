"""
Unit tests for white-label billing service (Stage 12 / fa056).
Mocks Stripe SDK and DB — no real Stripe or Postgres needed.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.services.white_label_billing import (
    _on_checkout_completed,
    _on_payment_failed,
    _on_subscription_deleted,
    _on_subscription_updated,
    is_wl_event,
)


# ---------------------------------------------------------------------------
# is_wl_event
# ---------------------------------------------------------------------------

def test_is_wl_event_true():
    event = MagicMock()
    event.data.object.metadata = {"wl_client_id": "5"}
    assert is_wl_event(event) is True


def test_is_wl_event_false():
    event = MagicMock()
    event.data.object.metadata = {"some_other_key": "value"}
    assert is_wl_event(event) is False


def test_is_wl_event_no_metadata():
    event = MagicMock()
    event.data.object.metadata = None
    assert is_wl_event(event) is False


# ---------------------------------------------------------------------------
# _on_checkout_completed
# ---------------------------------------------------------------------------

def test_checkout_completed_activates_client():
    db = MagicMock()
    existing = MagicMock()
    existing.status = "pending_verification"
    existing.admin_email = "admin@test.com"
    existing.company_name = "Test Corp"
    db.execute.return_value.fetchone.return_value = existing

    session = MagicMock()
    session.metadata = {"wl_client_id": "1", "plan_tier": "standard"}
    session.subscription = "sub_123"
    session.customer = "cus_abc"

    with patch("src.services.white_label_billing.send_activation_email") as mock_email:
        _on_checkout_completed(session, db)

    db.execute.assert_called()
    db.commit.assert_called()
    mock_email.assert_called_once_with("admin@test.com", "Test Corp")


def test_checkout_completed_skips_already_active():
    db = MagicMock()
    existing = MagicMock()
    existing.status = "active"
    db.execute.return_value.fetchone.return_value = existing

    session = MagicMock()
    session.metadata = {"wl_client_id": "1", "plan_tier": "standard"}

    with patch("src.services.white_label_billing.send_activation_email") as mock_email:
        _on_checkout_completed(session, db)

    mock_email.assert_not_called()
    db.commit.assert_not_called()


def test_checkout_completed_missing_client_id():
    db = MagicMock()
    session = MagicMock()
    session.metadata = {}  # no wl_client_id
    _on_checkout_completed(session, db)  # should return early without error
    db.execute.assert_not_called()


# ---------------------------------------------------------------------------
# _on_payment_failed
# ---------------------------------------------------------------------------

def test_payment_failed_sends_email():
    db = MagicMock()
    row = MagicMock()
    row.id = 1
    row.admin_email = "admin@test.com"
    row.company_name = "Test Corp"
    db.execute.return_value.fetchone.return_value = row

    invoice = MagicMock()
    invoice.customer = "cus_abc"

    with patch("src.services.white_label_billing.send_email") as mock_email:
        _on_payment_failed(invoice, db)
    mock_email.assert_called_once()


def test_payment_failed_no_client():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    invoice = MagicMock()
    invoice.customer = "cus_unknown"
    # Should not raise
    _on_payment_failed(invoice, db)


# ---------------------------------------------------------------------------
# _on_subscription_deleted
# ---------------------------------------------------------------------------

def test_subscription_deleted_sets_churned():
    db = MagicMock()
    row = MagicMock()
    row.id = 1
    row.admin_email = "admin@test.com"
    row.company_name = "Test Corp"
    db.execute.return_value.fetchone.return_value = row

    subscription = MagicMock()
    subscription.customer = "cus_abc"

    with patch("src.services.white_label_billing.send_email") as mock_email:
        _on_subscription_deleted(subscription, db)

    db.commit.assert_called_once()
    mock_email.assert_called_once()


# ---------------------------------------------------------------------------
# _on_subscription_updated
# ---------------------------------------------------------------------------

def test_subscription_updated_maps_active():
    db = MagicMock()
    subscription = MagicMock()
    subscription.status = "active"
    subscription.id = "sub_abc"
    subscription.customer = "cus_abc"

    _on_subscription_updated(subscription, db)
    db.commit.assert_called_once()


def test_subscription_updated_maps_unpaid_to_churned():
    db = MagicMock()
    subscription = MagicMock()
    subscription.status = "unpaid"
    subscription.id = "sub_abc"
    subscription.customer = "cus_abc"

    _on_subscription_updated(subscription, db)
    # Verify the UPDATE used status='churned'
    call_args = str(db.execute.call_args)
    assert "churned" in call_args
