"""
Regression tests for src/services/owner_alert.py — PR #126 review fixes:

1. Vendor I/O (Telnyx/SMTP) must never block the caller — webhook handlers
   call notify_owner() synchronously, so a stalled Telnyx/SMTP call must not
   hold up webhook acknowledgement or an async route's event loop.
2. A "queued" Telnyx response is acceptance, not delivery — email fallback
   must wait for delivery confirmation (or the sweep), not fire immediately,
   but must still fire once delivery is confirmed failed.
3. Retries (same idempotency_key) must not re-dispatch a duplicate alert.

DB access is mocked throughout — no real Postgres required.
"""
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import src.services.owner_alert as owner_alert
from src.services.telnyx_sms import TelnyxSMSError


@contextmanager
def _fake_db_context(session):
    yield session


def _settings(founder_phone: str | None = "+18135551234", telnyx_sms_enabled: bool = True):
    return SimpleNamespace(founder_phone=founder_phone, telnyx_sms_enabled=telnyx_sms_enabled)


def _wait_for_thread():
    """notify_owner()'s vendor I/O runs on a daemon thread; give it a beat to finish."""
    time.sleep(0.2)


@pytest.fixture(autouse=True)
def _no_real_db():
    """_claim_alert/_update_status/reconcile use get_db_context — always mock it."""
    with patch.object(owner_alert, "get_db_context") as mock_ctx:
        yield mock_ctx


def test_notify_owner_returns_immediately_even_if_telnyx_stalls(_no_real_db):
    _no_real_db.side_effect = lambda: _fake_db_context(MagicMock())

    def _slow_send(*a, **k):
        time.sleep(2)
        return {"status": "queued", "message_id": "msg_slow"}

    with patch.object(owner_alert, "get_settings", return_value=_settings()), \
         patch.object(owner_alert, "send_message", side_effect=_slow_send), \
         patch.object(owner_alert, "_claim_alert", return_value=1):
        start = time.monotonic()
        owner_alert.notify_owner("Test", "body", idempotency_key="stripe:evt_1")
        elapsed = time.monotonic() - start

    assert elapsed < 1.0, "notify_owner must not block on vendor I/O"


def test_no_telnyx_configured_falls_back_to_email(_no_real_db):
    with patch.object(owner_alert, "get_settings", return_value=_settings(founder_phone=None, telnyx_sms_enabled=False)), \
         patch.object(owner_alert, "send_alert") as mock_send_alert, \
         patch.object(owner_alert, "_claim_alert", return_value=1), \
         patch.object(owner_alert, "_update_status"):
        owner_alert.notify_owner("Test", "body", idempotency_key="stripe:evt_2")
        _wait_for_thread()

    assert mock_send_alert.called


def test_telnyx_send_failure_falls_back_to_email(_no_real_db):
    with patch.object(owner_alert, "get_settings", return_value=_settings()), \
         patch.object(owner_alert, "send_message", side_effect=TelnyxSMSError("boom")), \
         patch.object(owner_alert, "send_alert") as mock_send_alert, \
         patch.object(owner_alert, "_claim_alert", return_value=1), \
         patch.object(owner_alert, "_update_status"):
        owner_alert.notify_owner("Test", "body", idempotency_key="stripe:evt_3")
        _wait_for_thread()

    assert mock_send_alert.called


def test_queued_sms_does_not_trigger_email_fallback_yet(_no_real_db):
    with patch.object(owner_alert, "get_settings", return_value=_settings()), \
         patch.object(owner_alert, "send_message", return_value={"status": "queued", "message_id": "msg_1"}), \
         patch.object(owner_alert, "send_alert") as mock_send_alert, \
         patch.object(owner_alert, "_claim_alert", return_value=1), \
         patch.object(owner_alert, "_update_status") as mock_update:
        owner_alert.notify_owner("Test", "body", idempotency_key="stripe:evt_4")
        _wait_for_thread()

    assert not mock_send_alert.called
    mock_update.assert_called_with(1, status="sms_sent", telnyx_message_id="msg_1")


def test_duplicate_idempotency_key_does_not_redispatch(_no_real_db):
    with patch.object(owner_alert, "_claim_alert", return_value=None) as mock_claim, \
         patch.object(owner_alert, "_dispatch_alert") as mock_dispatch:
        owner_alert.notify_owner("Test", "body", idempotency_key="stripe:evt_5")

    mock_claim.assert_called_once()
    assert not mock_dispatch.called


def test_reconcile_delivered_marks_delivered_without_email(_no_real_db):
    fake_session = MagicMock()
    fake_session.execute.return_value.first.return_value = SimpleNamespace(id=7, subject="s", body="b")
    _no_real_db.side_effect = lambda: _fake_db_context(fake_session)

    with patch.object(owner_alert, "send_alert") as mock_send_alert, \
         patch.object(owner_alert, "_update_status") as mock_update:
        owner_alert.reconcile_delivery_status(telnyx_message_id="msg_1", delivery_status="delivered")

    mock_update.assert_called_once_with(7, status="sms_delivered")
    assert not mock_send_alert.called


def test_reconcile_failed_delivery_triggers_email_fallback(_no_real_db):
    fake_session = MagicMock()
    fake_session.execute.return_value.first.return_value = SimpleNamespace(id=8, subject="s", body="b")
    _no_real_db.side_effect = lambda: _fake_db_context(fake_session)

    with patch.object(owner_alert, "send_alert") as mock_send_alert, \
         patch.object(owner_alert, "_update_status") as mock_update:
        owner_alert.reconcile_delivery_status(telnyx_message_id="msg_1", delivery_status="delivery_failed")

    assert mock_send_alert.called
    mock_update.assert_called_with(8, status="email_sent")
