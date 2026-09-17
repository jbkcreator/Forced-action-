"""
WP-T2-1 go-live review (2026-09) — immediate bounce/unsubscribe suppression
via the Instantly webhook, replacing the ~30-min poll as the primary trigger.
"""
from __future__ import annotations

from unittest.mock import patch

from src.services.relay import bounce_webhook


class _FakeDb:
    pass


def test_email_bounced_event_suppresses_contact():
    with patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        acted = bounce_webhook.handle_event(
            _FakeDb(), {"event_type": "email_bounced", "email": "Dead@Example.com"}
        )
    assert acted is True
    mock_suppress.assert_called_once()
    _, kwargs = mock_suppress.call_args
    assert kwargs["email"] == "dead@example.com"
    assert kwargs["source"] == "instantly_webhook:email_bounced"


def test_lead_unsubscribed_event_suppresses_contact():
    with patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        acted = bounce_webhook.handle_event(
            _FakeDb(), {"event_type": "lead_unsubscribed", "lead": {"email": "unsub@example.com"}}
        )
    assert acted is True
    mock_suppress.assert_called_once()


def test_unrecognized_event_type_is_ignored_not_errored():
    with patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        acted = bounce_webhook.handle_event(
            _FakeDb(), {"event_type": "email_opened", "email": "someone@example.com"}
        )
    assert acted is False
    mock_suppress.assert_not_called()


def test_bounce_event_with_no_extractable_email_does_not_suppress():
    with patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        acted = bounce_webhook.handle_event(_FakeDb(), {"event_type": "email_bounced"})
    assert acted is False
    mock_suppress.assert_not_called()


def test_spam_complaint_is_not_a_recognized_event_type():
    """Documented gap: Instantly's webhook catalog has no complaint event —
    this must not silently be treated as equivalent to a bounce."""
    with patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        acted = bounce_webhook.handle_event(
            _FakeDb(), {"event_type": "spam_complaint", "email": "someone@example.com"}
        )
    assert acted is False
    mock_suppress.assert_not_called()
