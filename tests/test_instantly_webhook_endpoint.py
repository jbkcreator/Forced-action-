"""
WP-T2-1 go-live review (2026-09) — POST /webhooks/instantly: shared-secret
auth, idempotency, and fail-open-on-our-own-bug (never 5xx back to Instantly
for a handler exception, so it doesn't retry into the same bug).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


class _FakeSecret:
    def __init__(self, value):
        self._value = value

    def get_secret_value(self):
        return self._value


class _FakeDb:
    pass


@pytest.fixture
def client():
    from src.api.main import app
    from src.api.deps import get_db
    app.dependency_overrides[get_db] = lambda: _FakeDb()
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.pop(get_db, None)


def _settings_with_secret(secret="topsecret"):
    s = MagicMock()
    s.instantly_webhook_secret = _FakeSecret(secret) if secret else None
    return s


def test_missing_secret_header_rejected(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret()):
        resp = client.post("/webhooks/instantly", json={"event_type": "email_bounced", "email": "a@example.com"})
    assert resp.status_code == 403


def test_wrong_secret_header_rejected(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret()):
        resp = client.post(
            "/webhooks/instantly",
            json={"event_type": "email_bounced", "email": "a@example.com"},
            headers={"X-Instantly-Webhook-Secret": "wrong"},
        )
    assert resp.status_code == 403


def test_unconfigured_secret_fails_closed(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret(secret=None)):
        resp = client.post(
            "/webhooks/instantly",
            json={"event_type": "email_bounced", "email": "a@example.com"},
            headers={"X-Instantly-Webhook-Secret": "anything"},
        )
    assert resp.status_code == 403


def test_correct_secret_and_bounce_event_suppresses(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret()), \
         patch("src.api.main.already_logged", return_value=False, create=True), \
         patch("src.services.webhook_log.already_logged", return_value=False), \
         patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        resp = client.post(
            "/webhooks/instantly",
            json={"event_type": "email_bounced", "email": "a@example.com", "event_id": "evt_1"},
            headers={"X-Instantly-Webhook-Secret": "topsecret"},
        )
    assert resp.status_code == 200
    mock_suppress.assert_called_once()


def test_duplicate_event_id_is_a_noop(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret()), \
         patch("src.services.webhook_log.already_logged", return_value=True), \
         patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        resp = client.post(
            "/webhooks/instantly",
            json={"event_type": "email_bounced", "email": "a@example.com", "event_id": "evt_1"},
            headers={"X-Instantly-Webhook-Secret": "topsecret"},
        )
    assert resp.status_code == 200
    mock_suppress.assert_not_called()


def test_handler_exception_still_returns_200(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret()), \
         patch("src.services.webhook_log.already_logged", return_value=False), \
         patch("src.services.relay.bounce_webhook.handle_event", side_effect=RuntimeError("boom")):
        resp = client.post(
            "/webhooks/instantly",
            json={"event_type": "email_bounced", "email": "a@example.com"},
            headers={"X-Instantly-Webhook-Secret": "topsecret"},
        )
    assert resp.status_code == 200


def test_unrecognized_event_type_returns_200_without_suppressing(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret()), \
         patch("src.services.webhook_log.already_logged", return_value=False), \
         patch("src.services.relay.bounce_webhook.suppress_contact") as mock_suppress:
        resp = client.post(
            "/webhooks/instantly",
            json={"event_type": "email_opened", "email": "a@example.com"},
            headers={"X-Instantly-Webhook-Secret": "topsecret"},
        )
    assert resp.status_code == 200
    mock_suppress.assert_not_called()


def test_invalid_json_body_rejected(client):
    with patch("src.api.main.get_settings", return_value=_settings_with_secret()):
        resp = client.post(
            "/webhooks/instantly",
            content=b"not json",
            headers={
                "X-Instantly-Webhook-Secret": "topsecret",
                "Content-Type": "application/json",
            },
        )
    assert resp.status_code == 400
