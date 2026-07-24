"""
Tests for src.services.relay.channels_email (RELAY-v2.2 sub-task R2).

All Instantly I/O is mocked — no real API calls. Focuses on the two
safety-critical behaviors: refusing to send when unconfigured, and
"fail loud" on Instantly's duplicate-contact skip (a leads_skipped > 0
response must raise, never be treated as a successful send).
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.services.relay import channels_email
from src.services.relay.channels import DISPATCHERS
from src.services.relay.queue import QueueItem


def _make_item(payload: dict | None = None, recipient: str = "prospect@example.com") -> QueueItem:
    return QueueItem(
        id=1,
        idempotency_key="key-1",
        batch_id="batch-1",
        thread_id=None,
        channel="email",
        recipient=recipient,
        payload=payload if payload is not None else {"subject": "Hi", "body": "Hello there"},
        status="approved",
        slack_message_ts=None,
        decided_by="U_TEST",
        decided_at=None,
        error=None,
        dispatched_at=None,
        created_at=datetime.now(timezone.utc),
    )


def test_email_is_registered_in_dispatchers():
    assert DISPATCHERS.get("email") is channels_email.send_email


def test_raises_when_campaign_id_not_configured(monkeypatch):
    fake_settings = MagicMock(relay_instantly_campaign_id=None)
    monkeypatch.setattr(channels_email, "get_settings", lambda: fake_settings)

    with pytest.raises(RuntimeError, match="RELAY_INSTANTLY_CAMPAIGN_ID"):
        channels_email.send_email(_make_item())


def test_raises_when_body_missing(monkeypatch):
    fake_settings = MagicMock(relay_instantly_campaign_id="camp-1")
    monkeypatch.setattr(channels_email, "get_settings", lambda: fake_settings)

    with pytest.raises(RuntimeError, match="missing 'body'"):
        channels_email.send_email(_make_item(payload={"subject": "Hi"}))


def test_raises_when_add_leads_returns_none(monkeypatch):
    fake_settings = MagicMock(relay_instantly_campaign_id="camp-1")
    monkeypatch.setattr(channels_email, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(channels_email.instantly, "add_leads", lambda *a, **k: None)

    with pytest.raises(RuntimeError, match="add_leads call failed"):
        channels_email.send_email(_make_item())


def test_fail_loud_on_duplicate_skip(monkeypatch):
    """The core safety assertion: Instantly's silent dedup-skip must never
    be treated as a successful send."""
    fake_settings = MagicMock(relay_instantly_campaign_id="camp-1")
    monkeypatch.setattr(channels_email, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(
        channels_email.instantly, "add_leads",
        lambda *a, **k: {"leads_created": 0, "leads_skipped": 1},
    )

    with pytest.raises(RuntimeError, match="already a member"):
        channels_email.send_email(_make_item())


def test_raises_when_zero_leads_created_and_zero_skipped(monkeypatch):
    fake_settings = MagicMock(relay_instantly_campaign_id="camp-1")
    monkeypatch.setattr(channels_email, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(
        channels_email.instantly, "add_leads",
        lambda *a, **k: {"leads_created": 0, "leads_skipped": 0},
    )

    with pytest.raises(RuntimeError, match="0 leads created"):
        channels_email.send_email(_make_item())


def test_successful_send_passes_subject_and_body_as_merge_vars(monkeypatch):
    fake_settings = MagicMock(relay_instantly_campaign_id="camp-1")
    monkeypatch.setattr(channels_email, "get_settings", lambda: fake_settings)

    calls = []

    def _fake_add_leads(campaign_id, leads):
        calls.append((campaign_id, leads))
        return {"leads_created": 1, "leads_skipped": 0}

    monkeypatch.setattr(channels_email.instantly, "add_leads", _fake_add_leads)

    item = _make_item(payload={"subject": "Congrats on the auction win", "body": "Full drafted body"})
    channels_email.send_email(item)  # must not raise

    assert len(calls) == 1
    campaign_id, leads = calls[0]
    assert campaign_id == "camp-1"
    assert leads == [{
        "email": "prospect@example.com",
        "ra_subject": "Congrats on the auction win",
        "ra_body": "Full drafted body",
    }]
