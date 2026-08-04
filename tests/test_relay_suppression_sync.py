"""
Tests for src.services.relay.suppression_sync (RELAY-v2.2 sub-task R3,
client Q1 -- writing a Relay recipient's unsubscribe back into
email_opt_outs so it also stops the Lifecycle runtime).

CLONE-v2.2 / CL3: sync_unsubscribes() resolves its campaign id from the
calling venture's config (src.utils.venture_config), not the fleet-wide
settings value, so these tests patch get_venture_config rather than
get_settings.

Instantly I/O is mocked. suppress_contact() itself is monkeypatched too --
it does real DB writes (see src.services.email_suppression), which is
proven separately; these tests only need to verify sync_unsubscribes()
calls it for the right leads with the right status.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from config.venture_template import DEFAULT_VENTURE_KEY
from src.services.relay import suppression_sync


def _patch_venture(monkeypatch, *, campaign_id: str | None = "camp-1"):
    venture = MagicMock(relay_instantly_campaign_id=campaign_id)
    monkeypatch.setattr(suppression_sync, "get_venture_config", lambda key: venture)
    return venture


def test_returns_zero_when_channel_not_configured(monkeypatch):
    _patch_venture(monkeypatch, campaign_id=None)

    assert suppression_sync.sync_unsubscribes() == 0


def test_suppresses_unsubscribed_and_bounced_leads(monkeypatch):
    _patch_venture(monkeypatch)

    pages = [
        {
            "leads": [
                {"email": "opted-out@example.com", "interest_status": "unsubscribed"},
                {"email": "still-active@example.com", "interest_status": "active"},
                {"email": "hard-bounced@example.com", "status": "bounced"},
            ],
            "next_starting_after": None,
        },
    ]
    monkeypatch.setattr(suppression_sync.instantly, "list_leads", lambda *a, **k: pages.pop(0))

    calls = []
    monkeypatch.setattr(
        suppression_sync, "suppress_contact",
        lambda db, email, source: calls.append((email, source)),
    )

    n = suppression_sync.sync_unsubscribes()

    assert n == 2
    assert ("opted-out@example.com", "instantly_sync") in calls
    assert ("hard-bounced@example.com", "instantly_sync") in calls
    assert not any(e == "still-active@example.com" for e, _ in calls)


def test_paginates_until_no_cursor(monkeypatch):
    _patch_venture(monkeypatch)

    pages = [
        {"leads": [{"email": "a@example.com", "interest_status": "unsubscribed"}], "next_starting_after": "cursor-2"},
        {"leads": [{"email": "b@example.com", "interest_status": "unsubscribed"}], "next_starting_after": None},
    ]
    calls_made = []

    def _fake_list_leads(campaign_id, cursor=None):
        calls_made.append(cursor)
        return pages.pop(0)

    monkeypatch.setattr(suppression_sync.instantly, "list_leads", _fake_list_leads)
    monkeypatch.setattr(suppression_sync, "suppress_contact", lambda db, email, source: None)

    n = suppression_sync.sync_unsubscribes()

    assert n == 2
    assert calls_made == [None, "cursor-2"]


def test_stops_on_empty_page(monkeypatch):
    _patch_venture(monkeypatch)
    monkeypatch.setattr(suppression_sync.instantly, "list_leads", lambda *a, **k: {"leads": [], "next_starting_after": None})
    monkeypatch.setattr(suppression_sync, "suppress_contact", lambda **k: (_ for _ in ()).throw(AssertionError("should not be called")))

    assert suppression_sync.sync_unsubscribes() == 0


def test_stops_when_list_leads_returns_none(monkeypatch):
    """Matches instantly_service.list_leads()'s own contract: returns None
    on a request failure rather than raising."""
    _patch_venture(monkeypatch)
    monkeypatch.setattr(suppression_sync.instantly, "list_leads", lambda *a, **k: None)

    assert suppression_sync.sync_unsubscribes() == 0


def test_venture_key_defaults_to_venture_one(monkeypatch):
    seen: list[str] = []
    venture = MagicMock(relay_instantly_campaign_id=None)
    monkeypatch.setattr(
        suppression_sync, "get_venture_config",
        lambda key: seen.append(key) or venture,
    )

    suppression_sync.sync_unsubscribes()

    assert seen == [DEFAULT_VENTURE_KEY]


def test_scoped_to_the_given_venture(monkeypatch):
    """Each venture must resolve its OWN campaign id, not venture #1's --
    otherwise venture B's unsubscribes would never reach email_opt_outs and
    the execution guard would have nothing local to suppress them with."""
    seen: list[str] = []

    def _get_venture_config(key):
        seen.append(key)
        return MagicMock(relay_instantly_campaign_id="camp-venture-two" if key == "venture_two" else None)

    monkeypatch.setattr(suppression_sync, "get_venture_config", _get_venture_config)
    monkeypatch.setattr(
        suppression_sync.instantly, "list_leads",
        lambda campaign_id, cursor=None: {
            "leads": [{"email": "b@example.com", "interest_status": "unsubscribed"}],
            "next_starting_after": None,
        } if campaign_id == "camp-venture-two" else None,
    )
    calls = []
    monkeypatch.setattr(suppression_sync, "suppress_contact", lambda db, email, source: calls.append(email))

    n = suppression_sync.sync_unsubscribes(venture_key="venture_two")

    assert seen == ["venture_two"]
    assert n == 1
    assert calls == ["b@example.com"]
