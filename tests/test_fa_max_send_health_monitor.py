"""
WP-T2-1 go-live review (2026-09) — fa_max_send_health_monitor's warmup
check, scoped to the venture's actual sending mailbox.

Before this fix, _warmup_trip() called instantly_service.list_accounts()
unscoped and checked warmup health on every connected mailbox in the whole
Instantly workspace — an unrelated venture's mailbox could trip (or mask)
an FA Max EXCEPTIONS alert. These tests use fake account/warmup data
(no live Instantly credentials) to prove the fix's actual isolation
behavior, not just that it doesn't crash.
"""
from __future__ import annotations

from src.tasks import fa_max_send_health_monitor as monitor


def _account(email: str) -> dict:
    return {"email": email}


def _warmup(email: str, score: int) -> dict:
    return {"email": email, "health_score": score}


def test_no_trip_when_sender_mailbox_is_healthy(monkeypatch):
    monkeypatch.setattr(
        monitor.instantly_service, "list_accounts",
        lambda: [_account("fa-max@sender.com")],
    )
    monkeypatch.setattr(
        monitor.instantly_service, "get_warmup_analytics",
        lambda emails: [_warmup("fa-max@sender.com", 90)],
    )
    trip = monitor._warmup_trip("camp-1", "fa-max@sender.com")
    assert trip is None


def test_trips_when_sender_mailbox_is_unhealthy(monkeypatch):
    monkeypatch.setattr(
        monitor.instantly_service, "list_accounts",
        lambda: [_account("fa-max@sender.com")],
    )
    monkeypatch.setattr(
        monitor.instantly_service, "get_warmup_analytics",
        lambda emails: [_warmup("fa-max@sender.com", 40)],
    )
    trip = monitor._warmup_trip("camp-1", "fa-max@sender.com")
    assert trip is not None
    assert trip.rule == "fa_max_warmup_score_low"
    assert "fa-max@sender.com" in trip.detail


def test_unrelated_unhealthy_account_does_not_trip_fa_max_alert(monkeypatch):
    """The core isolation proof: an unrelated venture's failing mailbox must
    not page FA Max's EXCEPTIONS lane."""
    monkeypatch.setattr(
        monitor.instantly_service, "list_accounts",
        lambda: [_account("fa-max@sender.com"), _account("unrelated@othervent.com")],
    )
    calls = []

    def _fake_warmup(emails):
        calls.append(list(emails))
        return [_warmup("fa-max@sender.com", 95), _warmup("unrelated@othervent.com", 10)]

    monkeypatch.setattr(monitor.instantly_service, "get_warmup_analytics", _fake_warmup)

    trip = monitor._warmup_trip("camp-1", "fa-max@sender.com")
    assert trip is None
    # Only the FA Max sender's email was ever queried — the unrelated
    # account's poor score was never even fetched, let alone allowed to
    # influence the result.
    assert calls == [["fa-max@sender.com"]]


def test_unrelated_healthy_accounts_do_not_mask_fa_max_unhealthy_mailbox(monkeypatch):
    """The inverse isolation proof: a pile of unrelated healthy mailboxes
    must not average away or otherwise hide FA Max's own unhealthy one."""
    monkeypatch.setattr(
        monitor.instantly_service, "list_accounts",
        lambda: [
            _account("fa-max@sender.com"),
            _account("healthy1@othervent.com"),
            _account("healthy2@othervent.com"),
        ],
    )
    monkeypatch.setattr(
        monitor.instantly_service, "get_warmup_analytics",
        lambda emails: [_warmup(e, 30 if e == "fa-max@sender.com" else 99) for e in emails],
    )
    trip = monitor._warmup_trip("camp-1", "fa-max@sender.com")
    assert trip is not None
    assert trip.rule == "fa_max_warmup_score_low"


def test_no_trip_when_campaign_not_configured(monkeypatch):
    called = []
    monkeypatch.setattr(monitor.instantly_service, "list_accounts", lambda: called.append(1) or [])
    trip = monitor._warmup_trip(None, "fa-max@sender.com")
    assert trip is None
    assert called == []  # short-circuits before ever calling Instantly


def test_no_trip_when_sender_email_not_configured(monkeypatch):
    called = []
    monkeypatch.setattr(monitor.instantly_service, "list_accounts", lambda: called.append(1) or [])
    trip = monitor._warmup_trip("camp-1", None)
    assert trip is None
    assert called == []


def test_trips_when_sender_mailbox_not_found_among_connected_accounts(monkeypatch):
    """The configured sender doesn't match any connected Instantly account
    -- a real misconfiguration, distinct from an unhealthy mailbox, and
    worth its own alert rather than silently reporting healthy."""
    monkeypatch.setattr(
        monitor.instantly_service, "list_accounts",
        lambda: [_account("someone-else@othervent.com")],
    )
    trip = monitor._warmup_trip("camp-1", "fa-max@sender.com")
    assert trip is not None
    assert trip.rule == "fa_max_sender_mailbox_not_found"


def test_sender_email_match_is_case_insensitive(monkeypatch):
    monkeypatch.setattr(
        monitor.instantly_service, "list_accounts",
        lambda: [_account("FA-MAX@Sender.com")],
    )
    monkeypatch.setattr(
        monitor.instantly_service, "get_warmup_analytics",
        lambda emails: [_warmup(emails[0], 95)],
    )
    trip = monitor._warmup_trip("camp-1", "fa-max@sender.com")
    assert trip is None
