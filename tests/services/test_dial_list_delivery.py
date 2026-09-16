"""WP-9 Dial List — delivery adapter tests.

Formatter tests are pure (fixture in / payload out). Delivery-handoff tests
fake the Slack WebClient so no network is touched.
"""
from datetime import date
from decimal import Decimal

import pytest

from src.services.dial_list import (
    DialList,
    DialListEntry,
    deliver_dial_list,
    format_dial_list_digest,
)
from src.services.dial_list import delivery as delivery_mod

AS_OF = date(2026, 9, 16)


def _entry(**kw):
    base = dict(
        property_id=1,
        opportunity_id="opp-1",
        buyer_entity_id=100,
        triggers=["cash_purchase"],
        expected_revenue=Decimal("3000"),
        probability=Decimal("0.50"),
        expected_loan=Decimal("400000"),
        commission=Decimal("6000"),
        urgency=Decimal("1.0"),
        expected_loan_confidence="high",
        borrower_resolved=True,
        reason="Cash purchase, no financing — may want leverage next deal.",
        talking_points=["Owns 4 properties"],
        rank=1,
    )
    base.update(kw)
    return DialListEntry(**base)


def _dial_list(entries, **kw):
    base = dict(
        generated_for=AS_OF,
        entries=entries,
        candidate_count=len(entries),
        config_version="wp9-1.0.0",
    )
    base.update(kw)
    return DialList(**base)


# ---- formatter (pure) ------------------------------------------------------

def test_digest_renders_name_address_phone():
    dl = _dial_list([_entry(
        contact_name="ACME HOMES LLC", property_address="123 Main St, Tampa 33602",
        phone="813-555-0100",
    )])
    header, blocks = format_dial_list_digest(dl)
    text = "\n".join(b["text"]["text"] for b in blocks if b["type"] == "section")
    assert "ACME HOMES LLC" in text
    assert "123 Main St, Tampa 33602" in text
    assert "813-555-0100" in text


def test_digest_unresolved_name_marked_unverified():
    dl = _dial_list([_entry(
        contact_name="JOHN OWNER", borrower_resolved=False, buyer_entity_id=None,
    )])
    text = "\n".join(
        b["text"]["text"] for b in format_dial_list_digest(dl)[1]
        if b["type"] == "section"
    )
    assert "JOHN OWNER (unverified)" in text


def test_digest_renders_rank_trigger_size_reason_points():
    dl = _dial_list([_entry()])
    header, blocks = format_dial_list_digest(dl)
    text = "\n".join(b["text"]["text"] for b in blocks if b["type"] == "section")
    assert "#1" in text
    assert "cash_purchase" in text
    assert "$400,000" in text
    assert "(high)" in text
    assert "leverage" in text.lower()
    assert "Owns 4 properties" in text
    assert "top 1 calls" in header


def test_digest_deterministic():
    dl = _dial_list([_entry(), _entry(property_id=2, rank=2)])
    assert format_dial_list_digest(dl) == format_dial_list_digest(dl)


def test_digest_empty_list():
    header, blocks = format_dial_list_digest(_dial_list([]))
    assert "no opportunities today" in header
    # only the header section, no per-entry blocks / divider
    assert len(blocks) == 1


def test_digest_unresolved_borrower():
    dl = _dial_list([_entry(buyer_entity_id=None, borrower_resolved=False)])
    _, blocks = format_dial_list_digest(dl)
    text = "\n".join(b["text"]["text"] for b in blocks if b["type"] == "section")
    assert "Unresolved borrower" in text


def test_digest_no_loan_basis_shows_size_na():
    dl = _dial_list([_entry(expected_loan=Decimal("0"), expected_loan_confidence="low")])
    _, blocks = format_dial_list_digest(dl)
    text = "\n".join(b["text"]["text"] for b in blocks if b["type"] == "section")
    assert "size n/a" in text


def test_digest_low_confidence_surfaced():
    dl = _dial_list([_entry(expected_loan_confidence="low")])
    _, blocks = format_dial_list_digest(dl)
    text = "\n".join(b["text"]["text"] for b in blocks if b["type"] == "section")
    assert "(low)" in text


# ---- delivery handoff (faked Slack) ---------------------------------------

class _FakeResp(dict):
    pass


class _FakeClient:
    instances = []

    def __init__(self, token=None):
        self.token = token
        self.calls = []
        _FakeClient.instances.append(self)

    def chat_postMessage(self, **kw):
        self.calls.append(kw)
        return _FakeResp(ts="1700000000.0001")


class _SecretToken:
    def get_secret_value(self):
        return "xoxb-fake"


class _Settings:
    slack_bot_token = _SecretToken()
    dial_list_slack_channel = "#fa-max-money"


def _install_fakes(monkeypatch, settings=None):
    _FakeClient.instances = []
    monkeypatch.setattr(delivery_mod, "get_settings", lambda: settings or _Settings())
    import slack_sdk
    monkeypatch.setattr(slack_sdk, "WebClient", _FakeClient)


def test_deliver_posts_once_with_formatted_payload(monkeypatch):
    _install_fakes(monkeypatch)
    dl = _dial_list([_entry()])
    ts = deliver_dial_list(dl)
    assert ts == "1700000000.0001"
    assert len(_FakeClient.instances) == 1
    call = _FakeClient.instances[0].calls
    assert len(call) == 1
    assert call[0]["channel"] == "#fa-max-money"
    header, blocks = format_dial_list_digest(dl)
    assert call[0]["text"] == header
    assert call[0]["blocks"] == blocks


def test_deliver_explicit_channel_overrides_setting(monkeypatch):
    _install_fakes(monkeypatch)
    deliver_dial_list(_dial_list([_entry()]), channel="#override")
    assert _FakeClient.instances[0].calls[0]["channel"] == "#override"


def test_deliver_noops_when_unconfigured(monkeypatch):
    class _NoChannel:
        slack_bot_token = _SecretToken()
        dial_list_slack_channel = ""
    _install_fakes(monkeypatch, settings=_NoChannel())
    ts = deliver_dial_list(_dial_list([_entry()]))
    assert ts is None
    assert _FakeClient.instances == []  # never constructed a client


def test_deliver_logs_and_returns_none_on_post_failure(monkeypatch, caplog):
    class _BoomClient(_FakeClient):
        def chat_postMessage(self, **kw):
            raise RuntimeError("slack down")
    _FakeClient.instances = []
    monkeypatch.setattr(delivery_mod, "get_settings", lambda: _Settings())
    import slack_sdk
    monkeypatch.setattr(slack_sdk, "WebClient", _BoomClient)
    with caplog.at_level("ERROR"):
        ts = deliver_dial_list(_dial_list([_entry()]))
    assert ts is None
    assert any("Slack post failed" in r.message for r in caplog.records)
