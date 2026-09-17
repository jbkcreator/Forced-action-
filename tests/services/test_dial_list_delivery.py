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

def _all_text(blocks) -> str:
    """Extract all mrkdwn/plain_text strings from any block type."""
    parts = []
    for b in blocks:
        if b.get("type") in ("header",):
            t = b.get("text", {})
            parts.append(t.get("text", ""))
        elif b.get("type") == "section":
            if "text" in b:
                parts.append(b["text"].get("text", ""))
            for f in b.get("fields", []):
                parts.append(f.get("text", ""))
        elif b.get("type") == "context":
            for el in b.get("elements", []):
                parts.append(el.get("text", ""))
    return "\n".join(parts)


def test_digest_renders_name_address_phone():
    dl = _dial_list([_entry(
        contact_name="ACME HOMES LLC", property_address="123 Main St, Tampa 33602",
        phone="813-555-0100",
    )])
    header, blocks = format_dial_list_digest(dl)
    text = _all_text(blocks)
    assert "ACME HOMES LLC" in text
    assert "123 Main St, Tampa 33602" in text
    assert "813-555-0100" in text


def test_digest_unresolved_name_marked_unverified():
    dl = _dial_list([_entry(
        contact_name="JOHN OWNER", borrower_resolved=False, buyer_entity_id=None,
    )])
    text = _all_text(format_dial_list_digest(dl)[1])
    assert "JOHN OWNER (unverified)" in text


def test_digest_renders_rank_trigger_size_reason_points():
    dl = _dial_list([_entry()])
    header, blocks = format_dial_list_digest(dl)
    text = _all_text(blocks)
    assert "#1" in text
    assert "Cash Purchase" in text  # trigger label
    assert "$400K" in text  # compact money format
    assert "leverage" in text.lower()
    assert "Owns 4 properties" in text
    assert "1 calls" in header


def test_digest_deterministic():
    dl = _dial_list([_entry(), _entry(property_id=2, rank=2)])
    assert format_dial_list_digest(dl) == format_dial_list_digest(dl)


def test_digest_empty_list():
    header, blocks = format_dial_list_digest(_dial_list([]))
    assert "no opportunities today" in header
    assert len(blocks) == 1


def test_digest_unresolved_borrower():
    dl = _dial_list([_entry(buyer_entity_id=None, borrower_resolved=False)])
    _, blocks = format_dial_list_digest(dl)
    text = _all_text(blocks)
    assert "Unresolved borrower" in text


def test_digest_no_loan_basis_shows_size_na():
    dl = _dial_list([_entry(expected_loan=Decimal("0"), expected_loan_confidence="low")])
    _, blocks = format_dial_list_digest(dl)
    text = _all_text(blocks)
    assert "size n/a" in text


def test_digest_low_confidence_surfaced_in_header():
    dl = _dial_list([_entry(expected_loan_confidence="low")])
    header, blocks = format_dial_list_digest(dl)
    # confidence note appears in blocks (context), never as per-entry "(low)" tag
    text = _all_text(blocks)
    assert "rough estimates" in text or "assessed value" in text
    assert "(low)" not in text


def test_digest_stale_sources_warning_in_header():
    dl = _dial_list([_entry()], stale_sources=["deeds", "probate"])
    _, blocks = format_dial_list_digest(dl)
    text = _all_text(blocks)
    assert "Stale sources" in text
    assert "deeds" in text and "probate" in text


def test_digest_from_cache_warning_in_header():
    dl = _dial_list([_entry()], from_cache=True)
    _, blocks = format_dial_list_digest(dl)
    text = _all_text(blocks)
    assert "cached state" in text


def test_digest_no_stale_warning_when_fresh():
    dl = _dial_list([_entry()])  # no stale_sources, not from cache
    _, blocks = format_dial_list_digest(dl)
    text = _all_text(blocks)
    assert "Stale sources" not in text
    assert "cached state" not in text


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
    dial_list_source_sla_days = 2


def _install_fakes(monkeypatch, settings=None):
    _FakeClient.instances = []
    monkeypatch.setattr(delivery_mod, "get_settings", lambda: settings or _Settings())
    import slack_sdk
    monkeypatch.setattr(slack_sdk, "WebClient", _FakeClient)


def test_deliver_posts_header_then_entries_as_thread(monkeypatch):
    _install_fakes(monkeypatch)
    dl = _dial_list([_entry(), _entry(property_id=2, rank=2)])
    ts = deliver_dial_list(dl)
    assert ts == "1700000000.0001"
    client = _FakeClient.instances[0]
    calls = client.calls
    # 1 header call + 1 call per entry
    assert len(calls) == 3
    # First call is the header (no thread_ts)
    assert "thread_ts" not in calls[0]
    assert calls[0]["channel"] == "#fa-max-money"
    # Entry calls are threaded under the header ts
    assert calls[1]["thread_ts"] == "1700000000.0001"
    assert calls[2]["thread_ts"] == "1700000000.0001"


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


def test_deliver_removes_partial_thread_on_entry_failure(monkeypatch):
    class _PartialClient(_FakeClient):
        def __init__(self, token=None):
            super().__init__(token)
            self.deletes = []

        def chat_postMessage(self, **kw):
            self.calls.append(kw)
            if len(self.calls) == 3:
                raise RuntimeError("entry post failed")
            return _FakeResp(ts=f"1700000000.000{len(self.calls)}")

        def chat_delete(self, **kw):
            self.deletes.append(kw)

    _FakeClient.instances = []
    monkeypatch.setattr(delivery_mod, "get_settings", lambda: _Settings())
    import slack_sdk
    monkeypatch.setattr(slack_sdk, "WebClient", _PartialClient)

    assert deliver_dial_list(_dial_list([_entry(), _entry(property_id=2, rank=2)])) is None
    client = _FakeClient.instances[0]
    assert [call["ts"] for call in client.deletes] == ["1700000000.0002", "1700000000.0001"]


# ---- cached fallback (generate_and_deliver) --------------------------------

def test_generate_and_deliver_falls_back_to_cache(monkeypatch):
    from sqlalchemy.exc import SQLAlchemyError

    import src.services.dial_list.repository as repo
    from src.services.dial_list.delivery import generate_and_deliver

    _install_fakes(monkeypatch)
    cached = _dial_list([_entry()], from_cache=True)

    def _boom(*a, **k):
        raise SQLAlchemyError("db down")

    monkeypatch.setattr(repo, "generate_dial_list", _boom)
    monkeypatch.setattr(repo, "load_latest_dial_list_snapshot",
                        lambda session, county_id=None: cached)

    session = pytest.importorskip("unittest.mock").MagicMock()
    dial_list, ts = generate_and_deliver(session, as_of=AS_OF)
    assert dial_list.from_cache is True
    assert ts == "1700000000.0001"  # posted the cached list


def test_cached_fallback_rolls_back_failed_transaction(monkeypatch):
    from sqlalchemy.exc import SQLAlchemyError
    import src.services.dial_list.repository as repo
    from src.services.dial_list.delivery import generate_and_deliver

    _install_fakes(monkeypatch)
    cached = _dial_list([_entry()], from_cache=True)
    session = pytest.importorskip("unittest.mock").MagicMock()
    monkeypatch.setattr(repo, "generate_dial_list", lambda *a, **k: (_ for _ in ()).throw(SQLAlchemyError("db down")))
    monkeypatch.setattr(repo, "load_latest_dial_list_snapshot", lambda *a, **k: cached)

    generate_and_deliver(session, as_of=AS_OF)
    session.rollback.assert_called_once()


def test_generate_and_deliver_reraises_when_no_cache(monkeypatch):
    from sqlalchemy.exc import SQLAlchemyError

    import src.services.dial_list.repository as repo
    from src.services.dial_list.delivery import generate_and_deliver

    _install_fakes(monkeypatch)

    def _boom(*a, **k):
        raise SQLAlchemyError("db down")

    monkeypatch.setattr(repo, "generate_dial_list", _boom)
    monkeypatch.setattr(repo, "load_latest_dial_list_snapshot",
                        lambda session, county_id=None: None)

    with pytest.raises(SQLAlchemyError):
        generate_and_deliver(pytest.importorskip("unittest.mock").MagicMock(), as_of=AS_OF)


def test_generate_and_deliver_snapshots_on_success(monkeypatch):
    import src.services.dial_list.repository as repo
    from src.services.dial_list.delivery import generate_and_deliver

    _install_fakes(monkeypatch)
    fresh = _dial_list([_entry()])
    written = {}

    monkeypatch.setattr(repo, "generate_dial_list",
                        lambda session, **k: fresh)
    monkeypatch.setattr(repo, "write_dial_list_snapshot",
                        lambda session, dl, county_id=None: written.update(dl=dl))

    dial_list, ts = generate_and_deliver(object(), as_of=AS_OF)
    assert written["dl"] is fresh  # success path snapshots the fresh list
    assert ts == "1700000000.0001"
