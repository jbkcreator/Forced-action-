"""R1 1.5: record_inbound, the one write path into lending.inbox_events."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.lending.inbox import unified_triage
from src.lending.inbox.unified_triage import InboundItem, record_inbound
from src.services.phone_utils import normalize


class Db:
    """Returns id 1 for the first insert of a dedup key and None for a repeat (ON CONFLICT DO NOTHING)."""

    def __init__(self):
        self.seen, self.params = set(), []

    def execute(self, stmt, params=None):
        self.params.append(params)
        key = params["dedup_key"]
        first = key not in self.seen
        self.seen.add(key)
        return SimpleNamespace(scalar_one_or_none=lambda: 1 if first else None)


@pytest.fixture(autouse=True)
def enabled(monkeypatch):
    monkeypatch.setattr(unified_triage, "get_settings", lambda: SimpleNamespace(lending_unified_inbox_enabled=True))


def item(**overrides):
    base = dict(source="ghl", channel="ghl_sms", dedup_key="ghl_msg:m1", phone="(813) 555-0101", body="hi")
    base.update(overrides)
    return InboundItem(**base)


def test_record_inbound_is_idempotent_per_dedup_key():
    db = Db()
    assert record_inbound(db, item()) == 1
    assert record_inbound(db, item()) is None


def test_record_inbound_does_nothing_while_disabled(monkeypatch):
    monkeypatch.setattr(unified_triage, "get_settings", lambda: SimpleNamespace(lending_unified_inbox_enabled=False))
    db = Db()
    assert record_inbound(db, item()) is None and db.params == []


@pytest.mark.parametrize("bad", [dict(channel="whatsapp"), dict(source="hubspot"), dict(dedup_key=" "),
                                 dict(phone=None, email=None, ghl_contact_id=None)])
def test_record_inbound_refuses_a_bad_item(bad):
    with pytest.raises(ValueError):
        record_inbound(Db(), item(**bad))


def test_record_inbound_normalizes_phone_and_email_and_truncates():
    db = Db()
    record_inbound(db, item(email="A@B.com", subject="s" * 900))
    params = db.params[0]
    assert params["phone"] == normalize("(813) 555-0101") and params["email"] == "a@b.com"
    assert len(params["subject"]) == 500


def test_a_stop_item_is_stored_done_with_intent_stop():
    db = Db()
    record_inbound(db, item(stop=True))
    assert db.params[0]["status"] == "done" and db.params[0]["intent"] == "stop"
    db2 = Db()
    record_inbound(db2, item())
    assert db2.params[0]["status"] == "new" and db2.params[0]["intent"] is None
