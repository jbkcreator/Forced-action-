"""R1 1.1 contact card: one borrower seen in GHL, BatchDialer and Instantly is one card with every touch once."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending import cora_tools
from src.lending.cora_tools import ContactCardInput, CoraToolkit
from src.lending.spine.card import contact_card, find_contacts
from src.lending.spine.contact_spine import sync_contact_spine
from src.lending.spine.touches import GhlTouches, InstantlyTouches
from tests.lending.contact_spine_db import spine_session

NOW = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
PHONE = "+18135550101"


def _at(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _flags(_db, phones):
    return {p: {"text_consent": True, "suppressed": False, "do_not_contact": False, "last_call": {"x": 1}}
            for p in phones}


@pytest.fixture
def db():
    with spine_session() as session:
        yield session


def _borrower(db):
    """The Done-when shape: LendingFlow lead in GHL, dialer record + BatchDialer call, Instantly reply, loan file."""
    db.execute(text("INSERT INTO lending.lendingflow_leads VALUES (1, 'Ann', 'Lee', '8135550101', 'ann@x.com', 'G1')"))
    db.execute(text("INSERT INTO lending.dialer_load_records VALUES (2, '8135550101', 'A LEE', 'Test Holdings LLC', "
                    "NULL, 'BD9')"))
    db.execute(text("INSERT INTO lending.call_dispositions (id, dialer_call_id, phone, dialer_contact_id, direction, "
                    "disposition, call_source, call_started_at) VALUES "
                    "(1, 'bd-1', '+18135550101', 'BD9', 'outbound', 'CONNECTED', 'batchdialer', :at)"), {"at": _at(4)})
    db.execute(text("INSERT INTO lending.instantly_replies (id, dedup_key, lead_email, intent, received_at) "
                    "VALUES (1, 'reply:e1', 'ann@x.com', 'deal', :at)"), {"at": _at(3)})
    db.execute(text("INSERT INTO lending.entities VALUES (5, 'TEST HOLDINGS, LLC')"))
    db.execute(text("INSERT INTO lending.loan_files VALUES (8, 'Ann', 'Lee', NULL, NULL, NULL, 5)"))
    db.execute(text("INSERT INTO lending.contacts (id, phone, line_type) VALUES (3, '+18135550101', 'mobile')"))
    sync_contact_spine(db, now=NOW)


def _remote():
    ghl = GhlTouches(object(), now=NOW, read_notes=lambda cid: [], read_messages=lambda cid: [
        {"id": "m1", "messageType": "TYPE_SMS", "direction": "outbound", "dateAdded": _at(2)}])
    instantly = InstantlyTouches(object(), read_emails=lambda email: [
        {"id": "e1", "ue_type": 2, "timestamp_email": _at(3)}])
    return [ghl, instantly]


def test_card_merges_a_borrower_seen_in_ghl_batchdialer_and_instantly(db):
    _borrower(db)
    (contact_id,) = find_contacts(db, "(813) 555-0101")
    card = contact_card(db, contact_id, remote=_remote(), flags=_flags)
    assert card["display_name"] == "Ann Lee"
    assert card["systems"] == {"ghl": True, "batchdialer": True, "instantly": True}
    assert card["phones"] == [{"last4": "…0101", "line_type": "mobile", "text_consent": True, "suppressed": False,
                               "do_not_contact": False}]
    assert card["emails"] == ["ann@x.com"]
    assert card["entities"] == ["test holdings"]
    assert set(card["sources"]) >= {"lendingflow_lead:1", "loan_file:8", "contact:3", "dialer_load_record:2"}
    assert [(t["system"], t["kind"]) for t in card["timeline"]] == [
        ("ghl", "text"), ("instantly", "email"), ("batchdialer", "call")]
    assert card["unavailable"] == []


def test_lookup_by_email_entity_and_lead_ref_find_the_same_contact(db):
    _borrower(db)
    (contact_id,) = find_contacts(db, "ANN@x.com")
    assert find_contacts(db, "lendingflow_lead:1") == [contact_id]
    assert find_contacts(db, "loan_file:8") == [contact_id]
    assert find_contacts(db, "ann lee") == [contact_id]


def test_lookup_by_old_merged_id_returns_survivor_card(db):
    db.execute(text("INSERT INTO lending.web_leads VALUES (1, 'Bo', '8135550102', NULL, NULL)"))
    db.execute(text("INSERT INTO lending.web_leads VALUES (2, 'Bo', NULL, 'bo@x.com', NULL)"))
    sync_contact_spine(db, now=NOW)
    old = db.execute(text("SELECT max(contact_id) FROM lending.contact_sources")).scalar()
    db.execute(text("INSERT INTO lending.lendingflow_leads VALUES (9, 'Bo', 'Ray', '8135550102', 'bo@x.com', NULL)"))
    sync_contact_spine(db, now=NOW)
    card = contact_card(db, old, remote=[], flags=_flags)
    assert card["contact_id"] != old
    assert card["display_name"] == "Bo Ray"


def test_name_query_with_two_people_returns_matches_not_a_card(db):
    db.execute(text("INSERT INTO lending.web_leads VALUES (1, 'Sam Smith', '8135550102', NULL, NULL)"))
    db.execute(text("INSERT INTO lending.web_leads VALUES (2, 'Sam Smithers', '8135550103', NULL, NULL)"))
    sync_contact_spine(db, now=NOW)
    assert len(find_contacts(db, "sam smith")) == 2


def test_card_shows_last4_only(db):
    _borrower(db)
    (contact_id,) = find_contacts(db, PHONE)
    dumped = repr(contact_card(db, contact_id, remote=_remote(), flags=_flags))
    assert "8135550101" not in dumped and "last_call" not in dumped


def test_unknown_contact_has_no_card(db):
    assert contact_card(db, 404, remote=[], flags=_flags) is None
    assert find_contacts(db, "nobody") == []


def _toolkit_on(monkeypatch, db, remote):
    toolkit = CoraToolkit(engine=None, memory=None, contact_sources=lambda: remote)  # type: ignore[arg-type]

    @contextmanager
    def same_session(db_role=None):
        yield db

    monkeypatch.setattr(toolkit, "_read_only", same_session)
    monkeypatch.setattr(cora_tools, "phone_flags", _flags)
    return toolkit


def test_tool_returns_one_card(monkeypatch, db):
    _borrower(db)
    toolkit = _toolkit_on(monkeypatch, db, _remote())
    card = toolkit.contact_card(ContactCardInput(query="ann@x.com"), None)
    assert card["systems"] == {"ghl": True, "batchdialer": True, "instantly": True}
    assert len(card["timeline"]) == 3


def test_tool_lists_matches_when_the_query_is_ambiguous(monkeypatch, db):
    db.execute(text("INSERT INTO lending.web_leads VALUES (1, 'Sam Smith', '8135550102', NULL, NULL)"))
    db.execute(text("INSERT INTO lending.web_leads VALUES (2, 'Sam Smithers', '8135550103', NULL, NULL)"))
    sync_contact_spine(db, now=NOW)
    result = _toolkit_on(monkeypatch, db, []).contact_card(ContactCardInput(query="sam smith"), None)
    assert result["note"] == "pick one"
    assert sorted(m["phones_last4"][0] for m in result["matches"]) == ["…0102", "…0103"]
    assert _toolkit_on(monkeypatch, db, []).contact_card(ContactCardInput(query="nobody"), None) == {
        "matches": [], "note": "no contact matched"}


def test_tool_is_registered_read_only():
    tools = {t.name: t for t in CoraToolkit(engine=None, memory=None).tools()}  # type: ignore[arg-type]
    assert tools["get_contact_card"].safety is cora_tools.SafetyLevel.READ_ONLY
    with pytest.raises(ValueError):
        ContactCardInput(query="ab")
    with pytest.raises(ValueError):
        ContactCardInput(query="ann", limit=500)
