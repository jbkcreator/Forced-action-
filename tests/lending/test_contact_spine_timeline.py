"""R1 1.1 unified timeline: local and live touches, each once, newest first; a live outage is named, never hidden."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.spine.contact_spine import sync_contact_spine
from src.lending.spine.timeline import timeline
from src.lending.spine.touches import GhlTouches, InstantlyTouches, contact_keys
from tests.lending.contact_spine_db import spine_session

NOW = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
PHONE = "+18135550101"


def _at(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


@pytest.fixture
def db():
    with spine_session() as session:
        session.execute(text("INSERT INTO lending.lendingflow_leads (id, first_name, phone, email, ghl_contact_id) "
                             "VALUES (1, 'Ann', '8135550101', 'ann@x.com', 'G1')"))
        sync_contact_spine(session, now=NOW)
        yield session


def _contact(db) -> int:
    return db.execute(text("SELECT contact_id FROM lending.contact_sources WHERE source = 'lendingflow_leads'")).scalar()


def _ghl(messages=(), notes=(), account=object()):
    return GhlTouches(account, read_messages=lambda cid: list(messages), read_notes=lambda cid: list(notes), now=NOW)


class _Raising:
    system = "ghl"
    configured = True

    def touches(self, keys):
        raise ConnectionError("ghl down")


def test_contact_keys_gather_phones_emails_and_ids(db):
    keys = contact_keys(db, _contact(db))
    assert keys.phones == (PHONE,)
    assert keys.emails == ("ann@x.com",)
    assert keys.ghl_contact_ids == ("G1",)
    assert keys.lendingflow_lead_ids == (1,)


def test_same_touch_from_local_and_ghl_appears_once(db):
    db.execute(text("INSERT INTO lending.pending_actions (id, channel, status, provider_ref, recipient_phone, executed_at)"
                    " VALUES (1, 'ghl_sms', 'sent', 'm1', :p, :at)"), {"p": PHONE, "at": _at(2)})
    ghl = _ghl([{"id": "m1", "messageType": "TYPE_SMS", "direction": "outbound", "dateAdded": _at(2), "body": "hi"}])
    result = timeline(db, _contact(db), remote=[ghl])
    assert [(t.ref, t.system, t.kind) for t in result.touches] == [("ghl_msg:m1", "ghl", "text")]
    assert result.unavailable == ()


def test_ghl_call_messages_are_left_to_call_dispositions(db):
    db.execute(text("INSERT INTO lending.call_dispositions (id, dialer_call_id, phone, direction, disposition, "
                    "call_source, call_started_at) VALUES (1, 'ghl:c9', :p, 'outbound', 'NA', 'ghl', :at)"),
               {"p": PHONE, "at": _at(1)})
    ghl = _ghl([{"id": "c9", "messageType": "TYPE_CALL", "dateAdded": _at(1)}])
    touches = timeline(db, _contact(db), remote=[ghl]).touches
    assert [(t.ref, t.kind, t.outcome) for t in touches] == [("call:ghl:c9", "call", "NA")]


def test_instantly_reply_and_api_email_dedupe_on_email_id(db):
    db.execute(text("INSERT INTO lending.instantly_replies (id, dedup_key, lead_email, intent, received_at) "
                    "VALUES (1, 'reply:e7', 'ANN@x.com', 'deal', :at)"), {"at": _at(3)})
    instantly = InstantlyTouches(object(), read_emails=lambda email: [
        {"id": "e7", "ue_type": 2, "timestamp_email": _at(3)},
        {"id": "e6", "ue_type": 1, "timestamp_email": _at(30)}])
    touches = timeline(db, _contact(db), remote=[instantly]).touches
    assert [(t.ref, t.direction) for t in touches] == [("instantly:e7", "inbound"), ("instantly:e6", "outbound")]


def test_ghl_down_returns_local_touches_and_says_so(db):
    db.execute(text("INSERT INTO lending.ghl_stage_events (id, phone, stage_name, event_at) VALUES (1, :p, 'Booked', :at)"),
               {"p": PHONE, "at": _at(5)})
    result = timeline(db, _contact(db), remote=[_Raising()])
    assert result.unavailable == ("ghl",)
    assert [(t.kind, t.outcome) for t in result.touches] == [("stage", "Booked")]


def test_unconfigured_instantly_is_listed_unavailable(db):
    def never(email):
        raise AssertionError("called without an account")

    result = timeline(db, _contact(db), remote=[InstantlyTouches(None, read_emails=never)])
    assert result.unavailable == ("instantly",)


def test_touches_newest_first_and_capped(db):
    db.execute(text("INSERT INTO lending.file_routing_events (id, lead_id, action, created_at) VALUES "
                    "(1, 1, 'routed', :a), (2, 1, 'declined', :b), (3, 1, 'funded', :c)"),
               {"a": _at(9), "b": _at(1), "c": _at(5)})
    db.execute(text("INSERT INTO lending.missed_call_events (id, phone, status, created_at) VALUES (1, :p, 'sent', NULL)"),
               {"p": PHONE})
    touches = timeline(db, _contact(db), remote=[], limit=3).touches
    assert [t.outcome for t in touches] == ["declined", "funded", "routed"]
    assert len(timeline(db, _contact(db), remote=[]).touches) == 4  # the undated text sorts last


def test_old_ghl_messages_are_dropped(db):
    ghl = _ghl([{"id": "old", "messageType": "TYPE_SMS", "dateAdded": _at(24 * 400)},
                {"id": "new", "messageType": "TYPE_EMAIL", "dateAdded": _at(1)}],
               notes=[{"id": "n1", "dateAdded": _at(2), "body": "secret"}])
    refs = [t.ref for t in timeline(db, _contact(db), remote=[ghl]).touches]
    assert refs == ["ghl_msg:new", "ghl_note:n1"]


def test_no_message_text_or_full_phone_in_touches(db):
    db.execute(text("INSERT INTO lending.call_dispositions (id, dialer_call_id, phone, call_started_at) "
                    "VALUES (1, 'bd1', :p, :at)"), {"p": PHONE, "at": _at(1)})
    ghl = _ghl([{"id": "m2", "messageType": "TYPE_SMS", "dateAdded": _at(1), "body": "call me at 8135550101",
                 "status": "delivered"}], notes=[{"id": "n2", "dateAdded": _at(1), "body": "private note"}])
    dumped = repr(timeline(db, _contact(db), remote=[ghl]))
    for secret in ("call me", "private note", "8135550101"):
        assert secret not in dumped
