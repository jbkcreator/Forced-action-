"""14-day follow-up call tracking: GHL calls recorded in call_dispositions, the replacement rule, and
dialer-only jobs leaving GHL rows alone (Runbook § 4.1)."""
from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from src.lending.ghl_call_import import _call_row, import_calls
from src.lending.ghl_calls import call_duration_seconds, call_messages_reader
from src.lending.replacement_eligibility import lead_attempts

ARRIVED = datetime(2026, 10, 12, 13, 0, tzinfo=timezone.utc)  # Mon 9:00 AM ET
NOW = ARRIVED + timedelta(days=6)


def _phone() -> str:
    return f"+1813555{uuid.uuid4().int % 10_000:04d}"


def _msg(message_id="m1", *, direction="outbound", added=ARRIVED + timedelta(minutes=5), status="no-answer",
         meta_extra=None) -> dict:
    meta = {"callStatus": status, **(meta_extra or {})}
    return {"id": message_id, "direction": direction, "dateAdded": added.isoformat() if added else None,
            "meta": meta, "conversationId": "conv-1"}


# ── pure parts ───────────────────────────────────────────────────────────────

def test_an_outbound_call_after_arrival_becomes_one_ghl_row():
    row = _call_row(_msg(meta_extra={"call": {"duration": 95}}, status="completed"), "c-1", "+18135550101", ARRIVED)
    assert row["call_id"] == "ghl:m1" and row["source"] == "ghl" and row["seat"] == "ghl" and row["tag"] == "ghl"
    assert row["raw_outcome"] == "ghl:completed" and row["talk"] == 95
    assert row["ended"] - row["started"] == timedelta(seconds=95)
    raw = json.loads(row["raw_event"])
    assert raw == {"source": "ghl", "message_id": "m1", "conversation_id": "conv-1", "contact_id": "c-1",
                   "call_status": "completed", "date_added": raw["date_added"]}
    assert "+1813" not in row["raw_event"]


@pytest.mark.parametrize("message", [
    _msg(direction="internal"),
    _msg(added=ARRIVED - timedelta(minutes=1)),
    {**_msg(), "id": None},
    {**_msg(), "dateAdded": "2026-10-12T13:05:00"},  # no timezone
    {**_msg(), "dateAdded": "not-a-date"},
])
def test_calls_that_are_not_attempts_after_arrival_are_skipped(message):
    assert _call_row(message, "c-1", "+18135550101", ARRIVED) is None


def test_an_inbound_call_becomes_an_inbound_ghl_row():
    answered = _call_row(_msg("in-1", direction="inbound", status="completed", meta_extra={"call": {"duration": 40}}),
                         "c-1", "+18135550101", ARRIVED)
    missed = _call_row(_msg("in-2", direction="inbound", status="no-answer"), "c-1", "+18135550101", ARRIVED)
    assert (answered["direction"], answered["raw_outcome"], answered["talk"]) == ("inbound", "ghl:completed", 40)
    assert (missed["direction"], missed["raw_outcome"], missed["talk"]) == ("inbound", "ghl:no-answer", None)
    assert _call_row(_msg(), "c-1", "+18135550101", ARRIVED)["direction"] == "outbound"


def test_an_unknown_outcome_and_no_duration_are_kept_as_unknown():
    row = _call_row({**_msg(), "meta": {}}, "c-1", "+18135550101", ARRIVED)
    assert row["raw_outcome"] == "ghl:unknown" and row["talk"] is None and row["ended"] == row["started"]


@pytest.mark.parametrize("meta, expected", [
    ({"call": {"duration": 30}}, 30), ({"callDuration": "45"}, 45), ({}, None), ({"callDuration": "x"}, None),
    ({"call": {"duration": -1}}, None),
])
def test_call_duration_reads_both_shapes(meta, expected):
    assert call_duration_seconds({"meta": meta}) == expected


# The shape of a real GHL call message (2026-10-19, an outbound call that failed to connect).
LIVE_FAILED_CALL = {"id": "gfcmdjlAyZLqbTeGAY60", "direction": "outbound", "dateAdded": "2026-10-19T08:56:30.307Z",
                    "status": "failed", "meta": {"call": {"status": "failed"}}, "conversationId": "conv-1"}


@pytest.mark.parametrize("message, expected", [
    (LIVE_FAILED_CALL, "failed"),
    ({"meta": {"call": {"status": "completed"}}}, "completed"),
    ({"meta": {"callStatus": "no-answer"}}, "no-answer"),
    ({"callStatus": "busy"}, "busy"),
    ({"status": "answered"}, "answered"),
    ({}, None),
])
def test_call_state_reads_the_live_and_documented_shapes(message, expected):
    from src.lending.ghl_calls import call_state

    assert call_state(message) == expected


def test_a_live_shaped_call_is_stored_with_its_real_outcome():
    row = _call_row(LIVE_FAILED_CALL, "c-1", "+18135550101", datetime(2026, 10, 19, 8, 0, tzinfo=timezone.utc))
    assert row["call_id"] == "ghl:gfcmdjlAyZLqbTeGAY60" and row["raw_outcome"] == "ghl:failed"


def test_the_uncalled_alarm_sees_a_live_shaped_answered_call_as_connected():
    from src.lending.uncalled_alarm_worker import CONNECTED, ghl_call_lookup

    answered = {**LIVE_FAILED_CALL, "status": "completed", "meta": {"call": {"status": "completed"}}}
    lookup = ghl_call_lookup(MagicMock(api_key="key", location_id="loc"),
                             http=_fake_http({"conversations": [{"id": "conv-1"}]}, {"messages": [answered]}))
    assert lookup("contact-1", datetime(2026, 10, 19, 8, 0, tzinfo=timezone.utc)) == CONNECTED


def _fake_http(search_body, messages_body):
    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    return lambda url, **kwargs: Response(search_body if url.endswith("/conversations/search") else messages_body)


def test_the_reader_tags_each_message_with_its_conversation():
    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    def http(url, **kwargs):
        if url.endswith("/conversations/search"):
            return Response({"conversations": [{"id": "conv-a"}, {"id": "conv-b"}]})
        if "/conv-a/" in url:
            return Response({"messages": {"messages": [{"id": "1"}, "junk"]}})
        return Response({"messages": [{"id": "2", "conversationId": "conv-own"}]})

    account = MagicMock(api_key="key", location_id="loc")
    messages = call_messages_reader(account, http=http)("contact-1")
    assert [(m["id"], m["conversationId"]) for m in messages] == [("1", "conv-a"), ("2", "conv-own")]


def _paged_http(pages: dict, requests: list):
    """GHL's message pages keyed by the lastMessageId cursor sent (None = first page); records each request."""
    class Response:
        def __init__(self, body):
            self.body = body

        def raise_for_status(self):
            pass

        def json(self):
            return self.body

    def http(url, **kwargs):
        if url.endswith("/conversations/search"):
            return Response({"conversations": [{"id": "conv-a"}]})
        cursor = kwargs["params"].get("lastMessageId")
        requests.append(cursor)
        return Response(pages[cursor])

    return http


def test_the_reader_follows_every_page_of_a_conversation():
    first = [{"id": f"m{i}"} for i in range(20)]  # GHL's default page size
    pages = {None: {"messages": {"messages": first, "nextPage": True, "lastMessageId": "m19"}},
             "m19": {"messages": {"messages": [{"id": "m20"}], "nextPage": False, "lastMessageId": "m20"}}}
    requests: list = []
    messages = call_messages_reader(MagicMock(api_key="k", location_id="loc"), http=_paged_http(pages, requests))("c-1")
    assert len(messages) == 21 and messages[-1]["id"] == "m20"
    assert requests == [None, "m19"]  # the second page was asked for with the cursor


def test_the_reader_stops_on_a_cursor_that_does_not_move():
    pages = {None: {"messages": {"messages": [{"id": "m1"}], "nextPage": True, "lastMessageId": "m1"}},
             "m1": {"messages": {"messages": [{"id": "m1"}], "nextPage": True, "lastMessageId": "m1"}}}
    requests: list = []
    call_messages_reader(MagicMock(api_key="k", location_id="loc"), http=_paged_http(pages, requests))("c-1")
    assert requests == [None, "m1"]


def test_the_reader_stops_at_the_page_cap(monkeypatch):
    from src.lending import ghl_calls

    monkeypatch.setattr(ghl_calls, "MAX_MESSAGE_PAGES", 3)

    class Endless(dict):
        def __missing__(self, cursor):
            n = int(cursor or 0) + 1
            return {"messages": {"messages": [{"id": str(n)}], "nextPage": True, "lastMessageId": str(n)}}

    requests: list = []
    messages = call_messages_reader(MagicMock(api_key="k", location_id="loc"),
                                    http=_paged_http(Endless(), requests))("c-1")
    assert len(requests) == 3 and len(messages) == 3


def test_successive_runs_cover_every_slice_of_the_eligible_contacts():
    from src.lending.ghl_call_import import rotation_offset

    runs = [NOW + timedelta(minutes=15 * n) for n in range(4)]
    offsets = [rotation_offset(201, 200, run) for run in runs]
    assert set(offsets) == {0, 200}  # 201 contacts: the oldest one is read every second run
    assert offsets[0] != offsets[1]
    assert {rotation_offset(450, 200, NOW + timedelta(minutes=15 * n)) for n in range(3)} == {0, 200, 400}
    assert rotation_offset(200, 200, NOW) == 0 and rotation_offset(0, 200, NOW) == 0


# ── against the database (rolled back) ──────────────────────────────────────

def _lendingflow_lead(db, *, phone, contact_id, received=ARRIVED) -> int:
    unique = uuid.uuid4().hex
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, received_at, phone, "
        "ghl_contact_id) VALUES (:v, :h, '{}'::jsonb, :r, :p, :c) RETURNING id"),
        {"v": f"followup-{unique}", "h": unique + unique, "r": received, "p": phone, "c": contact_id}).scalar()


def _web_lead(db, *, phone, contact_id, received=ARRIVED) -> int:
    return db.execute(text(
        "INSERT INTO lending.web_leads (received_at, name, phone, ghl_contact_id) VALUES (:r, 'Test Lead', :p, :c) "
        "RETURNING id"), {"r": received, "p": phone, "c": contact_id}).scalar()


def _bd_call(db, *, phone, started, talk=0) -> None:
    """A BatchDialer call row (call_source defaults to batchdialer)."""
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, call_ended_at, "
        "talk_duration_sec, raw_event) VALUES (:id, 'outbound', :p, :s, :s, :t, '{}')"),
        {"id": f"followup-bd-{uuid.uuid4().hex}", "p": phone, "s": started, "t": talk})


def _ghl_rows(db, phone):
    return db.execute(text("SELECT dialer_call_id, direction, call_source, caller_seat, campaign_tag, disposition, "
                           "disposition_raw, call_started_at FROM lending.call_dispositions "
                           "WHERE phone = :p AND call_source = 'ghl' ORDER BY call_started_at"),
                      {"p": phone}).mappings().all()


def test_ghl_calls_of_a_lead_in_its_window_are_stored_once(lending_db):
    phone, contact = _phone(), f"c-{uuid.uuid4().hex[:8]}"
    _lendingflow_lead(lending_db, phone=phone, contact_id=contact)
    messages = {contact: [_msg(f"{contact}-1"), _msg(f"{contact}-2", added=ARRIVED + timedelta(days=1),
                                                       status="completed"),
                          _msg(f"{contact}-in", direction="inbound", added=ARRIVED + timedelta(hours=2),
                               status="completed")]}
    first = import_calls(lending_db, lambda cid: messages.get(cid, []), now=NOW)
    again = import_calls(lending_db, lambda cid: messages.get(cid, []), now=NOW)
    rows = _ghl_rows(lending_db, phone)
    assert [r["dialer_call_id"] for r in rows] == [f"ghl:{contact}-1", f"ghl:{contact}-in", f"ghl:{contact}-2"]
    assert [r["direction"] for r in rows] == ["outbound", "inbound", "outbound"]
    assert all(r["caller_seat"] == "ghl" and r["campaign_tag"] == "ghl" and r["disposition"] is None for r in rows)
    assert rows[2]["disposition_raw"] == "ghl:completed"
    assert first.imported >= 3 and again.imported == 0 and again.already_recorded >= 3


def test_inbound_calls_are_stored_but_never_count_as_outbound_attempts(lending_db):
    from src.lending.compliance import MAX_ATTEMPTS_PER_PERIOD, _attempt_cap

    phone, contact = _phone(), f"c-{uuid.uuid4().hex[:8]}"
    _lendingflow_lead(lending_db, phone=phone, contact_id=contact)
    calls = [_msg(f"{contact}-in{n}", direction="inbound", added=NOW - timedelta(hours=1, minutes=n),
                  status="completed" if n % 2 else "no-answer") for n in range(MAX_ATTEMPTS_PER_PERIOD + 2)]
    import_calls(lending_db, lambda cid: calls if cid == contact else [], now=NOW)
    rows = _ghl_rows(lending_db, phone)
    assert len(rows) == MAX_ATTEMPTS_PER_PERIOD + 2 and {r["direction"] for r in rows} == {"inbound"}
    assert _attempt_cap(lending_db, phone, NOW).allowed  # more inbound calls than the cap, still dialable


def test_a_contact_with_a_lendingflow_and_a_web_lead_is_read_once(lending_db):
    phone, contact = _phone(), f"c-{uuid.uuid4().hex[:8]}"
    _lendingflow_lead(lending_db, phone=phone, contact_id=contact)
    _web_lead(lending_db, phone=phone, contact_id=contact, received=ARRIVED + timedelta(hours=2))
    reads: list[str] = []

    def read(cid):
        reads.append(cid)
        return [_msg(f"{contact}-1")] if cid == contact else []

    import_calls(lending_db, read, now=NOW)
    assert reads.count(contact) == 1 and len(_ghl_rows(lending_db, phone)) == 1


def test_a_lead_outside_the_window_is_not_read(lending_db):
    phone, contact = _phone(), f"c-{uuid.uuid4().hex[:8]}"
    _lendingflow_lead(lending_db, phone=phone, contact_id=contact, received=NOW - timedelta(days=30))
    reads: list[str] = []
    import_calls(lending_db, lambda cid: reads.append(cid) or [], now=NOW)
    assert contact not in reads


def test_a_failed_lookup_skips_only_that_contact(lending_db):
    broken, working = f"c-{uuid.uuid4().hex[:8]}", f"c-{uuid.uuid4().hex[:8]}"
    working_phone = _phone()
    _lendingflow_lead(lending_db, phone=_phone(), contact_id=broken)
    _lendingflow_lead(lending_db, phone=working_phone, contact_id=working)

    def read(cid):
        if cid == broken:
            raise ConnectionError("ghl down")
        return [_msg(f"{working}-1")] if cid == working else []

    stats = import_calls(lending_db, read, now=NOW)
    assert stats.lookup_failures >= 1 and len(_ghl_rows(lending_db, working_phone)) == 1


def test_successive_runs_read_every_contact_when_there_are_more_than_one_run_reads(lending_db):
    contacts = [f"c-{uuid.uuid4().hex[:8]}" for _ in range(3)]
    for n, contact in enumerate(contacts):
        _lendingflow_lead(lending_db, phone=_phone(), contact_id=contact, received=ARRIVED + timedelta(minutes=n))
    reads: set[str] = set()
    first = import_calls(lending_db, lambda cid: reads.add(cid) or [], now=NOW, limit=2)
    runs = -(-first.eligible // 2)  # the shared database can hold other leads in the window too
    for n in range(1, runs):
        import_calls(lending_db, lambda cid: reads.add(cid) or [], now=NOW + timedelta(minutes=15 * n), limit=2)
    assert set(contacts) <= reads


def test_a_call_imported_in_progress_gains_its_final_outcome_and_talk_time(lending_db):
    phone, contact = _phone(), f"c-{uuid.uuid4().hex[:8]}"
    _lendingflow_lead(lending_db, phone=phone, contact_id=contact)
    in_progress = _msg(f"{contact}-1", status="in-progress")
    completed = _msg(f"{contact}-1", status="completed", meta_extra={"call": {"duration": 180}})
    unknown = {**_msg(f"{contact}-1"), "meta": {}}

    import_calls(lending_db, lambda cid: [in_progress] if cid == contact else [], now=NOW)
    later = import_calls(lending_db, lambda cid: [completed] if cid == contact else [], now=NOW)
    again = import_calls(lending_db, lambda cid: [unknown] if cid == contact else [], now=NOW)

    row = lending_db.execute(text("SELECT disposition_raw, talk_duration_sec, call_ended_at - call_started_at AS span "
                                  "FROM lending.call_dispositions WHERE dialer_call_id = :id"),
                             {"id": f"ghl:{contact}-1"}).mappings().one()
    assert (row["disposition_raw"], row["talk_duration_sec"], row["span"]) == (
        "ghl:completed", 180, timedelta(seconds=180))
    assert later.updated >= 1 and again.updated == 0  # an unknown outcome never erases a known one
    assert len(_ghl_rows(lending_db, phone)) == 1


def test_a_dialer_row_with_the_same_id_is_never_overwritten(lending_db):
    phone, contact = _phone(), f"c-{uuid.uuid4().hex[:8]}"
    _lendingflow_lead(lending_db, phone=phone, contact_id=contact)
    lending_db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, call_ended_at, "
        "disposition_raw, raw_event) VALUES (:id, 'outbound', :p, :s, :s, 'dialer', '{}')"),
        {"id": f"ghl:{contact}-1", "p": phone, "s": ARRIVED})
    import_calls(lending_db, lambda cid: [_msg(f"{contact}-1", status="completed")] if cid == contact else [], now=NOW)
    assert lending_db.execute(text("SELECT disposition_raw FROM lending.call_dispositions WHERE dialer_call_id = :id"),
                              {"id": f"ghl:{contact}-1"}).scalar() == "dialer"


def _eligibility(db, lead_id):
    (lead,) = lead_attempts(db, [lead_id])
    return lead


def test_five_attempts_on_four_days_is_not_eligible(lending_db):
    phone = _phone()
    lead = _lendingflow_lead(lending_db, phone=phone, contact_id=None)
    for day, hour in [(0, 1), (0, 3), (1, 1), (2, 1), (3, 1)]:
        _bd_call(lending_db, phone=phone, started=ARRIVED + timedelta(days=day, hours=hour))
    row = _eligibility(lending_db, lead)
    assert (row.attempts, row.call_days, row.eligible) == (5, 4, False)


def test_five_attempts_on_five_days_from_both_sources_is_eligible(lending_db):
    phone, contact = _phone(), f"c-{uuid.uuid4().hex[:8]}"
    lead = _lendingflow_lead(lending_db, phone=phone, contact_id=contact)
    for day in range(3):
        _bd_call(lending_db, phone=phone, started=ARRIVED + timedelta(days=day, hours=1), talk=40 if day == 0 else 0)
    ghl = [_msg(f"{contact}-{day}", added=ARRIVED + timedelta(days=day, hours=2)) for day in (3, 4)]
    import_calls(lending_db, lambda cid: ghl if cid == contact else [], now=NOW)
    row = _eligibility(lending_db, lead)
    assert (row.attempts, row.call_days, row.ghl_attempts, row.connected, row.eligible) == (5, 5, 2, 1, True)


def test_calls_before_arrival_or_to_another_phone_do_not_count(lending_db):
    phone = _phone()
    lead = _lendingflow_lead(lending_db, phone=phone, contact_id=None)
    _bd_call(lending_db, phone=phone, started=ARRIVED - timedelta(hours=1))
    _bd_call(lending_db, phone=_phone(), started=ARRIVED + timedelta(hours=1))
    row = _eligibility(lending_db, lead)
    assert (row.attempts, row.eligible) == (0, False)


def test_call_days_are_counted_on_the_eastern_date(lending_db):
    phone = _phone()
    lead = _lendingflow_lead(lending_db, phone=phone, contact_id=None)
    # 11 PM ET Monday and 1 AM ET Tuesday: two UTC-same-day calls on two Eastern dates.
    _bd_call(lending_db, phone=phone, started=datetime(2026, 10, 13, 3, 0, tzinfo=timezone.utc))
    _bd_call(lending_db, phone=phone, started=datetime(2026, 10, 13, 5, 0, tzinfo=timezone.utc))
    assert _eligibility(lending_db, lead).call_days == 2


# ── dialer-only jobs leave GHL rows alone ────────────────────────────────────

@pytest.fixture
def ghl_call_row(lending_db, monkeypatch):
    """One connected GHL call with no disposition, ended an hour ago, and the dialer-only tasks on this session."""
    phone = _phone()
    call_id = f"ghl:followup-{uuid.uuid4().hex}"
    ended = datetime.now(timezone.utc) - timedelta(hours=1)
    row_id = lending_db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, talk_duration_sec, "
        "call_started_at, call_ended_at, disposition_at, raw_event) "
        "VALUES (:id, 'ghl', 'outbound', :p, 60, :e, :e, :e, '{}') RETURNING id"),
        {"id": call_id, "p": phone, "e": ended}).scalar()

    @contextmanager
    def session():
        yield lending_db

    from src.tasks import (
        lending_disposition_delivery_retry,
        lending_missing_disposition_alert,
        lending_soft_approval_card_retry,
    )
    for module in (lending_disposition_delivery_retry, lending_missing_disposition_alert,
                   lending_soft_approval_card_retry):
        monkeypatch.setattr(module, "lending_session", session)
    return row_id, call_id


def test_ghl_calls_get_no_soft_approval_card_or_delivery_retry(ghl_call_row, lending_db):
    from src.tasks.lending_disposition_delivery_retry import behind_row_ids
    from src.tasks.lending_soft_approval_card_retry import cards_missing_row_ids

    row_id, _ = ghl_call_row
    assert row_id not in cards_missing_row_ids()
    # A coded disposition never delivered to the sheet or Slack would be retried, if it were a BatchDialer call.
    lending_db.execute(text("UPDATE lending.call_dispositions SET disposition = 'NO_ANSWER' WHERE id = :id"),
                       {"id": row_id})
    assert row_id not in behind_row_ids()


def test_ghl_calls_never_raise_the_missing_disposition_alert(ghl_call_row, lending_db):
    from src.tasks.lending_missing_disposition_alert import run

    row_id, _ = ghl_call_row
    lending_db.execute(text("UPDATE lending.call_dispositions SET disposition_raw = NULL WHERE id = :id"),
                       {"id": row_id})  # even without GHL's outcome, the source alone keeps it out
    run(slack_client=MagicMock(), minutes=1)
    alerted = lending_db.execute(text("SELECT disposition_missing_alerted_at FROM lending.call_dispositions "
                                      "WHERE id = :id"), {"id": row_id}).scalar()
    assert alerted is None
