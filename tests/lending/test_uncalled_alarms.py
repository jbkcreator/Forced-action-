"""T-10 uncalled-lead alarms: pickup, call classification (CDR + GHL), once-only alarms, wording."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from migrations.apply_lending_uncalled_alarms import apply_to

ARRIVED = datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)
LEAD_PHONE = "+17275550100"
JOSH = "+18135550199"
OPS = "C-OPS"

# Used only when T-11's real table is absent. The shared DB already has it (with rows), so CREATE IF NOT EXISTS
# is a no-op there and lead() fills the real table's NOT NULL columns.
_T11_LEADS = """
CREATE TABLE IF NOT EXISTS lending.lendingflow_leads (
    id SERIAL PRIMARY KEY,
    lead_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    vendor_lead_id VARCHAR(100) NOT NULL UNIQUE,
    dedupe_hash VARCHAR(64) NOT NULL UNIQUE,
    raw_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_name VARCHAR(80),
    phone VARCHAR(20) NOT NULL,
    property_state VARCHAR(20),
    suppressed BOOLEAN NOT NULL DEFAULT false,
    ghl_contact_id VARCHAR(64)
)"""


class Sms:
    def __init__(self, error: Exception | None = None):
        self.sent, self.error = [], error

    def __call__(self, to, body, first_name=None, *, deadline=None):
        if self.error:
            raise self.error
        self.sent.append((to, body))
        return "msg"


class Slack:
    def __init__(self, error: Exception | None = None):
        self.posts, self.error = [], error

    def chat_postMessage(self, channel, text):
        if self.error:
            raise self.error
        self.posts.append((channel, text))


@pytest.fixture
def db(lending_db):
    conn = lending_db.connection()
    conn.execute(text(_T11_LEADS))
    apply_to(conn)  # existing shared-DB leads become 'preexisting' and never alarm in tests
    return lending_db


def lead(db, *, arrived=ARRIVED, phone=LEAD_PHONE, suppressed=False, ghl_contact_id=None,
         first_name="Jane", state="FL") -> int:
    unique = uuid.uuid4().hex
    return db.execute(text(
        "INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, received_at, first_name, "
        "phone, property_state, suppressed, ghl_contact_id) "
        "VALUES (:v, :h, '{}'::jsonb, :r, :n, :p, :st, :s, :g) RETURNING id"),
        {"v": f"t10-{unique}", "h": unique + unique, "r": arrived, "n": first_name, "p": phone, "st": state,
         "s": suppressed, "g": ghl_contact_id}).scalar()


def call(db, *, started, phone=LEAD_PHONE, direction="outbound", disposition=None, talk_seconds=60, call_id=None):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, direction, phone, call_started_at, call_ended_at, "
        "disposition, talk_duration_sec, raw_event) VALUES (:id, :d, :p, :s, :s, :disp, :talk, '{}')"),
        {"id": call_id or f"t10-{uuid.uuid4().hex}", "d": direction, "p": phone, "s": started,
         "disp": disposition, "talk": talk_seconds})


def alarm(db, lead_id):
    return db.execute(text("SELECT * FROM lending.uncalled_alarms WHERE lendingflow_lead_id = :i"),
                      {"i": lead_id}).mappings().first()


def test_migration_is_idempotent(db):
    apply_to(db.connection())
    assert db.execute(text("SELECT to_regclass('lending.uncalled_alarms')")).scalar() is not None


def test_migration_marks_existing_leads_preexisting(db):
    lead_id = lead(db)
    apply_to(db.connection())  # the deploy step re-runs the migration right before the flag goes on
    row = alarm(db, lead_id)
    assert row["resolved_reason"] == "preexisting" and row["fired_120_at"] is None


from src.lending.uncalled_alarm_worker import pick_up_new_leads  # noqa: E402


def test_pickup_starts_the_stopwatch_at_received_at(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    row = alarm(db, lead_id)
    assert row["arrived_at"] == ARRIVED and row["phone"] == LEAD_PHONE and row["resolved_at"] is None


def test_pickup_twice_makes_one_alarm(db):
    lead_id = lead(db)
    pick_up_new_leads(db)
    pick_up_new_leads(db)
    assert db.execute(text("SELECT count(*) FROM lending.uncalled_alarms WHERE lendingflow_lead_id = :i"),
                      {"i": lead_id}).scalar() == 1


def test_suppressed_lead_is_never_picked_up(db):
    lead_id = lead(db, suppressed=True)
    pick_up_new_leads(db)
    assert alarm(db, lead_id) is None


def test_old_lead_is_still_picked_up(db):
    lead_id = lead(db, arrived=ARRIVED - timedelta(hours=3))
    pick_up_new_leads(db)
    assert alarm(db, lead_id)["resolved_at"] is None


from src.lending.ghl_account import GhlAccount  # noqa: E402
from src.lending.uncalled_alarm_worker import ATTEMPTED, CONNECTED, NONE, call_status, ghl_call_lookup  # noqa: E402


def _row(phone=LEAD_PHONE, ghl_contact_id=None, lead_uuid="u-1"):
    return {"phone": phone, "arrived_at": ARRIVED, "ghl_contact_id": ghl_contact_id, "lead_uuid": lead_uuid}


def test_no_calls_is_none(db):
    assert call_status(db, _row(), None) == NONE


@pytest.mark.parametrize("disposition", ["BOOKED", "CALLBACK_REQUESTED", "CONNECTED_NOT_INTERESTED", "WRONG_PERSON"])
def test_answered_disposition_is_connected(db, disposition):
    call(db, started=ARRIVED + timedelta(seconds=90), disposition=disposition)
    assert call_status(db, _row(), None) == CONNECTED


@pytest.mark.parametrize("disposition", ["NO_ANSWER", "LEFT_VOICEMAIL", "CALL_FAILED", "BAD_NUMBER"])
def test_unanswered_disposition_is_attempted(db, disposition):
    call(db, started=ARRIVED + timedelta(seconds=90), disposition=disposition, talk_seconds=20)
    assert call_status(db, _row(), None) == ATTEMPTED


@pytest.mark.parametrize("talk_seconds,expected", [(45, CONNECTED), (0, ATTEMPTED), (None, ATTEMPTED)])
def test_cdr_without_disposition_uses_talk_time(db, talk_seconds, expected):
    call(db, started=ARRIVED + timedelta(seconds=90), talk_seconds=talk_seconds)
    assert call_status(db, _row(), None) == expected


def test_one_connected_call_beats_earlier_unanswered_attempts(db):
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="NO_ANSWER", talk_seconds=0)
    call(db, started=ARRIVED + timedelta(seconds=90), disposition="BOOKED")
    assert call_status(db, _row(), None) == CONNECTED


@pytest.mark.parametrize("direction,started", [("inbound", 60), ("outbound", -60)])
def test_inbound_call_or_call_before_arrival_does_not_count(db, direction, started):
    call(db, started=ARRIVED + timedelta(seconds=started), direction=direction, disposition="BOOKED")
    assert call_status(db, _row(), None) == NONE


def test_lead_without_ghl_contact_uses_cdr_only(db):
    def must_not_run(contact_id, since):
        raise AssertionError("GHL must not be queried without a contact id")

    assert call_status(db, _row(ghl_contact_id=None), must_not_run) == NONE


def test_ghl_result_is_combined_with_cdr(db):
    assert call_status(db, _row(ghl_contact_id="g"), lambda c, s: CONNECTED) == CONNECTED
    assert call_status(db, _row(ghl_contact_id="g"), lambda c, s: ATTEMPTED) == ATTEMPTED
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="NO_ANSWER", talk_seconds=0)
    assert call_status(db, _row(ghl_contact_id="g"), lambda c, s: NONE) == ATTEMPTED


def test_ghl_failure_falls_back_to_cdr(db):
    def broken(contact_id, since):
        raise TimeoutError

    assert call_status(db, _row(ghl_contact_id="g"), broken) == NONE
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="BOOKED")
    assert call_status(db, _row(ghl_contact_id="g"), broken) == CONNECTED


def test_one_call_closes_two_leads_with_the_same_phone(db):
    call(db, started=ARRIVED + timedelta(seconds=30), disposition="BOOKED")
    assert call_status(db, _row(lead_uuid="u-1"), None) == CONNECTED
    assert call_status(db, _row(lead_uuid="u-2"), None) == CONNECTED


class _Response:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def _ghl_http(messages):
    def http(url, **kwargs):
        if url.endswith("/conversations/search"):
            assert kwargs["params"]["contactId"] == "ghl-1"
            return _Response({"conversations": [{"id": "conv-1"}]})
        assert kwargs["params"]["type"] == "TYPE_CALL"
        return _Response({"messages": {"messages": messages}})
    return http


def test_ghl_lookup_classifies_outbound_call_messages_after_arrival():
    account = GhlAccount("key", "loc")
    before = {"direction": "outbound", "dateAdded": "2026-10-09T14:59:00.000Z", "meta": {"callStatus": "completed"}}
    inbound = {"direction": "inbound", "dateAdded": "2026-10-09T15:01:00.000Z", "meta": {"callStatus": "completed"}}
    no_answer = {"direction": "outbound", "dateAdded": "2026-10-09T15:01:00.000Z", "meta": {"callStatus": "no-answer"}}
    answered = {"direction": "outbound", "dateAdded": "2026-10-09T15:02:00.000Z", "meta": {"callStatus": "completed"}}
    assert ghl_call_lookup(account, http=_ghl_http([before, inbound]))("ghl-1", ARRIVED) == NONE
    assert ghl_call_lookup(account, http=_ghl_http([no_answer]))("ghl-1", ARRIVED) == ATTEMPTED
    assert ghl_call_lookup(account, http=_ghl_http([no_answer, answered]))("ghl-1", ARRIVED) == CONNECTED


def test_ghl_lookup_naive_or_bad_dates_count_as_no_call():
    lookup = ghl_call_lookup(GhlAccount("key", "loc"), http=_ghl_http([
        {"direction": "outbound", "dateAdded": "2026-10-09T15:01:00", "meta": {"callStatus": "completed"}},
        {"direction": "outbound", "dateAdded": "not-a-date"},
        {"direction": "outbound"},
    ]))
    assert lookup("ghl-1", ARRIVED) == NONE
