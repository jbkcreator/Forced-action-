"""G4: a missed GHL call gets the missed-call text the moment GHL reports it, with every BatchDialer gate; a call
reported late is never texted. Parsing and the route are pure; the end-to-end cases run on TEST_DATABASE_URL."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from config.lending_dispositions import GHL_UNKNOWN_CALLER
from migrations.apply_lending_call_gate_failed import apply_to as apply_gate_failed
from migrations.apply_lending_ghl_disposition_events import apply_to as apply_events
from migrations.apply_lending_gl9_text_back import apply_to as apply_text_back
from src.lending import ghl_disposition_match as m
from src.lending import ghl_missed_call as mc
from src.lending.consent import record_consent
from src.lending.ghl_call_import import _UPSERT, UNKNOWN_OUTCOME
from src.lending.text_back import process_pending

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
NOW = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)  # 10:00 ET
BODY = {"call_id": "msg-1", "phone": "(813) 555-0142", "call_status": "no-answer", "contact_id": "c-1",
        "user": "Josh Kantor"}


# ---- parsing ------------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["no-answer", "No Answer", "NO_ANSWER", "noanswer", "busy", "failed", "voicemail"])
def test_every_unanswered_status_is_a_missed_call(status):
    call = mc.parse_missed_call({**BODY, "call_status": status}, NOW)
    assert call is not None and call.dialer_call_id == "ghl:msg-1" and call.phone == "+18135550142"


@pytest.mark.parametrize("extra", [{"call_status": "completed"}, {"call_status": ""}, {"call_status": "{{call.status}}"},
                                   {"direction": "inbound"}])
def test_answered_inbound_or_unknown_calls_are_ignored(extra):
    assert mc.parse_missed_call({**BODY, **extra}, NOW) is None


def test_a_missed_call_needs_a_phone():
    with pytest.raises(mc.InvalidMissedCall) as exc:
        mc.parse_missed_call({k: v for k, v in BODY.items() if k != "phone"}, NOW)
    assert exc.value.reason == mc.REASON_NO_PHONE


# The body exactly as built in the GHL workflow (no call id: GHL has no merge field for it).
GHL_BODY = {"phone": "+18135550142", "contact_phone": "+18135550142", "contact_id": "c-1",
            "call_status": "no-answer", "user": "Josh Kantor", "called_at": "2026-11-04T14:59:20.000Z",
            "ended_at": "2026-11-04T14:59:50.000Z", "duration": "30"}


def test_without_a_call_id_the_call_is_keyed_by_phone_and_start_time():
    call = mc.parse_missed_call(GHL_BODY, NOW)
    assert call.dialer_call_id == mc.missed_call_key("+18135550142", datetime(2026, 11, 4, 14, 59, 20, tzinfo=timezone.utc))
    assert call.dialer_call_id.startswith("ghl-missed:") and "8135550142" not in call.dialer_call_id
    assert mc.parse_missed_call(GHL_BODY, NOW + timedelta(seconds=30)).dialer_call_id == call.dialer_call_id  # a retry


def test_without_any_time_retries_in_the_same_minute_share_a_key():
    body = {k: v for k, v in GHL_BODY.items() if k not in ("called_at", "ended_at")}
    first = mc.parse_missed_call(body, NOW.replace(second=5))
    assert first.dialer_call_id == mc.parse_missed_call(body, NOW.replace(second=40)).dialer_call_id
    assert first.ended_at == NOW.replace(second=5)


def test_contact_phone_is_the_fallback_and_outgoing_is_outbound():
    body = {**GHL_BODY, "phone": "{{phoneCall.to}}", "direction": "Outgoing"}
    assert mc.parse_missed_call(body, NOW).phone == "+18135550142"
    assert mc.parse_missed_call({**GHL_BODY, "direction": "Incoming"}, NOW) is None


def test_an_unreadable_time_is_logged_without_the_phone_and_arrival_is_used(caplog):
    call = mc.parse_missed_call({**GHL_BODY, "called_at": "Nov 4 2026 at 10:59 AM", "ended_at": "Nov 4 2026 at 10:59 AM"},
                                NOW)
    assert call.ended_at == NOW
    assert "Nov 4 2026 at 10:59 AM" in caplog.text and "8135550142" not in caplog.text


def test_the_call_end_is_ghl_time_else_arrival_and_never_after_arrival():
    ended = NOW - timedelta(seconds=20)
    assert mc.parse_missed_call({**BODY, "ended_at": ended.isoformat()}, NOW).ended_at == ended
    assert mc.parse_missed_call({**BODY, "called_at": ended.isoformat()}, NOW).ended_at == ended
    assert mc.parse_missed_call(BODY, NOW).ended_at == NOW
    assert mc.parse_missed_call({**BODY, "ended_at": (NOW + timedelta(minutes=5)).isoformat()}, NOW).ended_at == NOW


def test_custom_data_keys_are_read():
    call = mc.parse_missed_call({"customData": {"callId": "msg-9", "contact_phone": "8135550142",
                                                "callStatus": "busy"}}, NOW)
    assert call.dialer_call_id == "ghl:msg-9" and call.status == "busy"


# ---- route --------------------------------------------------------------------------------------------------------------

class _Db:
    def __init__(self):
        self.commits = self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def route(monkeypatch):
    from config.settings import get_settings
    from src.api import lending_ghl_router as r
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    got = {"recorded": [], "ran": 0, "status": "pending"}
    monkeypatch.setattr(r, "record_missed_call", lambda db, call: got["recorded"].append(call) or got["status"])
    monkeypatch.setattr(r, "run_text_back_now", lambda: got.__setitem__("ran", got["ran"] + 1))
    db = _Db()
    app = FastAPI()
    app.include_router(r.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app), db, got


def test_a_missed_call_is_queued_committed_and_decided_right_away(route):
    client, db, got = route
    resp = client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json=BODY)
    assert resp.status_code == 200 and resp.json() == {"queued": True, "status": "pending"}
    assert got["recorded"][0].dialer_call_id == "ghl:msg-1" and db.commits == 1 and got["ran"] == 1


@pytest.mark.parametrize("status", ["blocked", "duplicate_day", None])
def test_a_call_not_queued_for_sending_runs_no_text_back(route, status):
    client, _, got = route
    got["status"] = status
    assert client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json=BODY).json() == {
        "queued": False, "status": status}
    assert got["ran"] == 0


def test_an_answered_call_is_acknowledged_and_ignored(route):
    client, db, got = route
    resp = client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json={**BODY, "call_status": "completed"})
    assert resp.json() == {"queued": False, "ignored": True} and got["recorded"] == [] and db.commits == 0


def test_a_missing_phone_is_refused_without_detail(route):
    client, _, _ = route
    resp = client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json={**BODY, "phone": ""})
    assert resp.status_code == 422 and resp.json() == {"detail": mc.REASON_NO_PHONE}


def test_the_ghl_workflow_body_without_call_id_is_accepted(route):
    client, _, got = route
    resp = client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json=GHL_BODY)
    assert resp.status_code == 200 and got["recorded"][0].dialer_call_id.startswith("ghl-missed:")


def test_the_ghl_workflow_body_with_outgoing_direction_is_queued(route):
    """Review #407: GHL's Call details trigger says "Outgoing"; the call must be queued, not ignored."""
    client, db, got = route
    resp = client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json={**GHL_BODY, "direction": "Outgoing"})
    assert resp.status_code == 200 and resp.json() == {"queued": True, "status": "pending"}
    assert len(got["recorded"]) == 1 and db.commits == 1 and got["ran"] == 1


@pytest.mark.parametrize("direction", ["Incoming", "inbound"])
def test_an_incoming_call_from_the_workflow_is_still_ignored(route, direction):
    client, db, got = route
    resp = client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json={**GHL_BODY, "direction": direction})
    assert resp.json() == {"queued": False, "ignored": True} and got["recorded"] == [] and db.commits == 0


def test_a_wrong_secret_is_refused(route):
    client, _, got = route
    assert client.post("/webhooks/lending/ghl-missed-call", headers={"X-Webhook-Secret": "x"}, json=BODY).status_code == 401
    assert got["recorded"] == []


def test_a_store_failure_rolls_back_and_hides_the_error(route, monkeypatch):
    from src.api import lending_ghl_router as r
    client, db, _ = route

    def boom(db_, call):
        raise RuntimeError("+18135550142")
    monkeypatch.setattr(r, "record_missed_call", boom)
    resp = client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json=BODY)
    assert resp.status_code == 500 and "8135550142" not in resp.text and db.rollbacks == 1


# ---- end to end (Postgres) ----------------------------------------------------------------------------------------------

class Sender:
    def __init__(self):
        self.sent = []

    def __call__(self, phone, body, first_name=None, *, deadline=None):
        self.sent.append(phone)
        return f"msg-{len(self.sent)}"


@pytest.fixture
def db(lending_db, monkeypatch):
    monkeypatch.setattr("src.lending.compliance._default_dialer_remover", lambda: None)  # never the real BatchDialer
    conn = lending_db.connection()
    apply_text_back(conn)
    apply_gate_failed(conn)
    apply_events(conn)
    return lending_db


def _phone():
    return f"+181372{uuid.uuid4().int % 100_000:05d}"


def _missed(db, phone, *, call_id=None, ended=None, status="no-answer"):
    body = {"call_id": call_id or f"t-{uuid.uuid4().hex}", "phone": phone, "call_status": status}
    if ended:
        body["ended_at"] = ended.isoformat()
    call = mc.parse_missed_call(body, NOW)
    return call, mc.record_missed_call(db, call)


def _event_status(db, call_id):
    return db.execute(text("SELECT status FROM lending.missed_call_events WHERE dialer_call_id = :c"),
                      {"c": call_id}).scalar()


def test_a_missed_ghl_call_to_a_consented_number_gets_one_text_within_sixty_seconds(db):
    phone = _phone()
    record_consent(db, phone, "on_call_yes")
    call, status = _missed(db, phone, ended=NOW - timedelta(seconds=5))
    assert status == "pending"
    row = db.execute(text("SELECT call_source, direction, caller_name, disposition_raw, call_ended_at "
                          "FROM lending.call_dispositions WHERE dialer_call_id = :c"), {"c": call.dialer_call_id}).one()
    assert row[:4] == ("ghl", "outbound", GHL_UNKNOWN_CALLER, "ghl:no-answer")
    sender = Sender()
    assert process_pending(db, sender=sender, enabled=True, number="+16562211100",
                           now=NOW + timedelta(seconds=10)) == {"sent": 1}
    assert sender.sent == [phone] and _event_status(db, call.dialer_call_id) == "sent"


def test_a_repeated_webhook_and_the_later_import_never_send_a_second_text(db):
    phone = _phone()
    record_consent(db, phone, "on_call_yes")
    call, _ = _missed(db, phone, call_id="dup-1", ended=NOW - timedelta(seconds=5))
    sender = Sender()
    process_pending(db, sender=sender, enabled=True, number="+16562211100", now=NOW)
    _, again = _missed(db, phone, call_id="dup-1", ended=NOW - timedelta(seconds=5))
    assert again == "sent"  # the call's own event: nothing new queued
    db.execute(_UPSERT, {  # the 15-minute import stores the same call under the same key
        "call_id": call.dialer_call_id, "source": "ghl", "direction": "outbound", "phone": phone, "seat": "ghl",
        "caller_name": "Josh Kantor", "tag": "ghl", "raw_outcome": "ghl:no-answer", "talk": None,
        "started": NOW - timedelta(seconds=30), "ended": NOW - timedelta(seconds=30), "raw_event": json.dumps({}),
        "unknown_outcome": UNKNOWN_OUTCOME, "unknown_caller": GHL_UNKNOWN_CALLER,
    })
    assert db.execute(text("SELECT count(*), max(caller_name) FROM lending.call_dispositions WHERE dialer_call_id = :c"),
                      {"c": call.dialer_call_id}).one() == (1, "Josh Kantor")
    assert process_pending(db, sender=sender, enabled=True, number="+16562211100", now=NOW) == {}
    assert sender.sent == [phone]


def test_a_second_missed_call_the_same_day_gets_no_second_text(db):
    phone = _phone()
    _, first = _missed(db, phone, ended=NOW - timedelta(seconds=5))
    _, second = _missed(db, phone, ended=NOW - timedelta(seconds=3))
    assert (first, second) == ("pending", "duplicate_day")


def test_an_opted_out_number_is_recorded_blocked(db):
    phone = _phone()
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'opt_out', 'sms')"),
               {"p": phone})
    _, status = _missed(db, phone)
    assert status == "blocked"


def test_a_number_without_consent_is_not_texted(db):
    phone = _phone()
    call, _ = _missed(db, phone, ended=NOW - timedelta(seconds=5))
    sender = Sender()
    assert process_pending(db, sender=sender, enabled=True, number="+16562211100", now=NOW) == {"skipped_no_consent": 1}
    assert sender.sent == []


def test_texting_switched_off_sends_nothing(db):
    phone = _phone()
    record_consent(db, phone, "on_call_yes")
    _missed(db, phone, ended=NOW - timedelta(seconds=5))
    sender = Sender()
    assert process_pending(db, sender=sender, enabled=False, number="+16562211100", now=NOW) == {"dry_run": 1}
    assert sender.sent == []


def test_a_call_whose_webhook_never_came_is_logged_late_and_never_texted(db):
    """No missed-call webhook: the call arrives with the import and its NO_ANSWER outcome is matched 15 min later."""
    phone = _phone()
    record_consent(db, phone, "on_call_yes")
    ended = NOW - timedelta(minutes=15)
    call_id = f"ghl:late-{uuid.uuid4().hex}"
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, "
                    "talk_duration_sec, call_started_at, call_ended_at, raw_event) "
                    "VALUES (:c, 'ghl', 'outbound', :p, 0, :e, :e, '{}')"), {"c": call_id, "p": phone, "e": ended})
    m.record_event(db, m.parse_event({"disposition": "No Answer", "phone": phone, "call_id": call_id[4:],
                                      "user": "Josh Kantor"}), {})
    assert m.attach_pending(db, NOW)
    assert _event_status(db, call_id) == "pending"
    sender = Sender()
    assert process_pending(db, sender=sender, enabled=True, number="+16562211100", now=NOW) == {"skipped_late": 1}
    assert sender.sent == []


def test_a_dnc_outcome_blocks_the_number_now_and_the_sweep_does_not_repeat_it(db):
    from src.lending.call_pipeline import retry_unpropagated_dnc

    phone = _phone()
    call_id = f"ghl:dnc-{uuid.uuid4().hex}"
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, "
                    "talk_duration_sec, call_started_at, call_ended_at, raw_event) "
                    "VALUES (:c, 'ghl', 'outbound', :p, 40, :e, :e, '{}')"),
               {"c": call_id, "p": phone, "e": NOW - timedelta(minutes=2)})
    m.record_event(db, m.parse_event({"disposition": "DNC Request", "phone": phone, "call_id": call_id[4:],
                                      "user": "Josh Kantor"}), {})
    assert db.execute(text("SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE phone = :p)"),
                      {"p": phone}).scalar() is True  # blocked on arrival, before any match
    assert m.attach_pending(db, NOW)
    assert db.execute(text("SELECT opt_out_propagated_at IS NOT NULL FROM lending.call_dispositions "
                           "WHERE dialer_call_id = :c"), {"c": call_id}).scalar() is True
    before = db.execute(text("SELECT count(*) FROM lending.opt_out_events")).scalar()
    retry_unpropagated_dnc(db)
    assert db.execute(text("SELECT count(*) FROM lending.opt_out_events")).scalar() == before


def test_text_consent_yes_on_a_ghl_call_is_saved(db):
    from src.lending.consent import has_text_consent

    phone = _phone()
    call_id = f"ghl:yes-{uuid.uuid4().hex}"
    db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, "
                    "talk_duration_sec, call_started_at, call_ended_at, raw_event) "
                    "VALUES (:c, 'ghl', 'outbound', :p, 90, :e, :e, '{}')"),
               {"c": call_id, "p": phone, "e": NOW - timedelta(minutes=2)})
    assert has_text_consent(db, phone) is False
    m.record_event(db, m.parse_event({"disposition": "Callback Requested", "phone": phone, "call_id": call_id[4:],
                                      "user": "Josh Kantor", "text_consent": "Yes"}), {})
    assert m.attach_pending(db, NOW)
    row = db.execute(text("SELECT source, call_id, captured_by FROM lending.text_consents WHERE phone = :p"),
                     {"p": phone}).one()
    assert tuple(row) == ("on_call_yes", call_id, "Josh Kantor") and has_text_consent(db, phone) is True


def _ghl_body(phone, started, **extra):
    return {"phone": phone, "call_status": "no-answer", "called_at": started.isoformat(),
            "ended_at": (started + timedelta(seconds=25)).isoformat(), **extra}


def _import_call(db, message_id, phone, started):
    db.execute(_UPSERT, {
        "call_id": f"ghl:{message_id}", "source": "ghl", "direction": "outbound", "phone": phone, "seat": "ghl",
        "caller_name": "Josh Kantor", "tag": "ghl", "raw_outcome": "ghl:no-answer", "talk": None,
        "started": started, "ended": started, "raw_event": json.dumps({}),
        "unknown_outcome": UNKNOWN_OUTCOME, "unknown_caller": GHL_UNKNOWN_CALLER,
    })


def test_the_import_takes_over_the_missed_call_row_and_its_event(db):
    phone, started = _phone(), NOW - timedelta(seconds=40)
    record_consent(db, phone, "on_call_yes")
    call = mc.parse_missed_call(_ghl_body(phone, started), NOW)
    assert mc.record_missed_call(db, call) == "pending"
    sender = Sender()
    assert process_pending(db, sender=sender, enabled=True, number="+16562211100", now=NOW) == {"sent": 1}
    # 15 minutes later the import stores the same call under GHL's message id
    assert mc.adopt_missed_call(db, "ghl:realmsg01", phone, started + timedelta(seconds=2)) is True
    _import_call(db, "realmsg01", phone, started + timedelta(seconds=2))
    rows = db.execute(text("SELECT dialer_call_id FROM lending.call_dispositions WHERE phone = :p"), {"p": phone}).all()
    assert [r[0] for r in rows] == ["ghl:realmsg01"]  # one call, one attempt
    assert _event_status(db, "ghl:realmsg01") == "sent" and _event_status(db, call.dialer_call_id) is None
    assert mc.adopt_missed_call(db, "ghl:realmsg01", phone, started) is False  # nothing left to take over


def test_the_import_never_takes_over_a_call_far_apart_or_to_another_phone(db):
    phone, started = _phone(), NOW - timedelta(minutes=30)
    mc.record_missed_call(db, mc.parse_missed_call(_ghl_body(phone, started), NOW))
    assert mc.adopt_missed_call(db, "ghl:other01", phone, started + timedelta(minutes=10)) is False
    assert mc.adopt_missed_call(db, "ghl:other02", _phone(), started) is False


def test_a_webhook_after_the_import_reuses_the_imported_call(db):
    phone, started = _phone(), NOW - timedelta(seconds=40)
    _import_call(db, "early01", phone, started)
    call = mc.parse_missed_call(_ghl_body(phone, started + timedelta(seconds=1)), NOW)
    assert mc.record_missed_call(db, call) == "pending"
    assert db.execute(text("SELECT count(*) FROM lending.call_dispositions WHERE phone = :p"), {"p": phone}).scalar() == 1
    assert _event_status(db, "ghl:early01") == "pending"


def test_a_retried_ghl_post_without_call_id_queues_one_event(db):
    phone, started = _phone(), NOW - timedelta(seconds=40)
    first = mc.record_missed_call(db, mc.parse_missed_call(_ghl_body(phone, started), NOW))
    again = mc.record_missed_call(db, mc.parse_missed_call(_ghl_body(phone, started), NOW + timedelta(seconds=5)))
    assert (first, again) == ("pending", "pending")
    assert db.execute(text("SELECT count(*) FROM lending.missed_call_events WHERE phone = :p"), {"p": phone}).scalar() == 1
