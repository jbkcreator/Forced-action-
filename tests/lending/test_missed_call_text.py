"""WP-GL-9 missed-call text: detect no-answers, decide, send through the consent-gated SMS path."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.lending.missed_call_text import MissedCall, parse_cdr, render_text

NOW = datetime(2026, 10, 5, 15, 0, tzinfo=timezone.utc)   # 11:00 ET
PHONE = "+18135558601"


def _cdr(**over):
    """Shape documented for GET /api/v2/cdrs/last (BatchDialer public API)."""
    row = {"id": "cdr-1", "direction": "out", "callStartTime": "2026-10-05T14:59:00Z",
           "callEndTime": "2026-10-05T14:59:30Z", "did": "8135550999", "customerNumber": "8135558601",
           "disposition": "No Answer", "status": "NOANSWER", "duration": 30,
           "agent": {"id": 42, "firstname": "A", "lastname": "B"}, "campaign": {"id": 10, "name": "Builders"},
           "contact": {"id": 789}, "callid": "abc-def-123"}
    row.update(over)
    return row


# ── Parsing call records (field names confirmed by the first real call) ──

def test_an_unanswered_outbound_call_becomes_a_missed_call():
    call = parse_cdr(_cdr())
    assert call == MissedCall(call_id="cdr-1", phone=PHONE, caller_id_number="+18135550999",
                              ended_at=datetime(2026, 10, 5, 14, 59, 30, tzinfo=timezone.utc))


@pytest.mark.parametrize("over", [{"status": "COMPLETED", "disposition": "ANSWER"}, {"direction": "in"}, {"id": None}])
def test_answered_inbound_or_unidentified_calls_are_ignored(over):
    assert parse_cdr(_cdr(**over)) is None


def test_text_names_the_property_and_always_carries_stop_language():
    assert "123 Main St" in render_text("123 Main St, Tampa FL 33602")
    for body in (render_text("123 Main St"), render_text(None)):
        assert "Reply STOP" in body and len(body) <= 320


# ── Decision + send (DB) ──

needs_db = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")


@pytest.fixture
def db():
    from migrations.apply_lending_missed_call_texts import apply_to
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    apply_to(conn)
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


class Sender:
    def __init__(self, allow=True):
        self.sent, self.allow = [], allow

    def __call__(self, to, body, from_number=None):
        self.sent.append((to, body, from_number))
        return self.allow


def _call(call_id=None, phone=PHONE, minutes_ago=0.5, caller_id=None):
    return MissedCall(call_id=call_id or f"c-{uuid.uuid4().hex[:8]}", phone=phone, caller_id_number=caller_id,
                      ended_at=NOW - timedelta(minutes=minutes_ago))


def _outcomes(db):
    return [r[0] for r in db.execute(text(
        "SELECT outcome FROM lending.missed_call_texts WHERE phone = :p ORDER BY id"), {"p": PHONE})]


@needs_db
def test_sends_once_and_logs_the_send(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call()], sender=sender, enabled=True, now=NOW)
    assert len(sender.sent) == 1 and sender.sent[0][0] == PHONE
    assert _outcomes(db) == ["sent"]


@needs_db
def test_one_text_per_person_per_day(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call(), _call()], sender=sender, enabled=True, now=NOW)
    assert len(sender.sent) == 1
    assert _outcomes(db) == ["sent", "skipped_daily_cap"]


@needs_db
def test_the_same_call_is_never_processed_twice(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    call = _call(call_id="dup-1")
    process_missed_calls(db, [call], sender=sender, enabled=True, now=NOW)
    process_missed_calls(db, [call], sender=sender, enabled=True, now=NOW)
    assert len(sender.sent) == 1 and _outcomes(db) == ["sent"]


@needs_db
def test_a_suppressed_number_is_never_texted(db):
    from src.lending.missed_call_text import process_missed_calls
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:p, 'OPT_OUT', 'test')"), {"p": PHONE})
    sender = Sender()
    process_missed_calls(db, [_call()], sender=sender, enabled=True, now=NOW)
    assert sender.sent == [] and _outcomes(db) == ["skipped_suppressed"]


@needs_db
def test_a_block_from_the_sms_gate_is_logged_as_such(db):
    from src.lending.missed_call_text import process_missed_calls
    process_missed_calls(db, [_call()], sender=Sender(allow=False), enabled=True, now=NOW)
    assert _outcomes(db) == ["skipped_sms_gate"]


@needs_db
def test_the_text_is_offered_the_dialed_caller_id_number(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call(caller_id="+18135550999")], sender=sender, enabled=True, now=NOW)
    assert sender.sent[0][2] == "+18135550999"


@needs_db
def test_a_call_older_than_60_seconds_is_never_texted(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call(minutes_ago=61 / 60)], sender=sender, enabled=True, now=NOW)
    assert sender.sent == [] and _outcomes(db) == ["skipped_late"]


@needs_db
def test_decisions_are_written_in_one_batch_and_input_is_bounded(db, monkeypatch):
    from src.lending import missed_call_text as mct
    monkeypatch.setattr(mct, "MAX_CALLS_PER_CYCLE", 2)
    calls = [_call(phone=f"+1813555{8610 + i}") for i in range(3)]
    counts = mct.process_missed_calls(db, calls, sender=Sender(), enabled=False, now=NOW)
    assert counts == {"dry_run": 2}


@needs_db
def test_disabled_sends_nothing_and_logs_a_dry_run(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call()], sender=sender, enabled=False, now=NOW)
    assert sender.sent == [] and _outcomes(db) == ["dry_run"]


@needs_db
def test_a_call_older_than_the_window_is_skipped_as_late(db):
    from src.lending.missed_call_text import process_missed_calls
    sender = Sender()
    process_missed_calls(db, [_call(minutes_ago=10)], sender=sender, enabled=True, now=NOW)
    assert sender.sent == [] and _outcomes(db) == ["skipped_late"]


@needs_db
def test_one_poll_cycle_reads_call_records_and_decides_new_no_answers(db):
    from src.lending.missed_call_poller import run_cycle
    fresh = (NOW - timedelta(seconds=20)).isoformat()
    records = {"items": [_cdr(id="p-1", callEndTime=fresh),
                         _cdr(id="p-2", status="COMPLETED", disposition="ANSWER", callEndTime=fresh)]}
    http = lambda method, path, json=None: records
    counts = run_cycle(db, http=http, enabled=False, now=NOW)
    assert counts == {"dry_run": 1}
    assert _outcomes(db) == ["dry_run"]


@needs_db
def test_a_second_poller_skips_while_the_first_holds_the_lock(db):
    from src.lending.missed_call_poller import run_cycle
    from config.lending_missed_call import POLL_LOCK_KEY
    other = db.get_bind().engine.connect()
    try:
        other.execute(text("SELECT pg_advisory_lock(:k)"), {"k": POLL_LOCK_KEY})
        assert run_cycle(db, http=lambda *a, **k: pytest.fail("must not poll"), enabled=False, now=NOW) is None
    finally:
        other.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": POLL_LOCK_KEY})
        other.close()


@needs_db
def test_a_poll_cycle_writes_attempt_rows_and_runs_the_cap_hook(db, monkeypatch):
    from src.lending import missed_call_poller
    hooked = []
    monkeypatch.setattr(missed_call_poller, "on_attempt_recorded", lambda db, phone, now=None: hooked.append(phone))
    fresh = (NOW - timedelta(seconds=20)).isoformat()
    records = {"items": [_cdr(id=f"a-{uuid.uuid4().hex[:6]}", status="COMPLETED", disposition="ANSWER",
                              callEndTime=fresh)]}
    missed_call_poller.run_cycle(db, http=lambda *a, **k: records, enabled=False, now=NOW)
    assert hooked == [PHONE]
    assert db.execute(text("SELECT count(*) FROM lending.call_dispositions WHERE phone = :p"), {"p": PHONE}).scalar() == 1



def test_the_no_answer_disposition_alone_is_enough():
    assert parse_cdr(_cdr(status="", disposition="No Answer")) is not None


def test_the_poller_never_uses_the_shared_since_last_poll_marker():
    from config.lending_missed_call import CDR_POLL_PATH
    assert not CDR_POLL_PATH.endswith("/last")


@needs_db
def test_a_call_dispositioned_do_not_call_suppresses_the_number(db):
    from src.lending.missed_call_poller import run_cycle
    fresh = (NOW - timedelta(seconds=20)).isoformat()
    records = {"items": [_cdr(id=f"d-{uuid.uuid4().hex[:6]}", status="COMPLETED", disposition="DNC_REQUEST",
                              callEndTime=fresh)]}
    run_cycle(db, http=lambda *a, **k: records, enabled=False, now=NOW)
    assert db.execute(text("SELECT count(*) FROM lending.suppression_list WHERE phone = :p"),
                      {"p": PHONE}).scalar() == 1


# ── Multi-line dialing ──

@pytest.mark.parametrize("over", [{"status": "ABANDONED", "disposition": "Abandoned"},
                                  {"status": "ABANDON", "disposition": ""}])
def test_an_abandoned_multi_line_call_is_never_a_missed_call(over):
    # The person picked up and the dialer dropped them: "sorry we missed you" would be wrong.
    assert parse_cdr(_cdr(**over)) is None


@needs_db
def test_a_dropped_call_with_no_agent_still_counts_as_an_attempt(db):
    from src.lending import missed_call_poller
    fresh = (NOW - timedelta(seconds=20)).isoformat()
    records = [_cdr(id=f"n-{uuid.uuid4().hex[:6]}", status="ABANDONED", disposition="Abandoned",
                    agent=None, callEndTime=fresh)]
    missed_call_poller.run_cycle(db, http=lambda *a, **k: records, enabled=False, now=NOW)
    assert db.execute(text("SELECT count(*) FROM lending.call_dispositions WHERE phone = :p"),
                      {"p": PHONE}).scalar() == 1


# ── Own bookmark: never share BatchDialer's per-integration "since last poll" marker ──

@needs_db
def test_the_poller_pages_the_call_list_until_it_reaches_calls_it_already_has(db):
    from src.lending import missed_call_poller
    fresh = (NOW - timedelta(seconds=20)).isoformat()
    seen = _cdr(id=f"s-{uuid.uuid4().hex[:6]}", status="COMPLETED", disposition="ANSWER", callEndTime=fresh)
    missed_call_poller.run_cycle(db, http=lambda *a, **k: [seen], enabled=False, now=NOW)

    new = _cdr(id=f"n-{uuid.uuid4().hex[:6]}", callEndTime=fresh)
    pages = {None: {"items": [new], "nextPage": "p2"}, "p2": {"items": [seen], "nextPage": "p3"},
             "p3": {"items": [], "nextPage": None}}
    requested = []

    def http(method, path, json=None):
        requested.append(path)
        cursor = path.split("next_page=")[1].split("&")[0] if "next_page=" in path else None
        return pages[cursor]
    counts = missed_call_poller.run_cycle(db, http=http, enabled=False, now=NOW)
    assert all(p.startswith("/v2/cdrs?") and "/last" not in p for p in requested)
    assert len(requested) == 2          # stopped on the page holding a call it already had
    assert counts == {"dry_run": 1}


# ── Shift groups: allow and warn (decision 2026-10-01) ──

@needs_db
def test_a_call_by_an_agent_with_no_shift_group_is_counted_and_warned(db, caplog, monkeypatch):
    from src.lending import missed_call_poller
    monkeypatch.setattr(missed_call_poller, "AGENT_SHIFT_GROUPS", {})
    fresh = (NOW - timedelta(seconds=20)).isoformat()
    records = [_cdr(id=f"g-{uuid.uuid4().hex[:6]}", status="COMPLETED", disposition="ANSWER", callEndTime=fresh)]
    with caplog.at_level("WARNING"):
        missed_call_poller.run_cycle(db, http=lambda *a, **k: records, enabled=False, now=NOW)
    assert db.execute(text("SELECT count(*) FROM lending.call_dispositions WHERE phone = :p"), {"p": PHONE}).scalar() == 1
    assert any("no shift group" in r.getMessage() and "42" in r.getMessage() for r in caplog.records)


@needs_db
def test_a_call_outside_the_agents_shift_is_warned_and_its_group_recorded(db, caplog, monkeypatch):
    from src.lending import missed_call_poller
    monkeypatch.setattr(missed_call_poller, "AGENT_SHIFT_GROUPS", {"42": "B"})
    early = datetime(2026, 10, 5, 14, 0, tzinfo=timezone.utc)   # 10:00 ET: before Group B starts at 13:00
    call_id = f"g-{uuid.uuid4().hex[:6]}"
    records = [_cdr(id=call_id, status="COMPLETED", disposition="ANSWER",
                    callStartTime=early.isoformat(), callEndTime=(early + timedelta(minutes=2)).isoformat())]
    with caplog.at_level("WARNING"):
        missed_call_poller.run_cycle(db, http=lambda *a, **k: records, enabled=False, now=early + timedelta(minutes=3))
    assert any("outside shift group B" in r.getMessage() for r in caplog.records)
    assert db.execute(text("SELECT seat_group FROM lending.call_dispositions WHERE dialer_call_id = :c"),
                      {"c": call_id}).scalar() == "B"
