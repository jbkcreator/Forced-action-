"""record_dialer_event against the real lending schema (each test rolled back)."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from config.lending_compliance import ReasonCode
from src.lending import dispositions as d
from src.lending.compliance import can_dial_now

pytestmark = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")

PHONE = "+18135550142"
NOW = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)  # 12:00 ET, inside every window


def _ev(call_id="c1", **over):
    base = dict(call_id=call_id, contact_id=None, direction="outbound", phone="8135550142", seat_id="7",
                seat_name="Sam", caller_id_number="+18135550100", campaign_id="55",
                started_at=NOW - timedelta(minutes=5), ended_at=NOW - timedelta(minutes=4), duration=60,
                disposition_raw=None, recording_ref=None, disclosure_logged=None, raw={"x": 1})
    base.update(over)
    return d.DialerCallEvent(**base)


def _row(db, call_id="c1"):
    return db.execute(text("SELECT * FROM lending.call_dispositions WHERE dialer_call_id = :c"), {"c": call_id}).mappings().one()


@pytest.fixture(autouse=True)
def seats(monkeypatch):
    monkeypatch.setattr(d, "get_settings", lambda: SimpleNamespace(lending_seat_groups="7:A, 8:b"))


def test_replay_gives_one_row_and_the_code_sticks(lending_db):
    d.record_dialer_event(lending_db, _ev(disposition_raw="CALLBACK_REQUESTED"))
    d.record_dialer_event(lending_db, _ev(disposition_raw="CALLBACK_REQUESTED"))
    assert lending_db.execute(text("SELECT count(*) FROM lending.call_dispositions")).scalar() == 1
    row = _row(lending_db)
    assert row["disposition"] == "CALLBACK_REQUESTED" and row["disposition_list_version"]
    assert (row["caller_seat"], row["seat_group"], row["caller_id_number"]) == ("7", "A", "+18135550100")


def test_disposition_before_the_call_end_converges_and_the_real_end_wins(lending_db):
    early = _ev(ended_at=None, disposition_raw="NOT_DECISION_MAKER", started_at=NOW - timedelta(minutes=9))
    d.record_dialer_event(lending_db, early)
    assert _row(lending_db)["call_ended_at"] == early.started_at + timedelta(seconds=60)
    d.record_dialer_event(lending_db, _ev(disposition_raw=None))
    row = _row(lending_db)
    assert row["call_ended_at"] == NOW - timedelta(minutes=4) and row["disposition"] == "NOT_DECISION_MAKER"


def test_telephony_status_does_not_wipe_a_stored_caller_code(lending_db):
    d.record_dialer_event(lending_db, _ev(disposition_raw="CALLBACK_REQUESTED"))
    d.record_dialer_event(lending_db, _ev(disposition_raw="ANSWER"))
    assert _row(lending_db)["disposition"] == "CALLBACK_REQUESTED"


def test_changed_code_replaces_the_old_one(lending_db):
    d.record_dialer_event(lending_db, _ev(disposition_raw="CALLBACK_REQUESTED"))
    d.record_dialer_event(lending_db, _ev(disposition_raw="DNC_REQUEST"))
    assert _row(lending_db)["disposition"] == "DNC_REQUEST"


def test_three_dials_make_a_fourth_blocked(lending_db):
    """Friday Test 4: the attempt rows this module writes are what the cap counts."""
    for i in range(3):
        d.record_dialer_event(lending_db, _ev(f"c{i}", ended_at=NOW - timedelta(hours=i + 1), disposition_raw="NO_ANSWER"))
    result = can_dial_now(PHONE, lending_db, now=NOW)
    assert not result.allowed and result.reason == ReasonCode.ATTEMPT_CAP_REACHED


def test_group_b_seat_is_blocked_after_the_1915_stop(lending_db):
    late = datetime(2026, 9, 29, 23, 20, tzinfo=timezone.utc)  # 19:20 ET
    assert can_dial_now(PHONE, lending_db, now=late, seat_group="B").reason == ReasonCode.OUTSIDE_CALL_WINDOW


def test_unanswered_call_queues_one_text_signal_per_contact_per_day(lending_db):
    d.record_dialer_event(lending_db, _ev("c1", duration=0, disposition_raw="No Answer"))
    d.record_dialer_event(lending_db, _ev("c2", duration=0, disposition_raw="Answering Machine"))
    d.record_dialer_event(lending_db, _ev("c1", duration=0, disposition_raw="No Answer"))  # replay
    rows = lending_db.execute(text("SELECT dialer_call_id, status FROM lending.missed_call_events ORDER BY 1")).all()
    assert [tuple(r) for r in rows] == [("c1", "pending"), ("c2", "duplicate_day")]
    assert _row(lending_db, "c2")["disposition"] == "LEFT_VOICEMAIL"


def test_unanswered_without_any_disposition_still_counts(lending_db):
    """A ring-out nobody dispositions still counts as an attempt and still raises the signal."""
    d.record_dialer_event(lending_db, _ev(duration=0, disposition_raw=None))
    assert lending_db.execute(text("SELECT status FROM lending.missed_call_events")).scalar() == "pending"


def test_connected_call_raises_no_signal(lending_db):
    d.record_dialer_event(lending_db, _ev(disposition_raw="CONNECTED_NOT_INTERESTED"))
    assert lending_db.execute(text("SELECT count(*) FROM lending.missed_call_events")).scalar() == 0


def test_suppressed_number_is_blocked_not_queued(lending_db):
    lending_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'test')"), {"p": PHONE})
    d.record_dialer_event(lending_db, _ev(duration=0, disposition_raw="NO_ANSWER"))
    assert lending_db.execute(text("SELECT status FROM lending.missed_call_events")).scalar() == "blocked"


def test_unknown_code_is_kept_raw_and_flagged_once(lending_db):
    first = d.record_dialer_event(lending_db, _ev(disposition_raw="Hot Lead"))
    again = d.record_dialer_event(lending_db, _ev(disposition_raw="Hot Lead"))
    assert first.unknown_code == "Hot Lead" and again.unknown_code is None
    row = _row(lending_db)
    assert row["disposition"] is None and row["disposition_raw"] == "Hot Lead"


def test_builtin_dialer_results_map_without_an_alert(lending_db):
    rec = d.record_dialer_event(lending_db, _ev(disposition_raw="Disconnected Number"))
    assert rec.unknown_code is None and rec.disposition == "BAD_NUMBER"


def test_cause_defaults_are_provisional(lending_db):
    d.record_dialer_event(lending_db, _ev(disposition_raw="BAD_NUMBER"))
    row = _row(lending_db)
    assert row["unfunded_cause"] == "contactability" and row["unfunded_cause_provisional"] is True


def test_dnc_code_is_reported_for_propagation(lending_db):
    rec = d.record_dialer_event(lending_db, _ev(disposition_raw="DNC_REQUEST"))
    assert rec.dnc_requested and not rec.opt_out_propagated


def test_booked_on_a_nurture_only_list_is_blocked(lending_db):
    lending_db.execute(text(
        "INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash, campaign_tag, dialer_contact_id) "
        "VALUES ('r', 'nurture', 'ref', :p, 'h', 'Nurture', 'k9')"), {"p": PHONE})
    rec = d.record_dialer_event(lending_db, _ev(contact_id="k9", disposition_raw="BOOKED"))
    assert rec.booking_blocked and _row(lending_db)["booking_blocked"] is True
    assert _row(lending_db)["queue"] == "nurture"


def test_booked_on_a_launch_queue_counts(lending_db):
    lending_db.execute(text(
        "INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash, campaign_tag) "
        "VALUES ('r', 'builders', 'ref', :p, 'h', 'Builders')"), {"p": PHONE})
    assert not d.record_dialer_event(lending_db, _ev(disposition_raw="BOOKED")).booking_blocked


def test_unmapped_seat_gets_no_group(lending_db):
    d.record_dialer_event(lending_db, _ev(seat_id="99"))
    assert _row(lending_db)["seat_group"] is None


def test_recording_reference_and_disclosure_are_stored_only_when_sent(lending_db):
    d.record_dialer_event(lending_db, _ev(recording_ref="https://x/rec", disclosure_logged=True))
    row = _row(lending_db)
    assert row["recording_ref"] == "https://x/rec" and row["recording_disclosure_logged"] is True
    d.record_dialer_event(lending_db, _ev("c2"))
    assert _row(lending_db, "c2")["recording_disclosure_logged"] is False


def test_new_recording_enters_the_check_and_no_recording_stays_null(lending_db):
    d.record_dialer_event(lending_db, _ev(recording_ref="https://x/rec"))
    d.record_dialer_event(lending_db, _ev("c2"))
    assert _row(lending_db)["recording_status"] == "pending"
    assert _row(lending_db, "c2")["recording_status"] is None
    lending_db.execute(text("UPDATE lending.call_dispositions SET recording_status = 'readable' WHERE dialer_call_id = 'c1'"))
    d.record_dialer_event(lending_db, _ev(recording_ref="https://x/rec"))
    assert _row(lending_db)["recording_status"] == "readable"


def test_campaign_id_is_stored_and_a_later_event_without_one_keeps_it(lending_db):
    d.record_dialer_event(lending_db, _ev(campaign_id="55"))
    d.record_dialer_event(lending_db, _ev(campaign_id=None))
    assert _row(lending_db)["dialer_campaign_id"] == "55"


def test_parse_event_reads_nested_and_epoch_fields_and_needs_a_call_id():
    ev = d.parse_event({"event": "x", "data": {"id": 9, "contact": {"id": 3, "phone": "813"}, "user": {"id": 7, "name": "Sam"},
                                                  "startTime": 1790000000, "callResult": "No Answer"}})
    assert (ev.call_id, ev.contact_id, ev.seat_id, ev.disposition_raw) == ("9", "3", "7", "No Answer")
    assert ev.started_at.tzinfo is not None
    assert d.parse_event({"data": {}}) is None and d.parse_event("nope") is None


def test_inbound_unanswered_call_raises_no_missed_call_signal(lending_db):
    d.record_dialer_event(lending_db, _ev(direction="inbound", duration=0, disposition_raw="NO_ANSWER"))
    assert lending_db.execute(text("SELECT count(*) FROM lending.missed_call_events")).scalar() == 0


def test_every_listed_code_is_stored_with_its_cause_default_and_only_unanswered_codes_queue_a_text(lending_db):
    """All 13 codes (including the client-approval proposals) work end to end."""
    from config import lending_dispositions as cfg

    for i, code in enumerate(cfg.DISPOSITIONS):
        d.record_dialer_event(lending_db, _ev(f"code-{i}", phone=f"81355501{i:02d}", disposition_raw=code, duration=0 if code in cfg.UNANSWERED_CODES else 60))
    rows = {r[0]: r[1:] for r in lending_db.execute(text(
        "SELECT disposition, unfunded_cause, unfunded_cause_provisional, disposition_list_version FROM lending.call_dispositions"))}
    assert set(rows) == set(cfg.DISPOSITIONS)
    for code, (cause, provisional, version) in rows.items():
        assert cause == cfg.DEFAULT_CAUSE.get(code) and provisional is (code in cfg.DEFAULT_CAUSE)
        assert version == cfg.DISPOSITION_LIST_VERSION
    queued = lending_db.execute(text("SELECT count(*) FROM lending.missed_call_events WHERE status = 'pending'")).scalar()
    assert queued == len(cfg.UNANSWERED_CODES)


# ── Text-consent capture (client Q27) ──

from src.lending.call_pipeline import process_event
from src.lending.consent import has_text_consent
from src.lending.dialer_port import DialerRequestError


class ConsentDialer:
    def __init__(self, fields=None, error=None):
        self.fields, self.error, self.reads = fields or {}, error, []

    def get_contact_customfields(self, contact_id, quick=False):
        self.reads.append(contact_id)
        if self.error:
            raise self.error
        return self.fields


def _live(call_id="k1", **over):
    """An event that ended a minute ago (consent is only checked for recent calls)."""
    now = datetime.now(timezone.utc)
    return _ev(call_id=call_id, contact_id="9", started_at=now - timedelta(minutes=3),
               ended_at=now - timedelta(minutes=1), **over)


def _consent_sources(db):
    return db.execute(text("SELECT source, captured_by FROM lending.text_consents")).all()


def test_answered_inbound_call_counts_as_text_consent(lending_db):
    process_event(lending_db, _live(direction="inbound"), dialer=ConsentDialer())
    assert has_text_consent(lending_db, PHONE) is True
    assert _consent_sources(lending_db) == [("inbound_call", None)]


def test_on_call_yes_is_stored_with_the_caller_name(lending_db):
    dialer = ConsentDialer({"text_consent": " Yes "})
    process_event(lending_db, _live(), dialer=dialer)
    assert _consent_sources(lending_db) == [("on_call_yes", "Sam")]
    assert dialer.reads == ["9"]


@pytest.mark.parametrize("fields", [{"text_consent": "no"}, {}])
def test_no_or_missing_field_stores_nothing(lending_db, fields):
    process_event(lending_db, _live(), dialer=ConsentDialer(fields))
    assert _consent_sources(lending_db) == []


def test_dialer_error_stores_nothing_and_does_not_raise(lending_db):
    process_event(lending_db, _live(), dialer=ConsentDialer(error=DialerRequestError("down")))
    assert _consent_sources(lending_db) == []
    assert _row(lending_db, "k1")["consent_checked_at"] is None  # a failed read is retried, not marked checked


def test_unanswered_call_never_reads_the_dialer(lending_db):
    dialer = ConsentDialer({"text_consent": "yes"})
    process_event(lending_db, _live(duration=0), dialer=dialer)
    assert dialer.reads == [] and _consent_sources(lending_db) == []


def test_rescan_does_not_reread_but_a_later_disposition_does(lending_db):
    dialer = ConsentDialer({"text_consent": "no"})
    process_event(lending_db, _live(), dialer=dialer)
    process_event(lending_db, _live(), dialer=dialer)
    assert len(dialer.reads) == 1
    process_event(lending_db, _live(disposition_raw="CALLBACK_REQUESTED"), dialer=dialer)
    assert len(dialer.reads) == 2
    process_event(lending_db, _live(disposition_raw="CALLBACK_REQUESTED"), dialer=dialer)
    assert len(dialer.reads) == 2


def test_calls_that_ended_over_two_hours_ago_are_not_checked(lending_db):
    dialer = ConsentDialer({"text_consent": "yes"})
    old = datetime.now(timezone.utc) - timedelta(hours=3)
    process_event(lending_db, _ev(call_id="old", contact_id="9", started_at=old - timedelta(minutes=2), ended_at=old), dialer=dialer)
    assert dialer.reads == []


def test_a_failing_consent_step_never_blocks_attempts_or_opt_out(lending_db, monkeypatch):
    from src.lending import call_pipeline

    attempts = []
    monkeypatch.setattr(call_pipeline, "on_attempt_recorded", lambda db, phone: attempts.append(phone))
    monkeypatch.setattr(call_pipeline, "capture_consent",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    recorded = process_event(lending_db, _live(disposition_raw="DNC_REQUEST"), dialer=ConsentDialer())
    row = _row(lending_db, "k1")
    assert recorded.row_id == row["id"] and attempts == [row["phone"]]
    assert row["opt_out_propagated_at"] is not None and row["consent_checked_at"] is None


def test_consent_read_is_a_single_quick_attempt(monkeypatch):
    import requests
    from src.lending import dialer_port

    calls = []

    def boom(url, **kw):
        calls.append(kw["timeout"])
        raise requests.ConnectionError("down")

    monkeypatch.setattr(requests, "get", boom)
    adapter = dialer_port.BatchDialerAdapter(http=dialer_port._requests_http("k"))
    with pytest.raises(DialerRequestError):
        adapter.get_contact_customfields(9, quick=True)
    assert len(calls) == 1 and calls[0] <= 5
