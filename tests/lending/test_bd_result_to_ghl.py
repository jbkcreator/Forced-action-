"""BatchDialer call results written to the lead's GHL contact (tags + "last BatchDialer call" field)."""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending import bd_result_to_ghl as notes
from src.lending.call_pipeline import follow_up
from src.lending.dispositions import RecordedCall

NOW = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)
ENDED = NOW - timedelta(minutes=2)  # 11:58 ET


class FakeWriter:
    def __init__(self, fail=False):
        self.tags: list[tuple[str, list[str]]] = []
        self.fields: list[tuple[str, str, str]] = []
        self.fail = fail

    def add_tags(self, contact_id, tags):
        if self.fail:
            raise notes.GhlWriteError("add tags: HTTP 503")
        self.tags.append((contact_id, tags))

    def set_field(self, contact_id, field_id, value):
        self.fields.append((contact_id, field_id, value))


@pytest.fixture
def db(web_leads_db, monkeypatch):
    from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow
    from migrations.apply_lending_lendingflow_dialer import apply_to as apply_dialer

    conn = web_leads_db.connection()
    apply_lendingflow(conn)
    apply_dialer(conn)
    monkeypatch.setattr(notes.get_settings(), "lending_ghl_cf_last_bd_call", "cf-last-call")
    return web_leads_db


def _lead(db, *, contact="ghl-9"):
    phone = f"+1813555{uuid.uuid4().int % 10_000:04d}"
    db.execute(text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, phone, ghl_status, "
                    "ghl_contact_id, raw_payload) VALUES (:v, :h, :p, 'synced', :c, CAST('{}' AS jsonb))"),
               {"v": uuid.uuid4().hex, "h": uuid.uuid4().hex, "p": phone, "c": contact})
    return phone


def _call(db, phone, disposition="NO_ANSWER", *, ended=ENDED, source="batchdialer"):
    return db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, "
                           "disposition, call_ended_at, raw_event) VALUES (:id, :src, 'outbound', :p, :d, :e, "
                           "CAST('{}' AS jsonb)) RETURNING id"),
                      {"id": f"c-{uuid.uuid4().hex}", "src": source, "p": phone, "d": disposition, "e": ended}).scalar()


def test_a_finished_batchdialer_call_tags_the_ghl_contact_and_sets_the_field(db):
    phone = _lead(db)
    row = _call(db, phone)
    writer = FakeWriter()
    assert notes.write_due_notes(db, writer, row_id=row, now=NOW) == 1
    assert writer.tags == [("ghl-9", ["called-in-batchdialer"])]
    assert writer.fields == [("ghl-9", "cf-last-call", "2026-10-27 11:58 ET, no answer, 1 BatchDialer call")]
    assert notes.write_due_notes(db, writer, now=NOW) == 0  # written once


def test_a_new_result_code_rewrites_the_note(db):
    phone = _lead(db)
    row = _call(db, phone, disposition=None)
    writer = FakeWriter()
    notes.write_due_notes(db, writer, now=NOW)
    db.execute(text("UPDATE lending.call_dispositions SET disposition = 'BOOKED' WHERE id = :id"), {"id": row})
    notes.write_due_notes(db, writer, now=NOW)
    assert writer.tags[-1] == ("ghl-9", ["called-in-batchdialer", "bd-connected", "bd-booked"])
    assert writer.fields[0][2].endswith("no result yet, 1 BatchDialer call")


def test_a_failed_write_is_retried_then_given_up(db, monkeypatch):
    monkeypatch.setattr(notes, "BD_NOTE_MAX_ATTEMPTS", 2)
    phone = _lead(db)
    _call(db, phone)
    failing = FakeWriter(fail=True)
    assert notes.write_due_notes(db, failing, now=NOW) == 0
    assert notes.write_due_notes(db, failing, now=NOW) == 0
    assert notes.write_due_notes(db, FakeWriter(), now=NOW) == 0  # attempts used up


def test_phones_without_a_ghl_contact_ghl_calls_and_old_calls_are_skipped(db):
    phone = _lead(db, contact=None)
    _call(db, phone)
    other = _lead(db)
    _call(db, other, source="ghl")
    _call(db, other, ended=NOW - timedelta(days=2))
    assert notes.write_due_notes(db, FakeWriter(), now=NOW) == 0


def test_calls_without_a_ghl_contact_do_not_fill_the_sweep_batch(db):
    for minutes in range(notes.SWEEP_LIMIT):
        _call(db, f"+1727555{minutes:04d}", ended=NOW - timedelta(hours=2, minutes=minutes))
    phone = _lead(db)
    _call(db, phone)
    writer = FakeWriter()
    assert notes.write_due_notes(db, writer, now=NOW) == 1
    assert writer.tags == [("ghl-9", ["called-in-batchdialer"])]


@pytest.mark.parametrize("code, tags", [
    ("NO_ANSWER", ["called-in-batchdialer"]),
    ("CONNECTED_NOT_INTERESTED", ["called-in-batchdialer", "bd-connected"]),
    ("CALLBACK_REQUESTED", ["called-in-batchdialer", "bd-connected", "bd-callback"]),
    (None, ["called-in-batchdialer"]),
])
def test_result_tags(code, tags):
    assert notes.result_tags(code) == tags


def test_off_or_halted_writes_nothing(monkeypatch):
    settings = notes.get_settings()
    monkeypatch.setattr(settings, "lending_bd_result_to_ghl_enabled", False)
    assert notes.write_notes(writer_factory=lambda: FakeWriter()) == 0
    monkeypatch.setattr(settings, "lending_bd_result_to_ghl_enabled", True)
    monkeypatch.setattr(notes, "is_globally_halted", lambda: True)
    assert notes.write_notes(writer_factory=lambda: FakeWriter()) == 0


def test_write_notes_uses_its_own_session(db, monkeypatch):
    monkeypatch.setattr(notes.get_settings(), "lending_bd_result_to_ghl_enabled", True)
    phone = _lead(db)
    _call(db, phone, ended=datetime.now(timezone.utc) - timedelta(minutes=1))
    writer = FakeWriter()

    @contextmanager
    def same():
        yield db

    assert notes.write_notes(sessions=same, writer_factory=lambda: writer) == 1


def test_follow_up_schedules_the_ghl_note_for_a_finished_call():
    tasks = []
    base = dict(row_id=7, call_id="c", phone="+18135550100", caller_seat=None, disposition=None,
                opt_out_propagated=False)
    follow_up(RecordedCall(call_ended=True, **base), lambda fn, *a: tasks.append(fn.__name__))
    assert "write_note_in_background" in tasks
    tasks.clear()
    follow_up(RecordedCall(call_ended=False, **base), lambda fn, *a: tasks.append(fn.__name__))
    assert "write_note_in_background" not in tasks


def test_the_dropdown_field_holds_only_the_latest_result_code(db, monkeypatch):
    monkeypatch.setattr(notes.get_settings(), "lending_ghl_cf_last_bd_result", "cf-last-result")
    phone = _lead(db)
    row = _call(db, phone, disposition="NO_ANSWER")
    writer = FakeWriter()
    notes.write_due_notes(db, writer, now=NOW)
    db.execute(text("UPDATE lending.call_dispositions SET disposition = 'BOOKED' WHERE id = :id"), {"id": row})
    notes.write_due_notes(db, writer, now=NOW)
    assert [f for f in writer.fields if f[1] == "cf-last-result"] == [("ghl-9", "cf-last-result", "NO_ANSWER"),
                                                                       ("ghl-9", "cf-last-result", "BOOKED")]


def test_no_result_yet_leaves_the_dropdown_alone(db, monkeypatch):
    monkeypatch.setattr(notes.get_settings(), "lending_ghl_cf_last_bd_result", "cf-last-result")
    _call(db, _lead(db), disposition=None)
    writer = FakeWriter()
    notes.write_due_notes(db, writer, now=NOW)
    assert all(f[1] != "cf-last-result" for f in writer.fields)


@pytest.mark.parametrize("code, expected", [("BOOKED", "BOOKED"), ("CALL_FAILED", "NO_ANSWER"),
                                            ("DATA_NURTURE_ONLY", "NURTURE"), (None, None), ("SOMETHING_ELSE", None)])
def test_result_code_maps_legacy_codes_and_drops_unknown_ones(code, expected):
    assert notes.result_code(code) == expected
