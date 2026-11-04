"""GHL call results written onto the phone's BatchDialer contact card (custom field card_ghl_call)."""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending import ghl_result_to_bd as cards
from src.lending.models import LendingDialerLoadRecord

NOW = datetime(2026, 11, 4, 16, 0, tzinfo=timezone.utc)
ENDED = NOW - timedelta(minutes=5)  # 10:55 ET


class FakeWriter:
    def __init__(self, fail=False):
        self.writes: list[tuple[str, dict]] = []
        self.fail = fail

    def set_custom_fields(self, contact_id, values):
        if self.fail:
            raise RuntimeError("BatchDialer down")
        self.writes.append((str(contact_id), dict(values)))


@pytest.fixture
def db(lending_db):
    from migrations.apply_lending_lendingflow import apply_to as apply_lendingflow

    conn = lending_db.connection()
    apply_lendingflow(conn)  # also adds the bd_note_* columns
    LendingDialerLoadRecord.__table__.create(conn, checkfirst=True)
    return lending_db


def _phone():
    return f"+1813555{uuid.uuid4().int % 10_000:04d}"


def _loaded(db, phone, contact_id="900", active=True):
    db.execute(text("INSERT INTO lending.dialer_load_records (run_id, pool, source_record_ref, phone, phone_hash, "
                    "campaign_tag, dialer_contact_id, active, deactivated_at, deactivation_reason) "
                    "VALUES ('t', 'lendingflow', :ref, :p, 'h', 'LendingFlow', :c, :a, "
                    "CASE WHEN :a THEN NULL ELSE now() END, CASE WHEN :a THEN NULL ELSE 'handled_in_ghl' END)"),
               {"ref": f"lendingflow:{uuid.uuid4()}", "p": phone, "c": contact_id, "a": active})


def _ghl_call(db, phone, *, disposition=None, raw="ghl:no-answer", ended=ENDED, source="ghl"):
    return db.execute(text("INSERT INTO lending.call_dispositions (dialer_call_id, call_source, direction, phone, "
                           "disposition, disposition_raw, call_ended_at, raw_event) VALUES (:id, :src, 'outbound', :p, "
                           ":d, :raw, :e, CAST('{}' AS jsonb)) RETURNING id"),
                      {"id": f"ghl:{uuid.uuid4().hex}", "src": source, "p": phone, "d": disposition, "raw": raw,
                       "e": ended}).scalar()


def test_a_ghl_call_is_written_onto_the_batchdialer_card_once(db):
    phone = _phone()
    _loaded(db, phone, "901")
    _ghl_call(db, phone)
    writer = FakeWriter()
    assert cards.write_due_card_notes(db, writer, now=NOW) == 1
    assert writer.writes == [("901", {"card_ghl_call": "2026-11-04 10:55 ET, no answer, 1 GHL call"})]
    assert cards.write_due_card_notes(db, writer, now=NOW) == 0


def test_a_result_code_that_arrives_later_rewrites_the_card(db):
    phone = _phone()
    _loaded(db, phone, "902")
    row = _ghl_call(db, phone)
    writer = FakeWriter()
    cards.write_due_card_notes(db, writer, now=NOW)
    db.execute(text("UPDATE lending.call_dispositions SET disposition = 'CALLBACK_REQUESTED' WHERE id = :id"),
               {"id": row})
    cards.write_due_card_notes(db, writer, now=NOW)
    assert writer.writes[-1] == ("902", {"card_ghl_call": "2026-11-04 10:55 ET, callback requested, 1 GHL call"})


def test_phones_not_in_batchdialer_dialer_calls_and_old_calls_are_skipped(db):
    not_loaded, inactive, loaded = _phone(), _phone(), _phone()
    _ghl_call(db, not_loaded)
    _loaded(db, inactive, active=False)
    _ghl_call(db, inactive)
    _loaded(db, loaded)
    _ghl_call(db, loaded, source="batchdialer")
    _ghl_call(db, loaded, ended=NOW - timedelta(days=2))
    assert cards.write_due_card_notes(db, FakeWriter(), now=NOW) == 0


def test_a_failed_write_is_retried_then_given_up(db, monkeypatch):
    monkeypatch.setattr(cards, "GHL_NOTE_MAX_ATTEMPTS", 2)
    phone = _phone()
    _loaded(db, phone)
    _ghl_call(db, phone)
    assert cards.write_due_card_notes(db, FakeWriter(fail=True), now=NOW) == 0
    assert cards.write_due_card_notes(db, FakeWriter(fail=True), now=NOW) == 0
    assert cards.write_due_card_notes(db, FakeWriter(), now=NOW) == 0  # attempts used up


@pytest.mark.parametrize("disposition, raw, expected", [
    ("NO_ANSWER", "ghl:no-answer", "no answer"),
    (None, "ghl:completed", "completed"),
    (None, None, "no result yet"),
])
def test_card_text(disposition, raw, expected):
    assert cards.ghl_call_text(ENDED, disposition, raw, 2) == f"2026-11-04 10:55 ET, {expected}, 2 GHL calls"


def test_off_or_halted_writes_nothing(monkeypatch):
    settings = cards.get_settings()
    monkeypatch.setattr(settings, "lending_ghl_result_to_bd_enabled", False)
    assert cards.write_card_notes(writer_factory=FakeWriter) == 0
    monkeypatch.setattr(settings, "lending_ghl_result_to_bd_enabled", True)
    monkeypatch.setattr(cards, "is_globally_halted", lambda: True)
    assert cards.write_card_notes(writer_factory=FakeWriter) == 0


def test_the_sweep_entry_point_uses_its_own_session(db, monkeypatch):
    monkeypatch.setattr(cards.get_settings(), "lending_ghl_result_to_bd_enabled", True)
    phone = _phone()
    _loaded(db, phone)
    _ghl_call(db, phone, ended=datetime.now(timezone.utc) - timedelta(minutes=1))
    writer = FakeWriter()

    @contextmanager
    def same():
        yield db

    assert cards.write_card_notes(sessions=same, writer_factory=lambda: writer) == 1
