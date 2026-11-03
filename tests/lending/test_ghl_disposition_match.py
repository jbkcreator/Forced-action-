"""GHL call outcome (workflow webhook) -> holding table -> matched onto the imported GHL call (G3). Pure: no DB."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config.lending_dispositions import DISPOSITIONS, GHL_DISPOSITION_LABELS
from src.lending import ghl_disposition_match as m
from src.lending.ghl_consent_events import InvalidEvent

NOW = datetime(2026, 11, 3, 18, 0, tzinfo=timezone.utc)
BODY = {"contact_id": "c-1", "phone": "(813) 555-0142", "disposition": "Requested Appointment", "user": "Josh"}


# ---- parsing --------------------------------------------------------------------------------------------------------

def test_every_ghl_label_maps_to_one_of_the_ten_codes():
    assert set(GHL_DISPOSITION_LABELS.values()) <= set(DISPOSITIONS)


@pytest.mark.parametrize("raw, code", [
    ("Requested Appointment", "BOOKED"), ("Follow Up", "CALLBACK_REQUESTED"), ("Incorrect Number", "BAD_NUMBER"),
    ("Wrong Contact", "WRONG_PERSON"), ("Not Interested", "CONNECTED_NOT_INTERESTED"), ("Voicemail", "LEFT_VOICEMAIL"),
    ("No Answer", "NO_ANSWER"), ("Referred", "REFERRED"), ("DNC Request", "DNC_REQUEST"), ("Nurture", "NURTURE"),
    ("BOOKED", "BOOKED"), ("disp:NURTURE", "NURTURE"), ("dnc_request", "DNC_REQUEST"),
])
def test_ghl_label_tag_or_code_is_normalized(raw, code):
    assert m.ghl_code(raw) == code


def test_parse_reads_the_fields_and_normalizes_the_phone():
    ev = m.parse_event({**BODY, "call_id": "msg-9", "called_at": "2026-11-03T17:55:00Z", "gate_failed": "yes"})
    assert (ev.contact_id, ev.phone, ev.code, ev.disposition_raw) == ("c-1", "+18135550142", "BOOKED", "Requested Appointment")
    assert (ev.ghl_call_id, ev.caller_name, ev.gate_failed) == ("msg-9", "Josh", True)
    assert ev.called_at == datetime(2026, 11, 3, 17, 55, tzinfo=timezone.utc)


def test_parse_reads_ghl_custom_data_and_alternate_key_names():
    ev = m.parse_event({"contact_id": "c-1", "customData": {"lending_disposition": "Nurture", "phone": "8135550142",
                                                            "call_message_id": "msg-2", "gate_failed": "gate_failed"}})
    assert (ev.code, ev.phone, ev.ghl_call_id, ev.gate_failed) == ("NURTURE", "+18135550142", "msg-2", True)


def test_unresolved_optional_merge_fields_are_dropped():
    ev = m.parse_event({**BODY, "call_id": "null", "called_at": "undefined", "user": "{{user.name}}"})
    assert ev.ghl_call_id is None and ev.called_at is None and ev.caller_name is None and ev.gate_failed is None


def test_a_legacy_code_is_not_a_ghl_outcome():
    assert m.ghl_code("GATE_FAILED_NURTURE") is None and m.ghl_code("CALL_FAILED") is None


@pytest.mark.parametrize("body", [
    {**BODY, "disposition": ""},
    {**BODY, "disposition": "null"},
    {**BODY, "disposition": "Spoke To Aunt"},          # not one of the 10
    {"contact_id": "c-1", "disposition": "Nurture"},   # no phone and no call id: nothing to match on
])
def test_unusable_event_is_refused(body):
    with pytest.raises(InvalidEvent):
        m.parse_event(body)


# ---- choosing the call ---------------------------------------------------------------------------------------------

def _call(i, minutes_before, disposition=None, call_id=None):
    return {"id": i, "phone": "+18135550142", "dialer_call_id": call_id or f"ghl:m{i}", "call_started_at": NOW - timedelta(minutes=minutes_before),
            "disposition": disposition, "unfunded_cause": None}


def _ev(**kw):
    base = {"id": 1, "phone": "+18135550142", "code": "BOOKED", "ghl_call_id": None, "called_at": None, "received_at": NOW}
    return {**base, **kw}


def test_call_id_match_wins_even_outside_the_window():
    calls = [_call(1, 2), _call(2, 300, call_id="ghl:msg-9")]
    assert m.choose_call(_ev(ghl_call_id="msg-9"), calls)["id"] == 2


def test_with_a_call_id_the_phone_fallback_is_not_used():
    """The exact call is not imported yet: wait for it rather than guess another call."""
    assert m.choose_call(_ev(ghl_call_id="msg-9"), [_call(1, 2)]) is None


def test_fallback_takes_the_latest_open_call_inside_30_minutes():
    calls = [_call(1, 25), _call(2, 5), _call(3, 45), _call(4, 1, disposition="NO_ANSWER")]
    assert m.choose_call(_ev(), calls)["id"] == 2


def test_fallback_measures_from_called_at_when_sent():
    calls = [_call(1, 5), _call(2, 120)]
    assert m.choose_call(_ev(called_at=NOW - timedelta(minutes=118)), calls)["id"] == 2


def test_no_call_in_the_window_is_no_match():
    assert m.choose_call(_ev(), [_call(1, 31), _call(2, 60)]) is None


def test_a_dnc_call_is_never_overwritten_by_a_later_outcome():
    calls = [_call(2, 300, disposition="DNC_REQUEST", call_id="ghl:msg-9")]
    assert m.choose_call(_ev(ghl_call_id="msg-9", code="NO_ANSWER"), calls) is None


def test_an_exact_match_may_replace_an_earlier_outcome():
    """Josh changes the outcome: the second event names the same call."""
    calls = [_call(2, 3, disposition="NO_ANSWER", call_id="ghl:msg-9")]
    assert m.choose_call(_ev(ghl_call_id="msg-9", code="BOOKED"), calls)["id"] == 2


# ---- attach_pending ------------------------------------------------------------------------------------------------

class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _Db:
    """Answers the matcher's two SELECTs from canned rows and records every UPDATE."""

    def __init__(self, events, calls, fail=False):
        self.events, self.calls, self.fail, self.updates = events, calls, fail, []

    def begin_nested(self):
        db = self

        class _Ctx:
            def __enter__(self):
                if db.fail:
                    raise RuntimeError('relation "lending.ghl_disposition_events" does not exist')

            def __exit__(self, *exc):
                return False
        return _Ctx()

    def execute(self, sql, params=None):
        s = str(sql)
        if s.lstrip().startswith("UPDATE"):
            self.updates.append((s, params))
            return _Result([])
        if "FROM lending.ghl_disposition_events" in s:
            return _Result(self.events)
        return _Result(self.calls)


def test_attach_sets_the_outcome_and_links_the_event():
    db = _Db([_ev(code="CALLBACK_REQUESTED")], [_call(7, 4)])
    assert m.attach_pending(db, NOW) == [7]
    call_sql, call_params = db.updates[0]
    assert "lending.call_dispositions" in call_sql and "gate_failed" not in call_sql  # column not migrated yet
    assert call_params["d"] == "CALLBACK_REQUESTED" and call_params["id"] == 7 and call_params["cause"] == "timing"
    event_sql, event_params = db.updates[1]
    assert "lending.ghl_disposition_events" in event_sql and event_params == {"call": 7, "at": NOW, "id": 1}


def test_unmatched_event_is_left_pending():
    db = _Db([_ev()], [])
    assert m.attach_pending(db, NOW) == [] and db.updates == []


def test_missing_table_fails_soft():
    assert m.attach_pending(_Db([], [], fail=True), NOW) == []


# ---- schema ---------------------------------------------------------------------------------------------------------

def test_event_table_ddl_compiles_with_its_pending_index_and_call_link():
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex, CreateTable

    from src.lending.models import LendingGhlDispositionEvent as T

    ddl = str(CreateTable(T.__table__).compile(dialect=postgresql.dialect()))
    assert "lending.ghl_disposition_events" in ddl and "REFERENCES lending.call_dispositions (id)" in ddl
    (index,) = T.__table__.indexes
    assert "WHERE matched_call_id IS NULL" in str(CreateIndex(index).compile(dialect=postgresql.dialect()))


def test_migration_skips_an_existing_table_and_takes_no_lock():
    from migrations import apply_lending_ghl_disposition_events as mig

    class _Conn:
        def __init__(self):
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append(str(sql))
            return type("R", (), {"scalar": lambda s: 1})()

    conn = _Conn()
    mig.apply_to(conn)
    assert len(conn.sql) == 1 and "information_schema.tables" in conn.sql[0]
