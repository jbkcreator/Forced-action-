"""Item 4 (b) caller attribution + gate_failed Gated / Nurture counts. Pure: fake sessions and dialers, no DB.

* GHL calls name their caller from the message ``userId`` (GHL_USER_NAMES); a missing or unknown id reads
  "GHL (unknown)" until the outcome webhook's ``{{user.name}}`` fills it in on match
* the shared BatchDialer seat is shown as one label, never as the login owner's name
* Gated / Nurture sent count NURTURE only when gate_failed is set; legacy GATE_FAILED_NURTURE rows still count
"""
from __future__ import annotations

import dataclasses
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

from config import lending_dispositions as cfg
from src.lending import call_pipeline as cp
from src.lending import ghl_call_import as imp
from src.lending import ghl_disposition_match as m
from src.lending import scoreboard as sb

NOW = datetime(2026, 11, 3, 18, 0, tzinfo=timezone.utc)
JOSH, DAVID = "3W4lyuTpPSgBjgiJMzJk", "57m9LlZrO1rV2A5cTKxI"


# ---- config ---------------------------------------------------------------------------------------------------------

def test_ghl_user_ids_map_to_people():
    assert cfg.GHL_USER_NAMES[JOSH] == "Josh Kantor" and cfg.GHL_USER_NAMES[DAVID] == "David Willis"
    assert cfg.GHL_USER_NAMES["YoBwFUwHL1fylqvqjzA1"] == cfg.GHL_USER_NAMES["0mboWYLanQkMz4RG3IbL"] == "Hari K"


def test_ghl_caller_name_falls_back_to_unknown():
    assert cfg.ghl_caller_name(JOSH) == "Josh Kantor"
    assert cfg.ghl_caller_name(None) == cfg.ghl_caller_name("") == cfg.ghl_caller_name("nobody") == cfg.GHL_UNKNOWN_CALLER
    assert cfg.GHL_UNKNOWN_CALLER == "GHL (unknown)"


def test_shared_seat_label_configured():
    assert cfg.SHARED_SEAT_LABELS == {"118131": "BatchDialer (shared seat)"}


# ---- GHL import -----------------------------------------------------------------------------------------------------

def _message(**kw):
    base = {"id": "m1", "direction": "outbound", "dateAdded": "2026-11-03T17:00:00Z",
            "meta": {"call": {"status": "completed", "duration": 90}}}
    return {**base, **kw}


def test_import_names_the_caller_from_the_message_user():
    row = imp._call_row(_message(userId=JOSH), "c-1", "+18135550142", NOW - timedelta(days=1))
    assert row["caller_name"] == "Josh Kantor" and row["seat"] == "ghl"


def test_import_without_user_reads_unknown():
    row = imp._call_row(_message(), "c-1", "+18135550142", NOW - timedelta(days=1))
    assert row["caller_name"] == "GHL (unknown)"


def test_import_upsert_writes_caller_name_and_only_upgrades_unknown():
    sql = " ".join(str(imp._UPSERT).split())
    assert "caller_name" in sql and ":caller_name" in sql
    assert "d.caller_name IS NULL OR d.caller_name = :unknown_caller" in sql


# ---- matcher --------------------------------------------------------------------------------------------------------

class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _MatchDb:
    def __init__(self, events, calls):
        self.events, self.calls, self.updates = events, calls, []

    @contextmanager
    def _nested(self):
        yield

    def begin_nested(self):
        return self._nested()

    def execute(self, sql, params=None):
        s = str(sql)
        if s.lstrip().startswith("UPDATE"):
            self.updates.append((" ".join(s.split()), params))
            return _Result([])
        if "FROM lending.ghl_disposition_events" in s:
            return _Result(self.events)
        return _Result(self.calls)


def _ev(**kw):
    base = {"id": 1, "phone": "+18135550142", "code": "NURTURE", "ghl_call_id": None, "called_at": None,
            "received_at": NOW, "caller_name": "David Willis", "gate_failed": True}
    return {**base, **kw}


def _call(i=7):
    return {"id": i, "phone": "+18135550142", "dialer_call_id": f"ghl:m{i}", "call_started_at": NOW - timedelta(minutes=4),
            "disposition": None, "unfunded_cause": None}


def test_pending_query_reads_caller_and_gate():
    sql = " ".join(str(m._PENDING).split())
    assert "caller_name" in sql and "gate_failed" in sql


def test_match_writes_gate_failed_and_fills_unknown_caller():
    db = _MatchDb([_ev()], [_call()])
    assert m.attach_pending(db, NOW) == [7]
    sql, (params,) = db.updates[0]
    assert "gate_failed = :gate" in sql
    assert "caller_name = CASE WHEN caller_name IS NULL OR caller_name = :unknown_caller" in sql
    assert params["gate"] is True and params["caller"] == "David Willis" and params["unknown_caller"] == "GHL (unknown)"


def test_match_without_gate_or_user_passes_nulls():
    db = _MatchDb([_ev(gate_failed=None, caller_name=None)], [_call()])
    m.attach_pending(db, NOW)
    _, (params,) = db.updates[0]
    assert params["gate"] is None and params["caller"] is None


def test_matcher_docstring_todo_is_gone():
    assert "TODO" not in (m.__doc__ or "")


# ---- scoreboard -----------------------------------------------------------------------------------------------------

class _BoardDb:
    def __init__(self):
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append((" ".join(str(sql).split()), params))
        return type("R", (), {"all": lambda s: []})()


def test_caller_key_labels_the_shared_seat():
    key = sb._KEYS["caller"]
    assert "CASE WHEN caller_seat = '118131' THEN 'BatchDialer (shared seat)'" in key
    assert "COALESCE(NULLIF(caller_name, ''), NULLIF(caller_seat, ''), '(unknown)')" in key


def test_gated_and_nurture_count_nurture_only_with_gate_failed():
    db = _BoardDb()
    sb._query(db, "caller", date(2026, 11, 3))
    sql, params = db.sql[0]
    assert "count(*) FILTER (WHERE disposition = ANY(:gated) OR (disposition = :nurture_code AND gate_failed IS TRUE)) AS gated" in sql
    assert "count(*) FILTER (WHERE disposition = ANY(:nurture) OR (disposition = :nurture_code AND gate_failed IS TRUE)) AS nurture" in sql
    assert params["nurture_code"] == "NURTURE"
    assert "GATE_FAILED_NURTURE" in params["gated"] and "BOOKED" in params["gated"]
    assert params["nurture"] == ["GATE_FAILED_NURTURE"]


# ---- BatchDialer gate field -----------------------------------------------------------------------------------------

class _PipeDb:
    def __init__(self):
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append((" ".join(str(sql).split()), params))
        return type("R", (), {"scalar": lambda s: None, "first": lambda s: None})()


class _Dialer:
    def __init__(self, fields):
        self.fields = fields

    def get_contact_customfields(self, contact_id, quick=False):
        return self.fields


def _bd_event():
    from src.lending.dispositions import DialerCallEvent
    fields = {f.name: None for f in dataclasses.fields(DialerCallEvent)}
    return DialerCallEvent(**{**fields, "call_id": "bd-1", "contact_id": "k-1", "direction": "outbound", "duration": 60})


def test_bd_gate_failed_field_sets_the_column():
    assert cfg.GATE_FAILED_FIELD == "gate_failed"
    db = _PipeDb()
    cp.capture_gate_flag(db, 5, {"gate_failed": "Yes"})
    gate = [p for s, p in db.sql if "SET gate_failed = true" in s]
    assert gate == [{"id": 5}]


def test_bd_without_gate_field_leaves_the_column():
    db = _PipeDb()
    cp.capture_gate_flag(db, 5, {"gate_failed": "no"})
    assert not [s for s, _ in db.sql if "gate_failed" in s]
