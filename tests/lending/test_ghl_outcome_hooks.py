"""Dev C hooks on GHL call outcomes: one call per stored (new) outcome, one call per matched batch, both inside the
caller's transaction in a savepoint; a failing hook never loses the outcome or the match. text_consent passes through."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.lending import ghl_disposition_match as m
from src.lending import ghl_outcome_hooks as hooks

NOW = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)
CALLED = "2026-11-03T14:55:00Z"


class _Result:
    def __init__(self, rows=(), scalar=None):
        self.rows, self._scalar = list(rows), scalar

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def scalar(self):
        return self._scalar

    def scalar_one(self):
        return self._scalar


class _Nested:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.savepoints += 1
        return self

    def __exit__(self, *exc):
        return False  # an error propagates, as SQLAlchemy's savepoint does after rolling back


class _Db:
    """Answers the record path (lock / repeat lookup / insert) and the match path (pending / candidates / updates)."""

    def __init__(self, *, existing=None, events=(), calls=()):
        self.existing, self.events, self.calls = existing, list(events), list(calls)
        self.executed, self.savepoints = [], 0

    def begin_nested(self):
        return _Nested(self)

    def execute(self, sql, params=None):
        s = " ".join(str(sql).split())
        self.executed.append((s, params))
        if "pg_advisory_xact_lock" in s or s.startswith("UPDATE"):
            return _Result()
        if s.startswith("SELECT id FROM lending.ghl_disposition_events"):
            return _Result(scalar=self.existing)
        if s.startswith("INSERT INTO lending.ghl_disposition_events"):
            return _Result(scalar=99)
        if "FROM lending.ghl_disposition_events" in s:
            return _Result(self.events)
        return _Result(self.calls)


@pytest.fixture
def seen(monkeypatch):
    got = {"received": [], "matched": []}
    monkeypatch.setattr(hooks, "on_ghl_outcome_received", lambda db, ev: got["received"].append(ev))
    monkeypatch.setattr(hooks, "on_ghl_outcome_matched", lambda db, items: got["matched"].append(list(items)))
    return got


def _parsed(**extra):
    return m.parse_event({"disposition": "DNC Request", "phone": "8135550142", "contact_id": "c-1",
                          "user": "Josh Kantor", **extra})


# ---- text_consent -----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("Yes", True), ("yes", True), ("TRUE", True), ("1", True), ("y", True),
    ("No", False), ("false", False), ("0", False), ("n", False),
    ("maybe", None), ("{{contact.text_consent}}", None), ("null", None), ("", None),
])
def test_text_consent_is_parsed_from_the_body(raw, expected):
    assert _parsed(text_consent=raw).text_consent is expected


def test_text_consent_accepts_the_camel_case_key():
    assert _parsed(textConsent="Yes").text_consent is True


def test_text_consent_is_none_when_not_sent():
    assert _parsed().text_consent is None


def test_text_consent_is_stored_in_raw_event():
    db = _Db()
    m.record_event(db, _parsed(text_consent="No"), {})
    insert = next(p for s, p in db.executed if s.startswith("INSERT INTO lending.ghl_disposition_events"))
    assert json.loads(insert["raw_event"])["text_consent"] is False


# ---- received hook ------------------------------------------------------------------------------------------------------

def test_a_new_outcome_calls_the_received_hook_once_with_its_fields(seen):
    db = _Db()
    assert m.record_event(db, _parsed(called_at=CALLED, text_consent="yes", gate_failed="no"), {}) == (99, False)
    [ev] = seen["received"]
    assert isinstance(ev, hooks.ReceivedOutcome)
    assert (ev.event_id, ev.contact_id, ev.phone, ev.code, ev.caller_name) == (
        99, "c-1", "+18135550142", "DNC_REQUEST", "Josh Kantor")
    assert ev.called_at == datetime(2026, 11, 3, 14, 55, tzinfo=timezone.utc)
    assert ev.gate_failed is False and ev.text_consent is True
    assert ev.received_at is not None and ev.received_at.tzinfo is not None
    assert db.savepoints == 1  # the hook runs in its own savepoint


def test_an_outcome_without_call_identity_also_calls_the_received_hook(seen):
    m.record_event(_Db(), _parsed(), {})
    assert len(seen["received"]) == 1


def test_a_repeated_delivery_does_not_call_the_received_hook(seen):
    assert m.record_event(_Db(existing=42), _parsed(called_at=CALLED), {}) == (42, True)
    assert seen["received"] == []


def test_a_failing_received_hook_keeps_the_stored_outcome(monkeypatch, caplog):
    def boom(db, ev):
        raise RuntimeError("secret detail +18135550142")
    monkeypatch.setattr(hooks, "on_ghl_outcome_received", boom)
    db = _Db()
    assert m.record_event(db, _parsed(called_at=CALLED), {}) == (99, False)
    assert "RuntimeError" in caplog.text and "secret detail" not in caplog.text and "8135550142" not in caplog.text


# ---- matched hook -------------------------------------------------------------------------------------------------------

def _event(i, **kw):
    return {"id": i, "phone": f"+1813555014{i}", "code": "DNC_REQUEST", "ghl_call_id": None, "called_at": None,
            "received_at": NOW, "caller_name": "Josh Kantor", "gate_failed": None, "contact_id": f"c-{i}",
            "text_consent": False, **kw}


def _call(i, phone):
    return {"id": i, "phone": phone, "dialer_call_id": f"ghl:m{i}", "call_started_at": NOW - timedelta(minutes=2),
            "disposition": None, "unfunded_cause": None}


def test_the_matched_hook_gets_one_batch_with_each_matched_outcome(seen):
    db = _Db(events=[_event(1), _event(2, code="BOOKED", text_consent=None)],
             calls=[_call(7, "+18135550141"), _call(8, "+18135550142")])
    assert m.attach_pending(db, NOW) == [7, 8]
    [batch] = seen["matched"]
    assert all(isinstance(x, hooks.MatchedOutcome) for x in batch)
    first, second = batch
    assert (first.call_row_id, first.dialer_call_id, first.event_id, first.contact_id, first.phone, first.code,
            first.caller_name, first.text_consent) == (
        7, "ghl:m7", 1, "c-1", "+18135550141", "DNC_REQUEST", "Josh Kantor", False)
    assert (second.call_row_id, second.code, second.text_consent) == (8, "BOOKED", None)


def test_the_matched_hook_is_called_after_the_writes(seen, monkeypatch):
    order = []
    db = _Db(events=[_event(1)], calls=[_call(7, "+18135550141")])
    real_execute = db.execute

    def execute(sql, params=None):
        if str(sql).lstrip().startswith("UPDATE"):
            order.append("write")
        return real_execute(sql, params)
    db.execute = execute
    monkeypatch.setattr(hooks, "on_ghl_outcome_matched", lambda db_, items: order.append("hook"))
    m.attach_pending(db, NOW)
    assert order == ["write", "write", "hook"]


def test_no_match_no_matched_hook(seen):
    m.attach_pending(_Db(events=[], calls=[]), NOW)
    assert seen["matched"] == []


def test_a_failing_matched_hook_keeps_the_match(monkeypatch, caplog):
    def boom(db, items):
        raise RuntimeError("detail +18135550141")
    monkeypatch.setattr(hooks, "on_ghl_outcome_matched", boom)
    db = _Db(events=[_event(1)], calls=[_call(7, "+18135550141")])
    assert m.attach_pending(db, NOW) == [7]
    assert sum(1 for s, _ in db.executed if s.startswith("UPDATE")) == 2
    assert "RuntimeError" in caplog.text and "8135550141" not in caplog.text


def test_the_pending_read_selects_contact_and_text_consent():
    db = _Db(events=[], calls=[])
    m.attach_pending(db, NOW)
    pending = next(s for s, _ in db.executed if "FROM lending.ghl_disposition_events" in s)
    assert "e.contact_id" in pending and "raw_event->>'text_consent'" in pending


# ---- G4: what the hooks do -----------------------------------------------------------------------------------------

@pytest.fixture
def acted(monkeypatch):
    got = {"opt_out": [], "consent": [], "missed": []}
    monkeypatch.setattr(hooks, "propagate_opt_out", lambda db, **kw: got["opt_out"].append(kw) or 5)
    monkeypatch.setattr(hooks, "record_consent", lambda db, phone, source, **kw: got["consent"].append((phone, source, kw)))
    monkeypatch.setattr(hooks, "queue_missed_call",
                        lambda db, call_id, phone, did, record, ended: got["missed"].append((call_id, phone, did, ended)))
    return got


class _HookDb:
    def __init__(self, rows=()):
        self.rows, self.executed = list(rows), []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(str(sql).split()), params))
        return _Result(self.rows)


def _received(code="DNC_REQUEST", phone="+18135550142", **kw):
    return hooks.ReceivedOutcome(event_id=11, contact_id="c-1", phone=phone, code=code, caller_name="Josh Kantor",
                                 called_at=None, received_at=NOW, gate_failed=None, text_consent=None, **kw)


def _matched(i, code, *, phone="+18135550142", consent=None):
    return hooks.MatchedOutcome(call_row_id=i, dialer_call_id=f"ghl:m{i}", event_id=100 + i, contact_id="c-1",
                                phone=phone, code=code, caller_name="Josh Kantor", gate_failed=None,
                                text_consent=consent)


def test_a_dnc_outcome_opts_the_number_out_as_soon_as_it_arrives(acted):
    hooks.on_ghl_outcome_received(_HookDb(), _received())
    assert acted["opt_out"] == [{"phone": "+18135550142", "source_ref": "ghl-disposition:11", "actor": "Josh Kantor"}]


@pytest.mark.parametrize("code, phone", [("BOOKED", "+18135550142"), ("DNC_REQUEST", None)])
def test_other_outcomes_or_no_phone_opt_nothing_out_on_arrival(acted, code, phone):
    hooks.on_ghl_outcome_received(_HookDb(), _received(code, phone))
    assert acted["opt_out"] == []


def test_a_matched_dnc_uses_the_same_opt_out_and_stamps_the_call_so_the_sweep_skips_it(acted):
    db = _HookDb()
    hooks.on_ghl_outcome_matched(db, [_matched(7, "DNC_REQUEST", consent=True)])
    assert acted["opt_out"] == [{"phone": "+18135550142", "source_ref": "ghl-disposition:107", "actor": "Josh Kantor"}]
    stamp = next(p for s, p in db.executed if s.startswith("UPDATE lending.call_dispositions SET opt_out_propagated_at"))
    assert stamp == {"ids": [7]}
    assert acted["consent"] == []  # a DNC never records consent


def test_text_consent_yes_on_an_answered_call_is_saved(acted):
    hooks.on_ghl_outcome_matched(_HookDb(), [_matched(7, "CALLBACK_REQUESTED", consent=True)])
    assert acted["consent"] == [("+18135550142", "on_call_yes", {"call_id": "ghl:m7", "captured_by": "Josh Kantor"})]


@pytest.mark.parametrize("code, consent", [
    ("CALLBACK_REQUESTED", False), ("CALLBACK_REQUESTED", None), ("NO_ANSWER", True), ("LEFT_VOICEMAIL", True),
    ("BAD_NUMBER", True),
])
def test_consent_needs_a_yes_and_someone_who_picked_up(acted, code, consent):
    hooks.on_ghl_outcome_matched(_HookDb(), [_matched(7, code, consent=consent)])
    assert acted["consent"] == []


def test_an_unanswered_outcome_queues_the_missed_call_event_with_the_call_end(acted):
    ended = NOW - timedelta(minutes=14)
    db = _HookDb(rows=[(7, ended, "+16562211100"), (8, ended, None)])
    hooks.on_ghl_outcome_matched(db, [_matched(7, "NO_ANSWER"), _matched(8, "LEFT_VOICEMAIL"), _matched(9, "BOOKED")])
    assert acted["missed"] == [("ghl:m7", "+18135550142", "+16562211100", ended), ("ghl:m8", "+18135550142", None, ended)]
    assert sum(1 for s, _ in db.executed if s.startswith("SELECT id, call_ended_at")) == 1  # one read per batch


def test_an_unanswered_call_with_no_end_time_or_phone_is_not_queued(acted):
    hooks.on_ghl_outcome_matched(_HookDb(rows=[(7, None, None)]),
                                 [_matched(7, "NO_ANSWER"), _matched(8, "NO_ANSWER", phone=None)])
    assert acted["missed"] == []
