"""PR #400 review round 2: causal phone matching, bounded unmatched claim, set membership, dedupe identity."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config import lending_dispositions as cfg
from src.lending import ghl_disposition_match as m

NOW = datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc)


def _call(i, minutes_from_now, **kw):
    return {"id": i, "phone": "+18135550142", "dialer_call_id": f"ghl:m{i}",
            "call_started_at": NOW + timedelta(minutes=minutes_from_now), "disposition": None, **kw}


def _event(**kw):
    return {"id": 1, "phone": "+18135550142", "code": "NO_ANSWER", "ghl_call_id": None, "called_at": None,
            "received_at": NOW, **kw}


# ---- Spec 2: an outcome never lands on a call that started after it ----------------------------------------------

def test_a_later_call_is_never_chosen_for_an_earlier_outcome():
    earlier, later = _call(1, -10), _call(2, +10)
    assert m.choose_call(_event(), [earlier, later])["id"] == 1


def test_clock_skew_allows_a_call_started_just_after_the_outcome_time():
    skew = cfg.GHL_DISPOSITION_CLOCK_SKEW_MINUTES
    assert skew == 2
    assert m.choose_call(_event(), [_call(1, skew - 1)])["id"] == 1
    assert m.choose_call(_event(), [_call(1, skew + 1)]) is None


def test_the_latest_call_before_the_outcome_wins():
    assert m.choose_call(_event(), [_call(1, -25), _call(2, -5), _call(3, +20)])["id"] == 2


def test_called_at_is_the_reference_when_sent():
    ev = _event(called_at=NOW - timedelta(minutes=20))
    assert m.choose_call(ev, [_call(1, -21), _call(2, -5)])["id"] == 1


def test_pending_sql_mirrors_the_causal_window():
    sql = " ".join(str(m._PENDING).split())
    assert ":skew" in sql and "+ make_interval(mins => :w)" not in sql


# ---- Std 2: bounded unmatched claim ----------------------------------------------------------------------------------

def test_unmatched_claim_is_bounded():
    from src.tasks import lending_missing_disposition_alert as alert
    sql = " ".join(str(alert._CLAIM_UNMATCHED).split())
    assert "ORDER BY received_at" in sql and "LIMIT :limit" in sql
    assert cfg.GHL_DISPOSITION_ALERT_BATCH == 200


# ---- Spec 1 (c): gate flag + contact are part of the delivery identity ---------------------------------------------

class _RecordDb:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(str(sql).split()), params))

        class R:
            def scalar(self_):
                return None

            def scalar_one(self_):
                return 7
        return R()


def test_dedupe_compares_gate_flag_and_contact():
    ev = m.parse_event({"disposition": "Nurture", "phone": "8135550142", "contact_id": "c1", "gate_failed": "yes"})
    db = _RecordDb()
    m.record_event(db, ev, {})
    lock, find = db.executed[0], db.executed[1]
    assert "gate_failed IS NOT DISTINCT FROM :gate" in find[0]
    assert "contact_id IS NOT DISTINCT FROM :contact" in find[0]
    assert find[1]["gate"] is True and find[1]["contact"] == "c1"
    assert "True" in lock[1]["key"] and "c1" in lock[1]["key"]


# ---- Spec 1 (a): call identity can be required ----------------------------------------------------------------------

def test_identity_not_required_by_default():
    assert cfg.GHL_DISPOSITION_REQUIRE_CALL_IDENTITY is False
    assert m.parse_event({"disposition": "Voicemail", "phone": "8135550142"}).code == "LEFT_VOICEMAIL"


def test_when_required_an_event_without_call_id_or_called_at_is_refused(monkeypatch):
    monkeypatch.setattr(m, "GHL_DISPOSITION_REQUIRE_CALL_IDENTITY", True)
    with pytest.raises(m.InvalidDispositionEvent) as info:
        m.parse_event({"disposition": "Voicemail", "phone": "8135550142"})
    assert info.value.reason == m.REASON_NO_CALL_IDENTITY
    assert m.REASON_NO_CALL_IDENTITY in m.INVALID_EVENT_REASONS
    assert m.parse_event({"disposition": "Voicemail", "phone": "8135550142",
                          "called_at": "2026-11-03T15:00:00Z"}).called_at is not None
    assert m.parse_event({"disposition": "Voicemail", "call_id": "abc"}).ghl_call_id == "abc"


# ---- Std 3: membership by set ---------------------------------------------------------------------------------------

def test_attach_tracks_matched_ids_in_a_set():
    import inspect
    assert "matched_set" in inspect.getsource(m._attach)
