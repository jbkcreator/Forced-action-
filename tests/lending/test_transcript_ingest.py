"""T-12 audio fallback: fetch each finished call's transcript and extract it, inside the CDR poller."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config import lending_dispositions as cfg
from src.lending import cdr_poller
from src.lending.dialer_port import DialerRequestError
from src.lending.transcript_ingest import PendingCall, ingest_calls

NOW = datetime(2026, 10, 19, 15, 0, tzinfo=timezone.utc)


def _call(call_id="c1", minutes_ago=1):
    return PendingCall(dialer_call_id=call_id, phone="+18135550101", call_ended_at=NOW - timedelta(minutes=minutes_ago))


class FakeDialer:
    def __init__(self, transcripts=None, errors=()):
        self.transcripts, self.errors, self.asked = transcripts or {}, set(errors), []

    def call_transcript(self, call_id):
        self.asked.append(call_id)
        if call_id in self.errors:
            raise DialerRequestError("boom", 500)
        return self.transcripts.get(call_id)


class Recorder:
    def __init__(self, fail=False):
        self.extracted, self.marked, self.fail = [], [], fail

    def extract(self, *, dialer_call_id, phone, transcript, extracted_at):
        if self.fail:
            raise RuntimeError("claude down")
        self.extracted.append((dialer_call_id, transcript, extracted_at))

    def mark(self, dialer_call_id, status):
        self.marked.append((dialer_call_id, status))


def _run(calls, dialer, rec):
    return ingest_calls(calls, dialer, now=NOW, extract=rec.extract, mark=rec.mark)


def test_ready_transcript_is_extracted_and_marked_done():
    rec = Recorder()
    counts = _run([_call()], FakeDialer({"c1": "agent: hi"}), rec)
    assert rec.extracted == [("c1", "agent: hi", NOW)]
    assert rec.marked == [("c1", cfg.TRANSCRIPT_DONE)]
    assert counts == {cfg.TRANSCRIPT_DONE: 1}


def test_not_ready_yet_is_left_pending_for_the_next_tick():
    rec = Recorder()
    counts = _run([_call()], FakeDialer({}), rec)
    assert rec.extracted == [] and rec.marked == []
    assert counts == {"not_ready": 1}


def test_fetch_error_is_retried_next_tick_and_does_not_stop_the_batch():
    rec = Recorder()
    counts = _run([_call("bad"), _call("ok")], FakeDialer({"ok": "agent: hi"}, errors={"bad"}), rec)
    assert rec.marked == [("ok", cfg.TRANSCRIPT_DONE)]
    assert counts == {"fetch_failed": 1, cfg.TRANSCRIPT_DONE: 1}


def test_extraction_error_leaves_the_call_pending():
    rec = Recorder(fail=True)
    counts = _run([_call()], FakeDialer({"c1": "agent: hi"}), rec)
    assert rec.marked == [] and counts == {"extract_failed": 1}


def test_call_past_the_wait_cap_is_marked_missing_without_a_fetch():
    rec = Recorder()
    dialer = FakeDialer({"old": "agent: hi"})
    cap_minutes = cfg.TRANSCRIPT_MAX_WAIT_SECONDS // 60
    counts = _run([_call("old", minutes_ago=cap_minutes + 1)], dialer, rec)
    assert dialer.asked == [] and rec.marked == [("old", cfg.TRANSCRIPT_MISSING)]
    assert counts == {cfg.TRANSCRIPT_MISSING: 1}


def test_poll_interval_is_five_seconds():
    """Runbook § 3 line 10 / § 5.1: 5-second ingestion."""
    assert cfg.CDR_POLL_SECONDS == 5


def test_transcript_step_does_nothing_while_the_switch_is_off(monkeypatch):
    monkeypatch.setattr(cdr_poller, "get_settings",
                        lambda: type("S", (), {"lending_transcript_ingest_enabled": False})())
    monkeypatch.setattr(cdr_poller, "get_dialer", lambda: pytest.fail("dialer must not be built"))
    cdr_poller.transcript_step()


def test_transcript_step_never_raises(monkeypatch):
    monkeypatch.setattr(cdr_poller, "get_settings",
                        lambda: type("S", (), {"lending_transcript_ingest_enabled": True})())

    def boom():
        raise RuntimeError("x")
    monkeypatch.setattr(cdr_poller, "get_dialer", boom)
    cdr_poller.transcript_step()






def test_pending_query_picks_answered_finished_calls_without_status(lending_db):
    from sqlalchemy import text
    from src.lending.transcript_ingest import expire_stale, fetch_pending, mark_status

    def add(call_id, *, talk, ended_minutes_ago, status=None):
        lending_db.execute(text(
            "INSERT INTO lending.call_dispositions (dialer_call_id, talk_duration_sec, call_ended_at, "
            "transcript_status, raw_event) VALUES (:id, :talk, :ended, :status, '{}'::jsonb)"),
            {"id": call_id, "talk": talk, "ended": NOW - timedelta(minutes=ended_minutes_ago), "status": status})

    add("t12-ready", talk=60, ended_minutes_ago=1)
    add("t12-unanswered", talk=0, ended_minutes_ago=1)
    add("t12-done", talk=60, ended_minutes_ago=1, status=cfg.TRANSCRIPT_DONE)
    add("t12-old", talk=60, ended_minutes_ago=600)
    assert expire_stale(lending_db, now=NOW) >= 1
    ids = [c.dialer_call_id for c in fetch_pending(lending_db, now=NOW, limit=50)]
    assert "t12-ready" in ids and not {"t12-unanswered", "t12-done", "t12-old"} & set(ids)
    mark_status(lending_db, "t12-ready", cfg.TRANSCRIPT_DONE)
    assert "t12-ready" not in [c.dialer_call_id for c in fetch_pending(lending_db, now=NOW, limit=50)]
