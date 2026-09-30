"""CDR polling against the real lending schema (each test rolled back), with a fake BatchDialer."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from src.lending import call_pipeline, cdr_poll

pytestmark = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")


def _cdr(cid=1, **over):
    base = {"id": cid, "direction": "out", "callStartTime": "2026-09-29T15:55:00Z", "callEndTime": "2026-09-29T15:56:00Z",
            "did": "8135550100", "customerNumber": "8135550142", "disposition": "ANSWER", "duration": 60,
            "agent": {"id": 7, "firstname": "Sam", "lastname": "C"}, "contact": {"id": 9}, "campaign": {"id": 55, "name": "Builders"},
            "callRecordUrl": f"/api/callrecording/{cid}", "comments": []}
    base.update(over)
    return base


class FakeHttp:
    def __init__(self, last=None, days=None):
        self.last, self.days, self.calls = last or [], days or {}, []

    def __call__(self, method, path, **kw):
        self.calls.append(path)
        if path.startswith("/v2/cdrs/last"):
            return {"items": self.last}
        day = path.split("callDate=")[1][:10]
        pages = self.days.get(day, [[]])
        cursor = int(path.split("next_page=")[1].split("&")[0]) if "next_page=" in path else 0
        body = {"items": pages[cursor]}
        if cursor + 1 < len(pages):
            body["nextPage"] = str(cursor + 1)
        return body


@pytest.fixture(autouse=True)
def campaigns(monkeypatch):
    monkeypatch.setattr(call_pipeline, "get_settings", lambda: SimpleNamespace(lending_dialer_campaign_ids="55"))
    monkeypatch.setattr(cdr_poll, "follow_up", lambda recorded, add_task: None)


def _rows(db):
    return db.execute(text("SELECT dialer_call_id, direction, disposition FROM lending.call_dispositions ORDER BY 1")).all()


def test_poll_new_stores_the_call_and_replays_do_not_duplicate(lending_db, monkeypatch):
    http = FakeHttp(last=[_cdr(1)])
    seen = []
    monkeypatch.setattr(cdr_poll, "follow_up", lambda recorded, add_task: seen.append(recorded))
    cdr_poll.poll_new(lending_db, http)
    cdr_poll.poll_new(lending_db, http)
    assert [tuple(r) for r in _rows(lending_db)] == [("1", "outbound", None)]  # "ANSWER" is a status, not a result
    raw = "SELECT disposition_raw FROM lending.call_dispositions WHERE dialer_call_id = '1'"
    assert lending_db.execute(text(raw)).scalar() is None and all(r.unknown_code is None for r in seen)


def test_other_campaigns_and_inbound_calls_are_ignored(lending_db):
    http = FakeHttp(last=[_cdr(1, campaign={"id": 999}), _cdr(2, direction="in"), _cdr(3)])
    stats = cdr_poll.poll_new(lending_db, http)
    assert [r[0] for r in _rows(lending_db)] == ["3"] and stats.processed == 1


def test_one_bad_cdr_does_not_stop_the_batch(lending_db, monkeypatch):
    real = cdr_poll.process_event

    def flaky(db, ev):
        if ev.call_id == "2":
            raise RuntimeError("boom")
        return real(db, ev)

    monkeypatch.setattr(cdr_poll, "process_event", flaky)
    stats = cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(1), _cdr(2), _cdr(3)]))
    assert [r[0] for r in _rows(lending_db)] == ["1", "3"] and stats.failed == 1


def test_rescan_pages_through_the_day_and_picks_up_a_later_disposition(lending_db):
    day = "2026-09-29"
    cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(1)]))
    http = FakeHttp(days={day: [[_cdr(1, disposition="CALLBACK_REQUESTED")], [_cdr(2, disposition="No Answer", duration=0)]]})
    now = datetime(2026, 9, 29, 17, 0, tzinfo=timezone.utc)
    cdr_poll.rescan_today(lending_db, http, now=now)
    assert [tuple(r) for r in _rows(lending_db)] == [("1", "outbound", "CALLBACK_REQUESTED"), ("2", "outbound", "NO_ANSWER")]
    assert any("next_page=1" in p for p in http.calls)


def test_rescan_skips_unchanged_rows(lending_db, monkeypatch):
    cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(1)]))
    calls = []
    real = cdr_poll.process_event
    monkeypatch.setattr(cdr_poll, "process_event", lambda db, ev: (calls.append(ev.call_id), real(db, ev))[1])
    http = FakeHttp(days={"2026-09-29": [[_cdr(1)]]})
    stats = cdr_poll.rescan_today(lending_db, http, now=datetime(2026, 9, 29, 17, 0, tzinfo=timezone.utc))
    assert calls == [] and stats.skipped == 1


def test_calls_lost_to_the_watermark_are_recovered_by_the_rescan(lending_db):
    """/last advanced but the process died before saving: the day scan still has the call."""
    http = FakeHttp(days={"2026-09-29": [[_cdr(5)]]})
    cdr_poll.rescan_today(lending_db, http, now=datetime(2026, 9, 29, 17, 0, tzinfo=timezone.utc))
    assert [r[0] for r in _rows(lending_db)] == ["5"]


def test_follow_up_failure_does_not_stop_the_batch(lending_db, monkeypatch):
    def boom(recorded, add_task):
        if recorded.call_id == "1":
            raise RuntimeError("slack down")

    monkeypatch.setattr(cdr_poll, "follow_up", boom)
    cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(1), _cdr(2)]))
    assert [r[0] for r in _rows(lending_db)] == ["1", "2"]


def test_dnc_whose_opt_out_failed_is_retried_by_the_rescan(lending_db, monkeypatch):
    monkeypatch.setattr(call_pipeline, "on_attempt_recorded", MagicMock())
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", MagicMock(side_effect=RuntimeError("down")))
    dnc = _cdr(1, disposition="DNC_REQUEST")
    stats = cdr_poll.poll_new(lending_db, FakeHttp(last=[dnc]))
    stamp = "SELECT opt_out_propagated_at FROM lending.call_dispositions WHERE dialer_call_id = '1'"
    assert stats.failed == 1 and lending_db.execute(text(stamp)).scalar() is None
    assert [r[0] for r in _rows(lending_db)] == ["1"]

    fake = MagicMock(return_value=1)
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", fake)
    cdr_poll.rescan_today(lending_db, FakeHttp(days={"2026-09-29": [[dnc]]}),
                          now=datetime(2026, 9, 29, 17, 0, tzinfo=timezone.utc))
    assert fake.called and lending_db.execute(text(stamp)).scalar() is not None


def _stamp(db, cid="1"):
    return db.execute(text("SELECT opt_out_propagated_at FROM lending.call_dispositions WHERE dialer_call_id = :c"),
                      {"c": cid}).scalar()


def test_inbound_dnc_request_is_opted_out_without_counting_an_attempt(lending_db, monkeypatch):
    attempt, opt_out = MagicMock(), MagicMock(return_value=1)
    monkeypatch.setattr(call_pipeline, "on_attempt_recorded", attempt)
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", opt_out)
    cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(1, direction="in", disposition="DNC_REQUEST")]))
    assert opt_out.called and not attempt.called
    assert [tuple(r) for r in _rows(lending_db)] == [("1", "inbound", "DNC_REQUEST")]
    assert _stamp(lending_db) is not None


def test_inbound_non_dnc_is_still_ignored(lending_db):
    stats = cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(1, direction="in", disposition="CALLBACK_REQUESTED")]))
    assert _rows(lending_db) == [] and stats.processed == 0


def test_cdr_without_an_end_time_still_ends_the_call(lending_db):
    for cid, disp in ((1, ""), (2, "ANSWER")):
        cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(cid, callEndTime=None, disposition=disp)]))
    ended = lending_db.execute(text("SELECT count(*) FROM lending.call_dispositions WHERE call_ended_at IS NOT NULL")).scalar()
    assert ended == 2


def test_duration_filling_in_later_is_picked_up_by_the_rescan(lending_db, monkeypatch):
    cdr_poll.poll_new(lending_db, FakeHttp(last=[_cdr(1, duration=0)]))
    calls = []
    real = cdr_poll.process_event
    monkeypatch.setattr(cdr_poll, "process_event", lambda db, ev: (calls.append(ev.call_id), real(db, ev))[1])
    cdr_poll.rescan_today(lending_db, FakeHttp(days={"2026-09-29": [[_cdr(1, duration=75)]]}),
                          now=datetime(2026, 9, 29, 17, 0, tzinfo=timezone.utc))
    assert calls == ["1"]


def test_phoneless_dnc_is_not_reselected_by_the_rescan(lending_db, monkeypatch):
    monkeypatch.setattr(call_pipeline, "on_attempt_recorded", MagicMock())
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", MagicMock())
    dnc = _cdr(1, disposition="DNC_REQUEST", customerNumber="")
    cdr_poll.poll_new(lending_db, FakeHttp(last=[dnc]))
    stats = cdr_poll.rescan_today(lending_db, FakeHttp(days={"2026-09-29": [[dnc]]}),
                                  now=datetime(2026, 9, 29, 17, 0, tzinfo=timezone.utc))
    assert stats.skipped == 1 and stats.processed == 0


def test_rescan_days_controls_how_many_days_are_requested(lending_db):
    http = FakeHttp()
    cdr_poll.rescan_today(lending_db, http, days=3, now=datetime(2026, 9, 29, 17, 0, tzinfo=timezone.utc))
    assert [p.split("callDate=")[1][:10] for p in http.calls] == ["2026-09-29", "2026-09-28", "2026-09-27"]


def _failed_dnc(db, monkeypatch, *calls):
    monkeypatch.setattr(call_pipeline, "on_attempt_recorded", MagicMock())
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", MagicMock(side_effect=RuntimeError("down")))
    cdr_poll.poll_new(db, FakeHttp(last=[_cdr(c, disposition="DNC_REQUEST", **kw) for c, kw in calls]))


def test_retry_sweep_stamps_the_unpropagated_dnc(lending_db, monkeypatch):
    _failed_dnc(lending_db, monkeypatch, (1, {}))
    assert _stamp(lending_db) is None
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", MagicMock(return_value=1))
    assert call_pipeline.retry_unpropagated_dnc(lending_db) == 1
    assert _stamp(lending_db) is not None
    assert call_pipeline.retry_unpropagated_dnc(lending_db) == 0


def test_retry_sweep_skips_rows_without_a_phone(lending_db, monkeypatch):
    _failed_dnc(lending_db, monkeypatch, (1, {"customerNumber": ""}))
    opt_out = MagicMock(return_value=1)
    monkeypatch.setattr(call_pipeline, "propagate_opt_out", opt_out)
    assert call_pipeline.retry_unpropagated_dnc(lending_db) == 0 and not opt_out.called


def test_retry_sweep_isolates_a_failing_row(lending_db, monkeypatch):
    _failed_dnc(lending_db, monkeypatch, (1, {}), (2, {"customerNumber": "8135550143"}))

    def flaky(db, *, phone, source_ref, actor):
        if source_ref == "1":
            raise RuntimeError("down")
        return 1

    monkeypatch.setattr(call_pipeline, "propagate_opt_out", flaky)
    assert call_pipeline.retry_unpropagated_dnc(lending_db) == 1
    assert _stamp(lending_db, "1") is None and _stamp(lending_db, "2") is not None
