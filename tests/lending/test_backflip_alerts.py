"""Backflip block alerts: each draft or agent job the Backflip check refused is raised once in Cora's inbox."""
from __future__ import annotations

import json
from types import SimpleNamespace

from sqlalchemy import text

from src.lending import backflip_alerts
from src.lending.backflip_alerts import EVENT_TYPE, raise_backflip_blocks
from src.lending.cora_agent import CoraServices, run_sweep


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeDb:
    def __init__(self, rows):
        self.rows, self.params = rows, None

    def execute(self, _statement, params):
        self.params = params
        return _Rows(self.rows)


_DRAFT = {"kind": "draft", "ref": "41", "source": "send_email", "channel": "ghl_email", "contact_ref": "lead:7",
          "blocked_at": None}
_JOB = {"kind": "job", "ref": "job-9", "source": "maya", "channel": "send_email", "contact_ref": None,
        "blocked_at": None}


def test_each_new_block_is_queued_once_with_references_only(monkeypatch):
    queued = []
    monkeypatch.setattr(backflip_alerts, "add_to_inbox", lambda db, **event: queued.append(event) or len(queued))
    db = _FakeDb([_DRAFT, _JOB])

    assert raise_backflip_blocks(db) == 2
    assert db.params == {"lane_prefix": "BACKFLIP_LANE%", "days": 7, "event_type": EVENT_TYPE}
    draft, job = queued
    assert (draft["event_type"], draft["dedup_key"]) == (EVENT_TYPE, "draft:41")
    assert draft["payload"] == {"action_id": 41, "tool": "send_email", "channel": "ghl_email",
                                "contact_ref": "lead:7"}
    assert "draft #41" in draft["summary"] and "Nothing was sent" in draft["summary"]
    assert job["dedup_key"] == "job:job-9"
    assert job["payload"] == {"job_id": "job-9", "created_by": "maya", "job_type": "send_email"}
    assert "agent job job-9 from maya" in job["summary"]


def test_a_block_already_in_the_inbox_is_not_counted(monkeypatch):
    monkeypatch.setattr(backflip_alerts, "add_to_inbox", lambda db, **event: None)
    assert raise_backflip_blocks(_FakeDb([_DRAFT])) == 0


def _services(calls, *, alerts_fail=False):
    def alerts():
        calls.append("alerts")
        if alerts_fail:
            raise RuntimeError("database down")
        return 1

    def digest():
        calls.append("digest")
        return 0

    return CoraServices(
        session=SimpleNamespace(expire_stale_actions=lambda: 0),
        relay=SimpleNamespace(run=lambda: SimpleNamespace(sent=0, failed=0, blocked=0)),
        gate=SimpleNamespace(retry_missing_cards=lambda: 0),
        inbox_digest=digest, backflip_alerts=alerts)


def test_the_sweep_raises_blocks_before_posting_the_digest():
    calls = []
    run_sweep(_services(calls))
    assert calls == ["alerts", "digest"]


def test_a_failed_alert_step_never_stops_the_digest():
    calls = []
    run_sweep(_services(calls, alerts_fail=True))
    assert calls == ["alerts", "digest"]


def _draft(db, status, error, *, days_ago=0):
    return db.execute(text("""
        INSERT INTO lending.pending_actions (tool_name, channel, payload, status, error, updated_at)
        VALUES ('send_email', 'ghl_email', '{}'::jsonb, :status, :error, now() - make_interval(days => :days))
        RETURNING action_id
    """), {"status": status, "error": error, "days": days_ago}).scalar_one()


def _job(db, job_id, reason, *, action_id=None):
    db.execute(text("""
        INSERT INTO lending.agent_jobs (job_id, drive_file_id, file_name, created_by, job_type, state, action_id,
                                        result_status, result)
        VALUES (:job_id, :job_id, 'job.json', 'maya', 'send_email', 'done', :action_id, 'blocked_lane',
                CAST(:result AS jsonb))
    """), {"job_id": job_id, "action_id": action_id, "result": json.dumps({"reason": reason})})


def test_blocks_are_found_and_raised_once_on_postgres(lending_db):
    from migrations.apply_lending_agent_jobs import apply_to as apply_jobs
    from migrations.apply_lending_cora_agent_core import apply_to as apply_core
    from migrations.apply_lending_cora_inbox import apply_to as apply_inbox

    conn = lending_db.connection()
    apply_core(conn)
    apply_inbox(conn)
    apply_jobs(conn)
    lane = _draft(lending_db, "blocked", "BACKFLIP_LANE: backflip_name")
    consent = _draft(lending_db, "blocked", "the contact opted out")  # not a Backflip block
    old = _draft(lending_db, "blocked", "BACKFLIP_LANE: backflip_domain", days_ago=8)  # older than the lookback
    _job(lending_db, "BF-TEST-JOB-1", "the job mentions a Backflip name, number or domain")
    _job(lending_db, "BF-TEST-JOB-2", "the job mentions a Backflip name, number or domain", action_id=lane)
    _job(lending_db, "BF-TEST-JOB-3", "the job is not in the NDL lane")

    raise_backflip_blocks(lending_db)
    keys = set(lending_db.execute(text("SELECT dedup_key FROM lending.cora_inbox WHERE event_type = :t"),
                                  {"t": EVENT_TYPE}).scalars())
    assert f"draft:{lane}" in keys and "job:BF-TEST-JOB-1" in keys
    assert not {f"draft:{consent}", f"draft:{old}", "job:BF-TEST-JOB-2", "job:BF-TEST-JOB-3"} & keys
    assert raise_backflip_blocks(lending_db) == 0


def test_a_job_blocked_by_the_send_gate_is_raised_once_as_its_draft_not_twice(lending_db):
    """What production writes: the gate stored the draft as blocked, and the worker's receipt has no action id."""
    from migrations.apply_lending_agent_jobs import apply_to as apply_jobs
    from migrations.apply_lending_cora_agent_core import apply_to as apply_core
    from migrations.apply_lending_cora_inbox import apply_to as apply_inbox
    from src.lending.agent_jobs.worker import JobQueueWorker  # noqa: F401  (the reason text below is the worker's)

    conn = lending_db.connection()
    apply_core(conn)
    apply_inbox(conn)
    apply_jobs(conn)
    lending_db.execute(text("DELETE FROM lending.cora_inbox WHERE event_type = :t"), {"t": EVENT_TYPE})
    draft = _draft(lending_db, "blocked", "BACKFLIP_LANE: blocked_name")
    _job(lending_db, "BF-GATE-JOB-1", "the send gate blocked the draft (lane deny list)")  # the worker's real reason
    raise_backflip_blocks(lending_db)
    keys = set(lending_db.execute(text("SELECT dedup_key FROM lending.cora_inbox WHERE event_type = :t"),
                                  {"t": EVENT_TYPE}).scalars())
    assert f"draft:{draft}" in keys and "job:BF-GATE-JOB-1" not in keys
