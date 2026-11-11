"""P1.1 job queue worker, end to end on in-memory SQLite: Drive file -> lending.agent_jobs row -> pending_actions draft
and card -> decision -> receipt in done/. Drive, GHL and Slack are fakes; the checks are injected."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from packages.agent_core.chatport import FakeChatPort
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.send_gate import SendGate
from packages.agent_core.store import AgentStore
from src.lending.agent_jobs.checks import Verdict
from src.lending.agent_jobs.drive import FakeJobFolders
from src.lending.agent_jobs.worker import JobQueueWorker
from src.lending.ghl_crm import GhlContact
from tests.agent_core.conftest import SQLITE_DDL

START = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
JOB_ID = "7f1c2e9a-3b4d-4c5e-8f60-718293a4b5c6"
CONTACT = GhlContact(contact_id="ghl-123", phone="+18135550142", email="mark@example.com",
                     dnd_channels=frozenset(), tags=frozenset())
AGENT_JOBS_SQLITE = """
    CREATE TABLE agent_jobs (
        job_id TEXT PRIMARY KEY, drive_file_id TEXT NOT NULL UNIQUE, file_name TEXT NOT NULL,
        created_by TEXT, job_type TEXT, template_version TEXT, ghl_contact_id TEXT, rail TEXT,
        job TEXT NOT NULL, state TEXT NOT NULL, held_reason TEXT, release_at TIMESTAMP, action_id INTEGER,
        result_status TEXT, result TEXT, result_written_at TIMESTAMP, received_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL)
"""


class Clock:
    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        return self.now


class Harness:
    def __init__(self) -> None:
        self.engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
        with self.engine.begin() as conn:
            for statement in (*SQLITE_DDL, AGENT_JOBS_SQLITE):
                conn.execute(text(statement))
        self.clock = Clock()
        self.queue = PendingActionQueue(AgentStore(self.engine, schema=None), clock=self.clock)
        self.chat = FakeChatPort(default_channel="C_APPROVALS")
        self.gate = SendGate(self.queue, self.chat, "C_APPROVALS")
        self.folders = FakeJobFolders()
        self.contacts: dict[str, Optional[GhlContact]] = {"ghl-123": CONTACT}
        self.verdicts: list[Verdict] = []
        self.graduated: set[str] = set()
        self.worker = JobQueueWorker(
            self.engine, self.folders, self.gate, self.queue, self.contacts.get, approval_channel="C_APPROVALS",
            check=self._check, is_graduated=lambda version: version in self.graduated, clock=self.clock,
            jobs_table="agent_jobs")

    def _check(self, job, contact, now) -> Verdict:
        if contact is None:
            return Verdict("block", status="error", reason="contact not found in NDL GHL")
        return self.verdicts.pop(0) if self.verdicts else Verdict("go", rail="ghl")

    def drop(self, job_id: str = JOB_ID, **overrides) -> str:
        job = {"job_id": job_id, "lane": "NDL", "created_by": "LendingFlow follow up",
               "created_at": (START - timedelta(hours=1)).isoformat(), "type": "send_sms",
               "ghl_contact_id": "ghl-123", "contact_label": "Mark T., Tampa", "template_version": "LF-D2-SMS-v1",
               "body": "Hi Mark, any questions?", "reason": "Day 2 touch", "requires_approval": True,
               "expires_at": (START + timedelta(days=1)).isoformat()}
        job.update(overrides)
        return self.folders.add(f"2026-11-12T10:00:00-05:00_lf_{job_id}.json", json.dumps(job).encode())

    def row(self, job_id: str = JOB_ID) -> dict:
        with self.engine.connect() as conn:
            return dict(conn.execute(text("SELECT * FROM agent_jobs WHERE job_id = :j"), {"j": job_id}).mappings().one())

    def action_ids(self) -> list[int]:
        with self.engine.connect() as conn:
            return [r[0] for r in conn.execute(text("SELECT action_id FROM pending_actions"))]

    def send(self, action_id: int, provider_ref: str = "ghl-msg-1") -> None:
        assert self.queue.approve(action_id, "U_JOSH")
        assert self.queue.claim_for_send(action_id)
        assert self.queue.mark_sent(action_id, provider_ref)


@pytest.fixture
def h() -> Harness:
    return Harness()


def test_job_file_becomes_card_and_moves_to_processed(h: Harness) -> None:
    file_id = h.drop()
    stats = h.worker.run_once()
    assert (stats.received, stats.carded) == (1, 1)
    assert h.folders.processed == [file_id] and h.folders.inbox == {}
    row = h.row()
    assert (row["state"], row["rail"], row["job_type"]) == ("carded", "ghl", "send_sms")
    action = h.queue.get(row["action_id"])
    assert (action.channel, action.tool_name, action.requested_by) == ("ghl_sms", "agent_job:send_sms", "agent_job")
    assert action.expires_at == START + timedelta(days=1)
    assert action.payload["job_id"] == JOB_ID
    assert len(h.chat.posts) == 1 and h.chat.posts[0]["channel"] == "C_APPROVALS"


def test_duplicate_job_id_creates_one_draft(h: Harness) -> None:
    h.drop()
    h.drop()
    h.worker.run_once()
    assert len(h.action_ids()) == 1
    assert len(h.chat.posts) == 1
    assert len(h.folders.processed) == 2


def test_rerun_after_crash_does_not_duplicate(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    with h.engine.begin() as conn:  # as if the process died after the card posted, before the row was updated
        conn.execute(text("UPDATE agent_jobs SET state = 'held', action_id = NULL, release_at = :now"),
                     {"now": START})
    h.worker.run_once()
    assert len(h.action_ids()) == 1 and len(h.chat.posts) == 1
    assert h.row()["state"] == "carded"


def test_approved_and_sent_writes_sent_receipt(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    h.send(h.row()["action_id"])
    h.worker.run_once()
    receipt = h.folders.results[JOB_ID]
    assert receipt["status"] == "sent"
    assert receipt["action_id"] == h.row()["action_id"]
    assert receipt["provider_message_id"] == "ghl-msg-1"
    assert receipt["rail"] == "ghl" and receipt["template_version"] == "LF-D2-SMS-v1"
    assert receipt["edited_body"] is None
    assert datetime.fromisoformat(receipt["timestamp"]).utcoffset() == timedelta(hours=-5)
    assert h.row()["state"] == "done" and h.row()["result_written_at"] is not None


def test_revised_then_sent_is_edited_sent_with_body(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    action_id = h.row()["action_id"]
    assert h.queue.request_revision(action_id, "U_JOSH")
    assert h.queue.apply_revision(action_id, payload={**h.queue.get(action_id).payload, "body": "Hi Mark, call me?"},
                                  summary="s", note="shorter", ttl=timedelta(hours=5))
    h.send(action_id)
    h.worker.run_once()
    receipt = h.folders.results[JOB_ID]
    assert (receipt["status"], receipt["edited_body"]) == ("edited_sent", "Hi Mark, call me?")


def test_rejected_receipt(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    assert h.queue.reject(h.row()["action_id"], "U_JOSH")
    h.worker.run_once()
    assert h.folders.results[JOB_ID]["status"] == "rejected"


def test_expired_receipt(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    h.clock.now = START + timedelta(days=2)
    h.queue.expire_stale()
    h.worker.run_once()
    assert h.folders.results[JOB_ID]["status"] == "expired"


def test_send_time_block_maps_to_blocked_consent(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    action_id = h.row()["action_id"]
    assert h.queue.approve(action_id, "U_JOSH") and h.queue.claim_for_send(action_id)
    assert h.queue.mark_blocked(action_id, "recipient is suppressed or marked do-not-contact")
    h.worker.run_once()
    receipt = h.folders.results[JOB_ID]
    assert receipt["status"] == "blocked_consent"
    assert "suppressed" in receipt["reason"]


def test_failed_send_is_error(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    action_id = h.row()["action_id"]
    assert h.queue.approve(action_id, "U_JOSH") and h.queue.claim_for_send(action_id)
    assert h.queue.mark_failed(action_id, "GhlCrmError: GHL stage move failed: HTTP 400")
    h.worker.run_once()
    assert h.folders.results[JOB_ID]["status"] == "error"


def test_intake_block_writes_receipt_without_draft(h: Harness) -> None:
    h.verdicts.append(Verdict("block", status="blocked_lane", reason="the job mentions a Backflip name"))
    h.drop()
    h.worker.run_once()
    assert h.action_ids() == []
    receipt = h.folders.results[JOB_ID]
    assert (receipt["status"], receipt["action_id"]) == ("blocked_lane", None)


def test_unknown_contact_is_error_receipt(h: Harness) -> None:
    h.drop(ghl_contact_id="ghl-missing")
    h.worker.run_once()
    assert h.folders.results[JOB_ID]["status"] == "error"


def test_held_job_released_when_window_opens(h: Harness) -> None:
    h.verdicts.append(Verdict("hold", release_at=START + timedelta(minutes=15), held_reason="hours", rail="ghl"))
    h.drop()
    h.worker.run_once()
    assert h.row()["state"] == "held" and h.chat.posts == []
    h.clock.now = START + timedelta(minutes=16)
    h.worker.run_once()
    assert h.row()["state"] == "carded" and len(h.chat.posts) == 1


def test_held_job_expiring_is_blocked_hours(h: Harness) -> None:
    h.verdicts.append(Verdict("hold", release_at=START + timedelta(minutes=15), held_reason="hours", rail="ghl"))
    h.verdicts.append(Verdict("block", status="expired", reason="the job expired before Cora could act on it"))
    h.drop()
    h.worker.run_once()
    h.clock.now = START + timedelta(minutes=16)
    h.worker.run_once()
    assert h.folders.results[JOB_ID]["status"] == "blocked_hours"


def test_receipt_write_failure_retries_next_sweep(h: Harness) -> None:
    h.verdicts.append(Verdict("block", status="blocked_consent", reason="no text consent on record"))
    h.drop()
    h.folders.fail_writes = True
    stats = h.worker.run_once()
    assert JOB_ID not in h.folders.results and h.row()["result_written_at"] is None
    assert stats.errors >= 1
    h.folders.fail_writes = False
    h.worker.run_once()
    assert h.folders.results[JOB_ID]["status"] == "blocked_consent"
    assert h.row()["result_written_at"] is not None


def test_bad_file_without_job_id_is_moved_and_logged(h: Harness, caplog) -> None:
    file_id = h.folders.add("notes.json", b"{not json")
    h.worker.run_once()
    assert h.folders.processed == [file_id]
    assert h.folders.results == {}
    assert any("could not be read as a job" in r.getMessage() for r in caplog.records)


def test_bad_file_with_job_id_gets_error_receipt(h: Harness) -> None:
    h.drop(type="send_fax")
    h.worker.run_once()
    receipt = h.folders.results[JOB_ID]
    assert receipt["status"] == "error" and "type" in receipt["reason"]


def test_requires_approval_false_still_cards_when_not_graduated(h: Harness) -> None:
    h.drop(requires_approval=False)
    h.worker.run_once()
    assert h.queue.get(h.row()["action_id"]).status is ActionStatus.PENDING


def test_graduated_job_is_auto_approved(h: Harness) -> None:
    h.graduated.add("LF-D2-SMS-v1")
    h.drop(requires_approval=False)
    h.worker.run_once()
    action = h.queue.get(h.row()["action_id"])
    assert (action.status, action.decided_by) == (ActionStatus.APPROVED, "auto:graduated")


def test_receipt_has_no_full_phone_or_email(h: Harness) -> None:
    h.drop()
    h.worker.run_once()
    h.send(h.row()["action_id"])
    h.worker.run_once()
    dumped = json.dumps(h.folders.results[JOB_ID])
    assert "+18135550142" not in dumped and "mark@example.com" not in dumped


def test_cold_email_receipt_has_rail_instantly(h: Harness) -> None:
    h.contacts["ghl-123"] = GhlContact("ghl-123", "+18135550142", "mark@example.com", frozenset({"Email"}),
                                       frozenset({"ndl-cold"}))
    h.verdicts.append(Verdict("go", rail="instantly"))
    h.drop(type="send_email", subject="Your refinance")
    h.worker.run_once()
    action = h.queue.get(h.row()["action_id"])
    assert action.channel == "instantly_email"
    h.send(action.action_id, provider_ref="lead-9")
    h.worker.run_once()
    receipt = h.folders.results[JOB_ID]
    assert (receipt["rail"], receipt["provider_message_id"]) == ("instantly", "lead-9")


def test_one_bad_job_does_not_stop_the_others(h: Harness) -> None:
    other = "0d7a1c3e-1111-4c5e-8f60-718293a4b5c6"
    h.drop(job_id=other, ghl_contact_id="ghl-boom")
    h.drop()

    def flaky(contact_id):
        if contact_id == "ghl-boom":
            raise RuntimeError("GHL down")
        return CONTACT

    h.worker._read_contact = flaky
    stats = h.worker.run_once()
    assert h.row()["state"] == "carded"
    assert h.row(other)["state"] == "held" and stats.errors >= 1


class _ConnEngine:
    """The worker and the queue only call ``engine.begin()`` and read ``engine.dialect``: run both on one
    connection, each transaction a savepoint inside the test's rolled-back transaction."""

    def __init__(self, conn) -> None:
        self._conn, self.dialect = conn, conn.dialect

    def begin(self):
        from contextlib import contextmanager

        @contextmanager
        def savepoint():
            with self._conn.begin_nested():
                yield self._conn

        return savepoint()


def test_worker_sql_runs_on_postgres(lending_db) -> None:
    from packages.agent_core.schema import apply_to

    conn = lending_db.connection()
    apply_to(conn, "lending")
    engine = _ConnEngine(conn)
    clock = Clock()
    queue = PendingActionQueue(AgentStore(engine, "lending"), clock=clock)
    folders = FakeJobFolders()
    worker = JobQueueWorker(engine, folders, SendGate(queue, FakeChatPort(), "C_APPROVALS"), queue,
                            {"ghl-123": CONTACT}.get, approval_channel="C_APPROVALS",
                            check=lambda job, contact, now: Verdict("go", rail="ghl"), clock=clock)
    job = {"job_id": JOB_ID, "lane": "NDL", "created_by": "LF", "created_at": START.isoformat(), "type": "send_sms",
           "ghl_contact_id": "ghl-123", "template_version": "v1", "body": "Hi", "requires_approval": True,
           "expires_at": (START + timedelta(days=1)).isoformat()}
    folders.add(f"x_lf_{JOB_ID}.json", json.dumps(job).encode())
    assert worker.run_once().carded == 1
    action_id = conn.execute(text("SELECT action_id FROM lending.agent_jobs WHERE job_id = :j"), {"j": JOB_ID}).scalar()
    assert queue.reject(action_id, "U_JOSH")
    worker.run_once()
    assert folders.results[JOB_ID]["status"] == "rejected"
    stored = conn.execute(text("SELECT state, result->>'status' FROM lending.agent_jobs WHERE job_id = :j"),
                          {"j": JOB_ID}).one()
    assert tuple(stored) == ("done", "rejected")
