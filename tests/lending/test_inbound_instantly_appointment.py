"""R1 0.5 T6: /webhooks/lending/instantly and /webhooks/lending/ghl-appointment on the inbound journal.

No database: the journal store is the in-memory fake from test_inbound_journal, the handlers' own stores are
monkeypatched, and the DLQ save is captured instead of written. With LENDING_INBOUND_JOURNAL_ENABLED on, a
repeat delivery has one effect and a handler failure answers 200 and is saved for the 5-minute replay.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.lending import booking_webhook, instantly_replies, instantly_webhook, webhook_failures
from src.lending.inbound import journal
from tests.lending.test_inbound_journal import MemoryStore, MemoryTx

SECRET = "s3cret"
LEAD = "mark.lee@example.com"


class FakeDb:
    """Enough of a Session for the handlers: savepoints that roll back on error, nothing else."""

    def __init__(self):
        self.savepoints = 0

    @contextmanager
    def begin_nested(self):
        self.savepoints += 1
        yield

    def execute(self, statement, params=None):  # the known-appointment lookup: none known
        return SimpleNamespace(mappings=lambda: SimpleNamespace(first=lambda: None))


class Store(MemoryStore):
    @contextmanager
    def transaction(self):
        tx = MemoryTx(self)
        tx.db = FakeDb()
        yield tx
        for event_id in tx.processed:
            self.rows[event_id].status = "processed"
            self.rows[event_id].attempts += 1


@pytest.fixture
def store(monkeypatch):
    from config.settings import get_settings

    store = Store()
    monkeypatch.setattr(journal, "SqlJournalStore", lambda: store)
    monkeypatch.setattr(get_settings(), "lending_inbound_journal_enabled", True, raising=False)
    return store


@pytest.fixture
def dlq(monkeypatch):
    """What the journal hands to webhook_failures when a handler fails: (source, payload, error class)."""
    saved: list[tuple[str, dict, str]] = []

    def insert(db, source, payload, exc):
        saved.append((source, payload, exc))
        return len(saved)

    def report(source, payload, exc, *, save=None):
        if save is not None:
            return save()
        saved.append((source, payload, type(exc).__name__))
        return len(saved)

    monkeypatch.setattr(webhook_failures, "insert_failure", insert)
    monkeypatch.setattr(webhook_failures, "report_webhook_failure", report)
    return saved


# ── Instantly ────────────────────────────────────────────────────────────────

def stop_body(**extra):
    body = {"event_type": "reply_received", "lead_email": LEAD, "email_id": "em-1", "reply_text": "STOP"}
    body.update(extra)
    return body


@pytest.fixture
def instantly(monkeypatch, store, dlq):
    from config.settings import get_settings
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_instantly_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(get_settings(), "lending_instantly_api_key", None, raising=False)
    app = FastAPI()
    app.include_router(instantly_webhook.router)
    app.dependency_overrides[get_db] = lambda: None  # flag on: the journal brings its own session
    client = TestClient(app)
    client.recorded, client.suppressed, client.notices, client.inbox = [], [], [], []

    def record(db, event):
        if event.dedup_key in {e.dedup_key for e in client.recorded}:
            return instantly_replies.RecordResult("duplicate")
        client.recorded.append(event)
        return instantly_replies.RecordResult("recorded", len(client.recorded), True)

    monkeypatch.setattr(instantly_webhook, "record_event", record)
    monkeypatch.setattr(instantly_webhook, "suppress_reply", lambda db, reply_id: client.suppressed.append(reply_id) or 31)
    monkeypatch.setattr(instantly_webhook, "post_ops_notice", lambda text: client.notices.append(text) or True)
    monkeypatch.setattr(instantly_webhook, "queue_suppression_for_cora",
                        lambda db, reply_id, email, event_id, posted: client.inbox.append(reply_id))
    return client


def post_instantly(client, body):
    return client.post("/webhooks/lending/instantly", headers={"X-Instantly-Webhook-Secret": SECRET}, json=body)


def test_an_instantly_stop_delivered_three_times_is_suppressed_once(instantly, store):
    answers = [post_instantly(instantly, stop_body()) for _ in range(3)]
    assert [a.status_code for a in answers] == [200, 200, 200]
    assert [a.json()["status"] for a in answers] == ["suppressed", "duplicate", "duplicate"]
    assert len(instantly.recorded) == 1 and instantly.suppressed == [1]
    assert instantly.notices == [] and instantly.inbox == [1]
    assert [r.source for r in store.rows.values()] == ["instantly_event"]


def test_an_instantly_event_that_cannot_be_stored_is_answered_200_and_replayed(instantly, store, dlq, monkeypatch):
    working_record = instantly_webhook.record_event

    def broken(db, event):
        raise RuntimeError(f"db down near {event.lead_email}")

    monkeypatch.setattr(instantly_webhook, "record_event", broken)
    answer = post_instantly(instantly, stop_body())
    assert answer.status_code == 200 and answer.json() == {"status": "failed"} and LEAD not in answer.text
    assert [(source, error) for source, _, error in dlq] == [("instantly_event", "RuntimeError")]
    assert instantly.suppressed == []

    monkeypatch.setattr(instantly_webhook, "record_event", working_record)
    replay = webhook_failures.ENDPOINT_HANDLERS["instantly_event"]
    assert replay(FakeDb(), dlq[0][1]) == {"status": "suppressed"}
    assert len(instantly.recorded) == 1 and instantly.suppressed == [1] and instantly.notices == []


def test_an_instantly_opt_out_that_fails_is_left_for_coras_sweep_without_failing_the_event(instantly, store, dlq,
                                                                                            monkeypatch):
    def broken(db, reply_id):
        raise RuntimeError("db down")

    monkeypatch.setattr(instantly_webhook, "suppress_reply", broken)
    assert post_instantly(instantly, stop_body()).json() == {"status": "suppressed"}
    assert len(instantly.recorded) == 1 and instantly.notices == [] and dlq == []
    assert [r.status for r in store.rows.values()] == ["processed"]


def test_an_instantly_raw_save_failure_asks_instantly_to_resend(instantly, store):
    store.insert_error = RuntimeError(f"insert failed for {LEAD}")
    answer = post_instantly(instantly, stop_body())
    assert answer.status_code == 503 and LEAD not in answer.text
    assert instantly.recorded == []


def test_instantly_events_the_route_ignores_are_not_journaled(instantly, store):
    assert post_instantly(instantly, {"event_type": "email_opened", "lead_email": LEAD}).json() == {"status": "ignored"}
    assert store.rows == {}


def test_the_instantly_journal_source_is_loaded_by_the_retry_cron():
    assert "src.lending.instantly_webhook" in journal.HANDLER_MODULES


# ── GHL appointment ──────────────────────────────────────────────────────────

GHL_SECRET = "ghl-s3cret"
START = "2026-12-01T15:00:00Z"


@pytest.fixture
def appointments(monkeypatch, store, dlq):
    from config.settings import get_settings
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(GHL_SECRET), raising=False)
    app = FastAPI()
    app.include_router(booking_webhook.router)
    app.dependency_overrides[get_db] = lambda: None  # flag on: the journal brings its own session
    client = TestClient(app)
    client.cancels, client.schedules, client.out_of_dialer = [], [], []

    def cancel(db, appointment_id, reason, *, spare_replanned_seconds=0):
        client.cancels.append((appointment_id, reason))
        return 4

    def schedule(db, payload):
        if client.schedules and all(client.schedules[-1][key] == payload[key]
                                    for key in ("booking_ref", "slot_start_utc", "phone", "email")):
            return SimpleNamespace(skip_reason=None, inserted=0)
        client.schedules.append(payload)
        return SimpleNamespace(skip_reason=None, inserted=3)

    monkeypatch.setattr(booking_webhook, "cancel_by_provider_event", cancel)
    monkeypatch.setattr(booking_webhook, "handle_booking_confirmed", schedule)
    monkeypatch.setattr(booking_webhook, "handle_handled_in_ghl",
                        lambda phone, reason: client.out_of_dialer.append(phone))
    return client


def post_appointment(client, body):
    return client.post("/webhooks/lending/ghl-appointment", headers={"X-Webhook-Secret": GHL_SECRET}, json=body)


def test_a_cancelled_appointment_delivered_twice_cancels_its_messages_once(appointments, store):
    body = {"appointmentId": "appt-1", "appointmentStatus": "cancelled"}
    answers = [post_appointment(appointments, body) for _ in range(2)]
    assert [a.json() for a in answers] == [{"status": "cancelled", "cancelled": 4}, {"status": "duplicate"}]
    assert appointments.cancels == [("appt-1", "booking_cancelled")]
    assert [r.source for r in store.rows.values()] == ["ghl_appointment"]


def test_a_cancel_that_fails_is_answered_200_saved_and_replayed(appointments, dlq, monkeypatch):
    working_cancel = booking_webhook.cancel_by_provider_event

    def broken(db, appointment_id, reason, *, spare_replanned_seconds=0):
        raise RuntimeError("db down")

    monkeypatch.setattr(booking_webhook, "cancel_by_provider_event", broken)
    answer = post_appointment(appointments, {"appointmentId": "appt-1", "appointmentStatus": "cancelled"})
    assert answer.status_code == 200 and answer.json() == {"status": "failed"}
    assert [(source, error) for source, _, error in dlq] == [("ghl_appointment", "RuntimeError")]

    monkeypatch.setattr(booking_webhook, "cancel_by_provider_event", working_cancel)
    replay = webhook_failures.ENDPOINT_HANDLERS["ghl_appointment"]
    assert replay(FakeDb(), dlq[0][1]) == {"status": "cancelled", "cancelled": 4}
    assert appointments.cancels == [("appt-1", "booking_cancelled")]


def test_a_booking_whose_scheduling_fails_is_saved_and_still_leaves_the_dialer(appointments, dlq, monkeypatch):
    def broken(db, payload):
        raise RuntimeError("db down")

    monkeypatch.setattr(booking_webhook, "handle_booking_confirmed", broken)
    body = {"appointmentId": "appt-2", "appointmentStatus": "confirmed", "startTime": START, "phone": "+18135550111"}
    answer = post_appointment(appointments, body)
    assert answer.status_code == 200 and answer.json() == {"status": "failed"} and "8135550111" not in answer.text
    assert [source for source, _, _ in dlq] == ["ghl_appointment"]
    assert appointments.out_of_dialer == ["+18135550111"]


def test_a_reschedule_to_a_new_time_is_a_new_event_and_a_repeat_is_not(appointments):
    first = {"appointmentId": "appt-3", "appointmentStatus": "rescheduled", "startTime": START}
    moved = {**first, "startTime": "2026-12-02T15:00:00Z"}
    answers = [post_appointment(appointments, b).json()["status"] for b in (first, first, moved, first)]
    assert answers == ["scheduled", "duplicate", "scheduled", "scheduled"]
    assert [p["slot_start_utc"].day for p in appointments.schedules] == [1, 2, 1]


def test_an_appointment_raw_save_failure_asks_ghl_to_resend(appointments, store):
    store.insert_error = RuntimeError("insert failed")
    answer = post_appointment(appointments, {"appointmentId": "appt-1", "appointmentStatus": "cancelled"})
    assert answer.status_code == 503 and appointments.cancels == []


def test_an_appointment_without_an_id_is_not_journaled(appointments, store):
    assert post_appointment(appointments, {"type": "ping"}).json() == {"status": "noop", "reason": "no_appointment_id"}
    assert store.rows == {}


def test_the_appointment_journal_source_is_loaded_by_the_retry_cron():
    assert "src.lending.booking_webhook" in journal.HANDLER_MODULES


def test_with_the_journal_off_a_cancel_runs_on_the_request_session_as_before(monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(GHL_SECRET), raising=False)
    monkeypatch.setattr(get_settings(), "lending_inbound_journal_enabled", False, raising=False)
    monkeypatch.setattr(journal, "SqlJournalStore", lambda: pytest.fail("the journal must not be used"))
    session = SimpleNamespace(commits=0)
    session.commit = lambda: setattr(session, "commits", session.commits + 1)
    monkeypatch.setattr(booking_webhook, "cancel_by_provider_event",
                        lambda db, appointment_id, reason, *, spare_replanned_seconds=0: 2)
    app = FastAPI()
    app.include_router(booking_webhook.router)
    app.dependency_overrides[get_db] = lambda: session
    answer = post_appointment(TestClient(app), {"appointmentId": "appt-1", "appointmentStatus": "cancelled"})
    assert answer.json() == {"status": "cancelled", "cancelled": 2} and session.commits == 1


def test_an_appointment_delivery_with_missing_contact_details_can_be_repaired(appointments):
    body = {"appointmentId": "appt-4", "appointmentStatus": "confirmed", "startTime": START}
    assert post_appointment(appointments, body).json()["status"] == "scheduled"
    assert post_appointment(appointments, {**body, "email": "borrower@example.com"}).json()["status"] == "scheduled"
    assert appointments.schedules[-1]["email"] == "borrower@example.com"
