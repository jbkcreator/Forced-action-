"""R1 1.5: a borrower who called NDL and nobody answered becomes an inbox item (GHL webhook, BatchDialer CDR)."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.api import lending_ghl_router
from src.lending import call_pipeline, cdr_poll
from src.lending import ghl_missed_call as mc

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
NOW = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
INBOUND = {"phone": "+18135550142", "contact_id": "c-1", "call_status": "no-answer", "direction": "incoming",
           "called_at": "2026-11-27T14:59:20.000Z", "ended_at": "2026-11-27T14:59:50.000Z"}


@pytest.mark.parametrize("status", ["no-answer", "voicemail", "busy", "No Answer"])
def test_an_inbound_unanswered_call_becomes_a_missed_call_item(status):
    item = mc.parse_inbound_missed_call({**INBOUND, "call_status": status}, NOW)
    assert (item.source, item.channel) == ("ghl", "missed_call")
    assert item.phone == "+18135550142" and item.ghl_contact_id == "c-1" and item.body is None


@pytest.mark.parametrize("direction", ["in", "inbound", "Incoming"])
def test_every_inbound_spelling_counts(direction):
    assert mc.parse_inbound_missed_call({**INBOUND, "direction": direction}, NOW) is not None


@pytest.mark.parametrize("extra", [{"call_status": "completed"}, {"call_status": ""}, {"direction": "outbound"},
                                   {"direction": ""}])
def test_answered_outbound_or_directionless_calls_are_not_inbound_items(extra):
    assert mc.parse_inbound_missed_call({**INBOUND, **extra}, NOW) is None


def test_an_outbound_missed_call_is_still_the_text_back_parsers():
    outbound = {**INBOUND, "direction": "outbound"}
    assert mc.parse_missed_call(outbound, NOW) is not None and mc.parse_inbound_missed_call(outbound, NOW) is None


def test_the_same_inbound_call_posted_twice_is_one_key_and_a_call_id_wins():
    first, second = (mc.parse_inbound_missed_call(INBOUND, NOW) for _ in range(2))
    assert first.dedup_key == second.dedup_key and first.dedup_key.startswith("ghl_call:")
    assert mc.parse_inbound_missed_call({**INBOUND, "call_id": "msg-7"}, NOW).dedup_key == "ghl_call:ghl:msg-7"


def test_an_inbound_call_needs_a_phone():
    with pytest.raises(mc.InvalidMissedCall):
        mc.parse_inbound_missed_call({**INBOUND, "phone": ""}, NOW)


# ── the route ────────────────────────────────────────────────────────────────

class Db:
    def __init__(self):
        self.commits, self.rollbacks = 0, 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def client(monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    monkeypatch.setattr(lending_ghl_router, "get_settings",
                        lambda: SimpleNamespace(lending_ghl_webhook_secret=SecretStr(SECRET),
                                                lending_unified_inbox_enabled=True))
    db = Db()
    app = FastAPI()
    app.include_router(lending_ghl_router.router)
    app.dependency_overrides[get_db] = lambda: db
    client = TestClient(app)
    client.db = db
    return client


def post(client, body):
    return client.post("/webhooks/lending/ghl-missed-call", headers=HEADERS, json=body)


def test_an_inbound_call_is_recorded_and_never_reaches_text_back(client, monkeypatch):
    saved = []
    monkeypatch.setattr(lending_ghl_router, "record_inbound", lambda db, item: saved.append(item) or 11)
    monkeypatch.setattr(lending_ghl_router, "record_missed_call", lambda *a: pytest.fail("inbound is not text-back"))
    monkeypatch.setattr(lending_ghl_router, "run_text_back_now", lambda: pytest.fail("no text-back"))
    response = post(client, INBOUND)
    assert response.status_code == 200 and response.json() == {"queued": False, "status": "inbox_recorded"}
    assert saved[0].channel == "missed_call" and client.db.commits == 1


def test_a_repeated_inbound_call_answers_duplicate(client, monkeypatch):
    monkeypatch.setattr(lending_ghl_router, "record_inbound", lambda db, item: None)
    assert post(client, INBOUND).json()["status"] == "duplicate"


def test_an_inbound_call_while_the_inbox_is_off_says_disabled(client, monkeypatch):
    monkeypatch.setattr(lending_ghl_router, "get_settings",
                        lambda: SimpleNamespace(lending_ghl_webhook_secret=SecretStr(SECRET),
                                                lending_unified_inbox_enabled=False))
    monkeypatch.setattr(lending_ghl_router, "record_inbound", lambda *a: pytest.fail("disabled"))
    assert post(client, INBOUND).json() == {"queued": False, "status": "disabled"}


def test_an_inbound_call_that_cannot_be_saved_answers_500_so_ghl_retries(client, monkeypatch):
    def boom(db, item):
        raise RuntimeError("db down")

    monkeypatch.setattr(lending_ghl_router, "record_inbound", boom)
    response = post(client, INBOUND)
    assert response.status_code == 500 and "db down" not in response.text and client.db.rollbacks == 1


def test_an_outbound_missed_call_still_goes_to_text_back(client, monkeypatch):
    monkeypatch.setattr(lending_ghl_router, "record_inbound", lambda *a: pytest.fail("outbound"))
    monkeypatch.setattr(lending_ghl_router, "record_missed_call", lambda db, call: "pending")
    ran = []
    monkeypatch.setattr(lending_ghl_router, "run_text_back_now", lambda: ran.append(1))
    response = post(client, {**INBOUND, "direction": "outbound"})
    assert response.json() == {"queued": True, "status": "pending"} and ran == [1]


# ── BatchDialer CDR ──────────────────────────────────────────────────────────

def cdr(cid=1, **over):
    base = {"id": cid, "direction": "in", "callStartTime": "2026-11-27T15:55:00Z", "callEndTime": "2026-11-27T15:55:20Z",
            "did": "8135550100", "customerNumber": "8135550142", "disposition": "NO ANSWER", "duration": 0,
            "agent": {"id": 7, "firstname": "Sam", "lastname": "C"}, "contact": {"id": 9},
            "campaign": {"id": 55, "name": "Inbound"}}
    base.update(over)
    return base


@pytest.fixture
def poll(monkeypatch):
    monkeypatch.setattr(call_pipeline, "get_settings", lambda: SimpleNamespace(lending_dialer_campaign_ids="55"))
    monkeypatch.setattr(cdr_poll, "record_consent", lambda *a, **k: None)
    saved = []
    monkeypatch.setattr(cdr_poll, "record_inbound", lambda db, item: saved.append(item) or 1)
    return saved


def run(items):
    db = MagicMock()
    db.execute.return_value.scalar.return_value = False
    return cdr_poll.ingest(db, items, only_changed=False)


def test_a_zero_second_inbound_cdr_becomes_a_missed_call_item(poll):
    run([cdr()])
    assert len(poll) == 1 and poll[0].source == "batchdialer" and poll[0].dedup_key == "bd_cdr:1"
    assert poll[0].phone == "+18135550142" and poll[0].channel == "missed_call"


@pytest.mark.parametrize("item", [cdr(duration=45, disposition="ANSWER"), cdr(direction="out"),
                                  cdr(campaign={"id": 999}), cdr(disposition="DNC_REQUEST")])
def test_answered_outbound_other_campaign_and_dnc_cdrs_are_not_missed_calls(poll, item):
    run([item])
    assert poll == []


def test_a_failed_inbox_write_never_stops_cdr_ingestion(monkeypatch, poll):
    def boom(db, item):
        raise RuntimeError("db")

    monkeypatch.setattr(cdr_poll, "record_inbound", boom)
    stats = run([cdr(1), cdr(2)])
    assert stats.seen == 2 and stats.failed == 0
