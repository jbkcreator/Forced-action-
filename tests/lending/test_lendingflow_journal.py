"""R1 0.5 T4: ``POST /api/lending/lendingflow`` on the inbound journal, the reconcile replay, and the optional HMAC.

No database: the journal store is the in-memory fake from ``test_inbound_journal`` and the lead table is a fake
``save_lead`` that dedupes on the vendor lead id or the phone+email hash, like ``lending.lendingflow_leads``.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.lending import webhook_failures
from src.lending.db import get_lending_db
from src.lending.inbound import journal
from src.lending.inbound import lendingflow as lf_inbound
from src.lending.lendingflow import SaveResult, dedupe_hash
from src.lending.lendingflow_webhook import router
from tests.lending.test_inbound_journal import MemoryStore

SECRET = "s3cret"
HMAC_KEY = "hmac-key"
FIXTURE = Path(__file__).parent / "fixtures" / "lendingflow_fake.json"


def _payload(**overrides):
    body = json.loads(FIXTURE.read_text(encoding="utf-8"))
    body.update(overrides)
    return body


class FakeLeads:
    """``lending.lendingflow_leads`` as far as save_lead's contract goes: one lead per vendor id / phone+email."""

    def __init__(self):
        self.leads: list[tuple[str, str]] = []
        self.error: Exception | None = None
        self.suppress = False

    def save_lead(self, db, parsed, raw_payload, *, now=None):
        if self.error is not None:
            raise self.error
        digest = dedupe_hash(parsed.phone, parsed.email)
        for i, (vendor, h) in enumerate(self.leads, start=1):
            if vendor == parsed.vendor_lead_id or h == digest:
                return SaveResult(i, f"uuid-{i}", created=False)
        self.leads.append((parsed.vendor_lead_id, digest))
        n = len(self.leads)
        return SaveResult(n, f"uuid-{n}", created=True, suppressed=self.suppress)


@pytest.fixture
def leads(monkeypatch):
    fake = FakeLeads()
    monkeypatch.setattr(lf_inbound, "save_lead", fake.save_lead)
    return fake


@pytest.fixture
def store(monkeypatch):
    memory = MemoryStore()
    monkeypatch.setattr(journal, "SqlJournalStore", lambda: memory)
    return memory


@pytest.fixture
def reported(monkeypatch):
    saved: list[tuple[str, dict, str]] = []

    def report(endpoint, body, error):
        saved.append((endpoint, body, type(error).__name__))
        return 900 + len(saved)

    monkeypatch.setattr(webhook_failures, "report_webhook_failure", report)
    return saved


@pytest.fixture
def settings(monkeypatch):
    from src.lending import lendingflow_webhook as hook

    s = hook.get_settings()
    monkeypatch.setattr(s, "lending_lendingflow_enabled", True)
    monkeypatch.setattr(s, "lending_lendingflow_webhook_secret", SecretStr(SECRET))
    monkeypatch.setattr(s, "lending_inbound_journal_enabled", True)
    monkeypatch.setattr(s, "lending_lendingflow_hmac_secret", None)
    return s


@pytest.fixture
def client(settings, store, leads, reported, monkeypatch):
    from src.lending import lendingflow_webhook as hook

    scheduled: list[int] = []
    monkeypatch.setattr(hook, "_deliver_in_background", lambda lead_id: scheduled.append(lead_id))
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_lending_db] = lambda: None  # journal path never uses the request session
    c = TestClient(app)
    c.scheduled = scheduled
    return c


def _post(client, payload=None, *, raw=None, headers=None):
    data = raw if raw is not None else json.dumps(payload if payload is not None else _payload()).encode()
    return client.post("/api/lending/lendingflow", content=data,
                       headers={"X-Webhook-Secret": SECRET, **(headers or {})})


def test_the_same_lead_sent_ten_times_is_one_lead_and_one_delivery(client, leads, store):
    responses = [_post(client) for _ in range(10)]
    assert [r.status_code for r in responses] == [200] * 10
    assert responses[0].json() == {"status": "created", "lead_id": "uuid-1"}
    assert {r.json()["status"] for r in responses[1:]} == {"duplicate"}
    assert len(leads.leads) == 1
    assert client.scheduled == [1]
    assert len(store.rows) == 1 and store.rows[1].source == "lendingflow_lead"


class DatabaseDown(Exception):
    pass


def test_a_failed_save_answers_200_and_goes_to_the_retry_queue(client, leads, store, reported):
    leads.error = DatabaseDown()
    response = _post(client)
    assert response.status_code == 200 and response.json() == {"status": "accepted"}
    assert reported == [("lendingflow_lead", _payload(), "DatabaseDown")]
    assert store.rows[1].status == "failed" and store.rows[1].failure_id == 901
    assert client.scheduled == []


def test_the_retry_queue_replays_the_saved_body_into_one_lead(client, leads, reported):
    leads.error = DatabaseDown()
    _post(client)
    leads.error = None
    endpoint, body, _ = reported[0]
    webhook_failures.ENDPOINT_HANDLERS[endpoint](None, body)
    webhook_failures.ENDPOINT_HANDLERS[endpoint](None, body)
    assert len(leads.leads) == 1
    assert webhook_failures.ENDPOINT_LABELS[endpoint] == "LendingFlow lead"


def test_a_raw_save_failure_is_500_so_lendingflow_sends_again(client, leads, store):
    store.insert_error = DatabaseDown()
    response = _post(client)
    assert response.status_code == 500 and response.json() == {"detail": "Could not store the lead"}
    assert leads.leads == []


def test_a_suppressed_lead_is_stored_but_not_delivered(client, leads):
    leads.suppress = True
    assert _post(client).json()["status"] == "created"
    assert client.scheduled == []


def test_a_resend_with_new_contact_details_still_reaches_the_duplicate_check(client, leads, store):
    """Same LendingFlow id, different phone: not a journal duplicate, so save_lead sees it (and raises the
    possible-duplicate inbox item) without creating a second lead."""
    _post(client)
    response = _post(client, _payload(phone="(727) 555-0199"))
    assert response.json() == {"status": "duplicate", "lead_id": "uuid-1"}
    assert len(store.rows) == 2 and len(leads.leads) == 1


@pytest.mark.parametrize("raw", [b"not json", b"[]", json.dumps({"lead_id": "LF-9"}).encode()])
def test_bad_bodies_are_400_and_never_journaled(client, store, raw):
    assert _post(client, raw=raw).status_code == 400
    assert store.rows == {}


def test_flag_off_keeps_the_direct_path(client, settings, leads, store, monkeypatch):
    from src.lending import lendingflow_webhook as hook

    monkeypatch.setattr(settings, "lending_inbound_journal_enabled", False)
    direct: list[str] = []
    monkeypatch.setattr(hook, "save_lead",
                        lambda db, parsed, raw: direct.append(parsed.vendor_lead_id) or SaveResult(5, "u-5", True))
    client.app.dependency_overrides[get_lending_db] = lambda: _NullDb()
    assert _post(client).json() == {"status": "created", "lead_id": "u-5"}
    assert direct == ["LF-1"] and store.rows == {}


class _NullDb:
    def commit(self):
        pass

    def rollback(self):
        pass


# ---- reconcile replay (T8) ---------------------------------------------------------------------------------------

def test_replay_creates_a_missing_lead_once(store, leads, reported):
    assert lf_inbound.replay_lendingflow_body(_payload()) == "created"
    assert lf_inbound.replay_lendingflow_body(_payload()) == "duplicate"
    assert len(leads.leads) == 1


def test_replay_of_a_lead_the_webhook_already_stored_is_a_duplicate(client, store, leads, reported):
    _post(client)
    assert lf_inbound.replay_lendingflow_body(_payload()) == "duplicate"
    assert len(leads.leads) == 1


def test_replay_of_a_lead_stored_before_the_journal_is_a_duplicate(store, leads, reported):
    leads.leads.append(("LF-1", "older-hash"))  # stored by the direct path: no journal row
    assert lf_inbound.replay_lendingflow_body(_payload()) == "duplicate"
    assert len(leads.leads) == 1


def test_replay_of_an_unparseable_body_saves_nothing(store, leads, reported):
    assert lf_inbound.replay_lendingflow_body({"lead_id": "LF-9"}) == "invalid"
    assert store.rows == {} and leads.leads == []


def test_replay_failure_is_saved_for_retry(store, leads, reported):
    leads.error = DatabaseDown()
    assert lf_inbound.replay_lendingflow_body(_payload()) == "failed"
    assert len(reported) == 1


# ---- D6 optional HMAC --------------------------------------------------------------------------------------------

def _signed(body: bytes, key: str = HMAC_KEY) -> str:
    return hmac.new(key.encode(), body, hashlib.sha256).hexdigest()


def test_hmac_unset_needs_no_signature(client):
    assert _post(client).status_code == 200


@pytest.mark.parametrize("prefix", ["", "sha256="])
def test_hmac_set_accepts_a_correct_signature(client, settings, monkeypatch, prefix):
    monkeypatch.setattr(settings, "lending_lendingflow_hmac_secret", SecretStr(HMAC_KEY))
    body = json.dumps(_payload()).encode()
    response = _post(client, raw=body, headers={"X-LendingFlow-Signature": prefix + _signed(body)})
    assert response.status_code == 200 and response.json()["status"] == "created"


@pytest.mark.parametrize("signature", [None, "", "deadbeef", "WRONG"])
def test_hmac_set_rejects_a_missing_or_wrong_signature(client, settings, store, monkeypatch, signature):
    monkeypatch.setattr(settings, "lending_lendingflow_hmac_secret", SecretStr(HMAC_KEY))
    body = json.dumps(_payload()).encode()
    sig = _signed(body, "other-key") if signature == "WRONG" else signature
    headers = {} if sig is None else {"X-LendingFlow-Signature": sig}
    response = _post(client, raw=body, headers=headers)
    assert response.status_code == 401 and response.json() == {"detail": "Invalid webhook signature"}
    assert store.rows == {}


def test_hmac_header_name_is_a_setting(client, settings, monkeypatch):
    monkeypatch.setattr(settings, "lending_lendingflow_hmac_secret", SecretStr(HMAC_KEY))
    monkeypatch.setattr(settings, "lending_lendingflow_hmac_header", "X-Signature")
    body = json.dumps(_payload()).encode()
    assert _post(client, raw=body, headers={"X-Signature": _signed(body)}).status_code == 200


def test_hmac_does_not_replace_the_shared_secret(client, settings, monkeypatch):
    monkeypatch.setattr(settings, "lending_lendingflow_hmac_secret", SecretStr(HMAC_KEY))
    body = json.dumps(_payload()).encode()
    response = client.post("/api/lending/lendingflow", content=body,
                           headers={"X-Webhook-Secret": "wrong", "X-LendingFlow-Signature": _signed(body)})
    assert response.status_code == 401
