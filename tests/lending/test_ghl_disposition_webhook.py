"""POST /webhooks/lending/ghl-disposition (G3). Pure: the store / match / delivery steps are faked, no DB."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
BODY = {"contact_id": "c-1", "phone": "(813) 555-0142", "disposition": "Requested Appointment", "user": "Josh"}


class _Db:
    def __init__(self):
        self.commits = self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def env(monkeypatch):
    from config.settings import get_settings
    from src.api import lending_ghl_router as r
    from src.api.deps import get_db

    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    calls = {"recorded": [], "delivered": [], "matched": [7]}
    monkeypatch.setattr(r, "record_disposition_event", lambda db, ev, body: calls["recorded"].append(ev) or 11)
    monkeypatch.setattr(r, "attach_pending", lambda db, now: list(calls["matched"]))
    monkeypatch.setattr(r, "deliver_matched", lambda ids: calls["delivered"].extend(ids))
    db = _Db()
    app = FastAPI()
    app.include_router(r.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app), db, calls


def test_records_matches_commits_then_delivers(env):
    client, db, calls = env
    resp = client.post("/webhooks/lending/ghl-disposition", headers=HEADERS, json=BODY)
    assert resp.status_code == 200 and resp.json() == {"recorded": True, "matched": 1}
    assert calls["recorded"][0].code == "BOOKED" and db.commits == 1
    assert calls["delivered"] == [7]


def test_waits_for_the_call_when_nothing_matches(env):
    client, _, calls = env
    calls["matched"] = []
    resp = client.post("/webhooks/lending/ghl-disposition", headers=HEADERS, json=BODY)
    assert resp.json() == {"recorded": True, "matched": 0} and calls["delivered"] == []


def test_wrong_secret_is_refused(env):
    client, _, calls = env
    resp = client.post("/webhooks/lending/ghl-disposition", headers={"X-Webhook-Secret": "nope"}, json=BODY)
    assert resp.status_code == 401 and calls["recorded"] == []


@pytest.mark.parametrize("body", [{**BODY, "disposition": "null"}, {**BODY, "disposition": "Spoke To Aunt"},
                                  {"contact_id": "c-1", "disposition": "Nurture"}])
def test_unusable_event_is_422(env, body):
    client, _, calls = env
    assert client.post("/webhooks/lending/ghl-disposition", headers=HEADERS, json=body).status_code == 422
    assert calls["recorded"] == []


def test_store_failure_is_500_with_a_generic_message(env, monkeypatch):
    from src.api import lending_ghl_router as r

    client, db, calls = env

    def boom(*a):
        raise RuntimeError('relation "lending.ghl_disposition_events" does not exist')
    monkeypatch.setattr(r, "record_disposition_event", boom)
    resp = client.post("/webhooks/lending/ghl-disposition", headers=HEADERS, json=BODY)
    assert resp.status_code == 500 and "relation" not in resp.text
    assert db.rollbacks == 1 and calls["delivered"] == []
