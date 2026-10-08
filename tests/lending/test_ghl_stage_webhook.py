"""GHL workflow webhook -> lending.ghl_stage_events (scoreboard "showed")."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
BODY = {"opportunity_id": "opp-1", "stage_name": "Held", "pipeline_id": "pipe-1", "phone": "(813) 555-0142", "booked_by": "Dana"}


@pytest.fixture
def client(lending_db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.api.lending_ghl_router import router
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: lending_db
    return TestClient(app)


def _rows(db):
    return db.execute(text("SELECT stage_key, phone, booked_by, pipeline_id FROM lending.ghl_stage_events")).all()


def test_records_the_stage_with_a_normalized_phone(client, lending_db):
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=BODY)
    assert r.status_code == 200 and r.json() == {"recorded": True}
    assert _rows(lending_db) == [("held", "+18135550142", "Dana", "pipe-1")]


def test_a_redelivered_webhook_is_not_counted_twice(client, lending_db):
    client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=BODY)
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json=BODY)
    assert r.json() == {"recorded": False} and len(_rows(lending_db)) == 1


def test_wrong_or_missing_secret_is_rejected(client, lending_db):
    assert client.post("/webhooks/lending/ghl-stage", json=BODY).status_code == 401
    assert client.post("/webhooks/lending/ghl-stage", headers={"X-Webhook-Secret": "nope"}, json=BODY).status_code == 401
    assert _rows(lending_db) == []


def test_missing_stage_or_opportunity_is_422(client):
    assert client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json={"opportunity_id": "x"}).status_code == 422
    assert client.post("/webhooks/lending/ghl-stage", headers=HEADERS, json={"stage_name": "Held"}).status_code == 422
