"""P2.4: /webhooks/lending/ghl-stage also keeps each opportunity's current stage and since when."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

SECRET = "test-ghl-secret"
HEADERS = {"X-Webhook-Secret": SECRET}
T0 = datetime(2026, 11, 10, 14, 0, tzinfo=timezone.utc)


@pytest.fixture
def client(closing_db, monkeypatch):
    from config.settings import get_settings
    from src.api.deps import get_db
    from src.api.lending_ghl_router import router
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SecretStr(SECRET), raising=False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: closing_db
    return TestClient(app)


@pytest.fixture
def clock(monkeypatch):
    now = {"t": T0}
    monkeypatch.setattr("src.api.lending_ghl_router._now", lambda: now["t"])
    return now


def _post(client, stage: str) -> None:
    r = client.post("/webhooks/lending/ghl-stage", headers=HEADERS,
                    json={"opportunity_id": "opp-9", "stage_name": stage, "phone": "(813) 555-0142"})
    assert r.status_code == 200


def _current(db):
    return db.execute(text("SELECT stage_key, entered_at, phone FROM lending.ghl_opportunity_stage")).one()


def test_redelivery_keeps_entered_at(client, closing_db, clock):
    _post(client, "Application")
    clock["t"] = T0 + timedelta(hours=1)
    _post(client, "Application")
    assert _current(closing_db) == ("application", T0, "+18135550142")


def test_real_move_back_resets_entered_at(client, closing_db, clock):
    _post(client, "Application")
    clock["t"] = T0 + timedelta(days=1)
    _post(client, "Submitted to lender")
    clock["t"] = T0 + timedelta(days=2)
    _post(client, "Application")
    assert _current(closing_db)[:2] == ("application", T0 + timedelta(days=2))


def test_scoreboard_events_unchanged(client, closing_db, clock):
    _post(client, "Application")
    _post(client, "Submitted to lender")
    _post(client, "Application")
    assert closing_db.execute(text("SELECT count(*) FROM lending.ghl_stage_events")).scalar() == 2
