"""STOP / DND in GoHighLevel -> FA lending suppression, via a GHL workflow webhook."""
from __future__ import annotations

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")

PHONE = "+18135559301"
SECRET = "test-ghl-secret"


@pytest.fixture
def db():
    engine = create_engine(os.environ["DATABASE_URL"])
    conn = engine.connect()
    tx = conn.begin()
    conn.execute(text("ALTER TABLE lending.opt_out_events ADD COLUMN IF NOT EXISTS ghl_dnd_at timestamptz"))
    session = Session(bind=conn)
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


@pytest.fixture
def client(db, monkeypatch):
    from src.api.deps import get_db
    from src.api.lending_ghl_router import router
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "lending_ghl_webhook_secret", SECRET, raising=False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def _suppressed(db):
    return db.execute(text("SELECT count(*) FROM lending.suppression_list WHERE phone = :p"), {"p": PHONE}).scalar()


def test_a_ghl_dnd_webhook_suppresses_the_number(client, db):
    response = client.post("/webhooks/lending/ghl-opt-out", headers={"X-Webhook-Secret": SECRET},
                           json={"contact_id": "ghl-c1", "phone": "(813) 555-9301"})
    assert response.status_code == 200
    assert _suppressed(db) == 1


def test_a_webhook_without_the_secret_is_rejected(client, db):
    response = client.post("/webhooks/lending/ghl-opt-out", json={"contact_id": "ghl-c1", "phone": PHONE})
    assert response.status_code == 401
    assert _suppressed(db) == 0


def test_the_backstop_poll_suppresses_dnd_contacts_once(db):
    from src.lending.ghl_dnd_backstop import run_backstop
    pages = [[{"id": "ghl-c9", "phone": PHONE, "dnd": True}], []]
    fetch = lambda page: pages[page - 1] if page <= len(pages) else []
    assert run_backstop(db, fetch_dnd_page=fetch) == 1
    assert run_backstop(db, fetch_dnd_page=fetch) == 0      # already recorded: no repeat
    assert _suppressed(db) == 1
