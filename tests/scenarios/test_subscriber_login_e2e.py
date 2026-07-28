"""
Stage / fa061 — End-to-End: subscriber password login + feed gating.

pytest marker: scenario (requires DATABASE_URL @ fa061)

Drives the real FastAPI app via TestClient with the DB dependencies overridden
to the rolled-back `fresh_db` session. Verifies:
  - feed is token-gated (bare uuid → 401)
  - email+password and feed_uuid+password login both work
  - wrong password / unknown email → uniform 401
  - a token for subscriber A cannot read subscriber B's feed (403)
  - forgot-password → reset-password sets a new password; expired token → 400
  - /stats shares the same gate
"""

from __future__ import annotations

import uuid

import pytest

pytestmark = pytest.mark.scenario


@pytest.fixture
def client(fresh_db):
    """TestClient with every get_db dependency pointed at the rolled-back session
    and the subscriber JWT secret forced to a known value."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    import src.api.main as main_mod
    import src.api.subscriber_router as sub_router
    import src.core.database as db_mod
    from src.services import subscriber_auth

    def _override_db():
        yield fresh_db

    app.dependency_overrides[main_mod.get_db] = _override_db
    app.dependency_overrides[sub_router.get_db] = _override_db
    app.dependency_overrides[db_mod.get_db] = _override_db

    # Deterministic secret so tokens verify regardless of env.
    orig_secret = subscriber_auth._subscriber_secret
    subscriber_auth._subscriber_secret = lambda: "e2e-test-secret"

    try:
        yield TestClient(app)
    finally:
        subscriber_auth._subscriber_secret = orig_secret
        app.dependency_overrides.clear()


def _make_subscriber(fresh_db, *, email, password=None, status="active"):
    from src.core.models import Subscriber
    from src.services import subscriber_auth
    uid = uuid.uuid4().hex[:10]
    sub = Subscriber(
        stripe_customer_id=f"cus_auth_{uid}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        status=status,
        email=email,
        event_feed_uuid=f"feed-{uid}",
        password_hash=(subscriber_auth.hash_password(password) if password else None),
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


# ── feed gating ─────────────────────────────────────────────────────────────

def test_feed_requires_token(client, fresh_db):
    sub = _make_subscriber(fresh_db, email=f"a_{uuid.uuid4().hex[:6]}@e.com", password="Passw0rd!")
    r = client.get(f"/api/feed/{sub.event_feed_uuid}")
    assert r.status_code == 401


def test_login_email_then_feed_200(client, fresh_db):
    pw = "Passw0rd!"
    sub = _make_subscriber(fresh_db, email=f"b_{uuid.uuid4().hex[:6]}@e.com", password=pw)

    r = client.post("/api/subscriber/login", json={"email": sub.email, "password": pw})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["feed_uuid"] == sub.event_feed_uuid
    token = body["access_token"]

    r2 = client.get(f"/api/feed/{sub.event_feed_uuid}", headers={"Authorization": f"Bearer {token}"})
    assert r2.status_code == 200, r2.text


def test_login_feed_uuid_mode(client, fresh_db):
    pw = "Passw0rd!"
    sub = _make_subscriber(fresh_db, email=f"c_{uuid.uuid4().hex[:6]}@e.com", password=pw)
    r = client.post("/api/subscriber/login", json={"feed_uuid": sub.event_feed_uuid, "password": pw})
    assert r.status_code == 200, r.text
    assert r.json()["feed_uuid"] == sub.event_feed_uuid


def test_wrong_password_401(client, fresh_db):
    sub = _make_subscriber(fresh_db, email=f"d_{uuid.uuid4().hex[:6]}@e.com", password="Passw0rd!")
    r = client.post("/api/subscriber/login", json={"email": sub.email, "password": "nope"})
    assert r.status_code == 401


def test_unknown_email_401(client, fresh_db):
    r = client.post("/api/subscriber/login", json={"email": "nobody@nowhere.com", "password": "x"})
    assert r.status_code == 401


def test_no_password_set_401(client, fresh_db):
    # Existing subscriber with no password yet → can't log in (must reset).
    sub = _make_subscriber(fresh_db, email=f"e_{uuid.uuid4().hex[:6]}@e.com", password=None)
    r = client.post("/api/subscriber/login", json={"email": sub.email, "password": "anything"})
    assert r.status_code == 401


def test_token_for_other_feed_403(client, fresh_db):
    pw = "Passw0rd!"
    a = _make_subscriber(fresh_db, email=f"f_{uuid.uuid4().hex[:6]}@e.com", password=pw)
    b = _make_subscriber(fresh_db, email=f"g_{uuid.uuid4().hex[:6]}@e.com", password=pw)

    token_a = client.post("/api/subscriber/login", json={"email": a.email, "password": pw}).json()["access_token"]
    # A's token against B's feed → 403
    r = client.get(f"/api/feed/{b.event_feed_uuid}", headers={"Authorization": f"Bearer {token_a}"})
    assert r.status_code == 403


def test_stats_gate(client, fresh_db):
    pw = "Passw0rd!"
    sub = _make_subscriber(fresh_db, email=f"h_{uuid.uuid4().hex[:6]}@e.com", password=pw)
    assert client.get(f"/api/feed/{sub.event_feed_uuid}/stats").status_code == 401
    token = client.post("/api/subscriber/login", json={"email": sub.email, "password": pw}).json()["access_token"]
    r = client.get(f"/api/feed/{sub.event_feed_uuid}/stats", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200, r.text


# ── forgot / reset ──────────────────────────────────────────────────────────

def test_forgot_then_reset_flow(client, fresh_db, monkeypatch):
    from sqlalchemy import text as sa_text
    old_pw = "OldPassw0rd!"
    sub = _make_subscriber(fresh_db, email=f"i_{uuid.uuid4().hex[:6]}@e.com", password=old_pw)

    # Capture the raw reset token by intercepting the email send.
    captured = {}

    def fake_reset_email(email, name, raw_token, **kwargs):
        captured["token"] = raw_token
    monkeypatch.setattr(
        "src.services.subscriber_auth.send_subscriber_password_reset_email", fake_reset_email
    )

    # forgot-password always 200
    r = client.post("/api/subscriber/forgot-password", json={"email": sub.email})
    assert r.status_code == 200 and r.json()["ok"] is True
    assert "token" in captured  # email path fired → token generated

    # reset-password with the raw token
    new_pw = "BrandNewPass1"
    r2 = client.post("/api/subscriber/reset-password", json={"token": captured["token"], "new_password": new_pw})
    assert r2.status_code == 200, r2.text

    # old password no longer works; new one does
    assert client.post("/api/subscriber/login", json={"email": sub.email, "password": old_pw}).status_code == 401
    assert client.post("/api/subscriber/login", json={"email": sub.email, "password": new_pw}).status_code == 200

    # token fields cleared
    row = fresh_db.execute(sa_text(
        "SELECT reset_token_hash FROM subscribers WHERE id = :id"
    ), {"id": sub.id}).first()
    assert row.reset_token_hash is None


def test_reset_with_invalid_token_400(client, fresh_db):
    r = client.post("/api/subscriber/reset-password", json={"token": "garbage", "new_password": "whatever123"})
    assert r.status_code == 400


def test_forgot_unknown_email_still_200(client, fresh_db):
    # No enumeration: unknown email returns 200 just like a known one.
    r = client.post("/api/subscriber/forgot-password", json={"email": "ghost@nowhere.com"})
    assert r.status_code == 200 and r.json()["ok"] is True
