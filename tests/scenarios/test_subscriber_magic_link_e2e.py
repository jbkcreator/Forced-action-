"""
Magic-link (passwordless) login — End-to-End.

pytest marker: scenario (requires DATABASE_URL)

Drives the real FastAPI app via TestClient with the DB dependencies overridden
to the rolled-back `fresh_db` session. Verifies:
  - request → email captured → verify → JWT opens the feed
  - a used token cannot be reused (single-use)
  - an expired token is rejected
  - an unknown email still returns {"ok": True} (no enumeration)
  - no signup path ever calls generate_random_password / emails a password
    (this is the Definition of Done for the magic-link rollout)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

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

    orig_secret = subscriber_auth._subscriber_secret
    subscriber_auth._subscriber_secret = lambda: "e2e-test-secret"

    try:
        yield TestClient(app)
    finally:
        subscriber_auth._subscriber_secret = orig_secret
        app.dependency_overrides.clear()


def _make_subscriber(fresh_db, *, email, status="active"):
    from src.core.models import Subscriber
    uid = uuid.uuid4().hex[:10]
    sub = Subscriber(
        stripe_customer_id=f"cus_magic_{uid}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        status=status,
        email=email,
        event_feed_uuid=f"feed-{uid}",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


def _capture_magic_link_email(monkeypatch):
    captured = {}

    def fake_send(email, name, raw_token):
        captured["email"] = email
        captured["token"] = raw_token

    monkeypatch.setattr("src.services.subscriber_auth.send_magic_link_email", fake_send)
    return captured


# ── request → verify → feed ─────────────────────────────────────────────────

def test_request_then_verify_opens_feed(client, fresh_db, monkeypatch):
    sub = _make_subscriber(fresh_db, email=f"ml_a_{uuid.uuid4().hex[:6]}@e.com")
    captured = _capture_magic_link_email(monkeypatch)

    r = client.post("/api/subscriber/magic-link/request", json={"email": sub.email})
    assert r.status_code == 200 and r.json()["ok"] is True
    assert captured["email"] == sub.email
    assert "token" in captured

    r2 = client.post("/api/subscriber/magic-link/verify", json={"token": captured["token"]})
    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["feed_uuid"] == sub.event_feed_uuid
    assert body["vertical"] == sub.vertical
    token = body["access_token"]

    r3 = client.get(f"/api/feed/{sub.event_feed_uuid}", headers={"Authorization": f"Bearer {token}"})
    assert r3.status_code == 200, r3.text


def test_verify_returns_investor_vertical_for_ui_routing(client, fresh_db, monkeypatch):
    """T-B3-02: the UI's postSignupRoute helper branches on this field to send
    investor signups to /deals/submit instead of the dashboard."""
    sub = _make_subscriber(fresh_db, email=f"ml_f_{uuid.uuid4().hex[:6]}@e.com")
    sub.vertical = "investor"
    fresh_db.flush()
    captured = _capture_magic_link_email(monkeypatch)

    client.post("/api/subscriber/magic-link/request", json={"email": sub.email})
    r = client.post("/api/subscriber/magic-link/verify", json={"token": captured["token"]})
    assert r.status_code == 200, r.text
    assert r.json()["vertical"] == "investor"


def test_reused_token_rejected(client, fresh_db, monkeypatch):
    sub = _make_subscriber(fresh_db, email=f"ml_b_{uuid.uuid4().hex[:6]}@e.com")
    captured = _capture_magic_link_email(monkeypatch)

    client.post("/api/subscriber/magic-link/request", json={"email": sub.email})
    first = client.post("/api/subscriber/magic-link/verify", json={"token": captured["token"]})
    assert first.status_code == 200

    second = client.post("/api/subscriber/magic-link/verify", json={"token": captured["token"]})
    assert second.status_code == 400


def test_expired_token_rejected(client, fresh_db, monkeypatch):
    from sqlalchemy import text as sa_text
    sub = _make_subscriber(fresh_db, email=f"ml_c_{uuid.uuid4().hex[:6]}@e.com")
    captured = _capture_magic_link_email(monkeypatch)

    client.post("/api/subscriber/magic-link/request", json={"email": sub.email})
    # Force the stored expiry into the past.
    fresh_db.execute(
        sa_text("UPDATE subscribers SET magic_link_expires_at = :exp WHERE id = :id"),
        {"exp": datetime.now(timezone.utc) - timedelta(minutes=1), "id": sub.id},
    )
    fresh_db.flush()

    r = client.post("/api/subscriber/magic-link/verify", json={"token": captured["token"]})
    assert r.status_code == 400


def test_garbage_token_rejected(client, fresh_db):
    r = client.post("/api/subscriber/magic-link/verify", json={"token": "not-a-real-token"})
    assert r.status_code == 400


def test_unknown_email_still_200_no_enumeration(client, fresh_db):
    r = client.post("/api/subscriber/magic-link/request", json={"email": "ghost@nowhere.com"})
    assert r.status_code == 200 and r.json()["ok"] is True


def test_login_still_works_as_dormant_fallback(client, fresh_db):
    """Decision D2: existing password hashes must keep working after the
    magic-link rollout — no removal of /login in this task."""
    from src.services import subscriber_auth as auth
    pw = "Passw0rd!"
    sub = _make_subscriber(fresh_db, email=f"ml_d_{uuid.uuid4().hex[:6]}@e.com")
    sub.password_hash = auth.hash_password(pw)
    fresh_db.flush()

    r = client.post("/api/subscriber/login", json={"email": sub.email, "password": pw})
    assert r.status_code == 200, r.text


# ── regression guard: no code path emits a password ─────────────────────────

def test_signup_never_calls_generate_random_password(client, fresh_db, monkeypatch):
    """This IS the Definition of Done: a cold signup receives a magic link,
    never a plaintext password. Force generate_random_password to explode and
    drive the REAL signup entrypoint (POST /api/free-signup -> signup_engine.
    create_free_account_by_email -> the send_welcome branch) — not a hand-built
    Subscriber row — so this actually fails if a real call site regresses."""
    from src.services import subscriber_auth
    from sqlalchemy import text as sa_text

    def _boom():
        raise AssertionError("generate_random_password must not be called during signup")

    monkeypatch.setattr(subscriber_auth, "generate_random_password", _boom)

    email = f"ml_e_{uuid.uuid4().hex[:6]}@e.com"
    r = client.post("/api/free-signup", json={
        "email": email, "vertical": "roofing", "county_id": "hillsborough",
    })
    assert r.status_code == 201, r.text

    # A magic link was actually issued (proves issue_magic_link ran on this
    # real signup path, via send_welcome_email(..., magic_link_url=...)) and
    # no password was ever set on the row.
    row = fresh_db.execute(sa_text(
        "SELECT password_hash, magic_link_hash FROM subscribers WHERE email = :email"
    ), {"email": email}).first()
    assert row.password_hash is None
    assert row.magic_link_hash is not None


def test_free_signup_accepts_investor_vertical(client, fresh_db, monkeypatch):
    """T-B3-02 routing hook: investor is a legitimate free-signup account
    type even though the full Block 5 deal-intake destination is not built.
    """
    _capture_magic_link_email(monkeypatch)

    email = f"ml_inv_{uuid.uuid4().hex[:6]}@e.com"
    r = client.post("/api/free-signup", json={
        "email": email, "vertical": "investor", "county_id": "hillsborough",
    })
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["vertical"] == "investor"

def test_welcome_email_body_never_contains_a_password(monkeypatch):
    """Regression guard on the email template itself: with a magic link
    supplied, the rendered body must not contain any password-related copy."""
    from types import SimpleNamespace
    from src.services.email import send_welcome_email

    sent = {}

    def fake_send_email(to, subject, body_text, body_html=None, **kwargs):
        sent["text"] = body_text
        sent["html"] = body_html
        return True

    monkeypatch.setattr("src.services.email.send_email", fake_send_email)

    subscriber = SimpleNamespace(
        email="test@example.com",
        name="Test",
        tier="starter",
        vertical="roofing",
        founding_member=False,
        event_feed_uuid="feed-xyz",
        id=1,
    )
    send_welcome_email(subscriber, magic_link_url="https://app.example.com/auth/verify?token=abc")

    assert "password" not in sent["text"].lower()
    assert "password" not in sent["html"].lower()
    assert "auth/verify?token=abc" in sent["text"]
    assert "auth/verify?token=abc" in sent["html"]
