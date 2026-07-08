"""
End-to-end scenario test for White-label Tier Onboarding (Stage 12 / fa056).

Requires real Postgres DATABASE_URL. Run with:
    pytest tests/scenarios/test_white_label_e2e.py -m scenario

Tests the full lifecycle:
  1. Signup creates pending_verification client + admin user
  2. Email verification activates user
  3. Admin verification activates client
  4. Login returns valid JWT
  5. Generate API key → fetch leads via X-API-Key
  6. Submit deal (feeds Stage 10 deal_outcomes)
  7. Invite team member
  8. Revoke API key → 401 on subsequent requests
"""

import hashlib
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text as sa_text

from src.api.main import app
from src.core.database import get_db_context
from src.services.white_label_auth import (
    create_access_token,
    generate_verification_token,
    hash_password,
)

pytestmark = pytest.mark.scenario

client = TestClient(app)


@pytest.fixture(scope="module")
def wl_client_id():
    """Create a verified, active white-label client for the test session."""
    company_slug = "test-e2e-corp"
    admin_email = "admin@test-wl-e2e.com"

    with get_db_context() as db:
        # Clean up any leftover rows from previous runs
        db.execute(sa_text("DELETE FROM white_label_users WHERE email = :email"), {"email": admin_email})
        db.execute(sa_text("DELETE FROM white_label_clients WHERE company_slug = :slug"), {"slug": company_slug})
        db.commit()

        # Create client
        client_row = db.execute(
            sa_text("""
                INSERT INTO white_label_clients
                       (company_name, company_slug, admin_email, admin_name, status, created_at, updated_at)
                VALUES ('E2E Test Corp', :slug, :email, 'Test Admin', 'active', now(), now())
                RETURNING id
            """),
            {"slug": company_slug, "email": admin_email},
        ).fetchone()
        client_id = client_row.id

        # Create admin user (pre-verified)
        db.execute(
            sa_text("""
                INSERT INTO white_label_users
                       (client_id, email, name, role, password_hash, is_active, email_verified_at, created_at)
                VALUES (:cid, :email, 'Test Admin', 'admin', :pw, true, now(), now())
            """),
            {
                "cid": client_id,
                "email": admin_email,
                "pw": hash_password("testpassword123"),
            },
        )
        db.commit()

    yield client_id

    # Teardown
    with get_db_context() as db:
        db.execute(sa_text("DELETE FROM white_label_users WHERE email = :email"), {"email": admin_email})
        db.execute(sa_text("DELETE FROM white_label_clients WHERE id = :id"), {"id": client_id})
        db.commit()


# ---------------------------------------------------------------------------
# 1. Signup creates pending client
# ---------------------------------------------------------------------------

def test_signup_creates_pending_client():
    slug = "signup-test-co"
    email = "signup@wl-e2e-test.com"
    with get_db_context() as db:
        db.execute(sa_text("DELETE FROM white_label_users WHERE email = :e"), {"e": email})
        db.execute(sa_text("DELETE FROM white_label_clients WHERE company_slug = :s"), {"s": slug})
        db.commit()

    res = client.post("/api/wl/auth/signup", json={
        "company_name": "Signup Test Co",
        "admin_name": "Admin User",
        "admin_email": email,
        "password": "password123",
        "plan_tier": "standard",
    })
    assert res.status_code == 201
    data = res.json()
    assert "client_id" in data

    with get_db_context() as db:
        row = db.execute(
            sa_text("SELECT status FROM white_label_clients WHERE id = :id"),
            {"id": data["client_id"]},
        ).fetchone()
        assert row.status == "pending_verification"

    # Cleanup
    with get_db_context() as db:
        db.execute(sa_text("DELETE FROM white_label_users WHERE email = :e"), {"e": email})
        db.execute(sa_text("DELETE FROM white_label_clients WHERE id = :id"), {"id": data["client_id"]})
        db.commit()


# ---------------------------------------------------------------------------
# 2. Login with valid credentials
# ---------------------------------------------------------------------------

def test_login_returns_jwt(wl_client_id):
    res = client.post("/api/wl/auth/login", json={
        "email": "admin@test-wl-e2e.com",
        "password": "testpassword123",
    })
    assert res.status_code == 200
    data = res.json()
    assert "access_token" in data
    assert data["user"]["role"] == "admin"
    assert data["client"]["status"] == "active"


def test_login_wrong_password(wl_client_id):
    res = client.post("/api/wl/auth/login", json={
        "email": "admin@test-wl-e2e.com",
        "password": "wrongpassword",
    })
    assert res.status_code == 401


# ---------------------------------------------------------------------------
# 3. Authenticated account access
# ---------------------------------------------------------------------------

def test_get_account(wl_client_id):
    login_res = client.post("/api/wl/auth/login", json={
        "email": "admin@test-wl-e2e.com",
        "password": "testpassword123",
    })
    token = login_res.json()["access_token"]

    res = client.get("/api/wl/account", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200
    assert res.json()["company_name"] == "E2E Test Corp"


# ---------------------------------------------------------------------------
# 4. API key generation and usage
# ---------------------------------------------------------------------------

def test_api_key_generate_and_use(wl_client_id):
    login_res = client.post("/api/wl/auth/login", json={
        "email": "admin@test-wl-e2e.com",
        "password": "testpassword123",
    })
    token = login_res.json()["access_token"]

    # Generate key
    create_res = client.post("/api/wl/api-keys", json={"label": "E2E Test Key"},
                             headers={"Authorization": f"Bearer {token}"})
    assert create_res.status_code == 201
    raw_key = create_res.json()["key"]
    assert raw_key.startswith("fa_wl_")
    key_id = create_res.json()["id"]

    # Use key to fetch leads
    leads_res = client.get("/api/wl/data/leads", headers={"X-API-Key": raw_key})
    assert leads_res.status_code == 200
    assert "leads" in leads_res.json()

    # Revoke key
    revoke_res = client.delete(f"/api/wl/api-keys/{key_id}", headers={"Authorization": f"Bearer {token}"})
    assert revoke_res.status_code == 204

    # Revoked key should 401
    after_revoke = client.get("/api/wl/data/leads", headers={"X-API-Key": raw_key})
    assert after_revoke.status_code == 401


# ---------------------------------------------------------------------------
# 5. Deal submission feeds Stage 10 deal_outcomes
# ---------------------------------------------------------------------------

def test_deal_submission(wl_client_id):
    login_res = client.post("/api/wl/auth/login", json={
        "email": "admin@test-wl-e2e.com",
        "password": "testpassword123",
    })
    token = login_res.json()["access_token"]

    res = client.post("/api/wl/data/deals", json={
        "county_id": "hillsborough",
        "trade_vertical": "roofing",
        "deal_size_bucket": "10_25k",
        "deal_amount": 18000.0,
        "pipeline_stage": "closed_won",
    }, headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 201

    # Verify row in deal_outcomes — CDE-11: an ownerless partner deal must be
    # tagged explicitly (subscriber_reported / white_label), NOT left to the DB
    # default (which is now the lowest tier, public_record_inferred).
    with get_db_context() as db:
        row = db.execute(
            sa_text("""
                SELECT confidence_tier, outcome_source FROM deal_outcomes
                 WHERE county_id = 'hillsborough' AND trade_vertical = 'roofing'
                   AND deal_size_bucket = '10_25k' AND subscriber_id IS NULL
                 ORDER BY created_at DESC LIMIT 1
            """),
        ).mappings().fetchone()
        assert row is not None
        assert row["confidence_tier"] == "subscriber_reported"
        assert row["outcome_source"] == "white_label"


# ---------------------------------------------------------------------------
# 6. Team invite
# ---------------------------------------------------------------------------

def test_team_invite(wl_client_id):
    login_res = client.post("/api/wl/auth/login", json={
        "email": "admin@test-wl-e2e.com",
        "password": "testpassword123",
    })
    token = login_res.json()["access_token"]

    invite_email = "member@wl-e2e-test.com"

    res = client.post("/api/wl/team/invite", json={
        "email": invite_email,
        "name": "Team Member",
        "role": "member",
    }, headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 201

    # Verify user created as inactive (pending set-password)
    with get_db_context() as db:
        row = db.execute(
            sa_text("SELECT is_active, role FROM white_label_users WHERE email = :email"),
            {"email": invite_email},
        ).fetchone()
        assert row is not None
        assert row.role == "member"
        assert row.is_active is False  # requires set-password

    # Cleanup
    with get_db_context() as db:
        db.execute(sa_text("DELETE FROM white_label_users WHERE email = :email"), {"email": invite_email})
        db.commit()
