"""
Tests that county admin CRUD can read/write the Task 8 landing fields
(landing_featured_testimonial, founding_price_deadline_at) — ADR 0029.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_db


def _rand_county_id() -> str:
    return f"testco_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def client_with_db(fresh_db, monkeypatch):
    monkeypatch.setattr("src.core.redis_client.redis_available", lambda: False)
    app.dependency_overrides[get_db] = lambda: fresh_db
    try:
        yield TestClient(app), fresh_db
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.fixture
def admin_token(monkeypatch):
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))

    from src.api.admin_router import create_access_token
    return create_access_token({"sub": "admin", "scope": "admin"})


@pytest.fixture
def auth_headers(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


def test_admin_can_set_testimonial_and_deadline_via_patch(client_with_db, auth_headers):
    client, db = client_with_db
    county_id = _rand_county_id()
    resp = client.post(
        "/api/admin/counties",
        json={"county_id": county_id, "display_name": "Test County"},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text

    deadline = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
    testimonials = [{"quote": "Closed two jobs in a week.", "name": "Sarah M."}]
    resp = client.patch(
        f"/api/admin/counties/{county_id}",
        json={"landing_featured_testimonials": testimonials, "founding_price_deadline_at": deadline},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["landing_featured_testimonials"] == testimonials
    assert body["founding_price_deadline_at"] is not None


def test_admin_can_set_testimonial_and_deadline_via_create(client_with_db, auth_headers):
    client, db = client_with_db
    county_id = _rand_county_id()
    deadline = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
    testimonials = [{"quote": "Closed two jobs in a week.", "name": "Sarah M."}]

    resp = client.post(
        "/api/admin/counties",
        json={
            "county_id": county_id,
            "display_name": "Test County",
            "landing_featured_testimonials": testimonials,
            "founding_price_deadline_at": deadline,
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()

    assert body["landing_featured_testimonials"] == testimonials
    assert body["founding_price_deadline_at"] is not None


def test_admin_can_clear_deadline_via_patch_null(client_with_db, auth_headers):
    client, db = client_with_db
    county_id = _rand_county_id()
    resp = client.post(
        "/api/admin/counties",
        json={"county_id": county_id, "display_name": "Test County"},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text

    deadline = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
    resp = client.patch(
        f"/api/admin/counties/{county_id}",
        json={"founding_price_deadline_at": deadline},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["founding_price_deadline_at"] is not None

    resp = client.patch(
        f"/api/admin/counties/{county_id}",
        json={"founding_price_deadline_at": None},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["founding_price_deadline_at"] is None
