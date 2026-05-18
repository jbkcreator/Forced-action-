"""
Shape test for GET /api/territory-map and GET /api/zip-availability.

Pins the `waitlist_count` field that the frontend MapZipPopup expects.
If this field disappears, the waitlist line in the popup silently breaks.

Uses a synthetic county_id so the endpoint falls back to the
`territory_db.keys()` branch (see main.py around line 3232) — keeps the
test isolated from the real Hillsborough centroid list and from any
pre-existing rows.

Run:
    pytest tests/test_territory_map.py -v
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.core.models import ZipTerritory


def _rand_zip() -> str:
    # 5-digit pseudo-ZIP outside any real centroid list — safe for non-prod county.
    return f"9{uuid.uuid4().int % 10000:04d}"


def _rand_county() -> str:
    return f"testco_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def client_with_db(fresh_db, monkeypatch):
    # Disable Redis cache so each test sees a fresh DB-derived response.
    monkeypatch.setattr("src.core.redis_client.redis_available", lambda: False)
    app.dependency_overrides[get_db] = lambda: fresh_db
    try:
        yield TestClient(app), fresh_db
    finally:
        app.dependency_overrides.pop(get_db, None)


def _mk_territory(db, *, zip_code, county, status, waitlist_emails=None,
                  vertical="roofing"):
    zt = ZipTerritory(
        zip_code=zip_code,
        vertical=vertical,
        county_id=county,
        status=status,
        waitlist_emails=waitlist_emails or [],
        locked_at=datetime.now(timezone.utc) if status != "available" else None,
        grace_expires_at=(
            datetime.now(timezone.utc) + timedelta(hours=24)
            if status == "grace" else None
        ),
    )
    db.add(zt)
    db.flush()
    return zt


def test_territory_map_returns_waitlist_count_for_grace_zip(client_with_db):
    client, db = client_with_db
    zip_code, county = _rand_zip(), _rand_county()
    _mk_territory(
        db, zip_code=zip_code, county=county, status="grace",
        waitlist_emails=["a@example.com", "b@example.com"],
    )

    resp = client.get(f"/api/territory-map?county_id={county}&vertical=roofing")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    target = next((z for z in body["zips"] if z["zip"] == zip_code), None)
    assert target is not None, f"{zip_code} missing from territory-map response"
    assert target["status"] == "grace"
    assert target["waitlist_count"] == 2


def test_territory_map_waitlist_count_zero_when_no_emails(client_with_db):
    client, db = client_with_db
    zip_code, county = _rand_zip(), _rand_county()
    _mk_territory(db, zip_code=zip_code, county=county, status="locked",
                  waitlist_emails=[])

    resp = client.get(f"/api/territory-map?county_id={county}&vertical=roofing")
    assert resp.status_code == 200
    target = next((z for z in resp.json()["zips"] if z["zip"] == zip_code), None)
    assert target is not None
    assert target["waitlist_count"] == 0
