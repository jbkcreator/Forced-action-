"""
Tests for GET /api/scarcity/zip.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.core.models import ZipTerritory


def _rand_zip() -> str:
    return f"8{uuid.uuid4().int % 10000:04d}"


def _rand_county() -> str:
    return f"testco_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def client_with_db(fresh_db, monkeypatch):
    monkeypatch.setattr("src.core.redis_client.redis_available", lambda: False)
    app.dependency_overrides[get_db] = lambda: fresh_db
    try:
        yield TestClient(app), fresh_db
    finally:
        app.dependency_overrides.pop(get_db, None)


def _mk_territory(db, *, zip_code, county, status, vertical="roofing"):
    row = ZipTerritory(zip_code=zip_code, vertical=vertical, county_id=county, status=status)
    db.add(row)
    db.flush()
    return row


@pytest.mark.parametrize(
    ("raw_status", "expected"),
    [
        ("available", "available"),
        ("held", "taken"),
        ("locked", "taken"),
        ("grace", "taken"),
    ],
)
def test_zip_endpoint_maps_all_statuses(client_with_db, raw_status, expected):
    client, db = client_with_db
    zip_code = _rand_zip()
    county = _rand_county()
    _mk_territory(db, zip_code=zip_code, county=county, status=raw_status)

    resp = client.get(f"/api/scarcity/zip?zip={zip_code}&vertical=roofing")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {"zip_code": zip_code, "vertical": "roofing", "status": expected}


def test_zip_endpoint_rejects_bad_zip(client_with_db):
    client, _ = client_with_db
    resp = client.get("/api/scarcity/zip?zip=abc&vertical=roofing")
    assert resp.status_code == 400


def test_zip_endpoint_rejects_bad_vertical(client_with_db):
    client, _ = client_with_db
    resp = client.get("/api/scarcity/zip?zip=33612&vertical=not_real")
    assert resp.status_code == 400


def test_zip_endpoint_404s_when_pair_missing(client_with_db):
    client, db = client_with_db
    zip_code = _rand_zip()
    county = _rand_county()
    _mk_territory(db, zip_code=zip_code, county=county, status="available", vertical="solar")

    resp = client.get(f"/api/scarcity/zip?zip={zip_code}&vertical=roofing")
    assert resp.status_code == 404


def test_zip_endpoint_matches_county_zip_status(client_with_db):
    client, db = client_with_db
    zip_code = _rand_zip()
    county = _rand_county()
    _mk_territory(db, zip_code=zip_code, county=county, status="held", vertical="roofing")

    zip_resp = client.get(f"/api/scarcity/zip?zip={zip_code}&vertical=roofing")
    county_resp = client.get(f"/api/scarcity/county?zip={zip_code}&vertical=roofing")

    assert zip_resp.status_code == 200, zip_resp.text
    assert county_resp.status_code == 200, county_resp.text
    assert zip_resp.json()["status"] == "taken"
    assert county_resp.json()["zip_status"] == "held"
