"""
Tests for the Task 8 ZIP scarcity roll-up + lead_count suppression on
GET /api/zip-availability (ADR 0029 — exclusivity is the scarcity signal,
lead_count is a secondary value signal).
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.core.models import Property, ZipTerritory


def _rand_zip() -> str:
    return f"9{uuid.uuid4().int % 10000:04d}"


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


def _mk_property(db, *, zip_code, county):
    p = Property(parcel_id=f"parcel_{uuid.uuid4().hex[:10]}", zip=zip_code, county_id=county)
    db.add(p)
    db.flush()
    return p


def _mk_territory(db, *, zip_code, county, status, vertical="roofing"):
    zt = ZipTerritory(zip_code=zip_code, vertical=vertical, county_id=county, status=status)
    db.add(zt)
    db.flush()
    return zt


def test_zip_availability_returns_open_and_total_zip_counts(client_with_db):
    client, db = client_with_db
    county = _rand_county()
    open_zip, taken_zip = _rand_zip(), _rand_zip()
    _mk_property(db, zip_code=open_zip, county=county)
    _mk_property(db, zip_code=taken_zip, county=county)
    _mk_territory(db, zip_code=taken_zip, county=county, status="locked")

    resp = client.get(f"/api/zip-availability?county_id={county}&vertical=roofing")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["total_zip_count"] == 2
    assert body["open_zip_count"] == 1


def test_zip_availability_omits_lead_count_for_taken_and_grace_zips(client_with_db):
    client, db = client_with_db
    county = _rand_county()
    taken_zip, grace_zip, available_zip = _rand_zip(), _rand_zip(), _rand_zip()
    _mk_property(db, zip_code=taken_zip, county=county)
    _mk_property(db, zip_code=grace_zip, county=county)
    _mk_property(db, zip_code=available_zip, county=county)
    _mk_territory(db, zip_code=taken_zip, county=county, status="locked")
    _mk_territory(db, zip_code=grace_zip, county=county, status="grace")

    resp = client.get(f"/api/zip-availability?county_id={county}&vertical=roofing")
    body = resp.json()
    by_zip = {z["zip_code"]: z for z in body["zips"]}

    assert by_zip[taken_zip]["lead_count"] is None
    assert by_zip[grace_zip]["lead_count"] is None
    # available_zip has no distress_scores rows -> zero leads -> also suppressed
    assert by_zip[available_zip]["lead_count"] is None
