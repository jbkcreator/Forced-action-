"""
Tests for T-B12-02 county-level territory scarcity on
GET /api/scarcity/county. Counts derived from ZipTerritory.status only.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.core.models import ZipTerritory


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


def _mk_territory(db, *, zip_code, county, status, vertical="roofing"):
    zt = ZipTerritory(zip_code=zip_code, vertical=vertical, county_id=county, status=status)
    db.add(zt)
    db.flush()
    return zt


def test_returns_open_and_locked_counts(client_with_db):
    client, db = client_with_db
    county = _rand_county()
    open_a, open_b, locked, grace = _rand_zip(), _rand_zip(), _rand_zip(), _rand_zip()
    _mk_territory(db, zip_code=open_a, county=county, status="available")
    _mk_territory(db, zip_code=open_b, county=county, status="available")
    _mk_territory(db, zip_code=locked, county=county, status="locked")
    _mk_territory(db, zip_code=grace, county=county, status="grace")

    resp = client.get(f"/api/scarcity/county?zip={open_a}&vertical=roofing")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["open_count"] == 2
    # grace is pressure but NOT the same as hard-locked — kept separate
    assert body["locked_count"] == 1
    assert body["grace_count"] == 1
    assert body["total_count"] == 4
    assert body["zip_status"] == "available"
    assert body["county_id"] == county


def test_vertical_scopes_counts(client_with_db):
    client, db = client_with_db
    county = _rand_county()
    z1, z2 = _rand_zip(), _rand_zip()
    _mk_territory(db, zip_code=z1, county=county, status="available", vertical="roofing")
    _mk_territory(db, zip_code=z2, county=county, status="locked", vertical="solar")

    resp = client.get(f"/api/scarcity/county?zip={z1}&vertical=roofing")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["open_count"] == 1
    assert body["locked_count"] == 0


def test_ambiguous_zip_status_picks_most_restrictive_when_vertical_omitted(client_with_db):
    client, db = client_with_db
    county = _rand_county()
    z1 = _rand_zip()
    # Same ZIP, different status per vertical — omitting `vertical` must not
    # arbitrarily report the ZIP as available just because one vertical is.
    _mk_territory(db, zip_code=z1, county=county, status="available", vertical="roofing")
    _mk_territory(db, zip_code=z1, county=county, status="locked", vertical="solar")

    resp = client.get(f"/api/scarcity/county?zip={z1}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["zip_status"] == "locked"


def test_unscoped_totals_do_not_double_count_a_zip_across_verticals(client_with_db):
    client, db = client_with_db
    county = _rand_county()
    ambiguous, plain_open = _rand_zip(), _rand_zip()
    # ambiguous ZIP: available for roofing, locked for solar — one real ZIP,
    # two territory rows. Its resolved (most-restrictive) status is locked.
    _mk_territory(db, zip_code=ambiguous, county=county, status="available", vertical="roofing")
    _mk_territory(db, zip_code=ambiguous, county=county, status="locked", vertical="solar")
    _mk_territory(db, zip_code=plain_open, county=county, status="available", vertical="roofing")

    resp = client.get(f"/api/scarcity/county?zip={plain_open}")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # 2 real ZIPs in this county, not 3 (naive per-status-row counting would
    # count `ambiguous` once as available AND once as locked).
    assert body["total_count"] == 2
    # `ambiguous` resolves to locked (most-restrictive) — it must NOT also be
    # counted as open just because one of its verticals is available.
    assert body["open_count"] == 1
    assert body["locked_count"] == 1


def test_unknown_zip_404(client_with_db):
    client, _ = client_with_db
    resp = client.get(f"/api/scarcity/county?zip={_rand_zip()}")
    assert resp.status_code == 404


def test_bad_zip_400(client_with_db):
    client, _ = client_with_db
    resp = client.get("/api/scarcity/county?zip=abc")
    assert resp.status_code == 400
