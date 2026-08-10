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
from sqlalchemy import text
from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.core.models import County, DistressScore, Property, ZipTerritory


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


def _mk_qualified_lead(db, *, zip_code, county, vertical="roofing", score=60):
    """A property with a qualified, non-guess distress score >= Silver floor
    for the vertical — i.e. one real sellable lead in this ZIP."""
    p = Property(parcel_id=f"T-{uuid.uuid4().hex[:10]}", address="1 Real St",
                 zip=zip_code, county_id=county)
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, county_id=county,
        score_date=datetime.now(timezone.utc).date(),
        qualified=True, is_guess_lead=False,
        vertical_scores={vertical: score},
    ))
    db.flush()
    return p


def test_available_zip_with_no_qualified_leads_shows_no_active_leads(client_with_db):
    # Item 6: an empty territory must not be offered for sale.
    client, db = client_with_db
    zip_code, county = _rand_zip(), _rand_county()
    _mk_territory(db, zip_code=zip_code, county=county, status="available")

    target = next(z for z in client.get(
        f"/api/territory-map?county_id={county}&vertical=roofing"
    ).json()["zips"] if z["zip"] == zip_code)
    assert target["status"] == "no_active_leads"
    assert target["lead_count"] == 0


def test_available_zip_with_a_qualified_lead_stays_available(client_with_db):
    client, db = client_with_db
    zip_code, county = _rand_zip(), _rand_county()
    _mk_territory(db, zip_code=zip_code, county=county, status="available")
    _mk_qualified_lead(db, zip_code=zip_code, county=county, score=60)

    target = next(z for z in client.get(
        f"/api/territory-map?county_id={county}&vertical=roofing"
    ).json()["zips"] if z["zip"] == zip_code)
    assert target["status"] == "available"
    assert target["lead_count"] == 1


def test_below_silver_floor_lead_does_not_make_zip_available(client_with_db):
    # A lead scoring below the Silver floor (40) for the vertical is not sellable,
    # so the ZIP still reads no_active_leads.
    client, db = client_with_db
    zip_code, county = _rand_zip(), _rand_county()
    _mk_territory(db, zip_code=zip_code, county=county, status="available")
    _mk_qualified_lead(db, zip_code=zip_code, county=county, score=10)

    target = next(z for z in client.get(
        f"/api/territory-map?county_id={county}&vertical=roofing"
    ).json()["zips"] if z["zip"] == zip_code)
    assert target["status"] == "no_active_leads"


def test_banner_and_map_agree_on_availability(client_with_db, monkeypatch):
    # The two surfaces must report the same available count for a vertical.
    client, db = client_with_db
    county = _rand_county()
    # landing-data only computes territory_availability for allowed counties.
    import src.api.main as _main
    monkeypatch.setattr(_main, "_ALLOWED_LANDING_COUNTIES",
                        _main._ALLOWED_LANDING_COUNTIES | {county})
    # landing-data (unlike territory-map) requires a real counties row.
    venture_key = db.execute(
        text("SELECT venture_key FROM counties WHERE venture_key IS NOT NULL LIMIT 1")
    ).scalar() or "hillsborough_distress"
    db.add(County(county_id=county, display_name="Test County",
                  venture_key=venture_key, zip_prefixes=[], is_active=True,
                  created_at=datetime.now(timezone.utc)))
    db.flush()
    z_lead, z_empty = _rand_zip(), _rand_zip()
    _mk_territory(db, zip_code=z_lead, county=county, status="available")
    _mk_territory(db, zip_code=z_empty, county=county, status="available")
    _mk_qualified_lead(db, zip_code=z_lead, county=county, score=60)

    ta = client.get(f"/api/landing-data?county_id={county}&vertical=roofing"
                    ).json()["territory_availability"]
    zips = client.get(f"/api/territory-map?county_id={county}&vertical=roofing"
                      ).json()["zips"]
    map_avail = sum(1 for z in zips if z["status"] == "available")

    assert ta["total_zips"] == 2
    assert ta["available_zips"] == 1          # only the ZIP with a real lead
    assert ta["available_zips"] == map_avail  # banner == map
