"""
Tests for the Task 8 additions to GET /api/landing-data: featured_testimonial
and the founding deadline block (ADR 0029). Uses a real Postgres fresh_db,
same pattern as test_territory_map.py, so the ORM-written County row round-
trips through the real query path.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.core.models import County, ExpansionCandidate, FoundingSubscriberCount


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


def _mk_launched_county(db, county_id, **kwargs):
    county = County(county_id=county_id, display_name="Test County", **kwargs)
    db.add(county)
    db.add(ExpansionCandidate(county_id=county_id, status="launched"))
    db.flush()
    return county


def test_landing_data_returns_testimonials_and_founding_block_when_set(client_with_db, monkeypatch):
    client, db = client_with_db
    county_id = _rand_county_id()
    monkeypatch.setattr("src.api.main._ALLOWED_LANDING_COUNTIES", {county_id})
    deadline = datetime.now(timezone.utc) + timedelta(days=5)
    testimonials = [{"quote": "Great leads.", "name": "Sarah M."}, {"quote": "Best ROI.", "name": "Mike T."}]
    _mk_launched_county(
        db, county_id,
        landing_featured_testimonials=testimonials,
        founding_price_deadline_at=deadline,
    )

    resp = client.get(f"/api/landing-data?county_id={county_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["featured_testimonials"] == testimonials
    assert body["founding"]["deadline_passed"] is False
    assert body["founding"]["founding_available"] is True


def test_landing_data_empty_list_when_testimonials_and_deadline_unset(client_with_db, monkeypatch):
    client, db = client_with_db
    county_id = _rand_county_id()
    monkeypatch.setattr("src.api.main._ALLOWED_LANDING_COUNTIES", {county_id})
    _mk_launched_county(db, county_id)

    resp = client.get(f"/api/landing-data?county_id={county_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["featured_testimonials"] == []
    # No deadline set -> gate falls back to spots-only; no FoundingSubscriberCount
    # rows for this fresh county means spots are wide open.
    assert body["founding"]["founding_available"] is True
    assert body["founding"]["deadline_passed"] is False


def test_landing_data_founding_available_agrees_with_founding_spots_on_exhaustion(client_with_db, monkeypatch):
    """The exact gap the PR review flagged: /api/landing-data must not say
    founding is available when /api/founding-spots would say it's sold out.
    No deadline involved here — spot exhaustion alone must be reflected."""
    client, db = client_with_db
    county_id = _rand_county_id()
    monkeypatch.setattr("src.api.main._ALLOWED_LANDING_COUNTIES", {county_id})
    _mk_launched_county(db, county_id)
    from config.settings import get_settings
    cap_per_tier = get_settings().founding_spot_limit
    for tier in ["starter", "pro", "dominator"]:
        db.add(FoundingSubscriberCount(tier=tier, vertical="roofing", county_id=county_id, count=cap_per_tier))
    db.flush()

    landing_resp = client.get(f"/api/landing-data?county_id={county_id}&vertical=roofing")
    summary_resp = client.get(f"/api/founding-summary?vertical=roofing&county_id={county_id}")

    assert landing_resp.json()["founding"]["founding_available"] is False
    assert summary_resp.json()["founding_available"] is False
