"""
Tests for the founding-price deadline wiring on /api/founding-spots and
/api/founding-summary (Task 8, ADR 0029) — both must agree with the shared
evaluate_founding_gate() helper so no visitor sees a contradictory price.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, get_db
from src.core.models import County, FoundingSubscriberCount


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


def test_founding_spots_unavailable_when_deadline_passed_even_with_spots_remaining(client_with_db):
    client, db = client_with_db
    county_id = _rand_county_id()
    db.add(County(
        county_id=county_id, display_name="Test County",
        founding_price_deadline_at=datetime.now(timezone.utc) - timedelta(days=1),
    ))
    db.add(FoundingSubscriberCount(tier="starter", vertical="roofing", county_id=county_id, count=0))
    db.flush()

    resp = client.get(f"/api/founding-spots?tier=starter&vertical=roofing&county_id={county_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["founding_remaining"] > 0
    assert body["deadline_passed"] is True
    assert body["founding_available"] is False


def test_founding_spots_available_when_deadline_null_and_spots_remain(client_with_db):
    client, db = client_with_db
    county_id = _rand_county_id()
    db.add(County(county_id=county_id, display_name="Test County"))
    db.add(FoundingSubscriberCount(tier="starter", vertical="roofing", county_id=county_id, count=0))
    db.flush()

    resp = client.get(f"/api/founding-spots?tier=starter&vertical=roofing&county_id={county_id}")
    body = resp.json()

    assert body["founding_available"] is True
    assert body["deadline_passed"] is False
    assert body["founding_price_deadline_at"] is None


def test_founding_summary_unavailable_when_deadline_passed_even_with_spots_remaining(client_with_db):
    client, db = client_with_db
    county_id = _rand_county_id()
    db.add(County(
        county_id=county_id, display_name="Test County",
        founding_price_deadline_at=datetime.now(timezone.utc) - timedelta(days=1),
    ))
    db.flush()

    resp = client.get(f"/api/founding-summary?vertical=roofing&county_id={county_id}")
    body = resp.json()

    assert body["total_remaining"] > 0
    assert body["deadline_passed"] is True
    assert body["founding_available"] is False
