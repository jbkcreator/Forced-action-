"""
Inventory gate for POST /api/demo/deal-room — blocks demo/deal-room creation
when a ZIP has fewer than MIN_EXCLUSIVE_LEADS sellable, exclusive leads.
"""
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from src.api.deps import get_db
from src.api.main import app
from src.core.models import DistressScore, Owner, Property, ZipTerritory
from src.services.lead_pool_service import MIN_EXCLUSIVE_LEADS

client = TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def demo_token(monkeypatch):
    from config.settings import settings
    from src.api.admin_router import create_access_token
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    return create_access_token({"sub": "closer@heu.ai", "scope": "demo"})


@pytest.fixture
def client_with_db(fresh_db):
    app.dependency_overrides[get_db] = lambda: fresh_db
    yield client, fresh_db
    app.dependency_overrides.pop(get_db, None)


def _mk_property(db, parcel, zip_code, county_id="hillsborough"):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county_id, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=80.0,
        vertical_scores={"roofing": 60.0},
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.add(Owner(property_id=p.id, phone_1="8135550100", contact_info_confidence="high"))
    db.flush()


_BODY = {
    "prospect_name": "Test Prospect",
    "prospect_email": "prospect@example.com",
    "vertical": "roofing",
    "county_id": "hillsborough",
    "tier": "starter",
    "job_value": 5000.0,
    "close_rate": 0.3,
}


class TestInventoryGate:
    def test_blocks_zip_with_too_few_leads(self, client_with_db, demo_token):
        http, db = client_with_db
        zip_code = "40001"
        db.add(ZipTerritory(zip_code=zip_code, vertical="roofing", county_id="hillsborough", status="available"))
        for i in range(MIN_EXCLUSIVE_LEADS - 1):
            _mk_property(db, f"GATE-{i}", zip_code)
        db.commit()

        with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
            resp = http.post(
                "/api/demo/deal-room",
                json={**_BODY, "zip_code": zip_code},
                headers={"Authorization": f"Bearer {demo_token}"},
            )

        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "insufficient_leads"

    def test_allows_zip_with_enough_leads(self, client_with_db, demo_token):
        http, db = client_with_db
        zip_code = "40002"
        db.add(ZipTerritory(zip_code=zip_code, vertical="roofing", county_id="hillsborough", status="available"))
        for i in range(MIN_EXCLUSIVE_LEADS):
            _mk_property(db, f"GATE-OK-{i}", zip_code)
        db.commit()

        with patch("src.api.deal_room_router.get_lead_pool", return_value=[]):
            resp = http.post(
                "/api/demo/deal-room",
                json={**_BODY, "zip_code": zip_code},
                headers={"Authorization": f"Bearer {demo_token}"},
            )

        assert resp.status_code == 201
        assert "deal_room_url" in resp.json()
