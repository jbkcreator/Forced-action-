"""
GET /api/insurance-distress/availability (Task #7 / ADR 0032, D7/D8).

Feeds the pack card: per-locked-ZIP qualifying lead count, gated to ZIPs
with >=5 (min-5, D8) so a sparse ZIP never surfaces a pack the buyer can't
actually receive.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.api.deps import get_db
from src.api.main import app
from src.core.models import DistressScore, Incident, Owner, Property, Subscriber, ZipTerritory


@pytest.fixture
def client_with_db(fresh_db):
    from fastapi.testclient import TestClient
    app.dependency_overrides[get_db] = lambda: fresh_db
    yield TestClient(app, raise_server_exceptions=False), fresh_db
    app.dependency_overrides.pop(get_db, None)


def _mk_subscriber(db, uuid, county="hillsborough"):
    s = Subscriber(
        stripe_customer_id=f"cus_{uuid}", tier="pro", vertical="wholesalers",
        county_id=county, status="active", event_feed_uuid=uuid,
        email=f"{uuid}@example.com",
    )
    db.add(s)
    db.flush()
    return s


def _mk_territory(db, subscriber, zip_code, county):
    db.add(ZipTerritory(
        subscriber_id=subscriber.id, zip_code=zip_code, vertical="wholesalers",
        county_id=county, status="locked",
    ))
    db.flush()


def _mk_property(db, parcel, zip_code, county, vertical_scores, has_incident=None, incident_days_ago=10):
    p = Property(parcel_id=parcel, zip=zip_code, county_id=county, address=f"{parcel} Test St")
    db.add(p)
    db.flush()
    db.add(DistressScore(
        property_id=p.id, qualified=True, final_cds_score=80.0,
        vertical_scores=vertical_scores,
        score_date=datetime.now(timezone.utc).date(),
    ))
    db.add(Owner(property_id=p.id, phone_1="8135550100", contact_info_confidence="high"))
    if has_incident:
        db.add(Incident(
            property_id=p.id, incident_type=has_incident,
            incident_date=(datetime.now(timezone.utc) - timedelta(days=incident_days_ago)).date(),
        ))
    db.flush()
    return p.id


class TestInsuranceDistressAvailability:
    def test_zip_with_5_qualifying_leads_is_surfaced_with_price(self, client_with_db):
        http, db = client_with_db
        zip_code = "95401"
        county = "hillsborough"
        sub = _mk_subscriber(db, "avail-ok", county)
        _mk_territory(db, sub, zip_code, county)
        for i in range(5):
            _mk_property(db, f"AV-A{i}", zip_code, county, {"wholesalers": 50.0}, has_incident="storm_damage")

        with patch("src.api.main.get_settings") as mock_get_settings, \
             patch("src.api.main.stripe") as mock_stripe:
            mock_settings = MagicMock()
            mock_settings.active_stripe_price.return_value = "price_insurance_distress_pack"
            mock_get_settings.return_value = mock_settings
            mock_stripe.Price.retrieve.return_value = {"unit_amount": 29900, "currency": "usd"}

            resp = http.get("/api/insurance-distress/availability", params={"feed_uuid": "avail-ok"})

        assert resp.status_code == 200
        body = resp.json()
        assert body["zips"] == [{"zip_code": zip_code, "count": 5}]
        assert body["amount"] == 29900
        assert body["currency"] == "usd"

    def test_zip_with_fewer_than_5_qualifying_leads_is_hidden(self, client_with_db):
        http, db = client_with_db
        zip_code = "95402"
        county = "hillsborough"
        sub = _mk_subscriber(db, "avail-short", county)
        _mk_territory(db, sub, zip_code, county)
        for i in range(4):
            _mk_property(db, f"AV-B{i}", zip_code, county, {"wholesalers": 50.0}, has_incident="storm_damage")

        with patch("src.api.main.get_settings") as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.active_stripe_price.return_value = "price_insurance_distress_pack"
            mock_get_settings.return_value = mock_settings

            resp = http.get("/api/insurance-distress/availability", params={"feed_uuid": "avail-short"})

        assert resp.status_code == 200
        assert resp.json()["zips"] == []
