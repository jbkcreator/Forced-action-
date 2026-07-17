"""
Lead-pack checkout `segment` param (Task #7 / ADR 0032, D5/D6/D7/D8).

`segment` is server-validated against an allowlist — the client must never be
able to steer arbitrary segment SQL. Unknown segments are rejected before any
DB/Stripe call. When segment="insurance_distress", the min-5 gate and the
Stripe price are both routed through the segment-specific (FEMA-safe) rules.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.deps import get_db
from src.api.main import app
from src.core.models import DistressScore, Incident, Owner, Property, Subscriber, ZipTerritory

client = TestClient(app, raise_server_exceptions=False)


def test_unknown_segment_rejected_422():
    resp = client.post("/api/lead-pack/checkout", json={
        "feed_uuid": "does-not-matter",
        "zip_code": "33601",
        "vertical": "roofing",
        "segment": "not_a_real_segment",
    })
    assert resp.status_code == 422


def _mk_subscriber(db, uuid, vertical="wholesalers", county="hillsborough"):
    s = Subscriber(
        stripe_customer_id=f"cus_{uuid}", tier="pro", vertical=vertical,
        county_id=county, status="active", event_feed_uuid=uuid,
        email=f"{uuid}@example.com",
    )
    db.add(s)
    db.flush()
    return s


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


@pytest.fixture
def client_with_db(fresh_db):
    app.dependency_overrides[get_db] = lambda: fresh_db
    yield client, fresh_db
    app.dependency_overrides.pop(get_db, None)


class TestSegmentGateAndPricing:
    def test_segment_gate_blocks_zip_with_fewer_than_5_qualifying_leads(self, client_with_db):
        """5 properties qualify for the plain wholesalers pack, but only 4 carry
        the segment's storm/flood damage — the insurance_distress pack must be
        blocked even though the plain pack would pass."""
        http, db = client_with_db
        zip_code = "95201"
        _mk_subscriber(db, "seg-short")
        for i in range(4):
            _mk_property(db, f"SEG-A{i}", zip_code, "hillsborough",
                         {"wholesalers": 50.0}, has_incident="storm_damage")
        # 5th qualifies for the plain pack (score >= floor) but has no damage incident.
        _mk_property(db, "SEG-A4", zip_code, "hillsborough", {"wholesalers": 50.0})

        with patch("src.api.main.get_settings") as mock_get_settings, \
             patch("src.api.main.stripe") as mock_stripe:
            mock_settings = MagicMock()
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_price.return_value = "price_test"
            mock_get_settings.return_value = mock_settings
            mock_stripe.Price.retrieve.return_value = {"unit_amount": 29900, "currency": "usd"}
            mock_stripe.PaymentIntent.create.return_value = {"client_secret": "secret_123"}

            resp = http.post("/api/lead-pack/checkout", json={
                "feed_uuid": "seg-short",
                "zip_code": zip_code,
                "vertical": "wholesalers",
                "segment": "insurance_distress",
            })

        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "insufficient_leads"

    def test_segment_selects_premium_price(self, client_with_db):
        http, db = client_with_db
        zip_code = "95202"
        _mk_subscriber(db, "seg-price")
        for i in range(5):
            _mk_property(db, f"SEG-B{i}", zip_code, "hillsborough",
                         {"wholesalers": 50.0}, has_incident="flood_damage")

        with patch("src.api.main.get_settings") as mock_get_settings, \
             patch("src.api.main.stripe") as mock_stripe:
            mock_settings = MagicMock()
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_price.side_effect = lambda name: f"price_{name}"
            mock_settings.checkout_recovery_lead_pack_enabled = False
            mock_get_settings.return_value = mock_settings
            mock_stripe.Price.retrieve.return_value = {"unit_amount": 29900, "currency": "usd"}
            mock_stripe.PaymentIntent.create.return_value = {"client_secret": "secret_123"}

            resp = http.post("/api/lead-pack/checkout", json={
                "feed_uuid": "seg-price",
                "zip_code": zip_code,
                "vertical": "wholesalers",
                "segment": "insurance_distress",
            })

        assert resp.status_code == 200
        mock_settings.active_stripe_price.assert_any_call("insurance_distress_pack")
        mock_stripe.Price.retrieve.assert_called_once_with("price_insurance_distress_pack")

    def test_segment_purchase_allowed_in_owned_zip(self, client_with_db):
        """PR #139 issue 1: the insurance-distress pack is a premium add-on
        sold inside the subscriber's OWN locked ZIPs (that's the only place the
        availability feed card surfaces it). Checkout must not reject it with
        zip_already_owned the way a plain base-lead pack would."""
        http, db = client_with_db
        zip_code = "95203"
        sub = _mk_subscriber(db, "seg-owned")
        db.add(ZipTerritory(
            subscriber_id=sub.id, zip_code=zip_code, vertical="wholesalers",
            county_id="hillsborough", status="locked",
        ))
        db.flush()
        for i in range(5):
            _mk_property(db, f"SEG-OWN{i}", zip_code, "hillsborough",
                         {"wholesalers": 50.0}, has_incident="storm_damage")

        with patch("src.api.main.get_settings") as mock_get_settings, \
             patch("src.api.main.stripe") as mock_stripe:
            mock_settings = MagicMock()
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_price.side_effect = lambda name: f"price_{name}"
            mock_settings.checkout_recovery_lead_pack_enabled = False
            mock_get_settings.return_value = mock_settings
            mock_stripe.Price.retrieve.return_value = {"unit_amount": 29900, "currency": "usd"}
            mock_stripe.PaymentIntent.create.return_value = {"client_secret": "secret_123"}

            resp = http.post("/api/lead-pack/checkout", json={
                "feed_uuid": "seg-owned",
                "zip_code": zip_code,
                "vertical": "wholesalers",
                "segment": "insurance_distress",
            })

        assert resp.status_code == 200
        mock_stripe.PaymentIntent.create.assert_called_once()

    def test_guess_lead_blocks_checkout_before_paymentintent(self, client_with_db):
        """PR #139 issue 3: checkout must exclude guess leads with the SAME
        predicate as the webhook reservation. Five segment matches exist but one
        is a guess lead — checkout must return insufficient_leads and never
        create a PaymentIntent (otherwise the webhook charges then refunds)."""
        http, db = client_with_db
        zip_code = "95204"
        _mk_subscriber(db, "seg-guess")
        for i in range(4):
            _mk_property(db, f"SEG-G{i}", zip_code, "hillsborough",
                         {"wholesalers": 50.0}, has_incident="storm_damage")
        guess = _mk_property(db, "SEG-G4", zip_code, "hillsborough",
                             {"wholesalers": 50.0}, has_incident="storm_damage")
        db.query(DistressScore).filter_by(property_id=guess).update({"is_guess_lead": True})
        db.flush()

        with patch("src.api.main.get_settings") as mock_get_settings, \
             patch("src.api.main.stripe") as mock_stripe:
            mock_settings = MagicMock()
            mock_settings.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
            mock_settings.active_stripe_price.side_effect = lambda name: f"price_{name}"
            mock_get_settings.return_value = mock_settings
            mock_stripe.Price.retrieve.return_value = {"unit_amount": 29900, "currency": "usd"}
            mock_stripe.PaymentIntent.create.return_value = {"client_secret": "secret_123"}

            resp = http.post("/api/lead-pack/checkout", json={
                "feed_uuid": "seg-guess",
                "zip_code": zip_code,
                "vertical": "wholesalers",
                "segment": "insurance_distress",
            })

        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "insufficient_leads"
        mock_stripe.PaymentIntent.create.assert_not_called()
