"""
Tests for Sprint 4.9 — lead report engine.

Tests 1-5: build_report_data (pure data, real Postgres, no Playwright).
Test 6:    pdf_export (gated on Playwright — xfail if not installed).
Test 7:    download endpoint (owner/non-owner/pending/expired).
"""

import json
import pytest
from datetime import datetime, timedelta, timezone

from src.core.models import (
    BuildingPermit, DistressScore, Financial, Owner,
    PremiumPurchase, Property, Subscriber,
)
from src.services.lead_report.report_data import ReportDataError, build_report_data


# ── helpers ───────────────────────────────────────────────────────────────────

def _prop(db, parcel_id):
    p = Property(parcel_id=parcel_id, zip="33601", county_id="hillsborough", address="1 Test St")
    db.add(p)
    db.flush()
    return p


def _distress(db, property_id):
    ds = DistressScore(
        property_id=property_id,
        final_cds_score=72.5,
        lead_tier="Gold",
        distress_types={"foreclosure": True},
        urgency_level="High",
        vertical_scores={"rehab": 80},
        factor_scores={"cds": 72.5},
        county_id="hillsborough",
    )
    db.add(ds)
    db.flush()
    return ds


# ── test 1 — tracer bullet: full report has all 5 sections ───────────────────

def test_full_report_has_all_sections(fresh_db):
    p = _prop(fresh_db, "RPT-T-001")
    _distress(fresh_db, p.id)

    data = build_report_data(p.id, fresh_db, full=True)

    assert "header" in data
    assert "distress" in data
    assert "equity" in data
    assert "permits" in data
    assert "contact_validity" in data


# ── test 2 — brief omits sections 4-5 ────────────────────────────────────────

def test_brief_omits_permits_and_contact(fresh_db):
    p = _prop(fresh_db, "RPT-T-002")
    _distress(fresh_db, p.id)

    data = build_report_data(p.id, fresh_db, full=False)

    assert "header" in data
    assert "distress" in data
    assert "equity" in data
    assert "permits" not in data
    assert "contact_validity" not in data


# ── test 3 — equity caveat flag ───────────────────────────────────────────────

def test_equity_caveat_when_mortgage_null(fresh_db):
    p = _prop(fresh_db, "RPT-T-003")
    _distress(fresh_db, p.id)
    fresh_db.add(Financial(
        property_id=p.id, county_id="hillsborough",
        assessed_value_mkt=300000, total_lien_amount=10000,
        est_mortgage_bal=None,
    ))
    fresh_db.flush()

    data = build_report_data(p.id, fresh_db, full=False)

    assert data["equity"]["caveat"] is True


def test_equity_no_caveat_when_mortgage_set(fresh_db):
    p = _prop(fresh_db, "RPT-T-004")
    _distress(fresh_db, p.id)
    fresh_db.add(Financial(
        property_id=p.id, county_id="hillsborough",
        assessed_value_mkt=300000, total_lien_amount=10000,
        est_mortgage_bal=200000,
    ))
    fresh_db.flush()

    data = build_report_data(p.id, fresh_db, full=False)

    assert data["equity"]["caveat"] is False


# ── test 4 — missing distress raises ─────────────────────────────────────────

def test_missing_distress_raises(fresh_db):
    p = _prop(fresh_db, "RPT-T-005")
    # no distress score

    with pytest.raises(ReportDataError):
        build_report_data(p.id, fresh_db, full=True)


# ── test 5 — contact section has no raw PII ──────────────────────────────────

def test_contact_section_no_raw_pii(fresh_db):
    p = _prop(fresh_db, "RPT-T-006")
    _distress(fresh_db, p.id)
    fresh_db.add(Owner(
        property_id=p.id,
        owner_name="John Doe",
        phone_1="8135550101",
        email_1="john@example.com",
        contact_info_confidence="high",
        contact_info_confidence_score=0.87,
        contactability_detail={
            "sources": ["whitepages", "spokeo"],
            "rule_fired": "multi_source_match",
            "corroboration": "2 sources corroborate",
        },
    ))
    fresh_db.flush()

    data = build_report_data(p.id, fresh_db, full=True)
    contact = data["contact_validity"]

    assert contact["label"] == "high"
    assert contact["score_pct"] == 87
    assert contact["source_count"] == 2

    serialized = json.dumps(contact)
    assert "8135550101" not in serialized
    assert "john@example.com" not in serialized


# ── test 6 — pdf render writes a non-empty file ──────────────────────────────

@pytest.mark.xfail(reason="Playwright/chromium may not be installed in CI")
def test_pdf_render_writes_file(fresh_db, tmp_path):
    from src.services.lead_report.pdf_export import render_lead_report_pdf

    p = _prop(fresh_db, "RPT-T-007")
    _distress(fresh_db, p.id)
    data = build_report_data(p.id, fresh_db, full=True)

    out = render_lead_report_pdf(data, purchase_id=9999, full=True, output_dir=tmp_path)

    assert out.exists()
    assert out.stat().st_size > 1000


# ── test 7 — download endpoint ────────────────────────────────────────────────

def test_download_endpoint(fresh_db, tmp_path):
    """Owner gets 200; non-owner gets 404; pending gets 409."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db
    from src.core.models import Subscriber

    def _sub(email, feed_uuid, stripe_id):
        return Subscriber(
            email=email, event_feed_uuid=feed_uuid,
            stripe_customer_id=stripe_id,
            tier="pro", vertical="investor", county_id="hillsborough", status="active",
        )

    sub = _sub("test-rpt@heu.ai", "rpt-feed-001", "cus_test_rpt001")
    fresh_db.add(sub)
    fresh_db.flush()

    p = _prop(fresh_db, "RPT-T-DLTEST")
    report_path = tmp_path / "test_report.pdf"
    report_path.write_bytes(b"%PDF fake content for test")

    purchase = PremiumPurchase(
        subscriber_id=sub.id,
        sku="report",
        paid_via="credits",
        credits_spent=3,
        property_id=p.id,
        status="delivered",
        output_ref=str(report_path),
        delivered_at=datetime.now(timezone.utc),
    )
    fresh_db.add(purchase)

    pending = PremiumPurchase(
        subscriber_id=sub.id,
        sku="report",
        paid_via="credits",
        credits_spent=3,
        property_id=p.id,
        status="pending",
    )
    fresh_db.add(pending)

    other_sub = _sub("other@heu.ai", "rpt-feed-002", "cus_test_rpt002")
    fresh_db.add(other_sub)
    fresh_db.flush()

    app.dependency_overrides[get_db] = lambda: fresh_db
    client = TestClient(app, raise_server_exceptions=False)
    try:
        # owner gets 200 PDF
        resp = client.get(f"/api/premium/{purchase.id}/download?feed_uuid={sub.event_feed_uuid}")
        assert resp.status_code == 200, resp.text
        assert "pdf" in resp.headers["content-type"]

        # non-owner gets 404 (403 collapsed)
        resp = client.get(f"/api/premium/{purchase.id}/download?feed_uuid={other_sub.event_feed_uuid}")
        assert resp.status_code == 404

        # pending → 409
        resp = client.get(f"/api/premium/{pending.id}/download?feed_uuid={sub.event_feed_uuid}")
        assert resp.status_code == 409
    finally:
        app.dependency_overrides.pop(get_db, None)


# ── test 8 — download enforces expiration ─────────────────────────────────

def test_download_expired_report(fresh_db, tmp_path):
    """Expired report returns 404 instead of the PDF."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db
    from src.core.models import Subscriber
    from datetime import datetime, timedelta, timezone

    sub = Subscriber(email="expired@heu.ai", event_feed_uuid="rpt-exp-001",
                     stripe_customer_id="cus_exp", tier="pro",
                     vertical="investor", county_id="hillsborough", status="active")
    fresh_db.add(sub)
    fresh_db.flush()

    p = _prop(fresh_db, "RPT-T-EXP")
    report_path = tmp_path / "expired_report.pdf"
    report_path.write_bytes(b"%PDF expired")

    purchase = PremiumPurchase(
        subscriber_id=sub.id, sku="report", paid_via="credits",
        credits_spent=3, property_id=p.id, status="delivered",
        output_ref=str(report_path),
        output_ref_expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        delivered_at=datetime.now(timezone.utc),
    )
    fresh_db.add(purchase)
    fresh_db.flush()

    app.dependency_overrides[get_db] = lambda: fresh_db
    client = TestClient(app, raise_server_exceptions=False)
    try:
        resp = client.get(f"/api/premium/{purchase.id}/download?feed_uuid={sub.event_feed_uuid}")
        assert resp.status_code == 404, resp.text
    finally:
        app.dependency_overrides.pop(get_db, None)


# ── test 9 — path traversal is blocked ────────────────────────────────────

def test_download_path_traversal_blocked(fresh_db, tmp_path):
    """A malicious output_ref outside reports/ returns 404."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    from src.api.deps import get_db
    from src.core.models import Subscriber
    from datetime import datetime, timedelta, timezone

    sub = Subscriber(email="traverse@heu.ai", event_feed_uuid="rpt-trv-001",
                     stripe_customer_id="cus_trv", tier="pro",
                     vertical="investor", county_id="hillsborough", status="active")
    fresh_db.add(sub)
    fresh_db.flush()

    p = _prop(fresh_db, "RPT-T-TRV")
    purchase = PremiumPurchase(
        subscriber_id=sub.id, sku="report", paid_via="credits",
        credits_spent=3, property_id=p.id, status="delivered",
        output_ref="../../../../etc/passwd",  # malicious path
        output_ref_expires_at=datetime.now(timezone.utc) + timedelta(days=1),
        delivered_at=datetime.now(timezone.utc),
    )
    fresh_db.add(purchase)
    fresh_db.flush()

    app.dependency_overrides[get_db] = lambda: fresh_db
    client = TestClient(app, raise_server_exceptions=False)
    try:
        resp = client.get(f"/api/premium/{purchase.id}/download?feed_uuid={sub.event_feed_uuid}")
        assert resp.status_code == 404, resp.text
    finally:
        app.dependency_overrides.pop(get_db, None)
