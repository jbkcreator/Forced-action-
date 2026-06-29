"""
A4 — degraded-provider detection tests.

Pure decision logic (no DB) + DB-backed orchestration (fresh_db / shared Postgres).

Run:
    pytest tests/test_enrichment_monitor.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.core.models import EnrichmentAnomalyLog, EnrichmentUsageLog, Owner, Property
from src.tasks.match_rate_monitor import (
    apply_degraded_discount,
    evaluate_provider_health,
    recent_hit_rate,
    run_provider_health_check,
)


def _log(vendor, success, created_at):
    return EnrichmentUsageLog(
        vendor=vendor, purpose="skip_trace", success=success,
        cost_cents=(2 if success else 0), created_at=created_at,
    )


class TestEvaluateProviderHealth:
    def test_below_floor_with_enough_sample_is_degraded(self):
        r = evaluate_provider_health(
            "batchdata", rate=0.10, sample_size=140, floor=0.25, min_sample=30
        )
        assert r["degraded"] is True
        assert r["skipped"] is False

    def test_below_min_sample_is_skipped_never_degraded(self):
        # Only 6 attempts, all misses (rate 0.0) — a fluke afternoon, not an outage.
        r = evaluate_provider_health(
            "tracerfy", rate=0.0, sample_size=6, floor=0.40, min_sample=30
        )
        assert r["skipped"] is True
        assert r["degraded"] is False

    def test_rate_exactly_at_floor_is_not_degraded(self):
        # Floor is the line: at the floor is still healthy; only below trips it.
        r = evaluate_provider_health(
            "batchdata", rate=0.25, sample_size=140, floor=0.25, min_sample=30
        )
        assert r["degraded"] is False
        assert r["skipped"] is False

    def test_rate_above_floor_is_healthy(self):
        r = evaluate_provider_health(
            "tracerfy", rate=0.41, sample_size=220, floor=0.40, min_sample=30
        )
        assert r["degraded"] is False
        assert r["skipped"] is False


class TestRecentHitRate:
    def test_aggregates_only_vendor_inside_window(self, fresh_db):
        # Synthetic vendor names so seeded rows are isolated from real
        # enrichment_usage_logs data in the shared DB.
        vendor = "test_a4_batchdata"
        other = "test_a4_tracerfy"
        now = datetime.now(timezone.utc)
        inside = now - timedelta(hours=1)
        outside = now - timedelta(hours=72)

        # vendor in-window: 2 hits / 10 → 0.20
        for _ in range(2):
            fresh_db.add(_log(vendor, True, inside))
        for _ in range(8):
            fresh_db.add(_log(vendor, False, inside))
        # noise that must be excluded:
        fresh_db.add(_log(vendor, True, outside))   # same vendor, too old
        fresh_db.add(_log(other, False, inside))    # other vendor
        fresh_db.flush()

        rate, n = recent_hit_rate(fresh_db, vendor, hours=48)
        assert n == 10
        assert rate == pytest.approx(0.20)


def _seed_degraded(db, vendor, hits, misses, when):
    for _ in range(hits):
        db.add(_log(vendor, True, when))
    for _ in range(misses):
        db.add(_log(vendor, False, when))
    db.flush()


class TestRunProviderHealthCheck:
    def test_degraded_provider_writes_row_and_alerts(self, fresh_db):
        vendor = "test_a4_batchdata"
        # 5 hits / 40 → 0.125, below a 0.25 floor, sample 40 ≥ 30.
        _seed_degraded(fresh_db, vendor, hits=5, misses=35,
                       when=datetime.now(timezone.utc) - timedelta(hours=1))

        with patch("src.tasks.match_rate_monitor.send_alert") as alert:
            alert.return_value = True
            results = run_provider_health_check(
                fresh_db, floors={vendor: 0.25},
                window_hours=48, min_sample=30, cooldown_hours=24,
            )

        assert any(r["provider"] == vendor and r["degraded"] for r in results)
        assert alert.call_count == 1

        rows = (
            fresh_db.query(EnrichmentAnomalyLog)
            .filter(EnrichmentAnomalyLog.provider == vendor)
            .all()
        )
        assert len(rows) == 1
        assert float(rows[0].observed_hit_rate) == pytest.approx(0.125)
        assert rows[0].alert_sent is True

    def test_degraded_run_discounts_batch_and_records_count(self, fresh_db):
        vendor = "test_a4_disc_run"
        when = datetime.now(timezone.utc) - timedelta(hours=1)
        # 5 scored hits + 35 misses → rate 0.125 < 0.25 floor, n=40 ≥ min_sample.
        scored_props = [
            _hit_with_owner(fresh_db, vendor, 0.800, when, f"A4-DRUN-{i}")
            for i in range(5)
        ]
        for _ in range(35):
            fresh_db.add(_log(vendor, False, when))
        fresh_db.flush()

        with patch("src.tasks.match_rate_monitor.send_alert") as alert:
            alert.return_value = True
            run_provider_health_check(
                fresh_db, floors={vendor: 0.25}, window_hours=48,
                min_sample=30, cooldown_hours=24, multiplier=0.5,
            )

        row = (
            fresh_db.query(EnrichmentAnomalyLog)
            .filter(EnrichmentAnomalyLog.provider == vendor).one()
        )
        assert row.records_affected == 5
        score = fresh_db.query(Owner).filter(
            Owner.property_id == scored_props[0]).one().contact_info_confidence_score
        assert float(score) == pytest.approx(0.400)   # 0.800 * 0.5

    def test_second_run_within_cooldown_does_not_realert(self, fresh_db):
        vendor = "test_a4_batchdata"
        _seed_degraded(fresh_db, vendor, hits=5, misses=35,
                       when=datetime.now(timezone.utc) - timedelta(hours=1))

        kwargs = dict(floors={vendor: 0.25}, window_hours=48,
                      min_sample=30, cooldown_hours=24)
        with patch("src.tasks.match_rate_monitor.send_alert") as alert:
            alert.return_value = True
            run_provider_health_check(fresh_db, **kwargs)
            run_provider_health_check(fresh_db, **kwargs)   # within cooldown

        assert alert.call_count == 1
        rows = (
            fresh_db.query(EnrichmentAnomalyLog)
            .filter(EnrichmentAnomalyLog.provider == vendor)
            .all()
        )
        assert len(rows) == 1


def _hit_with_owner(db, vendor, score, when, parcel, success=True):
    """Seed a property+owner (with a quality score) and a paid hit for it."""
    prop = Property(parcel_id=parcel)
    db.add(prop)
    db.flush()
    db.add(Owner(property_id=prop.id, contact_info_confidence_score=score))
    log = _log(vendor, success, when)
    log.property_id = prop.id
    db.add(log)
    db.flush()
    return prop.id


class TestApplyDegradedDiscount:
    def test_haircuts_only_window_paid_hits_with_a_score(self, fresh_db):
        vendor = "test_a4_discount"
        inside = datetime.now(timezone.utc) - timedelta(hours=1)
        outside = datetime.now(timezone.utc) - timedelta(hours=72)

        p1 = _hit_with_owner(fresh_db, vendor, 0.800, inside, "A4-DISC-1")          # discounted
        _hit_with_owner(fresh_db, vendor, 0.600, inside, "A4-DISC-2")               # discounted
        p3 = _hit_with_owner(fresh_db, vendor, 0.900, outside, "A4-DISC-3")         # too old → untouched
        _hit_with_owner(fresh_db, vendor, None, inside, "A4-DISC-4")               # no score → untouched
        _hit_with_owner(fresh_db, vendor, 0.700, inside, "A4-DISC-5", success=False)  # a miss → untouched

        n = apply_degraded_discount(fresh_db, vendor, hours=48, multiplier=0.5)
        assert n == 2

        def score(pid):
            return fresh_db.query(Owner).filter(Owner.property_id == pid).one().contact_info_confidence_score

        assert float(score(p1)) == pytest.approx(0.400)   # 0.800 * 0.5
        assert float(score(p3)) == pytest.approx(0.900)   # out of window, unchanged

    def test_discount_is_idempotent_across_runs(self, fresh_db):
        # A multi-day degradation re-runs the discount after each cooldown — the
        # same owner must NOT be re-discounted (no ratcheting toward zero).
        vendor = "test_a4_idem"
        when = datetime.now(timezone.utc) - timedelta(hours=1)
        pid = _hit_with_owner(fresh_db, vendor, 0.800, when, "A4-IDEM-1")

        first = apply_degraded_discount(fresh_db, vendor, hours=48, multiplier=0.5)
        second = apply_degraded_discount(fresh_db, vendor, hours=48, multiplier=0.5)

        assert first == 1
        assert second == 0   # already discounted → nothing to do on the re-run
        score = fresh_db.query(Owner).filter(Owner.property_id == pid).one().contact_info_confidence_score
        assert float(score) == pytest.approx(0.400)   # 0.800 × 0.5 once, NOT 0.20


# ── Admin endpoint: GET /api/admin/enrichment-health ────────────────────────

@pytest.fixture
def client():
    from src.api.main import app
    return TestClient(app)


@pytest.fixture
def admin_headers(monkeypatch):
    from config.settings import settings
    from pydantic import SecretStr
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))
    from src.api.admin_router import create_access_token
    return {"Authorization": f"Bearer {create_access_token({'sub': 'admin'})}"}


class TestEnrichmentHealthEndpoint:
    def test_no_token_rejected(self, client):
        r = client.get("/api/admin/enrichment-health")
        assert r.status_code in (401, 403)

    def test_shape_reports_degraded_and_anomalies(self, client, admin_headers, monkeypatch):
        from src.api.main import app
        from src.api.deps import get_db

        # Mock DB: the recent-anomalies query → one row.
        sess = MagicMock()
        sess.execute.return_value.mappings.return_value.all.return_value = [
            {"provider": "tracerfy", "detected_at": datetime(2026, 6, 26, 9, 12, tzinfo=timezone.utc),
             "observed_hit_rate": 0.10, "floor_hit_rate": 0.20, "records_affected": 18},
        ]
        app.dependency_overrides[get_db] = lambda: sess

        # Single provider with a degraded rate (recent_hit_rate stubbed).
        from config.settings import settings
        monkeypatch.setattr(settings, "enrichment_provider_floors", {"tracerfy": 0.20})
        with patch("src.tasks.match_rate_monitor.recent_hit_rate", return_value=(0.10, 140)):
            try:
                r = client.get("/api/admin/enrichment-health", headers=admin_headers)
            finally:
                app.dependency_overrides.pop(get_db, None)

        assert r.status_code == 200
        body = r.json()
        prov = {p["provider"]: p for p in body["providers"]}
        assert prov["tracerfy"]["degraded"] is True
        assert body["recent_anomalies"][0]["provider"] == "tracerfy"
        assert body["recent_anomalies"][0]["records_affected"] == 18
