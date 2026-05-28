"""
Vendor cost monitor service tests.

Unit tests: pure helpers (no DB).
Integration: fresh_db for DB-backed orchestration.

Run:
    pytest tests/test_vendor_cost_monitor.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.services.vendor_cost_monitor import (
    compute_baseline,
    detect_anomaly,
    run_daily_monitor,
)


# ============================================================================
# compute_baseline
# ============================================================================


class TestComputeBaseline:
    def test_empty_list(self):
        r = compute_baseline([])
        assert r["avg"] is None
        assert r["non_zero_days"] == 0

    def test_all_zeros(self):
        r = compute_baseline([0.0, 0.0, 0.0])
        assert r["avg"] is None
        assert r["non_zero_days"] == 0
        assert r["total_days"] == 3

    def test_single_nonzero(self):
        r = compute_baseline([5.0])
        assert r["avg"] == pytest.approx(5.0)
        assert r["stddev"] == pytest.approx(0.0)
        assert r["non_zero_days"] == 1

    def test_multiple_nonzero(self):
        r = compute_baseline([2.0, 4.0, 6.0])
        assert r["avg"] == pytest.approx(4.0)
        assert r["non_zero_days"] == 3

    def test_mixed_zero_nonzero(self):
        r = compute_baseline([0.0, 2.0, 0.0, 4.0])
        assert r["avg"] == pytest.approx(3.0)
        assert r["non_zero_days"] == 2
        assert r["total_days"] == 4

    def test_stddev_correct(self):
        # values [2, 4] → avg=3, variance=((2-3)^2+(4-3)^2)/1=2 → stddev≈1.414
        r = compute_baseline([2.0, 4.0])
        assert r["stddev"] == pytest.approx(1.4142135, rel=1e-4)


# ============================================================================
# detect_anomaly
# ============================================================================


class TestDetectAnomaly:
    def _baseline(self, avg, stddev, non_zero=10, total=14):
        return {"avg": avg, "stddev": stddev, "non_zero_days": non_zero, "total_days": total}

    def test_normal_cost_not_anomaly(self):
        b = self._baseline(avg=5.0, stddev=1.0)
        r = detect_anomaly(6.0, b, "ap_lite_sweep", "claude")
        assert r["is_anomaly"] is False

    def test_cost_above_two_sigma_is_anomaly(self):
        b = self._baseline(avg=5.0, stddev=1.0)
        # threshold = 5 + 2*1 = 7; today = 8
        r = detect_anomaly(8.0, b, "ap_lite_sweep", "claude")
        assert r["is_anomaly"] is True
        assert r["threshold_usd"] == pytest.approx(7.0)
        assert r["anomaly_score"] == pytest.approx(3.0)
        assert r["used_hard_cap"] is False

    def test_exactly_at_threshold_not_anomaly(self):
        b = self._baseline(avg=5.0, stddev=1.0)
        r = detect_anomaly(7.0, b, "ap_lite_sweep", "claude")
        assert r["is_anomaly"] is False

    def test_sparse_history_uses_hard_cap(self):
        b = {"avg": None, "stddev": None, "non_zero_days": 2, "total_days": 14}
        # ap_lite_sweep hard cap = 10.0
        r = detect_anomaly(12.0, b, "ap_lite_sweep", "claude")
        assert r["is_anomaly"] is True
        assert r["threshold_usd"] == pytest.approx(10.0)
        assert r["used_hard_cap"] is True

    def test_sparse_history_under_hard_cap_not_anomaly(self):
        b = {"avg": None, "stddev": None, "non_zero_days": 2, "total_days": 14}
        r = detect_anomaly(5.0, b, "ap_lite_sweep", "claude")
        assert r["is_anomaly"] is False
        assert r["used_hard_cap"] is True

    def test_unknown_target_uses_default_hard_cap(self):
        b = {"avg": None, "stddev": None, "non_zero_days": 0, "total_days": 14}
        # default hard cap = 20.0
        r = detect_anomaly(25.0, b, "unknown_target_xyz", "claude")
        assert r["is_anomaly"] is True
        assert r["threshold_usd"] == pytest.approx(20.0)

    def test_zero_stddev_with_spike(self):
        # All history was identical → stddev=0. Any deviation = anomaly.
        b = self._baseline(avg=3.0, stddev=0.0)
        r = detect_anomaly(5.0, b, "ap_lite_sweep", "claude")
        assert r["is_anomaly"] is True


# ============================================================================
# run_daily_monitor (DB-backed)
# ============================================================================


class TestRunDailyMonitor:
    """Use real Postgres via fresh_db fixture."""

    def test_dry_run_returns_no_pauses_created(self, fresh_db):
        from src.core.models import ApiUsageLog
        # Seed a big spike for ap_lite_sweep
        fresh_db.add(ApiUsageLog(
            service="claude",
            task_type="ap_lite_sweep",
            pause_target="ap_lite_sweep",
            cost_usd=50.0,
            blocked_by_pause=False,
        ))
        fresh_db.flush()

        result = run_daily_monitor(fresh_db, dry_run=True)
        assert result["dry_run"] is True
        assert result["pauses_created"] == 0  # dry_run never writes

    def test_spike_above_hard_cap_creates_pause(self, fresh_db):
        from src.core.models import ApiUsageLog, VendorCostPause
        from sqlalchemy import select

        # graph_name="ap_lite_close" resolves to pause_target="ap_lite_sweep" via attribution.
        # ap_lite_sweep hard cap = $10; spike $15 triggers pause.
        fresh_db.add(ApiUsageLog(
            service="claude",
            graph_name="ap_lite_close",
            task_type="sms_copy",
            pause_target="ap_lite_sweep",
            cost_usd=15.0,
            blocked_by_pause=False,
        ))
        fresh_db.flush()

        # Flush pause cache so we get a clean check
        from src.services.vendor_cost_pause_service import invalidate_cache
        invalidate_cache("claude", "ap_lite_sweep")

        result = run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        pauses = fresh_db.execute(
            select(VendorCostPause).where(
                VendorCostPause.vendor == "claude",
                VendorCostPause.pause_target == "ap_lite_sweep",
                VendorCostPause.status == "active",
            )
        ).scalars().all()
        assert len(pauses) == 1
        assert result["pauses_created"] >= 1

    def test_existing_active_pause_gets_extended(self, fresh_db):
        from datetime import timezone as tz
        from src.core.models import ApiUsageLog, VendorCostPause
        from src.services.vendor_cost_pause_service import invalidate_cache
        from sqlalchemy import select

        # Pre-create an active pause
        now = datetime.now(tz.utc)
        pause = VendorCostPause(
            vendor="claude",
            pause_target="ap_lite_sweep",
            reason="pre-existing",
            status="active",
            paused_at=now,
            auto_resume_at=now + timedelta(hours=24),
            created_by="test",
            metadata_json={},
        )
        fresh_db.add(pause)
        fresh_db.flush()
        invalidate_cache("claude", "ap_lite_sweep")

        # Add spike above hard cap — use graph_name for proper attribution
        fresh_db.add(ApiUsageLog(
            service="claude",
            graph_name="ap_lite_close",
            task_type="sms_copy",
            pause_target="ap_lite_sweep",
            cost_usd=15.0,
            blocked_by_pause=False,
        ))
        fresh_db.flush()

        result = run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        # The pre-existing "claude" pause must be extended
        assert result["pauses_extended"] >= 1

    def test_stripe_spend_never_creates_pause(self, fresh_db):
        from src.core.models import ApiUsageLog, VendorCostPause
        from sqlalchemy import select

        # Stripe way above any threshold
        fresh_db.add(ApiUsageLog(
            service="stripe",
            task_type="daily_fee_summary",
            cost_usd=999.0,
            blocked_by_pause=False,
        ))
        fresh_db.flush()

        result = run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        # No pauses for stripe
        stripe_pauses = fresh_db.execute(
            select(VendorCostPause).where(VendorCostPause.vendor == "stripe")
        ).scalars().all()
        assert stripe_pauses == []
        # Stripe appears in alert_only, not in anomalies that cause pausing
        assert result["pauses_created"] == 0 or all(
            a["pause_target"] != "daily_fee_summary"
            for a in result.get("anomalies", [])
            if a.get("vendor") == "stripe"
        )

    def test_zero_spend_excluded_from_anomalies(self, fresh_db):
        result = run_daily_monitor(fresh_db, dry_run=False)
        assert result["pauses_created"] == 0
        assert result["errors"] == []
