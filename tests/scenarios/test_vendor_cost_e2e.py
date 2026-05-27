"""
Vendor Cost Monitor E2E scenario test.

Seeds baseline + spike vendor usage, runs the full monitor cycle, and verifies:
  - Pause created on spike
  - Pause extended on second consecutive spike
  - Blocked calls logged with blocked_by_pause=True
  - Stripe spend aggregated but no pause created (alert-only)
  - Revenue Pulse SMS cost line rendered
  - HTML email cost section rendered

No real Claude/Telnyx/Stripe/email calls.

Marker: scenario_platform
Run:
    pytest tests/scenarios/test_vendor_cost_e2e.py -v -m scenario_platform
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import ApiUsageLog, VendorCostPause
from src.services.vendor_cost_monitor import run_daily_monitor
from src.services.vendor_cost_pause_service import invalidate_cache
from src.services.vendor_cost_report import (
    build_vendor_cost_summary,
    format_html_vendor_cost_report,
    format_sms_cost_summary,
)

pytestmark = pytest.mark.scenario_platform


# graph_name="ap_lite_close" resolves to pause_target "ap_lite_sweep" via attribution.
# We seed graph_name (drives aggregation) AND the pause_target column (drives the
# historical-baseline query, which filters ApiUsageLog.pause_target directly).
_GRAPH_FOR_TARGET = {"ap_lite_sweep": "ap_lite_close"}


def _seed_history(db, pause_target: str, daily_cost: float, days: int = 10):
    """Seed N days of normal-cost ApiUsageLog rows for baseline computation."""
    graph = _GRAPH_FOR_TARGET[pause_target]
    now = datetime.now(timezone.utc)
    for i in range(1, days + 1):
        ts = now - timedelta(days=i, hours=1)
        db.add(ApiUsageLog(
            service="claude",
            graph_name=graph,
            task_type="sms_copy",
            pause_target=pause_target,
            cost_usd=daily_cost,
            blocked_by_pause=False,
            created_at=ts,
        ))
    db.flush()


def _seed_today_spike(db, pause_target: str, cost: float):
    """Seed today's spike cost."""
    graph = _GRAPH_FOR_TARGET[pause_target]
    db.add(ApiUsageLog(
        service="claude",
        graph_name=graph,
        task_type="sms_copy",
        pause_target=pause_target,
        cost_usd=cost,
        blocked_by_pause=False,
    ))
    db.flush()


def _seed_stripe_fees(db, cost: float):
    """Seed Stripe fee log (alert-only)."""
    db.add(ApiUsageLog(
        service="stripe",
        task_type="daily_fee_summary",
        cost_usd=cost,
        blocked_by_pause=False,
    ))
    db.flush()


# ============================================================================
# Full E2E scenario
# ============================================================================


class TestVendorCostMonitorE2E:
    TARGET = "ap_lite_sweep"
    NORMAL_COST = 2.0       # $2/day baseline
    SPIKE_COST = 30.0       # $30 spike — well above hard cap ($10) and 2σ

    def setup_method(self):
        invalidate_cache("claude", self.TARGET)
        invalidate_cache("stripe", "daily_fee_summary")

    def test_full_cycle_creates_pause_on_spike(self, fresh_db):
        """Seed 10 days baseline + today spike → monitor creates active pause."""
        _seed_history(fresh_db, self.TARGET, self.NORMAL_COST, days=10)
        _seed_today_spike(fresh_db, self.TARGET, self.SPIKE_COST)
        invalidate_cache("claude", self.TARGET)

        result = run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        pauses = fresh_db.execute(
            __import__("sqlalchemy").select(VendorCostPause).where(
                VendorCostPause.vendor == "claude",
                VendorCostPause.pause_target == self.TARGET,
                VendorCostPause.status == "active",
            )
        ).scalars().all()

        assert len(pauses) == 1, f"Expected 1 active pause, got {len(pauses)}"
        assert result["pauses_created"] >= 1
        assert result["errors"] == []

    def test_second_spike_extends_existing_pause(self, fresh_db):
        """Active pause from day 1 → day 2 spike extends (not duplicates) it."""
        _seed_history(fresh_db, self.TARGET, self.NORMAL_COST, days=10)
        _seed_today_spike(fresh_db, self.TARGET, self.SPIKE_COST)
        invalidate_cache("claude", self.TARGET)

        # Day 1 run
        run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        # Add another spike (simulate day 2)
        _seed_today_spike(fresh_db, self.TARGET, self.SPIKE_COST)
        invalidate_cache("claude", self.TARGET)

        # Day 2 run
        result2 = run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        # Should extend, not create a second pause
        assert result2["pauses_extended"] >= 1
        assert result2["pauses_created"] == 0

        # Only one active pause row
        from sqlalchemy import select
        pauses = fresh_db.execute(
            select(VendorCostPause).where(
                VendorCostPause.vendor == "claude",
                VendorCostPause.pause_target == self.TARGET,
                VendorCostPause.status == "active",
            )
        ).scalars().all()
        assert len(pauses) == 1

    def test_blocked_call_logged_with_flag(self, fresh_db):
        """After pause is active, call_claude logs blocked_by_pause=True."""
        _seed_history(fresh_db, self.TARGET, self.NORMAL_COST, days=10)
        _seed_today_spike(fresh_db, self.TARGET, self.SPIKE_COST)
        invalidate_cache("claude", self.TARGET)

        run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()
        invalidate_cache("claude", self.TARGET)

        # Now call call_claude — should be blocked
        with patch("src.services.claude_router.settings") as mock_settings, \
             patch("src.services.claude_router.Anthropic") as mock_anthropic:
            mock_settings.anthropic_api_key.get_secret_value.return_value = "sk-test"
            mock_settings.claude_sonnet_model = "claude-sonnet-4-6"
            mock_settings.claude_haiku_model = "claude-haiku-4-5"
            mock_settings.claude_opus_model = "claude-opus-4-7"

            from src.services.claude_router import call_claude
            result_text = call_claude(
                "sms_copy",
                [{"role": "user", "content": "generate SMS"}],
                pause_target=self.TARGET,
                db=fresh_db,
            )
        fresh_db.flush()

        assert "[BLOCKED]" in result_text
        mock_anthropic.return_value.messages.create.assert_not_called()

        from sqlalchemy import select
        blocked_row = fresh_db.execute(
            select(ApiUsageLog).where(
                ApiUsageLog.pause_target == self.TARGET,
                ApiUsageLog.blocked_by_pause == True,  # noqa: E712
            )
        ).scalar_one_or_none()
        assert blocked_row is not None

    def test_stripe_spike_never_creates_pause(self, fresh_db):
        """Stripe fees — no matter how large — must not trigger a pause."""
        _seed_stripe_fees(fresh_db, 500.0)
        invalidate_cache("stripe", "daily_fee_summary")

        result = run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        from sqlalchemy import select
        stripe_pauses = fresh_db.execute(
            select(VendorCostPause).where(VendorCostPause.vendor == "stripe")
        ).scalars().all()
        assert stripe_pauses == []
        assert result["pauses_created"] == 0

    def test_stripe_spend_appears_in_vendor_totals(self, fresh_db):
        """Stripe fees in ApiUsageLog appear in build_vendor_cost_summary totals."""
        _seed_stripe_fees(fresh_db, 1.25)
        summary = build_vendor_cost_summary(fresh_db)
        assert summary["vendor_totals"].get("stripe", 0) == pytest.approx(1.25)

    def test_revenue_pulse_sms_includes_vendor_cost_line(self, fresh_db):
        """format_sms_cost_summary produces non-None string when spend present."""
        _seed_history(fresh_db, self.TARGET, self.NORMAL_COST, days=10)
        _seed_today_spike(fresh_db, self.TARGET, self.SPIKE_COST)
        invalidate_cache("claude", self.TARGET)
        run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        summary = build_vendor_cost_summary(fresh_db)
        sms_line = format_sms_cost_summary(summary)

        assert sms_line is not None
        assert "vendor" in sms_line
        assert "pause" in sms_line

    def test_html_email_report_includes_pause_table(self, fresh_db):
        """HTML report renders active pause row, not 'No active pauses'."""
        _seed_history(fresh_db, self.TARGET, self.NORMAL_COST, days=10)
        _seed_today_spike(fresh_db, self.TARGET, self.SPIKE_COST)
        invalidate_cache("claude", self.TARGET)
        run_daily_monitor(fresh_db, dry_run=False)
        fresh_db.flush()

        summary = build_vendor_cost_summary(fresh_db)
        html = format_html_vendor_cost_report(summary)

        assert self.TARGET in html
        assert "No active pauses" not in html

    def test_dry_run_no_db_writes(self, fresh_db):
        """dry_run=True produces anomaly detection output but no pause rows."""
        _seed_history(fresh_db, self.TARGET, self.NORMAL_COST, days=10)
        _seed_today_spike(fresh_db, self.TARGET, self.SPIKE_COST)
        invalidate_cache("claude", self.TARGET)

        result = run_daily_monitor(fresh_db, dry_run=True)
        fresh_db.flush()

        from sqlalchemy import select
        active_pauses = fresh_db.execute(
            select(VendorCostPause).where(
                VendorCostPause.vendor == "claude",
                VendorCostPause.status == "active",
            )
        ).scalars().all()
        assert active_pauses == [], f"dry_run should not create active pauses, got {active_pauses}"
        assert result["pauses_created"] == 0
        assert result["dry_run"] is True

    def test_auto_resume_expired_pause(self, fresh_db):
        """auto_resume_expired marks past-due pauses as auto_resumed."""
        from src.services.vendor_cost_pause_service import auto_resume_expired

        now = datetime.now(timezone.utc)
        expired = VendorCostPause(
            vendor="claude",
            pause_target="bundle_dispatcher",
            reason="expired test pause",
            status="active",
            paused_at=now - timedelta(hours=26),
            auto_resume_at=now - timedelta(hours=2),  # past
            created_by="test",
            metadata_json={},
        )
        fresh_db.add(expired)
        fresh_db.flush()

        count = auto_resume_expired(fresh_db)
        fresh_db.flush()

        fresh_db.expire(expired)
        assert expired.status == "auto_resumed"
        assert count == 1
