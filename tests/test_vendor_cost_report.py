"""
Vendor cost report service tests.

Unit tests: build_vendor_cost_summary, format_sms_cost_summary, format_html_vendor_cost_report.
Integration: fresh_db for DB-backed summary building.

Run:
    pytest tests/test_vendor_cost_report.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from src.services.vendor_cost_report import (
    build_vendor_cost_summary,
    format_html_vendor_cost_report,
    format_sms_cost_summary,
)


# ============================================================================
# format_sms_cost_summary (pure, no DB)
# ============================================================================


class TestFormatSmsCostSummary:
    def _summary(self, claude=0.0, telnyx=0.0, stripe=0.0, pauses=None):
        return {
            "vendor_totals": {"claude": claude, "telnyx": telnyx, "stripe": stripe},
            "active_pauses": pauses or [],
        }

    def test_returns_none_when_all_zero_no_pauses(self):
        s = self._summary()
        assert format_sms_cost_summary(s) is None

    def test_includes_total(self):
        s = self._summary(claude=5.0, telnyx=2.0)
        result = format_sms_cost_summary(s)
        assert "$7.00 vendor" in result

    def test_includes_claude_line(self):
        s = self._summary(claude=5.0)
        result = format_sms_cost_summary(s)
        assert "Claude=$5.00" in result

    def test_includes_telnyx_line(self):
        s = self._summary(telnyx=3.0)
        result = format_sms_cost_summary(s)
        assert "Telnyx=$3.00" in result

    def test_zero_vendors_omitted(self):
        s = self._summary(claude=5.0, telnyx=0.0)
        result = format_sms_cost_summary(s)
        assert "Telnyx" not in result

    def test_single_pause_shows_target_and_skipped(self):
        s = self._summary(
            claude=10.0,
            pauses=[{"pause_target": "ap_lite_sweep", "skipped_actions": 7, "severity": 3.0}],
        )
        result = format_sms_cost_summary(s)
        assert "pause:ap_lite_sweep" in result
        assert "skipped:7" in result

    def test_multiple_pauses_shows_plus_n_more(self):
        s = self._summary(
            claude=10.0,
            pauses=[
                {"pause_target": "ap_lite_sweep", "skipped_actions": 7, "severity": 3.0},
                {"pause_target": "bundle_dispatcher", "skipped_actions": 2, "severity": 1.5},
                {"pause_target": "nws_poll", "skipped_actions": 0, "severity": 1.0},
            ],
        )
        result = format_sms_cost_summary(s)
        assert "+2 more" in result

    def test_exactly_two_pauses_shows_plus_1_more(self):
        s = self._summary(
            claude=10.0,
            pauses=[
                {"pause_target": "ap_lite_sweep", "skipped_actions": 7, "severity": 3.0},
                {"pause_target": "bundle_dispatcher", "skipped_actions": 2, "severity": 1.5},
            ],
        )
        result = format_sms_cost_summary(s)
        assert "+1 more" in result


# ============================================================================
# format_html_vendor_cost_report (pure, no DB)
# ============================================================================


class TestFormatHtmlVendorCostReport:
    def _summary(self, claude=10.0, telnyx=5.0, stripe=2.0, pauses=None):
        return {
            "vendor_totals": {"claude": claude, "telnyx": telnyx, "stripe": stripe},
            "active_pauses": pauses or [],
            "digital_ocean": "pending",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def test_returns_html_string(self):
        html = format_html_vendor_cost_report(self._summary())
        assert isinstance(html, str)
        assert "<div" in html

    def test_shows_total(self):
        html = format_html_vendor_cost_report(self._summary(claude=10.0, telnyx=5.0, stripe=2.0))
        assert "$17.00 total" in html

    def test_shows_vendor_rows(self):
        html = format_html_vendor_cost_report(self._summary())
        assert "Claude/Anthropic" in html
        assert "Telnyx" in html
        assert "Stripe" in html

    def test_no_active_pauses_shows_fallback(self):
        html = format_html_vendor_cost_report(self._summary(pauses=[]))
        assert "No active pauses" in html

    def test_pause_row_rendered(self):
        pause = {
            "vendor": "claude",
            "pause_target": "ap_lite_sweep",
            "cost_usd": 15.0,
            "threshold_usd": 10.0,
            "skipped_actions": 4,
            "severity": 2.5,
            "paused_at": datetime.now(timezone.utc).isoformat(),
            "auto_resume_at": (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(),
        }
        html = format_html_vendor_cost_report(self._summary(pauses=[pause]))
        assert "ap_lite_sweep" in html
        assert "$15.00" in html
        assert "No active pauses" not in html

    def test_digital_ocean_pending_shown(self):
        html = format_html_vendor_cost_report(self._summary())
        assert "pending" in html


# ============================================================================
# build_vendor_cost_summary (DB-backed)
# ============================================================================


class TestBuildVendorCostSummary:
    def test_empty_db_returns_zero_totals(self, fresh_db):
        summary = build_vendor_cost_summary(fresh_db)
        assert summary["vendor_totals"] == {} or all(
            v == 0.0 for v in summary["vendor_totals"].values()
        )
        assert summary["active_pauses"] == []

    def test_aggregates_claude_spend(self, fresh_db):
        from src.core.models import ApiUsageLog

        fresh_db.add(ApiUsageLog(
            service="claude",
            task_type="sms_copy",
            cost_usd=3.50,
            blocked_by_pause=False,
        ))
        fresh_db.add(ApiUsageLog(
            service="claude",
            task_type="classification",
            cost_usd=1.25,
            blocked_by_pause=False,
        ))
        fresh_db.flush()

        summary = build_vendor_cost_summary(fresh_db)
        assert summary["vendor_totals"].get("claude", 0) == pytest.approx(4.75)

    def test_stripe_spend_aggregated(self, fresh_db):
        from src.core.models import ApiUsageLog

        fresh_db.add(ApiUsageLog(
            service="stripe",
            task_type="daily_fee_summary",
            cost_usd=0.85,
            blocked_by_pause=False,
        ))
        fresh_db.flush()

        summary = build_vendor_cost_summary(fresh_db)
        assert summary["vendor_totals"].get("stripe", 0) == pytest.approx(0.85)

    def test_blocked_calls_excluded_from_totals(self, fresh_db):
        from src.core.models import ApiUsageLog

        fresh_db.add(ApiUsageLog(
            service="claude",
            task_type="sms_copy",
            cost_usd=5.00,
            blocked_by_pause=True,
        ))
        fresh_db.flush()

        summary = build_vendor_cost_summary(fresh_db)
        assert summary["vendor_totals"].get("claude", 0) == pytest.approx(0.0)

    def test_active_pauses_included(self, fresh_db):
        from src.core.models import VendorCostPause

        now = datetime.now(timezone.utc)
        fresh_db.add(VendorCostPause(
            vendor="claude",
            pause_target="ap_lite_sweep",
            reason="test",
            status="active",
            paused_at=now,
            auto_resume_at=now + timedelta(hours=24),
            created_by="test",
            metadata_json={},
            today_cost_usd=12.0,
            anomaly_score=2.1,
        ))
        fresh_db.flush()

        summary = build_vendor_cost_summary(fresh_db)
        assert len(summary["active_pauses"]) == 1
        assert summary["active_pauses"][0]["pause_target"] == "ap_lite_sweep"

    def test_skipped_actions_counted_in_pause_summary(self, fresh_db):
        from src.core.models import ApiUsageLog, VendorCostPause

        now = datetime.now(timezone.utc)
        fresh_db.add(VendorCostPause(
            vendor="claude",
            pause_target="ap_lite_sweep",
            reason="test",
            status="active",
            paused_at=now - timedelta(minutes=5),
            auto_resume_at=now + timedelta(hours=24),
            created_by="test",
            metadata_json={},
        ))
        fresh_db.flush()

        for _ in range(3):
            fresh_db.add(ApiUsageLog(
                service="claude",
                pause_target="ap_lite_sweep",
                cost_usd=0.0,
                blocked_by_pause=True,
                block_reason="pause: test",
            ))
        fresh_db.flush()

        summary = build_vendor_cost_summary(fresh_db)
        pause_data = summary["active_pauses"][0]
        assert pause_data["skipped_actions"] == 3
