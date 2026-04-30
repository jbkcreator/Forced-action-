"""
Bundle dispatcher — Stage 5 — unit tests.

Run:
    pytest tests/test_bundle_dispatcher.py -v
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


class TestBundleScheduleGate:
    def test_weekend_friday_8pm_utc_dispatches(self):
        from src.tasks.bundle_dispatcher import _should_dispatch
        # Fri (weekday=4) 20:00 UTC
        now = datetime(2026, 4, 17, 20, 0, tzinfo=timezone.utc)
        assert _should_dispatch("weekend", now) is True

    def test_weekend_friday_3pm_utc_does_not_dispatch(self):
        from src.tasks.bundle_dispatcher import _should_dispatch
        now = datetime(2026, 4, 17, 15, 0, tzinfo=timezone.utc)
        assert _should_dispatch("weekend", now) is False

    def test_weekend_saturday_dispatches(self):
        from src.tasks.bundle_dispatcher import _should_dispatch
        now = datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc)  # Saturday
        assert _should_dispatch("weekend", now) is True

    def test_weekend_sunday_dispatches(self):
        from src.tasks.bundle_dispatcher import _should_dispatch
        now = datetime(2026, 4, 19, 23, 0, tzinfo=timezone.utc)  # Sunday
        assert _should_dispatch("weekend", now) is True

    def test_weekend_monday_does_not_dispatch(self):
        from src.tasks.bundle_dispatcher import _should_dispatch
        now = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)  # Monday
        assert _should_dispatch("weekend", now) is False

    def test_zip_booster_hourly_always_dispatches(self):
        from src.tasks.bundle_dispatcher import _should_dispatch
        now = datetime(2026, 4, 14, 3, 30, tzinfo=timezone.utc)
        assert _should_dispatch("zip_booster", now) is True

    def test_storm_dispatches_when_alert_window_check_runs(self):
        # The schedule gate is a coarse check; storm-active filter happens in
        # `_candidates_storm`. Schedule gate alone should not block.
        from src.tasks.bundle_dispatcher import _should_dispatch
        now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
        assert _should_dispatch("storm", now) is True


class TestPricingGuardrail:
    def test_within_band(self):
        from config.revenue_ladder import bundle_pricing_within_guardrail
        # Weekend base = $19 (1900 cents) → ±25% = [1425, 2375]
        assert bundle_pricing_within_guardrail("weekend", 1900) is True
        assert bundle_pricing_within_guardrail("weekend", 2200) is True
        assert bundle_pricing_within_guardrail("weekend", 1500) is True

    def test_outside_band_rejected(self):
        from config.revenue_ladder import bundle_pricing_within_guardrail
        assert bundle_pricing_within_guardrail("weekend", 1400) is False  # below
        assert bundle_pricing_within_guardrail("weekend", 2400) is False  # above
        assert bundle_pricing_within_guardrail("weekend", 1000) is False
        assert bundle_pricing_within_guardrail("weekend", 5000) is False

    def test_unknown_bundle_rejected(self):
        from config.revenue_ladder import bundle_pricing_within_guardrail
        assert bundle_pricing_within_guardrail("not_a_bundle", 1900) is False


class TestCooldownCheck:
    def test_cooldown_skips_recent(self):
        from src.tasks.bundle_dispatcher import _in_cooldown
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = 42  # recent row exists
        assert _in_cooldown(subscriber_id=1, bundle_type="weekend", hours=168, db=db) is True

    def test_no_cooldown_allows_dispatch(self):
        from src.tasks.bundle_dispatcher import _in_cooldown
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        assert _in_cooldown(subscriber_id=1, bundle_type="weekend", hours=168, db=db) is False


class TestRunSmoke:
    def test_run_returns_stats_dict(self):
        """Smoke test — run() should always return the stats dict shape, even if
        all candidate selectors short-circuit on empty data."""
        from src.tasks import bundle_dispatcher
        # We patch only the audience selectors so no DB session is needed.
        with patch.object(bundle_dispatcher, "_candidates_weekend", return_value=[]), \
             patch.object(bundle_dispatcher, "_candidates_storm", return_value=[]), \
             patch.object(bundle_dispatcher, "_candidates_zip_booster", return_value=[]), \
             patch.object(bundle_dispatcher, "_candidates_monthly_reload", return_value=[]):
            stats = bundle_dispatcher.run(dry_run=True)
        assert "checked" in stats
        assert "dispatched" in stats
        assert "skipped_cooldown" in stats
        assert "skipped_schedule" in stats
        assert stats["dispatched"] == 0
