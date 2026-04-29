"""
Revenue Pulse task tests — Items 9 + 18.

Unit tests: mock DB; no Twilio calls.

Run:
    pytest tests/test_revenue_pulse.py -v
"""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from config.revenue_pulse import (
    DAILY_PULSE_TEMPLATE,
    KILL_SWITCH_LEVELS,
    WEEKLY_PULSE_TEMPLATE,
)
from src.tasks.revenue_pulse import (
    _compose_daily,
    _compose_weekly,
    _kill_switch_status,
)


# ============================================================================
# Helpers
# ============================================================================


def _make_db(
    lead_count=5,
    wallet_active=3,
    top_deal=None,
    latest_card=None,
    avg_score=70.0,
    total_subs=10,
    churned_subs=0,
    new_subs_week=2,
):
    db = MagicMock()
    call_count = [0]

    # Build ordered return values for sequential db.execute() calls
    results = [lead_count, wallet_active, top_deal, latest_card]

    def side_effect_daily(stmt):
        idx = call_count[0]
        call_count[0] += 1
        result = MagicMock()
        val = results[idx] if idx < len(results) else None
        result.scalar_one_or_none.return_value = val
        return result

    db.execute.side_effect = side_effect_daily
    return db


# ============================================================================
# KILL_SWITCH_LEVELS config
# ============================================================================


class TestKillSwitchLevelsConfig:
    def test_has_green_yellow_red(self):
        statuses = [l["status"] for l in KILL_SWITCH_LEVELS]
        assert "GREEN" in statuses
        assert "YELLOW" in statuses

    def test_green_requires_high_score_and_low_churn(self):
        green = next(l for l in KILL_SWITCH_LEVELS if l["status"] == "GREEN")
        assert green["min_avg_revenue_score"] >= 50
        assert green["max_churn_rate_pct"] <= 10

    def test_all_levels_have_required_keys(self):
        for level in KILL_SWITCH_LEVELS:
            assert "status" in level
            assert "label" in level
            assert "min_avg_revenue_score" in level
            assert "max_churn_rate_pct" in level


# ============================================================================
# PULSE templates
# ============================================================================


class TestPulseTemplates:
    def test_daily_template_has_placeholders(self):
        for ph in ("{date}", "{lead_count}", "{wallet_active}", "{top_deal}", "{alert}", "{kill_switch}"):
            assert ph in DAILY_PULSE_TEMPLATE

    def test_weekly_template_has_placeholders(self):
        for ph in ("{week}", "{revenue}", "{new_subs}", "{churned}", "{kill_switch}", "{kill_label}", "{learning}"):
            assert ph in WEEKLY_PULSE_TEMPLATE

    def test_daily_template_renders(self):
        msg = DAILY_PULSE_TEMPLATE.format(
            date="4/22", lead_count=5, wallet_active=3,
            top_deal="$12,000", alert="test alert", kill_switch="GREEN",
        )
        assert "4/22" in msg
        assert "GREEN" in msg

    def test_weekly_template_renders(self):
        msg = WEEKLY_PULSE_TEMPLATE.format(
            week="16", revenue="8,000", new_subs=2, churned=0,
            kill_switch="GREEN", kill_label="healthy", learning="test learning",
        )
        assert "8,000" in msg
        assert "GREEN" in msg


# ============================================================================
# _kill_switch_status()
# ============================================================================


class TestKillSwitchStatusUnit:
    def _make_kill_db(self, avg_score, total, churned):
        db = MagicMock()
        results = [avg_score, total, churned]
        call_count = [0]

        def side_effect(stmt):
            idx = call_count[0]
            call_count[0] += 1
            result = MagicMock()
            result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
            return result

        db.execute.side_effect = side_effect
        return db

    def test_green_when_high_score_low_churn(self):
        db = self._make_kill_db(avg_score=75.0, total=10, churned=0)
        status = _kill_switch_status(db)
        assert status["status"] == "GREEN"

    def test_red_when_low_score_high_churn(self):
        db = self._make_kill_db(avg_score=20.0, total=10, churned=8)
        status = _kill_switch_status(db)
        assert status["status"] == "RED"

    def test_returns_dict_with_status_and_label(self):
        db = self._make_kill_db(avg_score=50.0, total=10, churned=1)
        result = _kill_switch_status(db)
        assert "status" in result
        assert "label" in result

    def test_zero_total_no_divide_by_zero(self):
        db = self._make_kill_db(avg_score=0.0, total=0, churned=0)
        result = _kill_switch_status(db)
        assert result["status"] in ("GREEN", "YELLOW", "RED")


# ============================================================================
# _compose_daily()
# ============================================================================


class TestComposeDailyUnit:
    def _make_db_daily(self, lead_count=5, wallet_active=3, top_deal=None, latest_card=None):
        db = MagicMock()
        results = [lead_count, wallet_active, top_deal, latest_card]
        call_count = [0]

        def side_effect(stmt):
            idx = call_count[0]
            call_count[0] += 1
            result = MagicMock()
            result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
            return result

        db.execute.side_effect = side_effect
        return db

    def _make_kill_db_for_compose(self, avg_score=70.0, total=10, churned=1):
        """compose_daily calls _kill_switch_status which needs 3 more queries at end."""
        db = MagicMock()
        results = [5, 3, None, None, avg_score, total, churned]  # leads, wallets, deal, card, kill params
        call_count = [0]

        def side_effect(stmt):
            idx = call_count[0]
            call_count[0] += 1
            result = MagicMock()
            result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
            return result

        db.execute.side_effect = side_effect
        return db

    def test_returns_string(self):
        db = self._make_kill_db_for_compose()
        msg = _compose_daily(db)
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_contains_lead_count(self):
        db = MagicMock()
        results = [7, 3, None, None, 70.0, 10, 1]
        call_count = [0]

        def side_effect(stmt):
            idx = call_count[0]
            call_count[0] += 1
            result = MagicMock()
            result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
            return result

        db.execute.side_effect = side_effect
        msg = _compose_daily(db)
        assert "7" in msg

    def test_no_deals_fallback_text(self):
        db = MagicMock()
        results = [5, 2, None, None, 60.0, 8, 0]
        call_count = [0]

        def side_effect(stmt):
            idx = call_count[0]
            call_count[0] += 1
            result = MagicMock()
            result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
            return result

        db.execute.side_effect = side_effect
        msg = _compose_daily(db)
        assert "no deals" in msg.lower()


# ============================================================================
# run_daily_pulse / run_weekly_pulse (dry-run mode)
# ============================================================================


class TestRunPulseDryRunUnit:
    def test_daily_dry_run_does_not_send_sms(self):
        with patch("src.tasks.revenue_pulse.get_db_context") as mock_ctx, \
             patch("src.tasks.revenue_pulse._send_sms") as mock_sms, \
             patch("src.tasks.revenue_pulse.settings") as mock_settings:
            mock_settings.founder_phone = "+18135550100"
            db = MagicMock()
            mock_ctx.return_value.__enter__.return_value = db
            results = [5, 3, None, None, 70.0, 10, 1]
            call_count = [0]

            def side_effect(stmt):
                idx = call_count[0]
                call_count[0] += 1
                result = MagicMock()
                result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
                return result

            db.execute.side_effect = side_effect

            from src.tasks.revenue_pulse import run_daily_pulse
            result = run_daily_pulse(dry_run=True)

        assert result["sent"] is False
        assert result["dry_run"] is True
        mock_sms.assert_not_called()

    def test_weekly_dry_run_returns_message(self):
        with patch("src.tasks.revenue_pulse.get_db_context") as mock_ctx, \
             patch("src.tasks.revenue_pulse._send_sms") as mock_sms, \
             patch("src.tasks.revenue_pulse.settings") as mock_settings:
            mock_settings.founder_phone = "+18135550100"
            db = MagicMock()
            mock_ctx.return_value.__enter__.return_value = db

            results = [2, 0, 10, 70.0, 10, 1, None]
            call_count = [0]

            def side_effect(stmt):
                idx = call_count[0]
                call_count[0] += 1
                result = MagicMock()
                result.scalar_one_or_none.return_value = results[idx] if idx < len(results) else None
                return result

            db.execute.side_effect = side_effect

            from src.tasks.revenue_pulse import run_weekly_pulse
            result = run_weekly_pulse(dry_run=True)

        assert result["sent"] is False
        assert "message" in result
        mock_sms.assert_not_called()
