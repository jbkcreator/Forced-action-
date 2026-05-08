"""
Unit tests for AP Lite sweep and manual action logging.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import date


class TestManualActionLogging:
    """wallet_engine.debit() logs manual actions for AP Lite threshold tracking."""

    def test_tracks_correct_action_types(self):
        from config.ap_lite import MANUAL_ACTION_TYPES
        assert "lead_unlock" in MANUAL_ACTION_TYPES
        assert "outbound_text" in MANUAL_ACTION_TYPES
        assert "skip_trace" in MANUAL_ACTION_TYPES
        assert "voicemail" in MANUAL_ACTION_TYPES

    def test_non_tracked_types_excluded(self):
        from config.ap_lite import MANUAL_ACTION_TYPES
        assert "monthly_allotment" not in MANUAL_ACTION_TYPES
        assert "bundle_purchase" not in MANUAL_ACTION_TYPES

    def test_eligible_tiers_contains_annual_lock(self):
        from config.ap_lite import AP_LITE_ELIGIBLE_TIERS
        assert "annual_lock" in AP_LITE_ELIGIBLE_TIERS


class TestCountActionsWeek:
    def test_returns_correct_count(self, mock_db):
        from src.tasks.ap_lite_sweep import count_actions_week
        mock_db.execute.return_value.scalar.return_value = 12
        result = count_actions_week(mock_db, 1, date(2026, 4, 27))
        assert result == 12

    def test_none_count_returns_zero(self, mock_db):
        from src.tasks.ap_lite_sweep import count_actions_week
        mock_db.execute.return_value.scalar.return_value = None
        result = count_actions_week(mock_db, 1, date(2026, 4, 27))
        assert result == 0


class TestApLiteSweep:
    def _make_sub(self, sub_id):
        sub = MagicMock()
        sub.id = sub_id
        sub.tier = "annual_lock"
        sub.status = "active"
        sub.ap_lite_candidate_at = None
        return sub

    def test_dry_run_no_events(self):
        from src.tasks.ap_lite_sweep import run_sweep
        from config.ap_lite import AP_LITE_THRESHOLD_PER_WEEK

        sub = self._make_sub(1)

        with (
            patch("src.tasks.ap_lite_sweep.get_db_context") as mock_ctx,
            patch("src.tasks.ap_lite_sweep.count_actions_week", return_value=AP_LITE_THRESHOLD_PER_WEEK + 5),
            patch("src.tasks.ap_lite_sweep._emit_event") as mock_emit,
        ):
            db_mock = MagicMock()
            db_mock.execute.return_value.scalars.return_value.all.return_value = [sub]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db_mock)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sweep(dry_run=True)

        mock_emit.assert_not_called()
        assert results["events_emitted"] == 0
        assert results["candidates_found"] == 1

    def test_below_threshold_not_emitted(self):
        from src.tasks.ap_lite_sweep import run_sweep
        from config.ap_lite import AP_LITE_THRESHOLD_PER_WEEK

        sub = self._make_sub(2)

        with (
            patch("src.tasks.ap_lite_sweep.get_db_context") as mock_ctx,
            patch("src.tasks.ap_lite_sweep.count_actions_week", return_value=AP_LITE_THRESHOLD_PER_WEEK - 1),
            patch("src.tasks.ap_lite_sweep._emit_event") as mock_emit,
        ):
            db_mock = MagicMock()
            db_mock.execute.return_value.scalars.return_value.all.return_value = [sub]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db_mock)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            run_sweep(dry_run=False)

        mock_emit.assert_not_called()

    def test_above_threshold_emits_event(self):
        from src.tasks.ap_lite_sweep import run_sweep
        from config.ap_lite import AP_LITE_THRESHOLD_PER_WEEK

        sub = self._make_sub(3)

        with (
            patch("src.tasks.ap_lite_sweep.get_db_context") as mock_ctx,
            patch("src.tasks.ap_lite_sweep.count_actions_week", return_value=AP_LITE_THRESHOLD_PER_WEEK + 5),
            patch("src.tasks.ap_lite_sweep._emit_event") as mock_emit,
        ):
            db_mock = MagicMock()
            db_mock.execute.return_value.scalars.return_value.all.return_value = [sub]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db_mock)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sweep(dry_run=False)

        mock_emit.assert_called_once()
        assert results["events_emitted"] == 1
        assert results["candidates_found"] == 1
