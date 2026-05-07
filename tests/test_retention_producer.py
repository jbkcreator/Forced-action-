"""
Unit tests for retention event producer.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta


class TestIsDeduped:
    def test_redis_down_not_deduped(self):
        from src.tasks.retention_event_producer import _is_deduplicated
        with patch("src.tasks.retention_event_producer.redis_available", return_value=False):
            assert _is_deduplicated(1) is False

    def test_key_present_is_deduped(self):
        from src.tasks.retention_event_producer import _is_deduplicated
        with (
            patch("src.tasks.retention_event_producer.redis_available", return_value=True),
            patch("src.tasks.retention_event_producer.rget", return_value="1"),
        ):
            assert _is_deduplicated(1) is True

    def test_key_absent_not_deduped(self):
        from src.tasks.retention_event_producer import _is_deduplicated
        with (
            patch("src.tasks.retention_event_producer.redis_available", return_value=True),
            patch("src.tasks.retention_event_producer.rget", return_value=None),
        ):
            assert _is_deduplicated(1) is False


class TestRetentionRun:
    def _make_sub(self, sub_id, tier, created_at=None):
        sub = MagicMock()
        sub.id = sub_id
        sub.tier = tier
        sub.status = "active"
        sub.created_at = created_at or datetime(2025, 1, 1, tzinfo=timezone.utc)
        return sub

    def test_dry_run_no_events(self):
        from src.tasks.retention_event_producer import run
        from config.retention import RETENTION_CADENCE_DAYS

        tier = next(iter(RETENTION_CADENCE_DAYS))
        days = RETENTION_CADENCE_DAYS[tier]
        sub = self._make_sub(1, tier, datetime(2024, 1, 1, tzinfo=timezone.utc))

        with (
            patch("src.tasks.retention_event_producer.get_db_context") as mock_ctx,
            patch("src.tasks.retention_event_producer._is_deduplicated", return_value=False),
            patch("src.tasks.retention_event_producer._last_engagement", return_value=None),
            patch("src.tasks.retention_event_producer._emit_event") as mock_emit,
        ):
            db_mock = MagicMock()
            db_mock.execute.return_value.scalars.return_value.all.return_value = [sub]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db_mock)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run(dry_run=True)

        mock_emit.assert_not_called()
        assert results["events_emitted"] == 0
        assert results["inactive_found"] >= 1

    def test_active_subscriber_skipped(self):
        from src.tasks.retention_event_producer import run
        from config.retention import RETENTION_CADENCE_DAYS

        tier = next(iter(RETENTION_CADENCE_DAYS))
        sub = self._make_sub(2, tier)
        # recent engagement — within cadence window
        recent = datetime.now(timezone.utc) - timedelta(days=1)

        with (
            patch("src.tasks.retention_event_producer.get_db_context") as mock_ctx,
            patch("src.tasks.retention_event_producer._is_deduplicated", return_value=False),
            patch("src.tasks.retention_event_producer._last_engagement", return_value=recent),
            patch("src.tasks.retention_event_producer._emit_event") as mock_emit,
        ):
            db_mock = MagicMock()
            db_mock.execute.return_value.scalars.return_value.all.return_value = [sub]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db_mock)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run(dry_run=False)

        mock_emit.assert_not_called()
        assert results["inactive_found"] == 0

    def test_emits_for_inactive(self):
        from src.tasks.retention_event_producer import run
        from config.retention import RETENTION_CADENCE_DAYS

        tier = next(iter(RETENTION_CADENCE_DAYS))
        days = RETENTION_CADENCE_DAYS[tier]
        sub = self._make_sub(3, tier)
        old_engagement = datetime.now(timezone.utc) - timedelta(days=days + 2)

        with (
            patch.dict(
                "src.tasks.retention_event_producer.RETENTION_CADENCE_DAYS",
                {tier: days}, clear=True,
            ),
            patch("src.tasks.retention_event_producer.get_db_context") as mock_ctx,
            patch("src.tasks.retention_event_producer._is_deduplicated", return_value=False),
            patch("src.tasks.retention_event_producer._last_engagement", return_value=old_engagement),
            patch("src.tasks.retention_event_producer._emit_event") as mock_emit,
            patch("src.tasks.retention_event_producer._mark_deduplicated"),
        ):
            db_mock = MagicMock()
            db_mock.execute.return_value.scalars.return_value.all.return_value = [sub]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db_mock)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run(dry_run=False)

        mock_emit.assert_called_once()
        assert results["events_emitted"] == 1

    def test_dedup_skips(self):
        from src.tasks.retention_event_producer import run
        from config.retention import RETENTION_CADENCE_DAYS

        tier = next(iter(RETENTION_CADENCE_DAYS))
        days = RETENTION_CADENCE_DAYS[tier]
        sub = self._make_sub(4, tier)

        with (
            patch.dict(
                "src.tasks.retention_event_producer.RETENTION_CADENCE_DAYS",
                {tier: days}, clear=True,
            ),
            patch("src.tasks.retention_event_producer.get_db_context") as mock_ctx,
            patch("src.tasks.retention_event_producer._is_deduplicated", return_value=True),
            patch("src.tasks.retention_event_producer._emit_event") as mock_emit,
        ):
            db_mock = MagicMock()
            db_mock.execute.return_value.scalars.return_value.all.return_value = [sub]
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db_mock)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run(dry_run=False)

        mock_emit.assert_not_called()
        assert results["deduped"] == 1
