"""
Unit tests for flash scarcity service.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


class TestDetectSpike:
    def test_above_threshold_returns_true(self, mock_db):
        from src.services.flash_scarcity import detect_spike, SPIKE_LEAD_COUNT
        mock_db.execute.return_value.scalar.return_value = SPIKE_LEAD_COUNT
        result = detect_spike(mock_db, "33647", "roofing")
        assert result is True

    def test_below_threshold_returns_false(self, mock_db):
        from src.services.flash_scarcity import detect_spike, SPIKE_LEAD_COUNT
        mock_db.execute.return_value.scalar.return_value = SPIKE_LEAD_COUNT - 1
        result = detect_spike(mock_db, "33647", "roofing")
        assert result is False

    def test_null_count_returns_false(self, mock_db):
        from src.services.flash_scarcity import detect_spike
        mock_db.execute.return_value.scalar.return_value = None
        result = detect_spike(mock_db, "33647", "roofing")
        assert result is False


class TestIsZipLocked:
    def test_locked_zip(self, mock_db):
        from src.services.flash_scarcity import _is_zip_locked
        mock_db.execute.return_value.scalar_one_or_none.return_value = MagicMock()
        assert _is_zip_locked(mock_db, "33647", "roofing") is True

    def test_unlocked_zip(self, mock_db):
        from src.services.flash_scarcity import _is_zip_locked
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        assert _is_zip_locked(mock_db, "33647", "roofing") is False


class TestOpenWindowIfSpike:
    def test_no_zip_returns_false(self, mock_db):
        from src.services.flash_scarcity import open_window_if_spike
        result = open_window_if_spike(mock_db, 1, "", "roofing")
        assert result is False

    def test_no_spike_returns_false(self, mock_db):
        from src.services.flash_scarcity import open_window_if_spike
        with (
            patch("src.core.redis_client.redis_available", return_value=False),
            patch("src.services.flash_scarcity.detect_spike", return_value=False),
        ):
            result = open_window_if_spike(mock_db, 1, "33647", "roofing")
        assert result is False

    def test_locked_zip_skipped(self, mock_db):
        from src.services.flash_scarcity import open_window_if_spike
        with (
            patch("src.core.redis_client.redis_available", return_value=False),
            patch("src.services.flash_scarcity.detect_spike", return_value=True),
            patch("src.services.flash_scarcity._is_zip_locked", return_value=True),
        ):
            result = open_window_if_spike(mock_db, 1, "33647", "roofing")
        assert result is False

    def test_spike_on_unlocked_zip_opens_window(self, mock_db):
        from src.services.flash_scarcity import open_window_if_spike
        mock_window = {"expires_at": "2026-05-06T17:00:00Z", "window_minutes": 60}
        with (
            patch("src.core.redis_client.redis_available", return_value=False),
            patch("src.services.flash_scarcity.detect_spike", return_value=True),
            patch("src.services.flash_scarcity._is_zip_locked", return_value=False),
            patch("src.services.flash_scarcity.create_window", return_value=mock_window),
            patch("src.services.flash_scarcity._emit_event") as mock_emit,
        ):
            result = open_window_if_spike(mock_db, 1, "33647", "roofing")
        assert result is True
        mock_emit.assert_called_once()

    def test_redis_dedup_prevents_double_open(self, mock_db):
        from src.services.flash_scarcity import open_window_if_spike
        with (
            patch("src.core.redis_client.redis_available", return_value=True),
            patch("src.core.redis_client.rget", return_value="1"),
        ):
            result = open_window_if_spike(mock_db, 1, "33647", "roofing")
        assert result is False


class TestEmitEvent:
    def test_emits_one_event_per_eligible_subscriber(self, mock_db):
        from src.services.flash_scarcity import _emit_event
        mock_db.execute.return_value.scalars.return_value.all.return_value = [10, 20]
        with patch("src.agents.supervisor.dispatch_event") as mock_dispatch:
            _emit_event(mock_db, "33647", "roofing", 1, {"expires_at": None, "window_minutes": 60})
        assert mock_dispatch.call_count == 2
        dispatched_subs = [c[0][0]["subscriber_id"] for c in mock_dispatch.call_args_list]
        assert dispatched_subs == [10, 20]

    def test_idempotency_key_includes_subscriber_and_bucket(self, mock_db):
        from src.services.flash_scarcity import _emit_event
        mock_db.execute.return_value.scalars.return_value.all.return_value = [10]
        with patch("src.agents.supervisor.dispatch_event") as mock_dispatch:
            now = datetime.now(timezone.utc)
            _emit_event(mock_db, "33647", "roofing", 1, {"expires_at": None, "window_minutes": 60})
        idem = mock_dispatch.call_args[0][0]["idempotency_key"]
        expected_prefix = f"flashscar:33647:roofing:{now.strftime('%Y%m%d%H')}"
        assert idem.startswith(expected_prefix)
        # suffix is <bucket_digit>:<sub_id>
        suffix = idem[len(expected_prefix):]
        assert suffix.endswith(":10")

    def test_no_subscribers_emits_nothing(self, mock_db):
        from src.services.flash_scarcity import _emit_event
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        with patch("src.agents.supervisor.dispatch_event") as mock_dispatch:
            _emit_event(mock_db, "33647", "roofing", 1, {"expires_at": None, "window_minutes": 60})
        mock_dispatch.assert_not_called()

    def test_dispatch_error_for_one_sub_does_not_abort_others(self, mock_db):
        from src.services.flash_scarcity import _emit_event
        mock_db.execute.return_value.scalars.return_value.all.return_value = [10, 20]
        call_count = 0
        def _side_effect(payload):
            nonlocal call_count
            call_count += 1
            if payload["subscriber_id"] == 10:
                raise RuntimeError("twilio down")
        with patch("src.agents.supervisor.dispatch_event", side_effect=_side_effect):
            _emit_event(mock_db, "33647", "roofing", 1, {"expires_at": None, "window_minutes": 60})
        assert call_count == 2
