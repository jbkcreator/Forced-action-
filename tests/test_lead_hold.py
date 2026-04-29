"""
Lead Hold service tests — Item 35.

All tests are unit tests (no DB required). Redis calls are mocked.

Run:
    pytest tests/test_lead_hold.py -v
"""
from unittest.mock import MagicMock, patch

import pytest

from src.services.lead_hold import (
    get_active_holds,
    get_holder,
    hold,
    is_held,
    is_held_by,
    release,
)


# ============================================================================
# Helpers
# ============================================================================


def _mock_redis_unavailable():
    return patch("src.services.lead_hold.redis_available", return_value=False)


def _mock_redis(existing_value=None):
    """Patch redis_available=True and return a mock Redis client."""
    r = MagicMock()
    r.get.return_value = existing_value
    patcher_avail = patch("src.services.lead_hold.redis_available", return_value=True)
    patcher_get = patch("src.services.lead_hold.get_redis", return_value=r)
    return patcher_avail, patcher_get, r


# ============================================================================
# hold()
# ============================================================================


class TestHoldUnit:
    def test_redis_unavailable_returns_held_true(self):
        with _mock_redis_unavailable():
            result = hold(lead_id=1, subscriber_id=100)
        assert result["held"] is True
        assert result["lead_id"] == 1
        assert result["subscriber_id"] == 100

    def test_hold_new_lead_succeeds(self):
        pa, pg, r = _mock_redis(existing_value=None)
        with pa, pg:
            result = hold(lead_id=42, subscriber_id=7)
        assert result["held"] is True
        r.setex.assert_called_once_with("lead_hold:42", 1200, "7")

    def test_hold_already_held_by_same_subscriber_refreshes(self):
        pa, pg, r = _mock_redis(existing_value=b"7")
        with pa, pg:
            result = hold(lead_id=42, subscriber_id=7)
        assert result["held"] is True

    def test_hold_already_held_by_other_subscriber(self):
        pa, pg, r = _mock_redis(existing_value=b"99")
        with pa, pg:
            result = hold(lead_id=42, subscriber_id=7)
        assert result["held"] is False
        assert result["held_by"] == 99

    def test_hold_returns_expires_at(self):
        pa, pg, r = _mock_redis(existing_value=None)
        with pa, pg:
            result = hold(lead_id=1, subscriber_id=1)
        assert "expires_at" in result
        assert "hold_minutes" in result
        assert result["hold_minutes"] == 20


# ============================================================================
# get_holder() / is_held() / is_held_by()
# ============================================================================


class TestGetHolderUnit:
    def test_returns_none_when_redis_unavailable(self):
        with _mock_redis_unavailable():
            assert get_holder(1) is None

    def test_returns_subscriber_id_when_held(self):
        pa, pg, r = _mock_redis(existing_value=b"55")
        with pa, pg:
            assert get_holder(1) == 55

    def test_returns_none_when_not_held(self):
        pa, pg, r = _mock_redis(existing_value=None)
        with pa, pg:
            assert get_holder(1) is None


class TestIsHeldUnit:
    def test_false_when_redis_unavailable(self):
        with _mock_redis_unavailable():
            assert is_held(1) is False

    def test_true_when_held(self):
        pa, pg, r = _mock_redis(existing_value=b"10")
        with pa, pg:
            assert is_held(1) is True

    def test_false_when_not_held(self):
        pa, pg, r = _mock_redis(existing_value=None)
        with pa, pg:
            assert is_held(1) is False


class TestIsHeldByUnit:
    def test_true_when_holder_matches(self):
        pa, pg, r = _mock_redis(existing_value=b"10")
        with pa, pg:
            assert is_held_by(1, 10) is True

    def test_false_when_holder_differs(self):
        pa, pg, r = _mock_redis(existing_value=b"99")
        with pa, pg:
            assert is_held_by(1, 10) is False

    def test_false_when_not_held(self):
        pa, pg, r = _mock_redis(existing_value=None)
        with pa, pg:
            assert is_held_by(1, 10) is False


# ============================================================================
# release()
# ============================================================================


class TestReleaseUnit:
    def test_redis_unavailable_returns_false(self):
        with _mock_redis_unavailable():
            assert release(1, 10) is False

    def test_release_by_holder_deletes_key(self):
        pa, pg, r = _mock_redis(existing_value=b"10")
        with pa, pg:
            result = release(1, 10)
        assert result is True
        r.delete.assert_called_once()

    def test_release_by_non_holder_fails(self):
        pa, pg, r = _mock_redis(existing_value=b"99")
        with pa, pg:
            result = release(1, 10)
        assert result is False
        r.delete.assert_not_called()

    def test_release_when_not_held_fails(self):
        pa, pg, r = _mock_redis(existing_value=None)
        with pa, pg:
            result = release(1, 10)
        assert result is False


# ============================================================================
# get_active_holds()
# ============================================================================


class TestGetActiveHoldsUnit:
    def test_returns_empty_when_redis_unavailable(self):
        with _mock_redis_unavailable():
            assert get_active_holds(10) == []

    def test_returns_held_lead_ids(self):
        r = MagicMock()
        r.scan_iter.return_value = ["lead_hold:1", "lead_hold:5", "lead_hold:9"]
        r.get.side_effect = lambda k: "10" if k in ("lead_hold:1", "lead_hold:5") else "99"
        with patch("src.services.lead_hold.redis_available", return_value=True), \
             patch("src.services.lead_hold.get_redis", return_value=r):
            result = get_active_holds(10)
        assert sorted(result) == [1, 5]

    def test_scan_exception_returns_empty(self):
        r = MagicMock()
        r.scan_iter.side_effect = Exception("Redis timeout")
        with patch("src.services.lead_hold.redis_available", return_value=True), \
             patch("src.services.lead_hold.get_redis", return_value=r):
            result = get_active_holds(10)
        assert result == []
