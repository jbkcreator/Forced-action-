"""
Monetization Wall service tests — Item 25.

All tests are unit tests. Redis calls are mocked; DB calls mocked for ROI frame.

Run:
    pytest tests/test_monetization_wall.py -v
"""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.services.monetization_wall import (
    _DEFAULT_ROI,
    _ROI_FRAMES,
    create_session,
    get_roi_frame,
    get_session_state,
    is_active,
    mark_converted,
)


# ============================================================================
# Helpers
# ============================================================================


def _no_redis():
    return patch("src.services.monetization_wall.redis_available", return_value=False)


def _with_redis(stored_value=None):
    r = MagicMock()
    r.get.return_value = json.dumps(stored_value).encode() if stored_value else None
    r.ttl.return_value = 3600
    patcher_avail = patch("src.services.monetization_wall.redis_available", return_value=True)
    patcher_get = patch("src.services.monetization_wall.get_redis", return_value=r)
    return patcher_avail, patcher_get, r


# ============================================================================
# create_session()
# ============================================================================


class TestCreateSessionUnit:
    def test_returns_state_dict(self):
        with _no_redis():
            state = create_session(subscriber_id=1, session_id="sess_abc")
        assert state["subscriber_id"] == 1
        assert state["session_id"] == "sess_abc"
        assert state["converted"] is False
        assert "countdown_expires" in state
        assert "created_at" in state

    def test_countdown_expires_15_minutes_from_now(self):
        with _no_redis():
            state = create_session(subscriber_id=1, session_id="sess_abc")
        created = datetime.fromisoformat(state["created_at"])
        expires = datetime.fromisoformat(state["countdown_expires"])
        diff = (expires - created).total_seconds()
        assert 890 <= diff <= 910   # ~15 minutes

    def test_stores_in_redis_when_available(self):
        pa, pg, r = _with_redis()
        with pa, pg:
            create_session(subscriber_id=2, session_id="sess_xyz")
        r.setex.assert_called_once()
        key, ttl, payload = r.setex.call_args[0]
        assert "sess_xyz" in key
        assert ttl == 24 * 3600
        parsed = json.loads(payload)
        assert parsed["subscriber_id"] == 2

    def test_no_crash_when_redis_unavailable(self):
        with _no_redis():
            state = create_session(subscriber_id=99, session_id="s1")
        assert state is not None


# ============================================================================
# get_session_state()
# ============================================================================


class TestGetSessionStateUnit:
    def test_returns_none_when_redis_unavailable(self):
        with _no_redis():
            assert get_session_state("sess_abc") is None

    def test_returns_none_when_key_missing(self):
        pa, pg, r = _with_redis(stored_value=None)
        with pa, pg:
            assert get_session_state("sess_missing") is None

    def test_returns_parsed_state(self):
        stored = {"subscriber_id": 5, "session_id": "sess_5", "converted": False}
        pa, pg, r = _with_redis(stored_value=stored)
        with pa, pg:
            result = get_session_state("sess_5")
        assert result["subscriber_id"] == 5
        assert result["converted"] is False


# ============================================================================
# mark_converted()
# ============================================================================


class TestMarkConvertedUnit:
    def test_no_op_when_redis_unavailable(self):
        with _no_redis():
            mark_converted("sess_abc")   # should not raise

    def test_no_op_when_key_missing(self):
        pa, pg, r = _with_redis(stored_value=None)
        with pa, pg:
            mark_converted("sess_missing")
        r.setex.assert_not_called()

    def test_sets_converted_true(self):
        stored = {"subscriber_id": 1, "session_id": "s", "converted": False}
        pa, pg, r = _with_redis(stored_value=stored)
        with pa, pg:
            mark_converted("s")
        r.setex.assert_called_once()
        _, _, payload = r.setex.call_args[0]
        updated = json.loads(payload)
        assert updated["converted"] is True
        assert "converted_at" in updated


# ============================================================================
# is_active()
# ============================================================================


class TestIsActiveUnit:
    def test_false_when_state_is_none(self):
        with _no_redis():
            assert is_active("missing") is False

    def test_true_when_state_exists(self):
        stored = {"subscriber_id": 1, "session_id": "s", "converted": False}
        pa, pg, r = _with_redis(stored_value=stored)
        with pa, pg:
            assert is_active("s") is True


# ============================================================================
# get_roi_frame()
# ============================================================================


class TestGetRoiFrameUnit:
    def test_known_vertical_returns_frame(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = 150
        result = get_roi_frame("roofing", "hillsborough", db)
        assert result["vertical"] == "roofing"
        assert result["live_lead_count"] == 150
        assert "headline" in result
        assert "avg_job_value" in result

    def test_unknown_vertical_uses_default(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = 0
        result = get_roi_frame("plumber", "hillsborough", db)
        assert result["headline"] == _DEFAULT_ROI["headline"]

    def test_db_exception_sets_live_count_none(self):
        db = MagicMock()
        db.execute.side_effect = Exception("DB error")
        result = get_roi_frame("roofing", "hillsborough", db)
        assert result["live_lead_count"] is None

    @pytest.mark.parametrize("vertical", list(_ROI_FRAMES.keys()))
    def test_all_defined_verticals_have_headline(self, vertical):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = 0
        result = get_roi_frame(vertical, "hillsborough", db)
        assert "headline" in result
