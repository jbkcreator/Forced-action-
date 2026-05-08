"""
Unit tests for Save / Pause 60-day flow.
"""
import sys
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, PropertyMock


def _stripe_mock():
    m = MagicMock()
    m.Subscription = MagicMock()
    return m


def _make_sub(sub_id=1, status="active", stripe_sub_id="sub_abc"):
    sub = MagicMock()
    sub.id = sub_id
    sub.status = status
    sub.stripe_subscription_id = stripe_sub_id
    sub.paused_at = None
    sub.pause_resume_at = None
    return sub


def _settings_mock(secret="sk_test"):
    s = MagicMock()
    s.active_stripe_secret_key.get_secret_value.return_value = secret
    return s


class TestPause:
    def _run_pause(self, sub, days=60, stripe_side_effect=None):
        from src.services.pause_subscription import pause_subscriber
        stripe_m = _stripe_mock()
        if stripe_side_effect:
            stripe_m.Subscription.modify.side_effect = stripe_side_effect
        db = MagicMock()
        db.get.return_value = sub
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("src.services.pause_subscription._settings", _settings_mock(), create=True):
            # settings is imported lazily via `from config.settings import settings as _settings`
            # patch at config module level
            with patch("config.settings.get_settings", return_value=_settings_mock()):
                result = pause_subscriber(db, sub.id, days=days)
        return result, stripe_m

    def test_active_sub_can_pause(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        from src.services.pause_subscription import pause_subscriber
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = pause_subscriber(db, 1)
        assert result is True
        assert sub.status == "paused"
        assert sub.paused_at is not None
        assert sub.pause_resume_at is not None

    def test_already_paused_returns_false(self):
        from src.services.pause_subscription import pause_subscriber
        sub = _make_sub(status="paused")
        db = MagicMock()
        db.get.return_value = sub
        result = pause_subscriber(db, 1)
        assert result is False

    def test_missing_subscriber_returns_false(self):
        from src.services.pause_subscription import pause_subscriber
        db = MagicMock()
        db.get.return_value = None
        result = pause_subscriber(db, 99)
        assert result is False

    def test_stripe_modify_called_with_void_behavior(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        from src.services.pause_subscription import pause_subscriber
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            pause_subscriber(db, 1)
        args, kwargs = stripe_m.Subscription.modify.call_args
        pause_arg = kwargs.get("pause_collection") or args[1]
        assert pause_arg["behavior"] == "void"
        assert "resumes_at" in pause_arg

    def test_resume_at_60_days_from_now(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        before = datetime.now(timezone.utc)
        from src.services.pause_subscription import pause_subscriber
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            pause_subscriber(db, 1, days=60)
        after = datetime.now(timezone.utc)
        assert sub.pause_resume_at >= before + timedelta(days=59)
        assert sub.pause_resume_at <= after + timedelta(days=61)

    def test_stripe_failure_returns_false(self):
        sub = _make_sub()
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        stripe_m.Subscription.modify.side_effect = Exception("Stripe error")
        from src.services.pause_subscription import pause_subscriber
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = pause_subscriber(db, 1)
        assert result is False
        assert sub.status == "active"


class TestResume:
    def test_clears_pause_collection(self):
        from src.services.pause_subscription import resume_subscriber
        sub = _make_sub(status="paused")
        sub.paused_at = datetime.now(timezone.utc)
        sub.pause_resume_at = datetime.now(timezone.utc) + timedelta(days=30)
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            result = resume_subscriber(db, 1)
        assert result is True
        _, kwargs = stripe_m.Subscription.modify.call_args
        assert kwargs.get("pause_collection") == ""

    def test_status_back_to_active(self):
        from src.services.pause_subscription import resume_subscriber
        sub = _make_sub(status="paused")
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            resume_subscriber(db, 1)
        assert sub.status == "active"

    def test_resume_dates_cleared(self):
        from src.services.pause_subscription import resume_subscriber
        sub = _make_sub(status="paused")
        sub.paused_at = datetime.now(timezone.utc)
        sub.pause_resume_at = datetime.now(timezone.utc) + timedelta(days=30)
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            resume_subscriber(db, 1)
        assert sub.paused_at is None
        assert sub.pause_resume_at is None

    def test_missing_subscriber_returns_false(self):
        from src.services.pause_subscription import resume_subscriber
        db = MagicMock()
        db.get.return_value = None
        result = resume_subscriber(db, 99)
        assert result is False


class TestSmsPauseFlow:
    def _make_sub(self, sub_id=1, status="active"):
        sub = MagicMock()
        sub.id = sub_id
        sub.status = status
        sub.stripe_subscription_id = "sub_abc"
        sub.event_feed_uuid = "uuid-123"
        sub.pause_resume_at = datetime(2026, 7, 5, tzinfo=timezone.utc)
        return sub

    def test_first_pause_sends_confirm(self):
        from src.services.sms_commands import _handle_pause
        sub = self._make_sub()
        db = MagicMock()
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.rget", return_value=None), \
             patch("src.core.redis_client.rset") as mock_rset:
            reply = _handle_pause(sub, db)
        assert "YES" in reply
        assert "60 days" in reply
        mock_rset.assert_called_once()

    def test_yes_within_5min_pauses(self):
        from src.services.sms_commands import _handle_pause_yes
        sub = self._make_sub()
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.rget", return_value="1"), \
             patch("src.core.redis_client.rdelete"), \
             patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            reply = _handle_pause_yes(sub, db)
        assert "July 05" in reply or "paused" in reply.lower()

    def test_yes_after_timeout_no_action(self):
        from src.services.sms_commands import _handle_pause_yes
        sub = self._make_sub()
        db = MagicMock()
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.rget", return_value=None):
            reply = _handle_pause_yes(sub, db)
        assert "PAUSE" in reply

    def test_redis_state_ttl_5min(self):
        from src.services.sms_commands import _handle_pause, _PAUSE_PENDING_TTL
        sub = self._make_sub()
        db = MagicMock()
        with patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.rget", return_value=None), \
             patch("src.core.redis_client.rset") as mock_rset:
            _handle_pause(sub, db)
        _, kwargs = mock_rset.call_args
        assert kwargs.get("ttl_seconds") == _PAUSE_PENDING_TTL

    def test_resume_command_active_sub(self):
        from src.services.sms_commands import _handle_resume
        sub = self._make_sub(status="active")
        db = MagicMock()
        reply = _handle_resume(sub, db)
        assert "already active" in reply.lower()

    def test_resume_command_paused_sub(self):
        from src.services.sms_commands import _handle_resume
        sub = self._make_sub(status="paused")
        db = MagicMock()
        db.get.return_value = sub
        stripe_m = _stripe_mock()
        with patch.dict(sys.modules, {"stripe": stripe_m}), \
             patch("config.settings.get_settings", return_value=_settings_mock()):
            reply = _handle_resume(sub, db)
        assert "resumed" in reply.lower()
