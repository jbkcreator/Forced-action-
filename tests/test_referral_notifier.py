"""
Unit tests for referral_notifier — Lifecycle-composed SMS + email for milestone events.

Covers:
- All 3 milestone types (per_referral, free_month_3, lock_slot_5)
- Both channels (SMS + email)
- Claude success path
- Claude failure → static fallback
- Missing subscriber email (no email send)
- Dedup lock behavior (second worker loses the lock)
"""
import json
import pytest
from unittest.mock import MagicMock, patch, call


def _make_payload(msg_type, referrer_id=1, event_id="evt-1", n_total=1, share_url="https://fa.io/share/abc"):
    return {
        "type": msg_type,
        "referrer_id": referrer_id,
        "event_id": event_id,
        "n_total": n_total,
        "share_url": share_url,
    }


def _make_sub(email="sub@example.com", phone="+18135550001", name="Alex"):
    sub = MagicMock()
    sub.id = 1
    sub.email = email
    sub.phone = phone
    sub.name = name
    return sub


# ── _compose_sms ─────────────────────────────────────────────────────────────

class TestComposeSms:
    @pytest.mark.parametrize("msg_type", ["per_referral", "free_month_3", "lock_slot_5"])
    def test_claude_success_returns_short_text(self, msg_type):
        mock_result = {"text": "Great job! Your referral was confirmed."}
        with patch("src.services.referral_notifier.call_claude_with_usage", return_value=mock_result), \
             patch("src.services.referral_notifier.get_prompt", return_value="prompt"):
            from src.services.referral_notifier import _compose_sms
            result = _compose_sms(msg_type, _make_payload(msg_type), "Alex")
        assert result == "Great job! Your referral was confirmed."

    @pytest.mark.parametrize("msg_type", ["per_referral", "free_month_3", "lock_slot_5"])
    def test_claude_failure_returns_static_fallback(self, msg_type):
        with patch("src.services.referral_notifier.call_claude_with_usage", side_effect=Exception("API down")), \
             patch("src.services.referral_notifier.get_prompt", return_value="prompt"):
            from src.services.referral_notifier import _compose_sms
            result = _compose_sms(msg_type, _make_payload(msg_type, n_total=2), "Alex")
        assert result  # non-empty fallback
        assert len(result) <= 160

    def test_claude_output_too_long_falls_back(self):
        long_text = "x" * 200
        mock_result = {"text": long_text}
        with patch("src.services.referral_notifier.call_claude_with_usage", return_value=mock_result), \
             patch("src.services.referral_notifier.get_prompt", return_value="prompt"):
            from src.services.referral_notifier import _compose_sms
            result = _compose_sms("per_referral", _make_payload("per_referral", n_total=1), "Alex")
        assert len(result) <= 160

    def test_lock_slot_5_fallback_includes_share_url(self):
        with patch("src.services.referral_notifier.call_claude_with_usage", side_effect=Exception("down")), \
             patch("src.services.referral_notifier.get_prompt", return_value="prompt"):
            from src.services.referral_notifier import _compose_sms
            payload = _make_payload("lock_slot_5", share_url="https://fa.io/ref/abc")
            result = _compose_sms("lock_slot_5", payload, "Alex")
        assert "https://fa.io/ref/abc" in result


# ── _compose_email ────────────────────────────────────────────────────────────

class TestComposeEmail:
    @pytest.mark.parametrize("msg_type", ["per_referral", "free_month_3", "lock_slot_5"])
    def test_claude_success_returns_subject_and_body(self, msg_type):
        mock_result = {"text": "SUBJECT: Nice work!\nBODY:\nHi Alex, great referral."}
        with patch("src.services.referral_notifier.call_claude_with_usage", return_value=mock_result), \
             patch("src.services.referral_notifier.get_prompt", return_value="prompt"):
            from src.services.referral_notifier import _compose_email
            subject, body = _compose_email(msg_type, _make_payload(msg_type), "Alex")
        assert subject == "Nice work!"
        assert "Alex" in body

    @pytest.mark.parametrize("msg_type", ["per_referral", "free_month_3", "lock_slot_5"])
    def test_claude_failure_returns_static_fallback(self, msg_type):
        with patch("src.services.referral_notifier.call_claude_with_usage", side_effect=Exception("down")), \
             patch("src.services.referral_notifier.get_prompt", return_value="prompt"):
            from src.services.referral_notifier import _compose_email
            subject, body = _compose_email(msg_type, _make_payload(msg_type), "Alex")
        assert subject
        assert body
        assert "Alex" in body

    def test_unknown_msg_type_returns_empty(self):
        from src.services.referral_notifier import _compose_email
        subject, body = _compose_email("unknown_type", {}, "Alex")
        assert subject == ""
        assert body == ""

    def test_lock_slot_5_fallback_includes_share_url(self):
        with patch("src.services.referral_notifier.call_claude_with_usage", side_effect=Exception("down")), \
             patch("src.services.referral_notifier.get_prompt", return_value="prompt"):
            from src.services.referral_notifier import _compose_email
            payload = _make_payload("lock_slot_5", share_url="https://fa.io/ref/xyz")
            _, body = _compose_email("lock_slot_5", payload, "Alex")
        assert "https://fa.io/ref/xyz" in body


# ── subscribe_and_send integration ───────────────────────────────────────────

class TestSubscribeAndSend:
    def _make_redis_messages(self, payloads):
        """Wrap payloads as Redis pub/sub messages, followed by a sentinel to stop iteration."""
        messages = [{"type": "message", "data": json.dumps(p)} for p in payloads]
        messages.append({"type": "subscribe", "data": None})  # non-message type stops processing
        return iter(messages)

    def _run_one(self, payload, sub, sms_body="SMS text", email_pair=("Subject", "Body text")):
        from src.services.referral_notifier import subscribe_and_send
        with patch("src.services.referral_notifier.redis_available", return_value=True), \
             patch("src.services.referral_notifier._get_client") as mock_client_fn, \
             patch("src.services.referral_notifier._acquire_dedup_lock", return_value=True), \
             patch("src.services.referral_notifier._compose_sms", return_value=sms_body), \
             patch("src.services.referral_notifier._compose_email", return_value=email_pair), \
             patch("src.services.referral_notifier._stamp_notified_at"), \
             patch("src.services.referral_notifier.can_send", return_value=True), \
             patch("src.services.referral_notifier.send_sms") as mock_sms, \
             patch("src.services.referral_notifier.send_email") as mock_email, \
             patch("src.services.referral_notifier.Database") as mock_db_cls:

            mock_client = MagicMock()
            mock_client_fn.return_value = mock_client
            mock_pubsub = MagicMock()
            mock_client.pubsub.return_value = mock_pubsub
            mock_pubsub.listen.return_value = self._make_redis_messages([payload])

            mock_db = MagicMock()
            mock_db.__enter__ = MagicMock(return_value=mock_db)
            mock_db.__exit__ = MagicMock(return_value=False)
            mock_session_scope = MagicMock()
            mock_session_scope.__enter__ = MagicMock(return_value=mock_db)
            mock_session_scope.__exit__ = MagicMock(return_value=False)
            mock_db_cls.return_value.session_scope.return_value = mock_session_scope
            mock_db.get.return_value = sub

            subscribe_and_send()

        return mock_sms, mock_email

    @pytest.mark.parametrize("msg_type", ["per_referral", "free_month_3", "lock_slot_5"])
    def test_sms_sent_for_all_milestones(self, msg_type):
        sub = _make_sub()
        payload = _make_payload(msg_type)
        mock_sms, _ = self._run_one(payload, sub)
        mock_sms.assert_called_once()

    @pytest.mark.parametrize("msg_type", ["per_referral", "free_month_3", "lock_slot_5"])
    def test_email_sent_for_all_milestones(self, msg_type):
        sub = _make_sub()
        payload = _make_payload(msg_type)
        _, mock_email = self._run_one(payload, sub, email_pair=("Subject", "Body"))
        mock_email.assert_called_once()
        kwargs = mock_email.call_args.kwargs
        assert kwargs["to"] == "sub@example.com"

    def test_no_email_when_subscriber_has_no_email(self):
        sub = _make_sub(email=None)
        payload = _make_payload("per_referral")
        _, mock_email = self._run_one(payload, sub)
        mock_email.assert_not_called()

    def test_dedup_lock_lost_skips_both_channels(self):
        sub = _make_sub()
        payload = _make_payload("per_referral")
        from src.services.referral_notifier import subscribe_and_send
        with patch("src.services.referral_notifier.redis_available", return_value=True), \
             patch("src.services.referral_notifier._get_client") as mock_client_fn, \
             patch("src.services.referral_notifier._acquire_dedup_lock", return_value=False), \
             patch("src.services.referral_notifier.send_sms") as mock_sms, \
             patch("src.services.referral_notifier.send_email") as mock_email, \
             patch("src.services.referral_notifier.Database"):
            mock_client = MagicMock()
            mock_client_fn.return_value = mock_client
            mock_pubsub = MagicMock()
            mock_client.pubsub.return_value = mock_pubsub
            mock_pubsub.listen.return_value = self._make_redis_messages([payload])
            subscribe_and_send()
        mock_sms.assert_not_called()
        mock_email.assert_not_called()

    def test_unknown_msg_type_skipped(self):
        sub = _make_sub()
        payload = _make_payload("unknown_event")
        mock_sms, mock_email = self._run_one(payload, sub)
        mock_sms.assert_not_called()
        mock_email.assert_not_called()

    def test_subscriber_not_found_skips_channels(self):
        payload = _make_payload("per_referral")
        from src.services.referral_notifier import subscribe_and_send
        with patch("src.services.referral_notifier.redis_available", return_value=True), \
             patch("src.services.referral_notifier._get_client") as mock_client_fn, \
             patch("src.services.referral_notifier._acquire_dedup_lock", return_value=True), \
             patch("src.services.referral_notifier.send_sms") as mock_sms, \
             patch("src.services.referral_notifier.send_email") as mock_email, \
             patch("src.services.referral_notifier.Database") as mock_db_cls:
            mock_client = MagicMock()
            mock_client_fn.return_value = mock_client
            mock_pubsub = MagicMock()
            mock_client.pubsub.return_value = mock_pubsub
            mock_pubsub.listen.return_value = self._make_redis_messages([payload])
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session.get.return_value = None  # subscriber not found
            mock_db_cls.return_value.session_scope.return_value = mock_session
            subscribe_and_send()
        mock_sms.assert_not_called()
        mock_email.assert_not_called()
