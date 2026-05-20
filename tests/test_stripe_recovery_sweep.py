"""
Unit tests for stripe_recovery_sweep task (Stage 6).

Day 1: soft reminder ~24h after payment_failed_at.
Day 3: urgency + missed-lead email ~72h after payment_failed_at.
Day 5: downgrade/save offer ~120h after payment_failed_at.
"""
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


def _make_sub(payment_failed_offset_hours, day1_sent=False, day3_sent=False, day5_sent=False):
    sub = MagicMock()
    sub.id = 1
    sub.email = "test@example.com"
    sub.name = "Joe"
    sub.event_feed_uuid = "abc-123"
    sub.vertical = "roofing"
    sub.payment_failed_at = datetime.now(timezone.utc) - timedelta(hours=payment_failed_offset_hours)
    sub.recovery_day1_sent = day1_sent
    sub.recovery_day3_sent = day3_sent
    sub.recovery_day5_sent = day5_sent
    return sub


class TestStripeRecoverySweep:
    def _run(self, subs, dry_run=False):
        from src.tasks.stripe_recovery_sweep import run
        with patch("src.tasks.stripe_recovery_sweep.get_db_context") as mock_ctx, \
             patch("src.tasks.stripe_recovery_sweep._send_day1") as mock_d1, \
             patch("src.tasks.stripe_recovery_sweep._send_day3") as mock_d3, \
             patch("src.tasks.stripe_recovery_sweep._send_day5") as mock_d5:
            db = MagicMock()
            db.execute.return_value.scalars.return_value.all.return_value = subs
            mock_ctx.return_value.__enter__ = MagicMock(return_value=db)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            result = run(dry_run=dry_run)
        return result, mock_d1, mock_d3, mock_d5

    def test_day1_sent_at_24h(self):
        sub = _make_sub(24)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub])
        assert result["day1"] == 1
        assert result["day3"] == 0
        assert result["day5"] == 0
        mock_d1.assert_called_once_with(sub)

    def test_day3_sent_at_72h(self):
        sub = _make_sub(72)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub])
        assert result["day3"] == 1
        assert result["day1"] == 0
        assert result["day5"] == 0

    def test_day5_sent_at_120h(self):
        sub = _make_sub(120)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub])
        assert result["day5"] == 1
        assert result["day1"] == 0
        assert result["day3"] == 0
        mock_d5.assert_called_once_with(sub)

    def test_dry_run_no_calls(self):
        sub = _make_sub(24)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub], dry_run=True)
        assert result["day1"] == 1
        mock_d1.assert_not_called()
        mock_d3.assert_not_called()
        mock_d5.assert_not_called()

    def test_day5_dry_run_counted_not_sent(self):
        sub = _make_sub(120)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub], dry_run=True)
        assert result["day5"] == 1
        mock_d5.assert_not_called()

    def test_day1_already_sent_skipped(self):
        sub = _make_sub(24, day1_sent=True)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub])
        mock_d1.assert_not_called()
        assert result["skipped"] == 1

    def test_day3_already_sent_skipped(self):
        sub = _make_sub(72, day3_sent=True)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub])
        mock_d3.assert_not_called()
        assert result["skipped"] == 1

    def test_day5_already_sent_skipped(self):
        sub = _make_sub(120, day5_sent=True)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub])
        mock_d5.assert_not_called()
        assert result["skipped"] == 1

    def test_too_early_skipped(self):
        sub = _make_sub(5)
        result, mock_d1, mock_d3, mock_d5 = self._run([sub])
        mock_d1.assert_not_called()
        mock_d3.assert_not_called()
        mock_d5.assert_not_called()
        assert result["skipped"] == 1

    def test_no_failed_subs_empty_result(self):
        result, mock_d1, mock_d3, mock_d5 = self._run([])
        assert result["day1"] == 0
        assert result["day3"] == 0
        assert result["day5"] == 0


class TestSendDay1Email:
    def test_claude_success_sends_composed_copy(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Jane"
        sub.event_feed_uuid = "uuid-123"
        mock_result = {"text": "SUBJECT: Your card didn't go through\nBODY:\nHi Jane, update your card."}
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", return_value=mock_result), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day1
            _send_day1(sub)
        mock_email.assert_called_once()
        kwargs = mock_email.call_args.kwargs
        assert kwargs["to"] == "test@example.com"
        assert "card" in kwargs["subject"].lower() or "payment" in kwargs["subject"].lower()

    def test_claude_failure_uses_fallback(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Jane"
        sub.event_feed_uuid = "uuid-123"
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", side_effect=Exception("API down")), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day1
            _send_day1(sub)
        mock_email.assert_called_once()
        kwargs = mock_email.call_args.kwargs
        assert "card" in kwargs["subject"].lower() or "payment" in kwargs["subject"].lower()
        assert "Jane" in kwargs["body_text"]

    def test_missing_event_feed_uuid_uses_base_url(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Jane"
        sub.event_feed_uuid = None
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", side_effect=Exception("skip")), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day1
            _send_day1(sub)
        kwargs = mock_email.call_args.kwargs
        assert "app.example.io" in kwargs["body_text"]


class TestSendDay3Email:
    def test_sends_urgency_with_gold_lead_count(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Bob"
        sub.event_feed_uuid = "uuid-456"
        sub.vertical = "roofing"
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = ["33647"]
        fake_leads = [
            {"address": "123 Main St", "zip": "33647", "tier": "gold", "score": 85.0},
        ]
        mock_result = {"text": "SUBJECT: 1 Gold lead you can't see\nBODY:\nHi Bob, fix billing."}
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", return_value=mock_result), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings, \
             patch("src.agents.tools.read_tools.get_lead_pool", return_value=fake_leads):
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day3
            _send_day3(sub, db)
        mock_email.assert_called_once()

    def test_claude_failure_uses_fallback(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Bob"
        sub.event_feed_uuid = "uuid-456"
        sub.vertical = "roofing"
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", side_effect=Exception("API down")), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings, \
             patch("src.agents.tools.read_tools.get_lead_pool", return_value=[]):
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day3
            _send_day3(sub, db)
        mock_email.assert_called_once()
        kwargs = mock_email.call_args.kwargs
        assert "Bob" in kwargs["body_text"]


class TestSendDay5Email:
    def test_claude_success_sends_downgrade_offer(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Sam"
        sub.event_feed_uuid = "uuid-789"
        mock_result = {"text": "SUBJECT: Stay for $97/mo — Data-Only plan\nBODY:\nHi Sam, keep your territory."}
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", return_value=mock_result), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day5
            _send_day5(sub)
        mock_email.assert_called_once()
        kwargs = mock_email.call_args.kwargs
        assert kwargs["to"] == "test@example.com"
        assert "97" in kwargs["subject"] or "data" in kwargs["subject"].lower()

    def test_claude_failure_uses_fallback(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Sam"
        sub.event_feed_uuid = "uuid-789"
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", side_effect=Exception("API down")), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day5
            _send_day5(sub)
        mock_email.assert_called_once()
        kwargs = mock_email.call_args.kwargs
        assert "97" in kwargs["subject"] or "data" in kwargs["subject"].lower()
        assert "Sam" in kwargs["body_text"]

    def test_missing_event_feed_uuid_uses_base_url(self):
        sub = MagicMock()
        sub.id = 1
        sub.email = "test@example.com"
        sub.name = "Sam"
        sub.event_feed_uuid = None
        with patch("src.tasks.stripe_recovery_sweep.call_claude_with_usage", side_effect=Exception("skip")), \
             patch("src.tasks.stripe_recovery_sweep.get_prompt", return_value="prompt"), \
             patch("src.services.email.send_email") as mock_email, \
             patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.app_base_url = "https://app.example.io"
            from src.tasks.stripe_recovery_sweep import _send_day5
            _send_day5(sub)
        kwargs = mock_email.call_args.kwargs
        assert "app.example.io" in kwargs["body_text"]
