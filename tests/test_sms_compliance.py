"""
SMS compliance service tests.

Unit tests:  mock DB session — no real DB required.
Integration: fresh_db fixture — real Postgres, rolled back after each test.

Run:
    pytest tests/test_sms_compliance.py -v
    pytest tests/test_sms_compliance.py -v -k "unit"
    pytest tests/test_sms_compliance.py -v -k "integration"
"""

from unittest.mock import MagicMock, patch, call

import pytest
from sqlalchemy import select

import src.services.sms_compliance  # noqa: F401 — ensure module importable

from src.services.sms_compliance import (
    _extract_stop_keyword,
    _normalize,
    _twiml_reply,
    add_to_dead_letter,
    can_send,
    handle_inbound,
    handle_opt_in_reply,
    has_opted_in,
    record_opt_in,
    record_opt_out,
    send_opt_in_prompt,
    send_sms,
)
from src.core.models import SmsDeadLetter, SmsOptIn, SmsOptOut, SmsSendLog


# ============================================================================
# Unit tests — mock DB, no Postgres required
# ============================================================================


class TestNormalizeUnit:
    def test_strips_whitespace(self):
        assert _normalize("  +18135550100  ") == "+18135550100"

    def test_empty_string(self):
        assert _normalize("") == ""

    def test_none_safe(self):
        assert _normalize(None) == ""


class TestExtractStopKeywordUnit:
    @pytest.mark.parametrize("body,expected", [
        ("STOP",        "stop"),
        ("stop",        "stop"),
        ("Stop",        "stop"),
        ("UNSUBSCRIBE", "unsubscribe"),
        ("CANCEL",      "cancel"),
        ("QUIT",        "quit"),
        ("END",         "end"),
        ("stop please", "stop"),        # STOP as first word
        ("  STOP  ",    "stop"),        # surrounding whitespace
    ])
    def test_recognises_stop_keywords(self, body, expected):
        assert _extract_stop_keyword(body) == expected

    @pytest.mark.parametrize("body", [
        "YES",
        "Hello",
        "I want to stop receiving messages",  # STOP not first word
        "",
        "   ",
    ])
    def test_non_stop_returns_none(self, body):
        assert _extract_stop_keyword(body) is None


class TestTwimlReplyUnit:
    def test_valid_xml_structure(self):
        xml = _twiml_reply("You have been unsubscribed.")
        assert xml.startswith('<?xml version="1.0"')
        assert "<Response>" in xml
        assert "<Message>" in xml
        assert "You have been unsubscribed." in xml

    def test_escapes_ampersand(self):
        xml = _twiml_reply("Terms & conditions")
        assert "&amp;" in xml
        assert "&" not in xml.replace("&amp;", "").replace("&lt;", "").replace("&gt;", "")

    def test_escapes_angle_brackets(self):
        xml = _twiml_reply("<script>alert(1)</script>")
        assert "<script>" not in xml
        assert "&lt;script&gt;" in xml


class TestCanSendUnit:
    def test_returns_true_when_not_suppressed(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        assert can_send("+18135550100", db) is True

    def test_returns_false_when_suppressed(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = (1,)
        assert can_send("+18135550100", db) is False

    def test_returns_false_for_empty_phone(self):
        db = MagicMock()
        assert can_send("", db) is False
        db.execute.assert_not_called()

    def test_normalises_phone_before_check(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        can_send("  +18135550100  ", db)
        # execute was called — phone was normalised and passed through
        db.execute.assert_called_once()


class TestRecordOptOutUnit:
    def test_writes_to_db_when_not_existing(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        record_opt_out("+18135550100", "STOP", "twilio_inbound", db)
        db.add.assert_called_once()
        db.flush.assert_called_once()
        added: SmsOptOut = db.add.call_args[0][0]
        assert added.phone == "+18135550100"
        assert added.keyword_used == "STOP"
        assert added.source == "twilio_inbound"

    def test_idempotent_when_already_exists(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = MagicMock()  # already exists
        record_opt_out("+18135550100", "STOP", "twilio_inbound", db)
        db.add.assert_not_called()

    def test_truncates_long_keyword(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        record_opt_out("+18135550100", "A" * 50, "manual", db)
        added: SmsOptOut = db.add.call_args[0][0]
        assert len(added.keyword_used) <= 20

    def test_uppercases_keyword(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        record_opt_out("+18135550100", "stop", "twilio_inbound", db)
        added: SmsOptOut = db.add.call_args[0][0]
        assert added.keyword_used == "STOP"

    def test_skips_empty_phone(self):
        db = MagicMock()
        record_opt_out("", "STOP", "twilio_inbound", db)
        db.add.assert_not_called()


class TestAddToDeadLetterUnit:
    def test_writes_valid_reason(self):
        db = MagicMock()
        add_to_dead_letter("+18135550100", "opt_out", {"body": "hello"}, db)
        db.add.assert_called_once()
        entry: SmsDeadLetter = db.add.call_args[0][0]
        assert entry.reason == "opt_out"
        assert entry.phone == "+18135550100"
        assert entry.payload == {"body": "hello"}

    @pytest.mark.parametrize("reason", [
        "opt_out", "delivery_failed", "error", "unresolvable", "quiet_hours", "no_opt_in",
    ])
    def test_all_valid_reasons_accepted(self, reason):
        db = MagicMock()
        add_to_dead_letter("+18135550100", reason, None, db)
        entry: SmsDeadLetter = db.add.call_args[0][0]
        assert entry.reason == reason

    def test_invalid_reason_defaults_to_error(self):
        db = MagicMock()
        add_to_dead_letter("+18135550100", "bad_reason", None, db)
        entry: SmsDeadLetter = db.add.call_args[0][0]
        assert entry.reason == "error"

    def test_accepts_none_phone(self):
        db = MagicMock()
        add_to_dead_letter(None, "error", {"context": "unknown"}, db)
        entry: SmsDeadLetter = db.add.call_args[0][0]
        assert entry.phone is None


class TestHandleInboundUnit:
    def test_stop_keyword_returns_twiml(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        result = handle_inbound("+18135550100", "STOP", db)
        assert result is not None
        assert "<Response>" in result
        assert "unsubscribed" in result.lower()

    @pytest.mark.parametrize("keyword", ["STOP", "UNSUBSCRIBE", "CANCEL", "QUIT", "END"])
    def test_all_stop_keywords_trigger_opt_out(self, keyword):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        result = handle_inbound("+18135550100", keyword, db)
        assert result is not None
        db.add.assert_called_once()

    def test_non_stop_returns_none(self):
        db = MagicMock()
        result = handle_inbound("+18135550100", "YES", db)
        assert result is None
        db.add.assert_not_called()

    def test_empty_body_returns_none(self):
        db = MagicMock()
        result = handle_inbound("+18135550100", "", db)
        assert result is None


class TestSendSmsUnit:
    def test_suppressed_number_returns_false_and_dlqs(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=False):
            with patch("src.services.sms_compliance.add_to_dead_letter") as mock_dlq:
                result = send_sms("+18135550100", "Hello", db)
        assert result is False
        mock_dlq.assert_called_once()
        _, reason, _ = mock_dlq.call_args[0][:3]
        assert reason == "opt_out"

    def test_quiet_hours_returns_false_and_dlqs_with_correct_reason(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=True):
            with patch("src.services.sms_compliance.add_to_dead_letter") as mock_dlq:
                result = send_sms("+18135550100", "Hello", db)
        assert result is False
        mock_dlq.assert_called_once()
        _, reason, _ = mock_dlq.call_args[0][:3]
        assert reason == "quiet_hours"

    def test_dry_run_returns_true_without_telnyx(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.telnyx_sms_enabled = False
                result = send_sms("+18135550100", "Hello", db)
        assert result is True

    def test_telnyx_not_configured_returns_false_and_dlqs(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.telnyx_sms_enabled = True
                mock_settings.telnyx_sms_api_key = None
                mock_settings.telnyx_messaging_profile_id = None
                mock_settings.telnyx_from_number = None
                with patch("src.services.sms_compliance.add_to_dead_letter") as mock_dlq:
                    result = send_sms("+18135550100", "Hello", db)
        assert result is False
        mock_dlq.assert_called_once()

    def test_telnyx_success_returns_true(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.telnyx_sms_enabled = True
                mock_settings.telnyx_sms_api_key = "tlnx_key"
                mock_settings.telnyx_messaging_profile_id = "mp_xxx"
                mock_settings.telnyx_from_number = "+18005550000"
                with patch("src.services.sms_compliance.telnyx_send_message") as mock_send:
                    mock_send.return_value = {
                        "message_id": "msg_xxx", "status": "queued",
                        "vendor": "telnyx", "cost_cents": 0,
                        "sent_at": "2026-05-11T00:00:00",
                    }
                    result = send_sms("+18135550100", "Hello", db)
        assert result is True

    def test_telnyx_exception_dlqs_and_returns_false(self):
        from src.services.telnyx_sms import TelnyxSMSError
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.telnyx_sms_enabled = True
                mock_settings.telnyx_sms_api_key = "tlnx_key"
                mock_settings.telnyx_messaging_profile_id = "mp_xxx"
                mock_settings.telnyx_from_number = "+18005550000"
                with patch("src.services.sms_compliance.telnyx_send_message") as mock_send:
                    mock_send.side_effect = TelnyxSMSError("Network error")
                    with patch("src.services.sms_compliance.add_to_dead_letter") as mock_dlq:
                        result = send_sms("+18135550100", "Hello", db)
        assert result is False
        mock_dlq.assert_called_once()
        _, reason, _ = mock_dlq.call_args[0][:3]
        assert reason == "delivery_failed"


# ============================================================================
# Integration tests — real Postgres, rolled back after each test
# ============================================================================


class TestCanSendIntegration:
    def test_returns_true_for_unknown_number(self, fresh_db):
        assert can_send("+18135550101", fresh_db) is True

    def test_returns_false_after_opt_out(self, fresh_db):
        fresh_db.add(SmsOptOut(phone="+18135550102", keyword_used="STOP", source="twilio_inbound"))
        fresh_db.flush()
        assert can_send("+18135550102", fresh_db) is False

    def test_unrelated_number_still_sendable(self, fresh_db):
        fresh_db.add(SmsOptOut(phone="+18135550103", keyword_used="STOP", source="twilio_inbound"))
        fresh_db.flush()
        assert can_send("+18135550104", fresh_db) is True


class TestRecordOptOutIntegration:
    def test_writes_row_to_db(self, fresh_db):
        record_opt_out("+18135550110", "UNSUBSCRIBE", "twilio_inbound", fresh_db)
        row = fresh_db.execute(
            select(SmsOptOut).where(SmsOptOut.phone == "+18135550110")
        ).scalar_one_or_none()
        assert row is not None
        assert row.keyword_used == "UNSUBSCRIBE"
        assert row.source == "twilio_inbound"

    def test_idempotent_on_duplicate_call(self, fresh_db):
        record_opt_out("+18135550111", "STOP", "twilio_inbound", fresh_db)
        record_opt_out("+18135550111", "STOP", "twilio_inbound", fresh_db)  # second call
        rows = fresh_db.execute(
            select(SmsOptOut).where(SmsOptOut.phone == "+18135550111")
        ).scalars().all()
        assert len(rows) == 1  # only one row


class TestAddToDeadLetterIntegration:
    def test_writes_row_to_db(self, fresh_db):
        add_to_dead_letter("+18135550120", "delivery_failed", {"body": "test msg"}, fresh_db)
        row = fresh_db.execute(
            select(SmsDeadLetter).where(SmsDeadLetter.phone == "+18135550120")
        ).scalar_one_or_none()
        assert row is not None
        assert row.reason == "delivery_failed"
        assert row.payload == {"body": "test msg"}
        assert row.reviewed_at is None

    def test_multiple_dlq_entries_same_phone(self, fresh_db):
        add_to_dead_letter("+18135550121", "error", {"attempt": 1}, fresh_db)
        add_to_dead_letter("+18135550121", "error", {"attempt": 2}, fresh_db)
        rows = fresh_db.execute(
            select(SmsDeadLetter).where(SmsDeadLetter.phone == "+18135550121")
        ).scalars().all()
        assert len(rows) == 2


class TestFullOptOutFlowIntegration:
    def test_opt_out_then_blocked(self, fresh_db):
        phone = "+18135550130"
        assert can_send(phone, fresh_db) is True
        record_opt_out(phone, "STOP", "twilio_inbound", fresh_db)
        assert can_send(phone, fresh_db) is False

    def test_handle_inbound_stop_writes_and_blocks(self, fresh_db):
        phone = "+18135550131"
        twiml = handle_inbound(phone, "STOP", fresh_db)
        assert twiml is not None
        assert can_send(phone, fresh_db) is False

    def test_handle_inbound_non_stop_does_not_block(self, fresh_db):
        phone = "+18135550132"
        result = handle_inbound(phone, "YES", fresh_db)
        assert result is None
        assert can_send(phone, fresh_db) is True


# ── Panhandle / multi-timezone quiet-hours guard ───────────────────────────────


class TestQuietHoursTimezone:
    """fa008+ (2026-05-04): 850 area code now maps to CST instead of ET.

    The TCPA window is 8am-9pm recipient local. For 850 numbers that are
    physically in CST (Pensacola etc.), the old ET mapping would have allowed
    sends between 8pm and 9pm CST = 9pm-10pm ET, which is a TCPA violation.
    """

    def _patch_now(self, hour_local: int, tz_name: str):
        """Force datetime.now(tz) to return `hour_local` in the given TZ."""
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        target = datetime.now(ZoneInfo(tz_name)).replace(
            hour=hour_local, minute=0, second=0, microsecond=0,
        )
        # Convert back to UTC for a "frozen" point in time
        utc_now = target.astimezone(timezone.utc)

        class _FrozenDT:
            @staticmethod
            def now(tz=None):
                if tz is None:
                    return utc_now
                return utc_now.astimezone(tz)
        return patch("src.services.sms_compliance.datetime", _FrozenDT)

    def test_850_treated_as_cst(self):
        """850 numbers should resolve to America/Chicago, not New_York."""
        from src.services.sms_compliance import _recipient_tz
        tz = _recipient_tz("+18505550100")
        assert str(tz) == "America/Chicago"

    def test_850_quiet_hours_at_8pm_cst_are_quiet(self):
        """8:30pm CST = 9:30pm ET. If the gate were ET-based we'd block, but
        we evaluate in CST — and 8:30pm CST is INSIDE quiet hours
        (gate fires at 9pm CST). So this should be blocked too."""
        from src.services.sms_compliance import is_quiet_hours
        # 21:00 CST → quiet
        with self._patch_now(21, "America/Chicago"):
            assert is_quiet_hours("+18505550100") is True

    def test_850_quiet_hours_at_2pm_cst_are_open(self):
        """2pm CST is well inside the 8am-9pm window — gate must be open."""
        from src.services.sms_compliance import is_quiet_hours
        with self._patch_now(14, "America/Chicago"):
            assert is_quiet_hours("+18505550100") is False

    def test_813_still_eastern(self):
        """Tampa numbers (813) must still resolve to America/New_York."""
        from src.services.sms_compliance import _recipient_tz
        tz = _recipient_tz("+18135550100")
        assert str(tz) == "America/New_York"

    def test_unknown_area_code_defaults_to_eastern(self):
        """Conservative fallback: any non-FL area code → ET. Prevents 'no quiet
        hours at all' for junk numbers."""
        from src.services.sms_compliance import _recipient_tz
        tz = _recipient_tz("+12125550100")  # NYC, not in our FL list
        assert str(tz) == "America/New_York"


# ============================================================================
# V3 + V4: per-send audit log and message_type gate — unit tests
# ============================================================================


class TestSendSmsMessageTypeGateUnit:
    """V4: message_type gate — opt-in required for marketing, skipped for others."""

    _good_settings = {
        "telnyx_sms_enabled": False,  # dry-run so we don't need Telnyx creds
        "telnyx_sandbox": False,
    }

    def _dry_run_send(self, db, phone="+18135550100", body="Hi", **kwargs):
        with patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.settings") as mock_s:
            mock_s.telnyx_sms_enabled = False
            mock_s.telnyx_sandbox = False
            return send_sms(phone, body, db, **kwargs)

    def test_marketing_no_opt_in_returns_false(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.has_opted_in", return_value=False), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.add_to_dead_letter") as mock_dlq:
            result = send_sms("+18135550100", "Hello", db, message_type="marketing")
        assert result is False
        mock_dlq.assert_called_once()
        _, reason, _ = mock_dlq.call_args[0][:3]
        assert reason == "no_opt_in"

    def test_transactional_no_opt_in_proceeds_to_dry_run(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.has_opted_in", return_value=False), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.settings") as mock_s:
            mock_s.telnyx_sms_enabled = False
            mock_s.telnyx_sandbox = False
            result = send_sms("+18135550100", "Hello", db, message_type="transactional")
        assert result is True

    def test_opt_in_prompt_bypasses_opt_in_gate(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.has_opted_in", return_value=False) as mock_hoi, \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.settings") as mock_s:
            mock_s.telnyx_sms_enabled = False
            mock_s.telnyx_sandbox = False
            result = send_sms("+18135550100", "Reply YES", db, message_type="opt_in_prompt")
        assert result is True
        mock_hoi.assert_not_called()

    def test_invalid_message_type_defaults_to_marketing(self):
        """Unknown message_type falls back to 'marketing' and applies opt-in gate."""
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.has_opted_in", return_value=False), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.add_to_dead_letter") as mock_dlq:
            result = send_sms("+18135550100", "Hello", db, message_type="unicorn")
        assert result is False
        _, reason, _ = mock_dlq.call_args[0][:3]
        assert reason == "no_opt_in"

    def test_opt_out_checked_before_opt_in(self):
        """Gate order: opt-out fires first; opt-in gate never runs if suppressed."""
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=False), \
             patch("src.services.sms_compliance.has_opted_in") as mock_hoi, \
             patch("src.services.sms_compliance.add_to_dead_letter"):
            result = send_sms("+18135550100", "Hello", db, message_type="marketing")
        assert result is False
        mock_hoi.assert_not_called()

    def test_opt_in_checked_before_quiet_hours(self):
        """Gate order: opt-in (no consent) fires before quiet-hours check."""
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.has_opted_in", return_value=False), \
             patch("src.services.sms_compliance.is_quiet_hours") as mock_qh, \
             patch("src.services.sms_compliance.add_to_dead_letter"):
            result = send_sms("+18135550100", "Hello", db, message_type="marketing")
        assert result is False
        mock_qh.assert_not_called()


class TestSendSmsAuditLogUnit:
    """V3: SmsSendLog written at every send_sms exit point."""

    def _call(self, db, *, can=True, opted_in=True, quiet=False, enabled=False, **kwargs):
        with patch("src.services.sms_compliance.can_send", return_value=can), \
             patch("src.services.sms_compliance.has_opted_in", return_value=opted_in), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=quiet), \
             patch("src.services.sms_compliance.settings") as mock_s:
            mock_s.telnyx_sms_enabled = enabled
            mock_s.telnyx_sandbox = False
            return send_sms("+18135550100", "Hello", db, **kwargs)

    def test_suppressed_opt_out_writes_log(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.add_to_dead_letter"), \
             patch("src.services.sms_send_log.log_send") as mock_log:
            self._call(db, can=False)
        mock_log.assert_called_once()
        assert mock_log.call_args.kwargs["outcome"] == "suppressed"
        assert mock_log.call_args.kwargs["suppress_reason"] == "opt_out"

    def test_no_opt_in_writes_log(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.add_to_dead_letter"), \
             patch("src.services.sms_send_log.log_send") as mock_log:
            self._call(db, opted_in=False, message_type="marketing")
        mock_log.assert_called_once()
        assert mock_log.call_args.kwargs["outcome"] == "suppressed"
        assert mock_log.call_args.kwargs["suppress_reason"] == "no_opt_in"

    def test_quiet_hours_writes_log(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.add_to_dead_letter"), \
             patch("src.services.sms_send_log.log_send") as mock_log:
            self._call(db, quiet=True, message_type="transactional")
        mock_log.assert_called_once()
        assert mock_log.call_args.kwargs["outcome"] == "suppressed"
        assert mock_log.call_args.kwargs["suppress_reason"] == "quiet_hours"

    def test_dry_run_writes_log(self):
        db = MagicMock()
        with patch("src.services.sms_send_log.log_send") as mock_log:
            self._call(db, enabled=False, message_type="transactional")
        mock_log.assert_called_once()
        assert mock_log.call_args.kwargs["outcome"] == "dry_run"

    def test_exactly_one_log_row_per_send(self):
        """Each send_sms call writes exactly one SmsSendLog, never more."""
        db = MagicMock()
        with patch("src.services.sms_send_log.log_send") as mock_log:
            self._call(db, message_type="transactional")
        assert mock_log.call_count == 1


# ============================================================================
# V4: message_type gate — integration tests (real Postgres)
# ============================================================================


class TestMessageTypeGateIntegration:
    """Marketing SMS blocked when SmsOptIn row is absent; transactional passes."""

    def _dry_run_send(self, db, phone, body, **kwargs):
        with patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.settings") as mock_s:
            mock_s.telnyx_sms_enabled = False
            mock_s.telnyx_sandbox = False
            return send_sms(phone, body, db, **kwargs)

    def test_marketing_blocked_without_opt_in_row(self, fresh_db):
        result = self._dry_run_send(
            fresh_db, "+18135550200", "Buy now", message_type="marketing"
        )
        assert result is False
        row = fresh_db.execute(
            select(SmsDeadLetter).where(SmsDeadLetter.phone == "+18135550200")
        ).scalar_one_or_none()
        assert row is not None
        assert row.reason == "no_opt_in"

    def test_marketing_sends_with_opt_in_row(self, fresh_db):
        phone = "+18135550201"
        record_opt_in(phone, "YES", "double_opt_in", fresh_db)
        result = self._dry_run_send(fresh_db, phone, "Buy now", message_type="marketing")
        assert result is True

    def test_transactional_sends_without_opt_in_row(self, fresh_db):
        result = self._dry_run_send(
            fresh_db, "+18135550202", "Your receipt", message_type="transactional"
        )
        assert result is True

    def test_opt_in_prompt_sends_without_opt_in_row(self, fresh_db):
        result = self._dry_run_send(
            fresh_db, "+18135550203", "Reply YES", message_type="opt_in_prompt"
        )
        assert result is True


# ============================================================================
# V3: SmsSendLog integration — real Postgres
# ============================================================================


# ============================================================================
# V5: opt-in sentinel — unit tests for handle_opt_in_reply and send_opt_in_prompt
# ============================================================================

import fakeredis as _fakeredis


class TestHandleOptInReplyV5Unit:
    """handle_opt_in_reply gates on the Redis sentinel (V5)."""

    @pytest.fixture
    def fake_redis(self):
        return _fakeredis.FakeRedis(decode_responses=True)

    @pytest.fixture(autouse=True)
    def inject_redis(self, fake_redis):
        with patch("src.core.redis_client._get_client", return_value=fake_redis):
            yield

    def test_returns_none_for_non_opt_in_keyword(self):
        db = MagicMock()
        assert handle_opt_in_reply("+18135550100", "HELP", db) is None
        assert handle_opt_in_reply("+18135550100", "STOP", db) is None
        assert handle_opt_in_reply("+18135550100", "", db) is None

    def test_yes_with_no_sentinel_returns_none(self):
        db = MagicMock()
        result = handle_opt_in_reply("+18135550100", "YES", db)
        assert result is None
        db.add.assert_not_called()

    def test_yes_with_sentinel_returns_twiml(self, fake_redis):
        from src.services.opt_in_sentinel import mark_pending
        mark_pending("+18135550100")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        result = handle_opt_in_reply("+18135550100", "YES", db)
        assert result is not None
        assert "confirmed" in result.lower()

    def test_yes_with_sentinel_records_opt_in(self, fake_redis):
        from src.services.opt_in_sentinel import mark_pending
        mark_pending("+18135550100")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        handle_opt_in_reply("+18135550100", "YES", db)
        db.add.assert_called_once()

    def test_sentinel_consumed_after_yes(self, fake_redis):
        from src.services.opt_in_sentinel import mark_pending, _key
        mark_pending("+18135550100")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        handle_opt_in_reply("+18135550100", "YES", db)
        assert fake_redis.get(_key("+18135550100")) is None

    def test_double_yes_second_returns_none(self, fake_redis):
        from src.services.opt_in_sentinel import mark_pending
        mark_pending("+18135550100")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        handle_opt_in_reply("+18135550100", "YES", db)
        # second YES — sentinel already consumed
        db2 = MagicMock()
        result = handle_opt_in_reply("+18135550100", "YES", db2)
        assert result is None

    @pytest.mark.parametrize("keyword", ["YES", "START", "JOIN", "SUBSCRIBE", "UNSTOP"])
    def test_all_opt_in_keywords_accepted(self, keyword, fake_redis):
        from src.services.opt_in_sentinel import mark_pending
        mark_pending("+18135550100")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        result = handle_opt_in_reply("+18135550100", keyword, db)
        assert result is not None

    def test_redis_unavailable_yes_returns_none(self):
        db = MagicMock()
        with patch("src.core.redis_client._get_client", return_value=None):
            result = handle_opt_in_reply("+18135550100", "YES", db)
        assert result is None
        db.add.assert_not_called()


class TestSendOptInPromptV5Unit:
    """send_opt_in_prompt sets the sentinel before sending (V5)."""

    @pytest.fixture
    def fake_redis(self):
        return _fakeredis.FakeRedis(decode_responses=True)

    @pytest.fixture(autouse=True)
    def inject_redis(self, fake_redis):
        with patch("src.core.redis_client._get_client", return_value=fake_redis):
            yield

    def _dry_run(self, db, phone="+18135550100"):
        with patch("src.services.sms_compliance.has_opted_in", return_value=False), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.settings") as ms:
            ms.telnyx_sms_enabled = False
            ms.telnyx_sandbox = False
            return send_opt_in_prompt(phone, db)

    def test_marks_sentinel_before_send(self, fake_redis):
        from src.services.opt_in_sentinel import _key
        db = MagicMock()
        self._dry_run(db)
        assert fake_redis.get(_key("+18135550100")) == "1"

    def test_clears_sentinel_on_send_failure(self, fake_redis):
        from src.services.opt_in_sentinel import _key
        db = MagicMock()
        with patch("src.services.sms_compliance.has_opted_in", return_value=False), \
             patch("src.services.sms_compliance.can_send", return_value=False), \
             patch("src.services.sms_compliance.add_to_dead_letter"):
            send_opt_in_prompt("+18135550100", db)
        assert fake_redis.get(_key("+18135550100")) is None

    def test_already_opted_in_skips_sentinel(self, fake_redis):
        from src.services.opt_in_sentinel import _key
        db = MagicMock()
        with patch("src.services.sms_compliance.has_opted_in", return_value=True):
            send_opt_in_prompt("+18135550100", db)
        assert fake_redis.get(_key("+18135550100")) is None


class TestSmsSendLogIntegration:
    def _dry_run_send(self, db, phone, body, **kwargs):
        with patch("src.services.sms_compliance.is_quiet_hours", return_value=False), \
             patch("src.services.sms_compliance.settings") as mock_s:
            mock_s.telnyx_sms_enabled = False
            mock_s.telnyx_sandbox = False
            return send_sms(phone, body, db, **kwargs)

    def test_dry_run_writes_one_log_row(self, fresh_db):
        phone = "+18135550210"
        record_opt_in(phone, "YES", "double_opt_in", fresh_db)
        self._dry_run_send(fresh_db, phone, "Hello", message_type="transactional",
                           task_type="test_task")
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == phone)
        ).scalar_one_or_none()
        assert row is not None
        assert row.outcome == "dry_run"
        assert row.message_type == "transactional"
        assert row.task_type == "test_task"
        assert row.suppress_reason is None

    def test_suppressed_opt_out_writes_log_row(self, fresh_db):
        phone = "+18135550211"
        record_opt_out(phone, "STOP", "inbound_sms", fresh_db)
        self._dry_run_send(fresh_db, phone, "Hello", message_type="transactional")
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == phone)
        ).scalar_one_or_none()
        assert row is not None
        assert row.outcome == "suppressed"
        assert row.suppress_reason == "opt_out"

    def test_no_opt_in_suppression_writes_log_row(self, fresh_db):
        phone = "+18135550212"
        # No SmsOptIn row for this phone
        self._dry_run_send(fresh_db, phone, "Marketing msg", message_type="marketing")
        row = fresh_db.execute(
            select(SmsSendLog).where(SmsSendLog.phone == phone)
        ).scalar_one_or_none()
        assert row is not None
        assert row.outcome == "suppressed"
        assert row.suppress_reason == "no_opt_in"
        assert row.message_type == "marketing"
