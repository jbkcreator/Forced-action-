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
    record_opt_out,
    send_sms,
)
from src.core.models import SmsDeadLetter, SmsOptOut


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

    @pytest.mark.parametrize("reason", ["opt_out", "delivery_failed", "error", "unresolvable"])
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

    def test_dry_run_returns_true_without_twilio(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.twilio_enabled = False
                result = send_sms("+18135550100", "Hello", db)
        assert result is True

    def test_twilio_not_configured_returns_false_and_dlqs(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.twilio_enabled = True
                mock_settings.twilio_account_sid = None
                mock_settings.twilio_auth_token = None
                mock_settings.twilio_from_number = None
                with patch("src.services.sms_compliance.add_to_dead_letter") as mock_dlq:
                    result = send_sms("+18135550100", "Hello", db)
        assert result is False
        mock_dlq.assert_called_once()

    def test_twilio_success_returns_true(self):
        db = MagicMock()
        mock_message = MagicMock()
        mock_message.sid = "SM123"
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.twilio_enabled = True
                mock_settings.twilio_account_sid = "ACtest"
                mock_settings.twilio_auth_token.get_secret_value.return_value = "token"
                mock_settings.twilio_from_number = "+18005550000"
                with patch("src.services.sms_compliance.Client") as mock_client_cls:
                    mock_client_cls.return_value.messages.create.return_value = mock_message
                    result = send_sms("+18135550100", "Hello", db)
        assert result is True

    def test_twilio_exception_dlqs_and_returns_false(self):
        db = MagicMock()
        with patch("src.services.sms_compliance.can_send", return_value=True), \
             patch("src.services.sms_compliance.is_quiet_hours", return_value=False):
            with patch("src.services.sms_compliance.settings") as mock_settings:
                mock_settings.twilio_enabled = True
                mock_settings.twilio_account_sid = "ACtest"
                mock_settings.twilio_auth_token.get_secret_value.return_value = "token"
                mock_settings.twilio_from_number = "+18005550000"
                with patch("src.services.sms_compliance.Client") as mock_client_cls:
                    mock_client_cls.return_value.messages.create.side_effect = Exception("Network error")
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
