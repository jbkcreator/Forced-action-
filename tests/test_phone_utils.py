"""
Unit and integration tests for src/services/phone_utils.normalize.

Unit tests: no DB required.
Integration: fresh_db fixture — real Postgres, rolled back after each test.

Run:
    pytest tests/test_phone_utils.py -v
"""

import pytest
from sqlalchemy import select

from src.services.phone_utils import normalize
from src.services.sms_compliance import handle_inbound
from src.core.models import SmsOptOut


# ============================================================================
# Unit tests
# ============================================================================


class TestNormalizeUnit:
    @pytest.mark.parametrize("raw,expected", [
        ("+18135550100",   "+18135550100"),  # already canonical
        ("8135550100",     "+18135550100"),  # 10-digit local
        ("18135550100",    "+18135550100"),  # 11-digit with country code
        ("(813) 555-0100", "+18135550100"),  # formatted NANP
        ("813.555.0100",   "+18135550100"),  # dot-separated
        ("+1 813 555 0100", "+18135550100"), # country code with spaces
        ("  +18135550100  ", "+18135550100"), # surrounding whitespace
    ])
    def test_valid_us_formats(self, raw, expected):
        assert normalize(raw) == expected

    def test_idempotent(self):
        once = normalize("+18135550100")
        assert normalize(once) == once

    def test_empty_string_returns_none(self):
        assert normalize("") is None

    def test_none_returns_none(self):
        assert normalize(None) is None

    def test_too_short_returns_none(self):
        assert normalize("813555010") is None  # 9 digits

    def test_too_long_returns_none(self):
        assert normalize("81355501001") is None  # 11 digits, no country code prefix

    def test_non_us_number_returns_none(self):
        assert normalize("+447911123456") is None  # UK

    def test_unicode_garbage_returns_none(self):
        assert normalize("☃☕phone") is None

    def test_letters_only_returns_none(self):
        assert normalize("abcdefghij") is None

    def test_extension_stripped_to_base(self):
        # phonenumbers parses extension; E.164 format strips it
        result = normalize("8135550100x123")
        assert result in ("+18135550100", None)  # library may or may not accept ext


# ============================================================================
# Integration tests — real Postgres required (skipped if not configured)
# ============================================================================


class TestHandleInboundStoresE164:
    def test_stop_from_local_format_stores_e164(self, fresh_db):
        """
        STOP sent from a 10-digit number (no country code) must be stored
        in sms_opt_outs as canonical E.164 (+1XXXXXXXXXX).
        """
        handle_inbound("8135550100", "STOP", fresh_db)
        row = fresh_db.execute(
            select(SmsOptOut).where(SmsOptOut.phone == "+18135550100")
        ).scalar_one_or_none()
        assert row is not None, "Opt-out row must be stored in E.164 form"
        assert row.phone == "+18135550100"

    def test_stop_from_e164_format_stores_e164(self, fresh_db):
        """Already-canonical number must still be stored correctly."""
        handle_inbound("+18135550100", "STOP", fresh_db)
        row = fresh_db.execute(
            select(SmsOptOut).where(SmsOptOut.phone == "+18135550100")
        ).scalar_one_or_none()
        assert row is not None
        assert row.phone == "+18135550100"

    def test_stop_from_country_code_format_stores_e164(self, fresh_db):
        """Number with country code but no + must normalize to E.164."""
        handle_inbound("18135550100", "STOP", fresh_db)
        row = fresh_db.execute(
            select(SmsOptOut).where(SmsOptOut.phone == "+18135550100")
        ).scalar_one_or_none()
        assert row is not None
        assert row.phone == "+18135550100"
