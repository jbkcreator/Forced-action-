"""
Signup Engine tests — Item 21.

Unit tests: mock DB. Integration: fresh_db (Postgres).

Run:
    pytest tests/test_signup_engine.py -v
    pytest tests/test_signup_engine.py -v -k "unit"
"""
from unittest.mock import MagicMock, patch

import pytest

from src.services.signup_engine import create_free_account, handle_missed_call
from src.core.models import Subscriber


# ============================================================================
# Unit tests — create_free_account
# ============================================================================


class TestCreateFreeAccountUnit:
    def _make_db(self):
        db = MagicMock()
        db.flush.return_value = None
        return db

    @staticmethod
    def _first_subscriber(db_mock):
        """The first `db.add(...)` call is always the Subscriber. fa017+ also
        adds WebhookEvent rows from the business-event audit log, so
        `db.add.call_args` (the LAST call) no longer points at the Subscriber.
        """
        for call in db_mock.add.call_args_list:
            obj = call[0][0]
            if isinstance(obj, Subscriber):
                return obj
        raise AssertionError("No Subscriber was added to the session")

    def test_creates_subscriber_with_free_tier(self):
        db = self._make_db()
        sub = create_free_account("+18135550100", "missed_call", db)
        assert db.add.call_count >= 1
        added = self._first_subscriber(db)
        assert isinstance(added, Subscriber)
        assert added.tier == "free"
        assert added.status == "active"

    def test_sets_county_id(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db, county_id="pinellas")
        assert self._first_subscriber(db).county_id == "pinellas"

    def test_default_county_is_hillsborough(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        assert self._first_subscriber(db).county_id == "hillsborough"

    def test_sets_name_when_provided(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db, name="John Doe")
        assert self._first_subscriber(db).name == "John Doe"

    def test_name_none_by_default(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        assert self._first_subscriber(db).name is None

    def test_event_feed_uuid_generated(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        added = self._first_subscriber(db)
        assert added.event_feed_uuid is not None
        assert len(added.event_feed_uuid) == 36  # UUID4 string

    def test_stripe_placeholder_set(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        added = self._first_subscriber(db)
        assert added.stripe_customer_id.startswith("free_")

    def test_referral_code_processed_when_provided(self):
        db = self._make_db()
        # process_signup is imported lazily inside create_free_account; patch at source
        with patch("src.services.referral_engine.process_signup") as mock_signup:
            mock_signup.return_value = None
            sub = create_free_account("+18135550100", "missed_call", db, referral_code="ABCD1234")
        # No exception raised — referral processing attempted
        assert sub.tier == "free"

    def test_db_flush_called(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        db.flush.assert_called_once()


# ============================================================================
# Unit tests — handle_missed_call
# ============================================================================


class TestHandleMissedCallUnit:
    def test_returns_twiml_string(self):
        db = MagicMock()
        sub = MagicMock()
        sub.id = 1
        sub.event_feed_uuid = "test-uuid"

        with patch("src.services.signup_engine.create_free_account", return_value=sub), \
             patch("src.services.signup_engine.can_send", return_value=False):
            result = handle_missed_call("+18135550100", db)

        assert result.startswith("<?xml")
        assert "<Response>" in result
        assert "<Say" in result
        assert "<Hangup/>" in result

    def test_sends_welcome_sms_when_can_send(self):
        db = MagicMock()
        sub = MagicMock()
        sub.id = 1
        sub.event_feed_uuid = "test-uuid"

        with patch("src.services.signup_engine.create_free_account", return_value=sub), \
             patch("src.services.signup_engine.can_send", return_value=True), \
             patch("src.services.signup_engine.send_sms") as mock_sms, \
             patch("src.services.signup_engine.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.com"
            handle_missed_call("+18135550100", db)

        mock_sms.assert_called_once()
        call_kwargs = mock_sms.call_args
        assert "+18135550100" in call_kwargs[1].get("to", call_kwargs[0][0] if call_kwargs[0] else "")

    def test_skips_sms_when_suppressed(self):
        db = MagicMock()
        sub = MagicMock()
        sub.id = 1
        sub.event_feed_uuid = "test-uuid"

        with patch("src.services.signup_engine.create_free_account", return_value=sub), \
             patch("src.services.signup_engine.can_send", return_value=False), \
             patch("src.services.signup_engine.send_sms") as mock_sms:
            handle_missed_call("+18135550100", db)

        mock_sms.assert_not_called()

    def test_sms_body_contains_stop_opt_out(self):
        db = MagicMock()
        sub = MagicMock()
        sub.id = 1
        sub.event_feed_uuid = "test-uuid"
        captured_body = {}

        def capture_sms(to, body, db, **kwargs):
            captured_body["body"] = body
            return True

        with patch("src.services.signup_engine.create_free_account", return_value=sub), \
             patch("src.services.signup_engine.can_send", return_value=True), \
             patch("src.services.signup_engine.send_sms", side_effect=capture_sms), \
             patch("src.services.signup_engine.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.com"
            handle_missed_call("+18135550100", db)

        assert "STOP" in captured_body.get("body", "")


# ============================================================================
# Integration test stubs (Postgres)
# ============================================================================


class TestCreateFreeAccountIntegration:
    def test_creates_row_in_db(self, fresh_db):
        from sqlalchemy import select
        sub = create_free_account("+18135550199", "missed_call", fresh_db)
        fresh_db.flush()
        assert sub.id is not None
        row = fresh_db.execute(
            select(Subscriber).where(Subscriber.id == sub.id)
        ).scalar_one_or_none()
        assert row is not None
        assert row.tier == "free"
        assert row.status == "active"


# ============================================================================
# create_free_account_by_email — phone + SMS opt-in (fa016 Accelerated Wallet Push)
# ============================================================================


class TestCreateFreeAccountByEmailPhone:
    def test_normalize_phone_us_10_digits(self):
        from src.services.signup_engine import _normalize_phone
        assert _normalize_phone("(813) 555-0123") == "+18135550123"
        assert _normalize_phone("8135550123") == "+18135550123"
        assert _normalize_phone("1-813-555-0123") == "+18135550123"

    def test_normalize_phone_e164(self):
        from src.services.signup_engine import _normalize_phone
        assert _normalize_phone("+918135550123") == "+918135550123"

    def test_normalize_phone_empty_returns_none(self):
        from src.services.signup_engine import _normalize_phone
        assert _normalize_phone("") is None
        assert _normalize_phone(None) is None
        assert _normalize_phone("   ") is None

    def test_normalize_phone_garbage_returns_none(self):
        from src.services.signup_engine import _normalize_phone
        assert _normalize_phone("abc") is None
        assert _normalize_phone("12") is None  # too few digits

    def test_creates_subscriber_with_phone_and_optin(self, fresh_db):
        from sqlalchemy import select
        from src.services.signup_engine import create_free_account_by_email
        from src.core.models import SmsOptIn
        sub = create_free_account_by_email(
            email="phonetest1@example.com",
            db=fresh_db,
            phone="(813) 555-0123",
            sms_consent=True,
        )
        fresh_db.flush()
        assert sub.phone == "+18135550123"
        opt = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.subscriber_id == sub.id)
        ).scalar_one_or_none()
        assert opt is not None
        assert opt.source == "widget"
        assert opt.phone == "+18135550123"

    def test_phone_without_consent_skips_optin(self, fresh_db):
        from sqlalchemy import select
        from src.services.signup_engine import create_free_account_by_email
        from src.core.models import SmsOptIn
        sub = create_free_account_by_email(
            email="phonetest2@example.com",
            db=fresh_db,
            phone="8135550124",
            sms_consent=False,
        )
        fresh_db.flush()
        # Phone is still stored (for transactional SMS) but no marketing opt-in.
        assert sub.phone == "+18135550124"
        opt = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.subscriber_id == sub.id)
        ).scalar_one_or_none()
        assert opt is None

    def test_no_phone_no_optin(self, fresh_db):
        from sqlalchemy import select
        from src.services.signup_engine import create_free_account_by_email
        from src.core.models import SmsOptIn
        sub = create_free_account_by_email(
            email="phonetest3@example.com",
            db=fresh_db,
        )
        fresh_db.flush()
        assert sub.phone is None
        opt = fresh_db.execute(
            select(SmsOptIn).where(SmsOptIn.subscriber_id == sub.id)
        ).scalar_one_or_none()
        assert opt is None


# ============================================================================
# fa017 — signup_source attribution
# ============================================================================


class TestSignupSourceAttribution:
    def test_coerce_valid_source(self):
        from src.services.signup_engine import _coerce_signup_source
        assert _coerce_signup_source("dbpr_email") == "dbpr_email"
        assert _coerce_signup_source("MISSED_CALL") == "missed_call"
        assert _coerce_signup_source("  referral ") == "referral"

    def test_coerce_unknown_falls_back_to_unknown(self):
        from src.services.signup_engine import _coerce_signup_source
        # Not in allow-list → 'unknown'
        assert _coerce_signup_source("facebook_ad") == "unknown"
        assert _coerce_signup_source("12345") == "unknown"

    def test_coerce_empty_uses_default(self):
        from src.services.signup_engine import _coerce_signup_source
        assert _coerce_signup_source(None) == "direct"
        assert _coerce_signup_source("") == "direct"
        assert _coerce_signup_source(None, default="landing_page") == "landing_page"

    def test_email_signup_persists_signup_source(self, fresh_db):
        from src.services.signup_engine import create_free_account_by_email
        sub = create_free_account_by_email(
            email="source1@example.com",
            db=fresh_db,
            signup_source="dbpr_email",
            utm_source="dbpr",
            utm_campaign="q2",
            campaign_id="DBPR-2026-Q2",
        )
        fresh_db.flush()
        assert sub.signup_source == "dbpr_email"
        assert sub.utm_source == "dbpr"
        assert sub.utm_campaign == "q2"
        assert sub.campaign_id == "DBPR-2026-Q2"

    def test_email_signup_referral_code_implies_referral_source(self, fresh_db):
        from src.services.signup_engine import create_free_account_by_email
        # Pre-seed a referrer
        ref_owner = create_free_account_by_email(
            email="referrer@example.com", db=fresh_db,
        )
        fresh_db.flush()
        sub = create_free_account_by_email(
            email="referee@example.com",
            db=fresh_db,
            referral_code=ref_owner.referral_code,
            # NOT passing signup_source — let referral_code drive it
        )
        fresh_db.flush()
        assert sub.signup_source == "referral"

    def test_email_signup_default_is_landing_page(self, fresh_db):
        from src.services.signup_engine import create_free_account_by_email
        sub = create_free_account_by_email(
            email="plain@example.com",
            db=fresh_db,
        )
        fresh_db.flush()
        # No source passed, no referral_code → default 'landing_page' (the
        # email flow knows the user came through the FE form).
        assert sub.signup_source == "landing_page"

    def test_revisit_does_not_clobber_existing_real_source(self, fresh_db):
        from src.services.signup_engine import create_free_account_by_email
        a = create_free_account_by_email(
            email="firsttouch@example.com", db=fresh_db, signup_source="dbpr_email",
        )
        fresh_db.flush()
        assert a.signup_source == "dbpr_email"
        # Second visit on a different channel — should keep dbpr_email.
        b = create_free_account_by_email(
            email="firsttouch@example.com", db=fresh_db, signup_source="cora_sms",
        )
        fresh_db.flush()
        assert b.id == a.id
        assert b.signup_source == "dbpr_email"  # first-touch wins

    def test_revisit_upgrades_unknown_source(self, fresh_db):
        from src.services.signup_engine import create_free_account_by_email
        from sqlalchemy import update
        from src.core.models import Subscriber
        a = create_free_account_by_email(email="upgradable@example.com", db=fresh_db)
        fresh_db.flush()
        # Simulate a row created before fa017 (or from admin) with unknown source
        fresh_db.execute(update(Subscriber).where(Subscriber.id == a.id).values(signup_source="unknown"))
        fresh_db.flush()
        b = create_free_account_by_email(
            email="upgradable@example.com", db=fresh_db, signup_source="referral",
        )
        fresh_db.flush()
        assert b.signup_source == "referral"

    def test_phone_signup_persists_missed_call_source(self, fresh_db):
        from src.services.signup_engine import create_free_account
        sub = create_free_account(
            phone="+18135550100", source="missed_call", db=fresh_db,
        )
        fresh_db.flush()
        assert sub.signup_source == "missed_call"
