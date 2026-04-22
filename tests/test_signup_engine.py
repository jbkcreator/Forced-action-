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

    def test_creates_subscriber_with_free_tier(self):
        db = self._make_db()
        sub = create_free_account("+18135550100", "missed_call", db)
        db.add.assert_called_once()
        added = db.add.call_args[0][0]
        assert isinstance(added, Subscriber)
        assert added.tier == "free"
        assert added.status == "active"

    def test_sets_county_id(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db, county_id="pinellas")
        added = db.add.call_args[0][0]
        assert added.county_id == "pinellas"

    def test_default_county_is_hillsborough(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        added = db.add.call_args[0][0]
        assert added.county_id == "hillsborough"

    def test_sets_name_when_provided(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db, name="John Doe")
        added = db.add.call_args[0][0]
        assert added.name == "John Doe"

    def test_name_none_by_default(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        added = db.add.call_args[0][0]
        assert added.name is None

    def test_event_feed_uuid_generated(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        added = db.add.call_args[0][0]
        assert added.event_feed_uuid is not None
        assert len(added.event_feed_uuid) == 36  # UUID4 string

    def test_stripe_placeholder_set(self):
        db = self._make_db()
        create_free_account("+18135550100", "missed_call", db)
        added = db.add.call_args[0][0]
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
