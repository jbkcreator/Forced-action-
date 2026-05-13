"""fa017 — business_events.log_business_event whitelist + persistence."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestBusinessEventsWhitelist:
    def test_allowed_event_writes_row(self):
        from src.services.business_events import log_business_event
        with patch("src.services.business_events.log_webhook_event") as mock_log:
            log_business_event(
                "SIGNUP_COMPLETED",
                subscriber_id=42,
                payload={"channel": "email"},
                source="business",
            )
        assert mock_log.called
        kw = mock_log.call_args.kwargs
        assert kw["source"] == "business"
        assert kw["event_type"] == "SIGNUP_COMPLETED"
        assert kw["status"] == "processed"
        assert kw["subscriber_id"] == 42

    def test_unknown_event_dropped_no_write(self, caplog):
        from src.services.business_events import log_business_event
        with patch("src.services.business_events.log_webhook_event") as mock_log:
            log_business_event(
                "FAKE_EVENT_NAME",
                subscriber_id=1,
                payload={"x": 1},
            )
        assert not mock_log.called
        assert any("unknown event_type" in rec.message for rec in caplog.records) \
            or any("FAKE_EVENT_NAME" in rec.message for rec in caplog.records)

    def test_failure_in_log_webhook_event_is_swallowed(self):
        """log_business_event must never raise — webhook_log handles its own
        failures, but if it somehow leaks one out, we still swallow."""
        from src.services.business_events import log_business_event
        with patch("src.services.business_events.log_webhook_event",
                   side_effect=RuntimeError("boom")):
            # Should NOT raise
            log_business_event("LANDING_PAGE_VIEWED", payload={"x": 1})

    def test_frontend_source_value_passed_through(self):
        from src.services.business_events import log_business_event
        with patch("src.services.business_events.log_webhook_event") as mock_log:
            log_business_event(
                "LANDING_PAGE_VIEWED",
                payload={"signupSource": "dbpr_email"},
                source="frontend",
            )
        assert mock_log.call_args.kwargs["source"] == "frontend"


class TestBusinessEventsConstant:
    def test_all_spec_events_in_allowlist(self):
        """The Phase 2B spec lists 13 required event types — make sure none
        of them got accidentally dropped or renamed."""
        from src.services.business_events import BUSINESS_EVENT_TYPES
        required = {
            "LANDING_PAGE_VIEWED", "SIGNUP_STARTED", "SIGNUP_COMPLETED",
            "SIGNUP_SOURCE_ATTRIBUTED", "PROOF_MOMENT_VIEWED",
            "LEAD_UNLOCK_CLICKED", "PAYMENT_STARTED", "PAYMENT_SUCCEEDED",
            "PREMIUM_PURCHASE_COMPLETED", "CARD_SAVED",
            "ACCELERATED_WALLET_ELIGIBLE", "WALLET_OFFER_SENT",
            "WALLET_ACTIVATED",
        }
        missing = required - BUSINESS_EVENT_TYPES
        assert not missing, f"spec events missing from allow-list: {missing}"
