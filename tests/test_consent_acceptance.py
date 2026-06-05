"""
Tests for T&C / TCPA Consent Acceptance on waitlist signup.

Uses FastAPI TestClient with a mock DB session (no real Postgres needed).
Tests cover:
  - POST /api/waitlist rejects without T&C acceptance
  - POST /api/waitlist succeeds with T&C only (no TCPA)
  - POST /api/waitlist succeeds + writes ConsentAcceptance + SmsOptIn when TCPA checked
  - POST /api/waitlist with invalid consent payload
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from src.api.main import app
from src.core.models import (
    ConsentAcceptance,
    County,
    ExpansionCandidate,
    SmsOptIn,
    WaitlistEntry,
    ZipTerritory,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _county(county_id="pinellas", display_name="Pinellas County"):
    c = MagicMock(spec=County)
    c.county_id = county_id
    c.display_name = display_name
    return c


def _candidate(status="queued", county_id="pinellas"):
    ec = MagicMock(spec=ExpansionCandidate)
    ec.county_id = county_id
    ec.status = status
    return ec


def _client():
    return TestClient(app)


def _with_db(db_override):
    """Context manager that wires db_override into FastAPI dependency_overrides."""
    from contextlib import contextmanager
    from src.api.main import get_db

    @contextmanager
    def _ctx():
        app.dependency_overrides[get_db] = lambda: db_override
        try:
            yield _client()
        finally:
            app.dependency_overrides.pop(get_db, None)

    return _ctx()


def _db_for_sold_out(any_taken=5, zip_count=40, opt_in_exists=False):
    """Mock DB returning a sold_out county state."""
    db = MagicMock()
    db.execute.side_effect = [
        MagicMock(**{"scalar_one_or_none.return_value": _county("hillsborough", "Hillsborough County")}),
        MagicMock(**{"scalar_one_or_none.return_value": None}),        # no candidate
        MagicMock(**{"scalar_one.return_value": any_taken}),           # taken ZIPs
        MagicMock(**{"scalar_one.return_value": zip_count}),           # total ZIPs
        MagicMock(**{"all.return_value": []}),                         # vertical counts
        MagicMock(**{"scalar_one_or_none.return_value":                # SmsOptIn lookup
                     (MagicMock() if opt_in_exists else None)}),
    ]
    return db


def _post(payload, db_override):
    """POST /api/waitlist with the given payload and mock DB."""
    from src.api.main import get_db
    app.dependency_overrides[get_db] = lambda: db_override
    try:
        client = _client()
        return client.post("/api/waitlist", json=payload)
    finally:
        app.dependency_overrides.pop(get_db, None)


def _default_payload(**overrides):
    """Standard valid waitlist payload with T&C accepted."""
    payload = {
        "zip_code": "33601",
        "vertical": "roofing",
        "county_id": "hillsborough",
        "name": "Jane Doe",
        "email": "jane@consenttest.com",
        "consent_acceptance": {
            "terms_accepted": True,
            "terms_version": "2026.06",
            "privacy_version": "2026.06",
            "accepted_text_hash": "a1b2c3d4000000000000000000000000",
            "modal_opened_at": "2026-06-04T10:30:00Z",
            "modal_scrolled_to_end_at": "2026-06-04T10:30:45Z",
        },
    }
    payload.update(overrides)
    return payload


# ===========================================================================
# Consent Acceptance Tests
# ===========================================================================

class TestConsentValidation:

    def test_terms_not_accepted_returns_422(self):
        """T&C checkbox not checked → 422 terms_not_accepted."""
        db = _db_for_sold_out()
        payload = _default_payload()
        payload["consent_acceptance"]["terms_accepted"] = False
        resp = _post(payload, db)
        assert resp.status_code == 422
        detail = resp.json().get("detail", {})
        assert detail.get("error") == "terms_not_accepted"

    def test_consent_acceptance_missing_returns_422(self):
        """No consent_acceptance field at all → 422."""
        db = _db_for_sold_out()
        payload = _default_payload()
        del payload["consent_acceptance"]
        resp = _post(payload, db)
        assert resp.status_code == 422

    def test_terms_accepted_without_tcpa_succeeds(self):
        """T&C accepted, TCPA not checked → 201, no SmsOptIn row."""
        db = _db_for_sold_out()
        payload = _default_payload()

        # Ensure TCPA-related flags are absent/False
        ca = payload["consent_acceptance"]
        ca["tcpa_accepted"] = False
        ca.pop("tcpa_consent_text", None)
        ca.pop("tcpa_consent_version", None)

        resp = _post(payload, db)
        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body.get("status") == "added"

        # Verify ConsentAcceptance was added to db
        added_ca = None
        for call_args in db.add.call_args_list:
            args, _ = call_args
            if isinstance(args[0], ConsentAcceptance):
                added_ca = args[0]
                break
        assert added_ca is not None, "ConsentAcceptance row should have been created"
        assert added_ca.email == "jane@consenttest.com"
        assert added_ca.terms_version == "2026.06"
        assert added_ca.source_flow == "waitlist"
        # TCPA fields should NOT be set
        assert added_ca.tcpa_consent_text is None
        assert added_ca.tcpa_checked_at is None
        assert added_ca.not_condition_of_purchase_ack is None

        # Verify no SmsOptIn added
        sms_opt_ins = [
            a[0] for a, _ in db.add.call_args_list
            if isinstance(a[0], SmsOptIn)
        ]
        assert len(sms_opt_ins) == 0, "SmsOptIn should NOT be created without TCPA consent"

    def test_terms_and_tcpa_accepted_succeeds(self):
        """T&C accepted + TCPA checked → 201, ConsentAcceptance + SmsOptIn written."""
        db = _db_for_sold_out()
        payload = _default_payload(
            phone="+18135551234",
            sms_opt_in=True,
        )
        payload["consent_acceptance"]["tcpa_accepted"] = True
        payload["consent_acceptance"]["tcpa_consent_text"] = (
            "I agree to receive recurring automated marketing text messages "
            "from Forced Action at the phone number provided. Consent is not "
            "a condition of purchase."
        )
        payload["consent_acceptance"]["tcpa_consent_version"] = "2026.06"

        resp = _post(payload, db)
        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body.get("status") == "added"

        # Verify ConsentAcceptance was created with TCPA fields
        added_ca = None
        for call_args in db.add.call_args_list:
            args, _ = call_args
            if isinstance(args[0], ConsentAcceptance):
                added_ca = args[0]
                break
        assert added_ca is not None, "ConsentAcceptance row should have been created"
        assert added_ca.tcpa_consent_text is not None
        assert added_ca.tcpa_consent_version == "2026.06"
        assert added_ca.consent_scope == "marketing"
        assert added_ca.not_condition_of_purchase_ack is True

        # Verify SmsOptIn was created
        sms_opt_ins = [
            a[0] for a, _ in db.add.call_args_list
            if isinstance(a[0], SmsOptIn)
        ]
        assert len(sms_opt_ins) == 1, "SmsOptIn should be created when TCPA is checked"
        assert sms_opt_ins[0].phone == "+18135551234"

    def test_waitlist_succeeds_with_phone_but_no_tcpa(self):
        """Phone provided, TCPA not checked → 201, no SmsOptIn, no TCPA fields in ConsentAcceptance."""
        db = _db_for_sold_out()
        payload = _default_payload(
            phone="+18135551234",
            sms_opt_in=False,
        )
        payload["consent_acceptance"]["tcpa_accepted"] = False

        resp = _post(payload, db)
        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"

        # Verify no SmsOptIn created (phone was provided but TCPA not checked)
        sms_opt_ins = [
            a[0] for a, _ in db.add.call_args_list
            if isinstance(a[0], SmsOptIn)
        ]
        assert len(sms_opt_ins) == 0, "SmsOptIn should NOT be created without TCPA consent even with phone"

        # ConsentAcceptance should exist but with no TCPA fields
        added_ca = None
        for call_args in db.add.call_args_list:
            args, _ = call_args
            if isinstance(args[0], ConsentAcceptance):
                added_ca = args[0]
                break
        assert added_ca is not None
        assert added_ca.tcpa_checked_at is None
        assert added_ca.not_condition_of_purchase_ack is None

    def test_scroll_timestamps_recorded(self):
        """Modal opened_at and scrolled_to_end_at are captured in ConsentAcceptance."""
        db = _db_for_sold_out()
        payload = _default_payload(
            consent_acceptance={
                "terms_accepted": True,
                "terms_version": "2026.06",
                "privacy_version": "2026.06",
                "accepted_text_hash": "a1b2c3d4e5f600000000000000000000",
                "modal_opened_at": "2026-06-04T10:30:00Z",
                "modal_scrolled_to_end_at": "2026-06-04T10:30:45Z",
                "user_agent": "Mozilla/5.0 TestAgent",
            },
        )

        resp = _post(payload, db)
        assert resp.status_code == 201

        added_ca = None
        for call_args in db.add.call_args_list:
            args, _ = call_args
            if isinstance(args[0], ConsentAcceptance):
                added_ca = args[0]
                break
        assert added_ca is not None
        assert added_ca.modal_opened_at is not None
        assert added_ca.modal_scrolled_to_end_at is not None
        assert added_ca.user_agent == "Mozilla/5.0 TestAgent"
        assert added_ca.accepted_text_hash == "a1b2c3d4e5f600000000000000000000"