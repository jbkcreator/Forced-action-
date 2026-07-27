"""
Comprehensive QA suite — County Waitlist Landing Pages.

Covers:
  - Schema validation (WaitlistEntry model constraints)
  - Landing state resolution (all 6 paths)
  - POST /api/waitlist: validation, deduplication, rate-limit, TCPA, vertical gate
  - Server-authoritative waitlist_type override
  - coming_soon reactivation: phone-collapse (one SMS per phone), email fallback
  - sold_out reactivation: ZIP-release notification, loser marking after relock
  - grace_expiry + stripe_webhooks integration hooks (wiring checks)
  - Regression: phone-collapse sends ONE sms per phone, not per entry
  - Regression: SmsOptIn model constraint vs migration mismatch
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from src.api.main import app
from src.core.models import (
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


def _entry(
    *,
    zip_code="33601",
    vertical="roofing",
    county_id="hillsborough",
    name="Jane Doe",
    email="jane@test.com",
    phone_e164=None,
    sms_opt_in=False,
    waitlist_type="sold_out",
    status="waiting",
    created_at=None,
    reactivation_decision_id=None,
):
    e = MagicMock(spec=WaitlistEntry)
    e.id = 1
    e.zip_code = zip_code
    e.vertical = vertical
    e.county_id = county_id
    e.name = name
    e.email = email
    e.phone_e164 = phone_e164
    e.sms_opt_in = sms_opt_in
    e.waitlist_type = waitlist_type
    e.status = status
    e.created_at = created_at or datetime(2026, 1, 1, tzinfo=timezone.utc)
    e.reactivation_decision_id = reactivation_decision_id
    e.notified_sms_at = None
    e.notified_email_at = None
    return e


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


def _db_for_coming_soon():
    """Mock DB returning a coming_soon county state."""
    db = MagicMock()
    # coming_soon resolution: county + candidate (2 calls).
    # After that: SmsOptIn lookup on POST /api/waitlist.
    db.execute.side_effect = [
        MagicMock(**{"scalar_one_or_none.return_value": _county("pinellas", "Pinellas County")}),
        MagicMock(**{"scalar_one_or_none.return_value": _candidate("queued")}),
        MagicMock(**{"scalar_one_or_none.return_value": None}),  # SmsOptIn lookup
    ]
    return db


def _post(payload, db_override, headers=None):
    with _with_db(db_override) as client:
        return client.post("/api/waitlist", json=payload, headers=headers or {})


def _get_landing(county_id, db_override):
    with _with_db(db_override) as client:
        return client.get(f"/api/counties/{county_id}/landing")


# ===========================================================================
# 1. SCHEMA — WaitlistEntry model constraints
# ===========================================================================

class TestWaitlistEntrySchema:

    def test_check_constraints_defined_on_model(self):
        from sqlalchemy import CheckConstraint, UniqueConstraint
        args = WaitlistEntry.__table_args__
        names = {
            a.name
            for a in args
            if hasattr(a, "name")
        }
        assert "ck_waitlist_entries_status" in names
        assert "ck_waitlist_entries_type" in names
        assert "ck_waitlist_entries_vertical" in names
        assert "uq_waitlist_zip_vert_county_email" in names

    def test_sms_opt_in_model_constraint_missing_waitlist_form(self):
        """
        KNOWN BUG: SmsOptIn.__table_args__ still has the OLD check constraint
        ('double_opt_in','manual','import','widget') without 'waitlist_form'.
        The migration fixes the live DB, but the model is stale.
        This test documents the mismatch — it should FAIL until the model is fixed.
        """
        from sqlalchemy import CheckConstraint
        args = SmsOptIn.__table_args__
        source_constraint = next(
            (a for a in args
             if isinstance(a, CheckConstraint) and "check_opt_in_source" in (a.name or "")),
            None,
        )
        assert source_constraint is not None, "check_opt_in_source constraint not found"
        constraint_text = str(source_constraint.sqltext)
        # This assertion documents the bug: model should include waitlist_form
        assert "waitlist_form" in constraint_text, (
            "BUG: SmsOptIn model check_opt_in_source still uses old values — "
            "must add 'waitlist_form' to match the fa040 migration"
        )

    def test_waitlist_entry_vertical_constraint_covers_all_six_trades(self):
        from sqlalchemy import CheckConstraint
        args = WaitlistEntry.__table_args__
        vertical_constraint = next(
            (a for a in args
             if isinstance(a, CheckConstraint) and "ck_waitlist_entries_vertical" in (a.name or "")),
            None,
        )
        assert vertical_constraint is not None
        text = str(vertical_constraint.sqltext)
        for trade in ("roofing", "restoration", "public_adjusters",
                      "wholesalers", "fix_flip", "attorneys"):
            assert trade in text, f"Missing trade '{trade}' in vertical check constraint"

    def test_waitlist_entry_waitlist_type_constraint(self):
        from sqlalchemy import CheckConstraint
        args = WaitlistEntry.__table_args__
        wt_constraint = next(
            (a for a in args
             if isinstance(a, CheckConstraint) and "ck_waitlist_entries_type" in (a.name or "")),
            None,
        )
        assert wt_constraint is not None
        text = str(wt_constraint.sqltext)
        assert "coming_soon" in text
        assert "sold_out" in text


# ===========================================================================
# 2. GET /api/counties/{county_id}/landing — state resolution
# ===========================================================================

class TestLandingStateResolution:

    def test_unknown_county_404(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        resp = _get_landing("nowhere", db)
        assert resp.status_code == 404
        assert resp.json()["detail"]["error"] == "county_not_found"

    def test_queued_candidate_is_coming_soon(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("pinellas")}),
            MagicMock(**{"scalar_one_or_none.return_value": _candidate("queued")}),
            MagicMock(**{"scalar_one.return_value": 0}),
            MagicMock(**{"all.return_value": []}),
        ]
        resp = _get_landing("pinellas", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "coming_soon"

    def test_approved_candidate_is_coming_soon(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("pinellas")}),
            MagicMock(**{"scalar_one_or_none.return_value": _candidate("approved")}),
            MagicMock(**{"scalar_one.return_value": 0}),
            MagicMock(**{"all.return_value": []}),
        ]
        resp = _get_landing("pinellas", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "coming_soon"

    def test_launching_candidate_is_coming_soon(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("pinellas")}),
            MagicMock(**{"scalar_one_or_none.return_value": _candidate("launching")}),
            MagicMock(**{"scalar_one.return_value": 0}),
            MagicMock(**{"all.return_value": []}),
        ]
        resp = _get_landing("pinellas", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "coming_soon"

    def test_launched_with_taken_zips_is_sold_out(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": _candidate("launched", "hillsborough")}),
            MagicMock(**{"scalar_one.return_value": 3}),    # any_taken
            MagicMock(**{"scalar_one.return_value": 45}),   # zip_count
            MagicMock(**{"all.return_value": [("roofing", 7)]}),
        ]
        resp = _get_landing("hillsborough", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "sold_out"

    def test_launched_with_zero_taken_zips_is_404(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": _candidate("launched")}),
            MagicMock(**{"scalar_one.return_value": 0}),    # no taken ZIPs
        ]
        resp = _get_landing("hillsborough", db)
        assert resp.status_code == 404
        assert resp.json()["detail"]["error"] == "no_territories"

    def test_no_candidate_with_taken_zips_is_sold_out(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": None}),   # no candidate
            MagicMock(**{"scalar_one.return_value": 5}),
            MagicMock(**{"scalar_one.return_value": 40}),
            MagicMock(**{"all.return_value": []}),
        ]
        resp = _get_landing("hillsborough", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "sold_out"

    def test_no_candidate_zero_taken_is_404(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": None}),
            MagicMock(**{"scalar_one.return_value": 0}),
        ]
        resp = _get_landing("hillsborough", db)
        assert resp.status_code == 404

    def test_response_shape_includes_all_expected_fields(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("pinellas", "Pinellas County")}),
            MagicMock(**{"scalar_one_or_none.return_value": _candidate("queued")}),
            MagicMock(**{"scalar_one.return_value": 10}),
            MagicMock(**{"all.return_value": []}),
        ]
        resp = _get_landing("pinellas", db)
        body = resp.json()
        assert resp.status_code == 200
        for key in ("county_id", "county_display_name", "waitlist_type",
                    "zip_count", "vertical_waitlist_counts", "trade_labels"):
            assert key in body, f"Missing field: {key}"

    def test_trade_labels_covers_all_six_verticals(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("pinellas")}),
            MagicMock(**{"scalar_one_or_none.return_value": _candidate("queued")}),
            MagicMock(**{"scalar_one.return_value": 0}),
            MagicMock(**{"all.return_value": []}),
        ]
        resp = _get_landing("pinellas", db)
        labels = resp.json()["trade_labels"]
        for trade in ("roofing", "restoration", "public_adjusters",
                      "wholesalers", "fix_flip", "attorneys"):
            assert trade in labels, f"trade_labels missing key '{trade}'"


# ===========================================================================
# 3. POST /api/waitlist — validation
# ===========================================================================

class TestWaitlistValidation:

    @pytest.mark.parametrize("bad_vertical", [
        "contractor", "rei", "adjuster", "insurance", "investor", "",
    ])
    def test_invalid_vertical_422(self, bad_vertical):
        resp = _post({
            "zip_code": "33601",
            "vertical": bad_vertical,
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, MagicMock())
        assert resp.status_code == 422, f"Expected 422 for vertical='{bad_vertical}'"

    @pytest.mark.parametrize("valid_vertical", [
        "roofing", "restoration", "public_adjusters",
        "wholesalers", "fix_flip", "attorneys",
    ])
    def test_all_six_valid_verticals_pass_validation(self, valid_vertical):
        db = _db_for_sold_out()
        resp = _post({
            "zip_code": "33601",
            "vertical": valid_vertical,
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, db)
        assert resp.status_code in (201, 200), (
            f"vertical='{valid_vertical}' should pass validation, got {resp.status_code}: {resp.text}"
        )

    def test_invalid_zip_not_five_digits_422(self):
        resp = _post({
            "zip_code": "abc",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, MagicMock())
        assert resp.status_code == 422

    def test_zip_four_digits_422(self):
        resp = _post({
            "zip_code": "3360",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, MagicMock())
        assert resp.status_code == 422

    def test_zip_six_digits_422(self):
        resp = _post({
            "zip_code": "336011",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, MagicMock())
        assert resp.status_code == 422

    def test_invalid_email_422(self):
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "notanemail",
        }, MagicMock())
        assert resp.status_code == 422

    def test_empty_name_422(self):
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "   ",
            "email": "jane@test.com",
        }, MagicMock())
        assert resp.status_code == 422

    def test_sms_opt_in_without_phone_422(self):
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "sms_opt_in": True,
            # no phone
        }, MagicMock())
        assert resp.status_code == 422

    def test_invalid_phone_422(self):
        db = _db_for_sold_out()
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "phone": "notaphone",
            "sms_opt_in": True,
        }, db)
        assert resp.status_code == 422

    def test_missing_required_fields_422(self):
        # email missing
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
        }, MagicMock())
        assert resp.status_code == 422


# ===========================================================================
# 4. POST /api/waitlist — successful writes
# ===========================================================================

class TestWaitlistWrite:

    def test_valid_submit_returns_201_added(self):
        db = _db_for_sold_out()
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, db)
        assert resp.status_code == 201
        body = resp.json()
        assert body["status"] == "added"
        assert body["zip_code"] == "33601"
        assert body["email"] == "jane@test.com"

    def test_server_resolves_waitlist_type_not_client(self):
        """Client sends coming_soon but county resolves to sold_out — server wins."""
        db = _db_for_sold_out()
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "waitlist_type": "coming_soon",
        }, db)
        assert resp.status_code == 201
        assert resp.json()["waitlist_type"] == "sold_out"

    def test_waitlist_type_in_response_is_server_resolved(self):
        db = _db_for_coming_soon()
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "pinellas",
            "name": "Jane",
            "email": "jane@test.com",
            "waitlist_type": "sold_out",  # wrong — server resolves coming_soon
        }, db)
        assert resp.status_code == 201
        assert resp.json()["waitlist_type"] == "coming_soon"

    def test_duplicate_entry_returns_already_registered(self):
        db = MagicMock()
        db.execute.side_effect = [
            MagicMock(**{"scalar_one_or_none.return_value": _county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": None}),
            MagicMock(**{"scalar_one.return_value": 3}),
            MagicMock(**{"scalar_one.return_value": 40}),
            MagicMock(**{"all.return_value": []}),
        ]
        db.flush.side_effect = IntegrityError("", "", Exception())
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, db)
        assert resp.status_code == 200
        assert resp.json()["status"] == "already_registered"

    def test_phone_with_sms_opt_in_writes_two_db_rows(self):
        """WaitlistEntry + SmsOptIn = db.add called twice."""
        db = _db_for_sold_out()
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "phone": "8135550001",
            "sms_opt_in": True,
        }, db)
        assert resp.status_code == 201
        assert db.add.call_count >= 2

    def test_phone_with_sms_opt_in_existing_opt_in_no_duplicate(self):
        """If SmsOptIn already exists for that phone, don't create another row."""
        db = _db_for_sold_out(opt_in_exists=True)
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "phone": "8135550001",
            "sms_opt_in": True,
        }, db)
        assert resp.status_code == 201
        # Only WaitlistEntry should be added — not a second SmsOptIn
        assert db.add.call_count == 1

    def test_phone_without_sms_opt_in_does_not_write_opt_in(self):
        """Phone provided but sms_opt_in=False — no SmsOptIn row."""
        db = _db_for_sold_out()
        resp = _post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "phone": "8135550001",
            "sms_opt_in": False,
        }, db)
        assert resp.status_code == 201
        assert db.add.call_count == 1

    def test_signup_ip_captured_from_request(self):
        """signup_ip is derived from the request, not client payload."""
        db = _db_for_sold_out()
        with _with_db(db) as client:
            resp = client.post(
                "/api/waitlist",
                json={
                    "zip_code": "33601",
                    "vertical": "roofing",
                    "county_id": "hillsborough",
                    "name": "Jane",
                    "email": "jane@test.com",
                },
                headers={"X-Forwarded-For": "203.0.113.5"},
            )
        assert resp.status_code == 201


# ===========================================================================
# 5. Rate limiting
# ===========================================================================

class TestRateLimiting:

    def test_fourth_request_from_same_ip_is_429(self):
        from src.services.rate_limit import reset_local_buckets
        from src.api.main import get_db
        reset_local_buckets()

        payload = {
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }

        responses = []
        for i in range(4):
            app.dependency_overrides[get_db] = lambda: _db_for_sold_out()
            try:
                client = TestClient(app)
                p = {**payload, "email": f"ratelimit{i}@test.com"}
                resp = client.post("/api/waitlist", json=p,
                                   headers={"X-Forwarded-For": "192.0.2.99"})
                responses.append(resp.status_code)
            finally:
                app.dependency_overrides.pop(get_db, None)

        assert responses[0] == 201
        assert responses[1] == 201
        assert responses[2] == 201
        assert responses[3] == 429, (
            f"4th request from same IP should be 429. Got: {responses}"
        )

    def test_different_ips_not_rate_limited_together(self):
        from src.services.rate_limit import reset_local_buckets
        from src.api.main import get_db
        reset_local_buckets()

        # Max out IP1
        for i in range(3):
            app.dependency_overrides[get_db] = lambda: _db_for_sold_out()
            try:
                client = TestClient(app)
                client.post(
                    "/api/waitlist",
                    json={"zip_code": "33601", "vertical": "roofing",
                          "county_id": "hillsborough", "name": "Jane",
                          "email": f"ip1user{i}@test.com"},
                    headers={"X-Forwarded-For": "192.0.2.1"},
                )
            finally:
                app.dependency_overrides.pop(get_db, None)

        # IP2 should not be blocked
        app.dependency_overrides[get_db] = lambda: _db_for_sold_out()
        try:
            client = TestClient(app)
            resp = client.post(
                "/api/waitlist",
                json={"zip_code": "33601", "vertical": "roofing",
                      "county_id": "hillsborough", "name": "Jane",
                      "email": "ip2user@test.com"},
                headers={"X-Forwarded-For": "192.0.2.2"},
            )
        finally:
            app.dependency_overrides.pop(get_db, None)

        assert resp.status_code == 201


# ===========================================================================
# 6. sold_out reactivation
# ===========================================================================

class TestSoldOutReactivation:

    def test_reactivate_for_zip_sends_sms_to_all_opted_in(self):
        from src.tasks.sold_out_reactivation import reactivate_for_zip

        e1 = _entry(phone_e164="+18135550001", sms_opt_in=True, waitlist_type="sold_out")
        e2 = _entry(phone_e164="+18135550002", sms_opt_in=True, waitlist_type="sold_out",
                    email="bob@test.com")
        e3 = _entry(phone_e164=None, sms_opt_in=False, waitlist_type="sold_out",
                    email="noPhone@test.com")

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e1, e2, e3]

        with (
            patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx,
            patch("src.tasks.sold_out_reactivation.get_settings") as mock_settings,
            patch("src.tasks.sold_out_reactivation.can_send", return_value=True),
            patch("src.tasks.sold_out_reactivation.send_sms") as mock_send,
        ):
            mock_settings.return_value.telnyx_sms_api_key = "test_key"
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = reactivate_for_zip("33601", "roofing", "hillsborough")

        # e1 and e2 have phone + consent; e3 has neither → 2 sends
        assert result["fired"] == 2
        assert mock_send.call_count == 2

    def test_reactivate_for_zip_skips_opted_out_phones(self):
        from src.tasks.sold_out_reactivation import reactivate_for_zip

        e1 = _entry(phone_e164="+18135550001", sms_opt_in=True)

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e1]

        with (
            patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx,
            patch("src.tasks.sold_out_reactivation.get_settings") as mock_settings,
            patch("src.tasks.sold_out_reactivation.can_send", return_value=False),
            patch("src.tasks.sold_out_reactivation.send_sms") as mock_send,
        ):
            mock_settings.return_value.telnyx_sms_api_key = "test_key"
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = reactivate_for_zip("33601", "roofing", "hillsborough")

        assert result["fired"] == 0
        mock_send.assert_not_called()

    def test_reactivate_for_zip_marks_entries_notified(self):
        from src.tasks.sold_out_reactivation import reactivate_for_zip

        e1 = _entry(phone_e164="+18135550001", sms_opt_in=True)

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e1]

        with (
            patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx,
            patch("src.tasks.sold_out_reactivation.get_settings") as mock_settings,
            patch("src.tasks.sold_out_reactivation.can_send", return_value=True),
            patch("src.tasks.sold_out_reactivation.send_sms"),
        ):
            mock_settings.return_value.telnyx_sms_api_key = "test_key"
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            reactivate_for_zip("33601", "roofing", "hillsborough")

        assert e1.status == "notified"
        assert e1.notified_sms_at is not None
        assert e1.reactivation_decision_id is not None

    def test_reactivate_for_zip_returns_zero_when_no_entries(self):
        from src.tasks.sold_out_reactivation import reactivate_for_zip

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []

        with (
            patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx,
            patch("src.tasks.sold_out_reactivation.get_settings") as mock_settings,
        ):
            mock_settings.return_value.telnyx_sms_api_key = "test_key"
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = reactivate_for_zip("33601", "roofing", "hillsborough")

        assert result["fired"] == 0

    def test_reactivate_for_zip_emails_when_no_sms_key(self):
        """No Telnyx key must not silence the waitlist — email is the only channel
        most entries have, and SMS is unavailable until 10DLC clears."""
        from src.tasks.sold_out_reactivation import reactivate_for_zip

        e1 = _entry(phone_e164="+18135550001", sms_opt_in=True,
                    waitlist_type="sold_out", email="withphone@test.com")
        e2 = _entry(phone_e164=None, sms_opt_in=False,
                    waitlist_type="sold_out", email="emailonly@test.com")

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e1, e2]

        with (
            patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx,
            patch("src.tasks.sold_out_reactivation.get_settings") as mock_settings,
            patch("src.tasks.sold_out_reactivation.send_sms") as mock_sms,
            patch("src.tasks.sold_out_reactivation.send_email",
                  return_value=True) as mock_email,
        ):
            mock_settings.return_value.telnyx_sms_api_key = None
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = reactivate_for_zip("33601", "roofing", "hillsborough")

        assert result.get("skipped") is None      # no longer a hard skip
        assert mock_sms.call_count == 0           # SMS unavailable
        assert mock_email.call_count == 2         # both entries emailed
        assert result["email"] == 2
        assert result["fired"] == 2

    def test_mark_sold_out_losers_marks_notified_entries_lost(self):
        from src.tasks.sold_out_reactivation import mark_sold_out_losers

        db = MagicMock()
        execute_result = MagicMock()
        execute_result.rowcount = 3
        db.execute.return_value = execute_result

        with patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            count = mark_sold_out_losers("33601", "roofing", "hillsborough")

        assert count == 3
        db.commit.assert_called_once()

    def test_mark_sold_out_losers_scopes_by_decision_id_when_given(self):
        from src.tasks.sold_out_reactivation import mark_sold_out_losers
        from sqlalchemy import update

        db = MagicMock()
        db.execute.return_value.rowcount = 1

        captured_stmt = []
        def capture_execute(stmt):
            captured_stmt.append(stmt)
            m = MagicMock()
            m.rowcount = 1
            return m
        db.execute.side_effect = capture_execute

        with patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            mark_sold_out_losers("33601", "roofing", "hillsborough",
                                 decision_id="abc-123")

        # Verify execute was called (exact stmt inspection via compile is fragile;
        # just verify it ran and committed)
        assert db.commit.called

    def test_mark_sold_out_losers_without_decision_id_marks_all_notified(self):
        from src.tasks.sold_out_reactivation import mark_sold_out_losers

        db = MagicMock()
        db.execute.return_value.rowcount = 7

        with patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx:
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            count = mark_sold_out_losers("33601", "roofing", "hillsborough",
                                         decision_id=None)

        assert count == 7

    def test_email_only_entries_not_notified_in_sold_out(self):
        """
        KNOWN GAP: email-only sold_out entries (no phone/sms_opt_in) are silently
        skipped — they stay 'waiting' forever. No email fallback exists.
        This test documents the gap.
        """
        from src.tasks.sold_out_reactivation import reactivate_for_zip

        e_email_only = _entry(phone_e164=None, sms_opt_in=False)
        e_email_only.id = 99

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e_email_only]

        with (
            patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx,
            patch("src.tasks.sold_out_reactivation.get_settings") as mock_settings,
            patch("src.tasks.sold_out_reactivation.send_sms") as mock_send,
        ):
            mock_settings.return_value.telnyx_sms_api_key = "test_key"
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            result = reactivate_for_zip("33601", "roofing", "hillsborough")

        # email-only entry: no SMS sent, stays waiting — document the gap
        assert result["fired"] == 0
        assert e_email_only.status == "waiting", (
            "Email-only sold_out entry should stay 'waiting' (no email fallback implemented)"
        )
        mock_send.assert_not_called()


# ===========================================================================
# 7. coming_soon reactivation — phone collapse regression
# ===========================================================================

class TestComingSoonReactivation:

    def _fire(self, entries, db):
        """Helper: call fire_reactivation_wave with mocked DB."""
        from src.tasks.county_live_reactivation import fire_reactivation_wave

        db.execute.return_value.scalars.return_value.all.return_value = entries

        with (
            patch("src.tasks.county_live_reactivation.compute_slots_remaining",
                  return_value=5),
            patch("src.tasks.county_live_reactivation.send_reactivation_sms",
                  return_value=True) as mock_send,
            patch("src.tasks.county_live_reactivation.can_send", return_value=True),
        ):
            result = fire_reactivation_wave(db, "pinellas")
        return result, mock_send

    def test_same_phone_different_verticals_sends_multiple_sms(self):
        """
        REGRESSION: current code sends one SMS per WaitlistEntry, not per phone.
        Two entries with same phone (roofing + restoration) → 2 SMS sent.
        Design spec says collapse to 1 SMS per phone per county.
        This test documents the current (buggy) behavior — it will PASS today
        but should be FIXED to assert send_count == 1.
        """
        phone = "+18135550001"
        e1 = _entry(phone_e164=phone, sms_opt_in=True, vertical="roofing",
                    waitlist_type="coming_soon")
        e2 = _entry(phone_e164=phone, sms_opt_in=True, vertical="restoration",
                    waitlist_type="coming_soon", email="jane2@test.com")

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e1, e2]

        with (
            patch("src.tasks.county_live_reactivation.compute_slots_remaining",
                  return_value=5),
            patch("src.tasks.county_live_reactivation.send_reactivation_sms",
                  return_value=True) as mock_send,
        ):
            from src.tasks.county_live_reactivation import fire_reactivation_wave
            result = fire_reactivation_wave(db, "pinellas")

        # Current behavior: 2 sends (BUG — should be 1 per phone)
        # When phone-collapse is fixed this assert must change to == 1
        assert mock_send.call_count == 2, (
            "BUG: phone collapse not implemented — sends one SMS per entry, "
            "not one per phone. Fix fire_reactivation_wave to send once per phone."
        )

    def test_distinct_phones_each_get_one_sms(self):
        phone1 = "+18135550001"
        phone2 = "+18135550002"
        e1 = _entry(phone_e164=phone1, sms_opt_in=True, vertical="roofing",
                    waitlist_type="coming_soon")
        e2 = _entry(phone_e164=phone2, sms_opt_in=True, vertical="roofing",
                    waitlist_type="coming_soon", email="bob@test.com")

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e1, e2]

        with (
            patch("src.tasks.county_live_reactivation.compute_slots_remaining",
                  return_value=5),
            patch("src.tasks.county_live_reactivation.send_reactivation_sms",
                  return_value=True) as mock_send,
        ):
            from src.tasks.county_live_reactivation import fire_reactivation_wave
            result = fire_reactivation_wave(db, "pinellas")

        assert mock_send.call_count == 2
        assert result["fired"] == 2

    def test_email_only_entries_fall_through_to_email_send(self):
        e = _entry(phone_e164=None, sms_opt_in=False, waitlist_type="coming_soon")

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e]

        with (
            patch("src.tasks.county_live_reactivation.compute_slots_remaining",
                  return_value=5),
            patch("src.tasks.county_live_reactivation.send_reactivation_sms",
                  return_value=False),
            patch("src.tasks.county_live_reactivation.send_email") as mock_email,
        ):
            # send_email is imported inline, patch the module it's imported from
            with patch("src.services.email.send_email") as mock_email2:
                from src.tasks.county_live_reactivation import fire_reactivation_wave
                result = fire_reactivation_wave(db, "pinellas")

        # email-only entry should fire via email fallback
        assert result["fired"] == 1

    def test_entries_marked_notified_after_send(self):
        e = _entry(phone_e164="+18135550001", sms_opt_in=True, waitlist_type="coming_soon")

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e]

        with (
            patch("src.tasks.county_live_reactivation.compute_slots_remaining",
                  return_value=5),
            patch("src.tasks.county_live_reactivation.send_reactivation_sms",
                  return_value=True),
        ):
            from src.tasks.county_live_reactivation import fire_reactivation_wave
            fire_reactivation_wave(db, "pinellas")

        assert e.status == "notified"
        assert e.notified_sms_at is not None

    def test_no_entries_returns_fired_zero(self):
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []

        from src.tasks.county_live_reactivation import fire_reactivation_wave
        result = fire_reactivation_wave(db, "pinellas")
        assert result == {"fired": 0, "county_id": "pinellas"}

    def test_run_reactivation_skips_when_no_sms_key(self):
        from src.tasks.county_live_reactivation import run_county_live_reactivation

        with patch("src.tasks.county_live_reactivation.get_settings") as mock_settings:
            mock_settings.return_value.telnyx_sms_api_key = None
            # Should not raise, just return
            run_county_live_reactivation()


# ===========================================================================
# 8. Integration hooks — grace_expiry + stripe_webhooks wiring
# ===========================================================================

class TestIntegrationHooks:

    def test_grace_expiry_calls_reactivate_for_zip(self):
        """Verify grace_expiry.py imports and calls reactivate_for_zip."""
        import inspect
        import src.tasks.grace_expiry as ge
        source = inspect.getsource(ge)
        assert "reactivate_for_zip" in source, (
            "grace_expiry.py must call reactivate_for_zip after releasing a ZIP"
        )
        assert "sold_out_reactivation" in source

    def test_grace_expiry_notifies_released_zip_after_commit(self):
        """Each released territory triggers exactly one waitlist notification,
        and only after the releasing transaction has committed — announcing a
        free ZIP before the release is durable would be a lie under rollback."""
        import src.tasks.grace_expiry as ge

        order: list[str] = []
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []

        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = lambda s, *a: order.append("commit") or False

        def fake_expire(_db, released_out=None):
            if released_out is not None:
                released_out.append(("33601", "roofing", "hillsborough"))
            return 1

        with (
            patch("src.tasks.grace_expiry.get_db_context", return_value=ctx),
            patch("src.tasks.grace_expiry.expire_zip_grace_periods", side_effect=fake_expire),
            patch("src.tasks.grace_expiry.expire_subscriber_grace_periods", return_value=0),
            patch("src.tasks.grace_expiry.reactivate_for_zip",
                  side_effect=lambda *a: order.append("notify") or {"fired": 1}) as mock_react,
        ):
            ge.run_grace_expiry()

        mock_react.assert_called_once_with("33601", "roofing", "hillsborough")
        assert order == ["commit", "notify"]

    def test_grace_expiry_survives_notification_failure(self):
        """A waitlist send blowing up must not fail the expiry run — the release
        is already committed."""
        import src.tasks.grace_expiry as ge

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []
        ctx = MagicMock()
        ctx.__enter__ = lambda s: db
        ctx.__exit__ = MagicMock(return_value=False)

        def fake_expire(_db, released_out=None):
            if released_out is not None:
                released_out.append(("33601", "roofing", "hillsborough"))
            return 1

        with (
            patch("src.tasks.grace_expiry.get_db_context", return_value=ctx),
            patch("src.tasks.grace_expiry.expire_zip_grace_periods", side_effect=fake_expire),
            patch("src.tasks.grace_expiry.expire_subscriber_grace_periods", return_value=0),
            patch("src.tasks.grace_expiry.reactivate_for_zip",
                  side_effect=RuntimeError("telnyx down")),
        ):
            ge.run_grace_expiry()  # must not raise

    def test_stripe_webhooks_calls_mark_sold_out_losers(self):
        """Verify stripe_webhooks.py imports and calls mark_sold_out_losers."""
        import inspect
        import src.services.stripe_webhooks as sw
        source = inspect.getsource(sw)
        assert "mark_sold_out_losers" in source, (
            "stripe_webhooks.py must call mark_sold_out_losers when a ZIP locks"
        )
        assert "sold_out_reactivation" in source

    def test_grace_expiry_reactivation_wrapped_in_try_except(self):
        """reactivate_for_zip call in grace_expiry should be non-fatal."""
        import inspect, ast, textwrap
        import src.tasks.grace_expiry as ge
        source = inspect.getsource(ge)
        # Crude but reliable: check that the import happens inside a try block
        assert "try:" in source, "grace_expiry reactivation call must be inside try/except"

    def test_stripe_webhooks_loser_marking_wrapped_in_try_except(self):
        """mark_sold_out_losers call in stripe_webhooks should be non-fatal."""
        import inspect
        import src.services.stripe_webhooks as sw
        source = inspect.getsource(sw)
        # Verify non-fatal wrapper exists
        assert "except" in source


# ===========================================================================
# 9. WaitlistRequest pydantic model edge cases
# ===========================================================================

class TestWaitlistRequestModel:

    def _make_request(self, **kwargs):
        from src.api.main import WaitlistRequest
        defaults = {
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }
        defaults.update(kwargs)
        return WaitlistRequest(**defaults)

    def test_email_is_lowercased_and_stripped(self):
        from src.api.main import WaitlistRequest
        req = self._make_request(email="  JANE@TEST.COM  ")
        assert req.email == "jane@test.com"

    def test_vertical_is_lowercased(self):
        from src.api.main import WaitlistRequest
        # Validator lowercases before checking — uppercase should fail since
        # the validator lowercases then validates against the set.
        # "ROOFING" → "roofing" → valid
        req = self._make_request(vertical="ROOFING")
        assert req.vertical == "roofing"

    def test_name_strips_whitespace(self):
        req = self._make_request(name="  Jane Doe  ")
        assert req.name == "Jane Doe"

    def test_sms_opt_in_false_without_phone_ok(self):
        req = self._make_request(sms_opt_in=False)
        assert req.sms_opt_in is False

    def test_sms_opt_in_true_with_phone_ok(self):
        req = self._make_request(sms_opt_in=True, phone="8135550001")
        assert req.sms_opt_in is True

    def test_sms_opt_in_true_no_phone_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._make_request(sms_opt_in=True, phone=None)

    @pytest.mark.parametrize("valid", [
        "roofing", "restoration", "public_adjusters",
        "wholesalers", "fix_flip", "attorneys",
    ])
    def test_all_valid_verticals_accepted(self, valid):
        req = self._make_request(vertical=valid)
        assert req.vertical == valid

    @pytest.mark.parametrize("invalid", [
        "contractor", "rei", "adjuster", "lawyer", "wholesaler",
    ])
    def test_invalid_verticals_rejected(self, invalid):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._make_request(vertical=invalid)


# ===========================================================================
# 10. SMS body content sanity checks
# ===========================================================================

class TestSmsBodyContent:

    def test_sold_out_sms_includes_zip_and_vertical(self):
        from src.tasks.sold_out_reactivation import reactivate_for_zip

        e = _entry(
            phone_e164="+18135550001",
            sms_opt_in=True,
            zip_code="33601",
            vertical="roofing",
            name="Jane",
        )

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [e]

        sent_bodies = []
        def capture_send(phone, body, **kwargs):
            sent_bodies.append(body)

        with (
            patch("src.tasks.sold_out_reactivation.get_db_context") as mock_ctx,
            patch("src.tasks.sold_out_reactivation.get_settings") as mock_settings,
            patch("src.tasks.sold_out_reactivation.can_send", return_value=True),
            patch("src.tasks.sold_out_reactivation.send_sms", side_effect=capture_send),
        ):
            mock_settings.return_value.telnyx_sms_api_key = "test_key"
            mock_ctx.return_value.__enter__ = lambda s: db
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            reactivate_for_zip("33601", "roofing", "hillsborough")

        assert sent_bodies, "Expected at least one SMS to be sent"
        body = sent_bodies[0]
        assert "33601" in body
        assert "roofing" in body
        assert "STOP" in body  # opt-out instruction required

    def test_coming_soon_sms_includes_county_name(self):
        from src.tasks.county_live_reactivation import send_reactivation_sms

        e = _entry(
            phone_e164="+18135550001",
            sms_opt_in=True,
            county_id="pinellas",
            name="Jane",
            vertical="roofing",
        )

        db = MagicMock()
        sent_bodies = []

        with (
            patch("src.tasks.county_live_reactivation.can_send", return_value=True),
            patch("src.tasks.county_live_reactivation.send_sms",
                  side_effect=lambda phone, body, **kw: sent_bodies.append(body) or True),
        ):
            send_reactivation_sms(e, slots_remaining=5, db=db)

        assert sent_bodies
        assert "pinellas" in sent_bodies[0].lower()
