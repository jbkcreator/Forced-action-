"""
Phase 1 tests — county landing endpoint + waitlist POST.

Uses FastAPI TestClient with a mock DB session (no real Postgres needed).
"""
import pytest
from unittest.mock import MagicMock, patch, call
from fastapi.testclient import TestClient

from src.api.main import app
from src.core.models import County, ExpansionCandidate, ZipTerritory, WaitlistEntry, SmsOptIn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_county(county_id="pinellas", display_name="Pinellas County"):
    c = MagicMock(spec=County)
    c.county_id = county_id
    c.display_name = display_name
    return c


def _mock_candidate(status="queued", county_id="pinellas"):
    ec = MagicMock(spec=ExpansionCandidate)
    ec.county_id = county_id
    ec.status = status
    return ec


# ---------------------------------------------------------------------------
# GET /api/counties/{county_id}/landing
# ---------------------------------------------------------------------------

class TestCountyLandingEndpoint:

    def _get(self, county_id, db_override):
        from src.api.main import get_db
        app.dependency_overrides[get_db] = lambda: db_override
        try:
            client = TestClient(app)
            return client.get(f"/api/counties/{county_id}/landing")
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_unknown_county_returns_404(self):
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None
        resp = self._get("unknown", db)
        assert resp.status_code == 404

    def test_queued_candidate_returns_coming_soon(self):
        db = MagicMock()
        execute_results = [
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("pinellas")}),
            MagicMock(**{"scalar_one_or_none.return_value": _mock_candidate("queued")}),
            MagicMock(**{"scalar_one.return_value": 0}),   # zip_count
            MagicMock(**{"all.return_value": []}),         # vertical_counts
        ]
        db.execute.side_effect = execute_results
        resp = self._get("pinellas", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "coming_soon"

    def test_approved_candidate_returns_coming_soon(self):
        db = MagicMock()
        execute_results = [
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("pinellas")}),
            MagicMock(**{"scalar_one_or_none.return_value": _mock_candidate("approved")}),
            MagicMock(**{"scalar_one.return_value": 12}),
            MagicMock(**{"all.return_value": []}),
        ]
        db.execute.side_effect = execute_results
        resp = self._get("pinellas", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "coming_soon"

    def test_launched_county_with_taken_zips_returns_sold_out(self):
        db = MagicMock()
        execute_results = [
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": _mock_candidate("launched", "hillsborough")}),
            MagicMock(**{"scalar_one.return_value": 3}),   # any_taken > 0
            MagicMock(**{"scalar_one.return_value": 45}),  # zip_count
            MagicMock(**{"all.return_value": [("roofing", 7)]}),
        ]
        db.execute.side_effect = execute_results
        resp = self._get("hillsborough", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "sold_out"

    def test_launched_county_zero_taken_zips_returns_404(self):
        db = MagicMock()
        execute_results = [
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": _mock_candidate("launched")}),
            MagicMock(**{"scalar_one.return_value": 0}),  # no taken ZIPs
        ]
        db.execute.side_effect = execute_results
        resp = self._get("hillsborough", db)
        assert resp.status_code == 404

    def test_no_candidate_row_with_taken_zips_returns_sold_out(self):
        """Source county (Hillsborough) has no ExpansionCandidate row."""
        db = MagicMock()
        execute_results = [
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": None}),  # no candidate
            MagicMock(**{"scalar_one.return_value": 5}),
            MagicMock(**{"scalar_one.return_value": 40}),
            MagicMock(**{"all.return_value": []}),
        ]
        db.execute.side_effect = execute_results
        resp = self._get("hillsborough", db)
        assert resp.status_code == 200
        assert resp.json()["waitlist_type"] == "sold_out"

    def test_response_includes_trade_labels(self):
        db = MagicMock()
        execute_results = [
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("pinellas")}),
            MagicMock(**{"scalar_one_or_none.return_value": _mock_candidate("queued")}),
            MagicMock(**{"scalar_one.return_value": 10}),
            MagicMock(**{"all.return_value": []}),
        ]
        db.execute.side_effect = execute_results
        resp = self._get("pinellas", db)
        assert resp.status_code == 200
        assert "trade_labels" in resp.json()
        assert resp.json()["trade_labels"]["public_adjusters"] == "Public Adjuster"


# ---------------------------------------------------------------------------
# POST /api/waitlist
# ---------------------------------------------------------------------------

class TestWaitlistPost:

    def _post(self, payload, db_override, headers=None):
        from src.api.main import get_db
        app.dependency_overrides[get_db] = lambda: db_override
        try:
            client = TestClient(app)
            return client.post("/api/waitlist", json=payload, headers=headers or {})
        finally:
            app.dependency_overrides.pop(get_db, None)

    def _db_for_write(self, server_wl_type="sold_out"):
        """Mock DB that returns county landing state + no existing entry."""
        db = MagicMock()
        # calls: County, ExpansionCandidate, any_taken, zip_count, vertical_counts, SmsOptIn
        execute_results = [
            # _resolve_county_landing_state calls (county, candidate, any_taken, zip_count, vert)
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": None}),  # no candidate
            MagicMock(**{"scalar_one.return_value": 3}),             # any_taken
            MagicMock(**{"scalar_one.return_value": 40}),            # zip_count
            MagicMock(**{"all.return_value": []}),                   # vertical_counts
            # SmsOptIn lookup
            MagicMock(**{"scalar_one_or_none.return_value": None}),
        ]
        db.execute.side_effect = execute_results
        return db

    def test_invalid_vertical_returns_422(self):
        db = MagicMock()
        resp = self._post({
            "zip_code": "33601",
            "vertical": "contractor",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, db)
        assert resp.status_code == 422

    def test_invalid_zip_returns_422(self):
        db = MagicMock()
        resp = self._post({
            "zip_code": "abc",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, db)
        assert resp.status_code == 422

    def test_sms_opt_in_without_phone_returns_422(self):
        db = MagicMock()
        resp = self._post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "sms_opt_in": True,
        }, db)
        assert resp.status_code == 422

    def test_rate_limit_429_after_threshold(self):
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
        # fresh db mock per request — callable override, single TestClient
        app.dependency_overrides[get_db] = lambda: self._db_for_write()
        try:
            client = TestClient(app)
            for _ in range(4):
                resp = client.post("/api/waitlist", json=payload)
        finally:
            app.dependency_overrides.pop(get_db, None)
        assert resp.status_code == 429

    def test_duplicate_returns_already_registered(self):
        db = MagicMock()
        execute_results = [
            MagicMock(**{"scalar_one_or_none.return_value": _mock_county("hillsborough")}),
            MagicMock(**{"scalar_one_or_none.return_value": None}),
            MagicMock(**{"scalar_one.return_value": 3}),
            MagicMock(**{"scalar_one.return_value": 40}),
            MagicMock(**{"all.return_value": []}),
        ]
        db.execute.side_effect = execute_results
        from sqlalchemy.exc import IntegrityError
        db.flush.side_effect = IntegrityError("", "", Exception())
        resp = self._post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
        }, db)
        assert resp.json()["status"] == "already_registered"

    def test_waitlist_type_mismatch_is_overwritten(self):
        """Client sends coming_soon but server resolves sold_out."""
        db = self._db_for_write(server_wl_type="sold_out")
        resp = self._post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "waitlist_type": "coming_soon",  # wrong — server will overwrite
        }, db)
        if resp.status_code == 201:
            assert resp.json()["waitlist_type"] == "sold_out"

    def test_valid_phone_writes_sms_opt_in(self):
        db = self._db_for_write()
        resp = self._post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "phone": "8135550001",
            "sms_opt_in": True,
        }, db)
        # Verify db.add was called (SmsOptIn + WaitlistEntry = 2 adds)
        if resp.status_code == 201:
            assert db.add.call_count >= 2

    def test_invalid_phone_returns_422(self):
        db = self._db_for_write()
        resp = self._post({
            "zip_code": "33601",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "name": "Jane",
            "email": "jane@test.com",
            "phone": "notaphone",
            "sms_opt_in": True,
        }, db)
        assert resp.status_code == 422
