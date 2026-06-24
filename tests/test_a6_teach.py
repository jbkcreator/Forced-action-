"""
A6 — Closer-to-Cora Teaching Interface tests.

TDD vertical slices:
  Step 1: config/closer.py correction vocabulary
  Step 4: CDS engine dampener logic (pure functions, no DB)
  Step 5: POST/DELETE /api/admin/closer/teach endpoints
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError


# ── Step 1: Correction vocabulary ────────────────────────────────────────────

class TestCorrectionVocab:
    def test_all_five_reasons_present(self):
        from config.closer import CORRECTION_REASONS
        assert set(CORRECTION_REASONS) == {
            "non_residential",
            "owner_not_motivated",
            "wrong_distress",
            "bad_contact",
            "other",
        }

    def test_reasons_set_matches_list(self):
        from config.closer import CORRECTION_REASONS, CORRECTION_REASONS_SET
        assert CORRECTION_REASONS_SET == frozenset(CORRECTION_REASONS)

    def test_dampening_reasons_exact(self):
        from config.closer import DAMPENING_REASONS_SET
        assert DAMPENING_REASONS_SET == frozenset({
            "non_residential", "owner_not_motivated", "wrong_distress"
        })

    def test_dampening_is_strict_subset(self):
        from config.closer import CORRECTION_REASONS_SET, DAMPENING_REASONS_SET
        assert DAMPENING_REASONS_SET < CORRECTION_REASONS_SET

    def test_bad_contact_not_dampening(self):
        from config.closer import DAMPENING_REASONS_SET
        assert "bad_contact" not in DAMPENING_REASONS_SET

    def test_other_not_dampening(self):
        from config.closer import DAMPENING_REASONS_SET
        assert "other" not in DAMPENING_REASONS_SET

    def test_teachable_signal_types_nonempty(self):
        from config.closer import TEACHABLE_SIGNAL_TYPES_SET
        assert len(TEACHABLE_SIGNAL_TYPES_SET) > 0
        assert "foreclosures" in TEACHABLE_SIGNAL_TYPES_SET
        assert "tax_delinquencies" in TEACHABLE_SIGNAL_TYPES_SET


# ── Step 4a: Signal filtering for wrong_distress ─────────────────────────────

def _make_signal(sig_type: str, sig_date=None, **kwargs) -> dict:
    return {"type": sig_type, "date": sig_date, "amount": None, **kwargs}


def _correction(reason: str, signal_type: str | None = None, created_at=None) -> dict:
    if created_at is None:
        created_at = datetime(2026, 1, 15, tzinfo=timezone.utc)
    return {
        "correction_reason": reason,
        "signal_type": signal_type,
        "created_at": created_at,
    }


class TestFilterSignalsForTeaching:
    def test_no_corrections_returns_all_signals(self):
        from src.services.cds_engine import _filter_signals_for_teaching
        signals = [
            _make_signal("foreclosures", date(2026, 1, 1)),
            _make_signal("tax_delinquencies", date(2025, 6, 1)),
        ]
        result = _filter_signals_for_teaching(signals, [])
        assert result == signals

    def test_non_wrong_distress_correction_leaves_signals_intact(self):
        from src.services.cds_engine import _filter_signals_for_teaching
        signals = [_make_signal("foreclosures", date(2026, 1, 1))]
        corr = [_correction("non_residential")]
        result = _filter_signals_for_teaching(signals, corr)
        assert result == signals

    def test_wrong_distress_drops_older_matching_signal(self):
        from src.services.cds_engine import _filter_signals_for_teaching
        corr_dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
        signals = [
            _make_signal("foreclosures", date(2026, 1, 10)),  # older than correction
            _make_signal("tax_delinquencies", date(2026, 1, 1)),
        ]
        corr = [_correction("wrong_distress", "foreclosures", corr_dt)]
        result = _filter_signals_for_teaching(signals, corr)
        types = [s["type"] for s in result]
        assert "foreclosures" not in types
        assert "tax_delinquencies" in types

    def test_wrong_distress_keeps_newer_signal(self):
        from src.services.cds_engine import _filter_signals_for_teaching
        corr_dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
        signals = [
            _make_signal("foreclosures", date(2026, 2, 1)),  # newer than correction
            _make_signal("foreclosures", date(2025, 12, 1)),  # older
        ]
        corr = [_correction("wrong_distress", "foreclosures", corr_dt)]
        result = _filter_signals_for_teaching(signals, corr)
        # Newer one kept, older dropped
        kept_dates = [s["date"] for s in result if s["type"] == "foreclosures"]
        assert date(2026, 2, 1) in kept_dates
        assert date(2025, 12, 1) not in kept_dates

    def test_wrong_distress_none_date_signal_is_dropped(self):
        from src.services.cds_engine import _filter_signals_for_teaching
        corr_dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
        signals = [_make_signal("foreclosures", None)]  # no date → treat as old
        corr = [_correction("wrong_distress", "foreclosures", corr_dt)]
        result = _filter_signals_for_teaching(signals, corr)
        assert not any(s["type"] == "foreclosures" for s in result)

    def test_wrong_distress_datetime_date_normalized(self):
        from src.services.cds_engine import _filter_signals_for_teaching
        corr_dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
        # signal.date is a datetime (not date) — should still compare correctly
        signals = [
            _make_signal("foreclosures", datetime(2026, 2, 1, tzinfo=timezone.utc)),  # newer
        ]
        corr = [_correction("wrong_distress", "foreclosures", corr_dt)]
        result = _filter_signals_for_teaching(signals, corr)
        assert len(result) == 1  # kept


# ── Step 4b: Vertical dampener ───────────────────────────────────────────────

def _make_vertical_scores(value: float = 50.0) -> dict:
    return {
        "wholesalers": value, "fix_flip": value, "restoration": value,
        "roofing": value, "public_adjusters": value, "attorneys": value,
    }


def _make_vertical_results(value: float = 50.0) -> dict:
    return {v: {"score": value} for v in
            ["wholesalers", "fix_flip", "restoration", "roofing", "public_adjusters", "attorneys"]}


class TestApplyVerticalDampener:
    def test_non_residential_zeros_all_verticals(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr = [_correction("non_residential")]
        _apply_vertical_dampener(vs, vr, corr, [])
        assert all(v == 0.0 for v in vs.values())

    def test_non_residential_zeroes_vertical_results_too(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr = [_correction("non_residential")]
        _apply_vertical_dampener(vs, vr, corr, [])
        assert all(v["score"] == 0.0 for v in vr.values())

    def test_owner_not_motivated_zeros_investment_verticals(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr = [_correction("owner_not_motivated")]
        signals = []  # no signals → no lift
        _apply_vertical_dampener(vs, vr, corr, signals)
        # Investment verticals zeroed
        assert vs["wholesalers"] == 0.0
        assert vs["fix_flip"] == 0.0
        assert vs["attorneys"] == 0.0
        # Contractor verticals untouched
        assert vs["roofing"] == 60.0
        assert vs["restoration"] == 60.0
        assert vs["public_adjusters"] == 60.0

    def test_owner_not_motivated_lifts_on_newer_signal(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr_dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
        corr = [_correction("owner_not_motivated", created_at=corr_dt)]
        # A signal newer than the correction → correction lifts
        signals = [_make_signal("foreclosures", date(2026, 2, 1))]
        _apply_vertical_dampener(vs, vr, corr, signals)
        assert vs["wholesalers"] == 60.0  # not zeroed — correction lifted

    def test_owner_not_motivated_no_lift_on_older_signal(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr_dt = datetime(2026, 1, 15, tzinfo=timezone.utc)
        corr = [_correction("owner_not_motivated", created_at=corr_dt)]
        signals = [_make_signal("foreclosures", date(2026, 1, 10))]  # older
        _apply_vertical_dampener(vs, vr, corr, signals)
        assert vs["wholesalers"] == 0.0

    def test_non_residential_dominates_all_other_corrections(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr = [
            _correction("non_residential"),
            _correction("owner_not_motivated"),
        ]
        _apply_vertical_dampener(vs, vr, corr, [])
        assert all(v == 0.0 for v in vs.values())

    def test_bad_contact_no_dampener(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr = [_correction("bad_contact")]
        _apply_vertical_dampener(vs, vr, corr, [])
        assert all(v == 60.0 for v in vs.values())

    def test_other_no_dampener(self):
        from src.services.cds_engine import _apply_vertical_dampener
        vs = _make_vertical_scores(60.0)
        vr = _make_vertical_results(60.0)
        corr = [_correction("other")]
        _apply_vertical_dampener(vs, vr, corr, [])
        assert all(v == 60.0 for v in vs.values())


# ── Step 5: Router endpoint tests ─────────────────────────────────────────────

@pytest.fixture(scope="module")
def app():
    from src.api.main import app as _app
    return _app


@pytest.fixture
def mock_admin():
    return {"email": "admin@test.com", "id": 1}


def _make_test_client(app, mock_session, mock_admin_dict):
    """Return a TestClient with get_db and get_current_admin overridden."""
    from src.api.deps import get_db
    from src.api.admin_router import get_current_admin

    app.dependency_overrides[get_db] = lambda: mock_session
    app.dependency_overrides[get_current_admin] = lambda: mock_admin_dict
    client = TestClient(app, raise_server_exceptions=False)
    return client


def _cleanup(app):
    app.dependency_overrides.clear()


def _mock_session_for_teach(
    property_exists: bool = True,
    duplicate: bool = False,
):
    """Build a mock DB session for teach endpoint tests."""
    session = MagicMock()

    def _execute(stmt, params=None):
        sql = str(stmt) if hasattr(stmt, "__str__") else ""
        result = MagicMock()
        if "FROM properties" in sql or (params and "pid" in (params or {})):
            row = MagicMock() if property_exists else None
            result.first.return_value = row
            result.fetchall.return_value = []
            return result
        result.first.return_value = None
        result.fetchall.return_value = []
        return result

    session.execute.side_effect = _execute

    if duplicate:
        from sqlalchemy.exc import IntegrityError
        session.flush.side_effect = IntegrityError("dup", {}, Exception())
        # Also need to return existing row for 409 response
        existing = MagicMock()
        existing.id = 42
        existing.subject_id = 1
        existing.correction_reason = "non_residential"
        existing.signal_type = None
        existing.dampener_active = True
        existing.queue_status = "pending"
        existing.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        session.execute.side_effect = None
        # First call (property check) returns a row; subsequent calls for duplicate fetch
        call_count = [0]

        def _execute_dup(stmt, params=None):
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] == 1:
                # property existence check
                row = MagicMock()
                result.first.return_value = row
                return result
            # duplicate check query
            result.scalars.return_value.first.return_value = existing
            return result

        session.execute.side_effect = _execute_dup
        session.flush.side_effect = IntegrityError("dup", {}, Exception())
    return session


class TestTeachEndpointCreate:
    def test_valid_non_residential_returns_201(self, app, mock_admin):
        session = _mock_session_for_teach(property_exists=True)
        client = _make_test_client(app, session, mock_admin)
        try:
            with patch("src.api.closer_router.CDSEngine") as mock_engine_cls:
                mock_engine_cls.return_value.score_properties_by_ids.return_value = []
                resp = client.post(
                    "/api/admin/closer/teach",
                    json={"subject_id": 1, "correction_reason": "non_residential"},
                    headers={"Authorization": "Bearer test"},
                )
            assert resp.status_code == 201
        finally:
            _cleanup(app)

    def test_valid_non_residential_stores_property_subject_as_string_ref(self, app, mock_admin):
        session = _mock_session_for_teach(property_exists=True)
        client = _make_test_client(app, session, mock_admin)
        try:
            with patch("src.api.closer_router.CDSEngine") as mock_engine_cls:
                mock_engine_cls.return_value.score_properties_by_ids.return_value = []
                resp = client.post(
                    "/api/admin/closer/teach",
                    json={"subject_id": 1, "correction_reason": "non_residential"},
                    headers={"Authorization": "Bearer test"},
                )
            assert resp.status_code == 201
            saved_row = session.add.call_args.args[0]
            assert saved_row.subject_type == "property"
            assert saved_row.subject_ref == "1"
            assert saved_row.subject_id == 1
        finally:
            _cleanup(app)

    def test_invalid_reason_returns_422(self, app, mock_admin):
        session = _mock_session_for_teach()
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.post(
                "/api/admin/closer/teach",
                json={"subject_id": 1, "correction_reason": "totally_invalid"},
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 422
        finally:
            _cleanup(app)

    def test_wrong_distress_without_signal_type_returns_422(self, app, mock_admin):
        session = _mock_session_for_teach()
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.post(
                "/api/admin/closer/teach",
                json={"subject_id": 1, "correction_reason": "wrong_distress"},
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 422
        finally:
            _cleanup(app)

    def test_wrong_distress_with_invalid_signal_type_returns_422(self, app, mock_admin):
        session = _mock_session_for_teach()
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.post(
                "/api/admin/closer/teach",
                json={
                    "subject_id": 1,
                    "correction_reason": "wrong_distress",
                    "signal_type": "not_a_real_signal",
                },
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 422
        finally:
            _cleanup(app)

    def test_wrong_distress_with_valid_signal_type_returns_201(self, app, mock_admin):
        session = _mock_session_for_teach(property_exists=True)
        client = _make_test_client(app, session, mock_admin)
        try:
            with patch("src.api.closer_router.CDSEngine") as mock_engine_cls:
                mock_engine_cls.return_value.score_properties_by_ids.return_value = []
                resp = client.post(
                    "/api/admin/closer/teach",
                    json={
                        "subject_id": 1,
                        "correction_reason": "wrong_distress",
                        "signal_type": "foreclosures",
                    },
                    headers={"Authorization": "Bearer test"},
                )
            assert resp.status_code == 201
        finally:
            _cleanup(app)

    def test_bad_contact_no_rescore(self, app, mock_admin):
        session = _mock_session_for_teach(property_exists=True)
        client = _make_test_client(app, session, mock_admin)
        try:
            with patch("src.api.closer_router.CDSEngine") as mock_engine_cls:
                resp = client.post(
                    "/api/admin/closer/teach",
                    json={"subject_id": 1, "correction_reason": "bad_contact"},
                    headers={"Authorization": "Bearer test"},
                )
            assert resp.status_code == 201
            # CDSEngine should not have been instantiated (no rescore)
            mock_engine_cls.assert_not_called()
        finally:
            _cleanup(app)

    def test_property_not_found_returns_404(self, app, mock_admin):
        session = _mock_session_for_teach(property_exists=False)
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.post(
                "/api/admin/closer/teach",
                json={"subject_id": 9999, "correction_reason": "non_residential"},
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 404
        finally:
            _cleanup(app)

    def test_non_residential_with_signal_type_returns_422(self, app, mock_admin):
        """signal_type must not be present for non-wrong_distress reasons."""
        session = _mock_session_for_teach()
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.post(
                "/api/admin/closer/teach",
                json={
                    "subject_id": 1,
                    "correction_reason": "non_residential",
                    "signal_type": "foreclosures",
                },
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 422
        finally:
            _cleanup(app)


class TestTeachEndpointDelete:
    def test_delete_existing_returns_200(self, app, mock_admin):
        session = MagicMock()
        existing = MagicMock()
        existing.id = 5
        existing.subject_id = 1
        existing.correction_reason = "non_residential"
        existing.dampener_active = True
        existing.queue_status = "pending"
        session.get.return_value = existing

        client = _make_test_client(app, session, mock_admin)
        try:
            with patch("src.api.closer_router.CDSEngine") as mock_engine_cls:
                mock_engine_cls.return_value.score_properties_by_ids.return_value = []
                resp = client.delete(
                    "/api/admin/closer/teach/5",
                    headers={"Authorization": "Bearer test"},
                )
            assert resp.status_code == 200
            assert existing.dampener_active is False
            assert existing.queue_status == "discarded"
        finally:
            _cleanup(app)

    def test_delete_missing_returns_404(self, app, mock_admin):
        session = MagicMock()
        session.get.return_value = None

        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.delete(
                "/api/admin/closer/teach/9999",
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 404
        finally:
            _cleanup(app)

    def test_delete_non_dampening_skips_rescore(self, app, mock_admin):
        """Deleting bad_contact/other (was never dampening) should still succeed
        but not trigger a rescore since dampener_active was already False."""
        session = MagicMock()
        existing = MagicMock()
        existing.id = 7
        existing.subject_id = 1
        existing.correction_reason = "bad_contact"
        existing.dampener_active = False  # bad_contact was never active
        existing.queue_status = "pending"
        session.get.return_value = existing

        client = _make_test_client(app, session, mock_admin)
        try:
            with patch("src.api.closer_router.CDSEngine") as mock_engine_cls:
                resp = client.delete(
                    "/api/admin/closer/teach/7",
                    headers={"Authorization": "Bearer test"},
                )
            assert resp.status_code == 200
            mock_engine_cls.assert_not_called()
        finally:
            _cleanup(app)


# ── Delivered leads endpoint (drives the cockpit Teach panel) ─────────────────

def _row(**kwargs):
    r = MagicMock()
    for k, v in kwargs.items():
        setattr(r, k, v)
    return r


def _mock_session_for_delivered(lead_rows, correction_rows):
    """Mock session: first execute → delivered leads, second → active corrections."""
    session = MagicMock()
    calls = {"n": 0}

    def _execute(stmt, params=None):
        calls["n"] += 1
        result = MagicMock()
        sql = str(stmt)
        if "cora_training_overrides" in sql:
            result.fetchall.return_value = correction_rows
        else:
            result.fetchall.return_value = lead_rows
        return result

    session.execute.side_effect = _execute
    return session


class TestDeliveredLeadsEndpoint:
    def test_returns_leads_with_signals(self, app, mock_admin):
        lead_rows = [
            _row(property_id=1, address="1234 Elm St", city="Tampa",
                 cds_score=71.0, lead_tier="Gold",
                 signals=["foreclosures", "tax_delinquencies"],
                 sent_at=datetime(2026, 6, 1, tzinfo=timezone.utc)),
        ]
        session = _mock_session_for_delivered(lead_rows, [])
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.get(
                "/api/admin/subscribers/42/delivered-leads",
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["subscriber_id"] == 42
            assert body["count"] == 1
            item = body["items"][0]
            assert item["property_id"] == 1
            assert item["address"] == "1234 Elm St"
            assert item["cds_score"] == 71.0
            assert item["lead_tier"] == "Gold"
            assert item["signals"] == ["foreclosures", "tax_delinquencies"]
            assert item["active_corrections"] == []
        finally:
            _cleanup(app)

    def test_attaches_active_corrections_to_right_lead(self, app, mock_admin):
        lead_rows = [
            _row(property_id=1, address="1234 Elm St", city="Tampa",
                 cds_score=71.0, lead_tier="Gold", signals=["foreclosures"],
                 sent_at=datetime(2026, 6, 1, tzinfo=timezone.utc)),
            _row(property_id=2, address="88 Oak Ave", city="Tampa",
                 cds_score=44.0, lead_tier="Silver", signals=[],
                 sent_at=datetime(2026, 5, 1, tzinfo=timezone.utc)),
        ]
        correction_rows = [
            _row(id=9, subject_id=2, correction_reason="non_residential", signal_type=None),
        ]
        session = _mock_session_for_delivered(lead_rows, correction_rows)
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.get(
                "/api/admin/subscribers/42/delivered-leads",
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 200
            items = {i["property_id"]: i for i in resp.json()["items"]}
            assert items[1]["active_corrections"] == []
            assert items[2]["active_corrections"][0]["id"] == 9
            assert items[2]["active_corrections"][0]["correction_reason"] == "non_residential"
        finally:
            _cleanup(app)

    def test_empty_when_no_leads(self, app, mock_admin):
        session = _mock_session_for_delivered([], [])
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.get(
                "/api/admin/subscribers/99/delivered-leads",
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["count"] == 0
            assert body["items"] == []
        finally:
            _cleanup(app)

    def test_null_signals_normalized_to_empty_list(self, app, mock_admin):
        lead_rows = [
            _row(property_id=1, address="1234 Elm St", city="Tampa",
                 cds_score=None, lead_tier=None, signals=None,
                 sent_at=datetime(2026, 6, 1, tzinfo=timezone.utc)),
        ]
        session = _mock_session_for_delivered(lead_rows, [])
        client = _make_test_client(app, session, mock_admin)
        try:
            resp = client.get(
                "/api/admin/subscribers/42/delivered-leads",
                headers={"Authorization": "Bearer test"},
            )
            assert resp.status_code == 200
            item = resp.json()["items"][0]
            assert item["signals"] == []
            assert item["cds_score"] is None
        finally:
            _cleanup(app)
