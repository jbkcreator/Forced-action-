"""Unit tests for Sprint 4.6: Underwriting Reason-Code Feedback.

Covers the service layer (record_feedback, apply_signal_nudges, rescore_property)
and the Pydantic request validator. No server or real DB needed — all DB calls
are mocked via MagicMock.

Run:
    PYTHONPATH=. .venv/Scripts/python.exe -m pytest tests/test_underwriting_feedback.py -v
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from config.scoring import UNDERWRITING_REASON_SIGNAL_NUDGES, VERTICAL_WEIGHTS
from src.services.underwriting_feedback_service import (
    VALID_REASON_CODES,
    UnderwritingReasonCode,
    apply_signal_nudges,
    record_feedback,
    rescore_property,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_db(existing_delta: float | None = None) -> MagicMock:
    """Minimal mock Session.

    existing_delta: if set, simulate a pre-existing scoring_weight_overrides row
                    for every (vertical, signal_type) pair queried.
    """
    db = MagicMock()

    existing_row = MagicMock()
    existing_row.delta = existing_delta

    def _execute_side_effect(sql, params=None):
        result = MagicMock()
        result.first.return_value = existing_row if existing_delta is not None else None
        result.fetchall.return_value = []
        result.scalar_one_or_none.return_value = None
        return result

    db.execute.side_effect = _execute_side_effect
    return db


# ---------------------------------------------------------------------------
# UnderwritingReasonCode enum
# ---------------------------------------------------------------------------

class TestUnderwritingReasonCode:
    def test_all_codes_in_nudges_map(self):
        """Every enum member must have an entry in UNDERWRITING_REASON_SIGNAL_NUDGES."""
        for member in UnderwritingReasonCode:
            assert member.value in UNDERWRITING_REASON_SIGNAL_NUDGES, (
                f"{member.value!r} is in the enum but missing from UNDERWRITING_REASON_SIGNAL_NUDGES"
            )

    def test_all_nudge_verticals_and_signals_exist(self):
        """Every (vertical, signal_type) in nudge map must exist in VERTICAL_WEIGHTS."""
        errors = []
        for code, nudges in UNDERWRITING_REASON_SIGNAL_NUDGES.items():
            for vertical, signal_type, _ in nudges:
                if vertical not in VERTICAL_WEIGHTS:
                    errors.append(f"{code}: vertical {vertical!r} not in VERTICAL_WEIGHTS")
                elif signal_type not in VERTICAL_WEIGHTS[vertical]:
                    errors.append(
                        f"{code}: signal {signal_type!r} not in VERTICAL_WEIGHTS[{vertical!r}]"
                    )
        assert not errors, "\n".join(errors)

    def test_all_nudges_are_negative(self):
        """All underwriting nudges must be negative (penalties, not bonuses)."""
        bad = [
            (code, v, s, d)
            for code, nudges in UNDERWRITING_REASON_SIGNAL_NUDGES.items()
            for v, s, d in nudges
            if d >= 0
        ]
        assert not bad, f"Non-negative nudge deltas found: {bad}"

    def test_valid_reason_codes_matches_enum(self):
        assert VALID_REASON_CODES == {c.value for c in UnderwritingReasonCode}


# ---------------------------------------------------------------------------
# record_feedback
# ---------------------------------------------------------------------------

class TestRecordFeedback:
    def test_inserts_row(self):
        db = _mock_db()
        record_feedback(
            property_id=42,
            reason_code="structural_damage",
            submitted_by="admin@example.com",
            db=db,
            reason_detail="Cracked foundation noted",
            lender_id="LENDER_001",
            loan_amount=280000.0,
        )
        db.execute.assert_called_once()
        sql, params = db.execute.call_args[0]
        assert "INSERT INTO underwriting_feedback" in str(sql)
        assert params["pid"] == 42
        assert params["code"] == "structural_damage"
        assert params["detail"] == "Cracked foundation noted"
        assert params["lender"] == "LENDER_001"
        assert params["amount"] == 280000.0
        assert params["by"] == "admin@example.com"

    def test_inserts_without_optional_fields(self):
        db = _mock_db()
        record_feedback(
            property_id=7,
            reason_code="ltv_too_high",
            submitted_by="closer@test.com",
            db=db,
        )
        _, params = db.execute.call_args[0]
        assert params["detail"] is None
        assert params["lender"] is None
        assert params["amount"] is None


# ---------------------------------------------------------------------------
# apply_signal_nudges
# ---------------------------------------------------------------------------

class TestApplySignalNudges:
    def test_returns_nudges_for_valid_code(self):
        db = _mock_db()
        with patch("src.services.underwriting_feedback_service.invalidate_cache") as mock_inv:
            applied = apply_signal_nudges("structural_damage", db)

        expected_count = len(UNDERWRITING_REASON_SIGNAL_NUDGES["structural_damage"])
        assert len(applied) == expected_count
        mock_inv.assert_called_once()

    def test_upserts_into_weight_overrides(self):
        db = _mock_db()
        with patch("src.services.underwriting_feedback_service.invalidate_cache"):
            apply_signal_nudges("flood_zone", db)

        # 1 SELECT + 1 UPSERT per nudge entry
        expected_calls = 2 * len(UNDERWRITING_REASON_SIGNAL_NUDGES["flood_zone"])
        assert db.execute.call_count == expected_calls

    def test_compounds_with_existing_delta(self):
        """New delta = existing + nudge, clamped to [-15, +15]."""
        db = _mock_db(existing_delta=-5.0)
        with patch("src.services.underwriting_feedback_service.invalidate_cache"):
            applied = apply_signal_nudges("ltv_too_high", db)

        # All applied deltas should be existing(-5) + nudge, clamped
        for entry in applied:
            assert entry["delta"] >= -15.0
            assert entry["delta"] <= 15.0

    def test_respects_lower_bound(self):
        """Nudging from already-min delta should stay at -15."""
        db = _mock_db(existing_delta=-14.0)
        with patch("src.services.underwriting_feedback_service.invalidate_cache"):
            applied = apply_signal_nudges("structural_damage", db)

        for entry in applied:
            assert entry["delta"] >= -15.0

    def test_unknown_code_returns_empty(self):
        db = _mock_db()
        with patch("src.services.underwriting_feedback_service.invalidate_cache") as mock_inv:
            applied = apply_signal_nudges("not_a_real_code", db)

        assert applied == []
        mock_inv.assert_not_called()

    def test_source_is_underwriting_feedback(self):
        db = _mock_db()
        with patch("src.services.underwriting_feedback_service.invalidate_cache"):
            apply_signal_nudges("commercial_zoning", db)

        upsert_calls = [
            c for c in db.execute.call_args_list
            if "INSERT INTO scoring_weight_overrides" in str(c)
        ]
        for c in upsert_calls:
            params = c[0][1]
            assert params.get("reason", "").startswith("Underwriting decline:")


# ---------------------------------------------------------------------------
# rescore_property
# ---------------------------------------------------------------------------

class TestRescoreProperty:
    def test_calls_engine_and_persists(self):
        db = MagicMock()

        mock_score_data = {
            "final_cds_score": 61.5,
            "lead_tier": "Gold",
            "vertical_scores": {"wholesalers": 61.5, "fix_flip": 0.0},
        }

        mock_scorer = MagicMock()
        mock_scorer._fetch_properties_by_ids.return_value = [MagicMock()]
        mock_scorer._fetch_signals_for_batch.return_value = {}
        mock_scorer._build_property_bundle.return_value = MagicMock()
        mock_scorer.score_property.return_value = mock_score_data

        with patch(
            "src.services.cds_engine.MultiVerticalScorer",
            return_value=mock_scorer,
        ):
            result = rescore_property(42, "12-3456-789", db)

        mock_scorer._fetch_properties_by_ids.assert_called_once_with([42])
        mock_scorer._fetch_signals_for_batch.assert_called_once_with([42])
        mock_scorer.score_property.assert_called_once()
        mock_scorer.save_score_to_database.assert_called_once_with(mock_score_data)
        db.commit.assert_called_once()
        assert result["final_cds_score"] == 61.5

    def test_raises_if_property_not_found(self):
        db = MagicMock()
        mock_scorer = MagicMock()
        mock_scorer._fetch_properties_by_ids.return_value = []

        with patch(
            "src.services.cds_engine.MultiVerticalScorer",
            return_value=mock_scorer,
        ):
            with pytest.raises(ValueError, match="not found for rescore"):
                rescore_property(999, "MISSING-ID", db)

        mock_scorer.save_score_to_database.assert_not_called()
        db.commit.assert_not_called()


# ---------------------------------------------------------------------------
# Request validator (Pydantic)
# ---------------------------------------------------------------------------

class TestUnderwritingFeedbackRequest:
    def _make_request(self, **kwargs):
        from src.api.underwriting_router import UnderwritingFeedbackRequest
        return UnderwritingFeedbackRequest(**kwargs)

    def test_valid_request(self):
        req = self._make_request(
            parcel_id="12-3456",
            reason_code="ltv_too_high",
            lender_id="PRIME001",
            loan_amount=300000.0,
        )
        assert req.reason_code == "ltv_too_high"
        assert req.reason_detail is None

    def test_invalid_reason_code_raises(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="reason_code"):
            self._make_request(parcel_id="12-3456", reason_code="bad_code")

    def test_all_enum_values_are_valid(self):
        for code in VALID_REASON_CODES:
            req = self._make_request(parcel_id="X", reason_code=code)
            assert req.reason_code == code
