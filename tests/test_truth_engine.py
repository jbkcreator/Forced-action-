"""Unit tests for the M6 Truth Engine grading logic and routing config.

These exercise the pure `assign_grade` function and `config.grading` helpers — no
DB required. The §8A worked example is the headline acceptance case.
"""
from datetime import datetime, timezone

import pytest

from config.grading import (
    GRADE_CHANNEL_ROUTING,
    GRADE_ORDER,
    compute_cohort_key,
    grade_rank,
    lower_grade,
    primary_channel,
)
from src.services.truth_engine import assign_grade

# Mirrors the grade_thresholds seed (CDS 0–100, contactability 0–1).
THRESHOLDS = {
    "Ultra":     {"cds_min": 85, "cds_max": None, "contactability_min": 0.40, "requires_mobile_consent": True},
    "Platinum":  {"cds_min": 70, "cds_max": 84,   "contactability_min": 0.25, "requires_mobile_consent": False},
    "Gold":      {"cds_min": 50, "cds_max": 69,   "contactability_min": 0.12, "requires_mobile_consent": False},
    "Silver":    {"cds_min": 30, "cds_max": 49,   "contactability_min": 0.05, "requires_mobile_consent": False},
    "Bronze":    {"cds_min": 15, "cds_max": 29,   "contactability_min": None, "requires_mobile_consent": False},
    "sub_grade": {"cds_min": None, "cds_max": 14,  "contactability_min": None, "requires_mobile_consent": False},
}


class TestAssignGrade:
    def test_worked_example_8a(self):
        # CDS 0.72 + realized contactability 28% → Platinum (spec §8A).
        r = assign_grade(72, 0.28, contactable=True, has_mobile_consent=True, thresholds=THRESHOLDS)
        assert r["grade"] == "Platinum"
        assert r["contactability_flag"] is False
        assert primary_channel(r["grade"]) == "loan_lane"

    def test_s1_state_path_no_rate(self):
        # No rate, contactable → grade at the CDS band, floor dormant.
        r = assign_grade(72, None, contactable=True, has_mobile_consent=True, thresholds=THRESHOLDS)
        assert r["grade"] == "Platinum"
        assert r["contactability_flag"] is False

    def test_low_rate_pulls_down_and_flags(self):
        # CDS says Platinum, but 5% contactability only clears Silver's floor.
        r = assign_grade(72, 0.05, contactable=True, has_mobile_consent=True, thresholds=THRESHOLDS)
        assert r["grade"] == "Silver"
        assert r["contactability_flag"] is True

    def test_high_rate_never_promotes_above_cds_band(self):
        r = assign_grade(40, 0.90, contactable=True, has_mobile_consent=True, thresholds=THRESHOLDS)
        assert r["grade"] == "Silver"
        assert r["contactability_flag"] is False

    def test_non_contactable_state_path_subgrade(self):
        r = assign_grade(72, None, contactable=False, has_mobile_consent=False, thresholds=THRESHOLDS)
        assert r["grade"] == "sub_grade"
        assert r["contactability_flag"] is False

    def test_ultra_capped_without_mobile_consent(self):
        # Qualifies for Ultra by score+rate, but no mobile/consent → capped at Platinum.
        r = assign_grade(90, 0.45, contactable=True, has_mobile_consent=False, thresholds=THRESHOLDS)
        assert r["grade"] == "Platinum"
        # Demotion is a consent cap, not a contactability gap → flag stays False.
        assert r["contactability_flag"] is False

    def test_ultra_full_qualification(self):
        r = assign_grade(90, 0.45, contactable=True, has_mobile_consent=True, thresholds=THRESHOLDS)
        assert r["grade"] == "Ultra"

    def test_subgrade_low_cds(self):
        r = assign_grade(10, None, contactable=True, has_mobile_consent=False, thresholds=THRESHOLDS)
        assert r["grade"] == "sub_grade"

    @pytest.mark.parametrize("score,expected", [
        (84.5, "Platinum"),   # between Platinum [70,84] and Ultra [85,] — must NOT fall to sub_grade
        (69.5, "Gold"),       # between Gold and Platinum
        (49.5, "Silver"),
        (29.5, "Bronze"),
        (14.5, "sub_grade"),  # below Bronze's 15 floor
        (84.99, "Platinum"),
        (85.0, "Ultra"),      # exact lower bound
        (70.0, "Platinum"),
    ])
    def test_fractional_scores_never_fall_through_band_gaps(self, score, expected):
        # State path (contactable, no rate) isolates the CDS-band logic.
        r = assign_grade(score, None, contactable=True, has_mobile_consent=True, thresholds=THRESHOLDS)
        assert r["grade"] == expected


class TestHasMobileConsent:
    def _p(self, mobile, consent):
        return {"enriched_contact": {"mobile": mobile}, "channel_consent": consent}

    def test_sms_consent_granted(self):
        from src.services.truth_engine import _has_mobile_consent
        assert _has_mobile_consent(self._p("8135551234", {"sms": True})) is True

    def test_call_consent_granted(self):
        from src.services.truth_engine import _has_mobile_consent
        assert _has_mobile_consent(self._p("8135551234", {"call": True})) is True

    def test_consent_false_or_missing(self):
        from src.services.truth_engine import _has_mobile_consent
        assert _has_mobile_consent(self._p("8135551234", {"sms": False})) is False
        assert _has_mobile_consent(self._p("8135551234", {})) is False

    def test_no_mobile_blocks_even_with_consent(self):
        from src.services.truth_engine import _has_mobile_consent
        assert _has_mobile_consent(self._p(None, {"sms": True})) is False

    def test_unexpected_nested_shape_fails_closed(self):
        from src.services.truth_engine import _has_mobile_consent
        assert _has_mobile_consent(self._p("8135551234", {"sms": {"granted": True}})) is False


class TestGradingConfig:
    def test_all_grades_route(self):
        for g in ("Ultra", "Platinum", "Gold", "Silver", "Bronze", "sub_grade"):
            assert g in GRADE_CHANNEL_ROUTING

    def test_primary_channels(self):
        assert primary_channel("Gold") == "contractor_subscription"
        assert primary_channel("Bronze") == "free_hand_delivered"
        assert primary_channel("sub_grade") == "recycle_suppress"
        assert primary_channel("nonexistent") == "recycle_suppress"

    def test_grade_order_and_rank(self):
        assert GRADE_ORDER == ["sub_grade", "Bronze", "Silver", "Gold", "Platinum", "Ultra"]
        assert grade_rank("Ultra") > grade_rank("Bronze")
        assert lower_grade("Platinum", "Silver") == "Silver"
        assert lower_grade("Bronze", "Ultra") == "Bronze"

    def test_compute_cohort_key(self):
        assert compute_cohort_key("Gold", "hillsborough", "tracerfy") == "Gold|hillsborough|tracerfy"
        assert compute_cohort_key(None, None, None) == "unknown|unknown|unknown"
        assert compute_cohort_key("Gold", "", "") == "Gold|unknown|unknown"


# ── DB round-trip (skips unless DATABASE_URL set AND the M6 migration applied) ──

def _m6_ready(session) -> bool:
    from sqlalchemy import text
    try:
        return session.execute(text("SELECT to_regclass('public.verdicts')")).scalar() is not None
    except Exception:
        return False


class TestGradeThresholdSeed:
    def test_seed_rows(self, fresh_db):
        if not _m6_ready(fresh_db):
            pytest.skip("M6 migration (fa091) not applied")
        from sqlalchemy import text
        rows = fresh_db.execute(text(
            "SELECT grade, cds_min, cds_max, contactability_min, requires_mobile_consent "
            "FROM grade_thresholds"
        )).mappings().all()
        by_grade = {r["grade"]: r for r in rows}
        assert len(by_grade) == 6
        assert by_grade["Ultra"]["cds_min"] == 85
        assert float(by_grade["Ultra"]["contactability_min"]) == 0.40
        assert by_grade["Ultra"]["requires_mobile_consent"] is True
        assert by_grade["sub_grade"]["cds_max"] == 14


class TestGradeProspectRoundTrip:
    """End-to-end write path via fresh_db (nested transaction — auto-rolled-back)."""

    def _seed(self, db, *, score, state="contactable", parcel="M6-TEST"):
        from src.core.models import DistressScore, Property, Prospect
        prop = Property(parcel_id=parcel)
        db.add(prop)
        db.flush()
        db.add(DistressScore(
            property_id=prop.id, score_date=datetime.now(timezone.utc),
            final_cds_score=score, lead_tier="Platinum", county_id="hillsborough",
        ))
        prospect = Prospect(property_id=prop.id, contactability_state=state)
        db.add(prospect)
        db.flush()
        return str(prospect.prospect_id)

    def test_grade_prospect_writes_verdict_and_event(self, fresh_db):
        if not _m6_ready(fresh_db):
            pytest.skip("M6 migration (fa091) not applied")
        from sqlalchemy import text
        from src.services.truth_engine import grade_prospect

        pid = self._seed(fresh_db, score=72, parcel="M6-TEST-RT1")
        result = grade_prospect(fresh_db, pid)

        assert result is not None
        assert result["grade"] == "Platinum"
        assert result["routed_channel"] == "loan_lane"
        assert result["contactability_flag"] is False

        v = fresh_db.execute(text(
            "SELECT grade, routed_channel, contributing_factors FROM verdicts WHERE prospect_id = CAST(:p AS uuid)"
        ), {"p": pid}).mappings().one()
        assert v["grade"] == "Platinum"
        assert v["contributing_factors"]["cds_score"] == 72

        evt = fresh_db.execute(text(
            "SELECT event_type, payload FROM events WHERE prospect_id = CAST(:p AS uuid) AND event_type = 'truth.verdict'"
        ), {"p": pid}).mappings().one()
        assert evt["payload"]["grade"] == "Platinum"

    def test_no_cds_score_is_held(self, fresh_db):
        if not _m6_ready(fresh_db):
            pytest.skip("M6 migration (fa091) not applied")
        from sqlalchemy import text
        from src.core.models import Property, Prospect
        from src.services.truth_engine import grade_prospect

        prop = Property(parcel_id="M6-TEST-HELD")
        fresh_db.add(prop)
        fresh_db.flush()
        prospect = Prospect(property_id=prop.id, contactability_state="contactable")
        fresh_db.add(prospect)
        fresh_db.flush()
        pid = str(prospect.prospect_id)

        result = grade_prospect(fresh_db, pid)
        assert result is not None
        assert result.get("held") is True
        count = fresh_db.execute(text(
            "SELECT COUNT(*) FROM verdicts WHERE prospect_id = CAST(:p AS uuid)"
        ), {"p": pid}).scalar()
        assert count == 0

    def test_record_sub_grade(self, fresh_db):
        if not _m6_ready(fresh_db):
            pytest.skip("M6 migration (fa091) not applied")
        from sqlalchemy import text
        from src.services.truth_engine import record_sub_grade

        pid = self._seed(fresh_db, score=72, state="exhausted", parcel="M6-TEST-SUB")
        result = record_sub_grade(fresh_db, pid, reason="enrichment_failed")
        assert result is not None
        assert result["grade"] == "sub_grade"
        assert result["routed_channel"] == "recycle_suppress"
        v = fresh_db.execute(text(
            "SELECT grade FROM verdicts WHERE prospect_id = CAST(:p AS uuid)"
        ), {"p": pid}).mappings().one()
        assert v["grade"] == "sub_grade"
