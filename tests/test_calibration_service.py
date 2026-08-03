"""Calibration cohort-mixing regression tests (REVINT-I1 review fix).

_compute_segment() must cohort its numerator (closed_count) and denominator
(sample_size) identically: both restricted to scores CREATED in the target
month. Before the fix, closed_count was filtered on the history snapshot's
month instead, letting an earlier cohort's closures inflate a later month's
close rate past 100%.

Uses far-future year/month values (2099-xx) so these assertions can never
collide with real production opportunity_scores/opportunity_score_history
rows created under the app's real clock.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.core.models import OpportunityScore, OpportunityScoreHistory
from src.services.calibration_service import _compute_segment

_SEGMENT = "default"

# Distinct months, none of which any real app code could ever have written to.
_EARLIER_MONTH = (2098, 12)
_TARGET_MONTH = (2099, 1)
_LATER_MONTH = (2099, 2)


def _dt(year: int, month: int, day: int = 15) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _make_score(db, thread_id: str, created_at: datetime) -> OpportunityScore:
    score = OpportunityScore(
        opportunity_thread_id=thread_id,
        buyer_entity_id=1,
        segment=_SEGMENT,
        revenue_type="subscription",
        expected_revenue_cents=55_000,
        expected_retained_gross_profit_cents=50_000,
        p_reply=0.5,
        p_close=0.5,
        time_to_cash_days=10,
        josh_minutes_required=2.0,
        nbra_score=25_000.0,
        is_automated=False,
        created_at=created_at,
    )
    db.add(score)
    db.flush()
    return score


def _make_closed_history(db, score: OpportunityScore, snapshot_at: datetime) -> OpportunityScoreHistory:
    h = OpportunityScoreHistory(
        opportunity_score_id=score.id,
        opportunity_thread_id=score.opportunity_thread_id,
        snapshot_at=snapshot_at,
        p_reply=0.9,
        p_close=0.9,
        time_to_cash_days=3,
        nbra_score=999.0,
        reason="closed",
    )
    db.add(h)
    db.flush()
    return h


class TestCalibrationCohortMixing:
    def test_earlier_cohort_closing_this_month_does_not_inflate_rate(self, fresh_db):
        """The bug scenario: a score created LAST month closes THIS month.
        It must not count toward this month's numerator or denominator, and
        this month's rate must never exceed 1.0."""
        earlier_score = _make_score(fresh_db, "OPP-2098-90001", _dt(*_EARLIER_MONTH))
        _make_closed_history(fresh_db, earlier_score, _dt(*_TARGET_MONTH))

        # A real target-month cohort member that never closes.
        _make_score(fresh_db, "OPP-2099-90002", _dt(*_TARGET_MONTH))

        result = _compute_segment(fresh_db, _SEGMENT, *_TARGET_MONTH)
        assert result is not None
        assert result["sample_size"] == 1
        assert result["actual_close_rate"] == 0.0
        assert result["actual_close_rate"] <= 1.0

    def test_score_created_this_month_closing_later_counts_for_this_month(self, fresh_db):
        """A score created this month that only closes NEXT month must still
        count toward THIS month's cohort — calibration is about the
        creation-time prediction, not when the closure snapshot lands."""
        score = _make_score(fresh_db, "OPP-2099-90003", _dt(*_TARGET_MONTH))
        _make_closed_history(fresh_db, score, _dt(*_LATER_MONTH))

        result = _compute_segment(fresh_db, _SEGMENT, *_TARGET_MONTH)
        assert result is not None
        assert result["sample_size"] == 1
        assert result["actual_close_rate"] == 1.0

    def test_same_month_creation_and_close_still_works(self, fresh_db):
        """Baseline case: created and closed in the same month — must keep
        working exactly as before the fix."""
        score = _make_score(fresh_db, "OPP-2099-90004", _dt(*_TARGET_MONTH, day=1))
        _make_closed_history(fresh_db, score, _dt(*_TARGET_MONTH, day=20))

        result = _compute_segment(fresh_db, _SEGMENT, *_TARGET_MONTH)
        assert result is not None
        assert result["sample_size"] == 1
        assert result["actual_close_rate"] == 1.0
