"""
Unit tests (pure, no DB): src.agents.hunter.buyer_type_classification's
_classify_one, reading portfolio_evidence-shaped dicts directly (the same
shape src.agents.hunter.portfolio_profiling.refresh_portfolio_profiling
persists).

DB-touching integration coverage lives in
tests/scenarios/test_hunter_profiling_scenarios.py -- classify_buyer_types
self-commits (matching whale_detection.refresh_whale_flags's convention), so
per this repo's own precedent (tests/scenarios/test_hunter_resolution_fixes.py)
that coverage belongs under tests/scenarios/ behind the `scenario` marker.
"""
from __future__ import annotations

from src.agents.hunter.buyer_type_classification import (
    FLIPPER_RATIO_MIN,
    INSTITUTIONAL_MIN_LLC_DENSITY,
    INSTITUTIONAL_MIN_PURCHASES,
    WHOLESALER_MIN_COUNT,
    WHOLESALER_MIN_RATIO,
    _classify_one,
)


def _evidence(exit_within_730_days=0, exit_within_7_days=0, still_held_past_730_days=0, acquisition_count=None):
    return {
        "exit_within_730_days": exit_within_730_days,
        "exit_within_7_days": exit_within_7_days,
        "still_held_past_730_days": still_held_past_730_days,
        "acquisition_count": acquisition_count if acquisition_count is not None else (
            exit_within_730_days + still_held_past_730_days
        ),
    }


class TestFlipperVsBuyAndHold:
    def test_majority_flips_is_flipper(self):
        result = _classify_one(_evidence(exit_within_730_days=5, still_held_past_730_days=1), 6, 0)
        assert result["buyer_type"] == "flipper"

    def test_majority_holds_is_buy_and_hold(self):
        result = _classify_one(_evidence(exit_within_730_days=1, still_held_past_730_days=5), 6, 0)
        assert result["buyer_type"] == "buy-and-hold"

    def test_exactly_at_ratio_floor_is_flipper(self):
        # 1/2 == FLIPPER_RATIO_MIN (0.5) -- boundary is inclusive
        assert FLIPPER_RATIO_MIN == 0.5
        result = _classify_one(_evidence(exit_within_730_days=1, still_held_past_730_days=1), 2, 0)
        assert result["buyer_type"] == "flipper"

    def test_normal_investor_with_no_distressed_history_still_classifies(self):
        """The evidence dict carries no notion of 'distressed acquisition' at
        all -- proving H3 classifies off full purchase history, not a
        distressed-only subset."""
        result = _classify_one(_evidence(exit_within_730_days=4, still_held_past_730_days=0), 4, 0)
        assert result["buyer_type"] == "flipper"

    def test_zero_evidence_stays_unclassified(self):
        result = _classify_one(_evidence(), 0, 0)
        assert result is None

    def test_still_held_within_window_excluded_from_ratio(self):
        """portfolio_evidence's still_held_past_730_days already excludes
        recent still-held acquisitions -- a 0/0 ratio (no eligible exits at
        all) must not be misread as 100% flipper or crash on division."""
        result = _classify_one(_evidence(exit_within_730_days=0, still_held_past_730_days=0, acquisition_count=3), 3, 0)
        assert result is None


class TestWholesaler:
    def test_single_rapid_flip_does_not_override_to_wholesaler(self):
        """One fast flip on an otherwise ordinary flipper must not flip the
        whole label -- requires WHOLESALER_MIN_COUNT and WHOLESALER_MIN_RATIO."""
        assert WHOLESALER_MIN_COUNT >= 2
        result = _classify_one(
            _evidence(exit_within_730_days=10, exit_within_7_days=1, still_held_past_730_days=0), 10, 0,
        )
        assert result["buyer_type"] == "flipper"

    def test_real_wholesaler_volume_overrides(self):
        evidence = _evidence(exit_within_730_days=5, exit_within_7_days=3, still_held_past_730_days=0)
        assert 3 >= WHOLESALER_MIN_COUNT
        assert 3 / 5 >= WHOLESALER_MIN_RATIO
        result = _classify_one(evidence, 5, 0)
        assert result["buyer_type"] == "wholesaler"

    def test_count_met_but_ratio_not_met_stays_flipper(self):
        evidence = _evidence(exit_within_730_days=20, exit_within_7_days=2, still_held_past_730_days=0)
        assert 2 >= WHOLESALER_MIN_COUNT
        assert 2 / 20 < WHOLESALER_MIN_RATIO
        result = _classify_one(evidence, 20, 0)
        assert result["buyer_type"] == "flipper"


class TestInstitutional:
    def test_scale_plus_llc_density_is_institutional(self):
        result = _classify_one(
            _evidence(exit_within_730_days=2, still_held_past_730_days=1),
            INSTITUTIONAL_MIN_PURCHASES, INSTITUTIONAL_MIN_LLC_DENSITY,
        )
        assert result["buyer_type"] == "institutional"

    def test_scale_without_llc_density_is_not_institutional(self):
        result = _classify_one(
            _evidence(exit_within_730_days=5, still_held_past_730_days=1),
            INSTITUTIONAL_MIN_PURCHASES, INSTITUTIONAL_MIN_LLC_DENSITY - 1,
        )
        assert result["buyer_type"] != "institutional"

    def test_institutional_overrides_wholesaler(self):
        evidence = _evidence(exit_within_730_days=5, exit_within_7_days=3, still_held_past_730_days=0)
        result = _classify_one(evidence, INSTITUTIONAL_MIN_PURCHASES, INSTITUTIONAL_MIN_LLC_DENSITY)
        assert result["buyer_type"] == "institutional"

    def test_institutional_can_apply_with_zero_eligible_exits(self):
        """A large buyer whose acquisitions are all recent (no exits/holds
        past the window yet) must still classify institutional on scale
        alone, not fall through to None."""
        evidence = _evidence(exit_within_730_days=0, still_held_past_730_days=0, acquisition_count=INSTITUTIONAL_MIN_PURCHASES)
        result = _classify_one(evidence, INSTITUTIONAL_MIN_PURCHASES, INSTITUTIONAL_MIN_LLC_DENSITY)
        assert result["buyer_type"] == "institutional"


class TestEvidencePersisted:
    def test_evidence_snapshot_matches_inputs_used(self):
        result = _classify_one(_evidence(exit_within_730_days=3, still_held_past_730_days=1), 4, 0)
        evidence = result["buyer_type_evidence"]
        assert evidence["flip_count"] == 3
        assert evidence["hold_count"] == 1
        assert evidence["total_purchase_count"] == 4
        assert evidence["is_institutional"] is False
