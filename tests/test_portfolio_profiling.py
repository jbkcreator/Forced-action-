"""
Unit tests (pure, no DB): the deed-qualification, financing-state, and
evidence-aggregation helpers in src.agents.hunter.portfolio_profiling.

DB-touching integration coverage (real deeds -> real refresh_portfolio_profiling
output) lives in tests/scenarios/test_hunter_profiling_scenarios.py -- this
module's own functions self-commit (matching whale_detection.refresh_whale_flags's
established convention), so per this repo's own precedent
(tests/scenarios/test_hunter_resolution_fixes.py), DB-touching coverage of a
self-committing Hunter function belongs under tests/scenarios/ behind the
`scenario` marker, seeded/cleaned against the real DB -- never under plain
tests/ relying on a fresh_db rollback that a self-commit would silently defeat.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from src.agents.hunter.portfolio_profiling import (
    AcquisitionEvidence,
    DeedRow,
    FLIP_MAX_HOLD_DAYS,
    _aggregate_entity_evidence,
    _financing_state,
    _find_exit,
    _is_financing_deed,
    _is_transfer_deed,
    _qualifies_as_transfer,
)


class TestIsTransferDeed:
    def test_warranty_deed_is_transfer(self):
        assert _is_transfer_deed("Warranty Deed") is True

    def test_quitclaim_is_not_transfer(self):
        assert _is_transfer_deed("Quit Claim Deed") is False

    def test_mortgage_is_not_transfer(self):
        assert _is_transfer_deed("Mortgage") is False

    def test_deed_of_trust_is_not_transfer(self):
        assert _is_transfer_deed("Deed of Trust") is False

    def test_lis_pendens_is_not_transfer(self):
        assert _is_transfer_deed("Lis Pendens") is False

    def test_none_is_transfer(self):
        assert _is_transfer_deed(None) is True


class TestIsFinancingDeed:
    def test_mortgage_is_financing(self):
        assert _is_financing_deed("Mortgage") is True

    def test_warranty_deed_is_not_financing(self):
        assert _is_financing_deed("Warranty Deed") is False

    def test_none_is_not_financing(self):
        assert _is_financing_deed(None) is False


class TestQualifiesAsTransfer:
    def test_qualifying_transfer(self):
        assert _qualifies_as_transfer("Warranty Deed", Decimal("100000")) is True

    def test_nominal_price_excluded(self):
        assert _qualifies_as_transfer("Warranty Deed", Decimal("10")) is False

    def test_null_price_allowed_through(self):
        assert _qualifies_as_transfer("Warranty Deed", None) is True

    def test_quitclaim_excluded_regardless_of_price(self):
        assert _qualifies_as_transfer("Quit Claim Deed", Decimal("100000")) is False


def _mk_row(id_, property_id, record_date, deed_type="Warranty Deed", sale_price=Decimal("100000"),
            mortgage_amount=None, buyer_entity_id=None) -> DeedRow:
    return DeedRow(
        id=id_, property_id=property_id, record_date=record_date, sale_price=sale_price,
        deed_type=deed_type, mortgage_amount=mortgage_amount, buyer_entity_id=buyer_entity_id,
    )


class TestFindExit:
    def test_skips_non_transfer_rows_between_acquisition_and_exit(self):
        """A mortgage row recorded between the acquisition and the real resale
        must not be mistaken for the exit, and must not stop the scan either."""
        group = [
            _mk_row(1, 100, date(2026, 1, 1), deed_type="Warranty Deed"),
            _mk_row(2, 100, date(2026, 1, 2), deed_type="Mortgage"),
            _mk_row(3, 100, date(2026, 6, 1), deed_type="Warranty Deed"),
        ]
        exit_row = _find_exit(0, group)
        assert exit_row is not None
        assert exit_row.id == 3

    def test_no_exit_when_still_held(self):
        group = [_mk_row(1, 100, date(2026, 1, 1))]
        assert _find_exit(0, group) is None

    def test_quitclaim_resale_not_treated_as_exit(self):
        group = [
            _mk_row(1, 100, date(2026, 1, 1)),
            _mk_row(2, 100, date(2026, 2, 1), deed_type="Quit Claim Deed"),
        ]
        assert _find_exit(0, group) is None


class TestFinancingState:
    def test_own_mortgage_amount_means_financed(self):
        acq = _mk_row(1, 100, date(2026, 1, 1), mortgage_amount=Decimal("50000"))
        group = [acq]
        assert _financing_state(acq, group) == "financed"

    def test_correlated_mortgage_deed_means_financed(self):
        acq = _mk_row(1, 100, date(2026, 1, 1))
        mortgage = _mk_row(2, 100, date(2026, 1, 5), deed_type="Mortgage")
        group = [acq, mortgage]
        assert _financing_state(acq, group) == "financed"

    def test_no_correlated_mortgage_means_cash_inferred(self):
        acq = _mk_row(1, 100, date(2026, 1, 1))
        later = _mk_row(2, 100, date(2026, 6, 1))
        group = [acq, later]
        assert _financing_state(acq, group) == "cash_inferred"

    def test_single_deed_property_is_unknown(self):
        """Nothing else recorded for this property to corroborate against --
        must not be silently treated as cash."""
        acq = _mk_row(1, 100, date(2026, 1, 1))
        group = [acq]
        assert _financing_state(acq, group) == "unknown"

    def test_mortgage_outside_correlation_window_does_not_count(self):
        acq = _mk_row(1, 100, date(2026, 1, 1))
        far_mortgage = _mk_row(2, 100, date(2026, 6, 1), deed_type="Mortgage")
        group = [acq, far_mortgage]
        assert _financing_state(acq, group) == "cash_inferred"


class TestAggregateEntityEvidence:
    def test_flipper_evidence(self):
        records = [
            AcquisitionEvidence(property_id=1, acquisition_date=date(2026, 1, 1), hold_days=60, financing_state="cash_inferred"),
            AcquisitionEvidence(property_id=2, acquisition_date=date(2026, 1, 1), hold_days=90, financing_state="cash_inferred"),
        ]
        result = _aggregate_entity_evidence(records, window_days=365)
        assert result["portfolio_evidence"]["exit_within_730_days"] == 2
        assert result["portfolio_evidence"]["still_held_count"] == 0
        assert result["avg_hold_days"] == 75
        assert result["financing_signal"] == "cash_inferred"

    def test_still_held_recent_not_counted_as_hold_evidence(self):
        """An acquisition still held but recent (inside FLIP_MAX_HOLD_DAYS) is
        'too early to tell' -- must not count toward still_held_past_730_days."""
        records = [
            AcquisitionEvidence(
                property_id=1, acquisition_date=date.today() - timedelta(days=30),
                hold_days=None, financing_state="financed",
            ),
        ]
        result = _aggregate_entity_evidence(records, window_days=365)
        assert result["portfolio_evidence"]["still_held_count"] == 1
        assert result["portfolio_evidence"]["still_held_past_730_days"] == 0

    def test_still_held_past_window_counts_as_hold_evidence(self):
        records = [
            AcquisitionEvidence(
                property_id=1, acquisition_date=date.today() - timedelta(days=FLIP_MAX_HOLD_DAYS + 30),
                hold_days=None, financing_state="cash_inferred",
            ),
        ]
        result = _aggregate_entity_evidence(records, window_days=365)
        assert result["portfolio_evidence"]["still_held_past_730_days"] == 1

    def test_unknown_financing_never_boosts_capacity_multiplier(self):
        records = [
            AcquisitionEvidence(property_id=1, acquisition_date=date.today(), hold_days=None, financing_state="unknown"),
        ]
        result = _aggregate_entity_evidence(records, window_days=365)
        assert result["financing_signal"] == "unknown"
        # baseline multiplier (1.0), not the cash-inferred boost (1.5)
        assert result["estimated_annual_acquisition_capacity"] == round(result["cadence_purchases_per_year"] * 1.0)

    def test_wholesale_hold_counts_toward_rapid_resale_bucket(self):
        records = [
            AcquisitionEvidence(property_id=1, acquisition_date=date(2026, 1, 1), hold_days=5, financing_state="cash_inferred"),
        ]
        result = _aggregate_entity_evidence(records, window_days=365)
        assert result["portfolio_evidence"]["exit_within_7_days"] == 1
        assert result["portfolio_evidence"]["exit_within_730_days"] == 1  # a wholesale flip is ALSO a flip
