"""
WP-8B ARV engine — pure unit tests.
Seam: compute_arv(ARVInput) -> ARVResult
No DB, no network, no mocks. Fixture in, object out.
"""
from decimal import Decimal

import pytest

from src.services.quote_ready import (
    ARVInput,
    ARVResult,
    CandidateSale,
    SubjectProperty,
    compute_arv,
)
from src.services.quote_ready.arv_config import ARVConfig


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _subject(**kwargs) -> SubjectProperty:
    defaults = dict(
        property_id=1,
        sqft=1500,
        property_use_code="01",
        building_condition=3,
        subdivision="OAK_GROVE",
        hcpa_neighborhood_code="N01",
        zip="33601",
        county="HILLSBOROUGH",
    )
    defaults.update(kwargs)
    return SubjectProperty(**defaults)


def _sale(property_id: int, sale_price: Decimal, *, sqft: int = 1500,
          building_condition: int = 3, qual_cd: str = "Q1",
          sale_yr: int = 2026, sale_mo: int = 3,
          property_use_code: str = "01",
          subdivision: str = "OAK_GROVE",
          hcpa_neighborhood_code: str = "N01",
          zip: str = "33601",
          county: str = "HILLSBOROUGH") -> CandidateSale:
    return CandidateSale(
        property_id=property_id,
        sale_price=sale_price,
        sale_yr=sale_yr,
        sale_mo=sale_mo,
        qual_cd=qual_cd,
        sqft=sqft,
        building_condition=building_condition,
        property_use_code=property_use_code,
        subdivision=subdivision,
        hcpa_neighborhood_code=hcpa_neighborhood_code,
        zip=zip,
        county=county,
    )


def _inp(candidates: list[CandidateSale], subject: SubjectProperty | None = None,
         config: ARVConfig | None = None) -> ARVInput:
    return ARVInput(
        subject=subject or _subject(),
        candidate_sales=candidates,
        as_of_yr=2026,
        as_of_mo=9,
        config=config or ARVConfig(),
    )


# ---------------------------------------------------------------------------
# Slice 1 — three strong same-subdivision comps → high confidence, correct range
# ---------------------------------------------------------------------------

def test_three_strong_comps_high_confidence():
    comps = [
        _sale(10, Decimal("290000")),
        _sale(11, Decimal("300000")),
        _sale(12, Decimal("310000")),
    ]
    result = compute_arv(_inp(comps))

    assert result.arv_unknown is False
    assert result.comp_count == 3
    assert result.confidence == "high"
    assert result.weak_comp is False
    assert result.locality_tier == "subdivision"
    assert result.point == Decimal("300000")
    assert result.low == Decimal("290000")
    assert result.high == Decimal("310000")
    assert result.recency_window_months == 12


# ---------------------------------------------------------------------------
# Slice 2 — one comp only → weak_comp=True, low confidence, low=high=point
# ---------------------------------------------------------------------------

def test_one_comp_weak_and_low_confidence():
    comps = [_sale(10, Decimal("300000"))]
    result = compute_arv(_inp(comps))

    assert result.comp_count == 1
    assert result.weak_comp is True
    assert result.confidence == "low"
    assert result.low == result.high == result.point


# ---------------------------------------------------------------------------
# Slice 3 — unqualified sales excluded → arv_unknown
# ---------------------------------------------------------------------------

def test_unqualified_sales_excluded():
    comps = [
        _sale(10, Decimal("300000"), qual_cd="98"),
        _sale(11, Decimal("310000"), qual_cd="99"),
        _sale(12, Decimal("290000"), qual_cd="U"),
    ]
    result = compute_arv(_inp(comps))

    assert result.arv_unknown is True
    assert result.comp_count == 0


# ---------------------------------------------------------------------------
# Slice 4 — wrong property_use_code → excluded
# ---------------------------------------------------------------------------

def test_wrong_property_use_code_excluded():
    comps = [
        _sale(10, Decimal("300000"), property_use_code="02"),
        _sale(11, Decimal("310000"), property_use_code="03"),
        _sale(12, Decimal("290000"), property_use_code="04"),
    ]
    result = compute_arv(_inp(comps))

    assert result.arv_unknown is True
    assert result.comp_count == 0


# ---------------------------------------------------------------------------
# Slice 5 — sqft outside ±20% → excluded
# ---------------------------------------------------------------------------

def test_sqft_outside_tolerance_excluded():
    # subject sqft=1500; ±20% = [1200, 1800]
    comps = [
        _sale(10, Decimal("300000"), sqft=1000),   # too small
        _sale(11, Decimal("310000"), sqft=2000),   # too large
        _sale(12, Decimal("290000"), sqft=900),    # too small
    ]
    result = compute_arv(_inp(comps))

    assert result.arv_unknown is True


# ---------------------------------------------------------------------------
# Slice 6 — subject property_id in pool → excluded from its own comp set
# ---------------------------------------------------------------------------

def test_subject_excluded_from_own_comp_set():
    subject = _subject(property_id=42)
    comps = [
        _sale(42, Decimal("300000")),  # subject itself — must be excluded
        _sale(10, Decimal("295000")),
        _sale(11, Decimal("305000")),
    ]
    result = compute_arv(_inp(comps, subject=subject))

    # Only 2 comps selected (subject excluded), so weak_comp but not unknown
    assert all(sc.property_id != 42 for sc in result.selected_comps)
    assert result.comp_count == 2


# ---------------------------------------------------------------------------
# Slice 7 — locality widening: no subdivision comps, falls to zip tier
# ---------------------------------------------------------------------------

def test_locality_widening_to_zip():
    subject = _subject(subdivision="OAK_GROVE", hcpa_neighborhood_code="N01", zip="33601")
    comps = [
        # Different subdivision AND different neighborhood, same zip → match only at zip
        _sale(10, Decimal("290000"), subdivision="PINE_RIDGE", hcpa_neighborhood_code="N99", zip="33601"),
        _sale(11, Decimal("300000"), subdivision="PINE_RIDGE", hcpa_neighborhood_code="N99", zip="33601"),
        _sale(12, Decimal("310000"), subdivision="PINE_RIDGE", hcpa_neighborhood_code="N99", zip="33601"),
    ]
    result = compute_arv(_inp(comps, subject=subject))

    assert result.arv_unknown is False
    assert result.locality_tier == "zip"
    assert result.weak_comp is True  # zip tier = weak


# ---------------------------------------------------------------------------
# Slice 8 — recency widening: nothing in 12mo, found in 12-24mo window
# ---------------------------------------------------------------------------

def test_recency_widening_to_24_months():
    # as_of = 2026-09; sales at 2025-06 = 15 months ago → outside 12mo but within 24mo
    comps = [
        _sale(10, Decimal("290000"), sale_yr=2025, sale_mo=6),
        _sale(11, Decimal("300000"), sale_yr=2025, sale_mo=5),
        _sale(12, Decimal("310000"), sale_yr=2025, sale_mo=4),
    ]
    result = compute_arv(_inp(comps))

    assert result.arv_unknown is False
    assert result.recency_window_months == 24


# ---------------------------------------------------------------------------
# Slice 9 — condition adjustment moves value correct direction
# ---------------------------------------------------------------------------

def test_condition_adjustment_direction():
    # subject condition=4, comp condition=2 → delta=+2 → comp adjusted UP
    subject = _subject(building_condition=4)
    comp = _sale(10, Decimal("200000"), sqft=1500, building_condition=2)
    result = compute_arv(_inp([comp], subject=subject))

    sc = result.selected_comps[0]
    # condition_delta = 4-2=2, per_step=0.05 → 10% upward adjustment
    assert sc.condition_adjustment > Decimal("0")
    assert sc.adjusted_value > sc.sale_price


# ---------------------------------------------------------------------------
# Slice 10 — sqft adjustment: larger subject → larger adjusted_value per comp
# ---------------------------------------------------------------------------

def test_sqft_adjustment_scales_with_subject_sqft():
    # comp sqft=1500 @ $200k → price_per_sqft=$133.33
    # subject sqft=1700 → size_normalized = 133.33 * 1700 ≈ $226k
    # subject sqft=1300 → size_normalized = 133.33 * 1300 ≈ $173k
    # Both 1300 and 1700 are within ±20% of comp sqft=1500 ([1200, 1800])
    subject_large = _subject(sqft=1700)
    subject_small = _subject(sqft=1300)
    comp = _sale(10, Decimal("200000"), sqft=1500)

    result_large = compute_arv(_inp([comp], subject=subject_large))
    result_small = compute_arv(_inp([comp], subject=subject_small))

    assert result_large.selected_comps[0].adjusted_value > result_small.selected_comps[0].adjusted_value


# ---------------------------------------------------------------------------
# Slice 11 — zero comps anywhere → arv_unknown with reason
# ---------------------------------------------------------------------------

def test_no_comps_at_all_arv_unknown():
    result = compute_arv(_inp([]))

    assert result.arv_unknown is True
    assert result.unknown_reason == "no_qualified_comps"
    assert result.low is None
    assert result.point is None
    assert result.high is None
    assert result.confidence is None


# ---------------------------------------------------------------------------
# Slice 12 — determinism: same input twice → identical result
# ---------------------------------------------------------------------------

def test_determinism():
    comps = [
        _sale(10, Decimal("290000")),
        _sale(11, Decimal("300000")),
        _sale(12, Decimal("310000")),
    ]
    inp = _inp(comps)
    r1 = compute_arv(inp)
    r2 = compute_arv(inp)

    assert r1.model_dump() == r2.model_dump()


# ---------------------------------------------------------------------------
# Slice 13 — spread > threshold → weak_comp=True even with 3+ comps at subdivision
# ---------------------------------------------------------------------------

def test_high_spread_triggers_weak_comp():
    # Spread > 20%: low=200k, high=320k → spread=(320-200)/270≈44%
    comps = [
        _sale(10, Decimal("200000")),
        _sale(11, Decimal("270000")),
        _sale(12, Decimal("320000")),
    ]
    result = compute_arv(_inp(comps))

    assert result.locality_tier == "subdivision"
    assert result.comp_count == 3
    assert result.weak_comp is True


# ---------------------------------------------------------------------------
# Slice 14 — IQR trimming: outlier trimmed from low/high range (4+ comps)
# ---------------------------------------------------------------------------

def test_iqr_trimming_removes_outlier():
    # 5 comps with one outlier high value — after IQR trimming, high should be lower
    comps = [
        _sale(10, Decimal("280000")),
        _sale(11, Decimal("290000")),
        _sale(12, Decimal("295000")),
        _sale(13, Decimal("305000")),
        _sale(14, Decimal("500000")),  # outlier
    ]
    result = compute_arv(_inp(comps))

    # Outlier 500k should be trimmed; high should be below 500k
    assert result.high < Decimal("500000")


# ---------------------------------------------------------------------------
# Slice 15 — ARVConfig overrides work (custom min_comps=1)
# ---------------------------------------------------------------------------

def test_config_override_min_comps():
    config = ARVConfig(min_comps=1)
    # Only 1 comp in pool — normally weak, but min_comps=1 so it's enough to not be unknown
    comps = [_sale(10, Decimal("300000"))]
    result = compute_arv(_inp(comps, config=config))

    assert result.arv_unknown is False
    assert result.comp_count == 1
    assert result.point == Decimal("300000")
