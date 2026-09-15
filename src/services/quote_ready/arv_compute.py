"""
WP-8B ARV engine — pure stateless compute.

Entrypoint: compute_arv(ARVInput) -> ARVResult

No DB, no network, no geo. Caller supplies candidate sales pre-joined to
property attributes. Engine filters → adjusts → derives range → scores confidence.
"""
from __future__ import annotations

from decimal import Decimal
from statistics import median
from typing import Optional

from .arv_config import ARVConfig
from .arv_models import (
    ARVInput,
    ARVResult,
    CandidateSale,
    Confidence,
    LocalityTier,
    SelectedComp,
    SubjectProperty,
)

_ZERO = Decimal("0")
_TIER_MAP: dict[str, str] = {
    "subdivision": "subdivision",
    "neighborhood": "hcpa_neighborhood_code",
    "zip": "zip",
    "county": "county",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _months_since(sale_yr: int, sale_mo: int, as_of_yr: int, as_of_mo: int) -> int:
    return (as_of_yr - sale_yr) * 12 + (as_of_mo - sale_mo)


def _locality_value(obj: SubjectProperty | CandidateSale, tier: str) -> Optional[str]:
    attr = _TIER_MAP[tier]
    return getattr(obj, attr, None)


def _filter_comps(
    candidates: list[CandidateSale],
    subject: SubjectProperty,
    tier: str,
    recency_months: int,
    as_of_yr: int,
    as_of_mo: int,
    config: ARVConfig,
) -> list[CandidateSale]:
    subject_locality = _locality_value(subject, tier)
    sqft_low = subject.sqft * (1 - config.sqft_tolerance_pct)
    sqft_high = subject.sqft * (1 + config.sqft_tolerance_pct)

    results = []
    for c in candidates:
        if c.property_id == subject.property_id:
            continue
        if c.qual_cd in config.excluded_qual_codes:
            continue
        if c.property_use_code != subject.property_use_code:
            continue
        if not (sqft_low <= c.sqft <= sqft_high):
            continue
        months_ago = _months_since(c.sale_yr, c.sale_mo, as_of_yr, as_of_mo)
        if months_ago < 0 or months_ago > recency_months:
            continue
        comp_locality = _locality_value(c, tier)
        if subject_locality is None or comp_locality != subject_locality:
            continue
        results.append(c)
    return results


def _adjust_comp(comp: CandidateSale, subject: SubjectProperty, tier: str, config: ARVConfig) -> SelectedComp:
    price_per_sqft = comp.sale_price / Decimal(comp.sqft)
    size_normalized = price_per_sqft * Decimal(subject.sqft)
    sqft_adjustment = size_normalized - comp.sale_price

    condition_delta = subject.building_condition - comp.building_condition
    condition_adjustment = size_normalized * (Decimal(condition_delta) * config.condition_adjustment_per_step)
    adjusted_value = size_normalized + condition_adjustment

    return SelectedComp(
        property_id=comp.property_id,
        sale_price=comp.sale_price,
        sale_yr=comp.sale_yr,
        sale_mo=comp.sale_mo,
        sqft=comp.sqft,
        building_condition=comp.building_condition,
        locality_tier=tier,
        price_per_sqft=price_per_sqft,
        adjusted_value=adjusted_value,
        sqft_adjustment=sqft_adjustment,
        condition_adjustment=condition_adjustment,
    )


def _derive_range(adjusted_values: list[Decimal]) -> tuple[Decimal, Decimal, Decimal]:
    """Return (low, point, high) from adjusted comp values."""
    sorted_vals = sorted(adjusted_values)
    n = len(sorted_vals)
    point = Decimal(str(median([float(v) for v in sorted_vals])))

    if n < 4:
        low = sorted_vals[0]
        high = sorted_vals[-1]
    else:
        q1_idx = n // 4
        q3_idx = (3 * n) // 4
        trimmed = sorted_vals[q1_idx:q3_idx + 1]
        low = trimmed[0] if trimmed else sorted_vals[0]
        high = trimmed[-1] if trimmed else sorted_vals[-1]

    return low, point, high


def _assign_confidence(
    comp_count: int,
    tier: str,
    spread: Decimal,
    config: ARVConfig,
) -> tuple[Confidence, bool]:
    """Return (confidence, weak_comp)."""
    spread_weak = spread > config.spread_threshold
    tier_weak = tier in ("zip", "county")
    count_weak = comp_count < config.min_comps

    weak_comp = count_weak or tier_weak or spread_weak

    if comp_count >= config.min_comps and tier in ("subdivision", "neighborhood") and not weak_comp:
        confidence: Confidence = "high"
    elif tier == "zip" or (not count_weak and spread_weak):
        confidence = "medium"
    else:
        confidence = "low"

    return confidence, weak_comp


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def compute_arv(inp: ARVInput) -> ARVResult:
    config = inp.config
    subject = inp.subject
    tiers = config.locality_tiers

    best: Optional[tuple[list[CandidateSale], str, int]] = None  # (comps, tier, window)

    # Try primary window across all tiers, then extended window across all tiers
    for window in [config.recency_months_primary, config.recency_months_extended]:
        for tier in tiers:
            qualified = _filter_comps(
                inp.candidate_sales, subject, tier, window,
                inp.as_of_yr, inp.as_of_mo, config,
            )
            if len(qualified) >= config.min_comps:
                best = (qualified, tier, window)
                break
        if best is not None:
            break

    # If never found enough, use widest tier + extended window with whatever we have
    if best is None:
        widest_tier = tiers[-1]
        fallback = _filter_comps(
            inp.candidate_sales, subject, widest_tier,
            config.recency_months_extended, inp.as_of_yr, inp.as_of_mo, config,
        )
        if not fallback:
            return ARVResult(
                arv_unknown=True,
                unknown_reason="no_qualified_comps",
                locality_tier="none",
                recency_window_months=config.recency_months_extended,
            )
        best = (fallback, widest_tier, config.recency_months_extended)

    comps, tier, window = best
    selected = [_adjust_comp(c, subject, tier, config) for c in comps]
    adjusted_values = [s.adjusted_value for s in selected]

    low, point, high = _derive_range(adjusted_values)
    spread = (high - low) / point if point > _ZERO else _ZERO
    confidence, weak_comp = _assign_confidence(len(selected), tier, spread, config)

    # Cast tier to LocalityTier
    locality_tier: LocalityTier = tier if tier in ("subdivision", "neighborhood", "zip", "county") else "none"  # type: ignore[assignment]

    return ARVResult(
        low=low,
        point=point,
        high=high,
        confidence=confidence,
        comp_count=len(selected),
        weak_comp=weak_comp,
        locality_tier=locality_tier,
        recency_window_months=window,
        selected_comps=selected,
        arv_unknown=False,
    )
