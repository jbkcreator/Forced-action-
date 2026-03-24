"""
Job Value Estimator — estimates repair/project cost per distressed property.

Uses distress signals, property specs (sq_ft, year_built), and vertical to
produce an estimated job value range (low–high) for each lead.

Estimation approach:
  • Each distress signal type maps to a base cost range per sq_ft.
  • Age and condition multipliers adjust for older properties.
  • Multiple concurrent signals use the highest single estimate (not cumulative).
  • Vertical-specific adjustments reflect different scopes of work.

Called from CDS engine after scoring, persists to Financial.est_repair_cost.

Usage:
    from src.services.job_estimator import estimate_job_value
    result = estimate_job_value(property, distress_types, vertical)
    # result = {"low": 8000, "high": 18000, "display": "$8K – $18K"}
"""

import logging
from datetime import date
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.models import Property

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Base cost per sq_ft ranges by signal type (Florida market, 2024-2026)
# Format: (low_per_sqft, high_per_sqft)
# ---------------------------------------------------------------------------

_SIGNAL_COST_PER_SQFT = {
    # Roofing / damage signals
    "building_permits":    (3.50,  7.00),   # Roof replacement: $7K–$14K on 2,000 sqft
    "roofing_permit":      (3.50,  7.00),   # Same scope as building permits (roof-specific)
    "storm_damage":        (4.00,  8.00),   # Storm damage repair: $8K–$16K
    "insurance_claim":     (5.00, 12.00),   # Insurance jobs: $10K–$25K
    "Fire":                (8.00, 20.00),    # Fire damage: $16K–$40K
    "flood_damage":        (6.00, 15.00),    # Water damage: $12K–$30K
    # Legal/financial signals — estimate via flat rate, not per sqft
    "foreclosures":        (0, 0),
    "tax_delinquencies":   (0, 0),
    "judgment_liens":      (0, 0),
    "irs_tax_liens":       (0, 0),
    "hoa_liens":           (0, 0),
    "mechanics_liens":     (3.00,  6.00),    # Indicates prior repair scope
    "county_code_liens":   (2.00,  5.00),
    "tampa_code_liens":    (2.00,  5.00),
    # Code violations — repair cost depends on severity
    "code_violations":     (2.00,  8.00),    # Wide range: $4K–$16K
    # Proceedings
    "probate":             (0, 0),
    "bankruptcy":          (0, 0),
    "evictions":           (1.50,  4.00),    # Tenant-damaged property cleanup
    # Deeds
    "deed_transfers":      (0, 0),
    "lis_pendens":         (0, 0),
}

# Flat-rate estimates for signals that don't scale with sqft
_SIGNAL_FLAT_RANGES = {
    "foreclosures":      (15000, 50000),    # Rehab/flip scope
    "tax_delinquencies": (5000,  25000),    # Deferred maintenance
    "judgment_liens":    (5000,  20000),    # Property in distress
    "irs_tax_liens":     (5000,  15000),
    "hoa_liens":         (3000,  10000),
    "probate":           (10000, 40000),    # Estate cleanup + deferred maintenance
    "bankruptcy":        (8000,  30000),
    "deed_transfers":    (0, 0),            # Not estimable
    "lis_pendens":       (5000,  25000),
    "roofing_permit":    (7000,  15000),    # Fallback if no sqft available
    "building_permits":  (7000,  15000),    # Fallback if no sqft available
    "insurance_claim":   (10000, 25000),    # Fallback if no sqft available
    "storm_damage":      (8000,  20000),    # Fallback if no sqft available
    "flood_damage":      (10000, 25000),    # Fallback if no sqft available
    "Fire":              (15000, 40000),    # Fallback if no sqft available
    "code_violations":   (4000,  16000),    # Fallback if no sqft available
    "evictions":         (3000,  8000),     # Fallback if no sqft available
}

# Vertical multipliers — scope of work differs by buyer type
_VERTICAL_MULTIPLIER = {
    "roofing":          1.0,     # Standard scope
    "restoration":      1.2,     # Remediation typically larger scope
    "fix_flip":         1.5,     # Full rehab
    "wholesalers":      1.3,     # ARV-based, larger scope assumed
    "attorneys":        0.8,     # Legal scope, not repair
    "public_adjusters": 1.1,     # Insurance claim scope
}

# Age-of-property multiplier (older homes cost more to repair)
_AGE_MULTIPLIER = {
    10:  1.0,    # < 10 years
    20:  1.05,   # 10-20 years
    30:  1.15,   # 20-30 years
    50:  1.25,   # 30-50 years
    999: 1.40,   # 50+ years
}


def _age_multiplier(year_built: Optional[int]) -> float:
    """Return age-based cost multiplier."""
    if not year_built:
        return 1.1  # assume moderate age
    age = date.today().year - year_built
    for threshold, mult in sorted(_AGE_MULTIPLIER.items()):
        if age <= threshold:
            return mult
    return 1.4


def estimate_job_value(
    prop: "Property",
    distress_types: List[str],
    vertical: str = "roofing",
) -> Dict:
    """
    Estimate job value for a distressed property.

    Args:
        prop: Property ORM object (needs sq_ft, year_built, financial)
        distress_types: List of active distress signal types (from CDS engine)
        vertical: Buyer vertical for scope adjustment

    Returns:
        dict with keys: low, high, display, method
        Returns {"low": 0, "high": 0, "display": "N/A", "method": "no_data"} if
        estimation isn't possible.
    """
    if not distress_types:
        return {"low": 0, "high": 0, "display": "N/A", "method": "no_data"}

    sq_ft = float(prop.sq_ft) if prop.sq_ft else None
    year_built = prop.year_built
    age_mult = _age_multiplier(year_built)
    vert_mult = _VERTICAL_MULTIPLIER.get(vertical, 1.0)

    best_low = 0
    best_high = 0
    method = "signal_based"

    for signal in distress_types:
        low_sqft, high_sqft = _SIGNAL_COST_PER_SQFT.get(signal, (0, 0))

        if low_sqft > 0 and sq_ft and sq_ft > 0:
            # Per-sqft estimate
            sig_low = low_sqft * sq_ft * age_mult * vert_mult
            sig_high = high_sqft * sq_ft * age_mult * vert_mult
        else:
            # Fall back to flat rate
            sig_low, sig_high = _SIGNAL_FLAT_RANGES.get(signal, (0, 0))
            sig_low = sig_low * vert_mult
            sig_high = sig_high * vert_mult

        # Use the highest single estimate (signals aren't strictly additive)
        if sig_high > best_high:
            best_low = sig_low
            best_high = sig_high

    # If no sqft-based estimate worked, try assessed value as proxy
    if best_high == 0 and prop.financial:
        assessed = (
            float(prop.financial.assessed_value_mkt)
            if prop.financial.assessed_value_mkt
            else None
        )
        if assessed and assessed > 0:
            # Rule of thumb: distressed repair = 5-15% of market value
            best_low = assessed * 0.05 * vert_mult
            best_high = assessed * 0.15 * vert_mult
            method = "assessed_value_pct"

    if best_high == 0:
        return {"low": 0, "high": 0, "display": "N/A", "method": "no_data"}

    # Round to nearest $500
    best_low = round(best_low / 500) * 500
    best_high = round(best_high / 500) * 500

    # Ensure minimum spread
    if best_low == best_high:
        best_low = int(best_low * 0.8)
    if best_low < 1000:
        best_low = 1000

    # Format display string
    def _fmt(v):
        if v >= 1000:
            return f"${v / 1000:.0f}K"
        return f"${v:,.0f}"

    display = f"{_fmt(best_low)} – {_fmt(best_high)}"

    return {
        "low": int(best_low),
        "high": int(best_high),
        "display": display,
        "method": method,
    }
