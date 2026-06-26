"""Pre-4.7: Standalone market pressure / spread calculation service.

Reads from macro_signals. Does NOT modify CDS scoring.
Returns structured context dicts that Sprint 4.7 can consume directly.

All public functions degrade gracefully — missing data returns "unknown" or
neutral context. Census ACS5 data is flagged as baseline-only and is never
treated as a live urgency signal.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# County FIPS → Tampa-St. Pete-Clearwater MSA CBSA code (proxy for county HPI)
# FHFA does not publish county-level HPI in public downloads; MSA is the finest
# available granularity for this market.
COUNTY_TO_MSA: dict[str, str] = {
    "12057": "45294",  # Hillsborough → Tampa, FL (MSAD — FHFA place_id)
    "12103": "45294",  # Pinellas → Tampa, FL (MSAD — FHFA place_id)
}

# Census signals classified by role for Sprint 4.7 readiness
CENSUS_SIGNAL_ROLES: dict[str, str] = {
    "total_housing_units":    "baseline_context",
    "occupied_housing_units": "baseline_context",
    "vacant_housing_units":   "slow_moving_risk_factor",
    "owner_occupied_units":   "baseline_context",
    "renter_occupied_units":  "baseline_context",
    "median_gross_rent":      "slow_moving_risk_factor",
    "median_home_value":      "slow_moving_risk_factor",
    "median_household_income":"slow_moving_risk_factor",
    "unemployed_civilian":    "slow_moving_risk_factor",
    "total_population":       "baseline_context",
}

_PRESSURE_THRESHOLDS = {
    "mortgage_rate": {
        "elevated":  0.25,   # spread > +0.25pp → elevated
        "high":      0.75,   # spread > +0.75pp → high
        "declining": -0.25,  # spread < -0.25pp → declining
    },
    "hpi": {
        "elevated":  5.0,    # +5 index points → elevated
        "high":      15.0,
        "declining": -5.0,
    },
    "unemployment": {
        "rising":    0.3,    # +0.3pp → rising
        "elevated":  0.8,    # +0.8pp → elevated
        "declining": -0.3,
    },
}


def get_trailing_average(
    session: Session,
    signal_key: str,
    geography_scope: str,
    geography_id: str,
    months: int = 12,
) -> Optional[float]:
    """Return the simple mean of a signal over the trailing N months.

    Returns None if fewer than 3 observations exist in the window
    (not enough data to be meaningful).
    """
    cutoff = date.today() - relativedelta(months=months)
    row = session.execute(
        text("""
            SELECT AVG(value::numeric), COUNT(*)
            FROM macro_signals
            WHERE signal_key       = :signal_key
              AND geography_scope  = :geo_scope
              AND geography_id     = :geo_id
              AND observed_at     >= :cutoff
        """),
        {
            "signal_key": signal_key,
            "geo_scope":  geography_scope,
            "geo_id":     geography_id,
            "cutoff":     cutoff,
        },
    ).fetchone()

    if row is None or row[1] < 3:
        return None
    return float(row[0])


def _latest_value(
    session: Session,
    signal_key: str,
    geography_scope: str,
    geography_id: str,
) -> Optional[tuple[float, date]]:
    """Return (value, observed_at) of the most recent row, or None."""
    row = session.execute(
        text("""
            SELECT value::numeric, observed_at
            FROM macro_signals
            WHERE signal_key      = :signal_key
              AND geography_scope = :geo_scope
              AND geography_id    = :geo_id
            ORDER BY observed_at DESC
            LIMIT 1
        """),
        {"signal_key": signal_key, "geo_scope": geography_scope, "geo_id": geography_id},
    ).fetchone()
    if row is None:
        return None
    return float(row[0]), row[1]


def _classify_rate_pressure(spread: float, thresholds: dict) -> str:
    """Classify spread into a pressure label using named thresholds.

    Supports: "high", "elevated", "rising" (lower first-level positive),
    and "declining". Checked in priority order.
    """
    if spread >= thresholds.get("high", float("inf")):
        return "high"
    if spread >= thresholds.get("elevated", float("inf")):
        return "elevated"
    if spread >= thresholds.get("rising", float("inf")):
        return "rising"
    if spread <= thresholds.get("declining", float("-inf")):
        return "declining"
    return "neutral"


def calculate_rate_spread(session: Session, trailing_months: int = 12) -> dict:
    """National mortgage-rate spread: current vs trailing average.

    Returns:
        {current, trailing_avg, spread, pressure, observed_at}
        or {pressure: "unknown"} if data is missing.
    """
    result = _latest_value(session, "mortgage_rate_30yr", "national", "US")
    if result is None:
        logger.debug("[MarketPressure] No MORTGAGE30US data — rate spread unknown")
        return {"pressure": "unknown", "signal_key": "mortgage_rate_30yr"}

    current, obs_at = result
    trailing_avg = get_trailing_average(
        session, "mortgage_rate_30yr", "national", "US", months=trailing_months
    )
    if trailing_avg is None:
        return {
            "current": round(current, 2),
            "observed_at": str(obs_at),
            "trailing_avg": None,
            "spread": None,
            "pressure": "unknown",
        }

    spread = round(current - trailing_avg, 4)
    pressure = _classify_rate_pressure(spread, _PRESSURE_THRESHOLDS["mortgage_rate"])

    return {
        "current":      round(current, 2),
        "observed_at":  str(obs_at),
        "trailing_avg": round(trailing_avg, 2),
        "spread":       spread,
        "pressure":     pressure,
    }


def calculate_county_hpi_spread(
    session: Session,
    county_fips: str,
    trailing_months: int = 12,
) -> dict:
    """County HPI spread using FHFA MSA-level data as proxy.

    FHFA does not publish county-level HPI in public downloads. Falls back to
    the MSA (metro) HPI for the county's CBSA via COUNTY_TO_MSA mapping.
    Returns unknown context if no mapping or no MSA data exists.
    """
    cbsa = COUNTY_TO_MSA.get(county_fips)
    if cbsa is None:
        logger.debug("[MarketPressure] No MSA mapping for county FIPS %s", county_fips)
        return {"pressure": "unknown", "signal_key": "house_price_index", "county_fips": county_fips}

    result = _latest_value(session, "house_price_index", "metro", cbsa)
    if result is None:
        logger.debug("[MarketPressure] No MSA HPI for CBSA %s (county %s)", cbsa, county_fips)
        return {"pressure": "unknown", "signal_key": "house_price_index", "county_fips": county_fips, "cbsa": cbsa}

    current, obs_at = result
    trailing_avg = get_trailing_average(
        session, "house_price_index", "metro", cbsa, months=trailing_months
    )
    if trailing_avg is None:
        return {
            "county_fips":  county_fips,
            "cbsa":         cbsa,
            "current":      round(current, 2),
            "observed_at":  str(obs_at),
            "trailing_avg": None,
            "spread":       None,
            "pressure":     "unknown",
        }

    spread = round(current - trailing_avg, 4)
    pressure = _classify_rate_pressure(spread, _PRESSURE_THRESHOLDS["hpi"])

    return {
        "county_fips":  county_fips,
        "cbsa":         cbsa,
        "current":      round(current, 2),
        "observed_at":  str(obs_at),
        "trailing_avg": round(trailing_avg, 2),
        "spread":       spread,
        "pressure":     pressure,
    }


def calculate_county_unemployment_spread(
    session: Session,
    county_fips: str,
    trailing_months: int = 12,
) -> dict:
    """County unemployment spread: latest BLS LAUS rate vs trailing average.

    Returns neutral context if no county unemployment data is available.
    """
    result = _latest_value(session, "county_unemployment_rate", "county", county_fips)
    if result is None:
        logger.debug("[MarketPressure] No county unemployment for FIPS %s", county_fips)
        return {
            "pressure": "unknown",
            "signal_key": "county_unemployment_rate",
            "county_fips": county_fips,
        }

    current, obs_at = result
    trailing_avg = get_trailing_average(
        session, "county_unemployment_rate", "county", county_fips, months=trailing_months
    )
    if trailing_avg is None:
        return {
            "county_fips":  county_fips,
            "current":      round(current, 2),
            "observed_at":  str(obs_at),
            "trailing_avg": None,
            "spread":       None,
            "pressure":     "unknown",
        }

    spread = round(current - trailing_avg, 4)
    pressure = _classify_rate_pressure(spread, _PRESSURE_THRESHOLDS["unemployment"])

    return {
        "county_fips":  county_fips,
        "current":      round(current, 2),
        "observed_at":  str(obs_at),
        "trailing_avg": round(trailing_avg, 2),
        "spread":       spread,
        "pressure":     pressure,
    }


def _overall_pressure(components: list[str]) -> str:
    """Derive overall market pressure label from component pressure strings."""
    known = [p for p in components if p not in ("unknown", "neutral")]
    if not known:
        return "neutral"
    if "high" in known:
        return "high"
    if known.count("elevated") >= 2:
        return "elevated"
    if "elevated" in known:
        return "moderate"
    if known.count("rising") >= 2:
        return "moderate"
    if "declining" in known:
        return "declining"
    return "neutral"


def get_county_market_pressure_context(
    session: Session,
    county_fips: str,
    trailing_months: int = 12,
) -> dict:
    """Aggregate market pressure context for a county.

    Combines national mortgage-rate spread, county FHFA HPI spread, and
    county BLS LAUS unemployment spread into a single context dict.

    Census ACS5 is intentionally excluded from urgency signals here —
    it is annual/lagged and is listed separately as baseline context only.

    Returns a dict safe for Sprint 4.7 consumption. Never raises.
    """
    try:
        mortgage = calculate_rate_spread(session, trailing_months)
        hpi      = calculate_county_hpi_spread(session, county_fips, trailing_months)
        unemp    = calculate_county_unemployment_spread(session, county_fips, trailing_months)

        census_baseline = _get_census_baseline(session, county_fips)

        component_pressures = [
            mortgage.get("pressure", "unknown"),
            hpi.get("pressure", "unknown"),
            unemp.get("pressure", "unknown"),
        ]

        return {
            "county_fips": county_fips,
            "mortgage_rate": {
                "current":      mortgage.get("current"),
                "trailing_avg": mortgage.get("trailing_avg"),
                "spread":       mortgage.get("spread"),
                "pressure":     mortgage.get("pressure", "unknown"),
                "observed_at":  mortgage.get("observed_at"),
            },
            "county_hpi": {
                "current":      hpi.get("current"),
                "trailing_avg": hpi.get("trailing_avg"),
                "spread":       hpi.get("spread"),
                "pressure":     hpi.get("pressure", "unknown"),
                "observed_at":  hpi.get("observed_at"),
            },
            "county_unemployment": {
                "current":      unemp.get("current"),
                "trailing_avg": unemp.get("trailing_avg"),
                "spread":       unemp.get("spread"),
                "pressure":     unemp.get("pressure", "unknown"),
                "observed_at":  unemp.get("observed_at"),
            },
            "census_baseline": census_baseline,
            "overall_market_pressure": _overall_pressure(component_pressures),
        }
    except Exception as exc:
        logger.error("[MarketPressure] Unexpected error for county %s: %s", county_fips, exc)
        return {
            "county_fips": county_fips,
            "overall_market_pressure": "unknown",
            "error": str(exc),
        }


def _get_census_baseline(session: Session, county_fips: str) -> dict:
    """Return the latest Census ACS5 baseline values for a county.

    These are slow-moving annual signals and must NOT be used as live
    urgency indicators in Sprint 4.7 scoring.
    """
    rows = session.execute(
        text("""
            SELECT signal_key, value::numeric, observed_at
            FROM macro_signals
            WHERE source        = 'census_acs5'
              AND geography_id  = :geo_id
              AND geography_scope = 'county'
            ORDER BY signal_key, observed_at DESC
        """),
        {"geo_id": county_fips},
    ).fetchall()

    if not rows:
        return {"available": False}

    # One row per signal_key (latest observation)
    seen: set[str] = set()
    baseline: dict = {"available": True, "signals": {}, "note": "annual ACS5 — baseline context only, not live urgency"}
    for row in rows:
        key = row[0]
        if key in seen:
            continue
        seen.add(key)
        role = CENSUS_SIGNAL_ROLES.get(key, "baseline_context")
        baseline["signals"][key] = {
            "value":       float(row[1]),
            "observed_at": str(row[2]),
            "role":        role,
        }

    return baseline
