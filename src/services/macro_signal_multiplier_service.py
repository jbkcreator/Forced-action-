"""A7: Macro-signal distress multiplier service.

Reads the latest FRED MORTGAGE30US value from the macro_signals table and
produces bounded per-signal multipliers that CDS scoring applies after the
base weight is calculated.

Design constraints:
  - No macro signal in DB → neutral multipliers (no scoring change).
  - Rate below threshold → neutral multipliers.
  - Rate at/above threshold → bounded boost per config/macro_signal_rules.json.
  - Multiplier is capped at max_multiplier per rule regardless of rate magnitude.
  - Only signals listed in affected_signals are touched; all others remain at 1.0.
  - Config parse failure → neutral multipliers + WARNING log (never crashes CDS).
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "macro_signal_rules.json"

# Sentinel — empty dict means "no multiplier active; use base weight unchanged"
_NEUTRAL: dict[str, float] = {}


@lru_cache(maxsize=1)
def _load_rules() -> dict:
    """Load and cache macro_signal_rules.json. Returns {} on any parse failure."""
    try:
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            rules = json.load(f)
        if not isinstance(rules, dict):
            raise ValueError("macro_signal_rules.json must be a JSON object")
        return rules
    except Exception as exc:
        logger.warning(
            "[MacroMultiplier] Failed to load config from %s: %s — multipliers disabled",
            _CONFIG_PATH, exc,
        )
        return {}


def _rules() -> dict:
    return _load_rules()


def get_latest_mortgage_rate_context(session: Session) -> Optional[dict]:
    """Fetch the most recent mortgage_30y_fixed observation from macro_signals.

    Returns dict with keys: value (float), observed_at (date), series_id (str).
    Returns None if no row exists.
    """
    row = session.execute(
        text("""
            SELECT value, observed_at, source_series_id
            FROM macro_signals
            WHERE signal_key = 'mortgage_30y_fixed'
              AND source = 'fred'
            ORDER BY observed_at DESC
            LIMIT 1
        """)
    ).mappings().first()

    if row is None:
        return None

    return {
        "value":       float(row["value"]),
        "observed_at": row["observed_at"],
        "series_id":   row["source_series_id"],
    }


def get_macro_distress_multipliers(session: Session) -> dict[str, float]:
    """Compute per-signal-type distress multipliers based on current mortgage rate.

    Returns a dict mapping signal_type → multiplier (float ≥ 1.0).
    Returns an empty dict (neutral — no change) when:
      - macro_signals table has no MORTGAGE30US row, or
      - the latest rate is below the configured threshold, or
      - config/macro_signal_rules.json is missing or malformed.

    The CDS engine treats a missing key as multiplier = 1.0 (no change).
    """
    rules = _rules()
    rule = rules.get("mortgage_30y_fixed")
    if not rule:
        return _NEUTRAL

    threshold: float = float(rule.get("high_rate_threshold", 6.5))
    max_mult: float  = float(rule.get("max_multiplier", 1.15))
    affected: dict   = rule.get("affected_signals", {})

    if not affected:
        return _NEUTRAL

    try:
        ctx = get_latest_mortgage_rate_context(session)
    except Exception as exc:
        logger.warning(
            "[MacroMultiplier] DB lookup failed: %s — returning neutral multipliers", exc
        )
        return _NEUTRAL

    if ctx is None:
        logger.debug("[MacroMultiplier] No mortgage_30y_fixed row in macro_signals — neutral")
        return _NEUTRAL

    rate = ctx["value"]

    if rate < threshold:
        logger.debug(
            "[MacroMultiplier] Rate %.2f%% below threshold %.2f%% — neutral multipliers",
            rate, threshold,
        )
        return _NEUTRAL

    multipliers: dict[str, float] = {}
    for sig_type, configured_mult in affected.items():
        clamped = min(float(configured_mult), max_mult)
        multipliers[sig_type] = clamped

    logger.info(
        "[MacroMultiplier] Rate %.2f%% >= threshold %.2f%% (obs %s) — applying multipliers: %s",
        rate, threshold, ctx["observed_at"], multipliers,
    )
    return multipliers


def apply_macro_multiplier(
    signal_type: str,
    base_weight: float,
    multipliers: dict[str, float],
) -> float:
    """Apply the macro multiplier for signal_type to base_weight.

    Returns base_weight unchanged if signal_type is not in multipliers.
    Result is clamped to [0, 100] to respect CDS weight bounds.
    """
    mult = multipliers.get(signal_type)
    if mult is None or mult == 1.0:
        return base_weight
    boosted = base_weight * mult
    clamped = min(100.0, max(0.0, boosted))
    logger.debug(
        "[MacroMultiplier] %s: base=%.1f x %.3f -> %.1f (clamped=%.1f)",
        signal_type, base_weight, mult, boosted, clamped,
    )
    return clamped
