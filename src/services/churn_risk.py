"""
Churn Risk scorer — Phase 3 (fa051).

`compute_churn_risk(sub_id, db, now)` is the single entry point. It:
    1. Extracts the four signals from churn_signals.py.
    2. Weights and dampens them into a 0–100 score.
    3. Derives the churn_risk_band.
    4. Projects predicted_inactivity_at from inactivity trajectory.
    5. Builds a plain-English reason string.

Pure function — no DB writes, no sends, no Claude calls.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.orm import Session

from config.churn import (
    BAND_THRESHOLDS,
    DAMPENER_MAX_REDUCTION,
    WEIGHTS,
)
from src.core.models import Subscriber
from src.services.churn_signals import (
    engagement_dampener,
    inactivity_trajectory,
    payment_stress,
    usage_slope,
)
from sqlalchemy import select


def band_for(score: int) -> str:
    """Map 0–100 score to band label. Clips out-of-range input."""
    s = max(0, min(100, int(score)))
    for threshold, label in BAND_THRESHOLDS:
        if s >= threshold:
            return label
    return "low"


def compute_churn_risk(
    sub_id: int,
    db: Session,
    now: Optional[datetime] = None,
) -> dict:
    """Compute full Churn Risk result for one subscriber.

    Returns:
        {
          "score": int 0-100,
          "band": str low/medium/high/very_high,
          "predicted_inactivity_at": datetime | None,
          "reason": str,
          "breakdown": {signal: weighted_contribution, ...},
          "features": {raw signal values for churn_predictions.features},
        }
    """
    reference = now or datetime.now(timezone.utc)

    sub = db.execute(
        select(Subscriber).where(Subscriber.id == sub_id)
    ).scalar_one_or_none()

    if sub is None:
        return {
            "score": 0,
            "band": "low",
            "predicted_inactivity_at": None,
            "reason": "subscriber not found",
            "breakdown": {},
            "features": {},
        }

    inact = inactivity_trajectory(sub_id, db, now=reference)
    slope = usage_slope(sub_id, db, now=reference)
    stress = payment_stress(sub, db)
    dampener = engagement_dampener(sub_id, db, now=reference)

    # Weighted sum of the three risk signals (0–100 range)
    raw_score = (
        WEIGHTS["inactivity_trajectory"] * inact["score"]
        + WEIGHTS["usage_slope"] * slope["score"]
        + WEIGHTS["payment_stress"] * stress["score"]
    ) * 100.0

    # Apply engagement dampener: high engagement reduces the score
    dampener_factor = 1.0 - DAMPENER_MAX_REDUCTION * dampener["score"]
    final_score = int(max(0, min(100, round(raw_score * dampener_factor))))

    band = band_for(final_score)

    # Projected inactivity onset: use the inactivity signal's raw data
    predicted_inactivity_at = _project_inactivity_at(inact["raw"], reference)

    breakdown = {
        "inactivity_trajectory": round(WEIGHTS["inactivity_trajectory"] * inact["score"] * 100, 1),
        "usage_slope":           round(WEIGHTS["usage_slope"] * slope["score"] * 100, 1),
        "payment_stress":        round(WEIGHTS["payment_stress"] * stress["score"] * 100, 1),
        "dampener_reduction":    round((raw_score - final_score), 1),
    }

    reason = _build_reason(inact, slope, stress, dampener, final_score)

    features = {
        "inactivity": inact["raw"],
        "slope":      slope["raw"],
        "stress":     stress["raw"],
        "dampener":   dampener["raw"],
    }

    return {
        "score": final_score,
        "band": band,
        "predicted_inactivity_at": predicted_inactivity_at,
        "reason": reason,
        "breakdown": breakdown,
        "features": features,
    }


# ── Private helpers ────────────────────────────────────────────────────────


def _project_inactivity_at(
    inact_raw: dict,
    reference: datetime,
) -> Optional[datetime]:
    """Estimate when the subscriber will cross the 5-day no-wallet-debit window.

    Uses the inactivity raw data: if days_since + remaining to onset < ONSET_DAYS,
    project forward from reference.

    Returns None if the subscriber is not trending toward onset.
    """
    from config.churn import HORIZON_DAYS

    days_since = inact_raw.get("days_since_last_debit", 0)
    # How many days until they would cross the 5-day onset threshold
    # Only meaningful when days_since is between 2 and 5 (approaching onset)
    onset_days = 5  # matches proactive_save._INACTIVE_MIN
    days_remaining = onset_days - days_since

    if days_remaining < 0:
        # Already past onset window
        return reference

    if days_remaining <= HORIZON_DAYS:
        return reference + timedelta(days=days_remaining)

    return None


def _build_reason(
    inact: dict, slope: dict, stress: dict, dampener: dict, score: int
) -> str:
    """Build a ≤255-char plain-English reason string."""
    parts = []

    if inact["score"] > 0.5:
        days = inact["raw"].get("days_since_last_debit", 0)
        if inact["raw"].get("personalized"):
            med = inact["raw"].get("median_interval_days")
            if med:
                parts.append(f"Inactive {days:.0f}d (normal cadence: {med:.0f}d)")
            else:
                parts.append(f"Inactive {days:.0f}d")
        else:
            parts.append(f"Inactive {days:.0f}d (global baseline)")

    if slope["score"] > 0.3:
        recent = slope["raw"].get("recent_7d_count", 0)
        prior = slope["raw"].get("prior_7d_count", 0)
        parts.append(f"Usage {recent}→{prior} debits (7d decline)")

    if stress["score"] > 0.3:
        flags = []
        if stress["raw"].get("in_grace"):
            flags.append("grace")
        if stress["raw"].get("recovery_day5_sent"):
            flags.append("recovery-d5")
        elif stress["raw"].get("recovery_day3_sent"):
            flags.append("recovery-d3")
        if flags:
            parts.append(f"Payment stress: {'+'.join(flags)}")

    if dampener["score"] > 0.5:
        parts.append("Dampened by recent engagement")

    if not parts:
        parts.append(f"Churn risk score {score}")

    reason = "; ".join(parts)
    return reason[:255]
