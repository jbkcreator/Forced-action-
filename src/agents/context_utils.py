"""
Personalization context utilities for Cora SMS graphs.

Pure Python — no I/O, no DB calls. Transforms raw segment/score/recency
values into the semantic labels Sonnet prompts need to produce materially
different copy across trade/county/behavior combinations.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

# ── County display names ─────────────────────────────────────────────────────
COUNTY_DISPLAY_NAMES: dict[str, str] = {
    "hillsborough": "Hillsborough County",
    "pinellas": "Pinellas County",
    "pasco": "Pasco County",
    "polk": "Polk County",
    "manatee": "Manatee County",
    "sarasota": "Sarasota County",
}


def county_display_name(county_id: Optional[str]) -> str:
    """Return human-readable county name, or 'your county' if unknown."""
    if not county_id:
        return "your county"
    return COUNTY_DISPLAY_NAMES.get(county_id.lower(), county_id.replace("_", " ").title())


# ── Revenue signal score band ────────────────────────────────────────────────

def score_to_band(score: int) -> str:
    """
    Convert 0-100 revenue signal score to semantic band label.
      80-100 → very_high
      60-79  → high
      30-59  → medium
      0-29   → low
    """
    if score >= 80:
        return "very_high"
    if score >= 60:
        return "high"
    if score >= 30:
        return "medium"
    return "low"


# ── Last-action recency ───────────────────────────────────────────────────────

def days_to_recency_band(days: Optional[int]) -> str:
    """
    Convert days-since-last-action to semantic recency band.
      0        → same_day
      1-3      → recent_1_3_days
      4-7      → cooling_4_7_days
      8+       → stale_8_plus_days
      None     → unknown
    """
    if days is None:
        return "unknown"
    if days == 0:
        return "same_day"
    if days <= 3:
        return "recent_1_3_days"
    if days <= 7:
        return "cooling_4_7_days"
    return "stale_8_plus_days"


def recency_from_timestamp(ts: Optional[datetime]) -> tuple[Optional[int], str]:
    """
    Given a last-action timestamp, return (days_since, recency_band).
    If ts is None returns (None, 'unknown').
    """
    if ts is None:
        return None, "unknown"
    now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    days = max(0, (now - ts).days)
    return days, days_to_recency_band(days)


# ── Segment / score → enriched personalization block ────────────────────────

def build_personalization_fields(
    profile: dict,
    segment_data: dict,
    raw_score: int,
) -> dict:
    """
    Return a flat dict of personalization fields ready to merge into a
    Cora render context. Accepts the dicts from get_subscriber_profile and
    get_segment_and_score (both may be empty — defaults are safe).

    Returned keys:
      county_id                  — raw county id from profile
      county_name                — human-readable display name
      behavioral_segment         — 8-bucket segment label
      revenue_signal_score       — 0-100 integer
      revenue_signal_score_band  — low/medium/high/very_high
      days_since_last_action     — int or None
      last_action_recency_band   — same_day/recent_1_3_days/cooling_4_7_days/stale_8_plus_days/unknown
    """
    county_id = profile.get("county_id") or ""
    behavioral_segment = segment_data.get("segment") or "unknown"

    # Prefer the pre-computed band stored by fa037; fall back to computing it.
    score_band = segment_data.get("revenue_signal_band") or score_to_band(raw_score)

    # Parse last_significant_action_at (ISO string from get_segment_and_score).
    last_action_ts = None
    ts_str = segment_data.get("last_significant_action_at")
    if ts_str:
        try:
            last_action_ts = datetime.fromisoformat(ts_str)
        except (ValueError, TypeError):
            pass

    days_since_last_action, recency_band = recency_from_timestamp(last_action_ts)

    return {
        "county_id": county_id,
        "county_name": county_display_name(county_id or None),
        "behavioral_segment": behavioral_segment,
        "revenue_signal_score": raw_score,
        "revenue_signal_score_band": score_band,
        "days_since_last_action": days_since_last_action if days_since_last_action is not None else "unknown",
        "last_action_recency_band": recency_band,
    }
