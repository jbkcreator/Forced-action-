"""
Property matching thresholds — edit here to retune, no loader/task changes needed.

TIER LOGIC (used in BaseLoader._classify_match and rematch_unmatched):
  normalized = raw_rapidfuzz_score / 100.0
  >= auto_match  → matched          (write to destination table)
  >= review_min  → pending_review   (Cora-triaged later)
  <  review_min  → unmatched        (stays in queue)
  llm_verified   → always matched   (LLM explicitly confirmed)

COUNTY OVERRIDES
----------------
Pinellas is a stopgap: post-launch audit (2026-05-21) showed deed/probate match
rates of 4–24%. Real fix is in column_mapper schemas + owner-name normalization;
this widens the pending_review band so Cora can triage borderline candidates
that would otherwise be discarded. auto_match stays strict — never auto-write
a weak match to a destination table.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class MatchingThresholds:
    auto_match: float = 0.92   # score >= this → auto-accepted match
    review_min: float = 0.75   # score >= this → pending_review

    # Floor thresholds passed to find_property_by_* (0–100 int scale).
    # Must equal review_min * 100 so review-band candidates are returned.
    address_floor: int = 75
    owner_name_floor: int = 75
    legal_desc_floor: int = 75


THRESHOLDS = MatchingThresholds()


# Per-county overrides. Counties not listed fall back to THRESHOLDS (defaults).
COUNTY_OVERRIDES: dict[str, MatchingThresholds] = {
    "pinellas": MatchingThresholds(
        auto_match=0.92,          # unchanged — never auto-match weak hits
        review_min=0.65,          # widened pending_review band (was 0.75)
        address_floor=65,
        owner_name_floor=65,
        legal_desc_floor=65,
    ),
}


def for_county(county_id: str | None) -> MatchingThresholds:
    """Return county-specific thresholds, falling back to global defaults."""
    if county_id is None:
        return THRESHOLDS
    return COUNTY_OVERRIDES.get(county_id, THRESHOLDS)
