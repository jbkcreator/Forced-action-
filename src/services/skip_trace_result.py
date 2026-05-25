"""Shared result type and confidence scorer for the skip trace waterfall."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class SkipTraceResult:
    """Standardised result returned by every skip trace provider."""

    provider: str
    success: bool
    skipped: bool      # True when API key absent — tier is bypassed cleanly
    confidence: float  # 0.0–1.0; waterfall stops when >= threshold
    cost_cents: int    # 0 on miss or skipped; actual provider cost on success

    mobile_phone: Optional[str] = None
    landline: Optional[str] = None
    email: Optional[str] = None
    mailing_address: Optional[str] = None
    relative_contacts: Optional[dict] = None
    raw_metadata: Optional[dict] = None
    error: Optional[str] = None


def compute_confidence(
    mobile_phone: Optional[str],
    landline: Optional[str],
    email: Optional[str],
    mailing_address: Optional[str],
    phone_reachability_score: Optional[int] = None,
) -> float:
    """
    Score a provider result 0.0–1.0 based on result richness.

      +0.50  any contact found (phone or email)
      +0.20  mobile phone present
      +0.15  phone reachability score >= 70 (BatchData/IDI return this per-number)
      +0.10  email present
      +0.05  mailing address present
    Capped at 1.00.
    """
    if not mobile_phone and not landline and not email:
        return 0.0

    score = 0.50
    if mobile_phone:
        score += 0.20
    if phone_reachability_score is not None and phone_reachability_score >= 70:
        score += 0.15
    if email:
        score += 0.10
    if mailing_address:
        score += 0.05
    return min(score, 1.0)
