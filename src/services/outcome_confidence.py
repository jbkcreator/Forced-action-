"""CDE-11 — Outcome Confidence Tier hierarchy.

Pure helpers over the DealOutcome confidence hierarchy: founder_verified >
subscriber_reported > public_record_inferred. No DB, no I/O. The numeric
multipliers live in config/scoring.py (OUTCOME_CONFIDENCE_WEIGHTS), not on the
row. See CONTEXT.md § Outcome Confidence (CDE-11) and ADR 0025.
"""
from __future__ import annotations

from config.scoring import OUTCOME_CONFIDENCE_WEIGHTS

FOUNDER_VERIFIED = "founder_verified"
SUBSCRIBER_REPORTED = "subscriber_reported"
PUBLIC_RECORD_INFERRED = "public_record_inferred"

# The valid tiers ARE the configured weight keys — one source of truth, so a
# tier can never be valid here yet unweighted (or vice versa). Must also match
# the deal_outcomes.confidence_tier CHECK constraint.
TIERS = frozenset(OUTCOME_CONFIDENCE_WEIGHTS)

# The only sources that are NOT public-record inferred. Everything else — every
# Cora Data Engine connector, plus any source we haven't seen — is inferred by
# nature, so the default is the lowest trust tier (safe, needs no edit per
# connector).
_SOURCE_TIERS = {
    "subscriber_tap": SUBSCRIBER_REPORTED,
    "founder_import": FOUNDER_VERIFIED,
}


def tier_weight(tier: str) -> float:
    """Return the learning-loop trust multiplier for a confidence tier."""
    return OUTCOME_CONFIDENCE_WEIGHTS[tier]


def default_tier_for_source(source: str) -> str:
    """Map an outcome source to its default confidence tier.

    Unknown/connector sources default to public_record_inferred.
    """
    return _SOURCE_TIERS.get(source, PUBLIC_RECORD_INFERRED)


def is_inferred(tier: str) -> bool:
    """Whether an outcome is public-record inferred — derived from the tier."""
    return tier == PUBLIC_RECORD_INFERRED


def validate_tier(tier: str) -> None:
    """Raise ValueError if tier is not one of the three known confidence tiers."""
    if tier not in TIERS:
        raise ValueError(f"unknown confidence_tier {tier!r}; expected one of {sorted(TIERS)}")
