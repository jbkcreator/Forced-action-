from __future__ import annotations
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field

Confidence = Literal["high", "medium", "low"]
RehabSource = Literal["job_estimator", "override"]

# Confidence ordering for propagation — lower index = lower confidence
_CONFIDENCE_RANK: dict[Confidence, int] = {"low": 0, "medium": 1, "high": 2}

# Rehab provenance → confidence: a human override is trusted over an estimate
_REHAB_CONFIDENCE: dict[RehabSource, Confidence] = {
    "job_estimator": "medium",
    "override": "high",
}


def min_confidence(*levels: Confidence) -> Confidence:
    """Return the weakest of the given confidence levels."""
    return min(levels, key=lambda c: _CONFIDENCE_RANK[c])


def rehab_confidence(source: RehabSource) -> Confidence:
    return _REHAB_CONFIDENCE[source]


class Figure(BaseModel):
    """A single computed figure with display formatting and provenance."""
    raw: Decimal
    display: str
    source: str
    confidence: Confidence


class QuoteReadyInput(BaseModel):
    property_id: str
    max_ltc: Decimal = Field(ge=0)
    max_ltv: Decimal = Field(ge=0)

    # Purchase basis — fallback chain:
    # purchase_price → estimated_value → assessed_value_mkt/last_sale_price
    purchase_price: Optional[Decimal] = Field(default=None, ge=0)
    estimated_value: Optional[Decimal] = Field(default=None, ge=0)
    assessed_value_mkt: Optional[Decimal] = Field(default=None, ge=0)
    last_sale_price: Optional[Decimal] = Field(default=None, ge=0)

    # Rehab — pre-filled from job_estimator by caller; caller may override.
    # rehab_source records provenance so the estimate vs override is visible.
    rehab_estimate: Optional[Decimal] = Field(default=None, ge=0)
    rehab_source: RehabSource = "job_estimator"

    # ARV — contract input for WP-8A; comp-derived ARV is WP-8B.
    # arv_confidence lets the caller carry the ARV's provenance strength.
    arv: Optional[Decimal] = Field(default=None, ge=0)
    arv_confidence: Confidence = "high"


class QuoteReadyResult(BaseModel):
    """Stateless deal-math result. No rate/term/commitment fields — by construction."""
    project_cost: Optional[Figure] = None
    proposed_loan: Optional[Figure] = None
    ltc: Optional[Figure] = None
    ltv: Optional[Figure] = None
    missing: list[str] = Field(default_factory=list)
