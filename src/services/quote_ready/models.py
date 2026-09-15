from __future__ import annotations
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field

Confidence = Literal["high", "medium", "low"]

# Confidence ordering for propagation — lower index = lower confidence
_CONFIDENCE_RANK: dict[Confidence, int] = {"low": 0, "medium": 1, "high": 2}


def min_confidence(a: Confidence, b: Confidence) -> Confidence:
    """Return the weaker of two confidence levels."""
    return a if _CONFIDENCE_RANK[a] <= _CONFIDENCE_RANK[b] else b


class Figure(BaseModel):
    """A single computed figure with display formatting and provenance."""
    raw: Decimal
    display: str
    source: str
    confidence: Confidence


class QuoteReadyInput(BaseModel):
    property_id: str
    max_ltc: Decimal
    max_ltv: Decimal

    # Purchase basis — fallback chain:
    # purchase_price → estimated_value → assessed_value_mkt/last_sale_price
    purchase_price: Optional[Decimal] = None
    estimated_value: Optional[Decimal] = None
    assessed_value_mkt: Optional[Decimal] = None
    last_sale_price: Optional[Decimal] = None

    # Rehab — pre-filled from job_estimator by caller; caller may override
    rehab_estimate: Optional[Decimal] = None

    # ARV — contract input for WP-8A; comp-derived ARV is WP-8B
    arv: Optional[Decimal] = None


class QuoteReadyResult(BaseModel):
    """Stateless deal-math result. No rate/term/commitment fields — by construction."""
    project_cost: Optional[Figure] = None
    proposed_loan: Optional[Figure] = None
    ltc: Optional[Figure] = None
    ltv: Optional[Figure] = None
    missing: list[str] = Field(default_factory=list)
