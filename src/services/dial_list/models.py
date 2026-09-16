"""WP-9 Dial List — domain models.

Pure data models for the ranking seam. The caller assembles `DialCandidate`s
(retrieval adapter, built separately); the ranker returns a `DialList` of
`DialListEntry`s. No I/O here.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

TriggerType = Literal[
    "cash_purchase",
    "stalled_flip",
    "permits_no_financing",
    "auction_probate",
    "out_of_state",
    "financing_intent",
    "builder",
]

IntentTier = Literal["high", "medium", "low"]
LoanConfidence = Literal["high", "low"]


class DialCandidate(BaseModel):
    """One assembled opportunity, borrower resolved-or-not, ready to rank."""

    property_id: int
    opportunity_id: Optional[str] = None
    buyer_entity_id: Optional[int] = None  # None = unresolved borrower
    triggers: List[TriggerType] = Field(min_length=1)
    intent_tier: Optional[IntentTier] = None  # None = detector-only candidate

    # expected-loan bases (first available wins: max_ltc×arv → assessed → last sale)
    arv: Optional[Decimal] = Field(default=None, ge=0)
    max_ltc: Optional[Decimal] = Field(default=None, ge=0)
    assessed_value_mkt: Optional[Decimal] = Field(default=None, ge=0)
    last_sale_price: Optional[Decimal] = Field(default=None, ge=0)

    is_builder: bool = False
    urgency_date: Optional[date] = None  # nearest actionable date

    # relationship facts for the reason line
    properties_owned: Optional[int] = None
    last_deal_months_ago: Optional[int] = None


class DialListEntry(BaseModel):
    property_id: int
    opportunity_id: Optional[str] = None
    buyer_entity_id: Optional[int] = None
    triggers: List[TriggerType]

    expected_revenue: Decimal
    probability: Decimal
    expected_loan: Decimal
    commission: Decimal
    urgency: Decimal
    expected_loan_confidence: LoanConfidence
    borrower_resolved: bool

    reason: str
    talking_points: List[str] = Field(default_factory=list)
    rank: int = 0


class DialList(BaseModel):
    generated_for: date
    entries: List[DialListEntry] = Field(default_factory=list)
    candidate_count: int = 0
    config_version: str
