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
    # config-gated, off by default — see DialListConfig + repository detectors.
    # maturities/1031 have partial heuristic sources; price_drop/expired_listing
    # have no data source at all (no MLS/listing table exists yet).
    "maturities",
    "exchange_1031",
    "price_drop",
    "expired_listing",
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

    # expected-loan bases (first available wins: override → max_ltc×arv →
    # assessed → last sale). The override carries Stage D's 85% LTC construction
    # sizing for builder candidates, whose loan basis is the build cost, not the
    # parcel's assessed value.
    expected_loan_override: Optional[Decimal] = Field(default=None, ge=0)
    expected_loan_override_confidence: Optional[LoanConfidence] = None
    arv: Optional[Decimal] = Field(default=None, ge=0)
    max_ltc: Optional[Decimal] = Field(default=None, ge=0)
    assessed_value_mkt: Optional[Decimal] = Field(default=None, ge=0)
    last_sale_price: Optional[Decimal] = Field(default=None, ge=0)

    is_builder: bool = False
    urgency_date: Optional[date] = None  # nearest actionable date

    # who to call + where (client wants "ranked names", not IDs)
    borrower_name: Optional[str] = None   # canonical entity name (resolved)
    owner_name: Optional[str] = None      # parcel owner (fallback when unresolved)
    property_address: Optional[str] = None
    phone: Optional[str] = None

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

    # display fields — the "name / property / phone" Josh calls from
    contact_name: Optional[str] = None
    property_address: Optional[str] = None
    phone: Optional[str] = None

    reason: str
    talking_points: List[str] = Field(default_factory=list)
    rank: int = 0


class DialList(BaseModel):
    generated_for: date
    entries: List[DialListEntry] = Field(default_factory=list)
    candidate_count: int = 0
    config_version: str
    # sources whose scraper feed is behind SLA when this list was built —
    # surfaced in the digest so Josh knows the data may be stale.
    stale_sources: List[str] = Field(default_factory=list)
    # True when live generation failed and this list was served from the last
    # cached snapshot (failure-behavior fallback).
    from_cache: bool = False
