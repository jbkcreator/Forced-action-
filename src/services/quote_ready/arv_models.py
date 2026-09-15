from __future__ import annotations

from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field

from .arv_config import ARVConfig, DEFAULT_CONFIG

Confidence = Literal["high", "medium", "low"]
LocalityTier = Literal["subdivision", "neighborhood", "zip", "county", "none"]


class SubjectProperty(BaseModel):
    property_id: int
    sqft: int = Field(gt=0)
    beds: Optional[int] = None
    baths: Optional[Decimal] = None
    property_use_code: str
    building_condition: int = Field(ge=1, le=5)  # current/as-is condition
    after_repair_condition: int = Field(ge=1, le=5)  # target repaired condition; ARV normalizes to this
    subdivision: Optional[str] = None
    hcpa_neighborhood_code: Optional[str] = None
    zip: Optional[str] = None
    county: Optional[str] = None


class CandidateSale(BaseModel):
    property_id: int
    sale_price: Decimal = Field(ge=0)
    sale_yr: int
    sale_mo: int = Field(ge=1, le=12)
    qual_cd: str
    sqft: int = Field(gt=0)
    beds: Optional[int] = None
    baths: Optional[Decimal] = None
    property_use_code: str
    building_condition: int = Field(ge=1, le=5)
    subdivision: Optional[str] = None
    hcpa_neighborhood_code: Optional[str] = None
    zip: Optional[str] = None
    county: Optional[str] = None


class SelectedComp(BaseModel):
    property_id: int
    sale_price: Decimal
    sale_yr: int
    sale_mo: int
    sqft: int
    building_condition: int
    locality_tier: str
    price_per_sqft: Decimal
    adjusted_value: Decimal
    sqft_adjustment: Decimal
    condition_adjustment: Decimal


class ARVInput(BaseModel):
    subject: SubjectProperty
    candidate_sales: list[CandidateSale]
    as_of_yr: int
    as_of_mo: int = Field(ge=1, le=12)
    config: ARVConfig = Field(default_factory=ARVConfig)


class ARVResult(BaseModel):
    low: Optional[Decimal] = None
    point: Optional[Decimal] = None
    high: Optional[Decimal] = None
    confidence: Optional[Confidence] = None
    comp_count: int = 0
    weak_comp: bool = True
    locality_tier: LocalityTier = "none"
    recency_window_months: int = 12
    after_repair_condition: Optional[int] = None
    selected_comps: list[SelectedComp] = Field(default_factory=list)
    arv_unknown: bool = False
    unknown_reason: Optional[str] = None
    source: str = "wp8b_comparable_sales"
