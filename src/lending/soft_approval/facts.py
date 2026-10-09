"""The property facts the caller enters in Slack after the call."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

_MAX_AMOUNT = Decimal("1000000000")


class SoftApprovalFacts(BaseModel):
    model_config = ConfigDict(frozen=True)

    property_address: str = Field(min_length=3, max_length=200)
    purchase_price: Decimal = Field(gt=0, lt=_MAX_AMOUNT)
    rehab_budget: Decimal = Field(ge=0, lt=_MAX_AMOUNT)
    arv: Decimal = Field(gt=0, lt=_MAX_AMOUNT)
    property_type: Optional[str] = Field(default=None, max_length=60)
    target_close_date: Optional[date] = None

    @field_validator("property_address", "property_type", mode="before")
    @classmethod
    def _strip(cls, value):
        if isinstance(value, str):
            return " ".join(value.split()) or None
        return value

    def snapshot(self) -> dict:
        return {
            "property_address": self.property_address,
            "purchase_price": str(self.purchase_price),
            "rehab_budget": str(self.rehab_budget),
            "arv": str(self.arv),
            "property_type": self.property_type,
            "target_close_date": self.target_close_date.isoformat() if self.target_close_date else None,
        }
