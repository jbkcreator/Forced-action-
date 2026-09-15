from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class ARVConfig(BaseModel):
    min_comps: int = 3
    recency_months_primary: int = 12
    recency_months_extended: int = 24
    sqft_tolerance_pct: Decimal = Decimal("0.20")
    spread_threshold: Decimal = Decimal("0.20")
    condition_adjustment_per_step: Decimal = Decimal("0.05")
    locality_tiers: list[str] = ["subdivision", "neighborhood", "zip", "county"]
    excluded_qual_codes: list[str] = ["98", "99", "Q", "U"]


DEFAULT_CONFIG = ARVConfig()
