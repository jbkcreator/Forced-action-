from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field, model_validator

_KNOWN_TIERS = {"subdivision", "neighborhood", "zip", "county"}


class ARVConfig(BaseModel):
    min_comps: int = Field(default=3, ge=1)
    recency_months_primary: int = Field(default=12, ge=1)
    recency_months_extended: int = Field(default=24, ge=1)
    sqft_tolerance_pct: Decimal = Field(default=Decimal("0.20"), gt=0)
    spread_threshold: Decimal = Field(default=Decimal("0.20"), gt=0)
    condition_adjustment_per_step: Decimal = Field(default=Decimal("0.05"), ge=0)
    locality_tiers: list[str] = ["subdivision", "neighborhood", "zip", "county"]
    # Allowlist of DOR qualified arm's-length sale codes (01-06). Source of
    # truth: src.connectors.dor_sale_outcomes.QUALIFIED_CODES — mirrored here
    # rather than imported to avoid dragging that module's heavy import chain
    # into the pure engine. Everything not in this set (pending 98/99, and any
    # disqualification code such as 11) is rejected as a comp.
    qualified_qual_codes: list[str] = ["01", "02", "03", "04", "05", "06"]
    # When True, comps whose building condition had to be inferred (source data
    # missing/unrecognized) downgrade confidence rather than silently passing.
    inferred_condition_downgrades: bool = True

    @model_validator(mode="after")
    def _validate(self) -> "ARVConfig":
        if self.recency_months_extended < self.recency_months_primary:
            raise ValueError(
                "recency_months_extended must be >= recency_months_primary"
            )
        if not self.locality_tiers:
            raise ValueError("locality_tiers must be non-empty")
        unknown = [t for t in self.locality_tiers if t not in _KNOWN_TIERS]
        if unknown:
            raise ValueError(
                f"locality_tiers contains unknown tier(s): {unknown}; "
                f"allowed: {sorted(_KNOWN_TIERS)}"
            )
        return self


DEFAULT_CONFIG = ARVConfig()
