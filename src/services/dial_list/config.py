"""WP-9 Dial List — scoring configuration.

Every coefficient (Q9/Q10/Q11) is a config field, so the ranking is tuned by
constructing a DialListConfig with overrides rather than editing the ranker.
Defaults are starting points and should be validated against real outcomes
before the list is trusted unattended.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Dict

from pydantic import BaseModel, Field, model_validator


class DialListConfig(BaseModel):
    """Config-driven coefficients for the expected-revenue ranking."""

    list_size: int = Field(default=30, ge=1)

    # probability ← financing intent tier; detector-only candidates take a floor
    intent_probability: Dict[str, Decimal] = Field(
        default_factory=lambda: {
            "high": Decimal("0.50"),
            "medium": Decimal("0.30"),
            "low": Decimal("0.15"),
        }
    )
    detector_only_probability_floor: Decimal = Field(default=Decimal("0.10"), gt=0)

    # expected loan fallback when no ARV/max_ltc available
    expected_loan_fallback_fraction: Decimal = Field(default=Decimal("0.70"), gt=0)

    # commission (permitted post client-Q18 — business-purpose hard money)
    commission_rate: Decimal = Field(default=Decimal("0.015"), gt=0)

    # per-trigger base urgency weight (higher = more time-sensitive)
    urgency_weights: Dict[str, Decimal] = Field(
        default_factory=lambda: {
            "auction_probate": Decimal("1.5"),
            "cash_purchase": Decimal("1.2"),
            "stalled_flip": Decimal("1.1"),
            "permits_no_financing": Decimal("1.0"),
            "financing_intent": Decimal("1.0"),
            "builder": Decimal("1.3"),
            "out_of_state": Decimal("0.8"),
        }
    )
    # a candidate with no recognised trigger urgency falls back to this
    urgency_default: Decimal = Field(default=Decimal("1.0"), gt=0)
    # date-proximity boost: an urgency_date within this window scales urgency up
    # to (1 + urgency_recent_boost); older/absent dates get no boost.
    urgency_recent_days: int = Field(default=90, ge=1)
    urgency_recent_boost: Decimal = Field(default=Decimal("0.5"), ge=0)

    # builder opportunities float above equal-dollar distress (Q10)
    builder_multiplier: Decimal = Field(default=Decimal("1.5"), gt=0)

    config_version: str = "wp9-1.0.0"

    @model_validator(mode="after")
    def _validate(self) -> "DialListConfig":
        if not self.intent_probability:
            raise ValueError("intent_probability must be non-empty")
        for tier in ("high", "medium", "low"):
            if tier not in self.intent_probability:
                raise ValueError(f"intent_probability missing tier {tier!r}")
        if any(v <= 0 for v in self.intent_probability.values()):
            raise ValueError("intent_probability values must be positive")
        if not self.urgency_weights:
            raise ValueError("urgency_weights must be non-empty")
        if any(v <= 0 for v in self.urgency_weights.values()):
            raise ValueError("urgency_weights values must be positive")
        return self


DEFAULT_CONFIG = DialListConfig()
