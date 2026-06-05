"""Structured reason codes for human overrides of Cora decisions."""

from __future__ import annotations

from enum import StrEnum


class OverrideReasonCode(StrEnum):
    FACTUAL_ERROR = "factual_error"
    COMPLIANCE_RISK = "compliance_risk"
    WRONG_AUDIENCE = "wrong_audience"
    BAD_TIMING = "bad_timing"
    LOW_LEAD_QUALITY = "low_lead_quality"
    OFFER_MISMATCH = "offer_mismatch"
    TONE_OR_BRAND_RISK = "tone_or_brand_risk"
    DUPLICATE_OR_REDUNDANT = "duplicate_or_redundant"
    CUSTOMER_CONTEXT_MISSING = "customer_context_missing"
    OPERATOR_STRATEGY = "operator_strategy"
    OTHER = "other"


OVERRIDE_REASON_VALUES = tuple(reason.value for reason in OverrideReasonCode)


def normalize_override_reason_code(value: str | OverrideReasonCode | None) -> str | None:
    """Return a persisted enum value or raise ValueError for unknown codes."""
    if value is None:
        return None
    if isinstance(value, OverrideReasonCode):
        return value.value
    try:
        return OverrideReasonCode(str(value).strip()).value
    except ValueError as exc:
        allowed = ", ".join(OVERRIDE_REASON_VALUES)
        raise ValueError(
            f"override_reason_code must be one of ({allowed}), got {value!r}"
        ) from exc
