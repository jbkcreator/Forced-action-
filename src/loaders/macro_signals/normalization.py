"""Shared normalization contract for all macro signal records.

Every loader must return dicts that satisfy REQUIRED_KEYS. The normalize_record()
helper enforces the schema and converts date objects to ISO strings so records are
JSON-serializable without a custom encoder.
"""
from __future__ import annotations

from datetime import date
from typing import Any

REQUIRED_KEYS: tuple[str, ...] = (
    "source",
    "signal_key",
    "source_series_id",
    "value",
    "observed_at",
    "frequency",
    "geography_scope",
    "geography_id",
    "unit",
    "raw_payload",
)


def normalize_record(
    *,
    source: str,
    signal_key: str,
    source_series_id: str,
    value: float,
    observed_at: date | str,
    frequency: str,
    geography_scope: str,
    geography_id: str,
    unit: str,
    raw_payload: Any,
    **extra: Any,
) -> dict:
    """Return a normalized macro signal record.

    All loaders must pass through this function so downstream consumers
    can rely on a consistent schema regardless of source format.
    """
    return {
        "source": source,
        "signal_key": signal_key,
        "source_series_id": source_series_id,
        "value": float(value),
        "observed_at": observed_at.isoformat() if isinstance(observed_at, date) else observed_at,
        "frequency": frequency,
        "geography_scope": geography_scope,
        "geography_id": geography_id,
        "unit": unit,
        "raw_payload": raw_payload,
        **extra,
    }
