"""FRED API client — fetches and normalizes time-series observations.

Endpoint: GET https://api.stlouisfed.org/fred/series/observations
Docs:     https://fred.stlouisfed.org/docs/api/fred/series_observations.html

Requires a free FRED API key: https://fred.stlouisfed.org/docs/api/api_key.html
Set FRED_API_KEY in .env — accessed via settings.fred_api_key.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from config.settings import settings
from src.loaders.macro_signals.normalization import normalize_record
from src.loaders.macro_signals.source_registry import SOURCES
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred"
_SERIES_META = SOURCES["fred"]["series"]


class FREDClient:
    """Thin client for the FRED observations endpoint.

    Usage:
        client = FREDClient()
        records = client.fetch_observations("MORTGAGE30US", limit=52)
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        if api_key:
            self._key = api_key
        elif settings.fred_api_key:
            self._key = settings.fred_api_key.get_secret_value()
        else:
            raise ValueError(
                "FRED_API_KEY is not set. Add it to .env or pass api_key= directly. "
                "Free registration: https://fred.stlouisfed.org/docs/api/api_key.html"
            )

    def fetch_observations(
        self,
        series_id: str,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        limit: int = 1000,
        sort_order: str = "desc",
    ) -> list[dict]:
        """Fetch observations for a FRED series and return normalized records.

        Entries where value == "." are skipped — FRED uses this as the
        missing-data sentinel (e.g. weekends, holidays, unreleased periods).

        Args:
            series_id: FRED series identifier, e.g. "MORTGAGE30US".
            observation_start: ISO date string "YYYY-MM-DD" (optional).
            observation_end: ISO date string "YYYY-MM-DD" (optional).
            limit: Max observations to return (FRED max: 100000).
            sort_order: "asc" or "desc".

        Returns:
            List of normalized records (see normalization.REQUIRED_KEYS).
        """
        params: dict = {
            "series_id": series_id,
            "api_key": self._key,
            "file_type": "json",
            "limit": limit,
            "sort_order": sort_order,
        }
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end

        resp = requests_get_with_retry(
            f"{FRED_BASE}/series/observations",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()

        meta = _SERIES_META.get(series_id, {})

        records: list[dict] = []
        skipped_missing = 0
        for obs in payload.get("observations", []):
            raw_value = obs.get("value", ".")
            if raw_value == ".":
                skipped_missing += 1
                continue
            try:
                value = float(raw_value)
            except (ValueError, TypeError):
                logger.warning(
                    "[FRED] Non-numeric value for %s on %s: %r",
                    series_id, obs.get("date"), raw_value,
                )
                continue

            records.append(
                normalize_record(
                    source="fred",
                    signal_key=meta.get("signal_key", series_id.lower()),
                    source_series_id=series_id,
                    value=value,
                    observed_at=date.fromisoformat(obs["date"]),
                    frequency=meta.get("frequency", "unknown"),
                    geography_scope=meta.get("geography_scope", "national"),
                    geography_id=meta.get("geography_id", "US"),
                    unit=meta.get("unit", "unknown"),
                    raw_payload=obs,
                )
            )

        logger.info(
            "[FRED] %s -> %d records fetched, %d missing-value entries skipped",
            series_id, len(records), skipped_missing,
        )
        return records
