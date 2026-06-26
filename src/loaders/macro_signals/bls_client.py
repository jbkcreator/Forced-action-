"""BLS Public Data API v2 client — unemployment, CPI, rental CPI.

Endpoint: POST https://api.bls.gov/publicAPI/v2/timeseries/data/
Docs:     https://www.bls.gov/developers/home.htm

No API key needed for basic access (25 years of history, up to 25 series per
request). With a free registered key the limit extends to 50 years and higher
concurrency. BLS registration: https://data.bls.gov/registrationEngine/

Useful series for housing intelligence:
  LNS14000000   — Unemployment Rate (national, seasonally adjusted)
  CUUR0000SA0   — CPI-U All Items (national)
  CUSR0000SEHA  — CPI Rent of Primary Residence (national, seasonally adjusted)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from src.loaders.macro_signals.normalization import normalize_record
from src.loaders.macro_signals.source_registry import SOURCES
from src.utils.http_helpers import requests_post_with_retry

logger = logging.getLogger(__name__)

SAMPLE_OUTPUT_DIR = Path("data/reference/macro_signals_samples")

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
_SERIES_META = SOURCES["bls"]["series"]

# Well-known series available without needing to navigate the BLS catalog.
KNOWN_SERIES: dict[str, str] = {
    "LNS14000000":           "unemployment_rate_national",
    "CUUR0000SA0":           "cpi_all_urban",
    "CUSR0000SEHA":          "cpi_rent_primary_residence",
    # LAUS county unemployment rates — FL target counties
    "LAUCN120570000000003":  "county_unemployment_rate",
    "LAUCN121030000000003":  "county_unemployment_rate",
}

# LAUS county unemployment series for Florida target counties (FIPS -> series ID).
# Format: LAUCN{state2}{county3}0000000003  (measure 003 = unemployment rate)
COUNTY_LAUS_SERIES: dict[str, str] = {
    "12057": "LAUCN120570000000003",  # Hillsborough County, FL
    "12103": "LAUCN121030000000003",  # Pinellas County, FL
}


class BLSClient:
    """Client for BLS Public Data API v2.

    Usage (no key):
        client = BLSClient()
        records = client.fetch_series(["LNS14000000"], start_year=2020, end_year=2024)

    Usage (with key for extended history):
        client = BLSClient(api_key="your_key")
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        self._key = api_key

    def fetch_series(
        self,
        series_ids: list[str],
        start_year: int,
        end_year: int,
    ) -> list[dict]:
        """Fetch one or more BLS series and return normalized monthly records.

        BLS periods are formatted as M01–M12 (monthly) or M13 (annual average).
        M13 (annual average) entries are skipped — use monthly observations only.

        Args:
            series_ids: List of BLS series IDs (max 25 without key).
            start_year: First year to include.
            end_year: Last year to include (BLS max range: 20 years without key).

        Returns:
            Normalized records sorted newest-first within each series.

        Raises:
            RuntimeError: If the BLS API returns a non-success status.
        """
        payload: dict = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
        if self._key:
            payload["registrationkey"] = self._key

        resp = requests_post_with_retry(
            BLS_API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()

        if body.get("status") != "REQUEST_SUCCEEDED":
            messages = body.get("message", [])
            logger.error("[BLS] API error: %s", messages)
            raise RuntimeError(f"BLS API error: {messages}")

        records: list[dict] = []
        for series in body.get("Results", {}).get("series", []):
            series_id = series["seriesID"]
            meta = _SERIES_META.get(series_id, {})
            signal_key = KNOWN_SERIES.get(series_id, series_id.lower())

            for item in series.get("data", []):
                period: str = item.get("period", "")
                year: str = item.get("year", "")

                # Skip annual averages (M13) and non-monthly entries
                if not period.startswith("M") or period == "M13":
                    continue

                try:
                    obs_date = date(int(year), int(period[1:]), 1)
                    value = float(item["value"])
                except (ValueError, KeyError):
                    logger.debug("[BLS] Skipping unparseable entry: %r", item)
                    continue

                records.append(
                    normalize_record(
                        source="bls",
                        signal_key=signal_key,
                        source_series_id=series_id,
                        value=value,
                        observed_at=obs_date,
                        frequency=meta.get("frequency", "monthly"),
                        geography_scope=meta.get("geography_scope", "national"),
                        geography_id=meta.get("geography_id", "US"),
                        unit=meta.get("unit", "index"),
                        raw_payload=item,
                    )
                )

        logger.info("[BLS] Fetched %d monthly records for %s", len(records), series_ids)
        return records


def save_sample(records: list[dict], path: Path | None = None) -> Path:
    """Write a sample slice (up to 200 records) to local JSON for review."""
    out = path or SAMPLE_OUTPUT_DIR / "bls_sample.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    sample = records[:200]
    with open(out, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, default=str)
    logger.info("[BLS] Sample written -> %s (%d records)", out, len(sample))
    return out


if __name__ == "__main__":
    from src.utils.logger import setup_logging
    setup_logging()

    series = list(KNOWN_SERIES.keys())
    client = BLSClient()
    records = client.fetch_series(series, start_year=2022, end_year=2025)
    out = save_sample(records)
    print(f"Fetched {len(records)} records -> {out}")
    if records:
        latest = records[0]
        print(f"Latest: {latest['observed_at']}  {latest['value']}  ({latest['signal_key']})")
