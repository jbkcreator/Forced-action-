"""Census ACS5 client — income, vacancy, tenure, and population by county.

Endpoint: GET https://api.census.gov/data/{year}/acs/acs5
Docs:     https://www.census.gov/data/developers/guidance/api-user-guide.html

Free API key required: https://api.census.gov/data/key_signup.html
Set CENSUS_API_KEY in .env — accessed via settings.census_api_key.

Without a key: up to 500 queries/day per IP still works but Census recommends
always using a key.

Geography notes:
  Florida state FIPS: 12
  Hillsborough County FIPS: 12057
  Pinellas County FIPS: 12103

Variable discovery: https://api.census.gov/data/2022/acs/acs5/variables.json
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from config.settings import settings
from src.loaders.macro_signals.normalization import normalize_record
from src.loaders.macro_signals.source_registry import SOURCES
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)

SAMPLE_OUTPUT_DIR = Path("data/reference/macro_signals_samples")

CENSUS_BASE = "https://api.census.gov/data"
_SERIES_META = SOURCES["census_acs5"]["series"]

# ACS5 variables relevant to distressed property intelligence.
# Append "M" instead of "E" for margin of error.
ACS_VARIABLES: dict[str, dict] = {
    "B25001_001E": {"signal_key": "total_housing_units",    "unit": "count"},
    "B25002_002E": {"signal_key": "occupied_housing_units", "unit": "count"},
    "B25002_003E": {"signal_key": "vacant_housing_units",   "unit": "count"},
    "B25003_002E": {"signal_key": "owner_occupied_units",   "unit": "count"},
    "B25003_003E": {"signal_key": "renter_occupied_units",  "unit": "count"},
    "B25064_001E": {"signal_key": "median_gross_rent",      "unit": "dollars"},
    "B25077_001E": {"signal_key": "median_home_value",      "unit": "dollars"},
    "B19013_001E": {"signal_key": "median_household_income","unit": "dollars"},
    "B23025_005E": {"signal_key": "unemployed_civilian",    "unit": "count"},
    "B01003_001E": {"signal_key": "total_population",       "unit": "count"},
}

# Census uses large negative numbers for suppressed/unavailable cells.
_CENSUS_SUPPRESSED_THRESHOLD = -1_000


class CensusClient:
    """Minimal ACS5 client. Returns county-level records for Florida by default.

    Usage:
        client = CensusClient(year=2022)
        records = client.fetch_county(list(ACS_VARIABLES.keys()), state_fips="12")
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        year: int = 2022,
    ) -> None:
        if api_key:
            self._key = api_key
        elif settings.census_api_key:
            self._key = settings.census_api_key.get_secret_value()
        else:
            self._key = None  # Works for basic access (<500 queries/day)
        self._year = year

    def fetch_county(
        self,
        variables: list[str],
        state_fips: str = "12",  # Florida
        county_fips: str = "*",  # all counties in state
    ) -> list[dict]:
        """Fetch ACS5 variables for counties in a state.

        Args:
            variables: ACS variable codes to retrieve (e.g. ["B19013_001E"]).
            state_fips: Two-digit state FIPS ("12" = Florida).
            county_fips: Three-digit county FIPS, or "*" for all counties.

        Returns:
            Normalized records — one per (variable, county) pair.
            Records with suppressed values (< -1,000,000) are skipped.
        """
        var_str = ",".join(["NAME"] + variables)
        params: dict = {
            "get": var_str,
            "for": f"county:{county_fips}",
            "in": f"state:{state_fips}",
        }
        if self._key:
            params["key"] = self._key

        url = f"{CENSUS_BASE}/{self._year}/acs/acs5"
        resp = requests_get_with_retry(url, params=params, timeout=30)
        resp.raise_for_status()

        rows: list[list[str]] = resp.json()
        if not rows or len(rows) < 2:
            logger.warning("[Census] Empty response for state=%s variables=%s", state_fips, variables)
            return []

        headers = rows[0]
        obs_date = date(self._year, 1, 1)
        records: list[dict] = []

        for row in rows[1:]:
            row_dict = dict(zip(headers, row))
            county_code = row_dict.get("county", "")
            state_code = row_dict.get("state", state_fips)
            geo_id = f"{state_code}{county_code}"

            for var_id, meta in ACS_VARIABLES.items():
                if var_id not in row_dict:
                    continue
                raw = row_dict[var_id]
                try:
                    value = float(raw)
                except (ValueError, TypeError):
                    continue
                # Skip suppressed / not-applicable cells
                if value < _CENSUS_SUPPRESSED_THRESHOLD:
                    continue

                records.append(
                    normalize_record(
                        source="census_acs5",
                        signal_key=meta["signal_key"],
                        source_series_id=f"ACS5_{self._year}_{var_id}",
                        value=value,
                        observed_at=obs_date,
                        frequency="annual",
                        geography_scope="county",
                        geography_id=geo_id,
                        unit=meta["unit"],
                        raw_payload=row_dict,
                    )
                )

        logger.info(
            "[Census] Fetched %d records for state=%s county=%s year=%d",
            len(records), state_fips, county_fips, self._year,
        )
        return records


def save_sample(records: list[dict], path: Path | None = None) -> Path:
    """Write a sample slice (up to 200 records) to local JSON for review."""
    out = path or SAMPLE_OUTPUT_DIR / "census_sample.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    sample = records[:200]
    with open(out, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, default=str)
    logger.info("[Census] Sample written -> %s (%d records)", out, len(sample))
    return out


if __name__ == "__main__":
    from src.utils.logger import setup_logging
    setup_logging()

    # Fetch all ACS5 variables for all Florida counties (Hillsborough + Pinellas focus)
    client = CensusClient(year=2022)
    records = client.fetch_county(list(ACS_VARIABLES.keys()), state_fips="12")
    out = save_sample(records)
    print(f"Fetched {len(records)} records -> {out}")
    if records:
        latest = records[0]
        print(f"Sample: {latest['geography_id']}  {latest['signal_key']}  {latest['value']}")
