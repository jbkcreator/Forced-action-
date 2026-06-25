"""FHFA House Price Index loader — downloads and normalizes hpi_master.csv.

Source:  https://www.fhfa.gov/data/hpi
File:    https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv
Format:  CSV (multi-MB, all geographies + index types in one file)
Auth:    None required — fully public, no API key, no rate limit.

hpi_master.csv columns:
  hpi_type    — "traditional" | "expanded-data"
  hpi_flavor  — "purchase-only" | "all-transactions"
  frequency   — "monthly" | "quarterly" | "annual"
  level       — geography level string (see LEVEL_SCOPE map below)
  place_name  — human-readable geography name
  place_id    — FIPS, CBSA, or ZIP code
  yr          — 4-digit year
  period      — month (1–12) or quarter (1–4)
  index_nsa   — HPI value, not seasonally adjusted
  index_sa    — HPI value, seasonally adjusted (null for some series)

Strategy: download once, parse in-memory, filter to the desired subset.
The full file is ~20MB — do not call repeatedly in hot paths.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from src.loaders.macro_signals.normalization import normalize_record
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)

FHFA_MASTER_URL = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"

SAMPLE_OUTPUT_DIR = Path("data/reference/macro_signals_samples")

# Map FHFA level strings -> normalized geography_scope values
LEVEL_SCOPE: dict[str, str] = {
    "USA or Census Division": "national_or_division",
    "state":                  "state",
    "MSA":                    "metro",
    "county":                 "county",
    "ZIP5":                   "zip5",
    "3-Digit ZIP":            "zip3",
    "census_tract":           "tract",
}


def download_hpi_master(timeout: int = 60) -> str:
    """Download the FHFA HPI master CSV and return the raw text."""
    logger.info("[FHFA] Downloading HPI master CSV from %s", FHFA_MASTER_URL)
    resp = requests_get_with_retry(FHFA_MASTER_URL, timeout=timeout)
    resp.raise_for_status()
    logger.info("[FHFA] Download complete (%d bytes)", len(resp.content))
    return resp.text


def parse_hpi_master(
    csv_text: str,
    *,
    hpi_flavor: str = "purchase-only",
    frequency: str = "monthly",
    levels: Optional[list[str]] = None,
    place_ids: Optional[list[str]] = None,
    min_year: Optional[int] = None,
) -> list[dict]:
    """Parse raw hpi_master CSV text and return normalized records.

    Args:
        csv_text: Raw CSV content from download_hpi_master().
        hpi_flavor: Filter to "purchase-only" or "all-transactions".
        frequency: Filter to "monthly", "quarterly", or "annual".
        levels: Geography levels to include (default: all). E.g. ["state", "MSA"].
        place_ids: FIPS/ZIP/CBSA codes to include (default: all).
        min_year: Drop records before this year.

    Returns:
        Normalized records. index_nsa is used as the primary value;
        index_sa is included as an extra field when present.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    records: list[dict] = []
    skipped = 0

    for row in reader:
        if row.get("hpi_flavor", "").strip() != hpi_flavor:
            continue
        if row.get("frequency", "").strip() != frequency:
            continue

        level = row.get("level", "").strip()
        if levels and level not in levels:
            continue

        place_id = row.get("place_id", "").strip()
        if place_ids and place_id not in place_ids:
            continue

        try:
            yr = int(row["yr"])
            period = int(row["period"])
            index_nsa_raw = row.get("index_nsa", "").strip()
            if not index_nsa_raw or index_nsa_raw in (".", ""):
                skipped += 1
                continue
            value = float(index_nsa_raw)
        except (ValueError, KeyError):
            skipped += 1
            continue

        if min_year and yr < min_year:
            continue

        # Build observation date: monthly -> day 1 of month; quarterly -> day 1 of first month
        try:
            if frequency == "monthly":
                obs_date = date(yr, period, 1)
            elif frequency == "quarterly":
                obs_date = date(yr, (period - 1) * 3 + 1, 1)
            else:
                obs_date = date(yr, 1, 1)
        except ValueError:
            skipped += 1
            continue

        index_sa_raw = row.get("index_sa", "").strip()
        index_sa = float(index_sa_raw) if index_sa_raw and index_sa_raw not in (".", "") else None

        records.append(
            normalize_record(
                source="fhfa",
                signal_key="house_price_index",
                source_series_id=f"FHFA_HPI_{hpi_flavor}_{level}",
                value=value,
                observed_at=obs_date,
                frequency=frequency,
                geography_scope=LEVEL_SCOPE.get(level, level),
                geography_id=place_id,
                unit="index",
                raw_payload=dict(row),
                # Extra fields carried alongside required schema
                hpi_type=row.get("hpi_type", "").strip(),
                hpi_flavor=hpi_flavor,
                place_name=row.get("place_name", "").strip(),
                index_sa=index_sa,
            )
        )

    logger.info(
        "[FHFA] Parsed %d records (skipped %d) — flavor=%s freq=%s",
        len(records), skipped, hpi_flavor, frequency,
    )
    return records


def fetch_hpi(
    *,
    hpi_flavor: str = "purchase-only",
    frequency: str = "monthly",
    levels: Optional[list[str]] = None,
    place_ids: Optional[list[str]] = None,
    min_year: Optional[int] = None,
    raw_csv: Optional[str] = None,
) -> list[dict]:
    """Download and parse FHFA HPI. Returns normalized records.

    Pass raw_csv to skip the download (useful in tests or when the file
    is already cached locally).
    """
    csv_text = raw_csv or download_hpi_master()
    return parse_hpi_master(
        csv_text,
        hpi_flavor=hpi_flavor,
        frequency=frequency,
        levels=levels,
        place_ids=place_ids,
        min_year=min_year,
    )


def save_sample(records: list[dict], path: Optional[Path] = None) -> Path:
    """Write a sample slice (up to 200 records) to local JSON for review."""
    import json
    out = path or SAMPLE_OUTPUT_DIR / "fhfa_hpi_sample.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    sample = records[:200]
    with open(out, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, default=str)
    logger.info("[FHFA] Sample written -> %s (%d records)", out, len(sample))
    return out


if __name__ == "__main__":
    from src.utils.logger import setup_logging
    setup_logging()

    # National + state, monthly, purchase-only since 2020
    records = fetch_hpi(
        levels=["USA or Census Division", "state"],
        min_year=2020,
    )
    out = save_sample(records)
    print(f"Fetched {len(records)} records -> {out}")
    if records:
        print(f"Sample: {records[0]}")
