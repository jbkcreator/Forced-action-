"""FRED mortgage rate loader — fetches MORTGAGE30US and MORTGAGE15US.

Intended entry point for A7 macro signal ingestion once a DB table exists.
For now: fetches, normalizes, and optionally saves a local JSON sample.

CLI / test mode:
    PYTHONPATH=. python -m src.loaders.macro_signals.fred_rates_loader
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from src.loaders.macro_signals.fred_client import FREDClient

logger = logging.getLogger(__name__)

SAMPLE_OUTPUT_DIR = Path("data/reference/macro_signals_samples")

# Series to load by default. Add more FRED series here as A7 expands.
DEFAULT_SERIES: list[str] = ["MORTGAGE30US", "MORTGAGE15US"]


def fetch_mortgage_rates(
    series_ids: list[str] = DEFAULT_SERIES,
    limit: int = 104,  # ~2 years of weekly data
    observation_start: Optional[str] = None,
    api_key: Optional[str] = None,
) -> list[dict]:
    """Fetch mortgage rate observations from FRED.

    Args:
        series_ids: List of FRED series identifiers to fetch.
        limit: Max observations per series (most recent first).
        observation_start: Optional ISO date floor, e.g. "2022-01-01".
        api_key: Override for FRED_API_KEY setting.

    Returns:
        Normalized records across all requested series.
    """
    client = FREDClient(api_key=api_key)
    records: list[dict] = []
    for series_id in series_ids:
        series_records = client.fetch_observations(
            series_id,
            limit=limit,
            observation_start=observation_start,
            sort_order="desc",
        )
        records.extend(series_records)
    return records


def save_sample(records: list[dict], path: Optional[Path] = None) -> Path:
    """Write normalized records to a local JSON file for review."""
    out = path or SAMPLE_OUTPUT_DIR / "fred_mortgage30us_sample.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)
    logger.info("[FRED] Sample written -> %s (%d records)", out, len(records))
    return out


if __name__ == "__main__":
    from src.utils.logger import setup_logging
    setup_logging()

    records = fetch_mortgage_rates(series_ids=["MORTGAGE30US"], limit=52)
    out = save_sample(records)
    print(f"Fetched {len(records)} records -> {out}")
    if records:
        latest = records[0]
        print(f"Latest: {latest['observed_at']}  {latest['value']}%  ({latest['signal_key']})")
