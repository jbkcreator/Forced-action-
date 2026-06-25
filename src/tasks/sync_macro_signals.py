"""A7: Sync task — fetch macro-signal data from all sources and persist to DB.

Runs each source loader independently. A failure in one source does not
block the others. Census is skipped gracefully if CENSUS_API_KEY is absent.

Usage:
    PYTHONPATH=. python -m src.tasks.sync_macro_signals
    PYTHONPATH=. python -m src.tasks.sync_macro_signals --sources fred fhfa
"""
from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import Optional

from config.settings import get_settings
from src.core.database import get_db_context
from src.loaders.macro_signals.bls_client import BLSClient, KNOWN_SERIES as BLS_SERIES
from src.loaders.macro_signals.census_client import ACS_VARIABLES, CensusClient
from src.loaders.macro_signals.fhfa_hpi_loader import fetch_hpi
from src.loaders.macro_signals.fred_rates_loader import fetch_mortgage_rates
from src.services.macro_signal_service import upsert_macro_signals
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_CURRENT_YEAR = date.today().year
_CENSUS_ACS5_YEAR = 2022  # latest stable ACS5 vintage


def _sync_fred(session) -> dict:
    try:
        records = fetch_mortgage_rates(limit=104)
        return upsert_macro_signals(session, records)
    except ValueError as exc:
        # Raised by FREDClient when FRED_API_KEY is absent
        logger.warning("[Sync:FRED] Skipped — %s", exc)
        return {"skipped": True, "reason": str(exc)}


def _sync_fhfa(session) -> dict:
    records = fetch_hpi(
        levels=["USA or Census Division", "state", "MSA", "county"],
        min_year=2020,
    )
    return upsert_macro_signals(session, records)


def _sync_bls(session) -> dict:
    client = BLSClient()
    series_ids = list(BLS_SERIES.keys())
    start_year = max(_CURRENT_YEAR - 5, 2010)
    records = client.fetch_series(series_ids, start_year=start_year, end_year=_CURRENT_YEAR)
    return upsert_macro_signals(session, records)


def _sync_census(session) -> dict:
    settings = get_settings()
    if not settings.census_api_key:
        logger.info("[Sync:Census] Skipped — CENSUS_API_KEY not configured")
        return {"skipped": True, "reason": "CENSUS_API_KEY not configured"}

    client = CensusClient(year=_CENSUS_ACS5_YEAR)
    records = client.fetch_county(list(ACS_VARIABLES.keys()), state_fips="12")
    return upsert_macro_signals(session, records)


_SOURCE_RUNNERS = {
    "fred":   _sync_fred,
    "fhfa":   _sync_fhfa,
    "bls":    _sync_bls,
    "census": _sync_census,
}


def run_sync(sources: Optional[list[str]] = None) -> dict[str, dict]:
    """Run macro-signal sync for the requested sources.

    Args:
        sources: List of source names to sync, e.g. ["fred", "bls"].
                 Defaults to all sources if None.

    Returns:
        Dict keyed by source name with upsert counts or error/skipped info.
    """
    targets = sources or list(_SOURCE_RUNNERS.keys())
    unknown = set(targets) - _SOURCE_RUNNERS.keys()
    if unknown:
        raise ValueError(f"Unknown source(s): {unknown}. Valid: {list(_SOURCE_RUNNERS)}")

    results: dict[str, dict] = {}

    with get_db_context() as session:
        for name in targets:
            runner = _SOURCE_RUNNERS[name]
            try:
                logger.info("[Sync] Starting %s...", name)
                result = runner(session)
                session.commit()
                results[name] = result
                if result.get("skipped"):
                    logger.info("[Sync] %s skipped: %s", name, result.get("reason"))
                else:
                    logger.info(
                        "[Sync] %s complete — inserted=%s updated=%s",
                        name,
                        result.get("inserted", 0),
                        result.get("updated", 0),
                    )
            except Exception as exc:
                session.rollback()
                logger.error("[Sync] %s failed: %s", name, exc, exc_info=True)
                results[name] = {"error": str(exc)}

    _log_summary(results)
    return results


def _log_summary(results: dict[str, dict]) -> None:
    total_inserted = sum(r.get("inserted", 0) for r in results.values())
    total_updated = sum(r.get("updated", 0) for r in results.values())
    errors = [k for k, v in results.items() if "error" in v]
    skipped = [k for k, v in results.items() if v.get("skipped")]

    logger.info(
        "[Sync] Summary — inserted=%d updated=%d errors=%s skipped=%s",
        total_inserted, total_updated, errors or "none", skipped or "none",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync macro-signal data to DB")
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list(_SOURCE_RUNNERS),
        default=None,
        help="Sources to sync (default: all)",
    )
    args = parser.parse_args()
    run_sync(args.sources)
