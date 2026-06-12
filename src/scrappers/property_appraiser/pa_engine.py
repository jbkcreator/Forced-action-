"""
HCPA Property Appraiser Enrichment Engine

Queries properties from DB, scrapes HCPA/TRIM/Tax Collector via sync Playwright
inside a ThreadPoolExecutor, and loads enriched data back via PropertyAppraiserLoader.

Usage:
    python -m src.scrappers.property_appraiser.pa_engine --county-id hillsborough --mode new-only --limit 500 --load-to-db
    python -m src.scrappers.property_appraiser.pa_engine --county-id hillsborough --mode refresh --stale-days 30 --limit 2000 --load-to-db
    python -m src.scrappers.property_appraiser.pa_engine --county-id hillsborough --mode all --limit 1 --load-to-db --headful

Modes:
    new-only   Properties where hcpa_last_refreshed IS NULL
    refresh    Properties where hcpa_last_refreshed < NOW() - stale_days
    all        No filter
"""

import argparse
import asyncio
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import setup_logging, get_logger
from src.utils.county_config import get_county_config
from src.core.database import get_db_context
from src.core.models import Property, ScraperRunStats

setup_logging()
logger = get_logger(__name__)

_MAX_WORKERS = 5
_COMMIT_BATCH_SIZE = 100


# ---------------------------------------------------------------------------
# Property query helpers
# ---------------------------------------------------------------------------

def _query_properties(
    county_id: str,
    mode: str,
    limit: int,
    stale_days: int,
    leads_only: bool = False,
    parcel_id: str | None = None,
) -> list[dict]:
    """Return list of {id, parcel_id} dicts matching the requested mode.

    leads_only=True restricts to properties whose most recent distress_score
    has qualified=true (the ~15k active leads).  Combines with mode filters —
    e.g. leads_only + new-only = leads not yet HCPA-enriched.
    """
    from sqlalchemy import text

    with get_db_context() as session:
        if parcel_id:
            rows = session.execute(
                text(
                    """
                    SELECT id, parcel_id
                    FROM properties
                    WHERE county_id = :cid
                      AND parcel_id = :parcel_id
                    LIMIT 1
                    """
                ),
                {"cid": county_id, "parcel_id": parcel_id},
            ).fetchall()
            return [{"id": r[0], "parcel_id": r[1]} for r in rows]

        if leads_only:
            # Join to the latest distress_score per property, keep only qualified=true.
            # DISTINCT ON ordered by score_date DESC picks the most recent row per property.
            base = (
                "SELECT p.id, p.parcel_id "
                "FROM properties p "
                "JOIN ( "
                "  SELECT DISTINCT ON (property_id) property_id "
                "  FROM distress_scores "
                "  WHERE county_id = :cid AND qualified = true "
                "  ORDER BY property_id, score_date DESC "
                ") qs ON qs.property_id = p.id "
                "WHERE p.county_id = :cid AND p.parcel_id IS NOT NULL"
            )
        else:
            base = "SELECT id, parcel_id FROM properties WHERE county_id = :cid AND parcel_id IS NOT NULL"

        if mode == "new-only":
            q = f"{base} AND p.hcpa_last_refreshed IS NULL LIMIT :lim" if leads_only else f"{base} AND hcpa_last_refreshed IS NULL LIMIT :lim"
        elif mode == "refresh":
            col = "p.hcpa_last_refreshed" if leads_only else "hcpa_last_refreshed"
            q = (
                f"{base} AND ({col} IS NULL OR "
                f"{col} < NOW() - INTERVAL '{stale_days} days') LIMIT :lim"
            )
        else:  # all
            q = f"{base} LIMIT :lim"

        rows = session.execute(text(q), {"cid": county_id, "lim": limit}).fetchall()
        return [{"id": r[0], "parcel_id": r[1]} for r in rows]


# ---------------------------------------------------------------------------
# Per-property worker (runs in thread pool)
# ---------------------------------------------------------------------------

def _scrape_and_parse(
    prop: dict,
    config: dict,
    county_id: str = "hillsborough",
    headful: bool = False,
    debug: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Worker function: scrape one property and return a canonical DataFrame row.
    Each call creates its own HCPAScraper instance (one browser per thread
    is held externally by the pool initializer).
    """
    from src.scrappers.property_appraiser.pa_scraper import HCPAScraper, PCPAOScraper
    from src.scrappers.property_appraiser.pa_parser import (
        parse_hcpa_page, parse_pcpao_page, parse_trim_pdf, to_canonical_dataframe
    )

    _SCRAPER_MAP = {
        "hcpa":  (HCPAScraper,  parse_hcpa_page),
        "pcpao": (PCPAOScraper, parse_pcpao_page),
    }
    parcel_id = prop["parcel_id"]
    pa_variant = config.get("pa_scraper", "hcpa")
    scraper_cls, page_parser = _SCRAPER_MAP.get(pa_variant, (HCPAScraper, parse_hcpa_page))
    try:
        with scraper_cls(config, headful=headful) as scraper:
            raw = scraper.scrape_property(parcel_id)

        if debug:
            print(f"\n{'='*60}")
            print(f"RAW SCRAPE — {parcel_id}")
            print(f"{'='*60}")
            print(f"  hcpa_text  : {len(raw.get('hcpa_text') or '')} chars")
            print(f"  hcpa_html  : {len(raw.get('hcpa_html') or '')} chars")
            print(f"  trim_pdf   : {raw.get('trim_pdf_path')}")
            print(f"  errors     : {raw.get('errors')}")
            if raw.get("hcpa_text"):
                print(f"\n--- HCPA TEXT (first 1500 chars) ---\n{(raw['hcpa_text'] or '')[:1500]}")

        if not (raw.get("hcpa_text") or raw.get("hcpa_html")):
            logger.warning(
                "No property appraiser page content for parcel %s (id=%s): %s",
                parcel_id,
                prop["id"],
                raw.get("errors") or [],
            )
            return None

        hcpa = page_parser(raw.get("hcpa_text") or "", raw.get("hcpa_html") or "")
        trim_path = raw.get("trim_pdf_path")
        trim = parse_trim_pdf(trim_path) if trim_path else {}
        # Delete TRIM PDF immediately after parsing — no need to keep it on disk
        if trim_path:
            try:
                Path(trim_path).unlink(missing_ok=True)
            except Exception:
                pass
        if debug:
            print(f"\n--- PARSED HCPA ---\n{hcpa}")
            print(f"\n--- PARSED TRIM ---\n{trim}")

        if not (hcpa or trim):
            logger.warning(
                "Property appraiser parse produced no data for parcel %s (id=%s)",
                parcel_id,
                prop["id"],
            )
            return None

        df = to_canonical_dataframe(hcpa, trim, parcel_id)
        df["_property_id"] = prop["id"]

        if debug:
            print(f"\n--- CANONICAL DATAFRAME ---")
            for col in df.columns:
                val = df[col].iloc[0]
                if val is not None and str(val) not in ("nan", "None", "[]"):
                    print(f"  {col}: {val}")

        return df

    except Exception as e:
        logger.error("Failed to scrape property %s (id=%s): %s", parcel_id, prop["id"], e)
        return None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_pa_pipeline(
    county_id: str = "hillsborough",
    mode: str = "new-only",
    limit: int = 500,
    stale_days: int = 30,
    load_to_db: bool = False,
    headful: bool = False,
    debug: bool = False,
    leads_only: bool = False,
    parcel_id: str | None = None,
) -> dict:
    """
    Full enrichment pipeline: query → scrape → parse → load.

    Returns summary stats dict.
    """
    run_start = time.time()
    logger.info("PA enrichment starting | county=%s mode=%s limit=%d load=%s leads_only=%s",
                county_id, mode, limit, load_to_db, leads_only)

    # Load county source config
    try:
        county_config = get_county_config(county_id)
        pa_config = county_config.get("sources", {}).get("property_appraiser", {})
        if not pa_config:
            logger.warning("No 'property_appraiser' CountySource found for %s — using defaults", county_id)
            pa_config = {}
    except Exception as e:
        logger.error("Could not load county config for %s: %s", county_id, e)
        pa_config = {}

    # Query properties to enrich
    properties = _query_properties(county_id, mode, limit, stale_days, leads_only=leads_only, parcel_id=parcel_id)
    if not properties:
        logger.info("No properties to enrich (mode=%s)", mode)
        return {"updated": 0, "skipped": 0, "errors": 0, "duration_s": 0}

    logger.info("Enriching %d properties", len(properties))

    # Scrape in thread pool — sync Playwright per thread
    results: list[pd.DataFrame] = []
    errors = 0

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {
            pool.submit(_scrape_and_parse, prop, pa_config, county_id, headful, debug): prop
            for prop in properties
        }
        for future in as_completed(futures):
            prop = futures[future]
            try:
                df = future.result()
                if df is not None:
                    results.append(df)
                else:
                    errors += 1
            except Exception as e:
                logger.error("Unexpected error for parcel %s: %s", prop.get("parcel_id"), e)
                errors += 1

    if not results:
        logger.warning("All scrapes failed or returned no data")
        _log_run_stats(county_id, mode, 0, 0, errors, time.time() - run_start)
        return {"updated": 0, "skipped": 0, "errors": errors, "duration_s": int(time.time() - run_start)}

    # Load to DB
    updated = skipped = 0
    if load_to_db:
        from src.loaders.property_appraiser import PropertyAppraiserLoader

        # Process in batches of COMMIT_BATCH_SIZE
        for batch_start in range(0, len(results), _COMMIT_BATCH_SIZE):
            batch = results[batch_start: batch_start + _COMMIT_BATCH_SIZE]
            batch_df = pd.concat(batch, ignore_index=True)

            with get_db_context() as session:
                loader = PropertyAppraiserLoader(session, county_id=county_id)
                u, _u, s = loader.load_from_dataframe(batch_df)
                updated += u
                skipped += s

            logger.info("Loaded batch %d–%d | updated=%d skipped=%d",
                        batch_start + 1, batch_start + len(batch), u, s)
    else:
        logger.info("Dry run — %d rows parsed, not loaded (use --load-to-db)", len(results))
        updated = len(results)

    duration_s = time.time() - run_start
    _log_run_stats(county_id, mode, updated, skipped, errors, duration_s)
    logger.info("PA enrichment done | updated=%d skipped=%d errors=%d duration=%.1fs",
                updated, skipped, errors, duration_s)

    return {"updated": updated, "skipped": skipped, "errors": errors, "duration_s": int(duration_s)}


def _log_run_stats(county_id: str, mode: str, updated: int, skipped: int, errors: int, duration_s: float) -> None:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    try:
        with get_db_context() as session:
            values = {
                "county_id":       county_id,
                "source_type":     "property_appraiser",
                "run_date":        datetime.now(timezone.utc).date(),
                "total_scraped":   updated + skipped + errors,
                "matched":         updated,
                "skipped":         skipped,
                "run_success":     errors == 0,
                "duration_seconds": round(duration_s, 2),
            }
            stmt = (
                pg_insert(ScraperRunStats)
                .values(**values)
                .on_conflict_do_update(
                    constraint="uq_scraper_run_stats",
                    set_={
                        "total_scraped":    values["total_scraped"],
                        "matched":          values["matched"],
                        "skipped":          values["skipped"],
                        "run_success":      values["run_success"],
                        "duration_seconds": values["duration_seconds"],
                    },
                )
            )
            session.execute(stmt)
    except Exception as e:
        logger.warning("Could not log ScraperRunStats: %s", e)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HCPA Property Appraiser Enrichment Engine")
    parser.add_argument("--county-id", default="hillsborough")
    parser.add_argument("--mode", choices=["new-only", "refresh", "all"], default="new-only")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--stale-days", type=int, default=30,
                        help="Days before a refreshed property is considered stale (refresh mode only)")
    parser.add_argument("--load-to-db", action="store_true", help="Write enriched data to DB")
    parser.add_argument("--headful", action="store_true", help="Show browser window")
    parser.add_argument("--debug", action="store_true", help="Print raw scraped text and parsed fields")
    parser.add_argument("--leads-only", action="store_true",
                        help="Restrict to properties with qualified=true in their latest distress_score (~15k leads)")
    parser.add_argument("--parcel-id", default=None, help="Scrape one exact parcel_id for debugging")
    args = parser.parse_args()

    result = asyncio.run(run_pa_pipeline(
        county_id=args.county_id,
        mode=args.mode,
        limit=args.limit,
        stale_days=args.stale_days,
        load_to_db=args.load_to_db,
        headful=args.headful,
        debug=args.debug,
        leads_only=args.leads_only,
        parcel_id=args.parcel_id,
    ))
    print(result)


if __name__ == "__main__":
    main()
