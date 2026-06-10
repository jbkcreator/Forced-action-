"""
Backfill description column for existing Pinellas building permits.

Re-scrapes the Pinellas Accela portal month-by-month, then for each row returned
does a targeted SQL UPDATE on building_permits WHERE permit_number matches
AND description IS NULL. Never inserts new records — only updates existing ones.

Idempotent: rows that already have a description are skipped (WHERE description IS NULL).

Usage:
    # Dry run — shows how many rows would be updated without committing
    python scripts/backfill_pinellas_permit_descriptions.py --dry-run

    # Full backfill (date range auto-detected from DB)
    python scripts/backfill_pinellas_permit_descriptions.py

    # Explicit date range
    python scripts/backfill_pinellas_permit_descriptions.py --start-date 2024-01-01 --end-date 2026-06-10

    # With visible browser (useful when Accela shows a CAPTCHA)
    python scripts/backfill_pinellas_permit_descriptions.py --headful
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

if "DATABASE_URL" not in os.environ:
    env_path = PROJECT_ROOT / ".env"
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DATABASE_URL="):
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip()
            break

import pandas as pd
from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

COUNTY_ID = "pinellas"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_date_range() -> tuple[date, date]:
    """Return (earliest_issue_date, latest_issue_date) for Pinellas permits in DB."""
    with get_db_context() as session:
        row = session.execute(
            text(
                "SELECT MIN(issue_date), MAX(issue_date) "
                "FROM building_permits "
                "WHERE county_id = :cid AND issue_date IS NOT NULL"
            ),
            {"cid": COUNTY_ID},
        ).fetchone()
    if not row or not row[0]:
        raise RuntimeError("No Pinellas building permits with issue_date found in DB.")
    return row[0], row[1]


def _null_description_count() -> int:
    with get_db_context() as session:
        row = session.execute(
            text(
                "SELECT COUNT(*) FROM building_permits "
                "WHERE county_id = :cid AND description IS NULL"
            ),
            {"cid": COUNTY_ID},
        ).fetchone()
    return row[0] if row else 0


def _monthly_chunks(start: date, end: date):
    """Yield (chunk_start, chunk_end) one calendar month at a time."""
    cursor = start.replace(day=1)
    while cursor <= end:
        next_month = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        yield cursor, chunk_end
        cursor = next_month


# ---------------------------------------------------------------------------
# Scraping — runs permit_engine for one month chunk, returns DataFrame
# ---------------------------------------------------------------------------

async def _scrape_month(start_dt: date, end_dt: date, headful: bool) -> Optional[pd.DataFrame]:
    """
    Run permit_engine (extract mode, no DB load) for the given date range.
    Returns the resulting DataFrame, or None on failure.
    """
    from src.scrappers.permit.permit_engine import main as permit_main
    from config.constants import RAW_PERMIT_DIR

    import argparse as ap
    args = ap.Namespace(
        county_id=COUNTY_ID,
        start_date=start_dt.strftime("%Y-%m-%d"),
        end_date=end_dt.strftime("%Y-%m-%d"),
        load_to_db=False,   # critical — we do targeted UPDATE below, never a full load
        headful=headful,
    )

    today_str = datetime.now().strftime("%Y%m%d")
    csv_path = RAW_PERMIT_DIR / COUNTY_ID / "new" / f"permits_{COUNTY_ID}_{today_str}.csv"

    try:
        await permit_main(args)
    except Exception as exc:
        logger.error("[scrape] %s→%s failed: %s", start_dt, end_dt, exc)
        return None

    if not csv_path.exists():
        logger.warning("[scrape] No CSV produced for %s→%s", start_dt, end_dt)
        return None

    try:
        df = pd.read_csv(csv_path, dtype=str)
        logger.info("[scrape] %s→%s: %d rows", start_dt, end_dt, len(df))
        return df
    except Exception as exc:
        logger.error("[scrape] Could not read CSV for %s→%s: %s", start_dt, end_dt, exc)
        return None
    finally:
        csv_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# DB update — targeted UPDATE, never INSERT
# ---------------------------------------------------------------------------

def _apply_descriptions(df: pd.DataFrame, dry_run: bool) -> tuple[int, int]:
    """
    For each row with a non-empty Description, UPDATE building_permits
    WHERE permit_number = <value> AND county_id = 'pinellas' AND description IS NULL.

    Returns (updated, skipped).
    - updated: rows where DB record existed with NULL description and was updated
    - skipped: rows with no description in CSV, or DB record already had description,
               or permit_number not found in DB at all
    """
    updated = 0
    skipped = 0

    # Build a lookup: permit_number → description (only rows that have a description)
    candidates: dict[str, str] = {}
    for _, row in df.iterrows():
        permit_number = str(row.get("Record Number") or "").strip()
        description = str(row.get("Description") or "").strip()
        if permit_number and description:
            candidates[permit_number] = description

    if not candidates:
        logger.info("[update] No rows with description in this chunk — skipping")
        return 0, len(df)

    with get_db_context() as session:
        for permit_number, description in candidates.items():
            if dry_run:
                exists = session.execute(
                    text(
                        "SELECT 1 FROM building_permits "
                        "WHERE permit_number = :pnum "
                        "  AND county_id = :cid "
                        "  AND description IS NULL"
                    ),
                    {"pnum": permit_number, "cid": COUNTY_ID},
                ).fetchone()
                if exists:
                    updated += 1
                else:
                    skipped += 1
            else:
                result = session.execute(
                    text(
                        "UPDATE building_permits "
                        "SET description = :desc "
                        "WHERE permit_number = :pnum "
                        "  AND county_id = :cid "
                        "  AND description IS NULL"
                    ),
                    {"desc": description, "pnum": permit_number, "cid": COUNTY_ID},
                )
                if getattr(result, "rowcount", 0) > 0:
                    updated += 1
                else:
                    skipped += 1

        if not dry_run:
            session.commit()

    return updated, skipped


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run(start_date: date, end_date: date, dry_run: bool, headful: bool) -> None:
    null_before = _null_description_count()
    chunks = list(_monthly_chunks(start_date, end_date))

    logger.info(
        "Pinellas permit description backfill: %s → %s | %d month(s) | "
        "NULL before=%d%s",
        start_date, end_date, len(chunks), null_before,
        " [DRY RUN]" if dry_run else "",
    )

    total_updated = 0
    total_skipped = 0

    for chunk_start, chunk_end in chunks:
        logger.info("── chunk %s → %s", chunk_start, chunk_end)
        df = await _scrape_month(chunk_start, chunk_end, headful)
        if df is None or df.empty:
            logger.info("   no records from scraper")
            continue

        updated, skipped = _apply_descriptions(df, dry_run)
        total_updated += updated
        total_skipped += skipped
        logger.info("   updated=%d  skipped=%d", updated, skipped)

    null_after = _null_description_count() if not dry_run else null_before
    logger.info(
        "── DONE%s: updated=%d  skipped=%d  NULL remaining=%d",
        " (dry run)" if dry_run else "",
        total_updated, total_skipped, null_after,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill description column for existing Pinellas building permits"
    )
    parser.add_argument(
        "--start-date", type=str,
        help="Start date YYYY-MM-DD (default: earliest permit issue_date in DB)",
    )
    parser.add_argument(
        "--end-date", type=str,
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count how many rows would be updated without committing",
    )
    parser.add_argument(
        "--headful", action="store_true",
        help="Run Playwright browser in headful mode (useful for manual CAPTCHA solving)",
    )
    args = parser.parse_args()

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date, _ = _db_date_range()
        logger.info("Auto-detected start date from DB: %s", start_date)

    end_date = (
        datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if args.end_date
        else date.today()
    )

    asyncio.run(run(start_date, end_date, args.dry_run, args.headful))


if __name__ == "__main__":
    main()
