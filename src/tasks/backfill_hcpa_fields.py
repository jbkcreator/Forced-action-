"""
One-time backfill: populate year_built, last_sale_date, last_sale_price
for existing property records that have NULL values.

Downloads the HCPA master parcel spreadsheet directly from hcpafl.org,
processes it, updates existing records, then cleans up the downloaded file.
Safe to run multiple times — only updates NULL fields, never overwrites.

Usage:
    python -m src.tasks.backfill_hcpa_fields
    python -m src.tasks.backfill_hcpa_fields --dry-run    # preview column detection only
    python -m src.tasks.backfill_hcpa_fields --county hillsborough
"""

import argparse
import asyncio
import logging
import shutil
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from src.core.database import get_db_context
from src.core.models import Financial, Property
from src.scrappers.master.master_engine import convert_xls_to_csv, download_parcel_master
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

STAGING_DIR  = Path("data/raw/master/backfill_staging")
BATCH_SIZE   = 2000

# Candidate column names (HCPA varies between releases)
_FOLIO_COLS     = ["FOLIO", "PARCEL_ID", "PARCELID"]
_YR_BLT_COLS    = ["YR_BLT", "YEAR_BUILT", "YR_BUILT"]
_SALE_DATE_COLS  = ["SALE1_DATE", "SALE_DATE", "SALESDATE", "SALE_DT"]
_SALE_PRC_COLS   = ["SALE1_PRC", "SALE_PRC", "SALESPRICE", "SALE_PRICE"]


def _pick_col(df_cols: list, candidates: list) -> str | None:
    upper = {c.upper(): c for c in df_cols}
    for cand in candidates:
        if cand.upper() in upper:
            return upper[cand.upper()]
    return None


def _parse_year(val) -> int | None:
    if pd.isna(val):
        return None
    try:
        y = int(float(val))
        return y if 1800 <= y <= date.today().year else None
    except (ValueError, TypeError):
        return None


def _parse_date(val):
    if pd.isna(val) or not val:
        return None
    try:
        result = pd.to_datetime(val, errors="coerce")
        return None if pd.isna(result) else result.date()
    except Exception:
        return None


def _parse_amount(val):
    if pd.isna(val) or not val:
        return None
    try:
        return float(str(val).replace(",", "").strip()) or None
    except (ValueError, TypeError):
        return None


def _detect_columns(csv_path: Path) -> dict:
    """Read first 5 rows to detect column names."""
    sample = pd.read_csv(csv_path, nrows=5, dtype=str)
    sample.columns = sample.columns.str.upper()
    cols = list(sample.columns)
    return {
        "folio":      _pick_col(cols, _FOLIO_COLS),
        "yr_blt":     _pick_col(cols, _YR_BLT_COLS),
        "sale_date":  _pick_col(cols, _SALE_DATE_COLS),
        "sale_price": _pick_col(cols, _SALE_PRC_COLS),
        "all_cols":   cols,
    }


def run_backfill(csv_path: Path, dry_run: bool, county_id: str) -> dict:
    stats = {
        "rows_read":       0,
        "year_built":      0,
        "last_sale_date":  0,
        "last_sale_price": 0,
        "already_filled":  0,
        "not_found":       0,
        "errors":          0,
    }

    col = _detect_columns(csv_path)

    logger.info("Column detection results:")
    logger.info(f"  FOLIO       → {col['folio']      or 'NOT FOUND'}")
    logger.info(f"  YR_BLT      → {col['yr_blt']     or 'not found — year_built skipped'}")
    logger.info(f"  SALE_DATE   → {col['sale_date']  or 'not found — last_sale_date skipped'}")
    logger.info(f"  SALE_PRICE  → {col['sale_price'] or 'not found — last_sale_price skipped'}")

    if not col["folio"]:
        logger.error("FOLIO column not found. Available columns:")
        logger.error(", ".join(col["all_cols"][:40]))
        sys.exit(1)

    if not any([col["yr_blt"], col["sale_date"], col["sale_price"]]):
        logger.error("None of the target columns (YR_BLT, SALE_DATE, SALE_PRICE) found.")
        logger.error(f"Available columns: {', '.join(col['all_cols'][:50])}")
        sys.exit(1)

    if dry_run:
        logger.info("DRY RUN — column detection complete. No DB writes.")
        return stats

    chunk_num = 0
    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=5000):
        chunk.columns = chunk.columns.str.upper()
        chunk_num += 1
        stats["rows_read"] += len(chunk)

        with get_db_context() as session:
            for _, row in chunk.iterrows():
                parcel_id = str(row.get(col["folio"], "")).strip()
                if not parcel_id or parcel_id == "nan":
                    continue

                try:
                    prop = session.query(Property).filter_by(
                        parcel_id=parcel_id, county_id=county_id
                    ).first()

                    if not prop:
                        stats["not_found"] += 1
                        continue

                    prop_dirty = False
                    if col["yr_blt"] and prop.year_built is None:
                        y = _parse_year(row.get(col["yr_blt"]))
                        if y:
                            prop.year_built = y
                            stats["year_built"] += 1
                            prop_dirty = True

                    fin = prop.financial
                    fin_dirty = False
                    if fin is None:
                        fin = Financial(property_id=prop.id)
                        session.add(fin)
                        fin_dirty = True

                    if col["sale_date"] and fin.last_sale_date is None:
                        d = _parse_date(row.get(col["sale_date"]))
                        if d:
                            fin.last_sale_date = d
                            stats["last_sale_date"] += 1
                            fin_dirty = True

                    if col["sale_price"] and fin.last_sale_price is None:
                        amt = _parse_amount(row.get(col["sale_price"]))
                        if amt:
                            fin.last_sale_price = amt
                            stats["last_sale_price"] += 1
                            fin_dirty = True

                    if not prop_dirty and not fin_dirty:
                        stats["already_filled"] += 1

                except Exception as e:
                    logger.warning(f"Error on parcel {parcel_id}: {e}")
                    stats["errors"] += 1

            session.commit()

        logger.info(
            f"Chunk {chunk_num}: rows={stats['rows_read']:,} "
            f"yr={stats['year_built']:,} "
            f"date={stats['last_sale_date']:,} "
            f"price={stats['last_sale_price']:,} "
            f"not_found={stats['not_found']:,}"
        )

    return stats


async def main(dry_run: bool, county_id: str):
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    xls_path = None
    csv_path = None

    try:
        # Step 1 — download HCPA spreadsheet
        logger.info("Downloading HCPA PARCEL_SPREADSHEET.xls ...")
        xls_path = await download_parcel_master()
        if not xls_path:
            logger.error("Download failed — cannot proceed")
            sys.exit(1)
        logger.info(f"Downloaded: {xls_path}")

        # Step 2 — convert to CSV
        logger.info("Converting XLS to CSV ...")
        csv_path = convert_xls_to_csv(xls_path, STAGING_DIR)
        logger.info(f"Converted: {csv_path}")

        # Step 3 — run backfill
        logger.info("Running backfill ...")
        result = run_backfill(csv_path, dry_run=dry_run, county_id=county_id)

        print("\nBackfill complete:")
        print(f"  Rows read         : {result['rows_read']:,}")
        print(f"  year_built filled : {result['year_built']:,}")
        print(f"  last_sale_date    : {result['last_sale_date']:,}")
        print(f"  last_sale_price   : {result['last_sale_price']:,}")
        print(f"  Already filled    : {result['already_filled']:,}")
        print(f"  Not in DB         : {result['not_found']:,}")
        print(f"  Errors            : {result['errors']:,}")

    finally:
        # Step 4 — clean up downloaded files regardless of success/failure
        if STAGING_DIR.exists():
            shutil.rmtree(STAGING_DIR)
            logger.info(f"Cleaned up staging dir: {STAGING_DIR}")
        if xls_path and xls_path.exists():
            xls_path.unlink()
            logger.info(f"Cleaned up XLS: {xls_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill year_built/last_sale fields from HCPA")
    parser.add_argument("--county",  default="hillsborough")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download and detect columns only — no DB writes")
    args = parser.parse_args()

    asyncio.run(main(dry_run=args.dry_run, county_id=args.county))
