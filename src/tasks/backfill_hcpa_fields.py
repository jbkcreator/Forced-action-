"""
One-time backfill: populate year_built, last_sale_date, last_sale_price
from an HCPA master CSV for existing property records that have NULL values.

Safe to run multiple times — only updates rows where values are NULL.
Never overwrites existing data.

Usage:
    python -m src.tasks.backfill_hcpa_fields --csv data/raw/master/new/master_YYYYMMDD.csv
    python -m src.tasks.backfill_hcpa_fields --csv data/reference/PARCEL_SPREADSHEET.csv
    python -m src.tasks.backfill_hcpa_fields --csv <path> --dry-run   # preview without writing
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from src.core.database import get_db_context
from src.core.models import Financial, Property
from src.loaders.base import BaseLoader
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

BATCH_SIZE = 2000

# Candidate column names to try for each field (HCPA varies between releases)
_YR_BLT_COLS   = ["YR_BLT", "YEAR_BUILT", "YR_BUILT"]
_SALE_DATE_COLS = ["SALE1_DATE", "SALE_DATE", "SALESDATE", "SALE_DT"]
_SALE_PRC_COLS  = ["SALE1_PRC", "SALE_PRC", "SALESPRICE", "SALE_PRICE"]
_FOLIO_COLS     = ["FOLIO", "PARCEL_ID", "PARCELID"]


def _pick_col(df_cols: list, candidates: list) -> str | None:
    """Return the first candidate column name present in df_cols."""
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
        return pd.to_datetime(val, errors="coerce").date()
    except Exception:
        return None


def _parse_amount(val):
    if pd.isna(val) or not val:
        return None
    try:
        return float(str(val).replace(",", "").strip()) or None
    except (ValueError, TypeError):
        return None


def run_backfill(csv_path: str, dry_run: bool = False, county_id: str = "hillsborough") -> dict:
    path = Path(csv_path)
    if not path.exists():
        logger.error(f"CSV not found: {path}")
        sys.exit(1)

    stats = {
        "rows_read":       0,
        "year_built":      0,
        "last_sale_date":  0,
        "last_sale_price": 0,
        "not_found":       0,
        "already_filled":  0,
        "errors":          0,
    }

    # --- Detect columns from first chunk ---
    sample = pd.read_csv(path, nrows=5, dtype=str)
    sample.columns = sample.columns.str.upper()
    cols = list(sample.columns)

    folio_col     = _pick_col(cols, _FOLIO_COLS)
    yr_col        = _pick_col(cols, _YR_BLT_COLS)
    sale_date_col = _pick_col(cols, _SALE_DATE_COLS)
    sale_prc_col  = _pick_col(cols, _SALE_PRC_COLS)

    logger.info("Column detection:")
    logger.info(f"  FOLIO       → {folio_col or 'NOT FOUND — cannot proceed'}")
    logger.info(f"  YR_BLT      → {yr_col or 'not found — year_built will be skipped'}")
    logger.info(f"  SALE_DATE   → {sale_date_col or 'not found — last_sale_date will be skipped'}")
    logger.info(f"  SALE_PRC    → {sale_prc_col or 'not found — last_sale_price will be skipped'}")

    if not folio_col:
        logger.error("FOLIO column not found in CSV. Available columns:")
        logger.error(", ".join(cols[:30]))
        sys.exit(1)

    if not any([yr_col, sale_date_col, sale_prc_col]):
        logger.error("None of the target columns found. Nothing to backfill.")
        logger.error(f"Available columns: {', '.join(cols[:50])}")
        sys.exit(1)

    if dry_run:
        logger.info("DRY RUN — no writes will occur")

    # --- Process in chunks ---
    chunk_num = 0
    for chunk in pd.read_csv(path, dtype=str, chunksize=5000):
        chunk.columns = chunk.columns.str.upper()
        chunk_num += 1
        stats["rows_read"] += len(chunk)

        with get_db_context() as session:
            batch_props = []
            batch_fins  = []

            for _, row in chunk.iterrows():
                parcel_id = str(row.get(folio_col, "")).strip()
                if not parcel_id or parcel_id == "nan":
                    continue

                prop = session.query(Property).filter_by(
                    parcel_id=parcel_id, county_id=county_id
                ).first()

                if not prop:
                    stats["not_found"] += 1
                    continue

                # --- Property: year_built ---
                prop_dirty = False
                if yr_col and prop.year_built is None:
                    y = _parse_year(row.get(yr_col))
                    if y:
                        prop.year_built = y
                        stats["year_built"] += 1
                        prop_dirty = True

                # --- Financial: last_sale_date / last_sale_price ---
                fin = prop.financial
                fin_dirty = False

                if fin is None:
                    fin = Financial(property_id=prop.id)
                    session.add(fin)
                    fin_dirty = True

                if sale_date_col and fin.last_sale_date is None:
                    d = _parse_date(row.get(sale_date_col))
                    if d:
                        fin.last_sale_date = d
                        stats["last_sale_date"] += 1
                        fin_dirty = True

                if sale_prc_col and fin.last_sale_price is None:
                    amt = _parse_amount(row.get(sale_prc_col))
                    if amt:
                        fin.last_sale_price = amt
                        stats["last_sale_price"] += 1
                        fin_dirty = True

                if not prop_dirty and not fin_dirty:
                    stats["already_filled"] += 1

            if not dry_run:
                try:
                    session.commit()
                except Exception as e:
                    logger.error(f"Commit error on chunk {chunk_num}: {e}")
                    stats["errors"] += 1

        logger.info(
            f"Chunk {chunk_num}: rows_read={stats['rows_read']:,} "
            f"yr={stats['year_built']:,} date={stats['last_sale_date']:,} "
            f"price={stats['last_sale_price']:,} not_found={stats['not_found']:,}"
        )

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill year_built / last_sale fields from HCPA master CSV")
    parser.add_argument("--csv",      required=True, help="Path to HCPA master CSV file")
    parser.add_argument("--county",   default="hillsborough")
    parser.add_argument("--dry-run",  action="store_true", help="Preview only — no DB writes")
    args = parser.parse_args()

    result = run_backfill(args.csv, dry_run=args.dry_run, county_id=args.county)

    print("\nBackfill complete:")
    print(f"  Rows read         : {result['rows_read']:,}")
    print(f"  year_built filled : {result['year_built']:,}")
    print(f"  last_sale_date    : {result['last_sale_date']:,}")
    print(f"  last_sale_price   : {result['last_sale_price']:,}")
    print(f"  Already filled    : {result['already_filled']:,}")
    print(f"  Not in DB         : {result['not_found']:,}")
    print(f"  Errors            : {result['errors']:,}")
