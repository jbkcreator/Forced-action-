"""Load the OFR individual Loan Originator (LO) files — List 4 gap (WP-W0-1).

Josh's own list taxonomy names List 4 "brokers and LOs", but Pool 3 only loads
broker businesses (MBR/MBRB) today — LOs are intentionally not wired into the
dialer pool yet. This loader exists to measure real LO volume/phone-coverage
so that decision can be made with real numbers, not a guess.

Confirmed from real sample data (LoanOriginators_AI_Monthly.csv):
  - NATIONWIDE NMLS registry. Sample rows show Michigan and Oregon addresses —
    most LOs with an FL license are NOT Florida residents. Filter on prim_state
    before assuming this is a local-FL list.
  - PHONE is blank on essentially every row. Every loaded record will need
    skip-trace before it is callable — budget credits accordingly, and only
    for the subset that's actually FL-relevant (see above).

OFR splits the LO file into 3 monthly zips by surname range, unlike the single
MBR-MBRB broker zip:
    LoanOriginators_AI_Monthly.zip   (A-I)
    LoanOriginators_JR_Monthly.zip   (J-R)
    LoanOriginators_SZ_Monthly.zip   (S-Z)

Two modes:
  * AUTO (default, for cron): downloads all 3 zips from settings, loads each.
      Requires OFR_LO_ENABLED=true and all 3 OFR_LO_DOWNLOAD_URL_* set.
  * MANUAL (--csv PATH): load one CSV already on disk (local/dev use). Run
      three times, once per surname-range file, for a full load.

Usage:
    PYTHONPATH=. python -m src.tasks.ofr_lo_load                      # AUTO (cron)
    PYTHONPATH=. python -m src.tasks.ofr_lo_load --csv "/path/AI.csv"  # MANUAL, one file
    PYTHONPATH=. python -m src.tasks.ofr_lo_load --dry-run             # parse+count only
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sys
import tempfile
import zipfile
from datetime import datetime

from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.phone_utils import normalize as normalize_phone
from src.utils.http_helpers import requests_get_with_retry
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# The 3 surname-range download URLs, in settings attribute order.
_URL_SETTINGS_ATTRS = (
    "ofr_lo_download_url_ai",
    "ofr_lo_download_url_jr",
    "ofr_lo_download_url_sz",
)


def download_and_extract_csvs(dest_dir: str) -> list[str]:
    """Download all 3 OFR LO zips and extract their CSVs. Returns the CSV paths.

    Gated on settings (OFR_LO_ENABLED + all 3 URLs). Fail-closed: raises with a
    clear message if disabled or any URL is missing, so cron surfaces it rather
    than silently loading a partial surname range.
    """
    settings = get_settings()
    if not settings.ofr_lo_enabled:
        raise RuntimeError(
            "OFR_LO_ENABLED is false — auto-download disabled. "
            "Set it true in production (and all 3 OFR_LO_DOWNLOAD_URL_* vars), or run with --csv."
        )
    urls = [getattr(settings, attr) for attr in _URL_SETTINGS_ATTRS]
    missing = [attr for attr, url in zip(_URL_SETTINGS_ATTRS, urls) if not url]
    if missing:
        raise RuntimeError(f"Missing OFR LO download URL(s): {missing}")

    csv_paths: list[str] = []
    for attr, url in zip(_URL_SETTINGS_ATTRS, urls):
        logger.info("Downloading OFR LO file (%s)…", attr)
        resp = requests_get_with_retry(url, timeout=120)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"No CSV found inside OFR LO zip {attr} (members: {zf.namelist()})")
            csv_name = csv_names[0]
            zf.extract(csv_name, dest_dir)
            extracted = os.path.join(dest_dir, csv_name)
            logger.info("Extracted OFR LO CSV: %s", os.path.basename(extracted))
            csv_paths.append(extracted)
    return csv_paths


def _parse_ofr_date(raw: str | None):
    """Parse OFR's DD-MON-YYYY dates (e.g. '06-JUL-2023'); return None on failure."""
    if not raw or not raw.strip():
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def load_csv(session, csv_path: str, *, dry_run: bool = False) -> dict:
    """Upsert individual LO rows from one OFR LO surname-range CSV. Returns a summary."""
    rows: list[dict] = []
    total = 0

    with open(csv_path, newline="", encoding="utf-8-sig", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            total += 1
            license_number = (r.get("LICENSE NUMBER") or "").strip()
            if not license_number:
                continue
            phone_raw = (r.get("PHONE") or "").strip()
            rows.append({
                "license_number": license_number,
                "nmls_id": (r.get("NMLS ID") or "").strip() or None,
                "last_name": (r.get("LAST NAME") or "").strip() or None,
                "first_name": (r.get("FIRST NAME") or "").strip() or None,
                "middle_name": (r.get("MIDDLE NAME") or "").strip() or None,
                "prim_address_1": (r.get("PRIM ADDRESS 1") or "").strip() or None,
                "prim_address_2": (r.get("PRIM ADDRESS 2") or "").strip() or None,
                "prim_city": (r.get("PRIM CITY") or "").strip() or None,
                "county": (r.get("COUNTY") or "").strip() or None,
                "prim_state": (r.get("PRIM STATE") or "").strip() or None,
                "prim_zip": (r.get("PRIM ZIP") or "").strip() or None,
                "phone_raw": phone_raw or None,
                "normalized_phone": normalize_phone(phone_raw),
                "status": (r.get("STATUS") or "").strip() or None,
                "status_effective_date": _parse_ofr_date(r.get("STATUS EFFECTIVE DATE")),
                "initial_approval": _parse_ofr_date(r.get("INTIAL APPROVAL")),
            })

    summary = {
        "csv_path": csv_path,
        "total_rows": total,
        "lo_rows": len(rows),
        "with_phone": sum(1 for x in rows if x["normalized_phone"]),
        "fl_state": sum(1 for x in rows if (x["prim_state"] or "").upper() == "FL"),
        "approved": sum(1 for x in rows if (x["status"] or "").strip().lower() == "approved"),
        "dry_run": dry_run,
    }

    if dry_run or not rows:
        logger.info("ofr_lo_load dry_run=%s summary=%s", dry_run, summary)
        return summary

    _COLS = [
        "license_number", "nmls_id", "last_name", "first_name", "middle_name",
        "prim_address_1", "prim_address_2", "prim_city", "county", "prim_state", "prim_zip",
        "phone_raw", "normalized_phone", "status", "status_effective_date", "initial_approval",
    ]
    upsert_sql = f"""
        INSERT INTO ofr_loan_originators ({", ".join(_COLS)})
        VALUES %s
        ON CONFLICT (license_number) DO UPDATE SET
            nmls_id               = EXCLUDED.nmls_id,
            last_name             = EXCLUDED.last_name,
            first_name            = EXCLUDED.first_name,
            middle_name           = EXCLUDED.middle_name,
            prim_address_1        = EXCLUDED.prim_address_1,
            prim_address_2        = EXCLUDED.prim_address_2,
            prim_city             = EXCLUDED.prim_city,
            county                = EXCLUDED.county,
            prim_state            = EXCLUDED.prim_state,
            prim_zip              = EXCLUDED.prim_zip,
            phone_raw             = EXCLUDED.phone_raw,
            normalized_phone      = EXCLUDED.normalized_phone,
            status                = EXCLUDED.status,
            status_effective_date = EXCLUDED.status_effective_date,
            initial_approval      = EXCLUDED.initial_approval,
            loaded_at             = NOW()
    """

    # Bulk upsert via psycopg2 execute_values — same pattern as ofr_broker_load.py
    # (one network round-trip per batch; plain executemany crawls over a remote connection).
    import psycopg2.extras

    batch_size = 1000
    upserted = 0
    raw = session.get_bind().raw_connection()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            values = [tuple(r[c] for c in _COLS) for r in batch]
            with raw.cursor() as cur:
                psycopg2.extras.execute_values(cur, upsert_sql, values, page_size=batch_size)
            raw.commit()
            upserted += len(batch)
            logger.info("ofr_lo_load: upserted %d/%d", upserted, len(rows))
    finally:
        raw.close()

    summary["rows_upserted"] = upserted
    logger.info("ofr_lo_load complete: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Load OFR individual LO files (auto-download by default)")
    parser.add_argument("--csv", default=None, help="Manual: path to one CSV already on disk (skips download)")
    parser.add_argument("--dry-run", action="store_true", help="Parse + count only, no DB write")
    args = parser.parse_args()

    tmp_dir = None
    csv_paths = [args.csv] if args.csv else None
    try:
        if csv_paths is None:
            # AUTO mode (cron): download + unzip all 3 surname-range files into a temp dir.
            tmp_dir = tempfile.mkdtemp(prefix="ofr_lo_")
            csv_paths = download_and_extract_csvs(tmp_dir)

        summaries = []
        with get_db_context() as session:
            for path in csv_paths:
                summaries.append(load_csv(session, path, dry_run=args.dry_run))

        combined = {
            "files_loaded": len(summaries),
            "total_rows": sum(s["total_rows"] for s in summaries),
            "lo_rows": sum(s["lo_rows"] for s in summaries),
            "with_phone": sum(s["with_phone"] for s in summaries),
            "fl_state": sum(s["fl_state"] for s in summaries),
            "approved": sum(s["approved"] for s in summaries),
            "per_file": summaries,
        }
        print(json.dumps(combined, indent=2, default=str))
    finally:
        if tmp_dir:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.info("Cleaned up temp download dir (no contact data left on disk).")


if __name__ == "__main__":
    main()
    sys.exit(0)
