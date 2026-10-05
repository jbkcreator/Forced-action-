"""Load the OFR Ch 494 MBR-MBRB broker file into ofr_mortgage_brokers — WP-W0-1 Pool 3.

Fully automated monthly pull for Pool 3: downloads the Florida OFR "Ch 494
Businesses - NMLS (MBR-MBRB)" zip, unzips it, and upserts each broker-business
license into ofr_mortgage_brokers.  This is the authoritative FL mortgage-broker
registry (spec §4.1).  Individual loan originators (the LO file) are
intentionally not loaded — they are employees, not brokers, carry no employer
link and no phone.

Two modes:
  * AUTO (default, for cron): download + unzip + load from settings.
      Requires OFR_BROKER_ENABLED=true and OFR_BROKER_DOWNLOAD_URL set.
      Run with no args — nothing to hand-load.
  * MANUAL (--csv PATH): load a CSV already on disk (local/dev use).

Downloaded/extracted files land in a temp dir and are deleted after load — the
CSV holds real contact data and is never written into the repo.

Usage:
    PYTHONPATH=. python -m src.tasks.ofr_broker_load                 # AUTO (cron)
    PYTHONPATH=. python -m src.tasks.ofr_broker_load --csv "/path/...csv"   # MANUAL
    PYTHONPATH=. python -m src.tasks.ofr_broker_load --dry-run       # parse+count only
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

# OFR license types that are mortgage BROKERS (not lenders, not loan officers).
_BROKER_LICENSE_TYPES = {"MBR", "MBRB"}


def download_and_extract_csv(dest_dir: str) -> str:
    """Download the OFR MBR-MBRB zip and extract its CSV. Returns the CSV path.

    Gated on settings (OFR_BROKER_ENABLED + OFR_BROKER_DOWNLOAD_URL). Fail-closed:
    raises with a clear message if disabled or misconfigured so cron surfaces it
    rather than silently loading nothing.
    """
    settings = get_settings()
    if not settings.ofr_broker_enabled:
        raise RuntimeError(
            "OFR_BROKER_ENABLED is false — auto-download disabled. "
            "Set it true in production (and OFR_BROKER_DOWNLOAD_URL), or run with --csv."
        )
    url = settings.ofr_broker_download_url
    if not url:
        raise RuntimeError("OFR_BROKER_DOWNLOAD_URL is not set — cannot auto-download.")

    logger.info("Downloading OFR broker file from configured URL…")
    resp = requests_get_with_retry(url, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found inside OFR zip (members: {zf.namelist()})")
        csv_name = csv_names[0]
        zf.extract(csv_name, dest_dir)
        extracted = os.path.join(dest_dir, csv_name)
        logger.info("Extracted OFR CSV: %s", os.path.basename(extracted))
        return extracted


def _parse_ofr_date(raw: str | None):
    """Parse OFR's DD-MON-YYYY dates (e.g. '08-SEP-2025'); return None on failure."""
    if not raw or not raw.strip():
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def load_csv(session, csv_path: str, *, dry_run: bool = False) -> dict:
    """Upsert broker-business rows from the OFR MBR-MBRB CSV. Returns a summary."""
    rows: list[dict] = []
    total = skipped_nonbroker = 0

    with open(csv_path, newline="", encoding="utf-8-sig", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            total += 1
            license_type = (r.get("LICENSE TYPE") or "").strip().upper()
            if license_type not in _BROKER_LICENSE_TYPES:
                skipped_nonbroker += 1
                continue
            license_number = (r.get("LICENSE NUMBER") or "").strip()
            if not license_number:
                continue
            phone_raw = (r.get("PHONE") or "").strip()
            rows.append({
                "license_number": license_number,
                "license_type": license_type,
                "nmls_id": (r.get("NMLS ID") or "").strip() or None,
                "firm_name": (r.get("FIRM NAME") or "").strip() or None,
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
        "skipped_non_broker": skipped_nonbroker,
        "broker_rows": len(rows),
        "with_phone": sum(1 for x in rows if x["normalized_phone"]),
        "dry_run": dry_run,
    }

    if dry_run or not rows:
        logger.info("ofr_broker_load dry_run=%s summary=%s", dry_run, summary)
        return summary

    _COLS = [
        "license_number", "license_type", "nmls_id", "firm_name",
        "prim_address_1", "prim_address_2", "prim_city", "county", "prim_state", "prim_zip",
        "phone_raw", "normalized_phone", "status", "status_effective_date", "initial_approval",
    ]
    upsert_sql = f"""
        INSERT INTO ofr_mortgage_brokers ({", ".join(_COLS)})
        VALUES %s
        ON CONFLICT (license_number) DO UPDATE SET
            license_type          = EXCLUDED.license_type,
            nmls_id               = EXCLUDED.nmls_id,
            firm_name             = EXCLUDED.firm_name,
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

    # Bulk upsert via psycopg2 execute_values — ONE network round-trip per batch
    # (plain executemany sends one INSERT per row, which crawls over a remote
    # connection). Use a dedicated raw connection from the engine so raw-cursor
    # work and commits don't collide with the passed SQLAlchemy session.
    # Commit per batch so a late failure keeps earlier progress. Upsert is
    # idempotent (ON CONFLICT), so a re-run is safe.
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
            logger.info("ofr_broker_load: upserted %d/%d", upserted, len(rows))
    finally:
        raw.close()

    summary["rows_upserted"] = upserted
    logger.info("ofr_broker_load complete: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Load OFR MBR-MBRB broker file (auto-download by default)")
    parser.add_argument("--csv", default=None, help="Manual: path to a CSV already on disk (skips download)")
    parser.add_argument("--dry-run", action="store_true", help="Parse + count only, no DB write")
    args = parser.parse_args()

    tmp_dir = None
    csv_path = args.csv
    try:
        if csv_path is None:
            # AUTO mode (cron): download + unzip into a temp dir, deleted after load.
            tmp_dir = tempfile.mkdtemp(prefix="ofr_broker_")
            csv_path = download_and_extract_csv(tmp_dir)

        with get_db_context() as session:
            summary = load_csv(session, csv_path, dry_run=args.dry_run)
        print(json.dumps(summary, indent=2, default=str))
    finally:
        if tmp_dir:
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.info("Cleaned up temp download dir (no contact data left on disk).")


if __name__ == "__main__":
    main()
    sys.exit(0)
