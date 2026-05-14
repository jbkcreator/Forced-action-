"""
DBPR Construction Industry Licensing — Weekly File Download + Loader

Downloads the public contractor license data files from the Florida DBPR
Construction Industry Licensing Board, parses and deduplicates by license
number, filters to target county ZIPs, maps license type to vertical, and
upserts into the dbpr_contacts table.

Two file types:
  - Certified contractors  (state-wide licence, exam-based)
  - Registered contractors (locally licensed, jurisdiction-specific)

Run:
    python -m src.scrappers.dbpr.dbpr_engine
    python -m src.scrappers.dbpr.dbpr_engine --source registered
    python -m src.scrappers.dbpr.dbpr_engine --dry-run
    python -m src.scrappers.dbpr.dbpr_engine --county hillsborough

Cron (weekly, Sunday 02:00 UTC — before enrichment job):
    0 2 * * 0 cd /app && python -m src.scrappers.dbpr.dbpr_engine >> /var/log/cron/dbpr.log 2>&1
"""

import argparse
import csv
import io
import logging
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import DBPRContact
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Direct CSV download URLs (no scraping needed — public extracts)
_DBPR_URLS = {
    "certified":  "https://www2.myfloridalicense.com/sto/file_download/extracts/cilb_certified.csv",
    "registered": "https://www2.myfloridalicense.com/sto/file_download/extracts/cilb_registered.csv",
}

# Local download staging directory
_DOWNLOAD_DIR = Path("data/dbpr")

# License type code → platform vertical
_LICENSE_TO_VERTICAL: dict[str, str] = {
    "CCC": "roofing",
    "CRC": "roofing",          # Registered roofing
    "CGC": "general",
    "CBC": "general",
    "RGC": "general",          # Registered general
    "RBC": "general",          # Registered building
    "CFC": "plumbing",
    "RFC": "plumbing",         # Registered plumbing
    "CAC": "hvac",
    "CMC": "hvac",
    "RAC": "hvac",             # Registered AC
    "RMC": "hvac",             # Registered mechanical
    "MRSA": "remediation",
    "MRSR": "remediation",
}

# TSV column positions (0-indexed) — confirmed from sample data
_COL_TYPE_CODE   = 0   # CGC / CFC / CBC …
_COL_TYPE_DESC   = 1   # Cert General / Cert Plumbing …
_COL_LIC_NUMBER  = 2   # CGC058548
_COL_FULL_NAME   = 3   # AALDERINK, JAMES
_COL_ADDRESS     = 4   # 623 SE 19 CT
_COL_BLANK       = 5   # always empty — skip
_COL_CITY_ST_ZIP = 6   # CAPE CORAL, FL  33990
_COL_EXPIRY      = 7   # 8/31/2026
# Columns 8–11 are CE course data — not loaded

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _map_vertical(license_code: str) -> Optional[str]:
    return _LICENSE_TO_VERTICAL.get((license_code or "").upper())


def _parse_city_state_zip(raw: str) -> tuple[str, str, str]:
    """Parse 'CAPE CORAL, FL  33990' → (city, state, zip)."""
    raw = raw.strip()
    # Match last token as ZIP (may include ZIP+4)
    m = re.match(r"^(.*?),?\s+([A-Z]{2})\s+([\d-]+)\s*$", raw)
    if m:
        city = m.group(1).strip().rstrip(",")
        state = m.group(2)
        zip_code = m.group(3)[:5]   # strip ZIP+4
        return city, state, zip_code
    return raw, "FL", ""


def _parse_expiry(raw: str) -> Optional[date]:
    try:
        return datetime.strptime(raw.strip(), "%m/%d/%Y").date()
    except Exception:
        return None


def _zip_to_county(zip_code: str, zip_county_map: dict[str, str]) -> Optional[str]:
    return zip_county_map.get(zip_code)


def _build_zip_county_map() -> dict[str, str]:
    """Build ZIP -> county_id lookup from the NWS same-to-zip crosswalk.

    Covers all counties currently defined in nws_same_to_zip.UGC_TO_ZIPS.
    To support a new FL county: add its SAME/UGC entries to nws_same_to_zip.py,
    then add the UGC -> county_id mapping here.
    """
    from src.services.nws_same_to_zip import UGC_TO_ZIPS

    # UGC zone -> county_id (extend when new counties are activated)
    ugc_to_county = {
        "FLC057": "hillsborough",
        "FLC103": "pinellas",
        "FLC101": "pasco",
        "FLC105": "polk",
        "FLC081": "manatee",
    }
    zip_map: dict[str, str] = {}
    for ugc, zips in UGC_TO_ZIPS.items():
        county_id = ugc_to_county.get(ugc)
        if county_id:
            for z in zips:
                zip_map[z] = county_id
    return zip_map


# ---------------------------------------------------------------------------
# Download (Playwright)
# ---------------------------------------------------------------------------

def _download_file(source: str = "certified", dry_run: bool = False) -> Optional[Path]:
    """
    Download the DBPR contractor CSV directly via requests.
    No Playwright needed — the file is a public direct-download URL.

    Returns the local path to the saved file, or None on failure.
    """
    import requests
    from src.utils.http_helpers import get_requests_proxies

    url = _DBPR_URLS.get(source)
    if not url:
        logger.error("[DBPR] Unknown source '%s'", source)
        return None

    _DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = _DOWNLOAD_DIR / f"dbpr_{source}_{date.today()}.csv"

    logger.info("[DBPR] Downloading %s file from %s", source, url)
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        existing_bytes = dest.stat().st_size if dest.exists() else 0
        headers = {"User-Agent": "ForcedAction/1.0 (distressed-property-intelligence)"}
        if existing_bytes:
            headers["Range"] = f"bytes={existing_bytes}-"
            logger.info("[DBPR] Resuming from byte %d (attempt %d/%d)", existing_bytes, attempt, max_retries)
        else:
            logger.info("[DBPR] Starting download (attempt %d/%d)", attempt, max_retries)

        try:
            resp = requests.get(
                url,
                headers=headers,
                proxies=get_requests_proxies(),
                timeout=(10, 120),
                stream=True,
            )
            if resp.status_code == 416:
                # Range not satisfiable — file already complete
                logger.info("[DBPR] File already fully downloaded")
                return dest
            resp.raise_for_status()

            mode = "ab" if existing_bytes and resp.status_code == 206 else "wb"
            with open(dest, mode) as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    f.write(chunk)

            size_mb = dest.stat().st_size / 1_048_576
            logger.info("[DBPR] Download complete -- %.1f MB saved to %s", size_mb, dest)
            return dest

        except Exception as e:
            logger.warning("[DBPR] Download attempt %d/%d failed: %s", attempt, max_retries, e)
            if attempt < max_retries:
                time.sleep(5)
            else:
                logger.error("[DBPR] All %d download attempts exhausted", max_retries)
                return None


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def _parse_file(path: Path, source: str = "certified") -> list[dict]:
    """
    Parse the DBPR TSV file. Deduplicates by license_number — keeps first
    occurrence of each license (all CE rows after the first are identical
    on columns 0–7, so first-seen is sufficient).

    Returns list of dicts, one per unique license.
    """
    seen: set[str] = set()
    records: list[dict] = []
    skipped_bad = 0

    with open(path, encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            try:
                row = next(csv.reader([raw_line], delimiter=","))
            except csv.Error:
                skipped_bad += 1
                continue
            if len(row) < 8:
                continue

            license_number = row[_COL_LIC_NUMBER].strip()
            if not license_number or license_number in seen:
                continue
            seen.add(license_number)

            type_code = row[_COL_TYPE_CODE].strip()
            city, state, zip_code = _parse_city_state_zip(row[_COL_CITY_ST_ZIP])

            records.append({
                "license_number":   license_number,
                "license_type_code": type_code,
                "license_type_desc": row[_COL_TYPE_DESC].strip(),
                "full_name":        row[_COL_FULL_NAME].strip(),
                "address":          row[_COL_ADDRESS].strip() or None,
                "city":             city or None,
                "state":            state or "FL",
                "zip_code":         zip_code or None,
                "license_expiry":   _parse_expiry(row[_COL_EXPIRY]),
                "vertical":         _map_vertical(type_code),
                "data_source":      source,
            })

    logger.info("[DBPR] Parsed %d unique licenses from %s (skipped %d malformed rows)",
                len(records), path.name, skipped_bad)
    return records


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------

def _filter_to_county(records: list[dict], target_county: str,
                       zip_county_map: dict[str, str]) -> list[dict]:
    """Keep only records whose ZIP maps to target_county. Sets county_id."""
    filtered = []
    for r in records:
        county = _zip_to_county(r["zip_code"] or "", zip_county_map)
        if county == target_county:
            r["county_id"] = county
            filtered.append(r)
    logger.info("[DBPR] %d records match county=%s", len(filtered), target_county)
    return filtered


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def _purge_stale_registered(sync_started_at: datetime, db) -> int:
    """
    Delete registered rows not present in the latest file.
    Registered licenses that disappear from the file have been revoked or
    upgraded to certified — the old row is no longer valid.
    Only runs after a registered file sync, not after certified.
    """
    from sqlalchemy import delete
    result = db.execute(
        delete(DBPRContact).where(
            DBPRContact.data_source == "registered",
            DBPRContact.last_synced_at < sync_started_at,
        )
    )
    db.commit()
    return result.rowcount


def _upsert_records(records: list[dict], db) -> tuple[int, int]:
    """
    Upsert into dbpr_contacts on license_number.

    Updates identity/location/expiry/vertical/data_source/last_synced_at.
    Preserves: email, phone, enrichment_status, email_status, subscriber_id.

    Returns (inserted, updated).
    """
    now = datetime.now(timezone.utc)
    inserted = updated = 0

    for r in records:
        stmt = pg_insert(DBPRContact).values(
            license_number=r["license_number"],
            license_type_code=r["license_type_code"],
            license_type_desc=r["license_type_desc"],
            full_name=r["full_name"],
            address=r["address"],
            city=r["city"],
            state=r["state"],
            zip_code=r["zip_code"],
            county_id=r.get("county_id"),
            license_expiry=r["license_expiry"],
            vertical=r["vertical"],
            data_source=r["data_source"],
            last_synced_at=now,
            created_at=now,
            updated_at=now,
        ).on_conflict_do_update(
            index_elements=["license_number"],
            set_={
                "license_type_code": r["license_type_code"],
                "license_type_desc": r["license_type_desc"],
                "full_name":         r["full_name"],
                "address":           r["address"],
                "city":              r["city"],
                "state":             r["state"],
                "zip_code":          r["zip_code"],
                "county_id":         r.get("county_id"),
                "license_expiry":    r["license_expiry"],
                "vertical":          r["vertical"],
                "data_source":       r["data_source"],
                "last_synced_at":    now,
                "updated_at":        now,
                # email / phone / enrichment_status / email_status preserved
            },
        )
        result = db.execute(stmt)
        if result.rowcount == 1:
            inserted += 1
        else:
            updated += 1

    db.commit()
    return inserted, updated


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_dbpr_engine(
    source: str = "certified",
    county: str = "hillsborough",
    local_file: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """
    Full pipeline: download → parse → filter → upsert.

    Args:
        source:     'certified' or 'registered'
        county:     target county_id to filter to
        local_file: skip download and use this local file path instead
        dry_run:    parse and log without writing to DB

    Returns stats dict.
    """
    sync_started_at = datetime.now(timezone.utc)
    stats = {
        "source": source,
        "county": county,
        "parsed": 0,
        "filtered": 0,
        "inserted": 0,
        "updated": 0,
        "purged": 0,
        "errors": 0,
    }

    # Step 1 — get file
    if local_file:
        file_path = Path(local_file)
        logger.info("[DBPR] Using local file: %s", file_path)
    else:
        file_path = _download_file(source=source, dry_run=dry_run)
        if not file_path:
            logger.error("[DBPR] Download failed — aborting")
            stats["errors"] += 1
            return stats

    # Step 2 — parse + dedup
    try:
        records = _parse_file(file_path, source=source)
        stats["parsed"] = len(records)
    except Exception as e:
        logger.error("[DBPR] Parse failed: %s", e)
        stats["errors"] += 1
        return stats

    # Step 3 — filter to county
    zip_county_map = _build_zip_county_map()
    records = _filter_to_county(records, county, zip_county_map)
    stats["filtered"] = len(records)

    if dry_run:
        logger.info("[DBPR DRY RUN] Would upsert %d records — no DB writes", len(records))
        for r in records[:5]:
            logger.info("[DBPR DRY RUN] %s | %s | %s | %s",
                        r["license_number"], r["full_name"], r["zip_code"], r["vertical"])
        return stats

    # Step 4 — upsert
    with get_db_context() as db:
        inserted, updated = _upsert_records(records, db)
        stats["inserted"] = inserted
        stats["updated"] = updated

    # Step 4b — purge stale registered rows (revoked/upgraded licenses)
    if source == "registered":
        with get_db_context() as db:
            purged = _purge_stale_registered(sync_started_at, db)
            stats["purged"] = purged
            if purged:
                logger.info("[DBPR] Purged %d stale registered rows not in latest file", purged)

    # Step 5 — delete downloaded file after successful load (don't accumulate stale files)
    # Only delete if we downloaded it (not a user-supplied --file path)
    if not local_file and file_path and file_path.exists():
        try:
            file_path.unlink()
            logger.info("[DBPR] Deleted downloaded file %s after successful load", file_path.name)
        except Exception as e:
            logger.warning("[DBPR] Could not delete file %s: %s", file_path.name, e)

    logger.info(
        "[DBPR] Complete -- parsed=%d filtered=%d inserted=%d updated=%d errors=%d",
        stats["parsed"], stats["filtered"], stats["inserted"], stats["updated"], stats["errors"],
    )
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DBPR contractor license loader")
    parser.add_argument("--source", choices=["certified", "registered"], default="certified")
    parser.add_argument("--county", default="hillsborough")
    parser.add_argument("--file", dest="local_file", default=None,
                        help="Use a local file instead of downloading")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and log without DB writes")
    args = parser.parse_args()

    result = run_dbpr_engine(
        source=args.source,
        county=args.county,
        local_file=args.local_file,
        dry_run=args.dry_run,
    )
    print(result)
