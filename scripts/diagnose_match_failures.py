"""
Match-failure diagnostic script — §5.5
Pulls unmatched Pinellas deed and probate records, finds the closest property
table candidate for each, and writes side-by-side comparison CSVs.

Outputs (relative to repo root):
  reports/audit/pinellas_deeds_10_sidebyside.csv
  reports/audit/pinellas_probate_10_sidebyside.csv
  reports/audit/pinellas_deeds_50_sidebyside.csv
  reports/audit/pinellas_probate_50_sidebyside.csv

Usage:
  cd Forced-action-
  python scripts/diagnose_match_failures.py
"""

import os
import sys
import json
import csv
import logging
from pathlib import Path

# Make project root importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text
from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

COUNTY_ID = "pinellas"
OUT_DIR = ROOT / "reports" / "audit"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Raw field definitions (what we expect in unmatched_records.raw_data)
# ─────────────────────────────────────────────────────────────────────────────

DEED_RAW_FIELDS = [
    "Instrument", "Grantor", "Grantee", "RecordDate", "SalesPrice",
    "DocType", "BookType", "BookNum", "PageNum", "Legal",
]

PROBATE_RAW_FIELDS = [
    "CaseNumber", "FilingDate", "FirstName", "MiddleName",
    "LastName/CompanyName", "PartyType", "PartyAddress",
    "Title", "CaseTypeDescription",
]

# Property + owner fields surfaced on the candidate side
PROP_FIELDS = [
    "prop_parcel_id", "prop_address", "prop_city", "prop_zip",
    "prop_legal_description", "prop_owner_name",
    "cand_match_method", "cand_similarity_score",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_engine():
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True)


def _pull_unmatched(conn, source_type: str, limit: int) -> list[dict]:
    """Fetch N unmatched records for the given source_type + county."""
    rows = conn.execute(
        text("""
            SELECT id, raw_data, instrument_number, grantor, address_string
            FROM unmatched_records
            WHERE source_type = :src
              AND county_id   = :county
              AND match_status = 'unmatched'
            ORDER BY date_added DESC
            LIMIT :lim
        """),
        {"src": source_type, "county": COUNTY_ID, "lim": limit},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def _best_candidate_by_address(conn, addr: str) -> dict | None:
    """Top-1 property by pg_trgm address similarity."""
    if not addr or not addr.strip():
        return None
    try:
        row = conn.execute(
            text("""
                SELECT p.parcel_id, p.address, p.city, p.zip,
                       p.legal_description,
                       o.owner_name,
                       similarity(p.address, :addr) AS sim
                FROM properties p
                LEFT JOIN owners o ON o.property_id = p.id
                WHERE p.county_id = :county
                  AND p.address IS NOT NULL
                  AND similarity(p.address, :addr) > 0.1
                ORDER BY sim DESC
                LIMIT 1
            """),
            {"addr": addr, "county": COUNTY_ID},
        ).fetchone()
        return dict(row._mapping) if row else None
    except Exception as e:
        log.warning("Address trgm query failed: %s", e)
        return None


def _best_candidate_by_name(conn, name: str) -> dict | None:
    """Top-1 property by pg_trgm owner_name similarity."""
    if not name or not name.strip():
        return None
    # Normalize: strip legal suffixes, upper
    import re
    n = name.upper().strip()
    for suf in ("LLC", "INC", "CORP", "TRUST", "ESTATE", "TRUSTEE"):
        n = re.sub(rf"\b{suf}\b\.?", "", n)
    n = re.sub(r"[^\w\s]", " ", n).strip()
    if not n:
        return None
    try:
        row = conn.execute(
            text("""
                SELECT p.parcel_id, p.address, p.city, p.zip,
                       p.legal_description,
                       o.owner_name,
                       similarity(o.owner_name, :nm) AS sim
                FROM owners o
                JOIN properties p ON p.id = o.property_id
                WHERE p.county_id = :county
                  AND o.owner_name IS NOT NULL
                  AND similarity(o.owner_name, :nm) > 0.1
                ORDER BY sim DESC
                LIMIT 1
            """),
            {"nm": n, "county": COUNTY_ID},
        ).fetchone()
        return dict(row._mapping) if row else None
    except Exception as e:
        log.warning("Name trgm query failed: %s", e)
        return None


def _best_candidate_by_legal(conn, legal: str) -> dict | None:
    """Top-1 property by ILIKE on subdivision word tokens in legal_description."""
    if not legal or not legal.strip():
        return None
    import re
    legal_up = legal.upper()
    lot_m = re.search(r"\bLOT\s+(\d+\w*)\b", legal_up)
    blk_m = re.search(r"\bB(?:LOCK|LK)\s+(\d+\w*)\b", legal_up)
    parts = re.split(r"\b(?:LOT|BLK|BLOCK|SEC|SECTION|UNIT|TRACT)\b", legal_up)
    subd_words = [w for w in parts[0].split() if len(w) > 3][:3]

    if not lot_m and not subd_words:
        return None

    clauses = ["p.county_id = :county", "p.legal_description IS NOT NULL"]
    params: dict = {"county": COUNTY_ID}

    if lot_m:
        clauses.append(f"p.legal_description ~* '\\mLOT {lot_m.group(1)}\\M'")
    if blk_m:
        clauses.append(f"p.legal_description ~* '\\mB(LOCK|LK) {blk_m.group(1)}\\M'")
    for i, word in enumerate(subd_words):
        clauses.append(f"p.legal_description ILIKE :w{i}")
        params[f"w{i}"] = f"%{word}%"

    sql = f"""
        SELECT p.parcel_id, p.address, p.city, p.zip,
               p.legal_description,
               o.owner_name,
               1.0 AS sim
        FROM properties p
        LEFT JOIN owners o ON o.property_id = p.id
        WHERE {' AND '.join(clauses)}
        LIMIT 1
    """
    try:
        row = conn.execute(text(sql), params).fetchone()
        return dict(row._mapping) if row else None
    except Exception as e:
        log.warning("Legal desc query failed: %s", e)
        return None


def _find_best_candidate(conn, rec: dict, source_type: str) -> tuple[dict | None, str]:
    """
    Waterfall: address → owner_name → legal_description.
    Returns (candidate_dict_or_None, method_used).
    """
    raw: dict = rec.get("raw_data") or {}

    if source_type == "deeds":
        # 1. Legal description
        legal = raw.get("Legal") or ""
        cand = _best_candidate_by_legal(conn, legal)
        if cand:
            return cand, "legal_desc"
        # 2. Grantor name
        grantor = rec.get("grantor") or raw.get("Grantor") or ""
        cand = _best_candidate_by_name(conn, grantor)
        if cand:
            return cand, "grantor_name"
        # 3. Grantee name
        grantee = raw.get("Grantee") or ""
        cand = _best_candidate_by_name(conn, grantee)
        if cand:
            return cand, "grantee_name"

    elif source_type == "probate":
        # 1. Party address
        addr = rec.get("address_string") or raw.get("PartyAddress") or ""
        cand = _best_candidate_by_address(conn, addr)
        if cand:
            return cand, "address"
        # 2. Decedent name
        first = raw.get("FirstName", "") or ""
        mid = raw.get("MiddleName", "") or ""
        last = raw.get("LastName/CompanyName", "") or ""
        full = " ".join(p.strip() for p in [first, mid, last] if p.strip())
        cand = _best_candidate_by_name(conn, full)
        if cand:
            return cand, "owner_name"
        # 3. Last+first only
        if first and last:
            cand = _best_candidate_by_name(conn, f"{first} {last}")
            if cand:
                return cand, "owner_name_short"

    return None, "none"


# ─────────────────────────────────────────────────────────────────────────────
# Row builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_deed_row(rec: dict, cand: dict | None, method: str) -> dict:
    raw = rec.get("raw_data") or {}
    row = {}
    for f in DEED_RAW_FIELDS:
        row[f"raw_{f}"] = raw.get(f, "")
    if cand:
        row["prop_parcel_id"]        = cand.get("parcel_id", "")
        row["prop_address"]          = cand.get("address", "")
        row["prop_city"]             = cand.get("city", "")
        row["prop_zip"]              = cand.get("zip", "")
        row["prop_legal_description"]= (cand.get("legal_description") or "")[:120]
        row["prop_owner_name"]       = cand.get("owner_name", "")
        row["cand_match_method"]     = method
        row["cand_similarity_score"] = round(float(cand.get("sim") or 0), 3)
    else:
        for f in PROP_FIELDS:
            row[f] = ""
    return row


def _build_probate_row(rec: dict, cand: dict | None, method: str) -> dict:
    raw = rec.get("raw_data") or {}
    row = {}
    for f in PROBATE_RAW_FIELDS:
        row[f"raw_{f}"] = raw.get(f, "")
    if cand:
        row["prop_parcel_id"]        = cand.get("parcel_id", "")
        row["prop_address"]          = cand.get("address", "")
        row["prop_city"]             = cand.get("city", "")
        row["prop_zip"]              = cand.get("zip", "")
        row["prop_legal_description"]= (cand.get("legal_description") or "")[:120]
        row["prop_owner_name"]       = cand.get("owner_name", "")
        row["cand_match_method"]     = method
        row["cand_similarity_score"] = round(float(cand.get("sim") or 0), 3)
    else:
        for f in PROP_FIELDS:
            row[f] = ""
    return row


def _write_csv(path: Path, rows: list[dict]):
    if not rows:
        log.warning("No rows to write for %s", path.name)
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    log.info("Wrote %d rows → %s", len(rows), path)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run():
    engine = _get_engine()

    for limit, label in [(10, "10"), (50, "50")]:
        with engine.connect() as conn:

            # ── DEEDS ──────────────────────────────────────────────────────
            log.info("Pulling %s unmatched Pinellas DEED records …", limit)
            deed_recs = _pull_unmatched(conn, "deeds", limit)
            log.info("  → %d records returned", len(deed_recs))

            deed_rows = []
            for rec in deed_recs:
                cand, method = _find_best_candidate(conn, rec, "deeds")
                deed_rows.append(_build_deed_row(rec, cand, method))

            _write_csv(
                OUT_DIR / f"pinellas_deeds_{label}_sidebyside.csv",
                deed_rows,
            )

            # ── PROBATE ────────────────────────────────────────────────────
            log.info("Pulling %s unmatched Pinellas PROBATE records …", limit)
            prob_recs = _pull_unmatched(conn, "probate", limit)
            log.info("  → %d records returned", len(prob_recs))

            prob_rows = []
            for rec in prob_recs:
                cand, method = _find_best_candidate(conn, rec, "probate")
                prob_rows.append(_build_probate_row(rec, cand, method))

            _write_csv(
                OUT_DIR / f"pinellas_probate_{label}_sidebyside.csv",
                prob_rows,
            )

    log.info("Done. CSVs written to %s", OUT_DIR)


if __name__ == "__main__":
    run()
