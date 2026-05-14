"""
DBPR Contact Enrichment — BatchData + IDI fallback.

Enriches DBPRContact rows (contractor license registry) with phone and email
using the same two-stage pipeline as the owner enrichment job, but targeting
the dbpr_contacts table directly via person-name + address lookup. Where a
contact has a full address + ZIP, the full address is included in the payload
for accuracy. Where address is missing, falls back to name + state only.

Stage 1 (BatchData): targets enrichment_status='pending'
Stage 2 (IDI):       targets enrichment_status='failed' (BatchData miss)

On match:   email + phone written, enrichment_status -> 'enriched'
On no-match: enrichment_status -> 'failed', enrichment_attempted_at set

Run:
    python -m src.tasks.dbpr_enrichment
    python -m src.tasks.dbpr_enrichment --county-id pinellas --limit 100
    python -m src.tasks.dbpr_enrichment --dry-run
"""

import time
import logging
from datetime import datetime, timezone
from typing import Optional

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import DBPRContact
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_DEFAULT_BATCHDATA_LIMIT = 200
_DEFAULT_IDI_LIMIT       = 100
_BATCH_SIZE              = 100   # BatchData max per request
_IDI_BATCH_SIZE          = 50    # IDI recommended
_DELAY_BETWEEN_BATCHES   = 1.0   # seconds


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------

def _split_name(full_name: str) -> tuple[str, str]:
    """
    Split DBPR full_name (LAST, FIRST format) into (first, last).
    Falls back to ("", full_name) for single-token names.
    """
    name = (full_name or "").strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        return parts[1], parts[0]   # LAST, FIRST -> first, last
    parts = name.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return "", name


# ---------------------------------------------------------------------------
# Stage 1 — BatchData
# ---------------------------------------------------------------------------

def _run_batchdata_stage(
    county_id: str,
    limit: int,
    dry_run: bool,
    api_key: str,
) -> dict:
    from src.services.skip_trace import _call_batch_data, _parse_result
    from src.services.enrichment_log import log_usage as _log_enrichment

    stats = {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    with get_db_context() as db:
        rows = (
            db.query(DBPRContact)
            .filter(
                DBPRContact.county_id == county_id,
                DBPRContact.enrichment_status == "pending",
            )
            .order_by(DBPRContact.created_at.asc())
            .limit(limit)
            .all()
        )

    if not rows:
        logger.info("[DBPREnrich] No pending contacts for BatchData (county=%s)", county_id)
        return stats

    stats["total"] = len(rows)
    logger.info("[DBPREnrich] BatchData: %d candidates (county=%s)", len(rows), county_id)

    if dry_run:
        for r in rows[:5]:
            first, last = _split_name(r.full_name)
            logger.info("[DBPREnrich DRY RUN] Would trace: %s %s | %s %s %s",
                        first, last, r.address, r.city, r.zip_code)
        logger.info("[DBPREnrich DRY RUN] Would process %d records — no API calls", len(rows))
        return stats

    now = datetime.now(timezone.utc)

    for batch_start in range(0, len(rows), _BATCH_SIZE):
        batch = rows[batch_start: batch_start + _BATCH_SIZE]
        batch_num = batch_start // _BATCH_SIZE + 1

        payloads = []
        index_map: list[DBPRContact] = []

        for contact in batch:
            first, last = _split_name(contact.full_name)
            if not last:
                stats["skipped"] += 1
                _mark(contact.id, "skipped", now)
                continue

            payload: dict = {"firstName": first, "lastName": last}
            if contact.address and contact.zip_code:
                payload["address"] = {
                    "street": contact.address,
                    "city":   contact.city or "",
                    "state":  contact.state or "FL",
                    "zip":    contact.zip_code[:5],
                }
            else:
                # No address — state-only still narrows the pool vs name-only
                payload["address"] = {"state": contact.state or "FL"}

            payloads.append(payload)
            index_map.append(contact)

        if not payloads:
            continue

        logger.info("[DBPREnrich] BatchData batch %d: %d records", batch_num, len(payloads))

        try:
            results = _call_batch_data(payloads, api_key)
        except RuntimeError as e:
            err_msg = str(e)
            logger.error("[DBPREnrich] BatchData batch %d failed: %s", batch_num, err_msg)
            if "401" in err_msg or "402" in err_msg:
                from src.services.email import send_alert
                send_alert(
                    subject="[Forced Action] DBPR enrichment: BatchData credential error",
                    body=(
                        f"DBPR enrichment halted: {err_msg}\n\n"
                        f"Check BATCH_SKIP_TRACING_API_KEY and BatchData credit balance."
                    ),
                )
                break
            for c in index_map:
                _mark(c.id, "failed", now)
                stats["failed"] += len(index_map)
            continue

        # Persist results
        with get_db_context() as db:
            for i, result in enumerate(results):
                if i >= len(index_map):
                    break
                contact_snap = index_map[i]
                try:
                    parsed = _parse_result(result)
                    contact = db.get(DBPRContact, contact_snap.id)
                    if contact is None:
                        continue

                    _log_enrichment(
                        db=db,
                        vendor="batchdata",
                        purpose="dbpr_enrichment",
                        success=parsed["match_success"],
                    )

                    if parsed["match_success"]:
                        if parsed.get("mobile_phone"):
                            contact.phone = parsed["mobile_phone"]
                        elif parsed.get("landline"):
                            contact.phone = parsed["landline"]
                        if parsed.get("email"):
                            contact.email = parsed["email"]
                        contact.enrichment_status = "enriched"
                        stats["success"] += 1
                    else:
                        contact.enrichment_status = "failed"
                        stats["failed"] += 1

                    contact.enrichment_attempted_at = now
                    contact.updated_at = now
                    db.add(contact)

                except Exception as e:
                    logger.error("[DBPREnrich] Persist error for contact_id=%d: %s",
                                 contact_snap.id, e)
                    stats["failed"] += 1

        if batch_start + _BATCH_SIZE < len(rows):
            time.sleep(_DELAY_BETWEEN_BATCHES)

    logger.info("[DBPREnrich] BatchData done: success=%d failed=%d skipped=%d / total=%d",
                stats["success"], stats["failed"], stats["skipped"], stats["total"])
    return stats


# ---------------------------------------------------------------------------
# Stage 2 — IDI fallback
# ---------------------------------------------------------------------------

def _run_idi_stage(
    county_id: str,
    limit: int,
    dry_run: bool,
    api_key: str,
) -> dict:
    from src.services.idi_fallback import _call_idi, _parse_idi_result
    from src.services.enrichment_log import log_usage as _log_enrichment

    stats = {"total": 0, "success": 0, "failed": 0}

    with get_db_context() as db:
        rows = (
            db.query(DBPRContact)
            .filter(
                DBPRContact.county_id == county_id,
                DBPRContact.enrichment_status == "failed",
                DBPRContact.address.isnot(None),
                DBPRContact.zip_code.isnot(None),
            )
            .order_by(DBPRContact.enrichment_attempted_at.asc())
            .limit(limit)
            .all()
        )

    if not rows:
        logger.info("[DBPREnrich] No IDI candidates (county=%s)", county_id)
        return stats

    stats["total"] = len(rows)
    logger.info("[DBPREnrich] IDI: %d candidates (county=%s)", len(rows), county_id)

    if dry_run:
        logger.info("[DBPREnrich DRY RUN] Would IDI-enrich %d contacts — no API calls", len(rows))
        return stats

    now = datetime.now(timezone.utc)

    for batch_start in range(0, len(rows), _IDI_BATCH_SIZE):
        batch = rows[batch_start: batch_start + _IDI_BATCH_SIZE]
        batch_num = batch_start // _IDI_BATCH_SIZE + 1

        searches = []
        index_map: list[DBPRContact] = []

        for contact in batch:
            first, last = _split_name(contact.full_name)
            search: dict = {"firstName": first, "lastName": last}
            if contact.address and contact.zip_code:
                search["address"] = {
                    "street": contact.address,
                    "city":   contact.city or "",
                    "state":  contact.state or "FL",
                    "zip":    (contact.zip_code or "")[:5],
                }
            else:
                search["address"] = {"state": contact.state or "FL"}
            searches.append(search)
            index_map.append(contact)

        logger.info("[DBPREnrich] IDI batch %d: %d records", batch_num, len(searches))

        try:
            results = _call_idi(searches, api_key)
        except RuntimeError as e:
            logger.error("[DBPREnrich] IDI batch %d failed: %s", batch_num, e)
            if "401" in str(e) or "402" in str(e):
                break
            stats["failed"] += len(searches)
            continue

        with get_db_context() as db:
            for i, result in enumerate(results):
                if i >= len(index_map):
                    break
                contact_snap = index_map[i]
                try:
                    parsed = _parse_idi_result(result)
                    contact = db.get(DBPRContact, contact_snap.id)
                    if contact is None:
                        continue

                    _log_enrichment(
                        db=db,
                        vendor="idi",
                        purpose="dbpr_enrichment",
                        success=parsed["match_success"],
                    )

                    if parsed["match_success"]:
                        if parsed.get("mobile_phone"):
                            contact.phone = parsed["mobile_phone"]
                        elif parsed.get("landline"):
                            contact.phone = parsed["landline"]
                        if parsed.get("email"):
                            contact.email = parsed["email"]
                        contact.enrichment_status = "enriched"
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1

                    contact.enrichment_attempted_at = now
                    contact.updated_at = now
                    db.add(contact)

                except Exception as e:
                    logger.error("[DBPREnrich] IDI persist error for contact_id=%d: %s",
                                 contact_snap.id, e)
                    stats["failed"] += 1

        if batch_start + _IDI_BATCH_SIZE < len(rows):
            time.sleep(2.0)

    logger.info("[DBPREnrich] IDI done: success=%d failed=%d / total=%d",
                stats["success"], stats["failed"], stats["total"])
    return stats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mark(contact_id: int, status: str, now: datetime) -> None:
    """Update enrichment_status in a short-lived session."""
    with get_db_context() as db:
        contact = db.get(DBPRContact, contact_id)
        if contact:
            contact.enrichment_status = status
            contact.enrichment_attempted_at = now
            contact.updated_at = now
            db.add(contact)


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_dbpr_enrichment(
    county_id: str = "hillsborough",
    batchdata_limit: int = _DEFAULT_BATCHDATA_LIMIT,
    idi_limit: int = _DEFAULT_IDI_LIMIT,
    skip_idi: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Two-stage enrichment for DBPR contractor contacts.

    Returns combined stats dict.
    """
    settings = get_settings()

    results: dict = {
        "county_id":     county_id,
        "started_at":    datetime.now(timezone.utc).isoformat(),
        "batchdata":     {},
        "idi":           {},
        "errors":        [],
        "total_enriched": 0,
    }

    # ── Stage 1: BatchData ────────────────────────────────────────────────────
    if not settings.batch_skip_tracing_api_key:
        logger.warning("[DBPREnrich] BATCH_SKIP_TRACING_API_KEY not set — BatchData skipped")
        results["batchdata"] = {"skipped": True, "reason": "API key not configured"}
    else:
        logger.info("[DBPREnrich] Stage 1: BatchData (limit=%d, county=%s)", batchdata_limit, county_id)
        try:
            bd_stats = _run_batchdata_stage(
                county_id=county_id,
                limit=batchdata_limit,
                dry_run=dry_run,
                api_key=settings.batch_skip_tracing_api_key.get_secret_value(),
            )
            results["batchdata"] = bd_stats
        except Exception as e:
            logger.error("[DBPREnrich] BatchData stage crashed: %s", e, exc_info=True)
            results["errors"].append(f"batchdata: {e}")

    # ── Stage 2: IDI fallback ─────────────────────────────────────────────────
    if skip_idi:
        results["idi"] = {"skipped": True}
    elif not settings.idi_api_key:
        logger.info("[DBPREnrich] IDI_API_KEY not set — IDI stage skipped")
        results["idi"] = {"skipped": True, "reason": "API key not configured"}
    else:
        logger.info("[DBPREnrich] Stage 2: IDI fallback (limit=%d)", idi_limit)
        try:
            idi_stats = _run_idi_stage(
                county_id=county_id,
                limit=idi_limit,
                dry_run=dry_run,
                api_key=settings.idi_api_key.get_secret_value(),
            )
            results["idi"] = idi_stats
        except Exception as e:
            logger.error("[DBPREnrich] IDI stage crashed: %s", e, exc_info=True)
            results["errors"].append(f"idi: {e}")

    results["finished_at"] = datetime.now(timezone.utc).isoformat()
    results["total_enriched"] = (
        results["batchdata"].get("success", 0)
        + results["idi"].get("success", 0)
    )

    logger.info(
        "[DBPREnrich] Complete. Enriched=%d | BatchData=%d | IDI=%d | Errors=%d",
        results["total_enriched"],
        results["batchdata"].get("success", 0),
        results["idi"].get("success", 0),
        len(results["errors"]),
    )
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="DBPR contractor contact enrichment")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--limit", type=int, default=_DEFAULT_BATCHDATA_LIMIT,
                        help="Max contacts for BatchData stage (default: 200)")
    parser.add_argument("--idi-limit", type=int, default=_DEFAULT_IDI_LIMIT,
                        help="Max BatchData misses for IDI stage (default: 100)")
    parser.add_argument("--skip-idi", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build payloads and log without API calls or DB writes")
    args = parser.parse_args()

    try:
        stats = run_dbpr_enrichment(
            county_id=args.county_id,
            batchdata_limit=args.limit,
            idi_limit=args.idi_limit,
            skip_idi=args.skip_idi,
            dry_run=args.dry_run,
        )
        print(f"  BatchData enriched : {stats['batchdata'].get('success', 0)}")
        print(f"  IDI enriched       : {stats['idi'].get('success', 0)}")
        print(f"  Total enriched     : {stats['total_enriched']}")
        if stats["errors"]:
            print(f"  Errors             : {stats['errors']}")
        sys.exit(0)
    except Exception as e:
        logger.error("[DBPREnrich] Pipeline failed: %s", e)
        sys.exit(1)
