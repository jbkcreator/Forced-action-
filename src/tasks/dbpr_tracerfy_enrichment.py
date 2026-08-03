"""
DBPR Contact Enrichment — Tracerfy (contractor phone lookup).

Enriches dbpr_contacts rows (contractor license registry) with phone/email
via Tracerfy's batch trace endpoint, filtered to specific verticals (e.g.
roofing+solar) rather than every pending contact in a county. Reuses the
provider-generic Tracerfy client pieces from src.services.tracerfy_fallback
(_submit_trace_batch/_poll_trace_queue/_parse_trace_row) — that module's own
selection/persist logic is property/owner-shaped and is not reused here.

Guaranteed idempotency: enrichment_status='pending' alone can't distinguish
"never submitted" from "submitted, billed, but the run crashed before the
result was polled and persisted" — a naive re-run would resubmit (and
double-pay for) that batch. This module adds a 'tracerfy_submitted' status,
committed immediately after a successful submission (with the queue_id AND
trace mode stored) and *before* polling starts. Every run resumes any
'tracerfy_submitted' rows first — polling their stored queue_id to
completion, using the stored mode to interpret the result — before
selecting any new work.

Optional address-only fallback (--address-only-fallback / opt-in, default
off — see module-level note below on why): a normal-mode (name+address,
$0.02/hit) miss becomes 'awaiting_address_only' instead of terminal
'failed'. A later run with the flag enabled retries those address-only
($0.04/hit, no name required) before giving up for good:

    pending -> tracerfy_submitted(mode=normal)   -> hit: enriched
                                                  -> miss: awaiting_address_only
    awaiting_address_only -> tracerfy_submitted(mode=advanced) -> hit: enriched
                                                                 -> miss: failed

Off by default because the failure mode address-only fixes on property/
owner data (name-parsing failures from LLCs/"ET AL"/entity strings) barely
applies to DBPR's clean "LAST, FIRST" individual-license-holder format — a
miss here is more likely a genuine no-phone-on-file than a parsing
artifact, so doubling the cost of every miss isn't obviously worth it.
Decide after seeing the normal-pass hit rate.

All reads/writes are raw parameterized SQL via sqlalchemy.text() per this
repo's convention — no ORM query API.

Run:
    python -m src.tasks.dbpr_tracerfy_enrichment
    python -m src.tasks.dbpr_tracerfy_enrichment --vertical roofing,solar --dry-run
    python -m src.tasks.dbpr_tracerfy_enrichment --vertical roofing,solar --county-id pinellas --limit 500
    python -m src.tasks.dbpr_tracerfy_enrichment --address-only-fallback
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.enrichment_log import log_usage
from src.services.skip_trace_ledger import trace_key
from src.services.tracerfy_fallback import _parse_trace_row, _poll_trace_queue, _submit_trace_batch
from src.tasks.dbpr_enrichment import _split_name
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_DEFAULT_LIMIT = 1000
_DEFAULT_VERTICALS = ("roofing", "solar")

_STATUS_PENDING = "pending"
_STATUS_SUBMITTED = "tracerfy_submitted"
_STATUS_ENRICHED = "enriched"
_STATUS_FAILED = "failed"
_STATUS_AWAITING_ADDRESS_ONLY = "awaiting_address_only"

_MODE_NORMAL = "normal"
_MODE_ADVANCED = "advanced"


# ---------------------------------------------------------------------------
# Record building
# ---------------------------------------------------------------------------

def _build_record(contact: dict) -> dict:
    first, last = _split_name(contact["full_name"])
    return {
        "label": str(contact["id"]),  # not echoed back by Tracerfy; kept for readability in logs only
        "address": contact["address"] or "",
        "city": contact["city"] or "",
        "state": contact["state"] or "FL",
        "zip": (contact["zip_code"] or "")[:5],
        "first_name": first,
        "last_name": last,
    }


# ---------------------------------------------------------------------------
# Persist a resolved batch of (contacts submitted, results returned)
# ---------------------------------------------------------------------------

def _name_key(full_name: str) -> tuple[str, str]:
    first, last = _split_name(full_name)
    return (first or "").strip().casefold(), (last or "").strip().casefold()


def _persist_batch_results(db: Session, contacts: list[dict], results: list[dict], mode: str) -> dict:
    """
    Match results back to contacts by building-level trace_key(address, zip)
    PLUS normalized (first, last) name — the queue endpoint does not echo our
    `label`, so this is the most specific join available (mirrors
    tracerfy_fallback.run_tracerfy_fallback's property/owner matching).
    Multiple contractor licenses can legitimately share a mailing address;
    matching on address alone would write one contact's phone/email onto
    every contact at that address. The name component is checked first; if a
    result's address key has exactly one submitted contact (the common case),
    that contact is used regardless of name match. If it has more than one
    and none of their names match the result, the row is left unmatched
    (falls through to the "no matching result" miss handling below) rather
    than guessed at random. Logs cost per contact (one Tracerfy record = one
    submitted contact = one potential billed hit), not per unique key, since
    contacts sharing an address were each submitted as separate records.

    `mode` decides what a miss means: a 'normal' miss is eligible for an
    address-only retry (-> awaiting_address_only); an 'advanced' miss has
    exhausted both trace types (-> failed, terminal).
    """
    now = datetime.now(timezone.utc)
    stats = {"success": 0, "failed": 0}
    miss_status = _STATUS_AWAITING_ADDRESS_ONLY if mode == _MODE_NORMAL else _STATUS_FAILED

    key_map: dict[tuple[str, str, str], list[dict]] = {}
    addr_map: dict[str, list[dict]] = {}
    for c in contacts:
        addr_key = trace_key(c["address"], c["zip_code"])
        if not addr_key:
            continue
        first, last = _name_key(c["full_name"])
        key_map.setdefault((addr_key, first, last), []).append(c)
        addr_map.setdefault(addr_key, []).append(c)

    matched_ids: set[int] = set()

    def _write_contact(contact: dict, match_success: bool, parsed: Optional[dict]) -> None:
        status = _STATUS_ENRICHED if match_success else miss_status
        mobile_phone = parsed.get("mobile_phone") if parsed else None
        landline = parsed.get("landline") if parsed else None
        email = parsed.get("email") if parsed else None
        phone = mobile_phone or (landline if not mobile_phone else None)

        db.execute(text("""
            UPDATE dbpr_contacts
               SET enrichment_status = :status,
                   enrichment_attempted_at = :now,
                   tracerfy_queue_id = NULL,
                   tracerfy_mode = NULL,
                   updated_at = :now,
                   mobile_phone = COALESCE(:mobile_phone, mobile_phone),
                   landline_phone = COALESCE(:landline, landline_phone),
                   phone = COALESCE(:phone, phone),
                   email = COALESCE(:email, email)
             WHERE id = :id
        """), {
            "status": status, "now": now, "id": contact["id"],
            "mobile_phone": mobile_phone, "landline": landline,
            "phone": phone, "email": email,
        })

    for row in results:
        addr_key = trace_key(row.get("address"), row.get("zip"))
        row_first = (row.get("first_name") or "").strip().casefold()
        row_last = (row.get("last_name") or "").strip().casefold()

        targets = key_map.get((addr_key, row_first, row_last))
        if not targets:
            candidates = addr_map.get(addr_key)
            if candidates and len(candidates) == 1:
                targets = candidates
            elif candidates:
                logger.warning(
                    "[DBPRTracerfy] Ambiguous result row: name %r %r matches none of "
                    "%d contacts sharing address-key %r — left unmatched",
                    row.get("first_name"), row.get("last_name"), len(candidates), addr_key,
                )
                continue
            else:
                logger.warning(
                    "[DBPRTracerfy] Unmatched result row (no address-key match): addr=%r zip=%r",
                    row.get("address"), row.get("zip"),
                )
                continue

        parsed = _parse_trace_row(row)
        for contact in targets:
            matched_ids.add(contact["id"])
            log_usage(
                db=db, vendor="tracerfy", purpose="skip_trace",
                success=parsed["match_success"], target_address=addr_key,
            )
            _write_contact(contact, parsed["match_success"], parsed)
            if parsed["match_success"]:
                stats["success"] += 1
            else:
                stats["failed"] += 1

    # Every submitted contact must leave 'tracerfy_submitted' even with no
    # matching result row — treat an unmatched/never-arrived row as a miss,
    # not as permanently stuck state (Tracerfy already billed 0 for a miss).
    for contact in contacts:
        if contact["id"] in matched_ids:
            continue
        log_usage(db=db, vendor="tracerfy", purpose="skip_trace", success=False,
                  target_address=trace_key(contact["address"], contact["zip_code"]))
        _write_contact(contact, False, None)
        stats["failed"] += 1

    db.commit()
    return stats


# ---------------------------------------------------------------------------
# Shared submit -> commit-before-poll -> poll -> persist (the idempotency guarantee)
# ---------------------------------------------------------------------------

def _submit_and_resolve(
    db: Session,
    submit_targets: list[dict],
    records: list[dict],
    mode: str,
    api_key: str,
) -> dict:
    try:
        queue_id, estimated_wait = _submit_trace_batch(records, api_key, address_only=(mode == _MODE_ADVANCED))
    except Exception as e:
        logger.error("[DBPRTracerfy] Submission failed (mode=%s) — no rows billed, all stay as-is: %s", mode, e)
        return {"success": 0, "failed": 0}

    # Commit the submission — status, queue_id, AND mode — BEFORE polling.
    # This is the idempotency guarantee. If the process dies anywhere after
    # this line, the next run's resume stage picks up queue_id + mode and
    # polls it; it will never resubmit these contacts.
    now = datetime.now(timezone.utc)
    ids = [c["id"] for c in submit_targets]
    db.execute(text("""
        UPDATE dbpr_contacts
           SET enrichment_status = :status, tracerfy_queue_id = :queue_id, tracerfy_mode = :mode,
               enrichment_attempted_at = :now, updated_at = :now
         WHERE id = ANY(:ids)
    """), {"status": _STATUS_SUBMITTED, "queue_id": queue_id, "mode": mode, "now": now, "ids": ids})
    db.commit()
    logger.info("[DBPRTracerfy] Submitted queue_id=%s mode=%s for %d contact(s) — billed on hit only",
                queue_id, mode, len(submit_targets))

    try:
        results = _poll_trace_queue(queue_id, api_key, estimated_wait=estimated_wait)
    except Exception as e:
        logger.error(
            "[DBPRTracerfy] Poll failed for queue_id=%s mode=%s — billed submission left 'tracerfy_submitted', "
            "will resume next run: %s", queue_id, mode, e,
        )
        return {"success": 0, "failed": 0}

    return _persist_batch_results(db, submit_targets, results, mode)


# ---------------------------------------------------------------------------
# Stage 1 — resume any submitted-but-unresolved batches (idempotency guarantee)
# ---------------------------------------------------------------------------

def _resume_submitted(api_key: str, dry_run: bool) -> dict:
    stats = {"resumed_contacts": 0, "success": 0, "failed": 0}

    with get_db_context() as db:
        rows = db.execute(text("""
            SELECT id, full_name, address, city, state, zip_code, county_id,
                   tracerfy_queue_id, tracerfy_mode
              FROM dbpr_contacts
             WHERE enrichment_status = :status
               AND tracerfy_queue_id IS NOT NULL
             ORDER BY enrichment_attempted_at ASC
        """), {"status": _STATUS_SUBMITTED}).mappings().all()

        if not rows:
            return stats

        stats["resumed_contacts"] = len(rows)
        if dry_run:
            logger.info(
                "[DBPRTracerfy DRY RUN] %d contact(s) resumable from a prior submission "
                "— would poll their stored queue_id, never resubmit", len(rows),
            )
            return stats

        # A single queue submission is always one mode — group on
        # (queue_id, mode) together so resolution knows which miss-handling
        # (awaiting_address_only vs terminal failed) applies.
        by_queue: dict[tuple[str, str], list[dict]] = {}
        for r in rows:
            key = (r["tracerfy_queue_id"], r["tracerfy_mode"] or _MODE_NORMAL)
            by_queue.setdefault(key, []).append(dict(r))

        for (queue_id, mode), contacts in by_queue.items():
            logger.info("[DBPRTracerfy] Resuming queue_id=%s mode=%s (%d contact(s))", queue_id, mode, len(contacts))
            try:
                results = _poll_trace_queue(queue_id, api_key)
            except Exception as e:
                logger.error("[DBPRTracerfy] Resume poll failed for queue_id=%s: %s — will retry next run", queue_id, e)
                continue
            batch_stats = _persist_batch_results(db, contacts, results, mode)
            stats["success"] += batch_stats["success"]
            stats["failed"] += batch_stats["failed"]

    return stats


# ---------------------------------------------------------------------------
# Stage 2 — normal trace: select fresh 'pending' work (name+address)
# ---------------------------------------------------------------------------

def _run_normal_stage(
    verticals: list[str],
    county_id: Optional[str],
    limit: int,
    dry_run: bool,
    api_key: str,
) -> dict:
    stats = {"total": 0, "skipped": 0, "success": 0, "failed": 0}

    with get_db_context() as db:
        rows = db.execute(text("""
            SELECT id, full_name, address, city, state, zip_code, county_id
              FROM dbpr_contacts
             WHERE enrichment_status = :status
               AND vertical = ANY(:verticals)
               AND (:county_id IS NULL OR county_id = :county_id)
             ORDER BY created_at ASC
             LIMIT :limit
        """), {
            "status": _STATUS_PENDING, "verticals": verticals,
            "county_id": county_id, "limit": limit,
        }).mappings().all()

        if not rows:
            logger.info("[DBPRTracerfy] No pending candidates (verticals=%s county=%s)", verticals, county_id)
            return stats

        stats["total"] = len(rows)

        now = datetime.now(timezone.utc)
        records = []
        submit_targets: list[dict] = []
        for row in rows:
            contact = dict(row)
            first, last = _split_name(contact["full_name"])
            if not last:
                db.execute(text("""
                    UPDATE dbpr_contacts
                       SET enrichment_status = 'skipped', enrichment_attempted_at = :now, updated_at = :now
                     WHERE id = :id
                """), {"now": now, "id": contact["id"]})
                stats["skipped"] += 1
                continue
            records.append(_build_record(contact))
            submit_targets.append(contact)
        db.commit()

        if not records:
            logger.info("[DBPRTracerfy] All %d candidates skipped (no parseable last name)", stats["total"])
            return stats

        if dry_run:
            logger.info(
                "[DBPRTracerfy DRY RUN] Would submit %d record(s) mode=normal (verticals=%s county=%s) — no API call, no billing",
                len(records), verticals, county_id,
            )
            for c in submit_targets[:5]:
                first, last = _split_name(c["full_name"])
                logger.info("[DBPRTracerfy DRY RUN] Would trace: %s %s | %s %s %s",
                            first, last, c["address"], c["city"], c["zip_code"])
            return stats

        batch_stats = _submit_and_resolve(db, submit_targets, records, _MODE_NORMAL, api_key)
        stats["success"] += batch_stats["success"]
        stats["failed"] += batch_stats["failed"]

    return stats


# ---------------------------------------------------------------------------
# Stage 3 — address-only fallback: retry normal-mode misses, address only
# ---------------------------------------------------------------------------

def _run_address_only_stage(
    verticals: list[str],
    county_id: Optional[str],
    limit: int,
    dry_run: bool,
    api_key: str,
) -> dict:
    stats = {"total": 0, "success": 0, "failed": 0}

    with get_db_context() as db:
        rows = db.execute(text("""
            SELECT id, full_name, address, city, state, zip_code, county_id
              FROM dbpr_contacts
             WHERE enrichment_status = :status
               AND vertical = ANY(:verticals)
               AND (:county_id IS NULL OR county_id = :county_id)
               AND address IS NOT NULL AND zip_code IS NOT NULL
             ORDER BY enrichment_attempted_at ASC
             LIMIT :limit
        """), {
            "status": _STATUS_AWAITING_ADDRESS_ONLY, "verticals": verticals,
            "county_id": county_id, "limit": limit,
        }).mappings().all()

        if not rows:
            logger.info("[DBPRTracerfy] No address-only candidates (verticals=%s county=%s)", verticals, county_id)
            return stats

        stats["total"] = len(rows)
        submit_targets = [dict(r) for r in rows]
        records = [_build_record(c) for c in submit_targets]

        if dry_run:
            logger.info(
                "[DBPRTracerfy DRY RUN] Would submit %d record(s) mode=advanced (address-only retry, verticals=%s county=%s) "
                "— no API call, no billing", len(records), verticals, county_id,
            )
            return stats

        batch_stats = _submit_and_resolve(db, submit_targets, records, _MODE_ADVANCED, api_key)
        stats["success"] += batch_stats["success"]
        stats["failed"] += batch_stats["failed"]

    return stats


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_dbpr_tracerfy_enrichment(
    verticals: Optional[list[str]] = None,
    county_id: Optional[str] = None,
    limit: int = _DEFAULT_LIMIT,
    dry_run: bool = False,
    enable_address_only_fallback: bool = False,
) -> dict:
    settings = get_settings()
    verticals = verticals or list(_DEFAULT_VERTICALS)

    results: dict = {
        "verticals": verticals,
        "county_id": county_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "resume": {},
        "normal": {},
        "address_only": {},
        "errors": [],
    }

    if not settings.tracerfy_api_key:
        logger.warning("[DBPRTracerfy] TRACERFY_API_KEY not set — skipping")
        results["errors"].append("TRACERFY_API_KEY not configured")
        return results

    api_key = settings.tracerfy_api_key.get_secret_value()

    try:
        results["resume"] = _resume_submitted(api_key, dry_run)
    except Exception as e:
        logger.error("[DBPRTracerfy] Resume stage crashed: %s", e, exc_info=True)
        results["errors"].append(f"resume: {e}")

    try:
        results["normal"] = _run_normal_stage(verticals, county_id, limit, dry_run, api_key)
    except Exception as e:
        logger.error("[DBPRTracerfy] Normal stage crashed: %s", e, exc_info=True)
        results["errors"].append(f"normal: {e}")

    if enable_address_only_fallback:
        try:
            results["address_only"] = _run_address_only_stage(verticals, county_id, limit, dry_run, api_key)
        except Exception as e:
            logger.error("[DBPRTracerfy] Address-only stage crashed: %s", e, exc_info=True)
            results["errors"].append(f"address_only: {e}")

    results["finished_at"] = datetime.now(timezone.utc).isoformat()
    results["total_enriched"] = (
        results["resume"].get("success", 0)
        + results["normal"].get("success", 0)
        + results["address_only"].get("success", 0)
    )

    logger.info(
        "[DBPRTracerfy] Complete. Enriched=%d | Resumed=%d | Normal=%d | AddressOnly=%d | Errors=%d",
        results["total_enriched"],
        results["resume"].get("resumed_contacts", 0),
        results["normal"].get("total", 0),
        results["address_only"].get("total", 0),
        len(results["errors"]),
    )
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="DBPR contractor contact enrichment via Tracerfy")
    parser.add_argument("--vertical", dest="vertical", default=",".join(_DEFAULT_VERTICALS),
                         help="Comma-separated verticals to target (default: roofing,solar)")
    parser.add_argument("--county-id", dest="county_id", default=None,
                         help="Restrict to one county (default: all counties)")
    parser.add_argument("--limit", type=int, default=_DEFAULT_LIMIT,
                         help=f"Max candidates per stage per run (default: {_DEFAULT_LIMIT})")
    parser.add_argument("--address-only-fallback", action="store_true",
                         help="Also retry normal-mode misses address-only (+$0.04/hit) — opt-in, off by default")
    parser.add_argument("--dry-run", action="store_true",
                         help="Build payloads and log without API calls or DB writes")
    args = parser.parse_args()

    verticals_arg = [v.strip() for v in args.vertical.split(",") if v.strip()]

    try:
        stats = run_dbpr_tracerfy_enrichment(
            verticals=verticals_arg,
            county_id=args.county_id,
            limit=args.limit,
            dry_run=args.dry_run,
            enable_address_only_fallback=args.address_only_fallback,
        )
        print(f"  Resumed (already submitted): {stats['resume'].get('resumed_contacts', 0)}")
        print(f"  Normal-mode candidates      : {stats['normal'].get('total', 0)}")
        if args.address_only_fallback:
            print(f"  Address-only candidates     : {stats['address_only'].get('total', 0)}")
        print(f"  Total enriched              : {stats['total_enriched']}")
        if stats["errors"]:
            print(f"  Errors                      : {stats['errors']}")
        sys.exit(0)
    except Exception as e:
        logger.error("[DBPRTracerfy] Pipeline failed: %s", e)
        sys.exit(1)
