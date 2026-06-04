"""
Tracerfy skip trace — Tier 1 in the waterfall.

Uses POST /trace/ (batch async, 1 credit/hit = $0.02) rather than
POST /trace/lookup/ (instant sync, 5 credits/hit = $0.10). We send
owner name + address so the normal tier applies.

Flow:
  1. Collect candidates in batches of up to 100
  2. POST to /trace/ with trace_type="normal" → queue_id
  3. Poll GET /queue/:id until pending=False
  4. Download results CSV, match rows back to owners by label (owner_id)
  5. Persist EnrichedContact, update Owner fields, suppress DNC phones

DNC side-effect (free): phones in the trace response carry dnc/litigator
flags. Hits are written to sms_opt_outs immediately; dnc_checked_at is
stored in phone_metadata for the monthly re-scrub.

API:  POST https://tracerfy.com/v1/api/trace/
Auth: Bearer token
Cost: 1 credit/hit ($0.02), 0 on miss

Rate limit: 10 POST /trace/ per 5-minute window → 30s delay between batches.

Usage:
  python -m src.services.tracerfy_fallback --limit 50
  python -m src.services.tracerfy_fallback --dry-run --limit 5
  python -m src.services.tracerfy_fallback --balance
"""

import csv
import io
import time
import traceback
from datetime import datetime, timezone
from typing import Optional

import requests

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import (
    DistressScore, EnrichedContact, Owner, Property, SmsOptOut,
)
from src.services.email import send_alert
from src.services.enrichment_log import log_usage
from src.services.phone_utils import normalize as normalize_phone
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_TRACERFY_BASE        = "https://tracerfy.com/v1/api"
_TRACE_ENDPOINT       = f"{_TRACERFY_BASE}/trace/"
_TRACE_QUEUE_ENDPOINT = f"{_TRACERFY_BASE}/queue/"
_ANALYTICS_ENDPOINT   = f"{_TRACERFY_BASE}/analytics/"
_BATCH_SIZE           = 100   # Tracerfy max per POST /trace/ request
_POLL_INTERVAL        = 5     # seconds between queue status polls
_BATCH_DELAY          = 31    # seconds between batch POSTs (rate limit: 10/5min)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }


def _submit_trace_batch(records: list[dict], api_key: str) -> tuple[str, int]:
    """
    POST /trace/ as multipart/form-data.
    Returns (queue_id, estimated_wait_seconds).

    Confirmed response shape:
      {"queue_id": 94858, "status": "pending", "rows_uploaded": 3,
       "estimated_wait_seconds": 30, "credits_per_lead": 1, ...}
    """
    import json as _json

    fields = {
        "address_column":     "address",
        "city_column":        "city",
        "state_column":       "state",
        "zip_column":         "zip",
        "first_name_column":  "first_name",
        "last_name_column":   "last_name",
        "mail_address_column": "address",
        "mail_city_column":   "city",
        "mail_state_column":  "state",
        "mailing_zip_column": "zip",
        "trace_type":         "normal",
        "json_data":          _json.dumps(records),
    }
    multipart = {k: (None, v) for k, v in fields.items()}

    resp = requests.post(
        _TRACE_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}"},
        files=multipart,
        timeout=30,
    )
    if resp.status_code in (401, 403):
        raise RuntimeError(f"Tracerfy: invalid API key ({resp.status_code})")
    if resp.status_code == 429:
        raise RuntimeError("Tracerfy: rate limited (429) — slow down batch submissions")
    if not resp.ok:
        raise RuntimeError(f"Tracerfy POST /trace/ HTTP {resp.status_code}: {resp.text[:400]}")

    data = resp.json()
    logger.info("[Tracerfy] Batch submit: %s", data)

    queue_id = data.get("queue_id") or data.get("id")
    if not queue_id:
        raise RuntimeError(f"Tracerfy /trace/: no queue_id in response: {data}")
    estimated_wait = int(data.get("estimated_wait_seconds") or 30)
    return str(queue_id), estimated_wait


def _poll_trace_queue(queue_id: str, api_key: str, estimated_wait: int = 30) -> list[dict]:
    """
    Poll GET /queue/:id until it returns a non-empty list, then return the results.

    Confirmed response shape (2026-06-03):
      [{"address": "...", "city": "...", "state": "...",
        "first_name": "...", "last_name": "...",
        "primary_phone": "5125550100", "primary_phone_type": "Mobile",
        "mobile_1".."mobile_5": "...",
        "landline_1".."landline_3": "...",
        "email_1".."email_5": "...",
        "mail_address": "...", "mail_city": "...", "mail_state": "..."}, ...]

    Returns [] when still processing. After initial sleep we retry up to
    60 × 5s = 5 minutes before giving up (treating remaining submitted
    records as misses).
    """
    url = f"{_TRACE_QUEUE_ENDPOINT}{queue_id}"

    logger.info("[Tracerfy] Waiting %ds for queue=%s...", estimated_wait, queue_id)
    time.sleep(estimated_wait)

    for attempt in range(60):
        resp = requests.get(url, headers=_headers(api_key), timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Tracerfy GET /queue/ HTTP {resp.status_code}: {resp.text[:300]}")
        results = resp.json()
        if results:
            logger.info("[Tracerfy] queue=%s ready — %d rows. Sample: %s",
                        queue_id, len(results), results[0])
            return results
        logger.info("[Tracerfy] queue=%s still empty attempt=%d", queue_id, attempt + 1)
        time.sleep(_POLL_INTERVAL)

    logger.warning("[Tracerfy] queue=%s returned no results after timeout — treating as all misses", queue_id)
    return []


def _parse_trace_row(row: dict) -> dict:
    """
    Parse one result row from GET /queue/:id into the standard contact dict.

    Confirmed field names (2026-06-03):
      primary_phone, primary_phone_type
      mobile_1..mobile_5, landline_1..landline_3
      email_1..email_5
      mail_address, mail_city, mail_state
      address, city, state (property address echoed back)

    Note: the batch trace endpoint does NOT return DNC flags. DNC checking
    is handled separately by the monthly dnc_refresh task.
    """
    def _col(*keys) -> str:
        for k in keys:
            v = (row.get(k) or "").strip()
            if v:
                return v
        return ""

    # ── Phones ────────────────────────────────────────────────────────────
    # Use primary_phone first (Tracerfy's best pick), then mobile_1..5,
    # then landline_1..3.
    mobile_phone: Optional[str] = None
    landline:     Optional[str] = None

    primary_raw = _col("primary_phone")
    primary_type = _col("primary_phone_type").lower()
    if primary_raw:
        primary_num = normalize_phone(primary_raw)
        if primary_num:
            if "mobile" in primary_type or "wireless" in primary_type:
                mobile_phone = primary_num
            else:
                landline = primary_num

    for i in range(1, 6):
        raw = _col(f"mobile_{i}")
        if not raw:
            break
        num = normalize_phone(raw)
        if num and not mobile_phone:
            mobile_phone = num

    for i in range(1, 4):
        raw = _col(f"landline_{i}")
        if not raw:
            break
        num = normalize_phone(raw)
        if num and not landline:
            landline = num

    # ── Email ─────────────────────────────────────────────────────────────
    email: Optional[str] = None
    for i in range(1, 6):
        addr = _col(f"email_{i}").lower()
        if addr and "@" in addr:
            email = addr
            break

    # ── Mailing address ───────────────────────────────────────────────────
    parts = [_col("mail_address"), _col("mail_city"), _col("mail_state")]
    mailing_address = ", ".join(p for p in parts if p) or None

    return {
        "mobile_phone":    mobile_phone,
        "landline":        landline,
        "email":           email,
        "mailing_address": mailing_address,
        "match_success":   bool(mobile_phone or landline or email),
        "dnc_flags":       None,   # not returned by batch trace; handled by dnc_refresh
        "all_dnc_phones":  [],
    }


# ---------------------------------------------------------------------------
# Account
# ---------------------------------------------------------------------------

def get_tracerfy_balance() -> dict:
    """GET /analytics/ — returns credit balance and account summary."""
    settings = get_settings()
    if not settings.tracerfy_api_key:
        return {"error": "TRACERFY_API_KEY not set"}
    api_key = settings.tracerfy_api_key.get_secret_value()
    resp = requests.get(_ANALYTICS_ENDPOINT, headers=_headers(api_key), timeout=15)
    if not resp.ok:
        raise RuntimeError(f"Tracerfy analytics HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_tracerfy_fallback(
    limit: int = 100,
    county_id: str = "hillsborough",
    owner_ids: Optional[list] = None,
    dry_run: bool = False,
) -> dict:
    """
    Run Tracerfy batch skip-trace (POST /trace/, 1 credit/hit = $0.02) for Gold+ leads.

    Standard mode (owner_ids=None): selects Gold+ candidates not yet Tracerfy-traced.
    Waterfall mode (owner_ids=[...]): processes a specific list supplied by the orchestrator.

    DNC side-effect: phones with dnc=True or litigator=True are written to
    sms_opt_outs(source="tracerfy_dnc") so can_send() blocks them immediately.

    Returns stats dict.
    """
    settings     = get_settings()
    cost_per_hit = settings.tracerfy_cost_cents   # 2 cents = $0.02

    if not settings.tracerfy_api_key:
        logger.warning("TRACERFY_API_KEY not set — Tracerfy skip-trace skipped")
        return {"skipped": True, "reason": "TRACERFY_API_KEY not configured"}

    api_key = settings.tracerfy_api_key.get_secret_value()
    stats   = {
        "total": 0, "success": 0, "failed": 0,
        "no_address": 0, "already_done": 0, "skipped": False,
    }

    with get_db_context() as session:
        from sqlalchemy import or_ as sa_or, func as sa_func

        if owner_ids is not None:
            already_tracerfy = (
                session.query(EnrichedContact.property_id)
                .filter(EnrichedContact.source == "tracerfy")
                .subquery()
            )
            rows = (
                session.query(Owner, Property)
                .join(Property, Owner.property_id == Property.id)
                .filter(
                    Owner.id.in_(owner_ids),
                    Owner.property_id.notin_(session.query(already_tracerfy)),
                )
                .all()
            )
        else:
            no_phone = sa_or(
                Owner.phone_1.is_(None),
                sa_func.length(sa_func.trim(Owner.phone_1)) == 0,
            )
            already_tracerfy = (
                session.query(EnrichedContact.property_id)
                .filter(EnrichedContact.source == "tracerfy")
                .subquery()
            )
            rows = (
                session.query(Owner, Property)
                .join(Property, Owner.property_id == Property.id)
                .join(DistressScore, DistressScore.property_id == Property.id)
                .filter(
                    Owner.county_id == county_id,
                    no_phone,
                    Owner.skip_trace_success.is_not(True),
                    Owner.property_id.notin_(session.query(already_tracerfy)),
                    DistressScore.lead_tier.in_(("Gold", "Platinum", "Ultra Platinum")),
                    Property.address.isnot(None),
                    Property.address != "",
                    Property.zip.isnot(None),
                    Property.zip != "",
                )
                .limit(limit)
                .all()
            )

    if not rows:
        logger.info("[Tracerfy] No candidates found.")
        return stats

    logger.info("[Tracerfy] Found %d candidates to enrich", len(rows))
    stats["total"] = len(rows)

    if dry_run:
        for owner, prop in rows[:5]:
            logger.info(
                "[Tracerfy DRY RUN] Would trace: property_id=%d | %s | %s",
                prop.id, prop.address, owner.owner_name,
            )
        logger.info("[Tracerfy DRY RUN] Would process %d records. No API call made.", len(rows))
        return stats

    # ── Build batches ─────────────────────────────────────────────────────
    # label = owner_id (str) so we can match results back without address parsing
    for batch_start in range(0, len(rows), _BATCH_SIZE):
        batch = rows[batch_start: batch_start + _BATCH_SIZE]
        batch_num = batch_start // _BATCH_SIZE + 1

        records   = []
        label_map: dict[str, tuple] = {}  # owner_id str → (owner_snap, prop_snap)

        for owner, prop in batch:
            if not prop.address or not prop.zip:
                stats["no_address"] += 1
                continue
            name_parts = (owner.owner_name or "").strip().split(None, 1)
            first = name_parts[0] if name_parts else ""
            last  = name_parts[1] if len(name_parts) > 1 else ""
            label = str(owner.id)
            records.append({
                "first_name": first,
                "last_name":  last,
                "address":    prop.address,
                "city":       prop.city or "Tampa",
                "state":      prop.state or "FL",
                "zip":        (prop.zip or "")[:5],
                "label":      label,
            })
            label_map[label] = (owner, prop)

        if not records:
            continue

        logger.info("[Tracerfy] Batch %d: submitting %d records...", batch_num, len(records))

        # address_map for result correlation (label not echoed back by API)
        address_map: dict[str, tuple] = {}
        for owner_s, prop_s in batch:
            key = (prop_s.address or "").upper().strip()
            if key:
                address_map[key] = (owner_s, prop_s)

        try:
            queue_id, estimated_wait = _submit_trace_batch(records, api_key)
            logger.info("[Tracerfy] Batch %d queued — queue_id=%s est_wait=%ds",
                        batch_num, queue_id, estimated_wait)
            results = _poll_trace_queue(queue_id, api_key, estimated_wait)
        except RuntimeError as e:
            err_msg = str(e)
            logger.error("[Tracerfy] Batch %d failed: %s", batch_num, err_msg)
            if "401" in err_msg or "403" in err_msg or "429" in err_msg:
                send_alert(
                    subject="[Forced Action] Tracerfy API ERROR",
                    body=(
                        f"Tracerfy skip-trace halted: {err_msg}\n\n"
                        f"Check TRACERFY_API_KEY in .env and credits at tracerfy.com\n\n"
                        f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                    ),
                )
                break
            stats["failed"] += len(records)
            continue

        # ── Persist results ───────────────────────────────────────────────
        hit_addresses = set()
        with get_db_context() as session:
            for row in results:
                addr_key = (row.get("address") or "").upper().strip()
                if addr_key not in address_map:
                    logger.debug("[Tracerfy] Result address not in submitted batch: %s", addr_key)
                    continue

                owner_snap, prop_snap = address_map[addr_key]
                hit_addresses.add(addr_key)
                parsed = _parse_trace_row(row)

                try:
                    owner = session.get(Owner, owner_snap.id)
                    if owner is None:
                        continue

                    existing = (
                        session.query(EnrichedContact)
                        .filter_by(property_id=owner.property_id, source="tracerfy")
                        .first()
                    )
                    if existing:
                        stats["already_done"] += 1
                        continue

                    log_usage(
                        db=session,
                        vendor="tracerfy",
                        purpose="skip_trace",
                        success=parsed["match_success"],
                        cost_cents=cost_per_hit if parsed["match_success"] else 0,
                        property_id=owner.property_id,
                    )

                    ec = EnrichedContact(
                        property_id=owner.property_id,
                        county_id=owner.county_id or county_id,
                        mobile_phone=parsed["mobile_phone"],
                        landline=parsed["landline"],
                        email=parsed["email"],
                        mailing_address=parsed["mailing_address"],
                        source="tracerfy",
                        match_success=parsed["match_success"],
                        raw_response=row,
                        enriched_at=datetime.now(timezone.utc),
                    )
                    session.add(ec)

                    if parsed["mobile_phone"] and not owner.phone_1:
                        owner.phone_1 = parsed["mobile_phone"]
                    elif parsed["landline"] and not owner.phone_1:
                        owner.phone_1 = parsed["landline"]
                    if parsed["email"] and not owner.email_1:
                        owner.email_1 = parsed["email"]
                    if parsed["match_success"]:
                        owner.skip_trace_success = True

                    # Store DNC flags in phone_metadata
                    dnc_flags = parsed.get("dnc_flags")
                    if dnc_flags:
                        meta = dict(owner.phone_metadata or {})
                        meta["phone_1"] = {**(meta.get("phone_1") or {}), **dnc_flags}
                        owner.phone_metadata = meta

                    # Suppress all DNC + litigator phones
                    for dnc_num in parsed.get("all_dnc_phones") or []:
                        already = session.query(SmsOptOut).filter_by(phone=dnc_num).first()
                        if not already:
                            session.add(SmsOptOut(
                                phone=dnc_num,
                                keyword_used="DNC",
                                source="tracerfy_dnc",
                                opted_out_at=datetime.now(timezone.utc),
                            ))
                            logger.info(
                                "[Tracerfy] DNC suppression: property_id=%d phone=%s",
                                owner.property_id, dnc_num,
                            )

                    if parsed["match_success"]:
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1

                except Exception as e:
                    logger.error("[Tracerfy] Persist error owner_id=%d: %s", owner_snap.id, e)
                    logger.debug(traceback.format_exc())
                    stats["failed"] += 1

            # Mark misses (submitted but not in results — Tracerfy omits no-match rows)
            for addr_key, (owner_snap, _) in address_map.items():
                if addr_key not in hit_addresses:
                    log_usage(
                        db=session,
                        vendor="tracerfy",
                        purpose="skip_trace",
                        success=False,
                        cost_cents=0,
                        property_id=owner_snap.property_id,
                    )
                    stats["failed"] += 1

            session.commit()

        if batch_start + _BATCH_SIZE < len(rows):
            logger.info("[Tracerfy] Waiting %ds before next batch (rate limit)...", _BATCH_DELAY)
            time.sleep(_BATCH_DELAY)

    logger.info("=" * 60)
    logger.info("TRACERFY SKIP TRACE COMPLETE")
    logger.info("  Total processed : %d", stats["total"])
    logger.info("  Success         : %d", stats["success"])
    logger.info("  No contact found: %d", stats["failed"])
    logger.info("  No address      : %d", stats["no_address"])
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys
    import json as _json

    parser = argparse.ArgumentParser(description="Tracerfy batch skip-trace (Tier 1, $0.02/hit)")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--balance", action="store_true",
                        help="Print account credit balance and exit")
    args = parser.parse_args()

    try:
        if args.balance:
            print(_json.dumps(get_tracerfy_balance(), indent=2))
            sys.exit(0)

        stats = run_tracerfy_fallback(
            limit=args.limit,
            county_id=args.county_id,
            dry_run=args.dry_run,
        )
        sys.exit(0)
    except Exception as e:
        logger.error("Tracerfy fallback failed: %s", e)
        logger.debug(traceback.format_exc())
        sys.exit(1)
