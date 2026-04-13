"""
BatchData Skip Tracing Service

Enriches high-priority distressed property leads with owner contact info
(phone, email, mailing address) via the BatchData skip-trace API.

Flow:
  1. Query owners with no contact info, filtered by lead tier
  2. POST to BatchData API (up to 100 records per batch)
  3. Parse response → phones, emails, mailing address
  4. Persist EnrichedContact record per property
  5. Update Owner.phone_1 / email_1 for GHL push

Usage:
  python -m src.services.skip_trace --limit 10 --dry-run
  python -m src.services.skip_trace --limit 100 --tier Gold
  python -m src.services.skip_trace --limit 500
  python -m src.services.skip_trace --limit 50 --vertical roofing
"""

import time
import traceback
from datetime import date, datetime, timezone
from typing import Optional

import requests
from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import Owner, Property, EnrichedContact, DistressScore
from src.services.email import send_alert
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_BATCH_DATA_BASE = "https://api.batchdata.com"
_SKIP_TRACE_ENDPOINT = f"{_BATCH_DATA_BASE}/api/v1/property/skip-trace"
_BATCH_SIZE = 100          # BatchData max per request
_DELAY_BETWEEN_BATCHES = 1.0  # seconds


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _call_batch_data(requests_payload: list, api_key: str) -> list:
    """
    POST one batch to BatchData skip-trace endpoint.
    Returns the list of result dicts from data.results[].
    Raises on HTTP error.
    """
    body = {"requests": requests_payload}

    resp = requests.post(
        _SKIP_TRACE_ENDPOINT,
        headers=_headers(api_key),
        json=body,
        timeout=60,
    )

    if resp.status_code == 402:
        raise RuntimeError("BatchData: out of credits or sandbox limit reached (402)")
    if resp.status_code == 401:
        raise RuntimeError("BatchData: invalid API key (401)")
    if not resp.ok:
        raise RuntimeError(f"BatchData HTTP {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    # Response shape: {"status": {"code": 200}, "results": {"persons": [...], "meta": {...}}}
    return data.get("results", {}).get("persons", [])


def _parse_result(person: dict) -> dict:
    """
    Extract contact fields from a single BatchData person entry.

    BatchData response shape per person:
      phoneNumbers: [{number, type, carrier, tested, reachable, score}]
      emails:       [{email}]
      mailingAddress: {street, city, state, zip, ...}
      name:         {first, last}
    """
    # Phones — prefer reachable Mobile first, then any Mobile, then Land Line
    phones = person.get("phoneNumbers") or []
    mobile_phone = None
    landline = None

    # Sort by score descending so best numbers come first
    phones_sorted = sorted(phones, key=lambda p: int(p.get("score", 0) or 0), reverse=True)
    for ph in phones_sorted:
        ptype = (ph.get("type") or "").lower()
        number = str(ph.get("number") or "").strip()
        if not number:
            continue
        if "mobile" in ptype and not mobile_phone:
            mobile_phone = number
        elif "land" in ptype and not landline:
            landline = number
    # fallback: use first number regardless of type
    if not mobile_phone and not landline and phones_sorted:
        mobile_phone = str(phones_sorted[0].get("number", "")).strip() or None

    # Email
    emails = person.get("emails") or []
    email = emails[0].get("email") if emails else None

    # Mailing address
    addr_obj = person.get("mailingAddress") or {}
    if addr_obj:
        parts = [
            addr_obj.get("street") or addr_obj.get("streetNoUnit") or "",
            addr_obj.get("city", ""),
            addr_obj.get("state", ""),
            addr_obj.get("zip", ""),
        ]
        mailing_address = ", ".join(p for p in parts if p) or None
    else:
        mailing_address = None

    # LLC / company name (BatchData doesn't typically return this for individuals)
    name_obj = person.get("name") or {}
    llc_owner_name = None  # only populate if business record

    # Relative contacts (not in standard BatchData response — placeholder for IDI)
    relative_contacts = None

    match_success = bool(mobile_phone or landline or email)

    return {
        "mobile_phone": mobile_phone,
        "landline": landline,
        "email": email,
        "mailing_address": mailing_address,
        "llc_owner_name": llc_owner_name,
        "relative_contacts": relative_contacts,
        "match_success": match_success,
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_skip_trace(
    limit: int = 100,
    tier_filter: Optional[str] = None,
    vertical: Optional[str] = None,
    dry_run: bool = False,
    county_id: str = "hillsborough",
    today_only: bool = False,
) -> dict:
    """
    Enrich owner contacts for high-priority leads with no contact info.

    Args:
        limit:      Max number of owners to process in this run.
        tier_filter: Filter by lead tier (e.g. 'Gold', 'Platinum'). None = all tiers ≥ Gold.
        dry_run:    If True, build payloads and print but do NOT call API or write DB.
        county_id:  County to process.
        today_only: If True, only process leads whose latest score was recorded today.
                    Use this for daily cron runs to avoid spending credits on stale leads.

    Returns:
        Stats dict with total/success/failed/skipped counts.
    """
    settings = get_settings()

    if not settings.batch_skip_tracing_api_key:
        raise RuntimeError("BATCH_SKIP_TRACING_API_KEY not set in .env")

    api_key = settings.batch_skip_tracing_api_key.get_secret_value()

    stats = {"total": 0, "success": 0, "failed": 0, "already_traced": 0, "no_address": 0}

    # --- 1. Pull candidates from DB ---
    tier_values = ["Ultra Platinum", "Platinum", "Gold"]
    if tier_filter:
        tier_values = [tier_filter]

    with get_db_context() as session:
        # Subquery: latest score_date per property
        from sqlalchemy import func as sa_func
        from sqlalchemy.orm import aliased

        ds_latest = (
            session.query(
                DistressScore.property_id,
                sa_func.max(DistressScore.score_date).label("max_date"),
            )
            .group_by(DistressScore.property_id)
            .subquery()
        )

        # Subquery: join back to get lead_tier on that latest date
        ds_q = (
            session.query(DistressScore.property_id, DistressScore.lead_tier)
            .join(
                ds_latest,
                (DistressScore.property_id == ds_latest.c.property_id)
                & (DistressScore.score_date == ds_latest.c.max_date),
            )
            .filter(DistressScore.lead_tier.in_(tier_values))
        )

        # today_only: restrict to leads scored today — prevents spending credits on stale leads
        if today_only:
            from sqlalchemy import cast as sa_cast, Date as SADate
            ds_q = ds_q.filter(
                sa_cast(DistressScore.score_date, SADate) == date.today()
            )
            logger.info("today_only mode: restricting skip trace to leads scored on %s", date.today())

        # Optional vertical filter — only enrich owners scoring > 0 for this vertical
        if vertical:
            ds_q = ds_q.filter(
                text(f"(vertical_scores->>:vert)::float > 0").bindparams(vert=vertical)
            )
            logger.info("Vertical filter: %s", vertical)

        ds_current = ds_q.subquery()

        rows = (
            session.query(Owner, Property)
            .join(Property, Owner.property_id == Property.id)
            .join(ds_current, ds_current.c.property_id == Property.id)
            .join(
                ds_latest,
                ds_latest.c.property_id == Property.id,
            )
            .filter(Owner.phone_1.is_(None))
            .filter(Owner.email_1.is_(None))
            .filter(Owner.skip_trace_success.is_not(True))
            .filter(Owner.county_id == county_id)
            .order_by(ds_latest.c.max_date.desc())   # freshest leads first
            .limit(limit)
            .all()
        )

    if not rows:
        logger.info("No candidates found — all high-priority owners already have contact info.")
        return stats

    logger.info(f"Found {len(rows)} owners to skip-trace (limit={limit}, tiers={tier_values}, vertical={vertical or 'all'})")
    stats["total"] = len(rows)

    if dry_run:
        for owner, prop in rows[:5]:
            logger.info(f"[DRY RUN] Would trace: property_id={prop.id} | {prop.address}, {prop.city} {prop.zip} | owner={owner.owner_name}")
        logger.info(f"[DRY RUN] Would process {len(rows)} records total. No API call made.")
        return stats

    # --- 2. Process in batches ---
    api_key_val = api_key

    for batch_start in range(0, len(rows), _BATCH_SIZE):
        batch = rows[batch_start: batch_start + _BATCH_SIZE]
        batch_num = batch_start // _BATCH_SIZE + 1
        logger.info(f"Batch {batch_num}: processing {len(batch)} records...")

        # Build request payloads, keep index → (owner, prop) mapping
        payloads = []
        index_map = []  # parallel list: index_map[i] = (owner, prop) for payloads[i]

        for owner, prop in batch:
            if not prop.address or not prop.zip:
                stats["no_address"] += 1
                logger.debug(f"Skipping property_id={prop.id} — missing address or ZIP")
                continue

            payload_entry = {
                "propertyAddress": {
                    "street": prop.address,
                    "city":   prop.city  or "Tampa",
                    "state":  prop.state or "FL",
                    "zip":    prop.zip,
                }
            }

            payloads.append(payload_entry)
            index_map.append((owner, prop))

        if not payloads:
            continue

        try:
            results = _call_batch_data(payloads, api_key_val)
        except Exception as e:
            err_msg = str(e)
            logger.error(f"BatchData API error on batch {batch_num}: {err_msg}")
            stats["failed"] += len(payloads)

            # Alert on credential/billing failures — these won't self-heal
            if "402" in err_msg or "401" in err_msg:
                send_alert(
                    subject="[Forced Action] BatchData API CREDENTIAL ERROR",
                    body=(
                        f"Skip-trace enrichment halted: {err_msg}\n\n"
                        f"Action required:\n"
                        f"  1. Check BATCH_SKIP_TRACING_API_KEY in .env\n"
                        f"  2. Verify credits at app.batchdata.com\n"
                        f"  3. Run: python -m src.services.skip_trace --dry-run --limit 5\n\n"
                        f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                    ),
                )
            break

        # --- 3. Persist results ---
        with get_db_context() as session:
            for i, result in enumerate(results):
                if i >= len(index_map):
                    break

                owner_snap, prop_snap = index_map[i]

                try:
                    parsed = _parse_result(result)

                    # Reload owner in this session
                    owner = session.get(Owner, owner_snap.id)
                    if owner is None:
                        continue

                    # Check for duplicate enriched_contact
                    existing = (
                        session.query(EnrichedContact)
                        .filter_by(property_id=owner.property_id, source="batch_skip_tracing")
                        .first()
                    )

                    if existing:
                        stats["already_traced"] += 1
                        continue

                    # Save EnrichedContact
                    ec = EnrichedContact(
                        property_id=owner.property_id,
                        county_id=owner.county_id or county_id,
                        mobile_phone=parsed["mobile_phone"],
                        landline=parsed["landline"],
                        email=parsed["email"],
                        mailing_address=parsed["mailing_address"],
                        llc_owner_name=parsed["llc_owner_name"],
                        relative_contacts=parsed["relative_contacts"],
                        source="batch_skip_tracing",
                        match_success=parsed["match_success"],
                        enriched_at=datetime.now(timezone.utc),
                    )
                    session.add(ec)

                    # Update Owner contact fields
                    if parsed["mobile_phone"]:
                        owner.phone_1 = parsed["mobile_phone"]
                    elif parsed["landline"]:
                        owner.phone_1 = parsed["landline"]
                    if parsed["email"]:
                        owner.email_1 = parsed["email"]
                    if parsed["match_success"]:
                        owner.skip_trace_success = True

                    if parsed["match_success"]:
                        stats["success"] += 1
                        logger.debug(
                            f"✓ property_id={owner.property_id} | "
                            f"phone={parsed['mobile_phone'] or parsed['landline']} | "
                            f"email={parsed['email']}"
                        )
                    else:
                        stats["failed"] += 1
                        logger.debug(f"✗ property_id={owner.property_id} — no contact found")

                except Exception as e:
                    logger.error(f"Error persisting result for owner_id={owner_snap.id}: {e}")
                    logger.debug(traceback.format_exc())
                    stats["failed"] += 1

        if batch_start + _BATCH_SIZE < len(rows):
            time.sleep(_DELAY_BETWEEN_BATCHES)

    logger.info("=" * 60)
    logger.info("SKIP TRACE COMPLETE")
    logger.info(f"  Total processed : {stats['total']}")
    logger.info(f"  Success         : {stats['success']}")
    logger.info(f"  No contact found: {stats['failed']}")
    logger.info(f"  Already traced  : {stats['already_traced']}")
    logger.info(f"  No address      : {stats['no_address']}")
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="BatchData skip tracing for high-priority leads")
    parser.add_argument("--limit", type=int, default=10,
                        help="Max owners to process (default: 10)")
    parser.add_argument("--tier", type=str, default=None,
                        choices=["Ultra Platinum", "Platinum", "Gold"],
                        help="Filter by lead tier (default: all Gold+)")
    parser.add_argument("--vertical", type=str, default=None,
                        help="Filter by vertical (e.g. roofing, remediation, investor)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build payloads but do NOT call API or write to DB")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--today-only", dest="today_only", action="store_true",
                        help="Only skip-trace leads scored today (safe for daily cron)")
    args = parser.parse_args()

    try:
        stats = run_skip_trace(
            limit=args.limit,
            tier_filter=args.tier,
            vertical=args.vertical,
            dry_run=args.dry_run,
            county_id=args.county_id,
            today_only=args.today_only,
        )
        sys.exit(0)
    except Exception as e:
        logger.error(f"Skip trace failed: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)
