"""
IDI (Interactive Data Inc.) skip-trace fallback.

Runs after BatchSkipTracing completes for properties where match_success=False.
IDI typically has higher match rates for LLCs and absentee owners.

Flow:
  1. Query EnrichedContact rows where source='batch_skip_tracing' AND match_success=False
  2. POST to IDI API
  3. Parse → phones, emails, relatives
  4. Create a new EnrichedContact with source='idi' (does NOT overwrite BatchData record)
  5. Update Owner.phone_1 / email_1 if enriched

Usage:
  python -m src.services.idi_fallback --limit 50
  python -m src.services.idi_fallback --dry-run

Required env:
  IDI_API_KEY — IDI Digicore API key
"""

import time
import traceback
from datetime import datetime, timezone
from typing import Optional

import requests

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import EnrichedContact, Owner, Property
from src.services.email import send_alert
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_IDI_BASE = "https://api.idicore.com"
_IDI_SEARCH_ENDPOINT = f"{_IDI_BASE}/api/v3/person/search"
_BATCH_SIZE = 50       # IDI recommended batch size
_DELAY_BETWEEN_BATCHES = 2.0  # seconds — IDI rate limit is lower than BatchData


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _call_idi(records: list, api_key: str) -> list:
    """
    POST one batch to IDI search endpoint.
    Returns list of result dicts.

    IDI request shape:
      {"searches": [{"firstName": ..., "lastName": ..., "address": {...}}, ...]}

    IDI response shape:
      {"results": [{"persons": [...], "request_id": ...}, ...]}
    """
    body = {"searches": records}

    resp = requests.post(
        _IDI_SEARCH_ENDPOINT,
        headers=_headers(api_key),
        json=body,
        timeout=90,
    )

    if resp.status_code == 401:
        raise RuntimeError("IDI: invalid API key (401)")
    if resp.status_code == 402:
        raise RuntimeError("IDI: out of credits or account suspended (402)")
    if resp.status_code == 429:
        raise RuntimeError("IDI: rate limited (429) — reduce batch size or add delay")
    if not resp.ok:
        raise RuntimeError(f"IDI HTTP {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    # Flatten: each search result contains a "persons" list; take first person per search
    raw_results = data.get("results", [])
    return raw_results


def _parse_idi_result(result: dict) -> dict:
    """
    Extract contact info from a single IDI search result.

    IDI response per result:
      persons: [{phones: [{number, type}], emails: [{address}],
                 relatives: [{name, phones}], currentAddress: {...}}]
    """
    persons = result.get("persons") or []
    if not persons:
        return {"mobile_phone": None, "landline": None, "email": None,
                "mailing_address": None, "relative_contacts": None, "match_success": False}

    person = persons[0]

    # Phones
    phones = person.get("phones") or []
    mobile_phone = None
    landline = None
    for ph in phones:
        ptype = (ph.get("type") or "").lower()
        number = str(ph.get("number") or "").strip()
        if not number:
            continue
        if "mobile" in ptype or "cell" in ptype:
            if not mobile_phone:
                mobile_phone = number
        elif "land" in ptype or "home" in ptype or "work" in ptype:
            if not landline:
                landline = number
    if not mobile_phone and not landline and phones:
        mobile_phone = str(phones[0].get("number", "")).strip() or None

    # Email
    emails = person.get("emails") or []
    email = emails[0].get("address") if emails else None

    # Mailing address
    addr = person.get("currentAddress") or {}
    if addr:
        parts = [
            addr.get("street") or "",
            addr.get("city") or "",
            addr.get("state") or "",
            addr.get("zip") or "",
        ]
        mailing_address = ", ".join(p for p in parts if p) or None
    else:
        mailing_address = None

    # Relative contacts (IDI-specific feature)
    relatives = person.get("relatives") or []
    relative_contacts = None
    if relatives:
        relative_contacts = [
            {
                "name": r.get("name") or "",
                "phones": [str(p.get("number", "")) for p in (r.get("phones") or [])],
            }
            for r in relatives[:5]  # cap at 5 relatives
        ]

    match_success = bool(mobile_phone or landline or email)
    return {
        "mobile_phone": mobile_phone,
        "landline": landline,
        "email": email,
        "mailing_address": mailing_address,
        "relative_contacts": relative_contacts,
        "match_success": match_success,
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_idi_fallback(
    limit: int = 100,
    county_id: str = "hillsborough",
    dry_run: bool = False,
) -> dict:
    """
    Run IDI fallback enrichment for BatchData misses.

    Targets properties where:
      - EnrichedContact(source='batch_skip_tracing', match_success=False) exists
      - No EnrichedContact(source='idi') exists yet

    Returns stats dict.
    """
    settings = get_settings()

    if not settings.idi_api_key:
        logger.warning("IDI_API_KEY not set — IDI fallback skipped")
        return {"skipped": True, "reason": "IDI_API_KEY not configured"}

    api_key = settings.idi_api_key.get_secret_value()
    stats = {"total": 0, "success": 0, "failed": 0, "no_address": 0, "already_done": 0}

    # Pull candidates: BatchData misses not yet tried via IDI
    with get_db_context() as session:
        # Properties with a failed BatchData trace and no IDI trace yet
        from sqlalchemy import exists, and_

        idi_exists = session.query(EnrichedContact.property_id).filter(
            EnrichedContact.source == "idi"
        ).subquery()

        candidates = (
            session.query(EnrichedContact, Owner, Property)
            .join(Owner, EnrichedContact.property_id == Owner.property_id)
            .join(Property, Property.id == EnrichedContact.property_id)
            .filter(
                EnrichedContact.source == "batch_skip_tracing",
                EnrichedContact.match_success == False,   # noqa: E712
                EnrichedContact.property_id.notin_(
                    session.query(idi_exists)
                ),
                Owner.county_id == county_id,
            )
            .limit(limit)
            .all()
        )

    if not candidates:
        logger.info("[IDI] No BatchData misses to retry — all already tried or no misses found.")
        return stats

    logger.info("[IDI] Found %d BatchData misses to retry via IDI", len(candidates))
    stats["total"] = len(candidates)

    if dry_run:
        for ec, owner, prop in candidates[:5]:
            logger.info("[IDI DRY RUN] Would retry: property_id=%d | %s | %s",
                        prop.id, prop.address, owner.owner_name)
        logger.info("[IDI DRY RUN] Would process %d records total.", len(candidates))
        return stats

    # Process in batches
    for batch_start in range(0, len(candidates), _BATCH_SIZE):
        batch = candidates[batch_start: batch_start + _BATCH_SIZE]
        batch_num = batch_start // _BATCH_SIZE + 1
        logger.info("[IDI] Batch %d: processing %d records...", batch_num, len(batch))

        search_records = []
        index_map = []

        for ec, owner, prop in batch:
            if not prop.address or not prop.zip:
                stats["no_address"] += 1
                continue

            # Build name parts
            name = owner.owner_name or ""
            name_parts = name.strip().split(None, 1)
            first_name = name_parts[0] if name_parts else ""
            last_name = name_parts[1] if len(name_parts) > 1 else ""

            search_records.append({
                "firstName": first_name,
                "lastName": last_name,
                "address": {
                    "street": prop.address,
                    "city": prop.city or "Tampa",
                    "state": prop.state or "FL",
                    "zip": prop.zip,
                },
            })
            index_map.append((ec, owner, prop))

        if not search_records:
            continue

        try:
            results = _call_idi(search_records, api_key)
        except RuntimeError as e:
            err_msg = str(e)
            logger.error("[IDI] API error on batch %d: %s", batch_num, err_msg)
            stats["failed"] += len(search_records)

            # Alert on credential/billing failures
            if "401" in err_msg or "402" in err_msg:
                send_alert(
                    subject="[Forced Action] IDI API CREDENTIAL ERROR",
                    body=(
                        f"IDI fallback enrichment halted: {err_msg}\n\n"
                        f"Action required:\n"
                        f"  1. Check IDI_API_KEY in .env\n"
                        f"  2. Verify IDI account credits at idicore.com\n\n"
                        f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                    ),
                )
            break

        # Persist results
        with get_db_context() as session:
            for i, result in enumerate(results):
                if i >= len(index_map):
                    break

                ec_snap, owner_snap, prop_snap = index_map[i]

                try:
                    parsed = _parse_idi_result(result)

                    owner = session.get(Owner, owner_snap.id)
                    if owner is None:
                        continue

                    # Create IDI EnrichedContact
                    idi_ec = EnrichedContact(
                        property_id=owner.property_id,
                        county_id=owner.county_id or county_id,
                        mobile_phone=parsed["mobile_phone"],
                        landline=parsed["landline"],
                        email=parsed["email"],
                        mailing_address=parsed["mailing_address"],
                        llc_owner_name=None,
                        relative_contacts=parsed["relative_contacts"],
                        source="idi",
                        match_success=parsed["match_success"],
                        enriched_at=datetime.now(timezone.utc),
                    )
                    session.add(idi_ec)

                    # Update Owner fields if IDI found contact info
                    if parsed["match_success"]:
                        if parsed["mobile_phone"] and not owner.phone_1:
                            owner.phone_1 = parsed["mobile_phone"]
                        elif parsed["landline"] and not owner.phone_1:
                            owner.phone_1 = parsed["landline"]
                        if parsed["email"] and not owner.email_1:
                            owner.email_1 = parsed["email"]
                        owner.skip_trace_success = True
                        stats["success"] += 1
                        logger.debug("[IDI] ✓ property_id=%d found contact", owner.property_id)
                    else:
                        stats["failed"] += 1
                        logger.debug("[IDI] ✗ property_id=%d — no contact", owner.property_id)

                except Exception as e:
                    logger.error("[IDI] Error persisting result for owner_id=%d: %s",
                                 owner_snap.id, e)
                    logger.debug(traceback.format_exc())
                    stats["failed"] += 1

        if batch_start + _BATCH_SIZE < len(candidates):
            time.sleep(_DELAY_BETWEEN_BATCHES)

    logger.info("=" * 60)
    logger.info("IDI FALLBACK COMPLETE")
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

    parser = argparse.ArgumentParser(description="IDI fallback skip-tracing for BatchData misses")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        stats = run_idi_fallback(
            limit=args.limit,
            county_id=args.county_id,
            dry_run=args.dry_run,
        )
        sys.exit(0)
    except Exception as e:
        logger.error("IDI fallback failed: %s", e)
        logger.debug(traceback.format_exc())
        sys.exit(1)
