"""
Whitepages Pro v2 skip-trace fallback — Tier 2 in the waterfall.

Runs after BatchData misses; escalates to PDL when confidence still < threshold.
Whitepages v2 API returns phones WITH type (mobile/landline) and emails.

API:  GET https://api.whitepages.com/v2/person
Auth: X-Api-Key header
Cost: $0.25/lookup (configurable via WHITEPAGES_COST_CENTS)

Usage:
  python -m src.services.whitepages_fallback --limit 50
  python -m src.services.whitepages_fallback --dry-run

Required env:
  WHITEPAGES_API_KEY — Whitepages Pro API key
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

_WP_BASE = "https://api.whitepages.com/v2/person"
_DELAY_BETWEEN_REQUESTS = 0.5  # seconds — stay within rate limits


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> dict:
    return {"X-Api-Key": api_key}


def _call_whitepages(
    first_name: str,
    last_name: str,
    street: str,
    city: str,
    state: str,
    zipcode: str,
    api_key: str,
) -> list:
    """
    GET /v2/person — returns a JSON array of matching person records.

    Whitepages response shape:
      [{"id": ..., "name": ..., "score": 0-100,
        "phones": [{"number": ..., "type": "mobile|landline", "score": 0-100}],
        "emails": [{"address": ..., "score": 0-100}],
        "current_addresses": [{"id": ..., "address": "123 Main St, Tampa, FL 33601"}],
        ...}]
    """
    params = {
        "first_name": first_name,
        "last_name":  last_name,
        "street":     street,
        "city":       city,
        "state_code": state,
        "zipcode":    zipcode,
    }
    resp = requests.get(
        _WP_BASE,
        headers=_headers(api_key),
        params={k: v for k, v in params.items() if v},
        timeout=30,
    )

    if resp.status_code == 403:
        raise RuntimeError("Whitepages: invalid API key (403)")
    if resp.status_code == 429:
        raise RuntimeError("Whitepages: rate limited (429) — reduce request rate")
    if resp.status_code == 404:
        return []
    if resp.status_code == 400:
        logger.warning("[Whitepages] Bad request (400): %s", resp.text[:200])
        return []
    if not resp.ok:
        raise RuntimeError(f"Whitepages HTTP {resp.status_code}: {resp.text[:500]}")

    return resp.json()


def _parse_wp_result(persons: list) -> dict:
    """
    Extract contact info from Whitepages v2 person array.

    phones[].type: "mobile", "landline", "voip", "home", "work"
    phones[].score: 0-100 confidence
    emails[].score: 0-100 confidence
    current_addresses[0].address: pre-formatted string
    """
    if not persons:
        return {
            "mobile_phone": None, "landline": None, "email": None,
            "mailing_address": None, "match_success": False,
        }

    # First result has highest match score
    person = persons[0]

    # Phones — sort by score desc, prefer mobile type
    phones = sorted(
        person.get("phones") or [],
        key=lambda p: p.get("score", 0),
        reverse=True,
    )
    mobile = landline = None
    for ph in phones:
        num   = str(ph.get("number") or "").strip()
        ptype = (ph.get("type") or "").lower()
        if not num:
            continue
        if ptype == "mobile" and not mobile:
            mobile = num
        elif ptype in ("landline", "home", "work", "voip") and not landline:
            landline = num
    if not mobile and not landline and phones:
        mobile = str(phones[0].get("number", "")).strip() or None

    # Emails — sort by score desc
    emails = sorted(
        person.get("emails") or [],
        key=lambda e: e.get("score", 0),
        reverse=True,
    )
    email = emails[0].get("address") if emails else None

    # current_addresses[0].address is a pre-formatted string
    current = person.get("current_addresses") or []
    mailing_address = current[0].get("address") if current else None

    return {
        "mobile_phone":    mobile,
        "landline":        landline,
        "email":           email,
        "mailing_address": mailing_address,
        "match_success":   bool(mobile or landline or email),
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_whitepages_fallback(
    limit: int = 100,
    county_id: str = "hillsborough",
    owner_ids: Optional[list] = None,
    dry_run: bool = False,
) -> dict:
    """
    Run Whitepages fallback enrichment for BatchData misses.

    Standard mode (owner_ids=None): targets BatchData misses not yet Whitepages-traced.
    Waterfall mode (owner_ids=[...]): processes specific owners directly.

    Returns stats dict.
    """
    settings = get_settings()

    if not settings.whitepages_api_key:
        logger.warning("WHITEPAGES_API_KEY not set — Whitepages fallback skipped")
        return {"skipped": True, "reason": "WHITEPAGES_API_KEY not configured"}

    api_key = settings.whitepages_api_key.get_secret_value()
    stats = {"total": 0, "success": 0, "failed": 0, "no_address": 0, "already_done": 0}

    with get_db_context() as session:
        from sqlalchemy import or_ as sa_or, func as sa_func

        wp_already = session.query(EnrichedContact.property_id).filter(
            EnrichedContact.source == "whitepages"
        ).subquery()

        if owner_ids is not None:
            # Waterfall mode: process given owners, skip any already Whitepages-traced
            owner_prop_rows = (
                session.query(Owner, Property)
                .join(Property, Owner.property_id == Property.id)
                .filter(
                    Owner.id.in_(owner_ids),
                    Owner.property_id.notin_(session.query(wp_already)),
                )
                .all()
            )
            candidates = [(None, owner, prop) for owner, prop in owner_prop_rows]
        else:
            # Standard mode: BatchData misses whose owner still has no phone
            no_phone = sa_or(
                Owner.phone_1.is_(None),
                sa_func.length(sa_func.trim(Owner.phone_1)) == 0,
            )
            candidates = (
                session.query(EnrichedContact, Owner, Property)
                .join(Owner, EnrichedContact.property_id == Owner.property_id)
                .join(Property, Property.id == EnrichedContact.property_id)
                .filter(
                    EnrichedContact.source == "batch_skip_tracing",
                    EnrichedContact.match_success == False,   # noqa: E712
                    EnrichedContact.property_id.notin_(session.query(wp_already)),
                    Owner.county_id == county_id,
                    no_phone,
                )
                .limit(limit)
                .all()
            )

    if not candidates:
        logger.info("[Whitepages] No candidates — every miss either has a phone now or was already retried.")
        return stats

    logger.info("[Whitepages] Found %d candidates to enrich", len(candidates))
    stats["total"] = len(candidates)

    if dry_run:
        for _, owner, prop in candidates[:5]:
            logger.info("[Whitepages DRY RUN] Would enrich: property_id=%d | %s | %s",
                        prop.id, prop.address, owner.owner_name)
        logger.info("[Whitepages DRY RUN] Would process %d records total.", len(candidates))
        return stats

    for idx, (_, owner_snap, prop_snap) in enumerate(candidates):
        if not prop_snap.address or not prop_snap.zip:
            stats["no_address"] += 1
            continue

        name = owner_snap.owner_name or ""
        name_parts = name.strip().split(None, 1)
        first_name = name_parts[0] if name_parts else ""
        last_name  = name_parts[1] if len(name_parts) > 1 else ""

        try:
            persons = _call_whitepages(
                first_name=first_name,
                last_name=last_name,
                street=prop_snap.address,
                city=prop_snap.city or "Tampa",
                state=prop_snap.state or "FL",
                zipcode=prop_snap.zip,
                api_key=api_key,
            )
        except RuntimeError as e:
            err_msg = str(e)
            logger.error("[Whitepages] API error for owner_id=%d: %s", owner_snap.id, err_msg)
            stats["failed"] += len(candidates) - idx  # all remaining will also fail

            if "403" in err_msg or "429" in err_msg:
                send_alert(
                    subject="[Forced Action] Whitepages API ERROR",
                    body=(
                        f"Whitepages fallback enrichment halted: {err_msg}\n\n"
                        f"Action required:\n"
                        f"  1. Check WHITEPAGES_API_KEY in .env\n"
                        f"  2. Check rate limits at api.whitepages.com\n\n"
                        f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                    ),
                )
            break

        parsed = _parse_wp_result(persons)

        with get_db_context() as session:
            try:
                owner = session.get(Owner, owner_snap.id)
                if owner is None:
                    continue

                wp_ec = EnrichedContact(
                    property_id=owner.property_id,
                    county_id=owner.county_id or county_id,
                    mobile_phone=parsed["mobile_phone"],
                    landline=parsed["landline"],
                    email=parsed["email"],
                    mailing_address=parsed["mailing_address"],
                    llc_owner_name=None,
                    relative_contacts=None,
                    source="whitepages",
                    match_success=parsed["match_success"],
                    enriched_at=datetime.now(timezone.utc),
                )
                session.add(wp_ec)

                if parsed["match_success"]:
                    if parsed["mobile_phone"] and not owner.phone_1:
                        owner.phone_1 = parsed["mobile_phone"]
                    elif parsed["landline"] and not owner.phone_1:
                        owner.phone_1 = parsed["landline"]
                    if parsed["email"] and not owner.email_1:
                        owner.email_1 = parsed["email"]
                    owner.skip_trace_success = True
                    stats["success"] += 1
                    logger.debug("[Whitepages] ✓ property_id=%d found contact", owner.property_id)
                else:
                    stats["failed"] += 1
                    logger.debug("[Whitepages] ✗ property_id=%d — no contact", owner.property_id)

            except Exception as e:
                logger.error("[Whitepages] Error persisting result for owner_id=%d: %s",
                             owner_snap.id, e)
                logger.debug(traceback.format_exc())
                stats["failed"] += 1

        if idx < len(candidates) - 1:
            time.sleep(_DELAY_BETWEEN_REQUESTS)

    logger.info("=" * 60)
    logger.info("WHITEPAGES FALLBACK COMPLETE")
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

    parser = argparse.ArgumentParser(description="Whitepages fallback skip-tracing for BatchData misses")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        stats = run_whitepages_fallback(
            limit=args.limit,
            county_id=args.county_id,
            dry_run=args.dry_run,
        )
        sys.exit(0)
    except Exception as e:
        logger.error("Whitepages fallback failed: %s", e)
        logger.debug(traceback.format_exc())
        sys.exit(1)
