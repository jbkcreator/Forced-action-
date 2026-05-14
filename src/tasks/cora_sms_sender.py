"""
Cora SMS outbound sender — DBPR contractor signup via SMS.

Targets enriched DBPR contacts that have a phone but haven't signed up yet.
For each contact:
  1. create_free_account(phone, source='cora_sms') — deduped on phone
  2. Generate signed landing link (72hr TTL)
  3. Send SMS with vertical-specific copy + link
  4. Update DBPRContact.subscriber_id + signed_up_at

Rate-limited (default 1s between sends). Safe to re-run — subscriber_id guard
and phone dedup make it idempotent.

Run:
    python -m src.tasks.cora_sms_sender --dry-run
    python -m src.tasks.cora_sms_sender --limit 50
    python -m src.tasks.cora_sms_sender --county-id pinellas --limit 100
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import DBPRContact
from src.services.signup_engine import create_free_account
from src.services.sms_compliance import can_send, send_sms
from src.services.signed_links import encode_landing_token
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_DEFAULT_LIMIT = 100
_DEFAULT_DELAY = 1.0

# Vertical-specific SMS copy (keep under 80 chars so URL fits in ~2 segments)
_VERTICAL_SMS: dict[str, str] = {
    "roofing":     "free roofing leads",
    "hvac":        "free HVAC leads",
    "plumbing":    "free plumbing leads",
    "general":     "free contractor leads",
    "remediation": "free remediation leads",
}


def _first_name(full_name: str) -> str:
    name = (full_name or "").strip()
    if "," in name:
        first = name.split(",", 1)[1].strip().split()
        return first[0].title() if first else "there"
    parts = name.split()
    return parts[0].title() if parts else "there"


def _sms_body(full_name: str, vertical: str, landing_url: str) -> str:
    first     = _first_name(full_name)
    lead_copy = _VERTICAL_SMS.get(vertical, "free property leads")
    return (
        f"Hi {first}, Forced Action has {lead_copy} in your county. "
        f"Free signup: {landing_url} — Reply STOP to opt out."
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_cora_sms_sender(
    county_id: str = "hillsborough",
    limit: int = _DEFAULT_LIMIT,
    delay: float = _DEFAULT_DELAY,
    dry_run: bool = False,
) -> dict:
    """
    Send outbound signup SMS to enriched DBPR contacts without a subscriber.

    Returns stats dict.
    """
    settings = get_settings()
    stats = {
        "county_id": county_id,
        "total":     0,
        "sent":      0,
        "deduped":   0,
        "skipped":   0,
        "failed":    0,
    }

    with get_db_context() as db:
        candidates = (
            db.query(DBPRContact)
            .filter(
                DBPRContact.county_id == county_id,
                DBPRContact.enrichment_status == "enriched",
                DBPRContact.phone.isnot(None),
                DBPRContact.subscriber_id.is_(None),
            )
            .order_by(DBPRContact.created_at.asc())
            .limit(limit)
            .all()
        )

    if not candidates:
        logger.info("[CoraSmS] No eligible contacts (county=%s)", county_id)
        return stats

    stats["total"] = len(candidates)
    logger.info("[CoraSMS] %d contacts to SMS (county=%s, dry_run=%s)",
                len(candidates), county_id, dry_run)

    now = datetime.now(timezone.utc)

    for i, contact in enumerate(candidates):
        phone    = (contact.phone or "").strip()
        vertical = contact.vertical or "general"

        if not phone:
            stats["skipped"] += 1
            continue

        if dry_run:
            token = "DRY_RUN_TOKEN"
            url   = f"{settings.app_base_url}/?signup_source=cora_sms&token={token}"
            body  = _sms_body(contact.full_name, vertical, url)
            logger.info("[CoraSMS DRY RUN] Would SMS %s | %s", phone, body[:80])
            stats["sent"] += 1
            continue

        # Step 1 — create (or dedup) subscriber
        with get_db_context() as db:
            sub = create_free_account(
                phone=phone,
                source="cora_sms",
                db=db,
                county_id=county_id,
            )
            is_deduped = sub.signup_source != "cora_sms"

        # Step 2 — signed link
        token = encode_landing_token(sub.id, "cora_sms", ttl_hours=72)
        if token:
            landing_url = f"{settings.app_base_url}/?signup_source=cora_sms&token={token}"
        else:
            landing_url = f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"

        # Step 3 — send SMS
        body = _sms_body(contact.full_name, vertical, landing_url)

        with get_db_context() as db:
            ok = can_send(phone, db) and send_sms(
                to=phone,
                body=body,
                db=db,
                subscriber_id=sub.id,
                task_type="cora_sms_signup",
            )

        # Step 4 — link DBPRContact to subscriber
        with get_db_context() as db:
            c = db.get(DBPRContact, contact.id)
            if c:
                c.subscriber_id = sub.id
                c.signed_up_at  = now
                c.updated_at    = now
                db.add(c)

        if ok:
            with get_db_context() as db:
                from src.services.business_events import log_business_event
                log_business_event(
                    "SMS_SENT",
                    subscriber_id=sub.id,
                    payload={
                        "channel":       "cora_sms",
                        "signup_source": "cora_sms",
                        "vertical":      vertical,
                        "county_id":     county_id,
                        "deduped":       is_deduped,
                    },
                    db=db,
                )
            if is_deduped:
                stats["deduped"] += 1
                logger.info("[CoraSMS] Deduped (%d/%d) → sub=%d phone=%s",
                            i + 1, len(candidates), sub.id, phone)
            else:
                stats["sent"] += 1
                logger.info("[CoraSMS] Sent (%d/%d) → sub=%d phone=%s",
                            i + 1, len(candidates), sub.id, phone)
        else:
            stats["failed"] += 1
            logger.warning("[CoraSMS] Send failed → phone=%s", phone)

        if i < len(candidates) - 1:
            time.sleep(delay)

    logger.info(
        "[CoraSMS] Done. sent=%d deduped=%d failed=%d skipped=%d / total=%d",
        stats["sent"], stats["deduped"], stats["failed"], stats["skipped"], stats["total"],
    )
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cora SMS outbound sender for DBPR contacts")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--limit",  type=int,   default=_DEFAULT_LIMIT)
    parser.add_argument("--delay",  type=float, default=_DEFAULT_DELAY,
                        help="Seconds between sends (default: 1.0)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        result = run_cora_sms_sender(
            county_id=args.county_id,
            limit=args.limit,
            delay=args.delay,
            dry_run=args.dry_run,
        )
        print(f"  Sent    : {result['sent']}")
        print(f"  Deduped : {result['deduped']}")
        print(f"  Failed  : {result['failed']}")
        print(f"  Skipped : {result['skipped']}")
        print(f"  Total   : {result['total']}")
        sys.exit(0 if result["failed"] == 0 else 1)
    except Exception as e:
        logger.error("[CoraSMS] Sender crashed: %s", e)
        sys.exit(1)
