"""
county_live_reactivation — Notify waitlist when a county launches.

Polls for ExpansionCandidate rows with status='launched' whose waitlist entries
are still 'waiting'. Fires a reactivation wave via the reactivation graph.

Run:
    python -m src.tasks.county_live_reactivation
"""

import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import ExpansionCandidate, WaitlistEntry
from src.services.sms_compliance import can_send, send_sms

logger = logging.getLogger(__name__)


def compute_slots_remaining(db: Session, county_id: str, vertical: str) -> int:
    from sqlalchemy import func, text
    return db.execute(text("""
        SELECT COUNT(*) FROM zip_territories
         WHERE county_id=:c AND vertical=:v AND status='available'
    """), {"c": county_id, "v": vertical}).scalar() or 0


def send_reactivation_sms(entry: WaitlistEntry, slots_remaining: int, db: Session) -> bool:
    from src.services.phone_utils import normalize as normalize_phone

    phone = normalize_phone(entry.phone_e164) if entry.phone_e164 else None
    if not phone or not entry.sms_opt_in:
        return False

    if not can_send(phone, db):
        return False

    now = datetime.now(timezone.utc)
    wait_days = (now - entry.created_at).days if entry.created_at else 0
    
    if wait_days < 1:
        wait_label = "since this morning"
    elif wait_days < 14:
        wait_label = f"{wait_days} days"
    elif wait_days < 60:
        wait_label = f"{wait_days // 7} weeks"
    else:
        wait_label = f"since {entry.created_at.strftime('%B')}" if entry.created_at else f"{wait_days} days"

    if slots_remaining > 20:
        scarcity = "limited spots"
    elif slots_remaining > 0:
        scarcity = f"{slots_remaining} left"
    else:
        scarcity = "just opened"

    body = (
        f"{entry.name}, {entry.county_id} is LIVE! You waited {wait_label}. "
        f"{scarcity} for {entry.vertical}. Lock yours: https://forcedactionleads.com"
    )

    try:
        send_sms(
            phone,
            body,
            message_type="marketing",
            campaign="county_live_reactivation",
        )
        return True
    except Exception as e:
        logger.error("Failed to send reactivation SMS to entry %d: %s", entry.id, e)
        return False


def fire_reactivation_wave(db: Session, county_id: str) -> dict:
    entries = db.execute(
        select(WaitlistEntry).where(
            WaitlistEntry.county_id == county_id,
            WaitlistEntry.status == "waiting",
            WaitlistEntry.waitlist_type == "coming_soon",
        )
    ).scalars().all()

    if not entries:
        return {"fired": 0, "county_id": county_id}

    from collections import defaultdict
    by_phone: dict[str, list[WaitlistEntry]] = defaultdict(list)
    email_only: list[WaitlistEntry] = []

    for e in entries:
        if e.sms_opt_in and e.phone_e164:
            by_phone[e.phone_e164].append(e)
        else:
            email_only.append(e)

    fired = 0
    now = datetime.now(timezone.utc)

    for phone, group in by_phone.items():
        slots = compute_slots_remaining(db, county_id, group[0].vertical)
        for entry in group:
            if send_reactivation_sms(entry, slots, db):
                entry.notified_sms_at = now
                entry.status = "notified"
                fired += 1

    for entry in email_only:
        slots = compute_slots_remaining(db, county_id, entry.vertical)
        body = (
            f"{entry.name}, {county_id} is live! {slots} {entry.vertical} ZIPs available. "
            f"Lock yours: https://forcedactionleads.com"
        )
        try:
            from src.services.email import send_email
            send_email(entry.email, f"{county_id} is live!", body)
            entry.notified_email_at = now
            entry.status = "notified"
            fired += 1
        except Exception as e:
            logger.error("Failed to send reactivation email to entry %d: %s", entry.id, e)

    logger.info("county_live_reactivation: fired %d/%d notifications for %s", fired, len(entries), county_id)
    return {"fired": fired, "total": len(entries), "county_id": county_id}


def run_county_live_reactivation() -> None:
    settings = get_settings()
    if not settings.telnyx_sms_api_key:
        logger.warning("county_live_reactivation: SMS not configured, skipping")
        return

    with get_db_context() as db:
        launched = db.execute(
            select(ExpansionCandidate).where(ExpansionCandidate.status == "launched")
        ).scalars().all()

        for cand in launched:
            result = fire_reactivation_wave(db, cand.county_id)
            logger.info(
                "county_live_reactivation: county=%s fired=%d total=%d",
                cand.county_id, result["fired"], result["total"],
            )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    run_county_live_reactivation()