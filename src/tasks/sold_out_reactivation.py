"""
sold_out_reactivation — Notify waitlist when a ZIP becomes available.

Triggered when a ZipTerritory transitions from locked/grace to available.
All matching waitlist entries are notified simultaneously; first to claim wins.

Run:
    python -m src.tasks.sold_out_reactivation --zip-code 33701 --vertical roofing --county-id pinellas
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import WaitlistEntry
from src.services.sms_compliance import can_send, send_sms
from src.services import telnyx_sms

logger = logging.getLogger(__name__)


def reactivate_for_zip(zip_code: str, vertical: str, county_id: str) -> dict:
    """
    Notify all waiting sold_out Waitlist Entries for this (zip, vertical) tuple.
    First to claim wins; non-winners transition to status='lost' once the ZIP locks.
    """
    settings = get_settings()
    if not settings.telnyx_sms_api_key:
        logger.warning("sold_out_reactivation: TELNYX_SMS_API_KEY not configured")
        return {"skipped": True, "reason": "sms_not_configured"}

    decision_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    with get_db_context() as db:
        entries = db.execute(
            select(WaitlistEntry).where(
                WaitlistEntry.zip_code == zip_code,
                WaitlistEntry.vertical == vertical,
                WaitlistEntry.county_id == county_id,
                WaitlistEntry.waitlist_type == "sold_out",
                WaitlistEntry.status == "waiting",
            )
        ).scalars().all()

        if not entries:
            logger.info(
                "sold_out_reactivation: no waitlist entries for %s/%s/%s",
                zip_code, vertical, county_id
            )
            return {"fired": 0, "zip_code": zip_code, "vertical": vertical, "county_id": county_id}

        slots_competing = len(entries)
        sent_count = 0

        for entry in entries:
            if not entry.phone_e164 or not entry.sms_opt_in:
                continue

            if not can_send(entry.phone_e164, db):
                logger.info(
                    "sold_out_reactivation: phone blocked opt-out for entry %d",
                    entry.id
                )
                continue

            body = (
                f"{entry.name}, 1 slot just opened for {zip_code} {vertical} in {county_id}. "
                f"{slots_competing} were waiting. Lock it: https://forcedaction.io?zip={zip_code} "
                f"Reply STOP to opt out."
            )

            try:
                send_sms(
                    entry.phone_e164,
                    body,
                    message_type="marketing",
                    campaign="county_live_reactivation",
                )
                sent_count += 1
                entry.notified_sms_at = now
                entry.reactivation_decision_id = decision_id
                entry.status = "notified"
                logger.info(
                    "sold_out_reactivation: sent to entry %d (phone=%s)",
                    entry.id, entry.phone_e164
                )
            except Exception as e:
                logger.error(
                    "sold_out_reactivation: send failed for entry %d: %s",
                    entry.id, e
                )

        db.flush()
        logger.info(
            "sold_out_reactivation: sent %d/%d notifications for %s/%s/%s",
            sent_count, len(entries), zip_code, vertical, county_id
        )
        return {
            "fired": sent_count,
            "total": len(entries),
            "county_id": county_id,
            "zip_code": zip_code,
            "vertical": vertical,
            "decision_id": decision_id,
        }


def mark_sold_out_losers(zip_code: str, vertical: str, county_id: str,
                         decision_id: str | None = None) -> int:
    """
    After a ZIP re-locks, transition 'notified' sold_out entries to 'lost'.
    If decision_id given, scope to that wave only; else mark all notified entries
    for this (zip, vertical, county) — used when called from the lock path.
    """
    with get_db_context() as db:
        conditions = [
            WaitlistEntry.zip_code == zip_code,
            WaitlistEntry.vertical == vertical,
            WaitlistEntry.county_id == county_id,
            WaitlistEntry.waitlist_type == "sold_out",
            WaitlistEntry.status == "notified",
        ]
        if decision_id:
            conditions.append(WaitlistEntry.reactivation_decision_id == decision_id)
        result = db.execute(
            update(WaitlistEntry).where(*conditions).values(status="lost")
        )
        db.commit()
        return result.rowcount


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="sold_out reactivation runner")
    parser.add_argument("--zip-code", required=True, help="5-digit ZIP code")
    parser.add_argument("--vertical", required=True, help="Vertical (e.g., roofing)")
    parser.add_argument("--county-id", required=True, help="County ID")
    args = parser.parse_args()

    result = reactivate_for_zip(args.zip_code, args.vertical, args.county_id)
    print(f"Result: {result}")