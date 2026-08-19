"""
sold_out_reactivation — Notify waitlist when a ZIP becomes available.

Fired automatically from grace_expiry when a ZipTerritory transitions out of
grace to available, and runnable by hand for a manual release. All matching
waitlist entries are notified simultaneously; first to claim wins.

Both channels are attempted per entry: SMS when the entry carries a consented
phone, email otherwise (or as well). Email is not a fallback for SMS failure —
it is the only channel most entries have, since the waitlist form makes phone
and SMS consent optional.

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
from src.services.email import send_email
from src.services.email_shell import paragraph, render_email_shell
from src.services.sms_compliance import can_send, send_sms
from src.services import telnyx_sms

logger = logging.getLogger(__name__)


def _claim_url(zip_code: str) -> str:
    return f"https://forcedactionleads.com?zip={zip_code}"


def _send_waitlist_email(entry, zip_code: str, vertical: str,
                         county_id: str, slots_competing: int, db) -> bool:
    """Email one waitlisted contractor that their ZIP just opened up."""
    subject = f"{zip_code} {vertical} just opened up"
    url = _claim_url(zip_code)
    body_text = (
        f"{entry.name},\n\n"
        f"A slot just opened for {zip_code} {vertical} in {county_id}.\n"
        f"{slots_competing} contractor(s) were waiting on this territory, and it "
        f"goes to whoever locks it first.\n\n"
        f"Lock it here: {url}\n"
    )
    inner_html = (
        paragraph(f"{entry.name},")
        + paragraph(
            f"A slot just opened for <strong>{zip_code} {vertical}</strong> in {county_id}."
        )
        + paragraph(
            f"{slots_competing} contractor(s) were waiting on this territory &mdash; "
            "it goes to whoever locks it first."
        )
    )
    body_html = render_email_shell(
        headline=f"{zip_code} {vertical} just opened up",
        subhead="First to lock it wins",
        inner_html=inner_html,
        cta_text="Lock this territory",
        cta_url=url,
        preheader=f"A slot opened for {zip_code} {vertical} in {county_id}.",
    )
    return send_email(to=entry.email, subject=subject, body_text=body_text,
                      body_html=body_html, db=db)


def reactivate_for_zip(zip_code: str, vertical: str, county_id: str) -> dict:
    """
    Notify all waiting sold_out Waitlist Entries for this (zip, vertical) tuple.
    First to claim wins; non-winners transition to status='lost' once the ZIP locks.

    SMS goes to entries with a consented phone; email goes to every entry with an
    address. An entry counts as notified if either channel was accepted, so a
    missing SMS configuration never silences the email path.
    """
    settings = get_settings()
    sms_enabled = bool(settings.telnyx_sms_api_key)
    if not sms_enabled:
        logger.warning(
            "sold_out_reactivation: TELNYX_SMS_API_KEY not configured — email only"
        )

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
        sms_count = 0
        email_count = 0

        for entry in entries:
            notified = False

            if sms_enabled and entry.phone_e164 and entry.sms_opt_in:
                if can_send(entry.phone_e164, db):
                    body = (
                        f"{entry.name}, 1 slot just opened for {zip_code} {vertical} "
                        f"in {county_id}. {slots_competing} were waiting. "
                        f"Lock it: {_claim_url(zip_code)} Reply STOP to opt out."
                    )
                    try:
                        send_sms(
                            entry.phone_e164,
                            body,
                            message_type="marketing",
                            campaign="county_live_reactivation",
                        )
                        entry.notified_sms_at = now
                        sms_count += 1
                        notified = True
                        logger.info(
                            "sold_out_reactivation: SMS sent to entry %d", entry.id
                        )
                    except Exception as e:
                        logger.error(
                            "sold_out_reactivation: SMS failed for entry %d: %s",
                            entry.id, e
                        )
                else:
                    logger.info(
                        "sold_out_reactivation: phone blocked opt-out for entry %d",
                        entry.id
                    )

            if entry.email:
                try:
                    if _send_waitlist_email(entry, zip_code, vertical, county_id,
                                            slots_competing, db):
                        entry.notified_email_at = now
                        email_count += 1
                        notified = True
                        logger.info(
                            "sold_out_reactivation: email sent to entry %d", entry.id
                        )
                except Exception as e:
                    logger.error(
                        "sold_out_reactivation: email failed for entry %d: %s",
                        entry.id, e
                    )

            if notified:
                sent_count += 1
                entry.reactivation_decision_id = decision_id
                entry.status = "notified"

        db.flush()
        logger.info(
            "sold_out_reactivation: notified %d/%d entries (sms=%d email=%d) for %s/%s/%s",
            sent_count, len(entries), sms_count, email_count,
            zip_code, vertical, county_id
        )
        return {
            "fired": sent_count,
            "total": len(entries),
            "sms": sms_count,
            "email": email_count,
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