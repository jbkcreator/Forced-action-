"""
County launch waitlist notifier — T+0 task.

Runs every 15 minutes. For every expansion_candidate with status='launched'
and waitlist_notified_at IS NULL, sends SMS (opt-in only) and email to all
waitlist_entries WHERE county_id matches, waitlist_type='coming_soon',
status='waiting'.

County-level idempotency: expansion_candidates.waitlist_notified_at is stamped
only when the entire batch completes with zero failures. Partial failures leave
the column NULL so the next cron tick retries only the un-notified entries
(notified_email_at IS NULL guard on each row).

Usage:
    python -m src.tasks.county_waitlist_notifier [--dry-run]

Cron:
    */15 * * * *   $PROJECT/scripts/cron/run.sh src.tasks.county_waitlist_notifier
"""
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import (
    County,
    CountyLaunchAudit,
    ExpansionCandidate,
    WaitlistEntry,
)
from src.services.email import send_email
from src.services.email_shell import paragraph, render_email_shell
from src.services.sms_compliance import send_sms

logger = logging.getLogger(__name__)

BATCH_SIZE = 100

_VERTICAL_LABELS: dict[str, str] = {
    "roofing": "Roofing",
    "restoration": "Restoration",
    "public_adjusters": "Public Adjuster",
    "wholesalers": "Wholesaler",
    "fix_flip": "Fix & Flip",
    "attorneys": "Attorney",
}

_SMS_TEMPLATE = (
    "Hi {name} — {county} is now live on Forced Action! "
    "Your {vertical} territory is open. "
    "Lock yours before competitors do: {url}"
)

_EMAIL_SUBJECT = "{county} is now live — your waitlist spot is ready"

_EMAIL_TEXT = """\
Hi {name},

Great news — {county} County is now live on Forced Action.

Your {vertical} territory is open and ready to claim. Lock it in before
another contractor beats you to it.

Get started here: {url}

— The Forced Action Team

(You're receiving this because you joined the waitlist for {county} County.
To unsubscribe, reply STOP.)
"""

def _email_html(ctx: dict) -> str:
    inner_html = (
        paragraph(f"Hi {ctx['name']},")
        + paragraph(
            f"Great news — <strong>{ctx['county']} County</strong> is now live on "
            "Forced Action."
        )
        + paragraph(
            f"Your <strong>{ctx['vertical']}</strong> territory is open and ready to "
            "claim. Lock it in before another contractor beats you to it."
        )
        + paragraph(
            f"You're receiving this because you joined the waitlist for "
            f"{ctx['county']} County.",
            muted=True,
        )
    )
    return render_email_shell(
        headline=f"{ctx['county']} is now live",
        subhead="Your waitlist spot is ready",
        inner_html=inner_html,
        cta_text="Claim your territory",
        cta_url=ctx["url"],
        preheader=f"{ctx['county']} County just opened — claim your territory.",
    )


def run_waitlist_notifier(dry_run: bool = False) -> dict:
    """Process all launched counties that still have un-notified waitlist entries."""
    with get_db_context() as db:
        candidates = db.execute(
            select(ExpansionCandidate).where(
                ExpansionCandidate.status == "launched",
                ExpansionCandidate.waitlist_notified_at.is_(None),
            )
        ).scalars().all()

        if not candidates:
            logger.info("[WaitlistNotifier] no pending counties")
            return {"no_pending_counties": True}

        results = []
        for candidate in candidates:
            county_display = _get_county_display(db, candidate.county_id)
            result = _notify_county(db, candidate, county_display, dry_run)
            results.append(result)
            logger.info(
                "[WaitlistNotifier] county=%s email=%d sms=%d failed=%d dry_run=%s",
                candidate.county_id,
                result["sent_email"],
                result["sent_sms"],
                result["failed"],
                dry_run,
            )

    return {"processed": results}


def _notify_county(
    db: Session,
    candidate: ExpansionCandidate,
    county_display: str,
    dry_run: bool,
) -> dict:
    county_id = candidate.county_id
    settings = get_settings()
    base_url = getattr(settings, "app_base_url", None) or "https://forcedactionleads.com"

    sent_email = 0
    sent_sms = 0
    failed = 0
    offset = 0

    while True:
        batch = db.execute(
            select(WaitlistEntry).where(
                WaitlistEntry.county_id == county_id,
                WaitlistEntry.waitlist_type == "coming_soon",
                WaitlistEntry.status == "waiting",
                WaitlistEntry.notified_email_at.is_(None),
            ).limit(BATCH_SIZE).offset(offset)
        ).scalars().all()

        if not batch:
            break

        for entry in batch:
            now = datetime.now(timezone.utc)
            vertical_label = _VERTICAL_LABELS.get(entry.vertical, entry.vertical.title())
            ctx = {
                "name": entry.name.split()[0],  # first name only
                "county": county_display,
                "vertical": vertical_label,
                "url": base_url,
            }

            # ── Email ──────────────────────────────────────────────────────
            try:
                if not dry_run:
                    send_email(
                        to=entry.email,
                        subject=_EMAIL_SUBJECT.format(**ctx),
                        body_text=_EMAIL_TEXT.format(**ctx),
                        body_html=_email_html(ctx),
                    )
                    entry.notified_email_at = now
                sent_email += 1
            except Exception as exc:
                logger.error(
                    "[WaitlistNotifier] email failed entry_id=%s email=%s: %s",
                    entry.id, entry.email, exc,
                )
                failed += 1

            # ── SMS (opt-in only) ──────────────────────────────────────────
            if entry.sms_opt_in and entry.phone_e164:
                try:
                    body = _SMS_TEMPLATE.format(**ctx)
                    if not dry_run:
                        send_sms(
                            to=entry.phone_e164,
                            body=body,
                            db=db,
                            message_type="transactional",
                            task_type="county_waitlist_launch",
                            campaign="county_launch_notification",
                        )
                        entry.notified_sms_at = now
                    sent_sms += 1
                except Exception as exc:
                    logger.error(
                        "[WaitlistNotifier] SMS failed entry_id=%s phone=%s: %s",
                        entry.id, entry.phone_e164, exc,
                    )
                    failed += 1

            if not dry_run:
                entry.status = "notified"

        if not dry_run:
            db.commit()
        offset += BATCH_SIZE

    # Stamp county-level guard only when fully clean
    if not dry_run and failed == 0:
        candidate.waitlist_notified_at = datetime.now(timezone.utc)
        db.commit()

    _write_audit(db, county_id, detail={
        "sent_email": sent_email,
        "sent_sms": sent_sms,
        "failed": failed,
        "dry_run": dry_run,
    })

    return {
        "county_id": county_id,
        "sent_email": sent_email,
        "sent_sms": sent_sms,
        "failed": failed,
    }


def _get_county_display(db: Session, county_id: str) -> str:
    row = db.execute(
        select(County.display_name).where(County.county_id == county_id)
    ).scalar_one_or_none()
    return row or county_id.replace("_", " ").title()


def _write_audit(db: Session, county_id: str, detail: Optional[dict] = None) -> None:
    row = CountyLaunchAudit(
        county_id=county_id,
        event_type="waitlist_notified",
        actor="waitlist_notifier",
        detail=detail,
    )
    db.add(row)
    db.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    result = run_waitlist_notifier(dry_run=dry)
    import json
    print(json.dumps(result, indent=2, default=str))
