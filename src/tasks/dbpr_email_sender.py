"""
DBPR outbound email sender.

Queries enriched DBPR contractor contacts that haven't been emailed yet,
sends each a personalised signup email with a vertical-specific CTA, and
marks email_status='sent'. Contacts who have already signed up (subscriber_id
is set) are skipped — no need to re-acquire them.

Rate limiting: configurable per-email delay (default 1s) to stay within SMTP
provider limits. Run in small batches; re-running is safe (email_status guard
is idempotent).

Run:
    python -m src.tasks.dbpr_email_sender --dry-run
    python -m src.tasks.dbpr_email_sender --limit 50
    python -m src.tasks.dbpr_email_sender --county-id pinellas --limit 100

CLI args:
    --limit N         Max emails to send in this run (default: 100)
    --county-id ID    County to target (default: hillsborough)
    --delay SECONDS   Sleep between sends (default: 1.0)
    --dry-run         Log what would be sent without calling SMTP or writing DB
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import DBPRContact
from src.services.dbpr_email_template import render_subject, render_text, render_html
from src.services.email import send_email
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_DEFAULT_LIMIT = 100
_DEFAULT_DELAY = 1.0   # seconds between sends


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_dbpr_email_sender(
    county_id: str = "hillsborough",
    limit: int = _DEFAULT_LIMIT,
    delay: float = _DEFAULT_DELAY,
    dry_run: bool = False,
) -> dict:
    """
    Send outbound signup emails to enriched DBPR contacts.

    Targets contacts where:
      - enrichment_status = 'enriched'
      - email IS NOT NULL
      - email_status = 'not_sent'
      - subscriber_id IS NULL  (not already signed up)
      - county_id matches

    Returns stats dict.
    """
    settings = get_settings()
    stats = {
        "county_id": county_id,
        "total":     0,
        "sent":      0,
        "skipped":   0,
        "failed":    0,
    }

    if not all([settings.smtp_host, settings.smtp_user, settings.smtp_pass]):
        logger.warning("[DBPREmail] SMTP not configured — aborting")
        return stats

    with get_db_context() as db:
        candidates = (
            db.query(DBPRContact)
            .filter(
                DBPRContact.county_id == county_id,
                DBPRContact.enrichment_status == "enriched",
                DBPRContact.email.isnot(None),
                DBPRContact.email_status == "not_sent",
                DBPRContact.subscriber_id.is_(None),
            )
            .order_by(DBPRContact.created_at.asc())
            .limit(limit)
            .all()
        )

    if not candidates:
        logger.info("[DBPREmail] No unsent enriched contacts for county=%s", county_id)
        return stats

    stats["total"] = len(candidates)
    logger.info("[DBPREmail] %d contacts to email (county=%s, dry_run=%s)",
                len(candidates), county_id, dry_run)

    now = datetime.now(timezone.utc)

    for i, contact in enumerate(candidates):
        email   = (contact.email or "").strip().lower()
        vertical = contact.vertical or "general"

        if not email or "@" not in email:
            logger.warning("[DBPREmail] contact_id=%d has invalid email '%s' — skipping",
                           contact.id, email)
            stats["skipped"] += 1
            continue

        subject    = render_subject(contact.full_name, vertical, county_id)
        body_text  = render_text(contact.full_name, vertical, county_id, email)
        body_html  = render_html(contact.full_name, vertical, county_id, email)

        if dry_run:
            logger.info("[DBPREmail DRY RUN] Would send to %s | subject: %s", email, subject)
            stats["sent"] += 1
            continue

        ok = send_email(
            to=email,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
        )

        with get_db_context() as db:
            c = db.get(DBPRContact, contact.id)
            if c:
                c.email_status  = "sent" if ok else "not_sent"
                c.email_sent_at = now if ok else None
                c.updated_at    = now
                db.add(c)

        if ok:
            stats["sent"] += 1
            logger.info("[DBPREmail] Sent (%d/%d) → %s | %s",
                        i + 1, len(candidates), email, subject)
        else:
            stats["failed"] += 1
            logger.warning("[DBPREmail] Send failed → %s", email)

        # Rate-limit delay (skip after the last email)
        if i < len(candidates) - 1:
            time.sleep(delay)

    logger.info(
        "[DBPREmail] Done. sent=%d failed=%d skipped=%d / total=%d",
        stats["sent"], stats["failed"], stats["skipped"], stats["total"],
    )
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DBPR outbound email sender")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--limit",  type=int,   default=_DEFAULT_LIMIT,
                        help="Max emails to send (default: 100)")
    parser.add_argument("--delay",  type=float, default=_DEFAULT_DELAY,
                        help="Seconds between sends (default: 1.0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log without sending or writing DB")
    args = parser.parse_args()

    try:
        result = run_dbpr_email_sender(
            county_id=args.county_id,
            limit=args.limit,
            delay=args.delay,
            dry_run=args.dry_run,
        )
        print(f"  Sent    : {result['sent']}")
        print(f"  Failed  : {result['failed']}")
        print(f"  Skipped : {result['skipped']}")
        print(f"  Total   : {result['total']}")
        sys.exit(0 if result["failed"] == 0 else 1)
    except Exception as e:
        logger.error("[DBPREmail] Sender crashed: %s", e)
        sys.exit(1)
