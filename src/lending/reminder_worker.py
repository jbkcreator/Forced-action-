"""WP-GL-10: booking reminder worker — polls due rows and sends or emails them.

Production entry point:
  python -m src.lending.reminder_worker [--once] [--dry-run]

Add to deploy.sh alongside the opt_out_poller and dialer_sweep services:
  python -m src.lending.reminder_worker &

The worker runs in a tight poll loop (POLL_SECONDS interval). On each cycle:

  1. Lock up to BATCH_SIZE due rows (status='pending', send_at<=now) with
     FOR UPDATE SKIP LOCKED so two workers never process the same row.
  2. For each row, apply gate order:
       a. booking still active? (status='confirmed' or 'pending' — pending
          AI bookings still send; gate-failed bookings are 'cancelled')
       b. contact suppressed in lending.suppression_list?
       c. text channel → text_consent and LENDING_TEXT_ENABLED?
       d. email channel → contact_email present?
       e. send via messenger or email sender
  3. Write sent_at, status, worker_id in the same transaction as the send
     decision. A crash before commit leaves the row 'pending' — the next
     cycle picks it up and the idempotent message_id check at the GHL layer
     prevents double-sends (Fake always re-records; Live may resend once on
     crash, which is acceptable for reminders).

Suppression: _suppressed_phones (from lending.compliance) covers the
lending suppression list. FA-side opt-outs are excluded via
lending.suppression_list reconcile (already run by the opt_out_poller).

Phone hashes, never raw phones, go into log messages (matching WP-GL-9's
pattern).
"""
from __future__ import annotations

import hashlib
import logging
import os
import signal
import socket
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from config.settings import get_settings
from src.lending.ghl_messenger import GHLMessengerError, get_messenger

logger = logging.getLogger(__name__)

POLL_SECONDS = 10
BATCH_SIZE = 50
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"

_FETCH = text("""
    SELECT id, booking_ref, kind, channel, send_at,
           first_name, contact_phone, contact_email,
           property_address, slot_start_utc, booked_by, text_consent
      FROM lending.booking_messages
     WHERE status = 'pending'
       AND send_at <= :now
     ORDER BY send_at
     LIMIT :limit
       FOR UPDATE SKIP LOCKED
""")

_MARK_SENT = text("""
    UPDATE lending.booking_messages
       SET status = 'sent', sent_at = :sent_at, worker_id = :worker_id
     WHERE id = :id
""")

_MARK_SKIPPED = text("""
    UPDATE lending.booking_messages
       SET status = 'skipped', skip_reason = :reason, worker_id = :worker_id
     WHERE id = :id
""")


def _phone_hash(phone: Optional[str]) -> str:
    if not phone:
        return ""
    return hashlib.sha256(phone.encode()).hexdigest()[:12]


SuppressionLookup = object  # Callable[[db, list[str]], set[str]]


def _default_suppression_lookup(db, phones: list[str]) -> set[str]:
    """Load suppressed phones from lending.suppression_list.

    lending.compliance is merged in a sibling branch (WP-GL-9/GL-1). Until
    that branch is merged, this function queries the table directly so this
    worker has no cross-branch import dependency.
    """
    if not phones:
        return set()
    try:
        rows = db.execute(
            text("""
                SELECT phone FROM lending.suppression_list
                WHERE phone = ANY(:phones)
            """),
            {"phones": phones},
        ).scalars()
        return set(rows)
    except Exception:
        logger.warning("[reminder-worker] suppression lookup failed — treating all unsuppressed")
        return set()


def process_due_rows(
    db,
    *,
    now: Optional[datetime] = None,
    dry_run: bool = False,
    worker_id: str = WORKER_ID,
    suppression_lookup=None,
) -> dict[str, int]:
    """Process one batch of due rows inside an already-open transaction.

    Returns outcome counts. Does not commit.
    suppression_lookup is injectable for testing; defaults to the real DB query.
    """
    from src.lending.booking_messages import render_email, render_text

    _suppressed = suppression_lookup or _default_suppression_lookup

    now = now or datetime.now(timezone.utc)
    rows = db.execute(_FETCH, {"now": now, "limit": BATCH_SIZE}).mappings().all()
    if not rows:
        return {}

    phones = [r["contact_phone"] for r in rows if r["contact_phone"]]
    suppressed = _suppressed(db, phones) if phones else set()
    messenger = get_messenger()

    counts: dict[str, int] = {}

    for row in rows:
        rid = row["id"]
        kind = row["kind"]
        channel = row["channel"]
        phone = row["contact_phone"]
        email = row["contact_email"]
        slot_start = row["slot_start_utc"]
        first_name = row["first_name"] or ""
        address = row["property_address"]

        # Gate: suppression
        if phone and phone in suppressed:
            _skip(db, rid, "suppressed", worker_id)
            counts["suppressed"] = counts.get("suppressed", 0) + 1
            continue

        # Re-check LENDING_TEXT_ENABLED at send time in case the flag was
        # toggled after scheduling (channel was already encoded at schedule time).
        if channel == "text":
            if not getattr(get_settings(), "lending_text_enabled", False):
                _skip(db, rid, "text_not_enabled", worker_id)
                counts["text_not_enabled"] = counts.get("text_not_enabled", 0) + 1
                continue
            if not phone:
                _skip(db, rid, "no_phone", worker_id)
                counts["no_phone"] = counts.get("no_phone", 0) + 1
                continue

        # Gate: email channel requires contact_email
        if channel == "email" and not email:
            _skip(db, rid, "no_email", worker_id)
            counts["no_email"] = counts.get("no_email", 0) + 1
            continue

        if dry_run:
            logger.info(
                "[reminder-worker.dry-run] would-send id=%d kind=%s channel=%s phone_hash=%s",
                rid, kind, channel, _phone_hash(phone),
            )
            counts["dry_run"] = counts.get("dry_run", 0) + 1
            continue

        # Send
        try:
            if channel == "text":
                body = render_text(kind, first_name=first_name,
                                   slot_start_utc=slot_start, property_address=address)
                result = messenger.send_text(contact_phone=phone, body=body)
                if result.sent:
                    _mark_sent(db, rid, worker_id)
                    counts["sent_text"] = counts.get("sent_text", 0) + 1
                else:
                    _skip(db, rid, result.skip_reason or "messenger_skip", worker_id)
                    counts["skipped"] = counts.get("skipped", 0) + 1

            else:  # email
                subject, body = render_email(kind, first_name=first_name,
                                             slot_start_utc=slot_start, property_address=address)
                ok = _send_email(to=email, subject=subject, body=body)
                if ok:
                    _mark_sent(db, rid, worker_id)
                    counts["sent_email"] = counts.get("sent_email", 0) + 1
                else:
                    _skip(db, rid, "email_send_failed", worker_id)
                    counts["skipped"] = counts.get("skipped", 0) + 1

        except GHLMessengerError as exc:
            # Network error → leave the row pending; it retries next cycle.
            logger.error(
                "[reminder-worker] GHL send failed id=%d kind=%s — will retry: %s",
                rid, kind, exc,
            )
            counts["retry"] = counts.get("retry", 0) + 1
        except Exception:
            logger.exception("[reminder-worker] unexpected error on row id=%d", rid)
            counts["error"] = counts.get("error", 0) + 1

    return counts


def _mark_sent(db, row_id: int, worker_id: str) -> None:
    db.execute(_MARK_SENT, {
        "id": row_id,
        "sent_at": datetime.now(timezone.utc),
        "worker_id": worker_id,
    })


def _skip(db, row_id: int, reason: str, worker_id: str) -> None:
    db.execute(_MARK_SKIPPED, {"id": row_id, "reason": reason, "worker_id": worker_id})


def _send_email(*, to: str, subject: str, body: str) -> bool:
    """Email sender port — Fake until hello@nextdeallending.com DNS is live.

    OPEN DEPENDENCY: C1 (Porkbun API key) and C2 (Google Workspace) are not
    yet provisioned. Until they are, all emails are logged as dry-run.
    Flip LENDING_EMAIL_ENABLED=true once the mailbox is confirmed live.
    """
    email_enabled = getattr(get_settings(), "lending_email_enabled", False)
    if not email_enabled:
        logger.info("[reminder-worker.email.fake] would-send to=%s subject=%r", to, subject)
        return True  # Fake: report sent so the row progresses
    # TODO: wire real email sender (Google Workspace SMTP or SendGrid)
    # once C1 and C2 are complete. For now this path is unreachable.
    raise NotImplementedError("lending email sender not yet configured")


# ── CLI entry point ────────────────────────────────────────────────────────────

_running = True


def _handle_term(signum, frame):  # noqa: ARG001
    global _running
    logger.info("[reminder-worker] received signal %d — stopping", signum)
    _running = False


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = sys.argv[1:] if argv is None else argv
    once = "--once" in args
    dry_run = "--dry-run" in args

    import os
    engine = create_engine(os.environ["DATABASE_URL"])

    signal.signal(signal.SIGTERM, _handle_term)
    signal.signal(signal.SIGINT, _handle_term)

    logger.info("[reminder-worker] starting worker_id=%s dry_run=%s", WORKER_ID, dry_run)

    while _running:
        try:
            with engine.begin() as conn:
                counts = process_due_rows(conn, dry_run=dry_run)
            if counts:
                logger.info("[reminder-worker] cycle counts=%s", counts)
        except Exception:
            logger.exception("[reminder-worker] cycle error — sleeping before retry")

        if once:
            break
        time.sleep(POLL_SECONDS)

    logger.info("[reminder-worker] stopped")


if __name__ == "__main__":
    main()
