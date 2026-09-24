"""
Abandonment Agent — WP-T2-5.

Manages the five-touch portal abandonment sequence. When a portal.stall
event fires (via WP-T2-4), enqueue_sequence() writes 5 rows to
abandonment_sequences with pre-calculated due_at timestamps.

A cron worker (scripts/run_abandonment_worker.py) runs every minute,
calls fire_due_touches(), and sends any rows where due_at <= now() and
the row is not yet sent or cancelled.

halt_sequence() is called by router.py when an inbound reply arrives, and
halt_for_portal_completion() when the borrower finishes the application —
it cancels all pending touches for that person so the sequence stops
immediately.

Touch schedule (from stall event):
  1 → +15 minutes
  2 → +4 hours
  3 → +24 hours
  4 → +48 hours
  5 → +72 hours

Send path: inserts directly into relay_approval_queue.
  Tier A (re-engagement of funded borrowers) → status='approved', lane='MONEY'
  Default (portal abandonment)               → status='pending',  lane='EXCEPTIONS'
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from src.services import fa_max_outbound_links
from src.services.fa_max_outbound_links import BOOKING_LINE, OutboundLinks
from src.services.fa_max_portal_completion import halt_for_portal_completion, is_portal_completed

logger = logging.getLogger(__name__)

_AGENT_NAME = "abandonment_agent"
_VENTURE_KEY = "fa_max_lending"

# Touch offsets from the stall event
_TOUCH_OFFSETS = {
    1: timedelta(minutes=15),
    2: timedelta(hours=4),
    3: timedelta(hours=24),
    4: timedelta(hours=48),
    5: timedelta(hours=72),
}

# Touch templates — stored here as defaults; can be overridden by DB rows
# in fa_max_abandonment_templates if that table exists in a future iteration.
_TOUCH_TEMPLATES = {
    1: (
        "Looks like you got pulled away — your application is still saved and takes "
        "about 3 minutes to finish. Whenever you're ready: {portal_link}"
    ),
    2: (
        "Totally understand if something came up. Your pre-qual is still open — happy "
        "to help if you hit a snag or have questions. Just reply here or finish up: {portal_link}"
    ),
    3: (
        "Wanted to follow up one more time. If you've got a deal lined up, finishing the "
        "pre-qual now means we can move quickly when you need us. Takes just a few minutes: {portal_link}"
    ),
    4: (
        "Would it help to jump on a quick call? I can walk you through the application "
        "in 10 minutes. Grab a time here: {calendar_link}\n\nOr finish the form at your "
        "own pace: {portal_link}"
    ),
    5: (
        "Last check-in from me — should I park this for now, or are you still looking "
        "to move forward? Just reply and let me know either way."
    ),
}

_COMPLIANCE_FOOTER = (
    "\n\n---\n"
    "Josh Kantor, Forced Action / 1320 W. Lemon St., Tampa, FL 33606 / "
    "(813) 361-8927 / "
    "This message is not an offer of credit. "
    "Reply STOP to opt out."
)


# ── Public API ────────────────────────────────────────────────────────────────

def enqueue_sequence(
    person_id: str,
    contact_email: Optional[str],
    db: Session,
    opportunity_id: Optional[str] = None,
    borrower_first_name: Optional[str] = None,
) -> int:
    """Write 5 touch rows to abandonment_sequences.

    Idempotent — if an active sequence already exists for this person
    (sent_at IS NULL AND cancelled_at IS NULL), skips all inserts and
    returns 0. Returns the number of rows inserted (5 on first call, 0
    on duplicate).
    """
    active = db.execute(
        text("""
            SELECT COUNT(*) FROM abandonment_sequences
            WHERE person_id = :pid
              AND sent_at IS NULL
              AND cancelled_at IS NULL
        """),
        {"pid": person_id},
    ).scalar()

    if active and active > 0:
        logger.info(
            "abandonment: person_id=%s already has %d active touch(es) — skipping enqueue",
            person_id, active,
        )
        return 0

    now = datetime.now(timezone.utc)
    inserted = 0

    for touch_number, offset in _TOUCH_OFFSETS.items():
        due_at = now + offset
        idem_key = _idem_key(person_id, touch_number, now)
        try:
            db.execute(
                text("""
                    INSERT INTO abandonment_sequences
                        (person_id, contact_email, opportunity_id, borrower_first_name,
                         touch_number, due_at, channel, idempotency_key)
                    VALUES
                        (:pid, :email, :oid, :fname,
                         :touch, :due_at, 'email', :idem)
                    ON CONFLICT (idempotency_key) DO NOTHING
                """),
                {
                    "pid": person_id,
                    "email": contact_email,
                    "oid": opportunity_id,
                    "fname": borrower_first_name,
                    "touch": touch_number,
                    "due_at": due_at,
                    "idem": idem_key,
                },
            )
            inserted += 1
        except Exception as exc:
            db.rollback()
            logger.error(
                "abandonment: failed to insert touch %d for person_id=%s — rolled back, "
                "no touches enqueued: %s",
                touch_number, person_id, exc,
            )
            return 0

    db.commit()
    logger.info(
        "abandonment: enqueued %d touches for person_id=%s first_due=%s",
        inserted, person_id, now + _TOUCH_OFFSETS[1],
    )
    return inserted


def halt_sequence(
    person_id: str,
    db: Session,
    reason: str = "reply_received",
) -> int:
    """Cancel all pending touches for a person. Returns the count cancelled."""
    try:
        result = db.execute(
            text("""
                UPDATE abandonment_sequences
                SET cancelled_at = NOW(), cancel_reason = :reason
                WHERE person_id     = :pid
                  AND sent_at       IS NULL
                  AND cancelled_at  IS NULL
            """),
            {"pid": person_id, "reason": reason},
        )
        db.commit()
        count = result.rowcount
        if count:
            logger.info(
                "abandonment: halted %d pending touch(es) for person_id=%s reason=%s",
                count, person_id, reason,
            )
        return count
    except Exception as exc:
        db.rollback()
        logger.error("abandonment: halt_sequence failed for person_id=%s: %s", person_id, exc)
        return 0


def fire_due_touches(db: Session) -> int:
    """Find all touches due now and send them. Called by the cron worker.

    Returns the number of touches fired.
    """
    rows = db.execute(
        text("""
            SELECT id, person_id, contact_email, opportunity_id,
                   borrower_first_name, touch_number, idempotency_key, created_at
            FROM abandonment_sequences
            WHERE due_at      <= NOW()
              AND sent_at      IS NULL
              AND cancelled_at IS NULL
            ORDER BY due_at
            LIMIT 50
        """),
    ).fetchall()

    if not rows:
        return 0

    fired = 0
    for row in rows:
        success = _fire_touch(
            seq_id=row[0],
            person_id=str(row[1]),
            contact_email=row[2],
            opportunity_id=str(row[3]) if row[3] else None,
            borrower_first_name=row[4],
            touch_number=row[5],
            idempotency_key=row[6],
            sequence_started_at=row[7],
            db=db,
        )
        if success:
            fired += 1

    return fired


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fire_touch(
    seq_id: int,
    person_id: str,
    contact_email: Optional[str],
    opportunity_id: Optional[str],
    borrower_first_name: Optional[str],
    touch_number: int,
    idempotency_key: str,
    sequence_started_at: datetime,
    db: Session,
) -> bool:
    if not contact_email:
        logger.warning(
            "abandonment: touch %d for person_id=%s has no email — cancelling",
            touch_number, person_id,
        )
        _cancel_touch(seq_id, "no_contact_email", db)
        return False

    if _is_suppressed(person_id, db):
        logger.info(
            "abandonment: person_id=%s is suppressed — cancelling touch %d",
            person_id, touch_number,
        )
        halt_sequence(person_id, db, reason="suppressed")
        return False

    if is_portal_completed(db, person_id, opportunity_id, sequence_started_at):
        logger.info(
            "abandonment: person_id=%s completed the portal — halting before touch %d",
            person_id, touch_number,
        )
        halt_sequence(person_id, db, reason="portal_completed")
        return False

    links = fa_max_outbound_links.resolve_or_alert(
        db, agent_name=_AGENT_NAME, person_id=person_id, opportunity_id=opportunity_id,
    )
    if links is None:
        _cancel_touch(seq_id, "link_unresolved", db)
        return False

    body = _render_template(touch_number, borrower_first_name, links)
    subject = _touch_subject(touch_number)

    relay_idem = f"abandonment:touch:{idempotency_key}"
    try:
        result = db.execute(
            text("""
                INSERT INTO relay_approval_queue
                    (idempotency_key, venture_key, lane, channel, recipient,
                     payload, status, agent_name, autonomy_tier_at_send, person_id)
                VALUES
                    (:idem, :vk, 'EXCEPTIONS', 'email', :recipient,
                     CAST(:payload AS JSONB), 'pending', :agent, 'B', :pid)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
            """),
            {
                "idem": relay_idem,
                "vk": _VENTURE_KEY,
                "recipient": contact_email,
                "payload": json.dumps({
                    "type": "abandonment_touch",
                    "touch_number": touch_number,
                    "subject": subject,
                    "body": body,
                    "opportunity_id": opportunity_id,
                }),
                "agent": _AGENT_NAME,
                "pid": person_id,
            },
        ).fetchone()

        db.execute(
            text("""
                UPDATE abandonment_sequences
                SET sent_at = NOW()
                WHERE id = :id
            """),
            {"id": seq_id},
        )
        db.commit()
        queue_id = result[0] if result else None
        logger.info(
            "abandonment: fired touch %d for person_id=%s relay_id=%s",
            touch_number, person_id, queue_id,
        )
        return True

    except Exception as exc:
        db.rollback()
        logger.error(
            "abandonment: failed to fire touch %d for person_id=%s: %s",
            touch_number, person_id, exc,
        )
        return False


def _cancel_touch(seq_id: int, reason: str, db: Session) -> None:
    try:
        db.execute(
            text("""
                UPDATE abandonment_sequences
                SET cancelled_at = NOW(), cancel_reason = :reason
                WHERE id = :id
            """),
            {"id": seq_id, "reason": reason},
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("abandonment: _cancel_touch id=%d failed: %s", seq_id, exc)


def _is_suppressed(person_id: str, db: Session) -> bool:
    row = db.execute(
        text("""
            SELECT lifecycle_state FROM fa_max_persons
            WHERE person_id = :pid
        """),
        {"pid": person_id},
    ).fetchone()
    if not row:
        return False
    return row[0] in ("suppressed", "do_not_contact", "dead")


def _render_template(
    touch_number: int, borrower_first_name: Optional[str], links: OutboundLinks,
) -> str:
    template = _TOUCH_TEMPLATES.get(touch_number, "")
    if "{calendar_link}" not in template:
        template += "\n\n" + BOOKING_LINE
    name = borrower_first_name or "there"
    body = f"Hey {name},\n\n" + template.format(
        portal_link=links.portal_url,
        calendar_link=links.calendar_url,
    )
    return body + _COMPLIANCE_FOOTER


def _touch_subject(touch_number: int) -> str:
    subjects = {
        1: "Still have a few minutes? Your application is saved",
        2: "Happy to help if something came up",
        3: "Quick follow-up on your application",
        4: "Want to jump on a quick call?",
        5: "Should I park this for now?",
    }
    return subjects.get(touch_number, "Following up on your application")


def _idem_key(person_id: str, touch_number: int, enqueue_time: datetime) -> str:
    raw = f"abandonment:{person_id}:{touch_number}:{enqueue_time.date().isoformat()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:40]
