"""WP-GL-10: send the due booking confirmations and reminders.

    python -m src.lending.reminder_worker            # loop
    python -m src.lending.reminder_worker --once     # one cycle and exit

Each cycle claims due ``lending.booking_messages`` rows (``FOR UPDATE SKIP LOCKED``, claim committed
before any send) and decides each row once. Gates, in order: the call has not started -> the contact
is not suppressed -> a channel exists (text needs the live consent evidence of WP-GL-9's
``has_text_consent``; otherwise email) -> the channel is switched on and configured -> the text window
(8am-8pm ET). Texts go out only through GoHighLevel (``ghl_sms.GhlSmsSender``, single attempt).

A text is never re-sent when its outcome is unknown: a row left in ``sending`` (crash mid-send) or an
ambiguous GHL error ends as ``send_unknown``. Only a failure that provably sent nothing is retried.
Logs carry ids and phone hashes, never phone numbers or message bodies.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Protocol

from sqlalchemy import text

from config.lending_reminders import (
    BATCH_SIZE,
    CHANNEL_EMAIL,
    CHANNEL_TEXT,
    KIND_NIGHT_BEFORE,
    KIND_NINETY_MIN,
    MAX_SEND_ATTEMPTS,
    NINETY_MIN_MAX_LATE_SECONDS,
    POLL_SECONDS,
    RETRY_DELAY_SECONDS,
    STALE_SEND_SECONDS,
    TIMEZONE,
)
from config.settings import get_settings
from src.lending.booking_messages import next_text_window, render_email, render_text, text_window_open
from src.lending.compliance import phone_hash
from src.lending.consent import has_text_consent
from src.lending.db import lending_session
from src.lending.ghl_email import get_email_sender
from src.lending.ghl_sms import GhlSmsError, get_sender, texting_number

logger = logging.getLogger(__name__)



class TextSender(Protocol):
    """The text-sending port: ``GhlSmsSender`` in production, ``FakeTextSender`` in tests and sandboxes."""

    def __call__(self, phone: str, body: str, first_name: Optional[str], *, deadline: datetime) -> str: ...


class FakeTextSender:
    """Records what would have been sent; raises ``error`` instead when given one. Never touches the network."""

    def __init__(self, error: Optional[Exception] = None) -> None:
        self.sent: list[tuple[str, str]] = []
        self.error = error

    def __call__(self, phone: str, body: str, first_name: Optional[str], *, deadline: datetime) -> str:
        if self.error is not None:
            raise self.error
        self.sent.append((phone, body))
        return f"fake-{len(self.sent)}"


class EmailSender(Protocol):
    """The email-sending port: ``GhlEmailSender`` in production. Raises ``GhlSmsError`` like the text sender."""

    def __call__(self, to: str, subject: str, body: str, *, phone: Optional[str], first_name: Optional[str],
                 deadline: datetime) -> str: ...

_COLUMNS = ("id, booking_ref, kind, send_at, first_name, contact_phone, contact_email, "
            "property_address, slot_start_utc, attempts")

_CLAIM = text(f"""
    WITH picked AS (
        SELECT id FROM lending.booking_messages
         WHERE status = 'pending' AND send_at <= :now
         ORDER BY send_at LIMIT :limit FOR UPDATE SKIP LOCKED)
    UPDATE lending.booking_messages m
       SET status = 'sending', decided_at = now(), attempts = m.attempts + 1
      FROM picked WHERE m.id = picked.id
    RETURNING m.id, m.booking_ref, m.kind, m.send_at, m.first_name, m.contact_phone, m.contact_email,
              m.property_address, m.slot_start_utc, m.attempts
""")

_STALE = text("""
    UPDATE lending.booking_messages SET status = 'send_unknown', skip_reason = 'stale_claim', decided_at = now()
     WHERE status = 'sending' AND decided_at < :cutoff
""")

_RECORD = text("""
    UPDATE lending.booking_messages
       SET status = :status, skip_reason = :reason, channel = COALESCE(:channel, channel),
           provider_message_id = COALESCE(:message_id, provider_message_id), decided_at = now(),
           sent_at = CASE WHEN :status = 'sent' THEN now() ELSE sent_at END
     WHERE id = :id AND status = 'sending'
""")

_REQUEUE = text("""
    UPDATE lending.booking_messages
       SET status = 'pending', send_at = :send_at, attempts = :attempts, skip_reason = :reason
     WHERE id = :id AND status = 'sending'
""")

_SUPPRESSED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.suppression_list WHERE (phone = :p AND :p IS NOT NULL)
                                                              OR (email = :e AND :e IS NOT NULL))
        OR EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND :p IS NOT NULL AND do_not_contact)
""")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_suppressed(db, phone: Optional[str], email: Optional[str]) -> bool:
    return bool(db.execute(_SUPPRESSED, {"p": phone, "e": (email or "").lower() or None}).scalar())


def _record(db, row_id: int, status: str, reason: Optional[str] = None, channel: Optional[str] = None,
            message_id: Optional[str] = None) -> None:
    db.execute(_RECORD, {"id": row_id, "status": status, "reason": reason, "channel": channel,
                         "message_id": message_id})


def _requeue(db, row: Mapping[str, Any], send_at: datetime, reason: str, *, count_attempt: bool) -> None:
    attempts = row["attempts"] if count_attempt else row["attempts"] - 1
    db.execute(_REQUEUE, {"id": row["id"], "send_at": send_at, "attempts": attempts, "reason": reason})


def _decide(db, row: Mapping[str, Any], *, now: datetime, text_sender: Optional[TextSender],
            email_sender: Optional[EmailSender], text_enabled: bool, email_enabled: bool,
            number: Optional[str], started: list[bool]) -> str:
    """Run the gates and, when they pass, the send. Records the outcome; returns its label."""
    rid, phone, email, kind = row["id"], row["contact_phone"], row["contact_email"], row["kind"]
    slot = row["slot_start_utc"]
    if slot <= now:
        _record(db, rid, "skipped", "call_started")
        return "skipped_call_started"
    if _too_late(row, now):
        _record(db, rid, "skipped", "too_late")
        return "skipped_too_late"
    if _is_suppressed(db, phone, email):
        _record(db, rid, "skipped", "suppressed")
        return "skipped_suppressed"

    if phone and has_text_consent(db, phone):
        return _send_text(db, row, now=now, sender=text_sender, enabled=text_enabled, number=number, started=started)
    if email:
        return _send_email(db, row, now=now, sender=email_sender, enabled=email_enabled, number=number, started=started)
    _record(db, rid, "skipped", "no_consent")
    return "skipped_no_consent"


def _too_late(row: Mapping[str, Any], now: datetime) -> bool:
    """True when sending now would make the reminder's wording false (see config.lending_reminders)."""
    if row["kind"] == KIND_NIGHT_BEFORE:
        return now.astimezone(TIMEZONE).date() >= row["slot_start_utc"].astimezone(TIMEZONE).date()
    if row["kind"] == KIND_NINETY_MIN:
        return (now - row["send_at"]).total_seconds() > NINETY_MIN_MAX_LATE_SECONDS
    return False


def _send_text(db, row: Mapping[str, Any], *, now: datetime, sender: Optional[TextSender], enabled: bool,
               number: Optional[str], started: list[bool]) -> str:
    rid, kind, slot = row["id"], row["kind"], row["slot_start_utc"]
    if not enabled:
        _record(db, rid, "skipped", "text_not_enabled", CHANNEL_TEXT)
        return "skipped_text_not_enabled"
    if sender is None or not number:
        _record(db, rid, "skipped", "not_configured", CHANNEL_TEXT)
        return "skipped_not_configured"
    if not text_window_open(now):
        # The 90-minute text promises a timing, so it is never delayed; the others wait for the window.
        opens = next_text_window(now)
        same_day_as_call = opens.astimezone(TIMEZONE).date() >= slot.astimezone(TIMEZONE).date()
        if kind == KIND_NINETY_MIN or opens >= slot or (kind == KIND_NIGHT_BEFORE and same_day_as_call):
            _record(db, rid, "skipped", "quiet_hours", CHANNEL_TEXT)
            return "skipped_quiet_hours"
        _requeue(db, row, opens, "deferred_quiet_hours", count_attempt=False)
        return "deferred_quiet_hours"
    body = render_text(kind, first_name=row["first_name"], slot_start_utc=slot, property_address=row["property_address"], number=number)
    started[0] = True
    try:
        message_id = sender(row["contact_phone"], body, row["first_name"], deadline=slot)
    except GhlSmsError as exc:
        if exc.ambiguous:
            _record(db, rid, "send_unknown", "ambiguous_send_error", CHANNEL_TEXT)
            return "send_unknown"
        if row["attempts"] >= MAX_SEND_ATTEMPTS:
            _record(db, rid, "failed", "send_failed", CHANNEL_TEXT)
            return "failed"
        _requeue(db, row, now + timedelta(seconds=RETRY_DELAY_SECONDS), "send_failed_retry", count_attempt=True)
        return "retry"
    _record(db, rid, "sent", None, CHANNEL_TEXT, message_id)
    return "sent"


def _send_email(db, row: Mapping[str, Any], *, now: datetime, sender: Optional[EmailSender], enabled: bool,
                number: Optional[str], started: list[bool]) -> str:
    rid = row["id"]
    if not enabled:
        _record(db, rid, "skipped", "email_not_enabled", CHANNEL_EMAIL)
        return "skipped_email_not_enabled"
    if sender is None or not number:
        _record(db, rid, "skipped", "not_configured", CHANNEL_EMAIL)
        return "skipped_not_configured"
    subject, body = render_email(row["kind"], first_name=row["first_name"], slot_start_utc=row["slot_start_utc"],
                                 property_address=row["property_address"], number=number)
    started[0] = True
    try:
        message_id = sender(row["contact_email"], subject, body, phone=row["contact_phone"],
                            first_name=row["first_name"], deadline=row["slot_start_utc"])
    except GhlSmsError as exc:
        if exc.ambiguous:
            _record(db, rid, "send_unknown", "ambiguous_send_error", CHANNEL_EMAIL)
            return "send_unknown"
        if row["attempts"] >= MAX_SEND_ATTEMPTS:
            _record(db, rid, "failed", "send_failed", CHANNEL_EMAIL)
            return "failed"
        _requeue(db, row, now + timedelta(seconds=RETRY_DELAY_SECONDS), "send_failed_retry", count_attempt=True)
        return "retry"
    _record(db, rid, "sent", None, CHANNEL_EMAIL, message_id)
    return "sent"


def process_due(db, *, text_sender: Optional[TextSender], email_sender: Optional[EmailSender] = None,
                text_enabled: bool, email_enabled: bool, number: Optional[str] = None,
                now: Optional[datetime] = None, limit: int = BATCH_SIZE, clock: Callable[[], datetime] = _utcnow) -> dict[str, int]:
    """One cycle. Commits: the claim first (so a crash cannot make another worker send the same row),
    then each row's outcome on its own."""
    db.execute(_STALE, {"cutoff": (now or clock()) - timedelta(seconds=STALE_SEND_SECONDS)})
    rows = db.execute(_CLAIM, {"now": now or clock(), "limit": limit}).mappings().all()
    db.commit()
    counts: dict[str, int] = {}
    for row in rows:
        started = [False]
        try:
            outcome = _decide(db, row, now=now or clock(), text_sender=text_sender, email_sender=email_sender,
                              text_enabled=text_enabled, email_enabled=email_enabled, number=number, started=started)
            db.commit()
        except Exception as exc:  # class only: SQL / HTTP errors can embed phone numbers
            logger.error("[reminder-worker] row %s crashed (%s)", row["id"], type(exc).__name__)
            db.rollback()
            if started[0]:
                continue  # outcome unknown: the stale sweep closes it as send_unknown, never resent
            _fail_unsent(db, row["id"])
            outcome = "failed"
        counts[outcome] = counts.get(outcome, 0) + 1
        level = logging.WARNING if outcome == "skipped_suppressed" else logging.INFO
        logger.log(level, "[reminder-worker] row=%s kind=%s phone_hash=%s outcome=%s", row["id"], row["kind"],
                   phone_hash(row["contact_phone"])[:12] if row["contact_phone"] else "-", outcome)
    return counts


def _fail_unsent(db, row_id: int) -> None:
    """After an unexpected error BEFORE any send was attempted nothing went out: record 'failed'."""
    try:
        _record(db, row_id, "failed", "internal_error")
        db.commit()
    except Exception as exc:
        logger.error("[reminder-worker] row %s could not be marked failed (%s)", row_id, type(exc).__name__)
        db.rollback()


def run_cycle(*, now: Optional[datetime] = None) -> dict[str, int]:
    """One pass with real settings and its own session."""
    settings = get_settings()
    with lending_session() as db:
        return process_due(db, text_sender=get_sender(), email_sender=get_email_sender(),
                           text_enabled=settings.booking_reminder_text_enabled,
                           email_enabled=settings.booking_reminder_email_enabled, number=texting_number(), now=now)


_running = True


def _stop(signum, _frame) -> None:
    global _running
    logger.info("[reminder-worker] signal %s received, stopping after this cycle", signum)
    _running = False


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args(argv)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    logger.info("[reminder-worker] starting (text=%s email=%s)", get_settings().booking_reminder_text_enabled,
                get_settings().booking_reminder_email_enabled)
    while _running:
        try:
            counts = run_cycle()
            if counts:
                logger.info("[reminder-worker] cycle %s", counts)
        except Exception as exc:  # class only
            logger.error("[reminder-worker] cycle failed (%s); retrying next interval", type(exc).__name__)
        if args.once:
            break
        time.sleep(POLL_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
