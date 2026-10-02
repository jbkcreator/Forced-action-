"""WP-GL-9 missed-call text-back (GoHighLevel).

Consumes ``lending.missed_call_events`` rows that the single BatchDialer CDR poller queues for an
unanswered outbound lending call. Each event gets exactly one decision: sent, or why not. Texts
go out only through GHL, only to consented numbers, once per Eastern day, within 60 seconds of
the call. Logs carry phone hashes, never phones, names or message bodies.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_missed_call import MAX_LATE_SECONDS, TIMEZONE
from config.lending_text_back import (
    CAPS, ENTITY_TOKENS, FALLBACK_CALLER, GENERAL, QUEUE_TEMPLATES, QUIET_END_HOUR, QUIET_START_HOUR,
    STALE_SENDING_SECONDS, TEMPLATES, TEMPLATE_NEEDS,
)
from src.lending.compliance import phone_hash
from src.lending.consent import has_text_consent
from src.lending.ghl_sms import GhlSmsError

logger = logging.getLogger(__name__)

_NAME = re.compile(r"^[A-Za-z][A-Za-z'\-]+$")
_INITIAL = re.compile(r"^[A-Za-z]\.?$")
_INVALID_CHARS = re.compile(r"[&/,\d]")


@dataclass(frozen=True)
class PendingText:
    event_id: int
    call_id: str
    phone: str
    ended_at: datetime
    property_address: Optional[str]
    queue: Optional[str]
    caller_name: Optional[str]
    borrower_name: Optional[str]
    county: Optional[str]


def first_name_of(borrower_name: Optional[str]) -> Optional[str]:
    """A person's first name, or None for blanks, initials and entity-looking names.

    Accepts only 2-3 token names where all tokens are name-like (letters, apostrophes, hyphens),
    no entity words, no special characters, and first token <= 20 chars.
    """
    name = (borrower_name or "").strip()
    if not name or _INVALID_CHARS.search(name):
        return None

    tokens = name.split()
    if len(tokens) not in (2, 3):
        return None

    # Check entity tokens and token lengths
    for token in tokens:
        cleaned = token.strip(".,").lower()
        if cleaned in ENTITY_TOKENS:
            return None

    first = tokens[0]
    if len(first) > CAPS["first_name"]:
        return None

    # For 2-token names: both must be name-like (no initials)
    if len(tokens) == 2:
        if not (_NAME.match(first) and _NAME.match(tokens[1])):
            return None
    # For 3-token names: first and third are name-like, second can be initial
    else:
        if not _NAME.match(first):
            return None
        if not (_INITIAL.match(tokens[1]) and _NAME.match(tokens[2])):
            return None

    return first.title() if first.isupper() or first.islower() else first


def _street(item: PendingText) -> str:
    """Extract street address (part before first comma), whitespace-collapsed."""
    addr = (item.property_address or "").split(",")[0].strip()
    return " ".join(addr.split())


def choose_template(item: PendingText) -> str:
    key = QUEUE_TEMPLATES.get(item.queue or "", GENERAL)
    for field in TEMPLATE_NEEDS[key]:
        if field == "property_address":
            if not _street(item):
                return GENERAL
        else:
            if not (getattr(item, field) or "").strip():
                return GENERAL
    return key


def format_number(e164: str) -> str:
    digits = re.sub(r"\D", "", e164)
    if len(digits) == 11 and digits.startswith("1"):
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return e164


def render_body(item: PendingText, template_key: str, *, number: str) -> str:
    if not number or not number.strip():
        raise ValueError("a texting number is required")

    first = first_name_of(item.borrower_name)
    street = _street(item)

    # Collapse whitespace in all substituted values
    caller = " ".join((item.caller_name or "").split())[:CAPS["caller"]] or FALLBACK_CALLER
    county = " ".join((item.county or "").split())[:CAPS["county"]]
    street = " ".join(street.split())[:CAPS["property"]]

    return TEMPLATES[template_key].format(
        greeting=f"Hi {first[:CAPS['first_name']]}" if first else "Hi",
        caller=caller,
        property=street,
        county=county,
        number=format_number(number),
    )


TextSender = Callable[[str, str, Optional[str]], str]

_ET = ZoneInfo(TIMEZONE)


def _release_stale_claims(db, now: datetime) -> int:
    """A row left in 'sending' (crash after the claim) is unknown: it keeps the day's slot and is never resent."""
    stale = db.execute(
        text("UPDATE lending.missed_call_events SET status = 'send_unknown' "
             "WHERE status = 'sending' AND decided_at < :cutoff RETURNING id"),
        {"cutoff": now - timedelta(seconds=STALE_SENDING_SECONDS)},
    ).scalars().all()
    if stale:
        logger.error("[text-back] %d event(s) stuck in 'sending' marked send_unknown; check GHL for duplicates", len(stale))
    return len(stale)


def _claim(db, limit: int) -> list[PendingText]:
    ids = db.execute(
        text("WITH picked AS (SELECT id FROM lending.missed_call_events WHERE status = 'pending' "
             "ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED) "
             "UPDATE lending.missed_call_events e SET status = 'sending', decided_at = now() "
             "FROM picked WHERE e.id = picked.id RETURNING e.id"),
        {"limit": limit},
    ).scalars().all()
    if not ids:
        return []
    rows = db.execute(
        text("SELECT e.id, e.dialer_call_id, e.phone, e.property_address, cd.call_ended_at, cd.queue, cd.caller_name "
             "FROM lending.missed_call_events e JOIN lending.call_dispositions cd ON cd.dialer_call_id = e.dialer_call_id "
             "WHERE e.id = ANY(:ids) ORDER BY e.created_at"),
        {"ids": ids},
    ).all()
    phones = sorted({r[2] for r in rows})
    names, counties = _borrower_names(db, phones), _counties(db, phones)
    return [PendingText(event_id=r[0], call_id=r[1], phone=r[2], ended_at=r[4], property_address=r[3], queue=r[5],
                        caller_name=r[6], borrower_name=names.get(r[2]), county=counties.get(r[2])) for r in rows]


def _borrower_names(db, phones: list[str]) -> dict[str, str]:
    if db.execute(text("SELECT to_regclass('lending.dialer_load_records') IS NOT NULL")).scalar() is not True:
        return {}
    rows = db.execute(
        text("SELECT DISTINCT ON (phone) phone, borrower_name FROM lending.dialer_load_records "
             "WHERE phone = ANY(:p) ORDER BY phone, loaded_at DESC"), {"p": phones}).all()
    return {r[0]: r[1] for r in rows if r[1]}


def _counties(db, phones: list[str]) -> dict[str, str]:
    if db.execute(text("SELECT to_regclass('lending.calling_pool_staging') IS NOT NULL")).scalar() is not True:
        return {}
    rows = db.execute(
        text("SELECT DISTINCT ON (normalized_phone) normalized_phone, county_name FROM lending.calling_pool_staging "
             "WHERE normalized_phone = ANY(:p) ORDER BY normalized_phone, id DESC"), {"p": phones}).all()
    return {r[0]: r[1] for r in rows if r[1]}


def _record(db, event_id: int, status: str, template_key: Optional[str] = None, message_id: Optional[str] = None) -> None:
    db.execute(
        text("UPDATE lending.missed_call_events SET status = :s, decided_at = now(), "
             "template_key = COALESCE(:t, template_key), provider_message_id = COALESCE(:m, provider_message_id) "
             "WHERE id = :id AND status = 'sending'"),
        {"s": status, "t": template_key, "m": message_id, "id": event_id},
    )


def _decide(db, item: PendingText, *, sender: Optional[TextSender], enabled: bool, now: datetime) -> str:
    """The outcome for one claimed event; 'send' means every gate passed and a sender exists."""
    if (now - item.ended_at).total_seconds() > MAX_LATE_SECONDS:
        return "skipped_late"
    if not has_text_consent(db, item.phone):
        return "skipped_no_consent"
    if not QUIET_START_HOUR <= now.astimezone(_ET).hour < QUIET_END_HOUR:
        return "skipped_quiet_hours"
    if not enabled:
        return "dry_run"
    if sender is None:
        return "skipped_not_configured"
    return "send"


def _render(item: PendingText, number: str) -> tuple[Optional[str], Optional[str]]:
    """(template_key, body), or (None, None) when rendering fails: nothing has gone out, so the
    caller records 'failed' instead of leaving the claim for the stale sweep (which would hold the
    day's slot for a text that never left)."""
    try:
        key = choose_template(item)
        return key, render_body(item, key, number=number)
    except Exception as exc:  # class only: never log values
        logger.error("[text-back] event %s could not be rendered (%s)", item.event_id, type(exc).__name__)
        return None, None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _mark_unsent_failed(db, item: PendingText) -> Optional[str]:
    """After an unexpected error BEFORE the sender was called nothing went out: record 'failed' in a
    fresh transaction so the event does not wait for the stale sweep (which would hold the day's slot).
    Returns 'failed' when recorded, None when even that write failed (the sweep then takes it)."""
    try:
        _record(db, item.event_id, "failed")
        db.commit()
        return "failed"
    except Exception as exc:  # class only
        logger.error("[text-back] event %s could not be marked failed (%s); left for the stale-claim sweep",
                     item.event_id, type(exc).__name__)
        db.rollback()
        return None


def _decide_and_send(db, item: PendingText, *, sender: Optional[TextSender], enabled: bool, number: Optional[str],
                     now: datetime, started: list[bool]) -> str:
    """Run the gates and, if all pass, the send; record and return the outcome (the caller commits).
    ``started`` is set the moment the sender is about to be called: from then on a failure is unknown."""
    outcome = _decide(db, item, sender=sender, enabled=enabled, now=now)
    template_key: Optional[str] = None
    if outcome == "send":
        if not number:
            outcome = "skipped_not_configured"
        else:
            template_key, body = _render(item, number)
            if body is None:
                outcome = "failed"
            else:
                started[0] = True
                try:
                    message_id = sender(item.phone, body, first_name_of(item.borrower_name))
                except GhlSmsError as exc:
                    outcome = "send_unknown" if exc.ambiguous else "failed"
                else:
                    _record(db, item.event_id, "sent", template_key, message_id)
                    return "sent"
    _record(db, item.event_id, outcome, template_key)
    return outcome


def process_pending(db, *, sender: Optional[TextSender], enabled: bool, number: Optional[str],
                    now: Optional[datetime] = None, limit: int = 50,
                    clock: Optional[Callable[[], datetime]] = None) -> dict[str, int]:
    """Decide every pending missed-call event once. Commits: the claim first (so a crash cannot make
    a second worker send the same text), then each event's outcome on its own.

    The 60 s and 8am-8pm gates are checked against the time the event is reached, not when the batch
    started (each send can take a while): ``clock`` is read per event unless ``now`` is injected."""
    clock = clock or _utcnow
    _release_stale_claims(db, now or clock())
    items = _claim(db, limit)
    db.commit()
    counts: dict[str, int] = {}
    for item in items:
        started = [False]
        try:
            outcome = _decide_and_send(db, item, sender=sender, enabled=enabled, number=number,
                                       now=now or clock(), started=started)
            db.commit()
        except Exception as exc:  # class only: SQL / HTTP errors can embed phones
            logger.error("[text-back] event %s crashed (%s)", item.event_id, type(exc).__name__)
            db.rollback()
            outcome = None if started[0] else _mark_unsent_failed(db, item)
            if outcome is None:
                continue
        counts[outcome] = counts.get(outcome, 0) + 1
        logger.info("[text-back] call=%s phone_hash=%s outcome=%s", item.call_id, phone_hash(item.phone)[:12], outcome)
    return counts


def run_text_back_cycle(*, now: Optional[datetime] = None) -> dict[str, int]:
    """One pass for the poller loop: real settings, own session."""
    from config.settings import get_settings
    from src.lending.db import lending_session
    from src.lending.ghl_sms import get_sender, texting_number

    with lending_session() as db:
        return process_pending(db, sender=get_sender(), enabled=get_settings().missed_call_text_enabled,
                               number=texting_number(), now=now)
