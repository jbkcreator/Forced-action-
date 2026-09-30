"""WP-GL-9 missed-call text.

Unanswered outbound calls are read from BatchDialer call records (``/api/cdrs``).
Each gets exactly one decision, stored in ``lending.missed_call_texts``:
sent, dry_run (feature off), or a skip reason. A text is sent only through FA's
consent-gated SMS path (``send_sms`` as marketing), so a number without an SMS
opt-in is skipped, never forced through. Logs carry phone hashes, never phones.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_missed_call import (
    CDR_CALLER_ID_FIELDS,
    CDR_DIRECTION_FIELDS,
    CDR_ENDED_AT_FIELDS,
    CDR_ID_FIELDS,
    CDR_PHONE_FIELDS,
    CDR_STATUS_FIELDS,
    MAX_LATE_SECONDS,
    MAX_TEXT_CHARS,
    NO_ANSWER_STATUSES,
    OUTBOUND_DIRECTIONS,
    SMS_CAMPAIGN,
    TEMPLATE_NO_PROPERTY,
    TEMPLATE_WITH_PROPERTY,
    TIMEZONE,
)
from src.lending.compliance import _suppressed_phones, phone_hash
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

Sender = Callable[[str, str], bool]  # (to, body) -> sent?


@dataclass(frozen=True)
class MissedCall:
    call_id: str
    phone: str
    caller_id_number: Optional[str]
    ended_at: datetime


def _first(row: Mapping[str, Any], fields: tuple[str, ...]) -> Any:
    return next((row[f] for f in fields if row.get(f) not in (None, "")), None)


def _parse_time(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_cdr(row: Mapping[str, Any]) -> Optional[MissedCall]:
    """A MissedCall for an unanswered outbound call record, else None."""
    call_id = _first(row, CDR_ID_FIELDS)
    status = str(_first(row, CDR_STATUS_FIELDS) or "").strip().lower()
    direction = str(_first(row, CDR_DIRECTION_FIELDS) or "outbound").strip().lower()
    phone = normalize_phone(str(_first(row, CDR_PHONE_FIELDS) or ""))
    ended_at = _parse_time(_first(row, CDR_ENDED_AT_FIELDS))
    if not call_id or not phone or ended_at is None:
        return None
    if status not in NO_ANSWER_STATUSES or direction not in OUTBOUND_DIRECTIONS:
        return None
    caller_id = _first(row, CDR_CALLER_ID_FIELDS)
    return MissedCall(call_id=str(call_id), phone=phone,
                      caller_id_number=normalize_phone(str(caller_id)) if caller_id else None, ended_at=ended_at)


def render_text(property_address: Optional[str]) -> str:
    street = (property_address or "").split(",")[0].strip()
    body = TEMPLATE_WITH_PROPERTY.format(property=street) if street else TEMPLATE_NO_PROPERTY
    return body[:MAX_TEXT_CHARS]


def _et_day(moment: datetime) -> date:
    return moment.astimezone(ZoneInfo(TIMEZONE)).date()


def _already_decided(db, call_ids: list[str]) -> set[str]:
    rows = db.execute(text("SELECT dialer_call_id FROM lending.missed_call_texts WHERE dialer_call_id = ANY(:ids)"),
                      {"ids": call_ids}).scalars()
    return set(rows)


def _sent_today(db, phones: list[str], day: date) -> set[str]:
    rows = db.execute(text("SELECT phone FROM lending.missed_call_texts WHERE outcome = 'sent' "
                           "AND event_date_et = :d AND phone = ANY(:p)"), {"d": day, "p": phones}).scalars()
    return set(rows)


def _properties(db, phones: list[str]) -> dict[str, str]:
    if db.execute(text("SELECT to_regclass('lending.dialer_load_records')")).scalar() is None:
        return {}
    rows = db.execute(text(
        "SELECT DISTINCT ON (phone) phone, property_address FROM lending.dialer_load_records "
        "WHERE phone = ANY(:p) ORDER BY phone, loaded_at DESC"), {"p": phones}).fetchall()
    return {r[0]: r[1] for r in rows if r[1]}


def _record(db, call: MissedCall, day: date, outcome: str) -> None:
    db.execute(text("INSERT INTO lending.missed_call_texts (dialer_call_id, phone, event_date_et, outcome) "
                    "VALUES (:c, :p, :d, :o) ON CONFLICT (dialer_call_id) DO NOTHING"),
               {"c": call.call_id, "p": call.phone, "d": day, "o": outcome})
    logger.info("[missed-call-text] call=%s phone_hash=%s outcome=%s", call.call_id, phone_hash(call.phone)[:12], outcome)


def process_missed_calls(
    db,
    calls: Iterable[MissedCall],
    *,
    sender: Sender,
    enabled: bool,
    now: Optional[datetime] = None,
) -> dict[str, int]:
    """Decide each call once, in order. Does not commit. Returns outcome counts."""
    now = now or datetime.now(timezone.utc)
    calls = list(calls)
    if not calls:
        return {}
    done = _already_decided(db, [c.call_id for c in calls])
    fresh = [c for c in calls if c.call_id not in done]
    phones = sorted({c.phone for c in fresh})
    suppressed = _suppressed_phones(db, phones) if phones else set()
    properties = _properties(db, phones) if phones else {}
    sent_by_day: dict[date, set[str]] = {}
    counts: dict[str, int] = {}

    for call in fresh:
        day = _et_day(call.ended_at)
        sent = sent_by_day.setdefault(day, _sent_today(db, phones, day))
        if call.phone in suppressed:
            outcome = "skipped_suppressed"
        elif call.phone in sent:
            outcome = "skipped_daily_cap"
        elif (now - call.ended_at).total_seconds() > MAX_LATE_SECONDS:
            outcome = "skipped_late"
        elif not enabled:
            outcome = "dry_run"
        elif sender(call.phone, render_text(properties.get(call.phone))):
            outcome = "sent"
            sent.add(call.phone)
        else:
            outcome = "skipped_no_consent"
        _record(db, call, day, outcome)
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts


def consent_gated_sender(db) -> Sender:
    """FA's central SMS dispatcher as marketing: opt-out, opt-in consent and quiet hours all apply."""
    from src.services.sms_compliance import send_sms

    def send(to: str, body: str) -> bool:
        return send_sms(to, body, db, message_type="marketing", campaign=SMS_CAMPAIGN)

    return send
