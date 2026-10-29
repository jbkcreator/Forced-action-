"""Attempt log for one LendingFlow lead: refund evidence (CONTEXT.md "Attempt", "Attempt log").

Every outbound try to reach the lead since it arrived, in time order: calls and voicemails from
lending.call_dispositions (BatchDialer and the GoHighLevel call import), texts and emails from the lead's GHL
conversations. Each attempt is marked automated (workflow, intro, reminders) or person (the ISA). Inbound messages
and calls are never attempts; they set ``first_reply_at``, as does an answered outbound call, so the evidence never
hides that the lead engaged. Built on request, nothing is stored.

GHL calls are skipped in the message read: the call import already stores them in call_dispositions.
Who sent a message comes from GHL's ``source`` / ``userId`` (assumed: ``app`` or a user id without a source = a
person); confirm on a live conversation.

    python -m src.lending.attempt_log --lead-id 1424 [--out attempts.csv]
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Optional, Sequence

from sqlalchemy import text

from config.lending_dispositions import ANSWERED_CODES
from src.lending.ghl_calls import GHL_CONVERSATIONS_VERSION, MAX_MESSAGE_PAGES, message_time
from src.lending.replacement_eligibility import LeadAttempts, lead_attempts

logger = logging.getLogger(__name__)

CALL, VOICEMAIL, TEXT, EMAIL = "call", "voicemail", "text", "email"
AUTOMATED, PERSON = "automated", "person"

_VOICEMAIL_CODE = "LEFT_VOICEMAIL"
_MESSAGE_CHANNELS = {"TYPE_SMS": TEXT, "TYPE_EMAIL": EMAIL}
_PERSON_SOURCES = frozenset({"app", "mobile_app"})
CSV_COLUMNS = ("lead_id", "vendor_lead_id", "phone_last4", "attempted_at", "channel", "sent_by", "outcome",
               "recording")

MessageReader = Callable[[str], list[dict[str, Any]]]


@dataclass(frozen=True)
class LeadRef:
    lead_id: int
    vendor_lead_id: Optional[str]
    phone: str
    received_at: datetime
    ghl_contact_id: Optional[str]

    @property
    def phone_last4(self) -> str:
        return self.phone[-4:]


@dataclass(frozen=True)
class CallRow:
    at: datetime
    direction: str
    disposition: Optional[str]
    talk_sec: Optional[int]
    call_source: Optional[str]
    recording_status: Optional[str]
    recording_ref: Optional[str]

    @property
    def answered(self) -> bool:
        return (self.disposition in ANSWERED_CODES) or (self.talk_sec or 0) > 0


@dataclass(frozen=True)
class Attempt:
    at: datetime
    channel: str
    sent_by: str
    outcome: str
    recording: str


@dataclass(frozen=True)
class AttemptLog:
    lead: LeadRef
    attempts: list[Attempt]
    first_reply_at: Optional[datetime]
    bad_number: bool
    wrong_person: bool
    replacement: Optional[LeadAttempts] = None

    @property
    def first_attempt_at(self) -> Optional[datetime]:
        return self.attempts[0].at if self.attempts else None

    @property
    def last_attempt_at(self) -> Optional[datetime]:
        return self.attempts[-1].at if self.attempts else None

    @property
    def minutes_to_first_attempt(self) -> Optional[int]:
        first = self.first_attempt_at
        return None if first is None else int((first - self.lead.received_at).total_seconds() // 60)


def _call_attempt(row: CallRow) -> Attempt:
    channel = VOICEMAIL if row.disposition == _VOICEMAIL_CODE else CALL
    recording = (row.recording_status or "") if row.recording_ref else ""
    return Attempt(row.at, channel, PERSON, row.disposition or "", recording)


def _sent_by(message: Mapping[str, Any]) -> str:
    source = (message.get("source") or "").lower()
    if source in _PERSON_SOURCES or (not source and message.get("userId")):
        return PERSON
    return AUTOMATED


def build_attempt_log(lead: LeadRef, calls: Sequence[CallRow], messages: Sequence[Mapping[str, Any]],
                      replacement: Optional[LeadAttempts] = None) -> AttemptLog:
    """The log from already-read call rows and GHL messages; anything before the lead arrived is left out."""
    attempts: list[Attempt] = []
    replies: list[datetime] = []
    for row in calls:
        if row.at < lead.received_at:
            continue
        if row.direction == "outbound":
            attempts.append(_call_attempt(row))
            if row.answered:
                replies.append(row.at)
        elif row.direction == "inbound":  # the lead calling us is engagement, answered or not
            replies.append(row.at)
    for message in messages:
        added = message_time(message)
        channel = _MESSAGE_CHANNELS.get(message.get("messageType") or "")
        if added is None or channel is None or added < lead.received_at:
            continue
        if message.get("direction") == "inbound":
            replies.append(added)
        elif message.get("direction") == "outbound":
            attempts.append(Attempt(added, channel, _sent_by(message), message.get("status") or "", ""))
    attempts.sort(key=lambda attempt: attempt.at)
    outcomes = {row.disposition for row in calls if row.at >= lead.received_at}
    return AttemptLog(lead, attempts, min(replies, default=None), "BAD_NUMBER" in outcomes,
                      "WRONG_PERSON" in outcomes, replacement)


def to_csv(log: AttemptLog) -> str:
    """One row per attempt; the phone appears as its last four digits only."""
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(CSV_COLUMNS)
    for attempt in log.attempts:
        writer.writerow((log.lead.lead_id, log.lead.vendor_lead_id or "", log.lead.phone_last4,
                         attempt.at.isoformat(), attempt.channel, attempt.sent_by, attempt.outcome,
                         attempt.recording))
    return out.getvalue()


_LEAD = text("""
    SELECT id, vendor_lead_id, phone, received_at, ghl_contact_id
      FROM lending.lendingflow_leads
     WHERE id = :lead_id
""")

_CALLS = text("""
    SELECT COALESCE(call_started_at, call_ended_at) AS at, direction, COALESCE(disposition, disposition_raw),
           talk_duration_sec, call_source, recording_status, recording_ref
      FROM lending.call_dispositions
     WHERE phone = :phone AND COALESCE(call_started_at, call_ended_at) >= :since
     ORDER BY 1
""")


def messages_reader(account, *, http: Optional[Callable[..., Any]] = None) -> MessageReader:
    """``read(contact_id)`` returns every message in the contact's GHL conversations (all types), following each
    conversation's pages. Raises on an HTTP error."""
    from config.lending_alarms import GHL_TIMEOUT_SECONDS
    from src.lending.ghl_account import ghl_headers
    from src.services.ghl_webhook import _GHL_BASE
    from src.utils.http_helpers import requests_get_with_retry

    get = http or (lambda url, **kw: requests_get_with_retry(url, max_retries=1, retry_delay=1,
                                                             timeout=GHL_TIMEOUT_SECONDS, **kw))
    headers = ghl_headers(account.api_key, GHL_CONVERSATIONS_VERSION)

    def read(contact_id: str) -> list[dict[str, Any]]:
        found = get(f"{_GHL_BASE}/conversations/search", headers=headers,
                    params={"locationId": account.location_id, "contactId": contact_id})
        found.raise_for_status()
        messages: list[dict[str, Any]] = []
        for conversation in (found.json() or {}).get("conversations") or []:
            messages.extend(_conversation_messages(conversation["id"]))
        return messages

    def _conversation_messages(conversation_id: str) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        for _ in range(MAX_MESSAGE_PAGES):
            page = get(f"{_GHL_BASE}/conversations/{conversation_id}/messages", headers=headers,
                       params={"lastMessageId": cursor} if cursor else {})
            page.raise_for_status()
            body = page.json() or {}
            listing = body.get("messages") or []
            if isinstance(listing, dict):
                body, listing = listing, listing.get("messages") or []
            batch = [message for message in listing if isinstance(message, dict)]
            messages.extend(batch)
            next_cursor = body.get("lastMessageId")
            if not (body.get("nextPage") and batch and next_cursor and next_cursor != cursor):
                return messages
            cursor = next_cursor
        logger.warning("[attempt-log] conversation %s: stopped after %d pages", conversation_id, MAX_MESSAGE_PAGES)
        return messages

    return read


class LeadNotFound(LookupError):
    pass


def load_attempt_log(db, lead_id: int, read_messages: Optional[MessageReader]) -> tuple[AttemptLog, bool]:
    """The log for one lead and whether its GHL messages were read (False: no reader, no GHL contact, or the read
    failed; the log then holds calls only)."""
    found = db.execute(_LEAD, {"lead_id": lead_id}).first()
    if found is None:
        raise LeadNotFound(lead_id)
    lead = LeadRef(*found)
    calls = [CallRow(*row) for row in db.execute(_CALLS, {"phone": lead.phone, "since": lead.received_at}).all()]
    messages: list[dict[str, Any]] = []
    messages_read = False
    if read_messages is not None and lead.ghl_contact_id:
        try:
            messages, messages_read = read_messages(lead.ghl_contact_id), True
        except Exception as exc:  # class only: GHL errors can echo request detail
            logger.warning("[attempt-log] lead=%s GHL message read failed (%s); calls only", lead_id,
                           type(exc).__name__)
    replacement = next(iter(lead_attempts(db, [lead_id])), None)
    return build_attempt_log(lead, calls, messages, replacement), messages_read


def summary(log: AttemptLog, messages_read: bool) -> dict[str, Any]:
    """The headline numbers for Slack and the CLI (ids and counts only)."""
    counts: dict[str, int] = {}
    for attempt in log.attempts:
        counts[attempt.channel] = counts.get(attempt.channel, 0) + 1
    replacement = log.replacement
    return {
        "lead_id": log.lead.lead_id, "vendor_lead_id": log.lead.vendor_lead_id,
        "phone_last4": log.lead.phone_last4, "received_at": log.lead.received_at.isoformat(),
        "attempts": len(log.attempts), "by_channel": counts,
        "automated": sum(a.sent_by == AUTOMATED for a in log.attempts),
        "by_person": sum(a.sent_by == PERSON for a in log.attempts),
        "first_attempt_at": log.first_attempt_at.isoformat() if log.first_attempt_at else None,
        "last_attempt_at": log.last_attempt_at.isoformat() if log.last_attempt_at else None,
        "minutes_to_first_attempt": log.minutes_to_first_attempt,
        "replied": log.first_reply_at is not None,
        "first_reply_at": log.first_reply_at.isoformat() if log.first_reply_at else None,
        "bad_number": log.bad_number, "wrong_person": log.wrong_person,
        "replacement_eligible": replacement.eligible if replacement else False,
        "replacement_call_attempts": replacement.attempts if replacement else 0,
        "replacement_call_days": replacement.call_days if replacement else 0,
        "texts_and_emails_read": messages_read,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    from src.lending.db import lending_session
    from src.lending.ghl_account import lending_ghl_account

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Attempt log for one LendingFlow lead (refund evidence)")
    parser.add_argument("--lead-id", type=int, required=True)
    parser.add_argument("--out", help="write the CSV here (default: print it)")
    args = parser.parse_args(argv)
    account = lending_ghl_account()
    reader = messages_reader(account) if account else None
    with lending_session() as db:
        try:
            log, messages_read = load_attempt_log(db, args.lead_id, reader)
        except LeadNotFound:
            logger.error("[attempt-log] no LendingFlow lead %s", args.lead_id)
            return 1
    logger.info("[attempt-log] %s", summary(log, messages_read))
    body = to_csv(log)
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="") as handle:
            handle.write(body)
    else:
        sys.stdout.write(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
