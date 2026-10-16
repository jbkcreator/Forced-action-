"""Opt-out mailbox listener: email sent to the published opt-out address becomes a lending opt-out.

Porkbun has no inbound-mail webhook, so this reads the mailbox over IMAP on a schedule. The mailbox is
either a dedicated ``optout@`` mailbox or the one ``optout@`` forwards into; only messages addressed to
``LENDING_OPTOUT_ADDRESS`` are acted on, so other mail in a shared inbox is ignored. The mailbox is opened
read-only: nothing is marked read, moved or deleted.

Each opt-out goes through ``compliance.propagate_opt_out`` (channel ``email``): the sender's address, and every
US phone number written in the subject or body, is suppressed, set do-not-contact and removed from the dialer.
A phone number in a signature is therefore blocked too; failing that way is the safe direction. Our own
business numbers, which appear in quoted replies, are never suppressed. Each new opt-out is posted to the ops
channel with the address masked and no phone number.

Nothing can be skipped. The IMAP search itself returns only mail addressed to the opt-out address, and every
examined message is recorded in ``lending.email_opt_out_messages`` (keyed by a hash of its Message-ID). Each
sweep drops settled messages before taking its batch, oldest first, so an unprocessed opt-out is always reached,
however much mail arrives after it. A failed attempt is retried on later sweeps (up to MAX_MESSAGE_ATTEMPTS, then
ops is alerted); an unreadable message is alerted at once. The opt-out itself is also keyed by Message-ID, so a
message is never recorded twice.

Logs carry message keys and counts only, never addresses, phone numbers or message text.

Usage (cron, every 5 minutes):
    python -m src.lending.email_opt_out
"""
from __future__ import annotations

import hashlib
import imaplib
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email import message_from_bytes, policy
from email.message import EmailMessage
from email.utils import getaddresses, parseaddr
from typing import Optional, Protocol

from sqlalchemy import text

from config.lending_compliance import OptOutChannel
from config.lending_prequal import BORROWER_CALLBACK_PHONE
from config.settings import get_settings
from packages.agent_core.redaction import mask_emails
from src.lending.compliance import propagate_opt_out
from src.lending.cora_inbox import add_to_inbox_in_own_session
from src.lending.db import lending_session
from src.lending.ops_alerts import post_ops_alert
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 30
MAX_MESSAGES_PER_RUN = 200
MAX_MESSAGE_ATTEMPTS = 10
IMAP_TIMEOUT_SECONDS = 30
SOURCE_REF_PREFIX = "email:"

_RECIPIENT_HEADERS = ("To", "Cc", "Delivered-To", "X-Original-To")
_BOUNCE_SENDERS = frozenset({"mailer-daemon", "postmaster"})
# A US number in any common written form: 8134369951, 813-436-9951, (813) 436 9951, +1 813.436.9951.
_PHONE = re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}(?!\d)")
_HTML_TAG = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class EmailOptOut:
    key: str                  # hash of the Message-ID: the opt-out's idempotency key, safe to log
    sender: str               # lower-cased address
    phones: tuple[str, ...]   # normalized E.164, our own numbers removed


def _recipients(message: EmailMessage) -> set[str]:
    values = [str(value) for header in _RECIPIENT_HEADERS for value in message.get_all(header, [])]
    return {address.strip().lower() for _, address in getaddresses(values) if address}


def _is_automated(message: EmailMessage, sender: str) -> bool:
    """Bounces and auto-replies (out-of-office, delivery reports) are not opt-outs."""
    if str(message.get("Auto-Submitted", "no")).strip().lower() != "no":
        return True
    return sender.split("@", 1)[0] in _BOUNCE_SENDERS


def _readable_text(message: EmailMessage) -> str:
    body = message.get_body(preferencelist=("plain", "html"))
    try:
        content = body.get_content() if body is not None else ""
    except (KeyError, LookupError, UnicodeError):
        content = ""
    if body is not None and body.get_content_subtype() == "html":
        content = _HTML_TAG.sub(" ", content)
    return f"{message.get('Subject', '')}\n{content}"


def _message_key(message: EmailMessage, raw: bytes) -> str:
    message_id = str(message.get("Message-ID", "")).strip()
    return hashlib.sha256((message_id or raw.decode("latin-1")).encode("utf-8")).hexdigest()[:40]


def parse_opt_out(raw: bytes, *, optout_address: str, own_numbers: frozenset[str]) -> Optional[EmailOptOut]:
    """The opt-out a raw message carries, or None when it is not addressed to the opt-out address,
    is automated, or has no usable sender."""
    message = message_from_bytes(raw, policy=policy.default)
    if optout_address.strip().lower() not in _recipients(message):
        return None
    # From, not Reply-To: a message must not be able to name someone else's address to suppress.
    sender = parseaddr(str(message.get("From") or ""))[1].strip().lower()
    if "@" not in sender or _is_automated(message, sender):
        return None
    found = {normalize(match) for match in _PHONE.findall(_readable_text(message))}
    phones = tuple(sorted(phone for phone in found if phone and phone not in own_numbers))
    return EmailOptOut(key=_message_key(message, raw), sender=sender, phones=phones)


def own_business_numbers() -> frozenset[str]:
    """Numbers that appear in our own messages (quoted in replies) and must never be suppressed."""
    candidates = (get_settings().lending_ghl_sms_from_number, BORROWER_CALLBACK_PHONE)
    return frozenset(number for number in (normalize(raw) for raw in candidates if raw) if number)


_EXISTING = text("SELECT id FROM lending.opt_out_events WHERE channel = :channel AND source_ref = :ref LIMIT 1")


def record_opt_out(db, opt_out: EmailOptOut) -> Optional[int]:
    """Record one email opt-out. Returns the new event id, or None when it was already recorded.
    Does not commit. The sender and the first phone share one event; each further phone gets its own."""
    ref = f"{SOURCE_REF_PREFIX}{opt_out.key}"
    if db.execute(_EXISTING, {"channel": OptOutChannel.EMAIL.value, "ref": ref}).scalar():
        return None
    phones = opt_out.phones or (None,)
    event_id = propagate_opt_out(db, email=opt_out.sender, phone=phones[0], source_ref=ref,
                                 channel=OptOutChannel.EMAIL)
    for index, phone in enumerate(phones[1:], start=2):
        propagate_opt_out(db, phone=phone, source_ref=f"{ref}:{index}", channel=OptOutChannel.EMAIL)
    return event_id


def header_key(header_bytes: bytes) -> str:
    """The tracking key of a message from its headers alone, so a settled message is never fetched in full again:
    a hash of the Message-ID, or of the whole header block when a message has none."""
    message = message_from_bytes(header_bytes, policy=policy.default)
    message_id = str(message.get("Message-ID", "")).strip()
    return hashlib.sha256((message_id or header_bytes.decode("latin-1")).encode("utf-8")).hexdigest()[:40]


class Mailbox(Protocol):
    def matching_ids(self, since: date) -> list[bytes]:
        """Messages since ``since`` addressed to the opt-out address (filtered on the server), oldest first."""

    def header_key(self, message_number: bytes) -> str: ...

    def raw(self, message_number: bytes) -> bytes: ...


class ImapMailbox:
    """The live mailbox, opened read-only: nothing is marked read, moved or deleted."""

    def __init__(self, *, host: str, port: int, user: str, password: str, optout_address: str) -> None:
        self._host, self._port, self._user, self._password = host, port, user, password
        self._address = optout_address.strip().lower()
        self._imap: Optional[imaplib.IMAP4_SSL] = None

    def __enter__(self) -> "ImapMailbox":
        self._imap = imaplib.IMAP4_SSL(self._host, self._port, timeout=IMAP_TIMEOUT_SECONDS)
        self._imap.login(self._user, self._password)
        self._imap.select("INBOX", readonly=True)
        return self

    def __exit__(self, *exc) -> None:
        if self._imap is not None:
            try:
                self._imap.logout()
            except Exception:  # the session may already be gone
                pass

    def matching_ids(self, since: date) -> list[bytes]:
        quoted = f'"{self._address}"'
        # OR (OR To Cc) (OR Delivered-To X-Original-To): only opt-out mail comes back, so other mail in a shared
        # inbox never competes for the sweep. parse_opt_out still checks the headers exactly.
        _, data = self._imap.search(None, "SINCE", since.strftime("%d-%b-%Y"), "OR", "OR", "TO", quoted, "CC", quoted,
                                    "OR", "HEADER", "Delivered-To", quoted, "HEADER", "X-Original-To", quoted)
        return (data[0] or b"").split()

    def _fetch(self, message_number: bytes, part: str) -> bytes:
        _, parts = self._imap.fetch(message_number, f"({part})")
        return next((piece[1] for piece in parts if isinstance(piece, tuple)), b"") or b""

    def header_key(self, message_number: bytes) -> str:
        message_id = self._fetch(message_number, "BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)]")
        if b":" in message_id:
            return header_key(message_id)
        return header_key(self._fetch(message_number, "BODY.PEEK[HEADER]"))

    def raw(self, message_number: bytes) -> bytes:
        return self._fetch(message_number, "BODY.PEEK[]")


class MessageLog(Protocol):
    def settled(self, keys: set[str]) -> set[str]:
        """The keys that need no further work (recorded, not an opt-out, given up or unreadable)."""

    def mark(self, key: str, outcome: str, *, event_id: Optional[int] = None, error: Optional[str] = None) -> None: ...

    def mark_failed(self, key: str, error: str) -> int:
        """Record a failed attempt; returns the attempt count."""


class SqlMessageLog:
    """``lending.email_opt_out_messages``: durable progress across sweeps."""

    _SETTLED = text("SELECT message_key FROM lending.email_opt_out_messages "
                    "WHERE message_key = ANY(:keys) AND outcome <> 'failed'")
    _MARK = text(
        "INSERT INTO lending.email_opt_out_messages (message_key, outcome, opt_out_event_id, last_error) "
        "VALUES (:key, :outcome, :event_id, :error) ON CONFLICT (message_key) DO UPDATE SET "
        "outcome = EXCLUDED.outcome, opt_out_event_id = COALESCE(EXCLUDED.opt_out_event_id, "
        "lending.email_opt_out_messages.opt_out_event_id), last_error = EXCLUDED.last_error, last_attempt_at = now()"
    )
    _FAILED = text(
        "INSERT INTO lending.email_opt_out_messages (message_key, outcome, attempts, last_error) "
        "VALUES (:key, 'failed', 1, :error) ON CONFLICT (message_key) DO UPDATE SET outcome = 'failed', "
        "attempts = lending.email_opt_out_messages.attempts + 1, last_error = EXCLUDED.last_error, "
        "last_attempt_at = now() RETURNING attempts"
    )

    def __init__(self, session_factory: Callable = lending_session) -> None:
        self._session_factory = session_factory

    def settled(self, keys: set[str]) -> set[str]:
        if not keys:
            return set()
        with self._session_factory() as db:
            return set(db.execute(self._SETTLED, {"keys": sorted(keys)}).scalars().all())

    def mark(self, key: str, outcome: str, *, event_id: Optional[int] = None, error: Optional[str] = None) -> None:
        with self._session_factory() as db:
            db.execute(self._MARK, {"key": key, "outcome": outcome, "event_id": event_id, "error": error})

    def mark_failed(self, key: str, error: str) -> int:
        with self._session_factory() as db:
            return int(db.execute(self._FAILED, {"key": key, "error": error}).scalar_one())


@dataclass
class SweepResult:
    examined: int = 0
    recorded: int = 0
    failed: int = 0


def _process_one(mailbox: Mailbox, log: MessageLog, message_number: bytes, key: str, *, optout_address: str,
                 own_numbers: frozenset[str], session_factory: Callable, notify: Callable[[str], bool],
                 result: SweepResult) -> None:
    try:
        opt_out = parse_opt_out(mailbox.raw(message_number), optout_address=optout_address, own_numbers=own_numbers)
    except Exception as exc:  # unreadable: needs a person, retrying will not help
        log.mark(key, "unreadable", error=type(exc).__name__)
        posted = notify(f":warning: An email to the opt-out address could not be read ({type(exc).__name__}, "
                        f"message {key}). Please check the opt-out mailbox and record the opt-out by hand if it is one.")
        add_to_inbox_in_own_session(
            event_type="opt_out_email_unreadable", dedup_key=key, target_channel="ops", channel_posted=bool(posted),
            summary="An opt-out mailbox email could not be read: check it and record the opt-out by hand",
            payload={"message_key": key, "error": type(exc).__name__},
        )
        return
    if opt_out is None:
        log.mark(key, "not_opt_out")
        return
    try:
        with session_factory() as db:
            event_id = record_opt_out(db, opt_out)
    except Exception as exc:  # class only: SQL errors embed bound parameters (addresses, phones)
        result.failed += 1
        attempts = log.mark_failed(key, type(exc).__name__)
        logger.error("[lending-email-opt-out] opt-out %s not recorded (attempt %d): %s", key, attempts,
                     type(exc).__name__)
        if attempts >= MAX_MESSAGE_ATTEMPTS:
            log.mark(key, "gave_up", error=type(exc).__name__)
            posted = notify(f":rotating_light: An opt-out email from {mask_emails(opt_out.sender)} could not be "
                            f"recorded after {attempts} attempts ({type(exc).__name__}, message {key}). It is NOT "
                            "recorded: record it by hand.")
            add_to_inbox_in_own_session(
                event_type="opt_out_email_not_recorded", dedup_key=key, target_channel="ops",
                channel_posted=bool(posted),
                summary="An opt-out email could not be recorded after every retry: record it by hand",
                payload={"message_key": key, "sender_masked": mask_emails(opt_out.sender), "attempts": attempts,
                         "error": type(exc).__name__},
            )
        return
    log.mark(key, "recorded", event_id=event_id)
    if event_id is None:
        return
    result.recorded += 1
    logger.info("[lending-email-opt-out] recorded opt-out %s (event %s, %d phone(s))", key, event_id,
                len(opt_out.phones))
    phones_note = f" and {len(opt_out.phones)} phone number(s) from the message" if opt_out.phones else ""
    notify(f":no_entry: Opt-out email from {mask_emails(opt_out.sender)}: suppressed the address{phones_note} "
           f"(texts, calls and the dialer). Opt-out event #{event_id}.")


def sweep(mailbox: Mailbox, log: MessageLog, *, since: date, optout_address: str, own_numbers: frozenset[str],
          session_factory: Callable = lending_session, notify: Callable[[str], bool] = post_ops_alert,
          limit: int = MAX_MESSAGES_PER_RUN) -> SweepResult:
    """Settle up to ``limit`` unsettled opt-out messages, oldest first. Settled messages are skipped before the
    limit is applied, so an unprocessed opt-out is always reached in a later sweep, however much mail follows it."""
    result = SweepResult()
    keyed = [(number, mailbox.header_key(number)) for number in mailbox.matching_ids(since)]
    settled = log.settled({key for _, key in keyed})
    pending = [(number, key) for number, key in keyed if key not in settled][:limit]
    for number, key in pending:
        result.examined += 1
        _process_one(mailbox, log, number, key, optout_address=optout_address, own_numbers=own_numbers,
                     session_factory=session_factory, notify=notify, result=result)
    return result


def run(now: Optional[datetime] = None) -> int:
    """One pass. Returns 0 on success or when not configured, 1 when the mailbox or the database failed."""
    settings = get_settings()
    password = settings.lending_optout_imap_password
    if not (settings.lending_optout_imap_user and password):
        logger.info("[lending-email-opt-out] not configured (LENDING_OPTOUT_IMAP_USER / _PASSWORD); skipped")
        return 0
    since = (now or datetime.now(timezone.utc)).date() - timedelta(days=LOOKBACK_DAYS)
    try:
        with ImapMailbox(host=settings.lending_optout_imap_host, port=settings.lending_optout_imap_port,
                         user=settings.lending_optout_imap_user, password=password.get_secret_value(),
                         optout_address=settings.lending_optout_address) as mailbox:
            result = sweep(mailbox, SqlMessageLog(), since=since, optout_address=settings.lending_optout_address,
                           own_numbers=own_business_numbers())
    except Exception as exc:  # class only: IMAP and SQL errors can echo the login or addresses
        logger.error("[lending-email-opt-out] sweep failed: %s", type(exc).__name__)
        return 1
    logger.info("[lending-email-opt-out] examined %d message(s), %d new opt-out(s), %d failed", result.examined,
                result.recorded, result.failed)
    return 0


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
