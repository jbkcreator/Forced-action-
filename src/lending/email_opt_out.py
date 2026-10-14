"""Opt-out mailbox listener: email sent to the published opt-out address becomes a lending opt-out.

Porkbun has no inbound-mail webhook, so this reads the mailbox over IMAP on a schedule. The mailbox is
either a dedicated ``optout@`` mailbox or the one ``optout@`` forwards into; only messages addressed to
``LENDING_OPTOUT_ADDRESS`` are acted on, so other mail in a shared inbox is ignored. The mailbox is opened
read-only: nothing is marked read, moved or deleted.

Each opt-out goes through ``compliance.propagate_opt_out`` (channel ``email``): the sender's address, and every
US phone number written in the subject or body, is suppressed, set do-not-contact and removed from the dialer.
A phone number in a signature is therefore blocked too; failing that way is the safe direction. Our own
business numbers, which appear in quoted replies, are never suppressed. Each message is keyed by a hash of its
Message-ID, so re-reading the mailbox never records an opt-out twice. Each new opt-out is posted to the ops
channel with the address masked and no phone number.

Logs carry message keys and counts only, never addresses, phone numbers or message text.

Usage (cron, every 5 minutes):
    python -m src.lending.email_opt_out
"""
from __future__ import annotations

import hashlib
import imaplib
import logging
import re
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email import message_from_bytes, policy
from email.message import EmailMessage
from email.utils import getaddresses, parseaddr
from typing import Optional

from sqlalchemy import text

from config.lending_compliance import OptOutChannel
from config.lending_prequal import BORROWER_CALLBACK_PHONE
from config.settings import get_settings
from packages.agent_core.redaction import mask_emails
from src.lending.compliance import propagate_opt_out
from src.lending.db import lending_session
from src.lending.ops_alerts import post_ops_alert
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 7
MAX_MESSAGES_PER_RUN = 200
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


def fetch_recent_messages(*, host: str, port: int, user: str, password: str, since: date) -> Iterator[bytes]:
    """Raw messages received since ``since``, newest last, at most MAX_MESSAGES_PER_RUN. Read-only."""
    with imaplib.IMAP4_SSL(host, port, timeout=IMAP_TIMEOUT_SECONDS) as imap:
        imap.login(user, password)
        imap.select("INBOX", readonly=True)
        _, data = imap.search(None, "SINCE", since.strftime("%d-%b-%Y"))
        for number in (data[0] or b"").split()[-MAX_MESSAGES_PER_RUN:]:
            _, parts = imap.fetch(number, "(BODY.PEEK[])")
            raw = next((part[1] for part in parts if isinstance(part, tuple)), None)
            if raw:
                yield raw


def process_messages(raw_messages: Iterable[bytes], *, optout_address: str, own_numbers: frozenset[str],
                     session_factory: Callable = lending_session,
                     notify: Callable[[str], bool] = post_ops_alert) -> int:
    """Record every new opt-out, one transaction each, and alert once per new one. Returns how many were new."""
    recorded = 0
    for raw in raw_messages:
        try:
            opt_out = parse_opt_out(raw, optout_address=optout_address, own_numbers=own_numbers)
        except Exception as exc:  # a malformed message must not stop the rest
            logger.warning("[lending-email-opt-out] unreadable message skipped: %s", type(exc).__name__)
            continue
        if opt_out is None:
            continue
        try:
            with session_factory() as db:
                event_id = record_opt_out(db, opt_out)
        except Exception as exc:  # class only: SQL errors embed bound parameters (addresses, phones)
            logger.error("[lending-email-opt-out] opt-out %s not recorded: %s", opt_out.key, type(exc).__name__)
            continue
        if event_id is None:
            continue
        recorded += 1
        logger.info("[lending-email-opt-out] recorded opt-out %s (event %s, %d phone(s))", opt_out.key, event_id,
                    len(opt_out.phones))
        phones_note = f" and {len(opt_out.phones)} phone number(s) from the message" if opt_out.phones else ""
        notify(f":no_entry: Opt-out email from {mask_emails(opt_out.sender)}: suppressed the address{phones_note} "
               f"(texts, calls and the dialer). Opt-out event #{event_id}.")
    return recorded


def run(now: Optional[datetime] = None) -> int:
    """One pass. Returns 0 on success or when not configured, 1 when the mailbox could not be read."""
    settings = get_settings()
    password = settings.lending_optout_imap_password
    if not (settings.lending_optout_imap_user and password):
        logger.info("[lending-email-opt-out] not configured (LENDING_OPTOUT_IMAP_USER / _PASSWORD); skipped")
        return 0
    since = (now or datetime.now(timezone.utc)).date() - timedelta(days=LOOKBACK_DAYS)
    try:
        raw_messages = list(fetch_recent_messages(
            host=settings.lending_optout_imap_host, port=settings.lending_optout_imap_port,
            user=settings.lending_optout_imap_user, password=password.get_secret_value(), since=since))
    except Exception as exc:  # class only: IMAP errors can echo the login
        logger.error("[lending-email-opt-out] mailbox could not be read: %s", type(exc).__name__)
        return 1
    recorded = process_messages(raw_messages, optout_address=settings.lending_optout_address,
                                own_numbers=own_business_numbers())
    logger.info("[lending-email-opt-out] checked %d message(s), %d new opt-out(s)", len(raw_messages), recorded)
    return 0


def main() -> int:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
