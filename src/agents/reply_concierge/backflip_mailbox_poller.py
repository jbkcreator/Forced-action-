"""src/agents/reply_concierge/backflip_mailbox_poller.py

WP-T2-6 Addendum 2 (2026-09-22) -- polls Josh's own mailbox (a Gmail App
Password + IMAP, not Cora's shared leads@forcedactionleads.com) for
notification emails from Backflip's own domain.

Why this exists, replacing the original Task 12 assumption: WP-T2-6
originally assumed Backflip's notification emails would land in Cora's
already-monitored shared mailbox, reusing its existing Gmail-API/domain-
wide-delegation access rather than standing up new credentials. That
assumption was never confirmed (Q7/Q8 open at the time) and does not
survive scrutiny: leads@forcedactionleads.com is Forced Action's own
COLD-OUTREACH reply mailbox -- prospects who reply to FA's own marketing
emails land there. Backflip is a third-party lending platform Josh
personally registered an account with; its notifications go wherever
HIS Backflip account is registered to -- his own inbox, not Forced
Action's outreach mailbox. Domain-wide delegation cannot reach a
personal mailbox anyway (no Workspace admin to grant it against), so
this mirrors the separate Banks project's own solution to the identical
problem (banks/emailport.py): a Gmail App Password + stdlib imaplib,
self-serve, no GCP project needed.

Safety design (same spirit as Banks' emailport.py/inbox.py):
- IMAP SEARCH is scoped to Backflip's own sender BEFORE any message is
  fetched -- nothing outside that sender is ever read, let alone
  touched. FA_MAX_BACKFLIP_NOTIFICATION_SENDER_DOMAIN accepts either a
  bare domain ("backflip.com", matches any sender on it) or a full
  address ("notify@backflip.com", matches only that one sender) -- the
  latter matters when the poll mailbox and the sender share a domain
  (e.g. Gmail-to-Gmail in dev/test), where a domain-level filter would
  also catch unrelated mail in the same inbox. A defensive re-check of
  the actual From header after fetch guards against IMAP SEARCH FROM
  being a loose substring match on some servers.
- A message is marked \\Seen only AFTER apply_parsed_event() succeeds,
  so a crash or DB failure mid-run leaves it unread and it is retried
  on the next poll -- at-least-once, never silently dropped.
  apply_parsed_event()'s downstream writes are already idempotency-
  keyed, so a retry is a safe no-op, not a duplicate.
- A genuinely unparseable email (parse_backflip_notification() returns
  None) IS marked seen -- there is nothing more this poller can do with
  it, and leaving it unread would mean re-attempting parse (including a
  fresh LLM fallback call) forever, every poll cycle, for no gain.
- All three settings (mailbox email, app password, sender domain) must
  be set for this poller to do anything; any one missing means it
  silently no-ops every poll, matching this codebase's convention for
  optional external integrations.
"""
from __future__ import annotations

import email
import email.message
import email.policy
import imaplib
import logging
from email.utils import parseaddr

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"


def _matches_sender_filter(from_address: str, sender_filter: str) -> bool:
    """sender_filter is FA_MAX_BACKFLIP_NOTIFICATION_SENDER_DOMAIN -- a
    bare domain ("backflip.com") matches any sender on it; a full address
    ("notify@backflip.com") matches only that one exact sender."""
    from_address = from_address.lower()
    sender_filter = sender_filter.lower()
    if "@" in sender_filter:
        return from_address == sender_filter
    return from_address.endswith(f"@{sender_filter}")


def _extract_body(msg: email.message.Message) -> str:
    """Best-effort plain-text body extraction (mirrors Banks' emailport.py)."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode("utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode("utf-8", errors="replace")
    return ""


def poll_backflip_mailbox(session: Session) -> int:
    """Polls Josh's mailbox for unread mail from Backflip's own sender
    domain, parses each with parse_backflip_notification(), and applies
    matches via apply_parsed_event(). Returns the count successfully
    applied. Never raises -- an IMAP failure is logged and returns 0,
    same fail-safe contract as reply_mailbox_poller.py.
    """
    from config.settings import get_settings

    settings = get_settings()
    mailbox_email = settings.fa_max_backflip_mailbox_email
    app_password = settings.fa_max_backflip_mailbox_app_password
    sender_domain = settings.fa_max_backflip_notification_sender_domain

    if not mailbox_email or not app_password or not sender_domain:
        logger.debug(
            "backflip_mailbox_poller: not configured (mailbox_email/app_password/"
            "sender_domain) -- no-op"
        )
        return 0

    from src.agents.reply_concierge.backflip_email_parser import parse_backflip_notification
    from src.agents.reply_concierge.backflip_stage_ingest import apply_parsed_event

    processed = 0
    try:
        with imaplib.IMAP4_SSL(IMAP_HOST) as conn:
            conn.login(mailbox_email, app_password.get_secret_value())
            conn.select("INBOX")
            _, data = conn.search(None, f'(UNSEEN FROM "{sender_domain}")')
            uids = (data[0] or b"").split()
            logger.info(
                "backflip_mailbox_poller: poll start -- %d unread from %s",
                len(uids), sender_domain,
            )

            for uid in uids:
                try:
                    processed += _process_one(
                        conn, uid, sender_domain, parse_backflip_notification,
                        apply_parsed_event, session,
                    )
                except Exception as exc:
                    logger.error(
                        "backflip_mailbox_poller: error processing uid=%s: %s",
                        uid, exc, exc_info=True,
                    )
                    session.rollback()
    except (imaplib.IMAP4.error, OSError) as exc:
        logger.error("backflip_mailbox_poller: IMAP failure: %s", exc, exc_info=True)
        return processed

    return processed


def _process_one(conn, uid, sender_domain, parse_fn, apply_fn, session) -> int:
    """Fetch, filter, parse, and apply one message. Returns 1 if applied,
    0 otherwise. Isolated so one bad message can't abort the whole poll."""
    _, msg_data = conn.fetch(uid, "(BODY.PEEK[])")
    if not msg_data or not isinstance(msg_data[0], tuple):
        return 0

    # policy.default decodes RFC 2047 encoded-word headers (e.g. a subject
    # with a non-ASCII character arrives as "=?UTF-8?Q?...?="). Without it,
    # msg.get("Subject") returns that raw encoded string, and "UTF-8" itself
    # is a false-positive match for a letters+digits reference token --
    # found via a real Gmail test send whose em dash triggered exactly this.
    msg = email.message_from_bytes(msg_data[0][1], policy=email.policy.default)
    _, from_address = parseaddr(msg.get("From", ""))
    if not _matches_sender_filter(from_address, sender_domain):
        # Defensive re-check -- IMAP SEARCH FROM is a loose match on some
        # servers; never let a message outside the confirmed sender reach
        # the parser just because the server's search returned it.
        return 0

    subject = msg.get("Subject", "")
    body_text = _extract_body(msg)
    message_id = msg.get("Message-ID", "")

    event = parse_fn(subject, body_text)
    if event is None:
        logger.warning(
            "backflip_mailbox_poller: sender=%s subject=%r message_id=%s "
            "did not match any known notification pattern -- marked seen, dropped",
            from_address, subject[:80], message_id,
        )
        conn.store(uid, "+FLAGS", "\\Seen")
        return 0

    applied = apply_fn(session, event, source="email_parsed", actor="backflip_mailbox_poller")
    if applied:
        conn.store(uid, "+FLAGS", "\\Seen")
        return 1

    logger.info(
        "backflip_mailbox_poller: message_id=%s not applied (unresolved backflip_ref) "
        "-- left unread for retry",
        message_id,
    )
    return 0
