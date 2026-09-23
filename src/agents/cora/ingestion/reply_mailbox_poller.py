"""
Real reply-mailbox ingestion via the Gmail API — a service account +
domain-wide delegation impersonating the monitored Google Workspace mailbox.

Full layered design: docs/plans/cora_reply_mailbox_ingestion_reliability_design.md

One-time setup outside this codebase (see docs/plans/cora_cold_outreach_completion_report.md
for the full checklist): a Google Cloud project with the Gmail API enabled,
a service account with a downloaded JSON key, and that service account's
Client ID authorized for domain-wide delegation in the Workspace Admin
console (Security > API controls > Domain-wide delegation) with EXACTLY
this scope:

    https://www.googleapis.com/auth/gmail.readonly

Deliberately read-only, deliberately not gmail.modify — that scope also
grants send capability, and Cora must never be able to send, even at the
credential level. Because of that, this module can never mark a message
read, so `is:unread` alone is not a usable long-term signal (a message
never leaves it) — see the watermark layer below, which is what actually
bounds cost instead.

Layers, in the order a message passes through them:
  1. Watermark (this file: _poll_via_history / _bootstrap_poll) — bounds
     WHAT gets fetched from Gmail at all. Primary: users.history.list with
     a saved startHistoryId cursor (Gmail's own incremental change-log).
     Fallback: a bounded search (after:<last-good-timestamp>) used only when
     the cursor has gone stale (Gmail retains history ~1 week) or on the
     very first run, after which a fresh historyId is captured to resume
     incremental mode.
  2. Category/sender filter (_UNREAD_REPLY_QUERY) — excludes Gmail's own
     Updates/Promotions/Social/Forums tabs and anything from google.com
     (Workspace's own system notifications — confirmed live, see the design
     doc for the false-positive this caught).
  3. Relevance filter (_process_candidate_message) — checks
     store.find_opportunity_thread_id_by_email(from_address) BEFORE
     publishing. No match -> logged, marked seen, never queued (a message
     Cora never drafted to is not queue/store noise). Match -> the resolved
     opportunity_thread_id is passed directly in the payload, so reply.py's
     own matching step doesn't repeat the same lookup.
  4. Seen-cache (_already_seen/_mark_seen) — short TTL (48h), NOT permanent.
     Its only remaining job is covering the deliberate overlap window the
     bootstrap fallback re-examines; the watermark (layer 1) is what
     prevents unbounded re-scanning now, so this no longer needs to
     remember forever.
  5-6. Queue + worker dedup — idempotency_key is derived from the stable
     Gmail message_id (via reply_stub_producer's idempotency_key override),
     not from a re-stamped processing-time timestamp, so a message that
     somehow gets reprocessed within the worker's own dedup TTL is still
     recognized as a duplicate.
"""
from __future__ import annotations

import base64
import logging
import re
import threading
from email.utils import parseaddr
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from src.agents.cora import queue
from src.agents.cora.ingestion.reply_stub_producer import produce_stub_reply
from src.agents.cora.store import find_opportunity_thread_id_by_email, now

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
DEFAULT_INTERVAL_SECONDS = 3 * 60
PAGE_SIZE = 100

_SEEN_KEY_PREFIX = "cora:gmail:seen:"
_SEEN_TTL_SECONDS = 48 * 3600  # 48h — covers the overlap window only; the watermark bounds re-scanning, not this.

_WATERMARK_HISTORY_ID_KEY = "cora:gmail:watermark:history_id"
_WATERMARK_TIMESTAMP_KEY = "cora:gmail:watermark:timestamp"

# category:primary excludes Gmail's own Updates/Promotions/Social/Forums tabs,
# where Workspace/Google system mail (storage alerts, admin notices, etc.)
# normally lands — a real prospect replying to a real 1:1 outreach email
# lands in Primary. -from:google.com is a second, cheap backstop: confirmed
# via a real poll against this mailbox that Workspace's own system
# notifications (notify-noreply@google.com, workspace-noreply@google.com)
# come from that domain, and no real prospect ever will.
_UNREAD_REPLY_QUERY = "is:unread category:primary -from:google.com"


def _build_gmail_service() -> Optional[Any]:
    from config.settings import get_settings

    settings = get_settings()
    key_path = getattr(settings, "cora_gmail_service_account_key_path", None)
    mailbox = getattr(settings, "cora_reply_mailbox_address", None)
    if not key_path or not mailbox:
        logger.debug("reply_mailbox_poller: not configured (missing key path or mailbox address) — skipping")
        return None

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=SCOPES,
        ).with_subject(mailbox)
        return build("gmail", "v1", credentials=credentials, cache_discovery=False)
    except Exception:
        logger.exception("reply_mailbox_poller: failed to build Gmail service")
        return None


# ── Layer 4: short-TTL seen-cache (overlap-window guard only) ────────────────

def _already_seen(message_id: str) -> bool:
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        return False
    return bool(get_redis().exists(f"{_SEEN_KEY_PREFIX}{message_id}"))


def _mark_seen(message_id: str) -> None:
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        return
    get_redis().set(f"{_SEEN_KEY_PREFIX}{message_id}", "1", ex=_SEEN_TTL_SECONDS)


# ── Layer 1: watermark storage ────────────────────────────────────────────────

def _get_saved_history_id() -> Optional[str]:
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        return None
    return get_redis().get(_WATERMARK_HISTORY_ID_KEY)


def _save_history_id(history_id: str) -> None:
    from src.core.redis_client import get_redis, redis_available

    if not redis_available() or not history_id:
        return
    get_redis().set(_WATERMARK_HISTORY_ID_KEY, history_id)


def _get_fallback_timestamp() -> Optional[int]:
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        return None
    raw = get_redis().get(_WATERMARK_TIMESTAMP_KEY)
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _save_fallback_timestamp(epoch_seconds: int) -> None:
    from src.core.redis_client import get_redis, redis_available

    if not redis_available():
        return
    get_redis().set(_WATERMARK_TIMESTAMP_KEY, str(int(epoch_seconds)))


def _header(headers: List[Dict[str, str]], name: str) -> str:
    for h in headers:
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def _decode_body(part: Dict[str, Any]) -> str:
    """Prefers text/plain; walks multipart recursively; falls back to tag-stripped text/html."""
    mime_type = part.get("mimeType", "")
    data = part.get("body", {}).get("data")
    if mime_type == "text/plain" and data:
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
    for sub_part in part.get("parts", []) or []:
        result = _decode_body(sub_part)
        if result:
            return result
    if mime_type == "text/html" and data:
        html = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        return re.sub(r"<[^<]+?>", "", html)
    return ""


# ── Layer 3: relevance filter + publish ───────────────────────────────────────

def _process_candidate_message(service: Any, message_id: str, db: Session) -> Optional[bool]:
    """
    Fetch, relevance-filter, publish if it matches a known contact_email.

    Returns:
        True  - published successfully. Safe to mark seen, safe to let the
                caller advance the watermark past this message.
        False - handled terminally with nothing to publish (already seen, an
                unmatched sender, or a Gmail 404 — the message itself is gone
                and messages.get() will never succeed on retry). Also safe to
                advance the watermark past.
        None  - a retryable failure (queue.publish() unavailable, or a
                transient error fetching/processing the message). The caller
                MUST NOT advance the watermark past this message, or it can
                never be reconsidered — Gmail's history API only returns
                items after the saved cursor.
    """
    if _already_seen(message_id):
        return False

    try:
        message = service.users().messages().get(userId="me", id=message_id, format="full").execute()
        headers = message.get("payload", {}).get("headers", [])
        _, from_address = parseaddr(_header(headers, "From"))
        subject = _header(headers, "Subject")
        body_text = _decode_body(message.get("payload", {}))

        thread_id = find_opportunity_thread_id_by_email(db, from_address)

        if thread_id is None:
            # Check if this is an FA Max contact before dropping.
            from sqlalchemy import text as _text
            fa_max_row = db.execute(
                _text("""
                    SELECT person_id FROM fa_max_persons
                    WHERE source_reference ILIKE :email
                    LIMIT 1
                """),
                {"email": from_address.strip().lower()},
            ).fetchone()
            if fa_max_row:
                from src.agents.reply_concierge.router import handle_inbound
                handle_inbound(
                    inbound_text=body_text,
                    channel="email",
                    person_id=str(fa_max_row[0]),
                    contact_email=from_address,
                    opportunity_id=None,
                    borrower_first_name=None,
                    db=db,
                )
                _mark_seen(message_id)
                return True
            logger.warning(
                "reply_mailbox_poller: unmatched sender=%s subject=%r message_id=%s — not queued, no draft was ever sent to this address",
                from_address, subject[:80], message_id,
            )
            _mark_seen(message_id)
            return False

        idempotency_key = queue.make_idempotency_key("reply.received", thread_id, f"gmail:{message_id}")
        queued_message_id = produce_stub_reply(
            {
                "opportunity_thread_id": thread_id,
                "from_address": from_address,
                "subject": subject,
                "body_text": body_text,
                "received_at": now().isoformat(),
                "raw_headers": {h["name"]: h["value"] for h in headers},
            },
            idempotency_key=idempotency_key,
        )
        if queued_message_id is None:
            logger.warning(
                "reply_mailbox_poller: publish failed (queue unavailable) for message_id=%s — "
                "will retry next poll, not marking seen",
                message_id,
            )
            return None

        _mark_seen(message_id)  # only now — after a confirmed successful publish
        return True
    except Exception as exc:
        status = getattr(getattr(exc, "resp", None), "status", None)
        if status == 404:
            # The message itself no longer exists (deleted/expunged) — permanent,
            # not transient. Treating it as retryable pins the watermark on a
            # message that can never succeed, blocking every reply behind it.
            logger.warning(
                "reply_mailbox_poller: message_id=%s no longer exists (404) — treating as terminal, marking seen",
                message_id,
            )
            _mark_seen(message_id)
            return False
        logger.exception("reply_mailbox_poller: failed to process message id=%s — will retry next poll", message_id)
        return None


# ── Layer 1: fetch paths ──────────────────────────────────────────────────────

def _poll_via_history(service: Any, history_id: str, db: Session) -> Tuple[int, Optional[str], bool]:
    """
    Primary path. Returns (published_count, newest_history_id, had_retryable_failure).
    Raises on a stale/invalid cursor.
    """
    published = 0
    had_retryable_failure = False
    page_token = None
    newest_history_id = history_id

    while True:
        request = service.users().history().list(
            userId="me", startHistoryId=history_id, historyTypes=["messageAdded"],
            pageToken=page_token, maxResults=PAGE_SIZE,
        )
        response = request.execute()
        newest_history_id = response.get("historyId", newest_history_id)

        for record in response.get("history", []):
            for added in record.get("messagesAdded", []):
                message_id = added.get("message", {}).get("id")
                if not message_id:
                    continue
                result = _process_candidate_message(service, message_id, db)
                if result is True:
                    published += 1
                elif result is None:
                    had_retryable_failure = True

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return published, newest_history_id, had_retryable_failure


def _bootstrap_poll(service: Any, since_timestamp: Optional[int], db: Session) -> Tuple[int, Optional[str], bool]:
    """
    Fallback path — first ever run, or the saved historyId cursor expired.
    Bounded search-based catch-up, then captures a fresh historyId to resume
    incremental (_poll_via_history) mode on the next call.

    Returns (published_count, fresh_history_id, had_retryable_failure). When
    had_retryable_failure is True, fresh_history_id is always None and the
    caller must NOT save it or the fallback timestamp — bootstrapping again
    from the same since_timestamp next poll is what lets the failed
    message(s) be retried, since this query is a re-runnable search, not an
    incremental cursor.
    """
    published = 0
    had_retryable_failure = False
    page_token = None
    query = _UNREAD_REPLY_QUERY
    if since_timestamp:
        query = f"{query} after:{since_timestamp}"

    while True:
        response = service.users().messages().list(
            userId="me", q=query, pageToken=page_token, maxResults=PAGE_SIZE,
        ).execute()

        for msg_ref in response.get("messages", []):
            result = _process_candidate_message(service, msg_ref["id"], db)
            if result is True:
                published += 1
            elif result is None:
                had_retryable_failure = True

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    if had_retryable_failure:
        return published, None, True

    try:
        profile = service.users().getProfile(userId="me").execute()
        fresh_history_id = profile.get("historyId")
    except Exception:
        logger.exception("reply_mailbox_poller: failed to fetch a fresh historyId after bootstrap — will bootstrap again next poll")
        fresh_history_id = None

    return published, fresh_history_id, False


def poll_once(db: Session) -> int:
    """Runs one poll cycle. Returns the number of reply.received events published."""
    service = _build_gmail_service()
    if service is None:
        return 0

    history_id = _get_saved_history_id()
    poll_started_at = int(now().timestamp())

    if history_id:
        try:
            published, new_history_id, had_retryable_failure = _poll_via_history(service, history_id, db)
            if had_retryable_failure:
                logger.warning(
                    "reply_mailbox_poller: retryable failure during history poll — watermark held at %s, will retry next poll",
                    history_id,
                )
            else:
                _save_history_id(new_history_id)
                _save_fallback_timestamp(poll_started_at)
            if published:
                logger.info("reply_mailbox_poller: published %d reply.received event(s) via history", published)
            return published
        except Exception as exc:
            status = getattr(getattr(exc, "resp", None), "status", None)
            if status == 404:
                logger.warning("reply_mailbox_poller: history cursor expired/invalid — falling back to a bounded catch-up poll")
            else:
                logger.exception("reply_mailbox_poller: history.list failed")
                return 0

    # Bootstrap: no cursor yet (first run), or the cursor just expired above.
    fallback_timestamp = _get_fallback_timestamp()
    try:
        published, fresh_history_id, had_retryable_failure = _bootstrap_poll(service, fallback_timestamp, db)
    except Exception:
        logger.exception("reply_mailbox_poller: bootstrap poll failed")
        return 0

    if had_retryable_failure:
        logger.warning(
            "reply_mailbox_poller: retryable failure during bootstrap catch-up — cursor held back, will retry next poll",
        )
    else:
        _save_history_id(fresh_history_id)
        _save_fallback_timestamp(poll_started_at)
    if published:
        logger.info("reply_mailbox_poller: published %d reply.received event(s) via bootstrap catch-up", published)
    return published


def run_periodic(stop_event: threading.Event, interval_seconds: int = DEFAULT_INTERVAL_SECONDS) -> None:
    from src.core.database import get_db_context

    logger.info("reply_mailbox_poller: starting periodic poll every %ds", interval_seconds)
    while not stop_event.is_set():
        try:
            with get_db_context() as db:
                poll_once(db)
        except Exception:
            logger.exception("reply_mailbox_poller: poll failed — will retry next interval")
        stop_event.wait(interval_seconds)
    logger.info("reply_mailbox_poller: stopped")
