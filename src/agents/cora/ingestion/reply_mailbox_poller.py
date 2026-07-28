"""
Real reply-mailbox ingestion via the Gmail API — a service account +
domain-wide delegation impersonating the monitored Google Workspace mailbox.

One-time setup outside this codebase (see docs/plans/cora_cold_outreach_completion_report.md
for the full checklist): a Google Cloud project with the Gmail API enabled,
a service account with a downloaded JSON key, and that service account's
Client ID authorized for domain-wide delegation in the Workspace Admin
console (Security > API controls > Domain-wide delegation) with EXACTLY
this scope:

    https://www.googleapis.com/auth/gmail.readonly

Deliberately read-only. Cora never marks a message read, moves it, or
modifies it in any way — the Gmail API has no way to do that under this
scope, and this module doesn't request a broader one. Dedup against
re-ingesting the same message on the next poll is tracked entirely on
Cora's own side (a Redis set of already-seen Gmail message ids), not by
mutating the mailbox.

Requires config/settings.py:
    CORA_GMAIL_SERVICE_ACCOUNT_KEY_PATH  - path to the service account JSON key file
    CORA_REPLY_MAILBOX_ADDRESS           - the Workspace address to impersonate

Publishes each new message as a real reply.received event via
reply_stub_producer's exact payload shape — opportunity_thread_id is left
None here; src.agents.cora.subgraphs.reply.py's _node_match_thread resolves
it downstream by matching from_address against store.find_opportunity_
thread_id_by_email. This module's only job is: list unread mail, decode it,
hand it off. Once a real mailbox exists, only this module needed to change
— reply.py and everything else was already built and tested against the
same payload shape via reply_stub_producer.
"""
from __future__ import annotations

import base64
import logging
import re
import threading
from email.utils import parseaddr
from typing import Any, Dict, List, Optional

from src.agents.cora.ingestion.reply_stub_producer import produce_stub_reply
from src.agents.cora.store import now

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
DEFAULT_INTERVAL_SECONDS = 3 * 60
_SEEN_KEY_PREFIX = "cora:gmail:seen:"
_SEEN_TTL_SECONDS = 30 * 24 * 3600  # 30 days — long enough that a slow poll gap never re-ingests

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


def poll_once(max_results: int = 20) -> int:
    """Fetches unread messages not yet seen, publishes each as a real reply.received event. Returns count published."""
    service = _build_gmail_service()
    if service is None:
        return 0

    try:
        response = service.users().messages().list(
            userId="me", q=_UNREAD_REPLY_QUERY, maxResults=max_results,
        ).execute()
    except Exception:
        logger.exception("reply_mailbox_poller: failed to list messages")
        return 0

    published = 0
    for msg_ref in response.get("messages", []):
        message_id = msg_ref["id"]
        if _already_seen(message_id):
            continue
        try:
            message = service.users().messages().get(userId="me", id=message_id, format="full").execute()
            headers = message.get("payload", {}).get("headers", [])
            _, from_address = parseaddr(_header(headers, "From"))
            subject = _header(headers, "Subject")
            body_text = _decode_body(message.get("payload", {}))

            produce_stub_reply({
                "opportunity_thread_id": None,
                "from_address": from_address,
                "subject": subject,
                "body_text": body_text,
                "received_at": now().isoformat(),
                "raw_headers": {h["name"]: h["value"] for h in headers},
            })
            _mark_seen(message_id)
            published += 1
        except Exception:
            logger.exception("reply_mailbox_poller: failed to process message id=%s — will retry next poll", message_id)

    if published:
        logger.info("reply_mailbox_poller: published %d reply.received event(s)", published)
    return published


def run_periodic(stop_event: threading.Event, interval_seconds: int = DEFAULT_INTERVAL_SECONDS) -> None:
    logger.info("reply_mailbox_poller: starting periodic poll every %ds", interval_seconds)
    while not stop_event.is_set():
        try:
            poll_once()
        except Exception:
            logger.exception("reply_mailbox_poller: poll failed — will retry next interval")
        stop_event.wait(interval_seconds)
    logger.info("reply_mailbox_poller: stopped")
