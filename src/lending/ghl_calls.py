"""Calls as GoHighLevel records them: the call messages in a contact's conversations.

GET /conversations/search?contactId=… then GET /conversations/{id}/messages?type=TYPE_CALL (GHL API v2, Version
2021-04-15; scopes conversations.readonly + conversations/message.readonly). Checked against a live call on
2026-10-19: ``conversations``, ``messages`` (nested), ``id``, ``direction``, ``dateAdded`` as documented; the call
outcome is in ``meta.call.status`` / ``status``, not the documented ``meta.callStatus`` (both are read). A connected
call's duration field has not been seen live yet.

Used by the uncalled-lead alarm (was the lead called?) and the follow-up call import (every attempt, for the
LendingFlow replacement rule).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Mapping, Optional

from config.lending_alarms import GHL_TIMEOUT_SECONDS
from config.lending_followup import RECENT_CALLERS_LIMIT

logger = logging.getLogger(__name__)

GHL_CONVERSATIONS_VERSION = "2021-04-15"
# A conversation's messages come in pages (20 by default) chained by ``lastMessageId`` while ``nextPage`` is true.
# The cap only guards against a cursor that never ends; 50 pages is far more calls than a 14-day cadence makes.
MAX_MESSAGE_PAGES = 50


def message_time(message: Mapping[str, Any]) -> Optional[datetime]:
    """``dateAdded`` as an aware datetime, or None when missing, unparseable or without a timezone."""
    try:
        added = datetime.fromisoformat(message.get("dateAdded") or "")
    except ValueError:
        return None
    return added if added.tzinfo else None


def call_state(message: Mapping[str, Any]) -> Optional[str]:
    """GHL's call outcome (e.g. completed, no-answer, busy, failed).

    A live response (2026-10-19) carries it in ``meta.call.status`` and the message's own ``status``, not the
    ``meta.callStatus`` the published spec shows; every shape is read, the documented one first."""
    meta = message.get("meta") or {}
    return (meta.get("callStatus") or (meta.get("call") or {}).get("status")
            or message.get("callStatus") or message.get("status"))


def call_duration_seconds(message: Mapping[str, Any]) -> Optional[int]:
    """Talk time when GHL reports it (``meta.call.duration`` or ``meta.callDuration``); None when it does not."""
    meta = message.get("meta") or {}
    value = (meta.get("call") or {}).get("duration", meta.get("callDuration"))
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None


def recent_callers_reader(account, *, http: Optional[Callable[..., Any]] = None,
                          limit: int = RECENT_CALLERS_LIMIT) -> Callable[[datetime], list[tuple[str, str]]]:
    """``read(since)`` returns ``(ghl_contact_id, phone)`` for the conversations whose last message is a call that
    is newer than ``since``, newest first, one page of at most ``limit``. This is how callers who are not leads (so
    have no lead row to look them up by) are found. Checked live on 2026-10-26: the search takes
    ``lastMessageType=TYPE_CALL`` with ``sort=desc&sortBy=last_message_date`` and returns each conversation's
    ``contactId``, ``phone``, ``lastMessageDate`` (epoch ms), ``lastMessageDirection`` and ``lastCallStatus``.
    Raises on an HTTP error; the caller decides whether that is fatal."""
    from src.lending.ghl_account import ghl_headers
    from src.services.ghl_webhook import _GHL_BASE
    from src.utils.http_helpers import requests_get_with_retry

    get = http or (lambda url, **kw: requests_get_with_retry(url, max_retries=1, retry_delay=1,
                                                             timeout=GHL_TIMEOUT_SECONDS, **kw))
    headers = ghl_headers(account.api_key, GHL_CONVERSATIONS_VERSION)

    def read(since: datetime) -> list[tuple[str, str]]:
        found = get(f"{_GHL_BASE}/conversations/search", headers=headers,
                    params={"locationId": account.location_id, "lastMessageType": "TYPE_CALL", "sort": "desc",
                            "sortBy": "last_message_date", "limit": limit})
        found.raise_for_status()
        since_ms = since.timestamp() * 1000
        callers: list[tuple[str, str]] = []
        for conversation in (found.json() or {}).get("conversations") or []:
            contact_id, phone = conversation.get("contactId"), conversation.get("phone")
            last = conversation.get("lastMessageDate")
            if contact_id and phone and isinstance(last, (int, float)) and last >= since_ms:
                callers.append((contact_id, phone))
        return callers

    return read


def call_messages_reader(account, *, http: Optional[Callable[..., Any]] = None
                         ) -> Callable[[str], list[dict[str, Any]]]:
    """``read(contact_id)`` returns every call message in the contact's conversations, each tagged with its
    ``conversationId``, following each conversation's pages to the end. Raises on an HTTP error; callers decide
    whether a failed lookup is fatal."""
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
        calls: list[dict[str, Any]] = []
        for conversation in (found.json() or {}).get("conversations") or []:
            calls.extend({**message, "conversationId": message.get("conversationId") or conversation["id"]}
                         for message in _conversation_calls(conversation["id"]))
        return calls

    def _conversation_calls(conversation_id: str) -> list[dict[str, Any]]:
        return conversation_messages(get, headers, conversation_id, "TYPE_CALL")

    return read


def conversation_messages(get: Callable[..., Any], headers: Mapping[str, str], conversation_id: str,
                          message_type: Optional[str] = None) -> list[dict[str, Any]]:
    """Every message in one conversation (of ``message_type`` when given), following its pages to the end. Raises
    on an HTTP error."""
    from src.services.ghl_webhook import _GHL_BASE

    messages: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    for _ in range(MAX_MESSAGE_PAGES):
        params = {**({"type": message_type} if message_type else {}), **({"lastMessageId": cursor} if cursor else {})}
        page = get(f"{_GHL_BASE}/conversations/{conversation_id}/messages", headers=headers, params=params)
        page.raise_for_status()
        body = page.json() or {}
        listing = body.get("messages") or []
        if isinstance(listing, dict):  # the spec nests the list: {"messages": {"messages": [...], "nextPage": ..}}
            body, listing = listing, listing.get("messages") or []
        batch = [message for message in listing if isinstance(message, dict)]
        messages.extend(batch)
        next_cursor = body.get("lastMessageId")
        if not (body.get("nextPage") and batch and next_cursor and next_cursor != cursor):
            return messages
        cursor = next_cursor
    logger.warning("[ghl-messages] conversation %s: stopped after %d pages", conversation_id, MAX_MESSAGE_PAGES)
    return messages
