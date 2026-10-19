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

from datetime import datetime
from typing import Any, Callable, Mapping, Optional

from config.lending_alarms import GHL_TIMEOUT_SECONDS

GHL_CONVERSATIONS_VERSION = "2021-04-15"


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


def call_messages_reader(account, *, http: Optional[Callable[..., Any]] = None
                         ) -> Callable[[str], list[dict[str, Any]]]:
    """``read(contact_id)`` returns every call message in the contact's conversations, each tagged with its
    ``conversationId``. Raises on an HTTP error; callers decide whether a failed lookup is fatal."""
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
            page = get(f"{_GHL_BASE}/conversations/{conversation['id']}/messages", headers=headers,
                       params={"type": "TYPE_CALL"})
            page.raise_for_status()
            messages = (page.json() or {}).get("messages") or []
            if isinstance(messages, dict):  # the spec nests the list: {"messages": {"messages": [...]}}
                messages = messages.get("messages") or []
            calls.extend({**message, "conversationId": message.get("conversationId") or conversation["id"]}
                         for message in messages if isinstance(message, dict))
        return calls

    return read
