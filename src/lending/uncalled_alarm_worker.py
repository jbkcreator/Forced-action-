"""T-10: alarm Josh when a LendingFlow lead has not been reached by phone 2 and 5 minutes after it arrived.

    python -m src.lending.uncalled_alarm_worker            # loop
    python -m src.lending.uncalled_alarm_worker --once     # one cycle and exit

Each cycle copies every new, non-suppressed row of T-11's ``lending.lendingflow_leads`` into
``lending.uncalled_alarms`` (the stopwatch starts at the lead's ``received_at``; no age limit), then decides the
due ones. Outbound calls to the lead's phone since arrival are classified from BatchDialer CDRs
(``lending.call_dispositions``) and the lead's GHL call messages: connected -> closed, nothing sent; attempted ->
"NOT REACHED" alarms; none -> "no call" alarms. At 120 s an SMS to Josh; at 300 s an URGENT SMS plus a Slack post
to #lendingops. SMS go through GoHighLevel (``ghl_sms.GhlSmsSender``, single attempt).

Calls are only visible once they END, so a long call that starts before 2:00 still alarms; the wording says so.
A GHL lookup that fails counts as GHL finding nothing: a false alarm is better than a silent miss.
Each alarm is claimed (committed) before it is sent and is never re-sent. Logs carry lead ids only.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

from sqlalchemy import text

from config.lending_alarms import (
    BATCH_SIZE,
    ESCALATION_SECONDS,
    FIRST_ALARM_SECONDS,
    GHL_CONNECTED_STATUSES,
    GHL_TIMEOUT_SECONDS,
    KIND_NO_CALL,
    KIND_NOT_REACHED,
    NOT_CONNECTED_CODES,
    POLL_SECONDS,
    SLACK_TEXT,
    SMS_TEXT,
)
from config.settings import get_settings
from src.lending.db import lending_session
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

CONNECTED, ATTEMPTED, NONE = "connected", "attempted", "none"
GhlCallStatus = Callable[[str, datetime], str]  # (ghl_contact_id, since) -> CONNECTED / ATTEMPTED / NONE

_PICKUP = text("""
    INSERT INTO lending.uncalled_alarms (lendingflow_lead_id, lead_uuid, phone, arrived_at)
    SELECT l.id, l.lead_uuid::text, l.phone, l.received_at FROM lending.lendingflow_leads l
     WHERE NOT l.suppressed
       AND NOT EXISTS (SELECT 1 FROM lending.uncalled_alarms a WHERE a.lendingflow_lead_id = l.id)
    ON CONFLICT (lendingflow_lead_id) DO NOTHING
""")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def pick_up_new_leads(db) -> int:
    """Start a stopwatch for every non-suppressed LendingFlow lead not yet watched (no age limit)."""
    count = db.execute(_PICKUP).rowcount
    db.commit()
    return count


_CDR_CALLS = text("""
    SELECT count(*) AS attempts,
           count(*) FILTER (WHERE CASE WHEN disposition IS NOT NULL THEN NOT (disposition = ANY(:not_connected))
                                       ELSE COALESCE(talk_duration_sec, 0) > 0 END) AS connected
      FROM lending.call_dispositions
     WHERE phone = :phone AND direction = 'outbound' AND COALESCE(call_started_at, call_ended_at) >= :since
""")


def _cdr_status(db, phone: str, since: datetime) -> str:
    counts = db.execute(_CDR_CALLS, {"phone": phone, "since": since,
                                     "not_connected": list(NOT_CONNECTED_CODES)}).mappings().one()
    if counts["connected"]:
        return CONNECTED
    return ATTEMPTED if counts["attempts"] else NONE


def call_status(db, row: Mapping[str, Any], ghl_status: Optional[GhlCallStatus]) -> str:
    """CONNECTED if any source saw a connected call since arrival, else ATTEMPTED if any saw a try, else NONE."""
    cdr = _cdr_status(db, row["phone"], row["arrived_at"])
    if cdr == CONNECTED or ghl_status is None or not row["ghl_contact_id"]:
        return cdr
    try:
        ghl = ghl_status(row["ghl_contact_id"], row["arrived_at"])
    except Exception as exc:  # class only; a failed lookup must not stop the alarm
        logger.warning("[uncalled-alarm] lead=%s GHL call lookup failed (%s); using CDR only",
                       row["lead_uuid"], type(exc).__name__)
        return cdr
    if CONNECTED in (cdr, ghl):
        return CONNECTED
    return ATTEMPTED if ATTEMPTED in (cdr, ghl) else NONE


def _message_time(message: Mapping[str, Any]) -> Optional[datetime]:
    """``dateAdded`` as an aware datetime, or None when missing, unparseable or without a timezone."""
    try:
        added = datetime.fromisoformat(message.get("dateAdded") or "")
    except ValueError:
        return None
    return added if added.tzinfo else None


def _ghl_call_state(message: Mapping[str, Any]) -> Optional[str]:
    return (message.get("meta") or {}).get("callStatus") or message.get("callStatus")


def ghl_call_lookup(account, *, http: Optional[Callable[..., Any]] = None) -> GhlCallStatus:
    """GHL source: classify the outbound call messages dated at/after ``since`` in the contact's conversations.

    GET /conversations/search?contactId=… then GET /conversations/{id}/messages?type=TYPE_CALL (GHL API v2,
    Version 2021-04-15; scopes conversations.readonly + conversations/message.readonly). Field names
    (``conversations``, ``messages``, ``direction``, ``dateAdded``, ``meta.callStatus``) follow GHL's published
    OpenAPI spec and are UNVERIFIED against a live response; an unexpected shape finds no call.
    """
    from src.lending.ghl_account import ghl_headers
    from src.services.ghl_webhook import _GHL_BASE
    from src.utils.http_helpers import requests_get_with_retry

    get = http or (lambda url, **kw: requests_get_with_retry(url, max_retries=1, retry_delay=1,
                                                             timeout=GHL_TIMEOUT_SECONDS, **kw))
    headers = ghl_headers(account.api_key, "2021-04-15")

    def status(contact_id: str, since: datetime) -> str:
        found = get(f"{_GHL_BASE}/conversations/search", headers=headers,
                    params={"locationId": account.location_id, "contactId": contact_id})
        found.raise_for_status()
        attempted = False
        for conversation in (found.json() or {}).get("conversations") or []:
            page = get(f"{_GHL_BASE}/conversations/{conversation['id']}/messages", headers=headers,
                       params={"type": "TYPE_CALL"})
            page.raise_for_status()
            messages = (page.json() or {}).get("messages") or []
            if isinstance(messages, dict):  # the spec nests the list: {"messages": {"messages": [...]}}
                messages = messages.get("messages") or []
            for message in messages:
                added = _message_time(message)
                if message.get("direction") != "outbound" or added is None or added < since:
                    continue
                if _ghl_call_state(message) in GHL_CONNECTED_STATUSES:
                    return CONNECTED
                attempted = True
        return ATTEMPTED if attempted else NONE

    return status
