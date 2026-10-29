"""David's activity for one ET day: calls, voicemails, texts and emails he made in GoHighLevel, the day's bookings
and show rate, and the leads he flagged for Josh.

Calls, texts and emails come from the GHL conversations API (Version 2021-04-15), not lending.call_dispositions:
the GHL call import stores no GHL user, so a GHL call there cannot be credited to David. A message counts as his
when its ``userId`` is LENDING_DAVID_GHL_USER_ID; workflow and campaign messages carry no ``userId`` and are left out.

* conversations are read newest first (``sortBy=last_message_date``) and the walk stops at the first one whose last
  message is before the day; each conversation's messages are read newest first until one is before the day
* call outcome and talk time use ``ghl_calls.call_state`` / ``call_duration_seconds`` (the shapes seen live)
* Booked / Held come from lending.ghl_stage_events (the GHL stage webhook) and cover every caller: a stage event
  does not say who booked the call. Show rate = Held today / Booked today, as on the scoreboard
* flagged leads = contacts carrying LENDING_FLAGGED_TAG now (GHL keeps no date for a tag)

UNVERIFIED (2026-10-29): ``userId`` on a hand-sent email and on a call, and the tag filter shape of the contact search.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Iterator, Mapping, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import DEFAULT_TZ
from config.lending_dispositions import (
    BOOKED_STAGE_KEYS, GHL_CONNECTED_STATES, GHL_VOICEMAIL_STATES, SHOWED_STAGE_KEYS,
)
from src.lending.ghl_calls import GHL_CONVERSATIONS_VERSION, call_duration_seconds, call_state, message_time

logger = logging.getLogger(__name__)

CONVERSATION_PAGE_SIZE = 100
MAX_CONVERSATION_PAGES = 20   # 2,000 conversations touched in one day: far above the lending volume
MAX_MESSAGE_PAGES = 20
CONTACT_PAGE_SIZE = 100
MAX_CONTACT_PAGES = 5
MAX_FLAGGED_SHOWN = 15

CALL, SMS, EMAIL = "TYPE_CALL", "TYPE_SMS", "TYPE_EMAIL"

Get = Callable[..., Any]   # (url, **kwargs) -> response
Post = Callable[..., Any]


@dataclass(frozen=True)
class FlaggedLead:
    contact_id: str
    name: str


@dataclass
class DavidActivity:
    day: date
    dials: int = 0
    contacts_reached: int = 0
    voicemails: int = 0
    inbound_answered: int = 0
    talk_seconds: int = 0
    texts: int = 0
    emails: int = 0
    booked: int = 0
    held: int = 0
    flagged: list[FlaggedLead] = field(default_factory=list)
    ghl_read_failed: bool = False

    @property
    def show_rate(self) -> Optional[float]:
        """Held today / Booked today; None when nothing was booked. Different calls, not a conversion rate."""
        return self.held / self.booked if self.booked else None


def day_bounds(day: date) -> tuple[datetime, datetime]:
    tz = ZoneInfo(DEFAULT_TZ)
    return datetime.combine(day, time.min, tzinfo=tz), datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz)


def count_messages(activity: DavidActivity, messages: list[Mapping[str, Any]], user_id: str,
                   start: datetime, end: datetime) -> None:
    """Add David's messages dated within [start, end) to ``activity``."""
    for message in messages:
        if not user_id or message.get("userId") != user_id:
            continue
        sent = message_time(message)
        if sent is None or not (start <= sent < end):
            continue
        kind, outbound = message.get("messageType"), message.get("direction") == "outbound"
        if kind == CALL:
            state = (call_state(message) or "").lower()
            if outbound:
                activity.dials += 1
                activity.voicemails += state in GHL_VOICEMAIL_STATES
                activity.contacts_reached += state in GHL_CONNECTED_STATES
            else:
                activity.inbound_answered += state in GHL_CONNECTED_STATES
            activity.talk_seconds += call_duration_seconds(message) or 0
        elif outbound and kind == SMS:
            activity.texts += 1
        elif outbound and kind == EMAIL:
            activity.emails += 1


def _conversation_ms(conversation: Mapping[str, Any]) -> Optional[int]:
    try:
        return int(conversation.get("lastMessageDate"))
    except (TypeError, ValueError):
        return None


def conversations_since(get: Get, base: str, headers: Mapping[str, str], location_id: str,
                        start: datetime) -> Iterator[str]:
    """Ids of the conversations whose last message is at or after ``start``, newest first."""
    start_ms = int(start.timestamp() * 1000)
    cursor: Optional[int] = None
    for _ in range(MAX_CONVERSATION_PAGES):
        params = {"locationId": location_id, "limit": CONVERSATION_PAGE_SIZE, "sortBy": "last_message_date",
                  "sort": "desc", **({"startAfterDate": cursor} if cursor else {})}
        response = get(f"{base}/conversations/search", headers=headers, params=params)
        response.raise_for_status()
        page = (response.json() or {}).get("conversations") or []
        for conversation in page:
            last = _conversation_ms(conversation)
            if last is not None and last < start_ms:
                return
            yield conversation["id"]
        last_on_page = _conversation_ms(page[-1]) if page else None
        if len(page) < CONVERSATION_PAGE_SIZE or last_on_page is None or last_on_page == cursor:
            return
        cursor = last_on_page
    logger.warning("[david-activity] stopped after %d pages of conversations", MAX_CONVERSATION_PAGES)


def messages_since(get: Get, base: str, headers: Mapping[str, str], conversation_id: str,
                   start: datetime) -> list[dict[str, Any]]:
    """A conversation's messages from ``start`` on (pages are newest first; stops at the first older message)."""
    found: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    for _ in range(MAX_MESSAGE_PAGES):
        params = {"limit": 100, **({"lastMessageId": cursor} if cursor else {})}
        response = get(f"{base}/conversations/{conversation_id}/messages", headers=headers, params=params)
        response.raise_for_status()
        body = response.json() or {}
        listing = body.get("messages") or []
        if isinstance(listing, dict):  # the spec nests the list: {"messages": {"messages": [...], "nextPage": ..}}
            body, listing = listing, listing.get("messages") or []
        batch = [m for m in listing if isinstance(m, dict)]
        for message in batch:
            sent = message_time(message)
            if sent is not None and sent < start:
                return found
            found.append(message)
        next_cursor = body.get("lastMessageId")
        if not (body.get("nextPage") and batch and next_cursor and next_cursor != cursor):
            return found
        cursor = next_cursor
    logger.warning("[david-activity] conversation %s: stopped after %d pages", conversation_id, MAX_MESSAGE_PAGES)
    return found


def flagged_leads(post: Post, base: str, headers: Mapping[str, str], location_id: str, tag: str) -> list[FlaggedLead]:
    """Contacts carrying ``tag`` now. Filter shape per the GHL v2 contact search; confirm on first live run."""
    leads: list[FlaggedLead] = []
    for page in range(1, MAX_CONTACT_PAGES + 1):
        response = post(f"{base}/contacts/search", headers=headers,
                        json={"locationId": location_id, "page": page, "pageLimit": CONTACT_PAGE_SIZE,
                              "filters": [{"field": "tags", "operator": "contains", "value": tag}]})
        response.raise_for_status()
        contacts = (response.json() or {}).get("contacts") or []
        for c in contacts:
            if c.get("id"):
                name = " ".join(p for p in (c.get("firstName"), c.get("lastName")) if p) or c.get("contactName") or "(no name)"
                leads.append(FlaggedLead(c["id"], str(name)))
        if len(contacts) < CONTACT_PAGE_SIZE:
            break
    return leads


def stage_counts(db, day: date) -> tuple[int, int]:
    """(booked, held) stage entries on ``day``. A missing table or failed query reports 0 / 0 rather than losing
    the rest of the summary; the savepoint keeps the outer transaction usable."""
    start, end = day_bounds(day)
    try:
        with db.begin_nested():
            row = db.execute(
                text("SELECT count(*) FILTER (WHERE stage_key = ANY(:booked)), "
                     "count(*) FILTER (WHERE stage_key = ANY(:held)) "
                     "FROM lending.ghl_stage_events WHERE event_at >= :start AND event_at < :end"),
                {"booked": sorted(BOOKED_STAGE_KEYS), "held": sorted(SHOWED_STAGE_KEYS), "start": start, "end": end},
            ).one()
    except Exception as exc:
        logger.warning("[david-activity] stage query failed, reporting 0: %s", type(exc).__name__)
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def build_activity(db, day: date, *, user_id: str, flagged_tag: str, get: Get, post: Post, base: str,
                   headers: Mapping[str, str], location_id: str) -> DavidActivity:
    """The day's numbers. A failed GHL read marks ``ghl_read_failed`` and keeps whatever was counted."""
    activity = DavidActivity(day)
    activity.booked, activity.held = stage_counts(db, day)
    start, end = day_bounds(day)
    try:
        for conversation_id in conversations_since(get, base, headers, location_id, start):
            count_messages(activity, messages_since(get, base, headers, conversation_id, start), user_id, start, end)
    except Exception as exc:  # class only: GHL errors can echo request detail
        activity.ghl_read_failed = True
        logger.warning("[david-activity] GHL message read failed for %s: %s", day, type(exc).__name__)
    if flagged_tag:
        try:
            activity.flagged = flagged_leads(post, base, headers, location_id, flagged_tag)
        except Exception as exc:
            activity.ghl_read_failed = True
            logger.warning("[david-activity] GHL flagged-lead read failed for %s: %s", day, type(exc).__name__)
    return activity


def ghl_transport(api_key: str) -> tuple[Get, Post, Mapping[str, str]]:
    from src.lending.ghl_account import ghl_headers
    from src.services import ghl_webhook

    return (lambda url, **kw: ghl_webhook._ghl_request("GET", url, **kw),
            lambda url, **kw: ghl_webhook._ghl_request("POST", url, **kw),
            ghl_headers(api_key, GHL_CONVERSATIONS_VERSION))


def _minutes(seconds: int) -> str:
    return f"{seconds // 60}m" if seconds < 3600 else f"{seconds // 3600}h {seconds % 3600 // 60}m"


def format_slack(a: DavidActivity, contact_url: Callable[[str], Optional[str]] = lambda _id: None) -> str:
    show = f"{a.show_rate:.0%}" if a.show_rate is not None else "–"
    lines = [
        f"*David · end of day · {a.day:%a %b} {a.day.day}, {a.day.year}*",
        f"Dials {a.dials} · Contacts reached {a.contacts_reached} · Voicemails {a.voicemails} · "
        f"Inbound answered {a.inbound_answered} · Talk time {_minutes(a.talk_seconds)}",
        f"Texts {a.texts} · Emails {a.emails}",
        f"Booked {a.booked} · Held {a.held} · Show rate {show} _(all callers: GHL stage moves today)_",
    ]
    if a.flagged:
        lines += ["", f"*Flagged for Josh ({len(a.flagged)})*"]
        for lead in a.flagged[:MAX_FLAGGED_SHOWN]:
            url = contact_url(lead.contact_id)
            name = lead.name.replace("<", "").replace(">", "").replace("|", "")
            lines.append(f"• <{url}|{name}>" if url else f"• {name}")
        if len(a.flagged) > MAX_FLAGGED_SHOWN:
            lines.append(f"... and {len(a.flagged) - MAX_FLAGGED_SHOWN} more")
    else:
        lines += ["", "No leads flagged."]
    if a.ghl_read_failed:
        lines += ["", ":warning: GoHighLevel could not be read in full: the GHL numbers may be low."]
    return "\n".join(lines)
