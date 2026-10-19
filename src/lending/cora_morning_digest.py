"""Cora's 7:30 AM morning digest in her own channel (Runbook § 3, Line 4).

Cora's minute sweep calls ``post_morning_digest``. From 7:30 to noon ET it posts one digest a day: drafts
waiting for approval, calls booked for today, web leads from the last 24 hours, open inbox items and whether
Cora is halted. ``lending.cora_morning_digests`` holds one row per ET day, written in the same transaction as
the post, so a restart or a second sweep never posts twice and a failed post is retried next minute. A Cora
started after noon skips that day rather than posting a stale morning digest.

The digest goes to Cora's internal channel only and is posted while Cora is halted (the halt stops sends to
borrowers). It shows references, first names and last-4 digits only: no emails, phone numbers or consent text.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from packages.agent_core.briefing import BriefSection, render_brief_blocks
from packages.agent_core.chatport import ChatPort

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
DIGEST_TIME = time(7, 30)
LAST_POST_TIME = time(12, 0)
LIST_LIMIT = 10

_CLAIM = text("""
    INSERT INTO lending.cora_morning_digests (digest_date) VALUES (:day)
    ON CONFLICT (digest_date) DO NOTHING RETURNING digest_date
""")
_RECORD_POST = text("""
    UPDATE lending.cora_morning_digests SET slack_channel = :channel, slack_ts = :ts, posted_at = CURRENT_TIMESTAMP
     WHERE digest_date = :day
""")
_PENDING_DRAFTS = text("""
    SELECT action_id, tool_name, contact_ref, count(*) OVER () AS total
      FROM lending.pending_actions WHERE status = 'pending' ORDER BY created_at LIMIT :limit
""")
_TODAYS_CALLS = text("""
    SELECT booking_ref, max(first_name) AS first_name, max(booked_by) AS booked_by,
           max(slot_start_utc) AS slot_start
      FROM lending.booking_messages
     WHERE slot_start_utc >= :start AND slot_start_utc < :end
     GROUP BY booking_ref ORDER BY slot_start LIMIT :limit
""")
_WEB_LEADS = text("""
    SELECT count(*) AS total, count(*) FILTER (WHERE ghl_status IN ('pending', 'failed')) AS not_in_ghl
      FROM lending.web_leads WHERE received_at >= :since
""")
_OPEN_INBOX = text("SELECT count(*) FROM lending.cora_inbox WHERE status IN ('new', 'surfaced')")


@dataclass(frozen=True)
class DigestData:
    halted: bool
    halt_reason: str
    pending_drafts: list[dict[str, Any]]
    pending_total: int
    todays_calls: list[dict[str, Any]]
    web_leads: int
    web_leads_not_in_ghl: int
    open_inbox: int


def digest_day(now: datetime) -> Optional[date]:
    """The ET day a digest is due for at ``now``, or None outside 7:30 to noon ET."""
    local = now.astimezone(ET)
    return local.date() if DIGEST_TIME <= local.time() < LAST_POST_TIME else None


def gather(db: Session, day: date, now: datetime, *, halted: bool, halt_reason: str) -> DigestData:
    start = datetime.combine(day, time.min, ET).astimezone(timezone.utc)
    drafts = [dict(row) for row in db.execute(_PENDING_DRAFTS, {"limit": LIST_LIMIT}).mappings()]
    calls = [dict(row) for row in db.execute(
        _TODAYS_CALLS, {"start": start, "end": start + timedelta(days=1), "limit": LIST_LIMIT}).mappings()]
    leads = db.execute(_WEB_LEADS, {"since": now - timedelta(hours=24)}).mappings().one()
    return DigestData(
        halted=halted, halt_reason=halt_reason, pending_drafts=drafts,
        pending_total=int(drafts[0]["total"]) if drafts else 0, todays_calls=calls,
        web_leads=int(leads["total"]), web_leads_not_in_ghl=int(leads["not_in_ghl"]),
        open_inbox=int(db.execute(_OPEN_INBOX).scalar() or 0),
    )


def _more(shown: int, total: int) -> list[str]:
    return [f"…and {total - shown} more"] if total > shown else []


def render(day: date, data: DigestData) -> tuple[str, list[dict]]:
    """The digest's fallback text and Block Kit blocks."""
    status = (f":octagonal_sign: Halted ({data.halt_reason or 'no reason given'}). Approved drafts will not send "
              "until an approver says `resume`." if data.halted else ":large_green_circle: Running. Approved drafts send.")
    drafts = [f"#{d['action_id']} {d['tool_name']}" + (f" for {d['contact_ref']}" if d["contact_ref"] else "")
              for d in data.pending_drafts] + _more(len(data.pending_drafts), data.pending_total)
    calls = [f"{c['slot_start'].astimezone(ET):%I:%M %p} {c['first_name'] or 'lead'} ({c['booked_by'] or 'unknown'})"
             for c in data.todays_calls]
    leads = [f"{data.web_leads} new"] + (
        [f"{data.web_leads_not_in_ghl} not in GHL yet"] if data.web_leads_not_in_ghl else []) if data.web_leads else []
    inbox = [f"{data.open_inbox} open item(s); reply in the inbox digest thread to route or dismiss"] if data.open_inbox else []
    sections = [
        BriefSection("Cora", [status]),
        BriefSection(f"Drafts waiting for approval ({data.pending_total})", drafts, "No drafts waiting."),
        BriefSection(f"Calls booked today ({len(data.todays_calls)})", calls, "No calls booked today."),
        BriefSection("Web leads, last 24 hours", leads, "No new web leads."),
        BriefSection("Inbox", inbox, "Inbox is clear."),
    ]
    header = f"Good morning. Cora's digest for {day:%A %B} {day.day}"
    return header, render_brief_blocks(header, sections)


def post_morning_digest(engine: Engine, chat: ChatPort, channel: Optional[str], *, halted: bool,
                        halt_reason: str = "", now: Optional[datetime] = None) -> bool:
    """Post today's digest if it is due and not yet posted. Returns True when a digest was posted."""
    now = now or datetime.now(timezone.utc)
    day = digest_day(now)
    if day is None or not channel:
        return False
    with Session(engine) as db, db.begin():
        if db.execute(_CLAIM, {"day": day}).scalar() is None:
            return False
        header, blocks = render(day, gather(db, day, now, halted=halted, halt_reason=halt_reason))
        result = chat.post(text=header, blocks=blocks, channel=channel)
        if not result.ok:
            # Raising rolls the claim back, so the next minute's sweep tries again.
            raise RuntimeError(f"morning digest not posted ({result.error or 'no Slack response'})")
        db.execute(_RECORD_POST, {"channel": result.channel or channel, "ts": result.ts, "day": day})
    logger.info("[cora] morning digest posted for %s", day.isoformat())
    return True
