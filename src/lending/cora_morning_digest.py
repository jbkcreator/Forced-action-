"""Cora's 7:30 AM morning digest in her own channel (Runbook § 3, Line 4).

Content follows the Chunk 2 spec, Task E5 (Morning Brief): "Josh's morning operational summary, highlighting
overdue actions, new high-score leads, and today's calling queues". Cora's minute sweep calls
``post_morning_digest``. From 7:30 to noon ET it posts one digest a day:

  * Overdue actions: drafts waiting for approval, confirmation calls due today or earlier, open inbox items.
  * New high-score leads (last 24 hours): LendingFlow leads, closer-priority and FULL_MACHINE first (the intake
    classification that exists today; the Chunk 2 A0 1-10 intake score replaces it when built), plus web leads.
  * Today's calling queues: calls booked for today and the active dialer contacts per campaign.
  * Whether Cora is halted.

 ``lending.cora_morning_digests`` holds one row per ET day, written in the same transaction as
the post, so a restart or a second sweep never posts twice and a failed post is retried next minute. A Cora
started after noon skips that day rather than posting a stale morning digest.

The digest goes to Cora's internal channel only and is posted while Cora is halted (the halt stops sends to
borrowers). It shows references, first names and last-4 digits only: no emails, phone numbers or consent text.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from packages.agent_core.briefing import BriefSection, render_brief_blocks
from packages.agent_core.chatport import ChatPort
from src.lending.cora_inbox import slack_safe

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
_DUE_CONFIRMATIONS = text("""
    SELECT booking_ref, due_date, count(*) OVER () AS total FROM lending.confirmation_tasks
     WHERE completed_at IS NULL AND cancelled_at IS NULL AND due_date <= :day ORDER BY due_date LIMIT :limit
""")
_OPEN_INBOX = text("SELECT count(*) FROM lending.cora_inbox WHERE status IN ('new', 'surfaced')")
_NEW_LENDINGFLOW_LEADS = text("""
    SELECT l.id, l.first_name, l.loan_type, l.loan_amount_range, l.property_city, e.routing_tag,
           coalesce(e.closer_priority, false) AS closer_priority, count(*) OVER () AS total
      FROM lending.lendingflow_leads l LEFT JOIN lending.lendingflow_enrichment e ON e.lead_id = l.id
     WHERE l.received_at >= :since AND NOT l.suppressed
     ORDER BY coalesce(e.closer_priority, false) DESC, coalesce(e.routing_tag = 'FULL_MACHINE', false) DESC,
              l.received_at DESC
     LIMIT :limit
""")
_WEB_LEADS = text("""
    SELECT count(*) AS total, count(*) FILTER (WHERE ghl_status IN ('pending', 'failed')) AS not_in_ghl
      FROM lending.web_leads WHERE received_at >= :since
""")
_TODAYS_CALLS = text("""
    SELECT booking_ref, max(first_name) AS first_name, max(booked_by) AS booked_by,
           max(slot_start_utc) AS slot_start
      FROM lending.booking_messages
     WHERE slot_start_utc >= :start AND slot_start_utc < :end
     GROUP BY booking_ref ORDER BY slot_start LIMIT :limit
""")
_DIALER_QUEUES = text("""
    SELECT coalesce(campaign_tag, 'untagged') AS campaign, count(*) AS contacts
      FROM lending.dialer_load_records WHERE active
     GROUP BY coalesce(campaign_tag, 'untagged') ORDER BY contacts DESC
""")


@dataclass(frozen=True)
class DigestData:
    halted: bool
    halt_reason: str
    pending_drafts: list[dict[str, Any]]
    pending_total: int
    due_confirmations: list[dict[str, Any]]
    due_confirmations_total: int
    open_inbox: int
    new_leads: list[dict[str, Any]]
    new_leads_total: int
    web_leads: int
    web_leads_not_in_ghl: int
    todays_calls: list[dict[str, Any]]
    dialer_queues: list[dict[str, Any]]
    unavailable: tuple[str, ...] = ()


def _rows(db: Session, statement, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(statement, params).mappings()]


def _total(rows: list[dict[str, Any]]) -> int:
    return int(rows[0]["total"]) if rows else 0


def digest_day(now: datetime) -> Optional[date]:
    """The ET day a digest is due for at ``now``, or None outside 7:30 to noon ET."""
    local = now.astimezone(ET)
    return local.date() if DIGEST_TIME <= local.time() < LAST_POST_TIME else None


def _section(db: Session, name: str, read: Callable[[], Any], default: Any, unavailable: list[str]) -> Any:
    """Read one section in its own savepoint: a missing table or failed query blanks that section only, so the
    rest of the digest still posts (a table can be absent when its feature's migration has not run)."""
    try:
        with db.begin_nested():
            return read()
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.warning("[cora] morning digest section %s not available (%s)", name, type(exc).__name__)
        unavailable.append(name)
        return default


def gather(db: Session, day: date, now: datetime, *, halted: bool, halt_reason: str) -> DigestData:
    start = datetime.combine(day, time.min, ET).astimezone(timezone.utc)
    since = now - timedelta(hours=24)
    missing: list[str] = []
    no_web = {"total": 0, "not_in_ghl": 0}
    drafts = _section(db, "drafts", lambda: _rows(db, _PENDING_DRAFTS, {"limit": LIST_LIMIT}), [], missing)
    confirmations = _section(db, "confirmation calls", lambda: _rows(
        db, _DUE_CONFIRMATIONS, {"day": day, "limit": LIST_LIMIT}), [], missing)
    open_inbox = _section(db, "inbox", lambda: int(db.execute(_OPEN_INBOX).scalar() or 0), 0, missing)
    leads = _section(db, "LendingFlow leads", lambda: _rows(
        db, _NEW_LENDINGFLOW_LEADS, {"since": since, "limit": LIST_LIMIT}), [], missing)
    web = _section(db, "web leads", lambda: dict(db.execute(_WEB_LEADS, {"since": since}).mappings().one()),
                   no_web, missing)
    calls = _section(db, "booked calls", lambda: _rows(
        db, _TODAYS_CALLS, {"start": start, "end": start + timedelta(days=1), "limit": LIST_LIMIT}), [], missing)
    queues = _section(db, "dialer queues", lambda: _rows(db, _DIALER_QUEUES, {}), [], missing)
    return DigestData(
        halted=halted, halt_reason=halt_reason,
        pending_drafts=drafts, pending_total=_total(drafts),
        due_confirmations=confirmations, due_confirmations_total=_total(confirmations),
        open_inbox=open_inbox, new_leads=leads, new_leads_total=_total(leads),
        web_leads=int(web["total"]), web_leads_not_in_ghl=int(web["not_in_ghl"]),
        todays_calls=calls, dialer_queues=queues, unavailable=tuple(missing),
    )


def _more(shown: int, total: int) -> list[str]:
    return [f"…and {total - shown} more"] if total > shown else []


def _lead_line(lead: dict[str, Any]) -> str:
    # Lead fields come from the borrower's form: escaped so a "<!channel>" name cannot ping the channel.
    flag = ":fire: " if lead["closer_priority"] else (":star: " if lead["routing_tag"] == "FULL_MACHINE" else "")
    details = ", ".join(slack_safe(str(v)) for v in (lead["loan_type"], lead["loan_amount_range"],
                                                      lead["property_city"]) if v)
    return (f"{flag}lendingflow_lead:{lead['id']} {slack_safe(lead['first_name'] or 'lead')}"
            + (f" ({details})" if details else ""))


def render(day: date, data: DigestData) -> tuple[str, list[dict]]:
    """The digest's fallback text and Block Kit blocks."""
    status = (f":octagonal_sign: Halted ({data.halt_reason or 'no reason given'}). Approved drafts will not send "
              "until an approver says `resume`." if data.halted else ":large_green_circle: Running. Approved drafts send.")
    overdue = (
        [f"Draft #{d['action_id']} {d['tool_name']}" + (f" for {d['contact_ref']}" if d["contact_ref"] else "")
         + " waiting for approval" for d in data.pending_drafts]
        + _more(len(data.pending_drafts), data.pending_total)
        + [f"Confirmation call for booking {slack_safe(str(c['booking_ref']))} due {c['due_date']}"
           for c in data.due_confirmations]
        + _more(len(data.due_confirmations), data.due_confirmations_total)
        + ([f"{data.open_inbox} open inbox item(s); reply in the inbox digest thread to route or dismiss"]
           if data.open_inbox else [])
    )
    leads = [_lead_line(lead) for lead in data.new_leads] + _more(len(data.new_leads), data.new_leads_total)
    if data.web_leads:
        leads.append(f"{data.web_leads} web lead(s)" + (
            f", {data.web_leads_not_in_ghl} not in GHL yet" if data.web_leads_not_in_ghl else ""))
    calls = [f"{c['slot_start'].astimezone(ET):%I:%M %p} {slack_safe(c['first_name'] or 'lead')} "
             f"({slack_safe(c['booked_by'] or 'unknown')})" for c in data.todays_calls]
    queues = [f"Booked calls today: {len(data.todays_calls)}"] + calls + [
        f"Dialer {q['campaign']}: {q['contacts']} contact(s) loaded" for q in data.dialer_queues]
    cora_lines = [status] + ([f":warning: Not available today: {', '.join(data.unavailable)}"]
                             if data.unavailable else [])
    sections = [
        BriefSection("Cora", cora_lines),
        BriefSection("Overdue actions", overdue, "Nothing overdue."),
        BriefSection(f"New leads, last 24 hours ({data.new_leads_total + data.web_leads})", leads, "No new leads."),
        BriefSection("Today's calling queues", queues),
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
