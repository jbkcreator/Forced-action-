"""Cora's 7:30 AM morning digest in her own channel (Runbook § 3, Line 4).

Content follows the Chunk 2 spec, Task E5 (Morning Brief): "Josh's morning operational summary, highlighting
overdue actions, new high-score leads, and today's calling queues". Cora's minute sweep calls
``post_morning_digest``. From 7:30 to noon ET it posts one digest a day:

  * Status: whether Cora is halted.
  * Needs your action: drafts waiting for approval, confirmation calls due today or earlier, open inbox items.
  * New leads (last 24 hours): LendingFlow leads, closer-priority and FULL_MACHINE first (the intake
    classification that exists today; the Chunk 2 A0 1-10 intake score replaces it when built), plus web leads.
  * Today's calls: calls booked for today. Dialer lists ready: active dialer contacts per campaign.
  * Loan files with no lender yet: the routing hold queue.

The digest is written for the client, not for a developer: first names, plain labels, how long something has waited
and where to act. Row ids, tool names and booking references never appear.

 ``lending.cora_morning_digests`` holds one row per ET day, written in the same transaction as
the post, so a restart or a second sweep never posts twice and a failed post is retried next minute. A Cora
started after noon skips that day rather than posting a stale morning digest.

The digest goes to Cora's internal channel only and is posted while Cora is halted (the halt stops sends to
borrowers). It shows first names and last-4 digits only: no emails, phone numbers or consent text.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
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
INBOX_PREVIEW = 3
INBOX_SUMMARY_MAX = 120

_CLAIM = text("""
    INSERT INTO lending.cora_morning_digests (digest_date) VALUES (:day)
    ON CONFLICT (digest_date) DO NOTHING RETURNING digest_date
""")
_RECORD_POST = text("""
    UPDATE lending.cora_morning_digests SET slack_channel = :channel, slack_ts = :ts, posted_at = CURRENT_TIMESTAMP
     WHERE digest_date = :day
""")
_PENDING_DRAFTS = text("""
    SELECT action_id, tool_name, summary, created_at, count(*) OVER () AS total
      FROM lending.pending_actions WHERE status = 'pending' ORDER BY created_at LIMIT :limit
""")
_DUE_CONFIRMATIONS = text("""
    SELECT t.booking_ref, t.due_date, b.first_name, b.slot_start, count(*) OVER () AS total
      FROM lending.confirmation_tasks t
      LEFT JOIN (SELECT booking_ref, max(first_name) AS first_name, max(slot_start_utc) AS slot_start
                   FROM lending.booking_messages GROUP BY booking_ref) b ON b.booking_ref = t.booking_ref
     WHERE t.completed_at IS NULL AND t.cancelled_at IS NULL AND t.due_date <= :day
     ORDER BY t.due_date LIMIT :limit
""")
_OPEN_INBOX = text("""
    SELECT summary, payload, count(*) OVER () AS total FROM lending.cora_inbox
     WHERE status IN ('new', 'surfaced') ORDER BY id LIMIT :limit
""")
_NEW_LENDINGFLOW_LEADS = text("""
    SELECT l.id, l.first_name, l.loan_type, l.loan_amount_range, l.property_city, e.routing_tag,
           coalesce(e.closer_priority, false) AS closer_priority, count(*) OVER () AS total
      FROM lending.lendingflow_leads l LEFT JOIN lending.lendingflow_enrichment e ON e.lead_id = l.id
     WHERE l.received_at >= :since AND NOT l.suppressed AND NOT l.is_test
     ORDER BY coalesce(e.closer_priority, false) DESC, coalesce(e.routing_tag = 'FULL_MACHINE', false) DESC,
              l.received_at DESC
     LIMIT :limit
""")
_WEB_LEADS = text("""
    SELECT count(*) AS total, count(*) FILTER (WHERE ghl_status IN ('pending', 'failed')) AS not_in_ghl
      FROM lending.web_leads WHERE received_at >= :since AND NOT is_test
""")
_TODAYS_CALLS = text("""
    SELECT booking_ref, max(first_name) AS first_name, max(booked_by) AS booked_by,
           max(slot_start_utc) AS slot_start
      FROM lending.booking_messages
     WHERE slot_start_utc >= :start AND slot_start_utc < :end
     GROUP BY booking_ref ORDER BY slot_start LIMIT :limit
""")
_HOLD_QUEUE = text("""
    SELECT count(*) FILTER (WHERE status = 'held') AS held, count(*) FILTER (WHERE status = 'exhausted') AS exhausted
      FROM lending.file_routing WHERE status IN ('held', 'exhausted') AND NOT is_test
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
    inbox_items: list[dict[str, Any]] = field(default_factory=list)
    held_files: int = 0
    exhausted_files: int = 0


def _rows(db: Session, statement, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(statement, params).mappings()]


def _total(rows: list[dict[str, Any]]) -> int:
    return int(rows[0]["total"]) if rows else 0


def _as_datetime(value: Any) -> Optional[datetime]:
    """A timestamp as an aware UTC datetime (Postgres returns one; a naive or text value is taken as UTC)."""
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _as_date(value: Any) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


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
    no_hold = {"held": 0, "exhausted": 0}
    drafts = _section(db, "approval drafts", lambda: [
        {**row, "created_at": _as_datetime(row["created_at"])}
        for row in _rows(db, _PENDING_DRAFTS, {"limit": LIST_LIMIT})], [], missing)
    confirmations = _section(db, "confirmation calls", lambda: [
        {**row, "due_date": _as_date(row["due_date"]), "slot_start": _as_datetime(row["slot_start"])}
        for row in _rows(db, _DUE_CONFIRMATIONS, {"day": day, "limit": LIST_LIMIT})],
        [], missing)
    inbox = _section(db, "inbox", lambda: _rows(db, _OPEN_INBOX, {"limit": INBOX_PREVIEW}), [], missing)
    leads = _section(db, "LendingFlow leads", lambda: _rows(
        db, _NEW_LENDINGFLOW_LEADS, {"since": since, "limit": LIST_LIMIT}), [], missing)
    web = _section(db, "web leads", lambda: dict(db.execute(_WEB_LEADS, {"since": since}).mappings().one()),
                   no_web, missing)
    calls = _section(db, "booked calls", lambda: [
        {**row, "slot_start": _as_datetime(row["slot_start"])}
        for row in _rows(db, _TODAYS_CALLS, {"start": start, "end": start + timedelta(days=1), "limit": LIST_LIMIT})], [], missing)
    queues = _section(db, "dialer queues", lambda: _rows(db, _DIALER_QUEUES, {}), [], missing)
    hold = _section(db, "loan files without a lender",
                    lambda: dict(db.execute(_HOLD_QUEUE).mappings().one()), no_hold, missing)
    return DigestData(
        halted=halted, halt_reason=halt_reason,
        pending_drafts=drafts, pending_total=_total(drafts),
        due_confirmations=confirmations, due_confirmations_total=_total(confirmations),
        open_inbox=_total(inbox), new_leads=leads, new_leads_total=_total(leads),
        web_leads=int(web["total"]), web_leads_not_in_ghl=int(web["not_in_ghl"]),
        todays_calls=calls, dialer_queues=queues, unavailable=tuple(missing), inbox_items=inbox,
        held_files=int(hold["held"] or 0), exhausted_files=int(hold["exhausted"] or 0),
    )


def _plural(count: int, word: str) -> str:
    return f"{count} {word}{'' if count == 1 else 's'}"


def _more(shown: int, total: int) -> list[str]:
    return [f"…and {total - shown} more"] if total > shown else []


def _clock(moment: datetime) -> str:
    local = moment.astimezone(ET)
    return f"{local.hour % 12 or 12}:{local:%M %p}"


def _age(since: Optional[datetime], now: datetime) -> str:
    if since is None:
        return ""
    hours = int((now - since).total_seconds() // 3600)
    if hours < 1:
        return "less than an hour"
    return _plural(hours, "hour") if hours < 24 else _plural(hours // 24, "day")


_EMAIL_IN_BRACKETS = re.compile(r"\s*<[^>]*@[^>]*>")
_AFTER_HOURS_DETAIL = re.compile(r",\s*drafted from.*$")
_TOOL_LABELS = {"after_hours_reply": "After-hours voicemail reply"}


def _draft_line(draft: dict[str, Any], now: datetime) -> str:
    """What the draft is, from the sentence written for its approval card (a name and the last 4 digits), with the
    email address and the send-time detail dropped. A draft with no sentence falls back to a plain label."""
    tool = str(draft["tool_name"] or "")
    summary = _AFTER_HOURS_DETAIL.sub("", _EMAIL_IN_BRACKETS.sub("", str(draft["summary"] or ""))).split(";")[0].strip()
    what = summary or _TOOL_LABELS.get(tool) or tool.replace("_", " ").capitalize() or "Message"
    age = _age(draft["created_at"], now)
    return f"• {slack_safe(what)}." + (f" Waiting {age}." if age else "")


def _confirmation_line(task: dict[str, Any], day: date) -> str:
    who = slack_safe(task["first_name"] or "Booked caller")
    slot = task["slot_start"].astimezone(ET) if task["slot_start"] else None
    when = f"call on {slot:%a, %b} {slot.day} at {_clock(slot)}" if slot else "confirmation call"
    late = (day - task["due_date"]).days
    return f"• {who}: {when}. " + ("Due today." if late <= 0 else f"Late by {_plural(late, 'day')}.")


def _lead_line(lead: dict[str, Any]) -> str:
    # Lead fields come from the borrower's form: escaped so a "<!channel>" name cannot ping the channel.
    flag = ":fire:" if lead["closer_priority"] else (":star:" if lead["routing_tag"] == "FULL_MACHINE" else "•")
    details = ", ".join(slack_safe(str(v)) for v in (lead["loan_type"], lead["loan_amount_range"],
                                                      lead["property_city"]) if v)
    return f"{flag} {slack_safe(lead['first_name'] or 'Lead')}" + (f": {details}" if details else "")


def _booker(booked_by: Optional[str]) -> str:
    return "AI" if (booked_by or "").lower() == "ai" else slack_safe(booked_by or "unknown")


def _inbox_text(item: dict[str, Any]) -> str:
    """The item's summary, with a lead's short reference swapped for the first name kept in its payload. Items raised
    before the alerts used names carry the reference in the summary."""
    summary = str(item["summary"] or "Inbox item")
    payload = item.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    ref, name = (payload.get("lead_ref"), payload.get("first_name")) if isinstance(payload, dict) else (None, None)
    return summary.replace(str(ref), str(name).strip()) if ref and name and str(ref) in summary else summary


def _action_lines(data: DigestData, day: date, now: datetime, approvals_channel: Optional[str]) -> list[str]:
    lines: list[str] = []
    if data.pending_drafts:
        where = f"<#{approvals_channel}>" if approvals_channel else "the approvals channel"
        lines += [f":white_check_mark: *Messages waiting for your approval: {data.pending_total}*  → approve in {where}"]
        lines += [_draft_line(d, now) for d in data.pending_drafts] + _more(len(data.pending_drafts), data.pending_total)
    if data.due_confirmations:
        lines += ["", f":telephone_receiver: *Confirmation calls to make: {data.due_confirmations_total}*"]
        lines += [_confirmation_line(c, day) for c in data.due_confirmations]
        lines += _more(len(data.due_confirmations), data.due_confirmations_total)
    if data.open_inbox:
        lines += ["", f":inbox_tray: *Open items in Cora's inbox: {data.open_inbox}*  → reply in the inbox thread "
                      "to route or dismiss"]
        lines += [f"• {slack_safe(_inbox_text(i))[:INBOX_SUMMARY_MAX]}" for i in data.inbox_items]
        lines += _more(len(data.inbox_items), data.open_inbox)
    return lines[1:] if lines and lines[0] == "" else lines


def _lead_lines(data: DigestData) -> list[str]:
    lines = [_lead_line(lead) for lead in data.new_leads] + _more(len(data.new_leads), data.new_leads_total)
    if data.web_leads:
        lines.append(f"• {_plural(data.web_leads, 'website lead')}" + (
            f", {data.web_leads_not_in_ghl} not yet sent to GoHighLevel" if data.web_leads_not_in_ghl else ""))
    return lines


def _hold_lines(data: DigestData) -> list[str]:
    lines = ['Open `/lenders` and choose "Show the hold queue" to review.'] if data.held_files else []
    if data.exhausted_files:
        lines.append(f"{_plural(data.exhausted_files, 'more file')} where every lender declined.")
    return lines


def render(day: date, data: DigestData, *, now: Optional[datetime] = None,
           approvals_channel: Optional[str] = None) -> tuple[str, list[dict]]:
    """The digest's fallback text and Block Kit blocks."""
    now = now or datetime.now(timezone.utc)
    status = (f":octagonal_sign: Cora is paused ({data.halt_reason or 'no reason given'}). Nothing will be sent to "
              "borrowers until an approver says `resume`." if data.halted
              else ":large_green_circle: Cora is running normally.")
    status_lines = [status, "_All times Eastern._"] + (
        [f":warning: Not available today: {', '.join(data.unavailable)}"] if data.unavailable else [])
    calls = [f"• {_clock(c['slot_start'])}: {slack_safe(c['first_name'] or 'Lead')} (booked by {_booker(c['booked_by'])})"
             for c in data.todays_calls]
    queues = [f"• {slack_safe(str(q['campaign']).replace('_', ' ').capitalize())}: {_plural(int(q['contacts']), 'contact')}"
              for q in data.dialer_queues]
    sections = [
        BriefSection("Status", status_lines),
        BriefSection("Needs your action", _action_lines(data, day, now, approvals_channel),
                     "Nothing needs your action."),
        BriefSection(f"New leads, last 24 hours: {data.new_leads_total + data.web_leads}", _lead_lines(data),
                     "No new leads."),
        BriefSection(f"Today's calls: {len(data.todays_calls)} booked", calls, "No calls booked today."),
        BriefSection("Dialer lists ready", queues, "No contacts loaded."),
        BriefSection(f"Loan files with no lender yet: {data.held_files}", _hold_lines(data),
                     "All loan files are routed."),
    ]
    header = f"Next Deal Lending morning update: {day:%A}, {day:%B} {day.day}"
    return header, render_brief_blocks(header, sections)


def post_morning_digest(engine: Engine, chat: ChatPort, channel: Optional[str], *, halted: bool,
                        halt_reason: str = "", now: Optional[datetime] = None,
                        approvals_channel: Optional[str] = None) -> bool:
    """Post today's digest if it is due and not yet posted. Returns True when a digest was posted."""
    now = now or datetime.now(timezone.utc)
    day = digest_day(now)
    if day is None or not channel:
        return False
    with Session(engine) as db, db.begin():
        if db.execute(_CLAIM, {"day": day}).scalar() is None:
            return False
        header, blocks = render(day, gather(db, day, now, halted=halted, halt_reason=halt_reason), now=now,
                                approvals_channel=approvals_channel)
        result = chat.post(text=header, blocks=blocks, channel=channel)
        if not result.ok:
            # Raising rolls the claim back, so the next minute's sweep tries again.
            raise RuntimeError(f"morning digest not posted ({result.error or 'no Slack response'})")
        db.execute(_RECORD_POST, {"channel": result.channel or channel, "ts": result.ts, "day": day})
    logger.info("[cora] morning digest posted for %s", day.isoformat())
    return True
