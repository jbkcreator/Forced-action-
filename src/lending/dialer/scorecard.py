"""R1 1.6: David's 2:45 PM caller scorecard.

The day's numbers come from the same BatchDialer and GHL-stage reads as the 7 PM summary
(``david_activity``). One extra query lists his connected calls with their extraction fields for the quality score,
the top 10 and the calls that still need notes. The quality score is a provisional rubric the client has not
confirmed (``QUALITY_RUBRIC_VERSION``).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_compliance import DEFAULT_TZ
from config.lending_dialer_queue import NOTES_LIST_MAX, QUALITY_CHECKS, TOP_CALLS
from config.lending_dispositions import BOOKED_CODE, LONG_CONVERSATION_SECONDS, RECORDING_RECORDED
from src.lending.david_activity import batchdialer_counts, day_bounds, read_stage_counts
from src.lending.dialer.call_notes import notes_button
from src.lending.dispositions import contact_view_url

logger = logging.getLogger(__name__)
ET = ZoneInfo(DEFAULT_TZ)

_CALLS = text("""
    SELECT c.dialer_call_id, c.phone, c.talk_duration_sec, c.disposition, c.recording_status,
           c.recording_disclosure_logged, c.dialer_contact_id, e.fields, e.source
      FROM lending.call_dispositions c
      LEFT JOIN lending.call_extractions e ON e.dialer_call_id = c.dialer_call_id
     WHERE c.call_source = 'batchdialer' AND c.direction = 'outbound' AND c.caller_seat = :agent_id
       AND coalesce(c.talk_duration_sec, 0) > 0
       AND c.call_ended_at >= :start AND c.call_ended_at < :end
""")


@dataclass(frozen=True)
class ScoredCall:
    dialer_call_id: str
    phone_last4: str
    talk_sec: int
    disposition: Optional[str]
    recorded: bool
    disclosure_logged: bool
    dialer_contact_id: Optional[str]
    extraction: Optional[Mapping[str, Any]]  # None = no transcript summary and no notes
    notes_source: Optional[str]  # 'transcript' | 'caller_notes' | None


@dataclass(frozen=True)
class Scorecard:
    day: date
    as_of: datetime
    dials: int
    connects: int
    long_conversations: int
    booked_stage: int
    booked_reported: int
    calls: tuple[ScoredCall, ...]
    read_failed: bool
    booked_available: bool = True  # False when the GHL stage read failed: shown as not available, never as 0


def quality_score(call: ScoredCall) -> Optional[int]:
    """0 to 5, one point per ``QUALITY_CHECKS`` entry; None when the call has no extraction to judge."""
    ext = call.extraction
    if ext is None:
        return None
    checks = (bool(call.disposition), call.talk_sec >= LONG_CONVERSATION_SECONDS, bool(ext.get("next_action")),
              bool(ext.get("decision_maker")),
              any(ext.get(f) for f in ("objection", "kill_reason_verbatim", "deal_status")))
    return sum(checks)


def top_calls(calls: Sequence[ScoredCall], n: int = TOP_CALLS) -> list[ScoredCall]:
    connected = [c for c in calls if c.talk_sec > 0]
    return sorted(connected, key=lambda c: (c.disposition != BOOKED_CODE, -c.talk_sec, c.dialer_call_id))[:n]


def needs_notes(calls: Sequence[ScoredCall]) -> list[ScoredCall]:
    return [c for c in calls if c.talk_sec > 0 and not c.recorded and c.extraction is None]


def _fields(raw: Any) -> Optional[Mapping[str, Any]]:
    if raw is None:
        return None
    return raw if isinstance(raw, dict) else json.loads(raw)


def _read_calls(db: Session, day: date, agent_id: str) -> tuple[ScoredCall, ...]:
    start, end = day_bounds(day)
    rows = db.execute(_CALLS, {"agent_id": agent_id, "start": start, "end": end}).mappings().all()
    return tuple(ScoredCall(
        dialer_call_id=r["dialer_call_id"], phone_last4="".join(ch for ch in (r["phone"] or "") if ch.isdigit())[-4:],
        talk_sec=int(r["talk_duration_sec"] or 0), disposition=r["disposition"],
        recorded=r["recording_status"] == RECORDING_RECORDED, disclosure_logged=bool(r["recording_disclosure_logged"]),
        dialer_contact_id=r["dialer_contact_id"], extraction=_fields(r["fields"]), notes_source=r["source"])
        for r in rows)


def build_scorecard(db: Session, day: date, *, agent_id: str, as_of: datetime) -> Scorecard:
    """A failed read keeps zeros and sets ``read_failed``; each read has its own savepoint."""
    read_failed = False
    counts: Mapping[str, int] = {}
    try:
        with db.begin_nested():
            counts = batchdialer_counts(db, day, agent_id)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        read_failed = True
        logger.warning("[scorecard] BatchDialer counts failed for %s (%s)", day, type(exc).__name__)
    calls: tuple[ScoredCall, ...] = ()
    try:
        with db.begin_nested():
            calls = _read_calls(db, day, agent_id)
    except Exception as exc:
        read_failed = True
        logger.warning("[scorecard] call list failed for %s (%s)", day, type(exc).__name__)
    booked_stage, booked_available = 0, True
    try:
        with db.begin_nested():
            booked_stage, _held = read_stage_counts(db, day)
    except Exception as exc:
        booked_available = False
        logger.warning("[scorecard] GHL stage counts failed for %s (%s)", day, type(exc).__name__)
    return Scorecard(
        day=day, as_of=as_of, dials=counts.get("bd_dials", 0), connects=counts.get("bd_connected", 0),
        long_conversations=sum(c.talk_sec >= LONG_CONVERSATION_SECONDS for c in calls), booked_stage=booked_stage,
        booked_reported=counts.get("bd_booked_reported", 0), calls=calls, read_failed=read_failed,
        booked_available=booked_available)


def _minutes(seconds: int) -> str:
    return f"{max(1, round(seconds / 60))}m"


def _quality_line(card: Scorecard) -> str:
    scores = [s for s in (quality_score(c) for c in card.calls) if s is not None]
    if not scores:
        return "Call quality (provisional): no scored calls yet"
    return (f"Call quality (provisional): {sum(scores) / len(scores):.1f} / {len(QUALITY_CHECKS)} over "
            f"{len(scores)} of {len(card.calls)} calls ({len(card.calls) - len(scores)} without notes)")


def _top_line(call: ScoredCall) -> str:
    score = quality_score(call)
    url = contact_view_url(call.dialer_contact_id)
    parts = [f"…{call.phone_last4}", _minutes(call.talk_sec), call.disposition or "no result logged",
             f"quality {score}/{len(QUALITY_CHECKS)}" if score is not None else "no notes"]
    if url:
        parts.append(f"<{url}|BatchDialer ↗>")
    return " · ".join(parts)


def _section(content: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": content}}


def render_scorecard(card: Scorecard) -> list[dict]:
    no_disclosure = sum(1 for c in card.calls if c.recorded and not c.disclosure_logged)
    lines = [
        f"*Dials* (all dials in his campaigns): {card.dials}",
        f"*Connects*: {card.connects}",
        f"*Conversations 2+ min*: {card.long_conversations}",
        f"*Booked* (GHL stage, all callers): {card.booked_stage if card.booked_available else 'not available'}"
        f" · he reported {card.booked_reported}",
        _quality_line(card),
        f"Recorded calls with no disclosure reported by the dialer: {no_disclosure}",
    ]
    if card.read_failed:
        lines.append(":warning: Some numbers could not be read; the zeros above may be wrong.")
    blocks = [
        _section(f":bar_chart: *David's scorecard · {card.day:%a %b} {card.day.day}* "
                 f"(as of {card.as_of.astimezone(ET):%-I:%M %p} ET)"),
        _section("\n".join(lines)),
    ]
    top = top_calls(card.calls)
    if top:
        blocks.append(_section("*Top calls*\n" + "\n".join(_top_line(c) for c in top)))
    return blocks


def render_notes_thread(card: Scorecard) -> list[list[dict]]:
    """Thread replies: one line and an "Add notes" button per connected call that has no recording and no notes,
    NOTES_LIST_MAX calls to a reply so every call gets its button. No replies when no call needs notes."""
    wanted = needs_notes(card.calls)
    return [[{"type": "section", "text": {"type": "mrkdwn", "text": (
        f"…{c.phone_last4} · {_minutes(c.talk_sec)} · {c.disposition or 'no result logged'}")},
        "accessory": notes_button(c.dialer_call_id)} for c in wanted[i:i + NOTES_LIST_MAX]]
        for i in range(0, len(wanted), NOTES_LIST_MAX)]
