"""Alert when a reply handoff has gone unanswered for one business hour (WP-GL-10).

Josh answers rate / terms questions and reschedules himself, Monday to Friday 9:00 AM - 7:15 PM ET, within one
business hour (Oct 1 / Oct 4 emails). Each handoff is posted to ``LENDING_REPLIES_CHANNEL`` by ``reply_guard``. A
handoff counts as answered when a person (not the AI or a workflow) sends the contact a message in GHL
afterwards; ``reply_guard`` records that. This job, run every 10 minutes, posts one alert listing the handoffs
still unanswered after ``REPLY_SLA_BUSINESS_MINUTES`` of Josh's hours, and marks them so each is alerted once.

UNVERIFIED: that a GHL outbound message carries a user id when a person sent it. If it does not, a reply by Josh
is not seen and the alert fires anyway (the safe direction); check with one real message once GHL access exists.
"""
from __future__ import annotations

import logging
from datetime import datetime, time, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text

from config.lending_reply_agent import (
    KIND_AI_HANDOFF,
    KIND_RATE_TERMS,
    KIND_RESCHEDULE,
    REPLY_HOURS_END,
    REPLY_HOURS_START,
    REPLY_SLA_BUSINESS_MINUTES,
    REPLY_WEEKDAYS,
)
from src.lending.db import lending_session
from src.lending.reply_guard import slack_poster

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
LOOKBACK_DAYS = 7
BATCH_LIMIT = 200
ANSWERABLE_KINDS = (KIND_RATE_TERMS, KIND_RESCHEDULE, KIND_AI_HANDOFF)

_OPEN_HANDOFFS = text("""
    SELECT message_id, kind, contact_id, posted_at FROM lending.reply_handoffs
     WHERE posted_at IS NOT NULL AND responded_at IS NULL AND overdue_alerted_at IS NULL
       AND kind IN :kinds AND posted_at > :since
     ORDER BY posted_at LIMIT :limit
""").bindparams(bindparam("kinds", expanding=True))
_MARK_ALERTED = text("""
    UPDATE lending.reply_handoffs SET overdue_alerted_at = now()
     WHERE message_id IN :ids AND overdue_alerted_at IS NULL
""").bindparams(bindparam("ids", expanding=True))


def business_minutes(start: datetime, end: datetime) -> float:
    """Minutes between ``start`` and ``end`` that fall inside Josh's hours (Mon-Fri 9:00 AM - 7:15 PM ET)."""
    if end <= start:
        return 0.0
    s, e = start.astimezone(ET), end.astimezone(ET)
    total, day = 0.0, s.date()
    while day <= e.date():
        if day.weekday() in REPLY_WEEKDAYS:
            opens = datetime.combine(day, time(*REPLY_HOURS_START), tzinfo=ET)
            closes = datetime.combine(day, time(*REPLY_HOURS_END), tzinfo=ET)
            low, high = max(opens, s), min(closes, e)
            if high > low:
                total += (high - low).total_seconds() / 60
        day += timedelta(days=1)
    return total


def overdue_handoffs(db, now: datetime) -> list[Mapping[str, Any]]:
    rows = db.execute(_OPEN_HANDOFFS, {"kinds": list(ANSWERABLE_KINDS), "since": now - timedelta(days=LOOKBACK_DAYS),
                                       "limit": BATCH_LIMIT}).mappings().all()
    return [r for r in rows if business_minutes(r["posted_at"], now) >= REPLY_SLA_BUSINESS_MINUTES]


def _posted_label(moment: datetime) -> str:
    local = moment.astimezone(ET)
    return f"{local:%a %b} {local.day} {local.hour % 12 or 12}:{local:%M} {'am' if local.hour < 12 else 'pm'} ET"


def format_alert(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [f"• {r['kind'].replace('_', ' ')} (GHL contact {r['contact_id'] or 'unknown'}), posted "
             f"{_posted_label(r['posted_at'])}" for r in rows]
    head = f"*{len(rows)} handoff(s) unanswered after {REPLY_SLA_BUSINESS_MINUTES} business minutes*"
    return "\n".join([head, *lines])


def main(*, now: Optional[datetime] = None) -> int:
    now = now or datetime.now(timezone.utc)
    poster = slack_poster()
    if poster is None:
        logger.error("[reply-sla] not checked: LENDING_REPLIES_CHANNEL or the Slack token is not configured")
        return 1
    try:
        with lending_session() as db:
            rows = overdue_handoffs(db, now)
            if not rows:
                return 0
            poster(format_alert(rows))
            db.execute(_MARK_ALERTED, {"ids": [r["message_id"] for r in rows]})
            db.commit()
    except Exception as exc:  # class only: rows carry GHL contact ids
        logger.error("[reply-sla] check failed (%s)", type(exc).__name__)
        return 1
    logger.info("[reply-sla] alerted on %d unanswered handoff(s)", len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
