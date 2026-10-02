"""Poll BatchDialer call records (CDRs) into lending.call_dispositions.

/v2/cdrs/last returns CDRs newer than a server-side watermark for this API key, so it is
the fast path but can lose calls if we die after reading. /v2/cdrs (stateless, by day) is
the safety net: the rescan re-reads today and the previous day(s) and only processes rows that are
new or whose disposition / end time changed.
"""
from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Optional
from urllib.parse import urlencode

from sqlalchemy import text

from config.lending_dispositions import DNC_CODE
from src.lending.call_pipeline import follow_up, lending_campaign_ids, process_event
from src.lending.dispositions import DialerCallEvent, normalize_code, parse_event

logger = logging.getLogger(__name__)

Http = Callable[..., Any]


@dataclass
class IngestStats:
    """`seen` counts every fetched item, filtered ones included; the rest count lending outbound calls only."""

    seen: int = 0
    processed: int = 0
    skipped: int = 0
    failed: int = 0

    def __add__(self, other: "IngestStats") -> "IngestStats":
        return IngestStats(self.seen + other.seen, self.processed + other.processed,
                           self.skipped + other.skipped, self.failed + other.failed)


def _path(path: str, **params: Any) -> str:
    query = urlencode({k: v for k, v in params.items() if v is not None})
    return f"{path}?{query}" if query else path


def fetch_last(http: Http) -> list[dict]:
    body = http("GET", "/v2/cdrs/last")
    return list(body.get("items") or []) if isinstance(body, dict) else []


def iter_day(http: Http, day: date, *, max_pages: int = 200) -> Iterator[dict]:
    cursor: Optional[str] = None
    seen_cursors: set[str] = set()
    for _ in range(max_pages):
        body = http("GET", _path("/v2/cdrs", callDate=f"{day.isoformat()}T00:00:00Z", pagelength=100, next_page=cursor))
        if not isinstance(body, dict):
            logger.warning("[lending] CDR day scan for %s ended early: non-dict response", day)
            return
        yield from body.get("items") or []
        cursor = body.get("nextPage")
        if not cursor:
            return
        if cursor in seen_cursors:
            logger.warning("[lending] CDR day scan for %s stopped: repeated cursor", day)
            return
        seen_cursors.add(cursor)
    logger.warning("[lending] CDR day scan stopped at %d pages for %s", max_pages, day)


def _changed(db, events: list[DialerCallEvent]) -> list[DialerCallEvent]:
    if not events:
        return events
    rows = db.execute(
        text("SELECT dialer_call_id, raw_event->>'disposition', raw_event->>'callEndTime', raw_event->>'duration', "
             "disposition, opt_out_propagated_at, phone "
             "FROM lending.call_dispositions WHERE dialer_call_id = ANY(:ids)"),
        {"ids": [e.call_id for e in events]},
    ).all()
    stored = {r[0]: (r[1], r[2], r[3]) for r in rows}
    # committed before the opt-out hook failed; a phoneless row cannot be retried, so do not reselect it
    unpropagated_dnc = {r[0] for r in rows if r[4] == DNC_CODE and r[5] is None and r[6] is not None}

    def key(e: DialerCallEvent):
        duration = e.raw.get("duration")
        return (e.raw.get("disposition"), e.raw.get("callEndTime"), None if duration is None else str(duration))

    return [e for e in events if e.call_id in unpropagated_dnc or stored.get(e.call_id) != key(e)]


def _finished(ev: DialerCallEvent) -> DialerCallEvent:
    """Every CDR is a finished dial: make sure it carries an end time so the attempt counts."""
    if ev.ended_at is not None:
        return ev
    end = ev.started_at + timedelta(seconds=ev.duration or 0) if ev.started_at else datetime.now(timezone.utc)
    return dataclasses.replace(ev, ended_at=end)


def ingest(db, items: list[dict], *, only_changed: bool) -> IngestStats:
    stats = IngestStats(seen=len(items))
    campaigns = lending_campaign_ids()
    parsed = [ev for ev in (parse_event(i) for i in items) if ev]
    for ev in parsed:
        if ev.campaign_id not in campaigns and normalize_code(ev.disposition_raw)[0] == DNC_CODE:
            logger.warning("[lending] DNC request on CDR %s ignored: campaign %s is not a lending campaign",
                           ev.call_id, ev.campaign_id)
    events = [ev for ev in parsed
              if ev.campaign_id in campaigns
              and (ev.direction != "inbound" or normalize_code(ev.disposition_raw)[0] == DNC_CODE)]
    if only_changed:
        pending = _changed(db, events)
        stats.skipped = len(events) - len(pending)
        events = pending
    for ev in events:
        try:
            recorded = process_event(db, _finished(ev))
        except Exception as exc:  # class only: SQL errors embed bound params (phones)
            db.rollback()
            stats.failed += 1
            logger.error("[lending] CDR %s failed: %s", ev.call_id, type(exc).__name__)
            continue
        stats.processed += 1
        try:
            follow_up(recorded, lambda fn, *args: fn(*args))
        except Exception as exc:
            logger.error("[lending] CDR %s follow-up failed: %s", ev.call_id, type(exc).__name__)
    return stats


def poll_new(db, http: Http) -> IngestStats:
    return ingest(db, fetch_last(http), only_changed=False)


def rescan_today(db, http: Http, *, now: Optional[datetime] = None, days: int = 2) -> IngestStats:
    today = (now or datetime.now(timezone.utc)).date()
    total = IngestStats()
    for back in range(days):
        day = today - timedelta(days=back)
        total = total + ingest(db, list(iter_day(http, day)), only_changed=True)
    return total
