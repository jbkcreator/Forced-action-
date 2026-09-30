"""Poll BatchDialer call records (CDRs) into lending.call_dispositions.

/v2/cdrs/last returns CDRs newer than a server-side watermark for this API key, so it is
the fast path but can lose calls if we die after reading. /v2/cdrs (stateless, by day) is
the safety net: the rescan re-reads today and yesterday and only processes rows that are
new or whose disposition / end time changed.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Optional
from urllib.parse import urlencode

from sqlalchemy import text

from src.lending.call_pipeline import follow_up, lending_campaign_ids, process_event
from src.lending.dispositions import DialerCallEvent, parse_event

logger = logging.getLogger(__name__)

Http = Callable[..., Any]


@dataclass
class IngestStats:
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
    for _ in range(max_pages):
        body = http("GET", _path("/v2/cdrs", callDate=f"{day.isoformat()}T00:00:00Z", pagelength=100, next_page=cursor))
        if not isinstance(body, dict):
            return
        yield from body.get("items") or []
        cursor = body.get("nextPage")
        if not cursor:
            return
    logger.warning("[lending] CDR day scan stopped at %d pages for %s", max_pages, day)


def _changed(db, events: list[DialerCallEvent]) -> list[DialerCallEvent]:
    if not events:
        return events
    rows = db.execute(
        text("SELECT dialer_call_id, raw_event->>'disposition', raw_event->>'callEndTime' "
             "FROM lending.call_dispositions WHERE dialer_call_id = ANY(:ids)"),
        {"ids": [e.call_id for e in events]},
    ).all()
    stored = {r[0]: (r[1], r[2]) for r in rows}
    return [e for e in events if stored.get(e.call_id) != (e.raw.get("disposition"), e.raw.get("callEndTime"))]


def ingest(db, items: list[dict], *, only_changed: bool) -> IngestStats:
    stats = IngestStats(seen=len(items))
    campaigns = lending_campaign_ids()
    events = [ev for ev in (parse_event(i) for i in items)
              if ev and ev.direction != "inbound" and ev.campaign_id in campaigns]
    if only_changed:
        pending = _changed(db, events)
        stats.skipped = len(events) - len(pending)
        events = pending
    for ev in events:
        try:
            recorded = process_event(db, ev)
        except Exception as exc:  # class only: SQL errors embed bound params (phones)
            db.rollback()
            stats.failed += 1
            logger.error("[lending] CDR %s failed: %s", ev.call_id, type(exc).__name__)
            continue
        stats.processed += 1
        follow_up(recorded, lambda fn, *args: fn(*args))
    return stats


def poll_new(db, http: Http) -> IngestStats:
    return ingest(db, fetch_last(http), only_changed=False)


def rescan_today(db, http: Http, *, now: Optional[datetime] = None) -> IngestStats:
    today = (now or datetime.now(timezone.utc)).date()
    total = IngestStats()
    for day in (today, today - timedelta(days=1)):
        total = total + ingest(db, list(iter_day(http, day)), only_changed=True)
    return total
