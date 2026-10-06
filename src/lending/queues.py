"""Assign pool records to launch queues and report the funnel per queue (Go Live G14/G16)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

from sqlalchemy import text

from config.lending_queues import NEVER_BOOKABLE_QUEUES, NURTURE, NURTURE_ONLY_TAGS, SOURCE_TAG_QUEUES
from src.lending.dialer_load import run_dialer_load
from src.services.phone_utils import normalize as normalize_phone

REPORT_RUN_ID = "queue-count-report"
HIT_RATE_WINDOW_DAYS = 30

# Exclusions made at the scrub stage vs at the Backflip conflict check.
_SCRUB_REASONS = frozenset({
    "NATIONAL_DNC", "STATE_DNC", "LITIGATOR", "SUPPRESSED", "NO_FRESH_SCRUB",
    "SCRUB_FAILED", "NEEDS_SCRUB", "GA_NATURAL_PERSON",
})


def stage_counts(*, traced: int, excluded_by_reason: Mapping[str, int], needs_scrub: int = 0,
                 needs_scrub_backflip_blocked: int = 0) -> dict[str, int]:
    """Numbers left after the scrub stage and after the Backflip check.

    A number with no fresh scrub never counts as scrubbed, even when the Backflip check
    is the reason recorded for it (a dry run records the first block that sticks)."""
    scrub_blocked = sum(n for r, n in excluded_by_reason.items() if r in _SCRUB_REASONS and r != "NEEDS_SCRUB")
    scrubbed = max(traced - scrub_blocked - needs_scrub, 0)
    backflip = sum(n for r, n in excluded_by_reason.items() if r.startswith("BACKFLIP_"))
    backflip_on_scrubbed = backflip - needs_scrub_backflip_blocked
    return {"scrubbed": scrubbed, "after_backflip": max(scrubbed - backflip_on_scrubbed, 0)}


def tracerfy_hit_rate(db, *, since: Optional[datetime] = None) -> Optional[float]:
    """Share of Tracerfy lookups that found a contact, from enrichment_usage_logs."""
    since = since or datetime.now(timezone.utc) - timedelta(days=HIT_RATE_WINDOW_DAYS)
    total, hits = db.execute(
        text("SELECT count(*), count(*) FILTER (WHERE success) FROM enrichment_usage_logs "
             "WHERE vendor = 'tracerfy' AND created_at >= :since"),
        {"since": since},
    ).one()
    return round(hits / total, 4) if total else None


def assign_queue(record: Mapping[str, Any]) -> dict[str, Any]:
    """Copy of the record with ``queue``, ``pool`` and ``bookable`` set from its ``source_tag``.

    Every ranked queue is bookable except NEVER_BOOKABLE_QUEUES (Partners/List 4:
    dialed as a real queue, partner script only, never pitched or booked as
    borrowers). A tag with no rank falls back to the nurture queue, never bookable.
    Unknown tags get ``queue`` None and are never dialed."""
    tag = str(record.get("source_tag") or "")
    queue = SOURCE_TAG_QUEUES.get(tag)
    bookable = queue is not None and queue not in NEVER_BOOKABLE_QUEUES
    if queue is None and tag in NURTURE_ONLY_TAGS:
        queue = NURTURE
    return {**record, "queue": queue, "pool": queue, "bookable": bookable}


def queue_count_report(
    records: Sequence[Mapping[str, Any]],
    db,
    *,
    tracerfy_balance: Optional[int] = None,
    tracerfy_hit_rate: Optional[float] = None,
) -> dict[str, Any]:
    """Per queue: raw -> traced (valid phone) -> eligible deduplicated people.

    Runs the dialer-load gates as a dry run: no Tracerfy credits, no dialer call, no
    writes. Numbers still needing a scrub are counted separately, not as eligible.
    """
    by_queue: dict[str, list[dict]] = {}
    for record in (assign_queue(r) for r in records):
        if record["queue"]:
            by_queue.setdefault(record["queue"], []).append(record)

    queues: dict[str, dict[str, Any]] = {}
    for queue, rows in by_queue.items():
        report = run_dialer_load(rows, db, run_id=REPORT_RUN_ID, dry_run=True)
        traced = sum(1 for r in rows if normalize_phone(r.get("normalized_phone") or r.get("phone") or ""))
        queues[queue] = {
            "raw": len(rows),
            "traced": traced,
            **stage_counts(traced=traced, excluded_by_reason=report.excluded_by_reason,
                           needs_scrub=report.needs_scrub,
                           needs_scrub_backflip_blocked=report.needs_scrub_backflip_blocked),
            "needs_scrub": report.needs_scrub,
            "excluded_by_reason": dict(report.excluded_by_reason),
            "eligible": report.distinct_phones,
        }
    return {
        "queues": queues,
        "tracerfy_balance": tracerfy_balance,
        "tracerfy_hit_rate": tracerfy_hit_rate,
    }
