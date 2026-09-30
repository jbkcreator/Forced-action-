"""Assign pool records to launch queues and report the funnel per queue (Go Live G14/G16)."""
from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

from config.lending_queues import NURTURE_ONLY_TAGS, SOURCE_TAG_QUEUES
from src.lending.dialer_load import run_dialer_load
from src.services.phone_utils import normalize as normalize_phone

REPORT_RUN_ID = "queue-count-report"


def assign_queue(record: Mapping[str, Any]) -> dict[str, Any]:
    """Copy of the record with ``queue`` and ``pool`` set from its ``source_tag``.
    Nurture-only and unknown tags get ``queue`` None: never dialed."""
    queue = SOURCE_TAG_QUEUES.get(str(record.get("source_tag") or ""))
    return {**record, "queue": queue, "pool": queue}


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
    assigned = [assign_queue(r) for r in records]
    by_queue: dict[str, list[dict]] = {}
    nurture_only = 0
    for record in assigned:
        if record["queue"]:
            by_queue.setdefault(record["queue"], []).append(record)
        elif str(record.get("source_tag") or "") in NURTURE_ONLY_TAGS:
            nurture_only += 1

    queues: dict[str, dict[str, Any]] = {}
    for queue, rows in by_queue.items():
        report = run_dialer_load(rows, db, run_id=REPORT_RUN_ID, dry_run=True)
        queues[queue] = {
            "raw": len(rows),
            "traced": sum(1 for r in rows if normalize_phone(r.get("normalized_phone") or r.get("phone") or "")),
            "needs_scrub": report.excluded_by_reason.get("NEEDS_SCRUB", 0),
            "excluded_by_reason": dict(report.excluded_by_reason),
            "eligible": report.distinct_phones,
        }
    return {
        "queues": queues,
        "nurture_only": nurture_only,
        "tracerfy_balance": tracerfy_balance,
        "tracerfy_hit_rate": tracerfy_hit_rate,
    }
