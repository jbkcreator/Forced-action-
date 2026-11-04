"""Every number loaded into BatchDialer also lands in GHL, with no manual step.

The table is the contract: any loader that writes ``lending.dialer_load_records`` is covered, whoever runs it.
Each run takes up to GHL_LOAD_SYNC_BATCH_SIZE active rows not yet handled (``ghl_sync_status IS NULL``) and:

- warm numbers (a website or LendingFlow lead) -> ``skipped_warm``: they have their own GHL flow, and cold
  do-not-disturb would block their texts;
- suppressed / do-not-contact numbers -> ``skipped_suppressed``;
- pools missing from GHL_LOAD_SYNC_POOL_TAGS -> ``skipped_unmapped`` (logged; a new pool never reaches GHL unmapped);
- everything else -> contact upsert with cold-lead do-not-disturb (calls allowed), then add-tags
  ``<pool tag>``, ``ndl-cold`` and ``ndl-state-<xx>`` when the state is known -> ``pushed``.

A failed push is retried next run; after GHL_LOAD_SYNC_MAX_ATTEMPTS the row is ``failed`` and one ops notice is
posted (ids only). The upsert matches an existing contact, so a re-push never duplicates. Rows are claimed with
``FOR UPDATE SKIP LOCKED`` under an advisory lock, one run at a time. Refused while the lending emergency halt is on.

Usage (cron */5; idle until LENDING_GHL_LOAD_SYNC_ENABLED=true):
    python -m src.lending.ghl_load_sync
    python -m src.lending.ghl_load_sync --dry-run      # counts what the next run would do; pushes nothing
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import text

from config.lending_compliance import (
    GHL_LOAD_SYNC_BATCH_SIZE,
    GHL_LOAD_SYNC_LOCK_KEY,
    GHL_LOAD_SYNC_MAX_ATTEMPTS,
    GHL_LOAD_SYNC_POOL_TAGS,
    GHL_TAG_COLD_LEAD,
)
from config.lending_wholesalers import GHL_TAG_STATE_PREFIX
from config.settings import get_settings
from src.lending.global_halt import is_globally_halted
from src.lending.wholesaler_lists import GhlTagSink, ghl_sink

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_REFUSED = 3

PUSHED = "pushed"
SKIPPED_WARM = "skipped_warm"
SKIPPED_SUPPRESSED = "skipped_suppressed"
SKIPPED_UNMAPPED = "skipped_unmapped"
FAILED = "failed"

# State comes from the PropertyRadar list record behind a pr_list ref; other pools carry none.
_PENDING = text("""
    SELECT d.id, d.pool, d.source_record_ref, d.phone, d.borrower_name, d.entity_name, d.ghl_sync_attempts,
           (SELECT r.state FROM lending.pr_list_records r
             WHERE d.source_record_ref LIKE 'pr_list:%'
               AND r.list_key = split_part(d.source_record_ref, ':', 2)
               AND r.radar_id = split_part(d.source_record_ref, ':', 3)
             LIMIT 1) AS state,
           (EXISTS (SELECT 1 FROM lending.web_leads w WHERE w.phone = d.phone)
            OR EXISTS (SELECT 1 FROM lending.lendingflow_leads l WHERE l.phone = d.phone)) AS warm,
           (EXISTS (SELECT 1 FROM lending.suppression_list s WHERE s.phone = d.phone)
            OR EXISTS (SELECT 1 FROM lending.contacts c WHERE c.phone = d.phone AND c.do_not_contact)) AS suppressed
      FROM lending.dialer_load_records d
     WHERE d.active AND d.ghl_sync_status IS NULL
     ORDER BY d.id
     LIMIT :limit
       FOR UPDATE OF d SKIP LOCKED
""")

_MARK = text("""
    UPDATE lending.dialer_load_records AS d
       SET ghl_sync_status = v.status,
           ghl_sync_attempts = d.ghl_sync_attempts + v.attempted,
           ghl_synced_at = CASE WHEN v.status = 'pushed' THEN now() ELSE d.ghl_synced_at END,
           ghl_sync_error = v.error
      FROM unnest(CAST(:ids AS bigint[]), CAST(:statuses AS text[]), CAST(:attempted AS int[]),
                  CAST(:errors AS text[])) AS v(id, status, attempted, error)
     WHERE d.id = v.id
""")


def sync_tags(row: Mapping[str, Any]) -> Optional[list[str]]:
    """Tags for a row, or None when its pool has no GHL list."""
    pool_tag = GHL_LOAD_SYNC_POOL_TAGS.get(str(row.get("pool")))
    if pool_tag is None:
        return None
    tags = [pool_tag, GHL_TAG_COLD_LEAD]
    state = str(row.get("state") or "").strip().lower()
    if state:
        tags.append(f"{GHL_TAG_STATE_PREFIX}{state}")
    return tags


def classify(row: Mapping[str, Any]) -> Optional[str]:
    """Skip status for a row that must not be pushed, else None."""
    if row["warm"]:
        return SKIPPED_WARM
    if row["suppressed"]:
        return SKIPPED_SUPPRESSED
    if sync_tags(row) is None:
        return SKIPPED_UNMAPPED
    return None


@dataclass
class Outcome:
    """Per-row result: status None = left pending for the next run."""
    id: int
    status: Optional[str]
    attempted: int = 0
    error: Optional[str] = None


@dataclass
class SyncResult:
    counts: Counter = field(default_factory=Counter)
    newly_failed: list[int] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"counts": dict(self.counts), "newly_failed": len(self.newly_failed)}


def process(rows: Sequence[Mapping[str, Any]], sink: Optional[GhlTagSink]) -> list[Outcome]:
    """Decide every row; push the pushable ones through ``sink`` (None = dry run, nothing pushed)."""
    outcomes: list[Outcome] = []
    for row in rows:
        skip = classify(row)
        if skip is not None:
            if skip == SKIPPED_UNMAPPED:
                logger.warning("[ghl-load-sync] pool %s has no GHL tag; load record %s not pushed", row["pool"], row["id"])
            outcomes.append(Outcome(row["id"], skip))
            continue
        if sink is None:
            outcomes.append(Outcome(row["id"], PUSHED))
            continue
        try:
            sink.push(row, sync_tags(row) or [])
        except Exception as exc:
            error = str(exc)[:200] or type(exc).__name__
            logger.warning("[ghl-load-sync] push failed for load record %s (%s): %s", row["id"], row["pool"], error)
            final = row["ghl_sync_attempts"] + 1 >= GHL_LOAD_SYNC_MAX_ATTEMPTS
            outcomes.append(Outcome(row["id"], FAILED if final else None, attempted=1, error=error))
        else:
            outcomes.append(Outcome(row["id"], PUSHED, attempted=1))
    return outcomes


def _record(db, outcomes: Sequence[Outcome]) -> None:
    if not outcomes:
        return
    db.execute(_MARK, {
        "ids": [o.id for o in outcomes],
        "statuses": [o.status for o in outcomes],
        "attempted": [o.attempted for o in outcomes],
        "errors": [o.error for o in outcomes],
    })


def run(db, sink: Optional[GhlTagSink], *, limit: int = GHL_LOAD_SYNC_BATCH_SIZE, dry_run: bool = False) -> SyncResult:
    """One sync pass inside the caller's transaction (the caller commits; a dry run is rolled back)."""
    rows = [dict(r) for r in db.execute(_PENDING, {"limit": limit}).mappings()]
    outcomes = process(rows, None if dry_run else sink)
    result = SyncResult()
    for outcome in outcomes:
        result.counts[outcome.status or "retry"] += 1
        if outcome.status == FAILED:
            result.newly_failed.append(outcome.id)
    if not dry_run:
        _record(db, outcomes)
    return result


def mark_pushed(db, source_record_refs: Sequence[str]) -> None:
    """Record active rows a loader already pushed to GHL itself (the wholesaler load), so the sync skips them."""
    if source_record_refs:
        db.execute(text(
            "UPDATE lending.dialer_load_records SET ghl_sync_status = 'pushed', ghl_synced_at = now() "
            "WHERE active AND ghl_sync_status IS NULL AND source_record_ref = ANY(:refs)"
        ), {"refs": list(source_record_refs)})


def _notify_failed(ids: Sequence[int]) -> None:
    from src.lending.ops_alerts import post_ops_notice

    post_ops_notice(f"GHL load sync: {len(ids)} dialer load record(s) could not be pushed to GHL after "
                    f"{GHL_LOAD_SYNC_MAX_ATTEMPTS} attempts (ids {', '.join(map(str, ids[:20]))}). "
                    "See lending.dialer_load_records.ghl_sync_error.")


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push BatchDialer loads to GHL as cold contacts.")
    parser.add_argument("--dry-run", action="store_true", help="count what the next run would do; push nothing")
    parser.add_argument("--limit", type=int, default=GHL_LOAD_SYNC_BATCH_SIZE)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    from src.core.database import get_db_context

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    if not args.dry_run:
        if not get_settings().lending_ghl_load_sync_enabled:
            logger.info("[ghl-load-sync] LENDING_GHL_LOAD_SYNC_ENABLED is off; nothing done")
            return EXIT_OK
        if is_globally_halted():
            logger.warning("[ghl-load-sync] lending emergency halt is on; nothing pushed")
            return EXIT_REFUSED
    sink = None if args.dry_run else ghl_sink()
    if not args.dry_run and sink is None:
        logger.error("[ghl-load-sync] lending GHL account not configured; nothing pushed")
        return EXIT_REFUSED
    with get_db_context() as db:
        if not db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": GHL_LOAD_SYNC_LOCK_KEY}).scalar():
            logger.info("[ghl-load-sync] another run holds the lock; skipped")
            return EXIT_OK
        result = run(db, sink, limit=args.limit, dry_run=args.dry_run)
        if args.dry_run:
            db.rollback()
        else:
            db.commit()
    logger.info("[ghl-load-sync] %s%s", "dry run: " if args.dry_run else "", json.dumps(result.as_dict()))
    if result.newly_failed:
        _notify_failed(result.newly_failed)
    return EXIT_FAILED if result.counts.get("retry") or result.newly_failed else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
