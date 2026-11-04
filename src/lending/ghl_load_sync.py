"""Every number loaded into BatchDialer also lands in GHL, with no manual step.

The table is the contract: any loader that writes ``lending.dialer_load_records`` is covered, whoever runs it, and
this job is the only code that pushes dialer loads to GHL. Each run takes up to GHL_LOAD_SYNC_BATCH_SIZE active rows
not yet handled (``ghl_sync_status IS NULL``) and:

- warm numbers (a website or LendingFlow lead) -> ``skipped_warm``: they have their own GHL flow, and cold
  do-not-disturb would block their texts (a cold contact that later turns warm is reconciled by
  ``src.lending.cold_leads.reconcile_warm_contact``);
- suppressed / do-not-contact numbers -> ``skipped_suppressed``;
- the same phone already pushed for the same pool by an earlier load (a reload) -> ``skipped_already_in_ghl``;
- pools missing from GHL_LOAD_SYNC_POOL_TAGS -> ``skipped_unmapped`` (logged; a new pool never reaches GHL unmapped);
- everything else -> contact upsert with cold-lead do-not-disturb (calls allowed), then add-tags: the pool's call-list
  tag, ``ndl-cold``, ``ndl-state-<xx>`` when the state is known and ``ndl-src-<source>`` for wholesaler lists
  -> ``pushed``. While the GHL call gate is on, the contact also gets ``ndl-do-not-call-now`` so it enters Josh's
  lists held until the gate's checks pass. The emergency halt is rechecked during the run; rows not reached stay
  pending.

Row locks are never held across GHL calls: one run at a time (session advisory lock), rows are read and the read
transaction ends, GHL is called, then every outcome is written in one short transaction. A failed push is retried
next run; after GHL_LOAD_SYNC_MAX_ATTEMPTS the row is ``failed`` and one ops notice is posted (ids only). Errors are
stored as the exception class and the GHL step / HTTP status only, never a message body. ``--requeue`` puts
``failed`` and ``skipped_suppressed`` rows back in the queue (e.g. after a GHL fix or an un-suppression).

Usage (cron */5; idle until LENDING_GHL_LOAD_SYNC_ENABLED=true):
    python -m src.lending.ghl_load_sync
    python -m src.lending.ghl_load_sync --dry-run      # counts what the next run would do; no locks, no pushes
    python -m src.lending.ghl_load_sync --requeue      # failed + skipped_suppressed -> pending
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from sqlalchemy import text

from config.lending_compliance import (
    GHL_DO_NOT_CALL_NOW_TAG,
    GHL_LOAD_SYNC_BATCH_SIZE,
    GHL_LOAD_SYNC_HALT_CHECK_EVERY,
    GHL_LOAD_SYNC_LOCK_KEY,
    GHL_LOAD_SYNC_MAX_ATTEMPTS,
    GHL_LOAD_SYNC_POOL_TAGS,
    GHL_TAG_COLD_LEAD,
)
from config.lending_wholesalers import GHL_TAG_SOURCE_PREFIX, GHL_TAG_STATE_PREFIX, WHOLESALER_STATES
from config.settings import get_settings
from src.lending.global_halt import is_globally_halted
from src.lending.wholesaler_lists import GhlTagSink, ghl_sink

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_REFUSED = 3

RETRY = "retry"  # counter key for a failed push left pending; never stored


class SyncStatus(str, Enum):
    PUSHED = "pushed"
    SKIPPED_WARM = "skipped_warm"
    SKIPPED_SUPPRESSED = "skipped_suppressed"
    SKIPPED_ALREADY_IN_GHL = "skipped_already_in_ghl"
    SKIPPED_UNMAPPED = "skipped_unmapped"
    FAILED = "failed"


REQUEUE_STATUSES = (SyncStatus.FAILED.value, SyncStatus.SKIPPED_SUPPRESSED.value)

# Wholesaler lists are FL-only (config.lending_wholesalers.WHOLESALER_STATES), so their state is that one value.
(_WHOLESALER_STATE,) = WHOLESALER_STATES

# State and wholesaler source per ref shape: pr_list:<list>:<radar_id> | staging:<id> | wholesaler:<source>:<hash>.
_PENDING = text("""
    SELECT d.id, d.pool, d.source_record_ref, d.phone, d.borrower_name, d.entity_name, d.ghl_sync_attempts,
           CASE split_part(d.source_record_ref, ':', 1)
             WHEN 'pr_list' THEN (SELECT r.state FROM lending.pr_list_records r
                                   WHERE r.list_key = split_part(d.source_record_ref, ':', 2)
                                     AND r.radar_id = split_part(d.source_record_ref, ':', 3) LIMIT 1)
             WHEN 'staging' THEN (SELECT s.state FROM lending.calling_pool_staging s
                                   WHERE s.id::text = split_part(d.source_record_ref, ':', 2))
             WHEN 'wholesaler' THEN :wholesaler_state
           END AS state,
           CASE WHEN d.source_record_ref LIKE 'wholesaler:%'
                THEN split_part(d.source_record_ref, ':', 2) END AS wholesaler_source,
           (EXISTS (SELECT 1 FROM lending.web_leads w WHERE w.phone = d.phone)
            OR EXISTS (SELECT 1 FROM lending.lendingflow_leads l WHERE l.phone = d.phone)) AS warm,
           (EXISTS (SELECT 1 FROM lending.suppression_list s WHERE s.phone = d.phone)
            OR EXISTS (SELECT 1 FROM lending.contacts c WHERE c.phone = d.phone AND c.do_not_contact)) AS suppressed,
           EXISTS (SELECT 1 FROM lending.dialer_load_records p
                    WHERE p.phone = d.phone AND p.pool = d.pool AND p.id <> d.id
                      AND p.ghl_sync_status = :pushed) AS already_in_ghl
      FROM lending.dialer_load_records d
     WHERE d.active AND d.ghl_sync_status IS NULL
     ORDER BY d.id
     LIMIT :limit
""")

_MARK = text("""
    UPDATE lending.dialer_load_records AS d
       SET ghl_sync_status = v.status,
           ghl_sync_attempts = d.ghl_sync_attempts + v.attempted,
           ghl_synced_at = CASE WHEN v.status = :pushed THEN now() ELSE d.ghl_synced_at END,
           ghl_sync_error = v.error
      FROM unnest(CAST(:ids AS bigint[]), CAST(:statuses AS text[]), CAST(:attempted AS int[]),
                  CAST(:errors AS text[])) AS v(id, status, attempted, error)
     WHERE d.id = v.id AND d.ghl_sync_status IS NULL
""")

_REQUEUE = text("""
    UPDATE lending.dialer_load_records
       SET ghl_sync_status = NULL, ghl_sync_attempts = 0, ghl_sync_error = NULL
     WHERE active AND ghl_sync_status = ANY(:statuses)
""")

# GhlWholesalerSink raises RuntimeError("<step>: HTTP <code>") / ("<step>: <ExceptionClass>"): no request data.
_SAFE_SINK_ERROR = re.compile(r"^[a-z ]{1,40}: (?:HTTP \d{3}|[A-Za-z]{1,60}|no contact id in response)$")


def safe_error(exc: Exception) -> str:
    """The exception class, plus the sink's step/status when it has that exact shape; never a free-form message."""
    message = str(exc)
    if isinstance(exc, RuntimeError) and _SAFE_SINK_ERROR.match(message):
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__


def sync_tags(row: Mapping[str, Any]) -> Optional[list[str]]:
    """Tags for a row, or None when its pool has no GHL list."""
    pool_tag = GHL_LOAD_SYNC_POOL_TAGS.get(str(row.get("pool")))
    if pool_tag is None:
        return None
    tags = [pool_tag, GHL_TAG_COLD_LEAD]
    state = str(row.get("state") or "").strip().lower()
    if state:
        tags.append(f"{GHL_TAG_STATE_PREFIX}{state}")
    source = str(row.get("wholesaler_source") or "").strip().lower()
    if source:
        tags.append(f"{GHL_TAG_SOURCE_PREFIX}{source}")
    return tags


@dataclass
class Decision:
    """What to do with one row: ``skip`` set = record it and move on; else push with ``tags``."""
    skip: Optional[SyncStatus] = None
    tags: list[str] = field(default_factory=list)


def decide(row: Mapping[str, Any]) -> Decision:
    if row["warm"]:
        return Decision(SyncStatus.SKIPPED_WARM)
    if row["suppressed"]:
        return Decision(SyncStatus.SKIPPED_SUPPRESSED)
    if row["already_in_ghl"]:
        return Decision(SyncStatus.SKIPPED_ALREADY_IN_GHL)
    tags = sync_tags(row)
    if tags is None:
        return Decision(SyncStatus.SKIPPED_UNMAPPED)
    return Decision(tags=tags)


@dataclass
class Outcome:
    """Per-row result: status None = left pending for the next run."""
    id: int
    status: Optional[SyncStatus]
    attempted: int = 0
    error: Optional[str] = None


@dataclass
class SyncResult:
    counts: Counter = field(default_factory=Counter)
    newly_failed: list[int] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"counts": dict(self.counts), "newly_failed": len(self.newly_failed)}


def process(rows: Sequence[Mapping[str, Any]], sink: Optional[GhlTagSink], *,
            extra_tags: Sequence[str] = (), halted: Optional[Callable[[], bool]] = None) -> list[Outcome]:
    """Decide every row; push the pushable ones through ``sink`` (None = dry run, nothing pushed).

    ``extra_tags`` ride on every push (the call gate's hold tag). ``halted`` is checked before the first row and
    every GHL_LOAD_SYNC_HALT_CHECK_EVERY rows; once it is true the rest get no outcome and stay pending.
    """
    outcomes: list[Outcome] = []
    for index, row in enumerate(rows):
        if halted is not None and index % GHL_LOAD_SYNC_HALT_CHECK_EVERY == 0 and halted():
            logger.warning("[ghl-load-sync] emergency halt set mid-run; %d row(s) left pending", len(rows) - index)
            break
        decision = decide(row)
        if decision.skip is not None:
            if decision.skip is SyncStatus.SKIPPED_UNMAPPED:
                logger.warning("[ghl-load-sync] pool %s has no GHL tag; load record %s not pushed", row["pool"], row["id"])
            outcomes.append(Outcome(row["id"], decision.skip))
            continue
        if sink is None:
            outcomes.append(Outcome(row["id"], SyncStatus.PUSHED))
            continue
        try:
            sink.push(row, [*decision.tags, *extra_tags])
        except Exception as exc:
            error = safe_error(exc)
            logger.warning("[ghl-load-sync] push failed for load record %s (%s): %s", row["id"], row["pool"], error)
            final = row["ghl_sync_attempts"] + 1 >= GHL_LOAD_SYNC_MAX_ATTEMPTS
            outcomes.append(Outcome(row["id"], SyncStatus.FAILED if final else None, attempted=1, error=error))
        else:
            outcomes.append(Outcome(row["id"], SyncStatus.PUSHED, attempted=1))
    return outcomes


def summarize(outcomes: Sequence[Outcome]) -> SyncResult:
    result = SyncResult()
    for outcome in outcomes:
        result.counts[outcome.status.value if outcome.status else RETRY] += 1
        if outcome.status is SyncStatus.FAILED:
            result.newly_failed.append(outcome.id)
    return result


def fetch_pending(db, limit: int) -> list[dict[str, Any]]:
    params = {"limit": limit, "pushed": SyncStatus.PUSHED.value, "wholesaler_state": _WHOLESALER_STATE}
    return [dict(r) for r in db.execute(_PENDING, params).mappings()]


def record(db, outcomes: Sequence[Outcome]) -> None:
    """Write every outcome in one statement; only rows still pending are touched."""
    if not outcomes:
        return
    db.execute(_MARK, {
        "pushed": SyncStatus.PUSHED.value,
        "ids": [o.id for o in outcomes],
        "statuses": [o.status.value if o.status else None for o in outcomes],
        "attempted": [o.attempted for o in outcomes],
        "errors": [o.error for o in outcomes],
    })


def requeue(db) -> int:
    return int(db.execute(_REQUEUE, {"statuses": list(REQUEUE_STATUSES)}).rowcount or 0)


def _notify_failed(ids: Sequence[int]) -> None:
    from src.lending.ops_alerts import post_ops_notice

    try:
        post_ops_notice(f"GHL load sync: {len(ids)} dialer load record(s) could not be pushed to GHL after "
                        f"{GHL_LOAD_SYNC_MAX_ATTEMPTS} attempts (ids {', '.join(map(str, ids[:20]))}). "
                        "See lending.dialer_load_records.ghl_sync_error; requeue with --requeue.")
    except Exception as exc:
        logger.error("[ghl-load-sync] ops notice for %d failed record(s) not posted: %s", len(ids), type(exc).__name__)


def _parse_args(argv: Optional[list[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push BatchDialer loads to GHL as cold contacts.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="count what the next run would do; push nothing")
    mode.add_argument("--requeue", action="store_true", help="put failed and skipped_suppressed rows back in the queue")
    parser.add_argument("--limit", type=int, default=GHL_LOAD_SYNC_BATCH_SIZE)
    return parser.parse_args(argv)


def _dry_run(limit: int) -> int:
    from src.core.database import get_db_context

    with get_db_context() as db:
        rows = fetch_pending(db, limit)
        db.rollback()
    logger.info("[ghl-load-sync] dry run: %s", json.dumps(summarize(process(rows, None)).as_dict()))
    return EXIT_OK


def _requeue() -> int:
    from src.core.database import get_db_context

    with get_db_context() as db:
        count = requeue(db)
        db.commit()
    logger.info("[ghl-load-sync] requeued %d row(s)", count)
    return EXIT_OK


def hold_tags() -> tuple[str, ...]:
    """While the GHL call gate runs, a new contact enters Josh's lists held (ndl-do-not-call-now); the gate lifts the
    hold once every calling rule passes. With the gate off nothing would ever lift it, so no hold is added."""
    return (GHL_DO_NOT_CALL_NOW_TAG,) if get_settings().lending_ghl_call_gate_enabled else ()


def _live_run(limit: int, sink: GhlTagSink) -> SyncResult:
    """Read (no row locks), push with no transaction open, then write all outcomes in one short transaction."""
    from src.core.database import get_db_context

    with get_db_context() as db:
        rows = fetch_pending(db, limit)
        db.commit()
    outcomes = process(rows, sink, extra_tags=hold_tags(), halted=is_globally_halted)
    try:
        with get_db_context() as db:
            record(db, outcomes)
            db.commit()
    except Exception as exc:
        # Pushed rows stay pending and are pushed again next run: the upsert matches, so no duplicate contact.
        logger.error("[ghl-load-sync] outcomes for %d row(s) not recorded: %s", len(outcomes), type(exc).__name__)
        raise
    return summarize(outcomes)


def main(argv: Optional[list[str]] = None) -> int:
    from src.core.database import get_db_context

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    if args.dry_run:
        return _dry_run(args.limit)
    if args.requeue:
        return _requeue()
    if not get_settings().lending_ghl_load_sync_enabled:
        logger.info("[ghl-load-sync] LENDING_GHL_LOAD_SYNC_ENABLED is off; nothing done")
        return EXIT_OK
    if is_globally_halted():
        logger.warning("[ghl-load-sync] lending emergency halt is on; nothing pushed")
        return EXIT_REFUSED
    sink = ghl_sink()
    if sink is None:
        logger.error("[ghl-load-sync] lending GHL account not configured; nothing pushed")
        return EXIT_REFUSED

    # Session-level lock on its own connection: held across the GHL calls without holding any row lock.
    with get_db_context() as lock_db:
        if not lock_db.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": GHL_LOAD_SYNC_LOCK_KEY}).scalar():
            logger.info("[ghl-load-sync] another run holds the lock; skipped")
            return EXIT_OK
        try:
            result = _live_run(args.limit, sink)
        finally:
            lock_db.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": GHL_LOAD_SYNC_LOCK_KEY})
            lock_db.commit()
    logger.info("[ghl-load-sync] %s", json.dumps(result.as_dict()))
    if result.newly_failed:
        _notify_failed(result.newly_failed)
    return EXIT_FAILED if result.counts.get(RETRY) or result.newly_failed else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
