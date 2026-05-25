"""
Live batch Sunbiz enrichment runner (Area 2).

Pulls 20–50 pending LLC/Corporate owner rows from the real DB and runs them
through the Playwright scraper. Reports before/after stats and sample rows.

Usage:
    python -m scripts.live_batch_sunbiz                 # 30-row batch, dry-run
    python -m scripts.live_batch_sunbiz --limit 20 --live
    python -m scripts.live_batch_sunbiz --limit 50 --live --headful
    python -m scripts.live_batch_sunbiz --county pinellas --limit 20 --live

NOTE:
  - Default is dry-run (--live required for actual DB writes).
  - Pulls from sunbiz_status='pending' only — never touches 'matched' rows.
  - No skip-trace, no SMS, no GHL sync.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from sqlalchemy import func, select, text

from src.core.database import get_db_context
from src.core.models import Owner, Property, SunbizSnapshot as SunbizSnapshotRow
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def _before_counts(db, county_id: str) -> dict:
    """Snapshot of owners table by sunbiz_status."""
    rows = db.execute(
        select(Owner.sunbiz_status, func.count().label("n"))
        .join(Property, Property.id == Owner.property_id)
        .where(Property.county_id == county_id)
        .group_by(Owner.sunbiz_status)
    ).all()
    counts = {r.sunbiz_status: r.n for r in rows}
    snap_count = db.execute(
        select(func.count()).select_from(SunbizSnapshotRow)
    ).scalar() or 0
    counts["_snapshots"] = snap_count
    return counts


def _sample_pending(db, county_id: str, n: int = 5) -> list[str]:
    """Return up to n owner_names that are currently pending."""
    rows = db.execute(
        select(Owner.owner_name)
        .join(Property, Property.id == Owner.property_id)
        .where(
            Property.county_id == county_id,
            Owner.sunbiz_status == "pending",
            Owner.owner_type.in_(("LLC", "Corporate")),
        )
        .limit(n)
    ).scalars().all()
    return list(rows)


def _sample_updated(db, county_id: str, since: datetime, n: int = 5) -> list[dict]:
    """Return a few rows that were enriched since `since` (matched or parser_failed)."""
    rows = db.execute(
        select(
            Owner.owner_name,
            Owner.sunbiz_status,
            Owner.sunbiz_doc_number,
            Owner.entity_status,
            Owner.registered_agent_name,
        )
        .join(Property, Property.id == Owner.property_id)
        .where(
            Property.county_id == county_id,
            Owner.sunbiz_enriched_at >= since,
        )
        .limit(n)
    ).all()
    return [
        {
            "owner_name": r.owner_name,
            "status": r.sunbiz_status,
            "doc": r.sunbiz_doc_number,
            "entity_status": r.entity_status,
            "agent": r.registered_agent_name,
        }
        for r in rows
    ]


def _explain_portfolio_size_query(db) -> str:
    """EXPLAIN ANALYZE on the portfolio_size query to check index usage."""
    try:
        result = db.execute(
            text(
                "EXPLAIN ANALYZE SELECT COUNT(id) FROM owners "
                "WHERE owner_name = 'ACME TEST LLC'"
            )
        ).fetchall()
        return "\n".join(r[0] for r in result)
    except Exception as e:
        return f"EXPLAIN failed: {e}"


def main():
    ap = argparse.ArgumentParser(description="Live batch Sunbiz enrichment runner")
    ap.add_argument("--county", default="hillsborough")
    ap.add_argument("--limit", type=int, default=30, help="Max owners to process (20-50 recommended)")
    ap.add_argument("--live", action="store_true", help="Write to DB (default: dry-run)")
    ap.add_argument("--headful", action="store_true")
    args = ap.parse_args()

    dry_run = not args.live
    start_ts = datetime.now(timezone.utc)

    print(f"\n{'='*65}")
    print(f"LIVE BATCH SUNBIZ ENRICHMENT")
    print(f"  county={args.county}  limit={args.limit}  dry_run={dry_run}")
    print(f"{'='*65}")

    # ── Before state ─────────────────────────────────────────────────────────
    with get_db_context() as db:
        before = _before_counts(db, args.county)
        sample_before = _sample_pending(db, args.county)

    print(f"\n[BEFORE] owners by sunbiz_status (county={args.county}):")
    for k, v in sorted(before.items()):
        print(f"  {k:<20} {v:>8}")
    print(f"\n[BEFORE] sample pending names:")
    for n in sample_before:
        print(f"  - {n}")

    # ── Run enrichment ────────────────────────────────────────────────────────
    print(f"\n[RUN] Starting Playwright batch (dry_run={dry_run}) ...")
    t0 = time.perf_counter()

    from src.scrappers.sunbiz.sunbiz_engine import run_sunbiz_pipeline
    stats = run_sunbiz_pipeline(
        limit=args.limit,
        dry_run=dry_run,
        county_id=args.county,
        headless=not args.headful,
    )

    elapsed = time.perf_counter() - t0
    print(f"\n[RUN] Completed in {elapsed:.1f}s")
    print(f"  processed={stats.get('processed', 0)}")
    print(f"  enriched ={stats.get('enriched', 0)}")
    print(f"  skipped  ={stats.get('skipped', 0)}")
    print(f"  failed   ={stats.get('failed', 0)}")

    # ── After state ───────────────────────────────────────────────────────────
    with get_db_context() as db:
        after = _before_counts(db, args.county)
        sample_after = _sample_updated(db, args.county, since=start_ts)
        explain_out = _explain_portfolio_size_query(db)

    if not dry_run:
        print(f"\n[AFTER] owners by sunbiz_status (county={args.county}):")
        for k, v in sorted(after.items()):
            delta = v - before.get(k, 0)
            sign = f"+{delta}" if delta > 0 else str(delta)
            print(f"  {k:<20} {v:>8}  ({sign})")

        print(f"\n[AFTER] sample updated rows (since run start):")
        if sample_after:
            for row in sample_after:
                print(f"  - {row['owner_name'][:40]:<42}  status={row['status']:<14}  doc={row['doc']}  agent={row['agent']}")
        else:
            print("  (none updated — dry-run or no pending rows)")

    # ── Portfolio size index check ────────────────────────────────────────────
    print(f"\n[PERF] EXPLAIN ANALYZE on portfolio_size query:")
    for line in explain_out.splitlines()[:10]:
        print(f"  {line}")

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    passed = stats.get("enriched", 0) + stats.get("skipped", 0) + stats.get("failed", 0) == stats.get("processed", 0)
    print(f"RESULT: {'PASS' if passed else 'FAIL'} — stats consistent")
    if not dry_run:
        matched_delta = after.get("matched", 0) - before.get("matched", 0)
        snap_delta = after.get("_snapshots", 0) - before.get("_snapshots", 0)
        print(f"  matched rows added: {matched_delta}")
        print(f"  snapshot rows added: {snap_delta}")
    print(f"{'='*65}\n")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
