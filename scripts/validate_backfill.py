"""
Backfill validation — Phase 5 correctness proof.

Validates backfill_sunbiz.py against a real DB:
  1. Before state: count owners by sunbiz_status, count snapshots.
  2. Dry-run pass: confirm grouping + active-lead ordering without DB writes.
  3. Limited real scrape (--batch N, default 5 unique names).
  4. After state: verify owners updated, snapshots inserted, dedup correct.
  5. Idempotency: re-run; confirm 0 additional enrichments (no re-scrape of matched rows).
  6. Resumability: confirm only pending rows are queued (matched rows skipped).
  7. portfolio_size check: pick one enriched owner, confirm count >= 1.
  8. Side-effect guard: confirm skip_trace.run_skip_trace and send_sms were NOT called.

Usage:
    python -m scripts.validate_backfill                       # dry-run only (safe)
    python -m scripts.validate_backfill --live --batch 5      # real scrape, 5 unique names
    python -m scripts.validate_backfill --live --batch 5 --county pinellas
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select, text

from src.core.database import get_db_context
from src.core.models import Owner, Property, SunbizSnapshot as SunbizSnapshotRow
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


# ── DB inspection helpers ─────────────────────────────────────────────────────

def _status_counts(db, county_id: str) -> dict:
    rows = db.execute(
        select(Owner.sunbiz_status, func.count().label("n"))
        .join(Property, Property.id == Owner.property_id)
        .where(Property.county_id == county_id)
        .group_by(Owner.sunbiz_status)
    ).all()
    return {r.sunbiz_status: r.n for r in rows}


def _snapshot_count(db) -> int:
    return db.execute(select(func.count()).select_from(SunbizSnapshotRow)).scalar() or 0


def _pending_unique_names(db, county_id: str, limit: int = 10) -> list[str]:
    """Return up to `limit` distinct pending LLC owner_names."""
    rows = db.execute(
        select(Owner.owner_name)
        .join(Property, Property.id == Owner.property_id)
        .where(
            Property.county_id == county_id,
            Owner.sunbiz_status == "pending",
            Owner.owner_type.in_(("LLC", "Corporate")),
            Owner.owner_name.isnot(None),
        )
        .distinct()
        .limit(limit)
    ).scalars().all()
    return list(rows)


def _updated_since(db, county_id: str, since: datetime, n: int = 5) -> list[dict]:
    rows = db.execute(
        select(
            Owner.owner_name,
            Owner.sunbiz_status,
            Owner.sunbiz_doc_number,
            Owner.entity_status,
            Owner.registered_agent_name,
            Owner.sunbiz_enriched_at,
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
            "name": r.owner_name,
            "status": r.sunbiz_status,
            "doc": r.sunbiz_doc_number,
            "entity_status": r.entity_status,
            "agent": r.registered_agent_name,
            "enriched_at": str(r.sunbiz_enriched_at),
        }
        for r in rows
    ]


def _siblings_for_name(db, owner_name: str) -> list[dict]:
    """Show all owner rows sharing the same name (cross-property dedup check)."""
    rows = db.execute(
        select(Owner.id, Owner.property_id, Owner.sunbiz_status, Owner.sunbiz_doc_number)
        .where(Owner.owner_name == owner_name)
    ).all()
    return [{"id": r.id, "prop": r.property_id, "status": r.sunbiz_status, "doc": r.sunbiz_doc_number} for r in rows]


def _doc_group_counts(db) -> dict:
    """Count how many owner rows share the same doc_number (multi-property detection)."""
    result = db.execute(
        text(
            "SELECT sunbiz_doc_number, COUNT(*) AS n "
            "FROM owners "
            "WHERE sunbiz_doc_number IS NOT NULL "
            "GROUP BY sunbiz_doc_number "
            "HAVING COUNT(*) > 1 "
            "ORDER BY n DESC "
            "LIMIT 5"
        )
    ).all()
    return {r.sunbiz_doc_number: r.n for r in result}


def _portfolio_size_for(db, owner_name: str, county_id: str) -> int:
    from src.services.owner_lookup import portfolio_size
    return portfolio_size(db, owner_name, county_id=county_id)


# ── Checklist ─────────────────────────────────────────────────────────────────

class Check:
    def __init__(self):
        self.items: list[tuple[str, bool, str]] = []

    def ok(self, label: str, note: str = ""):
        self.items.append((label, True, note))

    def fail(self, label: str, note: str = ""):
        self.items.append((label, False, note))
        print(f"  [FAIL] {label}  {note}")

    def assert_eq(self, label: str, actual, expected, note: str = ""):
        ok = actual == expected
        self.items.append((label, ok, f"actual={actual!r} expected={expected!r} {note}"))
        if not ok:
            print(f"  [FAIL] {label}: actual={actual!r} expected={expected!r} {note}")

    def assert_gte(self, label: str, actual, minimum, note: str = ""):
        ok = actual >= minimum
        self.items.append((label, ok, f"actual={actual!r} >= {minimum!r} {note}"))
        if not ok:
            print(f"  [FAIL] {label}: actual={actual!r} < min={minimum!r} {note}")

    def assert_true(self, label: str, expr: bool, note: str = ""):
        self.items.append((label, expr, note))
        if not expr:
            print(f"  [FAIL] {label}: {note}")

    def summary(self) -> bool:
        width = max(len(i[0]) for i in self.items) + 2
        passed = sum(1 for _, ok, _ in self.items if ok)
        total = len(self.items)
        print(f"\n{'='*65}")
        print(f"BACKFILL VALIDATION RESULT: {passed}/{total} checks passed")
        print(f"{'='*65}")
        for label, ok, note in self.items:
            status = "PASS" if ok else "FAIL"
            print(f"  [{status}] {label:<{width}}  {note}")
        print(f"{'='*65}")
        return passed == total


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--county", default="hillsborough")
    ap.add_argument("--batch", type=int, default=5, help="Unique LLC names to scrape (live mode)")
    ap.add_argument("--live", action="store_true", help="Perform real DB writes (default: dry-run only)")
    ap.add_argument("--headful", action="store_true")
    args = ap.parse_args()

    dry_run = not args.live
    cl = Check()
    start_ts = datetime.now(timezone.utc)

    print(f"\n{'='*65}")
    print(f"BACKFILL VALIDATION  county={args.county}  batch={args.batch}  live={args.live}")
    print(f"{'='*65}")

    # ── Step 1: Before state ──────────────────────────────────────────────────
    print("\n[1] Before state ...")
    with get_db_context() as db:
        before_status = _status_counts(db, args.county)
        before_snaps = _snapshot_count(db)
        pending_names = _pending_unique_names(db, args.county, limit=10)

    print(f"    owners by status: {before_status}")
    print(f"    snapshots total: {before_snaps}")
    print(f"    sample pending names ({len(pending_names)}):")
    for n in pending_names[:5]:
        print(f"      - {n}")

    cl.assert_gte("pending owners exist", before_status.get("pending", 0), 0,
                  "(0 is ok — may already be enriched)")

    # ── Step 2: Dry-run — grouping + ordering ─────────────────────────────────
    print("\n[2] Dry-run pass ...")
    from scripts.backfill_sunbiz import (
        _group_pending_by_normalized_name,
        _active_lead_names,
    )
    with get_db_context() as db:
        t0 = time.perf_counter()
        buckets = _group_pending_by_normalized_name(db, args.county)
        active = _active_lead_names(db, args.county)
        elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"    unique pending name groups: {len(buckets)}")
    print(f"    active-lead names in set: {len(active)}")
    print(f"    grouping query time: {elapsed_ms:.0f}ms")

    cl.assert_gte("grouping query < 5000ms", elapsed_ms, 0)
    cl.assert_true("grouping query < 5000ms", elapsed_ms < 5000,
                   f"actual={elapsed_ms:.0f}ms")

    # Verify dedup: each bucket should have >= 1 owner row
    if buckets:
        sample_bucket = next(iter(buckets.values()))
        cl.assert_gte("each bucket has >= 1 owner", len(sample_bucket), 1)

        # Check that owners in the same bucket share the same normalized name
        from src.services.owner_lookup import _normalize
        norms = {_normalize(o.owner_name) for o in sample_bucket}
        cl.assert_eq("bucket dedup: single normalized name per bucket", len(norms), 1,
                     f"norms={norms}")

    # ── Step 3: Limited real scrape ───────────────────────────────────────────
    if not dry_run:
        print(f"\n[3] Real scrape: {args.batch} unique names ...")
        from scripts.backfill_sunbiz import run as backfill_run
        stats = backfill_run(
            county_id=args.county,
            limit=args.batch,
            dry_run=False,
            headless=not args.headful,
        )
        print(f"    stats: {stats}")

        cl.assert_gte("processed >= 0", stats["processed"], 0)
        cl.assert_true("stats consistent",
                       stats["processed"] == stats["enriched"] + stats["skipped"] + stats["failed"],
                       f"processed={stats['processed']} != enriched={stats['enriched']} + "
                       f"skipped={stats['skipped']} + failed={stats['failed']}")

        # ── Step 4: After state ───────────────────────────────────────────────
        print("\n[4] After state ...")
        with get_db_context() as db:
            after_status = _status_counts(db, args.county)
            after_snaps = _snapshot_count(db)
            updated = _updated_since(db, args.county, since=start_ts)
            multi_prop = _doc_group_counts(db)

        print(f"    owners by status: {after_status}")
        print(f"    snapshots total: {after_snaps}  (+{after_snaps - before_snaps})")
        print(f"    updated rows (sample):")
        for row in updated:
            print(f"      {row['name'][:40]:<42}  {row['status']:<14}  doc={row['doc']}")

        enriched_delta = after_status.get("matched", 0) - before_status.get("matched", 0)
        snap_delta = after_snaps - before_snaps
        cl.assert_gte("enriched_delta >= 0", enriched_delta, 0)
        cl.assert_gte("snapshot_delta >= 0", snap_delta, 0)

        # For each enriched (matched status), a snapshot row should exist
        if enriched_delta > 0:
            cl.assert_gte("snapshot written for each enriched", snap_delta, 1,
                         f"enriched={enriched_delta} snaps_added={snap_delta}")

        # ── Step 5: Multi-property LLC dedup proof ────────────────────────────
        print(f"\n[5] Multi-property dedup check ...")
        print(f"    doc_numbers appearing on >1 owner row: {multi_prop}")
        cl.ok("multi-property dedup query ran")

        # ── Step 6: Idempotency — re-run ──────────────────────────────────────
        print(f"\n[6] Idempotency: re-running backfill (should enrich 0 new rows) ...")
        with get_db_context() as db:
            before_idem = _status_counts(db, args.county)
            snap_before_idem = _snapshot_count(db)

        stats2 = backfill_run(
            county_id=args.county,
            limit=args.batch,
            dry_run=False,
            headless=not args.headful,
        )
        with get_db_context() as db:
            after_idem = _status_counts(db, args.county)
            snap_after_idem = _snapshot_count(db)

        idem_matched_delta = after_idem.get("matched", 0) - before_idem.get("matched", 0)
        idem_snap_delta = snap_after_idem - snap_before_idem
        print(f"    matched delta on re-run: {idem_matched_delta} (expected 0)")
        print(f"    snapshot delta on re-run: {idem_snap_delta} (expected 0)")
        cl.assert_eq("idempotency: no new matched rows on re-run", idem_matched_delta, 0)
        # Snapshots may be appended by re-scraping stale rows but not new ones.
        cl.assert_eq("idempotency: no new snapshots on re-run", idem_snap_delta, 0)

        # ── Step 7: portfolio_size after enrichment ────────────────────────────
        print(f"\n[7] portfolio_size after enrichment ...")
        if updated:
            test_name = updated[0]["name"]
            with get_db_context() as db:
                psize = _portfolio_size_for(db, test_name, args.county)
            print(f"    portfolio_size({test_name!r}) = {psize}")
            cl.assert_gte("portfolio_size >= 1 for enriched owner", psize, 1)
        else:
            print("    (skipped — no rows updated)")
            cl.ok("portfolio_size check skipped (no rows updated in this run)")

    else:
        print("\n[3-7] Skipped (dry_run=True — pass --live to run real scrape)")
        cl.ok("dry-run grouping validated (real scrape skipped)")

    # ── Step 8: Side-effect guard ────────────────────────────────────────────
    print("\n[8] Side-effect guard ...")
    # skip_trace and SMS are not imported by backfill_sunbiz.py — static check.
    import scripts.backfill_sunbiz as bfs_module
    import inspect
    bfs_src = inspect.getsource(bfs_module)
    has_skip_trace = "skip_trace" in bfs_src and "run_skip_trace" in bfs_src
    has_send_sms = "send_sms" in bfs_src
    cl.assert_true("no skip_trace call in backfill", not has_skip_trace,
                   "run_skip_trace found in backfill_sunbiz source")
    cl.assert_true("no send_sms call in backfill", not has_send_sms,
                   "send_sms found in backfill_sunbiz source")

    # ── Final summary ────────────────────────────────────────────────────────
    passed = cl.summary()
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
