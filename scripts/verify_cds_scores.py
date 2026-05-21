"""
CDS Scoring Verification Script — Phase 1

Selects a sample of properties with varied signal combinations, runs the
current scoring engine against them, and prints a summary suitable for
comparing before/after a code change.

Usage:
    # Capture a baseline before changes:
    python scripts/verify_cds_scores.py --county hillsborough --sample 200 > baseline.txt

    # After deploying changes, compare:
    python scripts/verify_cds_scores.py --county hillsborough --sample 200 > after.txt
    diff baseline.txt after.txt

    # Dry-run (no DB writes):
    python scripts/verify_cds_scores.py --county hillsborough --sample 200 --dry-run

    # Also print EXPLAIN ANALYZE for key queries:
    python scripts/verify_cds_scores.py --explain-queries

Exit codes:
    0 — all checks passed
    1 — mismatch detected or error
"""

import argparse
import hashlib
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

# ── Bootstrap project path ────────────────────────────────────────────────────
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.services.cds_engine import MultiVerticalScorer

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ── Scoring fingerprint ───────────────────────────────────────────────────────

def _score_fingerprint(score: dict) -> str:
    """Stable hash of the fields that must not change between optimizations."""
    key = {
        "final_cds_score": round(score["final_cds_score"], 2),
        "lead_tier":        score["lead_tier"],
        "urgency_level":    score["urgency_level"],
        "qualified":        score["qualified"],
        "vertical_scores":  {k: round(v, 2) for k, v in score["vertical_scores"].items()},
        "distress_types":   sorted(score["distress_types"]),
        "signal_count":     score["signal_count"],
    }
    return hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()[:16]


# ── Query performance helpers ─────────────────────────────────────────────────

def _explain(session, label: str, sql: str, params: dict) -> None:
    print(f"\n{'─'*60}")
    print(f"EXPLAIN ANALYZE — {label}")
    print(f"{'─'*60}")
    rows = session.execute(
        sa_text(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql}"),
        params,
    ).fetchall()
    for row in rows:
        print(row[0])


def run_explain_queries(session, sample_pid: int) -> None:
    today = date.today()
    today_start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    tomorrow    = datetime(today.year, today.month, today.day + 1, tzinfo=timezone.utc)

    _explain(session, "today score lookup (range filter)", """
        SELECT id, final_cds_score, lead_tier
        FROM distress_scores
        WHERE property_id = :pid
          AND score_date >= :start
          AND score_date  < :end
        LIMIT 1
    """, {"pid": sample_pid, "start": today_start, "end": tomorrow})

    _explain(session, "latest score lookup (ORDER BY score_date DESC)", """
        SELECT final_cds_score, lead_tier
        FROM distress_scores
        WHERE property_id = :pid
        ORDER BY score_date DESC
        LIMIT 1
    """, {"pid": sample_pid})

    _explain(session, "property batch load (keyset placeholder)", """
        SELECT id, parcel_id, address, city, state, zip, county_id,
               year_built, sq_ft, beds, baths, lot_size
        FROM properties
        WHERE id > :after_id
          AND county_id = :county
        ORDER BY id
        LIMIT 500
    """, {"after_id": sample_pid - 1, "county": "hillsborough"})


# ── Sample property selection ─────────────────────────────────────────────────

def get_sample_property_ids(session, county_id: str, sample_size: int) -> list:
    """
    Pick a representative sample: mix of properties with/without signals,
    spread across all tiers in recent distress_scores.
    """
    # Tier-stratified: grab up to sample_size/5 per tier, then fill with random
    tiers = ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]
    per_tier = max(1, sample_size // len(tiers))

    ids = set()
    for tier in tiers:
        rows = session.execute(sa_text("""
            SELECT DISTINCT ds.property_id
            FROM distress_scores ds
            JOIN properties p ON p.id = ds.property_id
            WHERE p.county_id = :county
              AND ds.lead_tier = :tier
            ORDER BY ds.property_id
            LIMIT :n
        """), {"county": county_id, "tier": tier, "n": per_tier}).fetchall()
        ids.update(r[0] for r in rows)

    # Top up with properties that have never been scored (signals exist but no score)
    remaining = sample_size - len(ids)
    if remaining > 0:
        rows = session.execute(sa_text("""
            SELECT p.id FROM properties p
            WHERE p.county_id = :county
              AND NOT EXISTS (SELECT 1 FROM distress_scores ds WHERE ds.property_id = p.id)
            LIMIT :n
        """), {"county": county_id, "n": remaining}).fetchall()
        ids.update(r[0] for r in rows)

    return list(ids)[:sample_size]


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="CDS Scoring Verification Script")
    parser.add_argument("--county", default="hillsborough", help="County ID to sample from")
    parser.add_argument("--sample", type=int, default=200, help="Number of properties to score")
    parser.add_argument("--dry-run", action="store_true", help="Score but do not write to DB")
    parser.add_argument("--explain-queries", action="store_true", help="Print EXPLAIN ANALYZE for key queries")
    parser.add_argument("--property-ids", nargs="*", type=int, help="Score specific property IDs only")
    args = parser.parse_args()

    print(f"CDS Scoring Verification — {date.today()}")
    print(f"County: {args.county} | Sample: {args.sample} | Dry-run: {args.dry_run}")
    print("=" * 60)

    with get_db_context() as session:
        if args.property_ids:
            property_ids = args.property_ids
        else:
            property_ids = get_sample_property_ids(session, args.county, args.sample)

        if not property_ids:
            print("ERROR: No properties found for the given county/sample.")
            return 1

        print(f"Scoring {len(property_ids)} properties...")

        if args.explain_queries and property_ids:
            run_explain_queries(session, property_ids[0])

        scorer = MultiVerticalScorer(session)
        scores = scorer.score_all_properties(
            save_to_db=not args.dry_run,
            property_ids=property_ids,
            county_id=args.county,
        )
        total_scored = scorer._total_scored

    # ── Summary output ────────────────────────────────────────────────────────
    # scores only contains properties with ≥1 signal; _total_scored is the full count.
    print(f"\n{'='*60}")
    print(f"RESULTS: {total_scored} properties scored")

    with_signals = [s for s in scores if s["signal_count"] > 0 and s["final_cds_score"] > 0]
    no_signal_count = total_scored - len(with_signals)
    qualified    = [s for s in with_signals if s["qualified"]]

    print(f"With signals:   {len(with_signals)}")
    print(f"No signals:     {no_signal_count}")
    print(f"Qualified:      {len(qualified)}")

    # Tier distribution
    print("\nTIER DISTRIBUTION:")
    tier_counts = Counter(s["lead_tier"] for s in with_signals)
    for tier in ["Ultra Platinum", "Platinum", "Gold", "Silver", "Bronze"]:
        print(f"  {tier:<18} {tier_counts.get(tier, 0):>5}")

    # Vertical distribution
    print("\nTOP VERTICAL (driving final_cds_score):")
    top_v = Counter(
        max(s["vertical_scores"], key=s["vertical_scores"].get)
        for s in with_signals
    )
    for v, count in top_v.most_common():
        print(f"  {v:<22} {count:>5}")

    # Signal type frequency
    print("\nSIGNAL TYPE FREQUENCY:")
    sig_counts = Counter(t for s in with_signals for t in s["distress_types"])
    for sig_type, count in sig_counts.most_common():
        print(f"  {sig_type:<28} {count:>5}")

    # Score stats
    if with_signals:
        score_vals = [s["final_cds_score"] for s in with_signals]
        avg = sum(score_vals) / len(score_vals)
        print(f"\nScore avg: {avg:.1f}  min: {min(score_vals):.1f}  max: {max(score_vals):.1f}")

    # Fingerprint table — paste into diff to detect regressions
    print(f"\n{'─'*60}")
    print("SCORE FINGERPRINTS (property_id → sha256[:16] of key scoring fields)")
    print("Diff these between before/after to find any scoring changes:")
    fingerprints = {s["property_id"]: _score_fingerprint(s) for s in scores}
    for pid in sorted(fingerprints):
        print(f"  {pid:>10}  {fingerprints[pid]}")

    print(f"\n{'='*60}")
    print("Verification complete. Diff the fingerprint block to detect regressions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
