"""
One-time rescore operation: backup → delete stale scores → reset GHL fields → rescore all.

Addresses the lead quality fix approved 2026-04-14:
  - 21,604 distress_scores written under the old algorithm are stale.
  - Scoring now gates building-permit-only leads, recent deed transfers
    (< 45 days), and solo insurance claims — none of which were filtered
    before.

Pre-requisite (do this BEFORE running --confirm)
-------------------------------------------------
Bulk-delete all distressed-property contacts from the GHL UI.
  GHL Contacts → filter by tag "distressed-property" → Select All → Delete

This avoids API rate limits entirely. The script then clears the matching
gohighlevel_contact_id values from the properties table so ghl_sync creates
fresh contacts instead of trying to PUT to deleted IDs.

Steps
-----
1. Preview current distress_scores counts by tier.
2. Export all distress_scores to a timestamped CSV backup at reports/backups/.
3. Delete all distress_scores for the county.
4. Clear gohighlevel_contact_id and reset sync_status on all previously-synced
   properties (since their GHL contacts were deleted from the UI).
5. Run the CDS scoring engine — Gold+ properties get sync_status=pending_sync.
6. Print summary and GHL sync instructions.

Usage
-----
    # Dry-run: preview counts and write backup, skip all destructive steps
    python scripts/rescore_operation.py --dry-run

    # Full run (requires explicit confirmation)
    python scripts/rescore_operation.py --confirm

After this script completes, run GHL sync in an off-hours window:
    python -m src.tasks.ghl_sync --county-id hillsborough
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path when run as a script
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import DistressScore
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

BACKUP_DIR = Path("reports/backups")
RESCORE_FLAG = Path("data/rescore_in_progress.flag")

_EXPORT_COLS = [
    "id", "property_id", "county_id", "score_date", "final_cds_score",
    "lead_tier", "urgency_level", "qualified", "multiplier",
    "scoring_run_id", "vertical_scores", "distress_types", "factor_scores",
]


# ---------------------------------------------------------------------------
# Step 1 — Preview counts
# ---------------------------------------------------------------------------

def preview_counts(session, county_id: str) -> dict:
    rows = session.execute(
        text("""
            SELECT lead_tier, COUNT(*) AS cnt
            FROM distress_scores
            WHERE county_id = :county_id
            GROUP BY lead_tier
            ORDER BY cnt DESC
        """),
        {"county_id": county_id},
    ).fetchall()

    total = sum(r.cnt for r in rows)
    tiers = {r.lead_tier or "NULL": r.cnt for r in rows}
    gold_plus = sum(tiers.get(t, 0) for t in ("Ultra Platinum", "Platinum", "Gold"))

    # Count previously-synced properties so we can report how many GHL
    # contact IDs will be cleared in step 4.
    synced_count = session.execute(
        text("""
            SELECT COUNT(*) FROM properties
            WHERE county_id = :county_id
              AND gohighlevel_contact_id IS NOT NULL
        """),
        {"county_id": county_id},
    ).scalar() or 0

    return {
        "total": total,
        "tiers": tiers,
        "gold_plus": gold_plus,
        "synced_properties": synced_count,
    }


# ---------------------------------------------------------------------------
# Step 2 — Backup
# ---------------------------------------------------------------------------

def backup_scores(session, county_id: str) -> tuple:
    """Export all distress_scores for county_id to a timestamped CSV.

    Returns (backup_path, row_count).
    """
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    backup_path = BACKUP_DIR / f"distress_scores_backup_{county_id}_{ts}.csv"

    rows = (
        session.query(DistressScore)
        .filter(DistressScore.county_id == county_id)
        .yield_per(1000)
    )

    row_count = 0
    with open(backup_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(_EXPORT_COLS)
        for row in rows:
            writer.writerow([
                row.id,
                row.property_id,
                row.county_id,
                row.score_date.isoformat() if row.score_date else None,
                float(row.final_cds_score) if row.final_cds_score is not None else None,
                row.lead_tier,
                row.urgency_level,
                row.qualified,
                float(row.multiplier) if row.multiplier is not None else None,
                row.scoring_run_id,
                json.dumps(row.vertical_scores) if row.vertical_scores else None,
                json.dumps(row.distress_types) if row.distress_types else None,
                json.dumps(row.factor_scores) if row.factor_scores else None,
            ])
            row_count += 1

    return backup_path, row_count


# ---------------------------------------------------------------------------
# Step 3 — Delete stale scores
# ---------------------------------------------------------------------------

def delete_scores(session, county_id: str) -> int:
    """Hard-delete all distress_scores for county_id. Returns deleted row count."""
    result = session.execute(
        text("DELETE FROM distress_scores WHERE county_id = :county_id"),
        {"county_id": county_id},
    )
    session.commit()
    return result.rowcount


# ---------------------------------------------------------------------------
# Step 4 — Reset GHL contact fields on properties
# ---------------------------------------------------------------------------

def reset_ghl_fields(session, county_id: str) -> int:
    """
    Clear gohighlevel_contact_id and reset sync_status to 'not_synced' for
    all properties that were previously pushed to GHL.

    Why: contacts were deleted from GHL via the UI. If we leave the old
    contact IDs in place, ghl_sync will try to PUT to deleted contacts and
    get 404s. Clearing them forces ghl_sync to POST (create fresh contacts)
    for the newly qualified leads after rescoring.
    """
    result = session.execute(
        text("""
            UPDATE properties
               SET gohighlevel_contact_id = NULL,
                   sync_status            = 'pending',
                   updated_at             = NOW()
             WHERE county_id = :county_id
               AND gohighlevel_contact_id IS NOT NULL
        """),
        {"county_id": county_id},
    )
    session.commit()
    return result.rowcount


# ---------------------------------------------------------------------------
# Step 5 — Rescore
# ---------------------------------------------------------------------------

def run_rescore(county_id: str) -> dict:
    """Run CDS scoring engine. Gold+ leads get sync_status=pending_sync.
    The actual GHL API push is handled by ghl_sync.py, run separately.
    """
    from src.services.cds_engine import MultiVerticalScorer

    with get_db_context() as session:
        scorer = MultiVerticalScorer(session)
        scores = scorer.score_all_properties(save_to_db=True)
        session.commit()

    tier_counts: dict = {}
    for s in scores:
        t = s.get("lead_tier") or "unscored"
        tier_counts[t] = tier_counts.get(t, 0) + 1

    gold_plus = sum(
        tier_counts.get(t, 0)
        for t in ("Ultra Platinum", "Platinum", "Gold")
    )
    return {"total_scored": len(scores), "gold_plus": gold_plus, "tier_counts": tier_counts}


# ---------------------------------------------------------------------------
# Sentinel helpers
# ---------------------------------------------------------------------------

def _write_flag(county_id: str) -> None:
    Path("data").mkdir(parents=True, exist_ok=True)
    RESCORE_FLAG.write_text(
        f"Rescore started at {datetime.now(timezone.utc).isoformat()} "
        f"for county={county_id}\n"
    )


def _clear_flag() -> None:
    try:
        RESCORE_FLAG.unlink(missing_ok=True)
    except Exception as exc:
        logger.warning("Could not remove rescore flag: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backup, delete, and rescore all distress_scores under the new algorithm."
    )
    parser.add_argument("--county", default="hillsborough")
    parser.add_argument(
        "--confirm", action="store_true",
        help="Required to execute the destructive steps.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview counts and write backup CSV without deleting or rescoring.",
    )
    args = parser.parse_args()

    if not args.confirm and not args.dry_run:
        print(
            "\nSafety check: pass --confirm to execute, or --dry-run to preview.\n"
            "  Dry-run : python scripts/rescore_operation.py --dry-run\n"
            "  Full run: python scripts/rescore_operation.py --confirm\n"
        )
        sys.exit(1)

    county = args.county

    print(f"\n{'='*60}")
    print(f"  Rescore Operation  |  county={county}")
    print(f"{'='*60}")

    # ── Step 1: Preview ───────────────────────────────────────────────────────
    print("\n[1/5] Checking current state...")
    with get_db_context() as session:
        counts = preview_counts(session, county)

    print(f"      distress_scores : {counts['total']:,} rows")
    print(f"      Gold+           : {counts['gold_plus']:,}")
    for tier, cnt in counts["tiers"].items():
        print(f"        {tier:<22} {cnt:>6,}")
    print(f"      GHL contact IDs : {counts['synced_properties']:,} properties to reset")

    # ── Step 2: Backup ────────────────────────────────────────────────────────
    print(f"\n[2/5] Backing up {counts['total']:,} score rows to CSV...")
    with get_db_context() as session:
        backup_path, backed_up = backup_scores(session, county)
    print(f"      Backup written: {backup_path}  ({backed_up:,} rows)")

    if args.dry_run:
        print("\n[DRY-RUN] Stopping here. Re-run with --confirm to proceed.\n")
        return

    # ── Step 3: Delete scores ─────────────────────────────────────────────────
    print(f"\n[3/5] Deleting {counts['total']:,} stale score rows...")
    _write_flag(county)
    try:
        with get_db_context() as session:
            deleted = delete_scores(session, county)
        print(f"      Deleted: {deleted:,} rows")
    except Exception as exc:
        _clear_flag()
        logger.error("Delete failed — flag cleared, no data lost: %s", exc, exc_info=True)
        sys.exit(1)

    # ── Step 4: Reset GHL fields ──────────────────────────────────────────────
    print(f"\n[4/5] Clearing stale GHL contact IDs from properties table...")
    try:
        with get_db_context() as session:
            reset_count = reset_ghl_fields(session, county)
        print(f"      Reset: {reset_count:,} properties (gohighlevel_contact_id → NULL)")
    except Exception as exc:
        logger.error("GHL field reset failed: %s", exc, exc_info=True)
        print("  WARNING: GHL field reset failed — see logs. Continuing to rescore.")

    # ── Step 5: Rescore ───────────────────────────────────────────────────────
    print("\n[5/5] Running CDS scoring engine (this will take several minutes)...")
    try:
        result = run_rescore(county)
    except Exception as exc:
        logger.error("Rescore failed: %s", exc, exc_info=True)
        print(
            "\n  ERROR: Rescore failed — see logs above.\n"
            "  data/rescore_in_progress.flag is still present.\n"
            "  Fix the issue and re-run with --confirm, or delete the flag manually.\n"
        )
        sys.exit(1)

    _clear_flag()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  RESCORE COMPLETE")
    print(f"{'='*60}")
    print(f"  Backup       : {backup_path}")
    print(f"  Rows deleted : {deleted:,}")
    print(f"  GHL IDs reset: {reset_count:,} properties")
    print(f"  Rescored     : {result['total_scored']:,} properties")
    print(f"  Gold+        : {result['gold_plus']:,} leads queued for GHL sync")
    print()
    for tier, cnt in sorted(result["tier_counts"].items(), key=lambda x: x[1], reverse=True):
        print(f"    {tier:<24} {cnt:>6,}")
    print()
    print("  Next step — GHL sync (run in an off-hours window):")
    print("    python -m src.tasks.ghl_sync --county-id hillsborough")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
