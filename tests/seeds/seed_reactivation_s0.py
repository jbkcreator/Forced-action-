"""
seed_reactivation_s0 — E2E seed + cleanup for Sprint S0 reactivation scheduler.

Usage:
    python tests/seeds/seed_reactivation_s0.py --seed
    python tests/seeds/seed_reactivation_s0.py --run-test [--live]
    python tests/seeds/seed_reactivation_s0.py --cleanup
    python tests/seeds/seed_reactivation_s0.py --show

Flow:
    1. --seed   : Inserts all test fixtures and writes .reactivation_seed_state.json.
    2. --run-test: Runs the scheduler against seeded data (dry-run by default; use --live
                   to send a real email to the seed address).
    3. --cleanup : Reads .reactivation_seed_state.json and removes/restores every row.
    4. --show   : Prints the current tracking state without touching the DB.

Seed identifiers (clearly fake — safe to grep for in prod logs):
    subscriber.email              = s0.reactivation.seed@forcedaction.internal
    subscriber.stripe_customer_id = cus_S0REACTIVATION_SEED001
    property.parcel_id            = S0-SEED-PROP-TEST-001
    county                        = hillsborough
    zip_code                      = 33601
    vertical                      = roofing

The seed creates rows in dependency order:
    subscribers → expansion_candidates (upsert) → properties → distress_scores
    → waitlist_entries → gold_plus_zip_snapshots

Cleanup runs in reverse order and restores expansion_candidates to their
original status/launched_at if the row already existed.
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text

from src.core.database import get_db_context

logger = logging.getLogger(__name__)

# ── Seed constants ────────────────────────────────────────────────────────────

SEED_EMAIL = "s0.reactivation.seed@forcedaction.internal"
SEED_STRIPE_CID = "cus_S0REACTIVATION_SEED001"
SEED_PARCEL_ID = "S0-SEED-PROP-TEST-001"
SEED_COUNTY = "hillsborough"
SEED_ZIP = "33601"
SEED_VERTICAL = "roofing"

STATE_FILE = Path(__file__).parents[2] / ".reactivation_seed_state.json"


# ── State helpers ─────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    return json.loads(STATE_FILE.read_text())


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
    print(f"  [state] written to {STATE_FILE}")


# ── Seed ──────────────────────────────────────────────────────────────────────

def do_seed() -> None:
    if STATE_FILE.exists():
        print("ERROR: Seed state file already exists. Run --cleanup first, then --seed again.")
        sys.exit(1)

    state: dict = {}

    with get_db_context() as db:
        # 1. Subscriber
        existing_sub = db.execute(
            text("SELECT id FROM subscribers WHERE email = :email"),
            {"email": SEED_EMAIL},
        ).first()

        if existing_sub:
            print(f"  [skip] subscriber already exists id={existing_sub.id}")
            state["subscriber_id"] = existing_sub.id
            state["subscriber_created"] = False
        else:
            now = datetime.now(timezone.utc)
            row = db.execute(text("""
                INSERT INTO subscribers (
                    stripe_customer_id, tier, vertical, county_id,
                    status, email, name,
                    churned_at, is_trial,
                    signup_source, icp_channel_key,
                    founding_member, has_saved_card, auto_mode_enabled,
                    created_at, updated_at
                ) VALUES (
                    :stripe_cid, 'starter', :vertical, :county,
                    'churned', :email, 'S0 Seed Subscriber',
                    :churned_at, false,
                    'direct', 'contractor',
                    false, false, false,
                    :now, :now
                )
                RETURNING id
            """), {
                "stripe_cid": SEED_STRIPE_CID,
                "vertical": SEED_VERTICAL,
                "county": SEED_COUNTY,
                "email": SEED_EMAIL,
                "churned_at": now - timedelta(days=90),
                "now": now,
            }).first()
            sub_id = row.id
            state["subscriber_id"] = sub_id
            state["subscriber_created"] = True
            print(f"  [seed] subscriber id={sub_id}")

        sub_id = state["subscriber_id"]

        # 2. ExpansionCandidate (county_id is UNIQUE — upsert/restore)
        existing_ec = db.execute(
            text("SELECT county_id, status, launched_at FROM expansion_candidates WHERE county_id = :county"),
            {"county": SEED_COUNTY},
        ).first()

        if existing_ec:
            original = {
                "status": existing_ec.status,
                "launched_at": existing_ec.launched_at.isoformat() if existing_ec.launched_at else None,
            }
            state["expansion_candidate_original"] = original
            state["expansion_candidate_created"] = False
            db.execute(text("""
                UPDATE expansion_candidates
                SET status = 'launched', launched_at = :now
                WHERE county_id = :county
            """), {"now": datetime.now(timezone.utc), "county": SEED_COUNTY})
            print(f"  [seed] expansion_candidate updated (was status={original['status']})")
        else:
            ec_now = datetime.now(timezone.utc)
            db.execute(text("""
                INSERT INTO expansion_candidates (county_id, status, launched_at, priority, created_at)
                VALUES (:county, 'launched', :now, 10, :now)
            """), {"county": SEED_COUNTY, "now": ec_now})
            state["expansion_candidate_created"] = True
            state["expansion_candidate_original"] = None
            print(f"  [seed] expansion_candidate inserted county={SEED_COUNTY}")

        # 3. Property (for sold_out supply chain)
        existing_prop = db.execute(
            text("SELECT id FROM properties WHERE parcel_id = :pid"),
            {"pid": SEED_PARCEL_ID},
        ).first()

        if existing_prop:
            state["property_id"] = existing_prop.id
            state["property_created"] = False
            print(f"  [skip] property already exists id={existing_prop.id}")
        else:
            now = datetime.now(timezone.utc)
            row = db.execute(text("""
                INSERT INTO properties (parcel_id, zip, county_id, created_at, updated_at)
                VALUES (:pid, :zip, :county, :now, :now)
                RETURNING id
            """), {
                "pid": SEED_PARCEL_ID,
                "zip": SEED_ZIP,
                "county": SEED_COUNTY,
                "now": now,
            }).first()
            state["property_id"] = row.id
            state["property_created"] = True
            print(f"  [seed] property id={row.id} parcel_id={SEED_PARCEL_ID}")

        property_id = state["property_id"]

        # 4. DistressScore (Gold-tier, today)
        existing_ds = db.execute(
            text("""
                SELECT id FROM distress_scores
                WHERE property_id = :pid AND score_date = :today
            """),
            {"pid": property_id, "today": date.today()},
        ).first()

        if existing_ds:
            state["distress_score_id"] = existing_ds.id
            state["distress_score_created"] = False
            print(f"  [skip] distress_score already exists id={existing_ds.id}")
        else:
            row = db.execute(text("""
                INSERT INTO distress_scores (property_id, score_date, lead_tier, county_id)
                VALUES (:pid, :today, 'Gold', :county)
                RETURNING id
            """), {
                "pid": property_id,
                "today": date.today(),
                "county": SEED_COUNTY,
            }).first()
            state["distress_score_id"] = row.id
            state["distress_score_created"] = True
            print(f"  [seed] distress_score id={row.id} tier=Gold")

        # 5. WaitlistEntry (links subscriber email to ZIP — geo_interest join path)
        existing_we = db.execute(
            text("""
                SELECT id FROM waitlist_entries
                WHERE zip_code = :zip AND county_id = :county
                  AND vertical = :vertical AND email = :email
            """),
            {"zip": SEED_ZIP, "county": SEED_COUNTY, "vertical": SEED_VERTICAL, "email": SEED_EMAIL},
        ).first()

        if existing_we:
            state["waitlist_entry_id"] = existing_we.id
            state["waitlist_entry_created"] = False
            print(f"  [skip] waitlist_entry already exists id={existing_we.id}")
        else:
            row = db.execute(text("""
                INSERT INTO waitlist_entries (zip_code, county_id, vertical, email, name, status)
                VALUES (:zip, :county, :vertical, :email, 'S0 Seed', 'waiting')
                RETURNING id
            """), {
                "zip": SEED_ZIP,
                "county": SEED_COUNTY,
                "vertical": SEED_VERTICAL,
                "email": SEED_EMAIL,
            }).first()
            state["waitlist_entry_id"] = row.id
            state["waitlist_entry_created"] = True
            print(f"  [seed] waitlist_entry id={row.id} zip={SEED_ZIP} vertical={SEED_VERTICAL}")

        # 6. GoldPlusZipSnapshot (triggers sold_out cohort)
        existing_snap = db.execute(
            text("""
                SELECT id FROM gold_plus_zip_snapshots
                WHERE zip_code = :zip AND county_id = :county AND snapshot_date = :today
            """),
            {"zip": SEED_ZIP, "county": SEED_COUNTY, "today": date.today()},
        ).first()

        if existing_snap:
            state["zip_snapshot_id"] = existing_snap.id
            state["zip_snapshot_created"] = False
            print(f"  [skip] zip_snapshot already exists id={existing_snap.id}")
        else:
            row = db.execute(text("""
                INSERT INTO gold_plus_zip_snapshots
                    (zip_code, county_id, snapshot_date, gold_plus_lead_count, computed_at)
                VALUES (:zip, :county, :today, 3, :now)
                RETURNING id
            """), {
                "zip": SEED_ZIP,
                "county": SEED_COUNTY,
                "today": date.today(),
                "now": datetime.now(timezone.utc),
            }).first()
            state["zip_snapshot_id"] = row.id
            state["zip_snapshot_created"] = True
            print(f"  [seed] zip_snapshot id={row.id} zip={SEED_ZIP} count=3")

        db.commit()

    state["seeded_at"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)
    print("\nSeed complete. Run --run-test to verify, --cleanup when done.")


# ── Run test ──────────────────────────────────────────────────────────────────

def do_run_test(live: bool = False) -> None:
    state = _load_state()
    if not state:
        print("ERROR: No seed state found. Run --seed first.")
        sys.exit(1)

    sub_id = state.get("subscriber_id")
    dry_run = not live

    print(f"\nRunning scheduler against seed data (dry_run={dry_run}) ...")
    print(f"  subscriber_id: {sub_id}")
    print(f"  county:        {SEED_COUNTY}")
    print(f"  zip_code:      {SEED_ZIP}")

    # Import here to avoid circular import during setup
    from src.tasks.reactivation_scheduler import run

    # County-Live cohort
    result_cl = run(
        cohort="county_live",
        dry_run=dry_run,
        county_id=SEED_COUNTY,
    )
    print(f"\n  [county_live] {result_cl}")

    # Sold-Out cohort
    result_so = run(
        cohort="sold_out",
        dry_run=dry_run,
        zip_code=SEED_ZIP,
    )
    print(f"  [sold_out]    {result_so}")

    # Validate
    ok = True
    for label, result in [("county_live", result_cl), ("sold_out", result_so)]:
        if result["checked"] == 0:
            print(f"\n  FAIL [{label}]: checked=0 — subscriber was not found by geo query")
            ok = False
        elif result["eligible"] == 0:
            print(f"\n  FAIL [{label}]: eligible=0 — subscriber found but failed eligibility gate")
            ok = False
        elif result["sent"] == 0 and not dry_run:
            print(f"\n  FAIL [{label}]: sent=0 — message was not dispatched")
            ok = False
        else:
            flag = "(dry-run, no actual send)" if dry_run else ""
            print(f"\n  PASS [{label}]: checked={result['checked']} eligible={result['eligible']} sent={result['sent']} {flag}")

    if ok:
        print("\nAll cohorts PASSED.")
    else:
        print("\nOne or more cohorts FAILED. Check logs above.")
        sys.exit(1)


# ── Cleanup ───────────────────────────────────────────────────────────────────

def do_cleanup() -> None:
    state = _load_state()
    if not state:
        print("No seed state found. Nothing to clean up.")
        return

    print(f"Cleaning up seed data from {state.get('seeded_at', 'unknown time')} ...")

    with get_db_context() as db:
        # Reverse dependency order

        # 6. GoldPlusZipSnapshot
        if state.get("zip_snapshot_created") and state.get("zip_snapshot_id"):
            db.execute(
                text("DELETE FROM gold_plus_zip_snapshots WHERE id = :id"),
                {"id": state["zip_snapshot_id"]},
            )
            print(f"  [delete] zip_snapshot id={state['zip_snapshot_id']}")

        # 5. WaitlistEntry
        if state.get("waitlist_entry_created") and state.get("waitlist_entry_id"):
            db.execute(
                text("DELETE FROM waitlist_entries WHERE id = :id"),
                {"id": state["waitlist_entry_id"]},
            )
            print(f"  [delete] waitlist_entry id={state['waitlist_entry_id']}")

        # 4. DistressScore
        if state.get("distress_score_created") and state.get("distress_score_id"):
            db.execute(
                text("DELETE FROM distress_scores WHERE id = :id"),
                {"id": state["distress_score_id"]},
            )
            print(f"  [delete] distress_score id={state['distress_score_id']}")

        # 3. Property
        if state.get("property_created") and state.get("property_id"):
            db.execute(
                text("DELETE FROM properties WHERE id = :id"),
                {"id": state["property_id"]},
            )
            print(f"  [delete] property id={state['property_id']}")

        # 2. ExpansionCandidate
        if state.get("expansion_candidate_created"):
            db.execute(
                text("DELETE FROM expansion_candidates WHERE county_id = :county"),
                {"county": SEED_COUNTY},
            )
            print(f"  [delete] expansion_candidate county={SEED_COUNTY}")
        elif state.get("expansion_candidate_original") is not None:
            original = state["expansion_candidate_original"]
            launched_at = (
                datetime.fromisoformat(original["launched_at"])
                if original.get("launched_at") else None
            )
            db.execute(text("""
                UPDATE expansion_candidates
                SET status = :status, launched_at = :launched_at
                WHERE county_id = :county
            """), {
                "status": original["status"],
                "launched_at": launched_at,
                "county": SEED_COUNTY,
            })
            print(f"  [restore] expansion_candidate county={SEED_COUNTY} → status={original['status']}")

        # 1. Subscriber — delete Cora-generated rows that FK to subscriber first
        if state.get("subscriber_created") and state.get("subscriber_id"):
            sid = state["subscriber_id"]
            db.execute(text("DELETE FROM message_outcomes WHERE subscriber_id = :id"), {"id": sid})
            db.execute(text("DELETE FROM agent_decisions WHERE subscriber_id = :id"), {"id": sid})
            db.execute(text("DELETE FROM subscribers WHERE id = :id"), {"id": sid})
            print(f"  [delete] subscriber id={sid} (+ message_outcomes + agent_decisions)")

        db.commit()

    STATE_FILE.unlink()
    print(f"\nCleanup complete. Removed {STATE_FILE.name}")


# ── Show ──────────────────────────────────────────────────────────────────────

def do_show() -> None:
    state = _load_state()
    if not state:
        print("No seed state file found.")
        return
    print(json.dumps(state, indent=2))


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="S0 reactivation E2E seed/cleanup")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--seed", action="store_true", help="Insert seed fixtures")
    group.add_argument("--run-test", action="store_true", help="Run scheduler against seed data")
    group.add_argument("--cleanup", action="store_true", help="Remove all seeded rows")
    group.add_argument("--show", action="store_true", help="Print current seed state")
    parser.add_argument(
        "--live",
        action="store_true",
        help="With --run-test: actually send email (default: dry-run only)",
    )
    args = parser.parse_args()

    if args.seed:
        do_seed()
    elif args.run_test:
        do_run_test(live=args.live)
    elif args.cleanup:
        do_cleanup()
    elif args.show:
        do_show()
