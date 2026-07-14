"""Read-only audit of guarantee_credits — run once before/after deploying the
guarantee-sweep race-condition fix (src/tasks/guarantee_shortfall_sweep.py).

Prints:
  - count by status
  - subscribers with more than one 'issued' row for the same period_end
    (the duplicate-credit bug: same cycle credited to Stripe twice)
  - subscribers with more than one stripe_balance_txn_id for the same
    period_end (confirms two distinct Stripe transactions, not just two rows)
  - 'failed' rows older than 2 cycles (~60 days) that the old sweep would
    have permanently skipped instead of retried
  - subscribers currently on a non-guaranteed tier (upgraded away from
    starter/pro/dominator) with no guarantee_credits row covering their
    last active period on the old tier — the missed-period-on-upgrade bug

Usage:
    PYTHONPATH=. .venv/Scripts/python.exe scripts/audit_guarantee_credits.py
"""
from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config.settings import get_settings
from src.utils.logger import setup_logging

setup_logging()


def run_audit() -> None:
    settings = get_settings()
    engine = create_engine(str(settings.database_url))
    Session = sessionmaker(bind=engine)

    with Session() as session:
        print("\n" + "=" * 60)
        print("guarantee_credits — Duplicate / Missed Credit Audit")
        print("=" * 60)

        total = session.execute(text("SELECT COUNT(*) FROM guarantee_credits")).scalar()
        print(f"\nTotal rows: {total}")
        if not total:
            print("\nTable is empty — nothing to audit.\n")
            return

        # ── Count by status ───────────────────────────────────────
        print("\n--- Count by status ---")
        for r in session.execute(text("""
            SELECT status, COUNT(*) AS cnt FROM guarantee_credits
            GROUP BY status ORDER BY status
        """)).fetchall():
            print(f"  {r.status:<25} {r.cnt:>6}")

        # ── Duplicate 'issued' rows for the same (subscriber, period_end) ──
        # Should be impossible given the unique constraint, but the pre-fix
        # code could still double-call Stripe for a period whose row never
        # committed (crash) or lost the DB uniqueness race (concurrent run) —
        # this only catches the DB-row half of that; see the txn_id check
        # below for the Stripe-side signal.
        print("\n--- Subscribers with >1 guarantee_credits row for the same period_end ---")
        dupe_rows = session.execute(text("""
            SELECT subscriber_id, period_end, COUNT(*) AS cnt
            FROM guarantee_credits
            GROUP BY subscriber_id, period_end
            HAVING COUNT(*) > 1
            ORDER BY subscriber_id, period_end
        """)).fetchall()
        if dupe_rows:
            for r in dupe_rows:
                print(f"  subscriber_id={r.subscriber_id}  period_end={r.period_end}  rows={r.cnt}")
        else:
            print("  none found")

        # ── Distinct Stripe balance transactions for the same period ──
        # This is the real signal for "did we actually double-credit Stripe":
        # >1 distinct non-null stripe_balance_txn_id for one (subscriber,
        # period_end) means two separate Stripe balance transactions were
        # created for what should be one guarantee cycle.
        print("\n--- Subscribers credited via >1 distinct Stripe txn for the same period_end ---")
        dupe_txns = session.execute(text("""
            SELECT subscriber_id, period_end,
                   COUNT(DISTINCT stripe_balance_txn_id) AS distinct_txns,
                   array_agg(DISTINCT stripe_balance_txn_id) AS txn_ids
            FROM guarantee_credits
            WHERE stripe_balance_txn_id IS NOT NULL
            GROUP BY subscriber_id, period_end
            HAVING COUNT(DISTINCT stripe_balance_txn_id) > 1
            ORDER BY subscriber_id, period_end
        """)).fetchall()
        if dupe_txns:
            for r in dupe_txns:
                print(
                    f"  subscriber_id={r.subscriber_id}  period_end={r.period_end}  "
                    f"txns={r.distinct_txns}  ids={r.txn_ids}"
                )
            print(
                f"\n  ACTION: {len(dupe_txns)} period(s) above were credited to Stripe more than "
                "once — reconcile manually (reverse the extra balance transaction or note it "
                "as a customer-favorable adjustment)."
            )
        else:
            print("  none found — no evidence of an actual duplicate Stripe credit")

        # ── Stale 'failed' rows the old sweep would have permanently skipped ──
        print("\n--- 'failed' rows older than 60 days (pre-fix, these were never retried) ---")
        stale_failed = session.execute(text("""
            SELECT subscriber_id, period_end, credit_cents, created_at
            FROM guarantee_credits
            WHERE status = 'failed' AND period_end < now() - interval '60 days'
            ORDER BY period_end
        """)).fetchall()
        if stale_failed:
            for r in stale_failed:
                print(
                    f"  subscriber_id={r.subscriber_id}  period_end={r.period_end}  "
                    f"credit_cents={r.credit_cents}  first_attempted={r.created_at}"
                )
            print(
                f"\n  These {len(stale_failed)} row(s) will be auto-retried by the fixed sweep "
                "on its next run (status IN ('pending','failed') is no longer treated as terminal)."
            )
        else:
            print("  none found")

        # ── Subscribers who upgraded off a guaranteed tier with a gap ──
        # Only the /api/upgrade target tiers can be reached FROM a guaranteed
        # tier (starter/pro/dominator) — a subscriber sitting on one of these
        # today, with zero guarantee_credits rows, either upgraded before
        # their first cycle ever closed (fine) or upgraded after a cycle
        # closed that the pre-fix code silently dropped (the bug). Signup
        # age > 1 cycle is a rough filter for "had time to close a cycle
        # before upgrading" — still needs manual delivery-history review to
        # confirm, since there's no stored record of which tier was active
        # during a specific past period.
        print("\n--- Post-upgrade subscribers with no guarantee_credits history (candidates) ---")
        upgrade_target_tiers = ["autopilot_lite", "autopilot_pro", "data_only", "partner"]
        candidates = session.execute(text("""
            SELECT s.id, s.tier, s.created_at, s.status
            FROM subscribers s
            LEFT JOIN guarantee_credits gc ON gc.subscriber_id = s.id
            WHERE s.tier = ANY(:tiers)
              AND gc.id IS NULL
              AND s.created_at < now() - interval '30 days'
            ORDER BY s.id
        """), {"tiers": upgrade_target_tiers}).fetchall()
        if candidates:
            for r in candidates:
                print(
                    f"  subscriber_id={r.id}  current_tier={r.tier}  status={r.status}  "
                    f"signed_up={r.created_at}"
                )
            print(
                f"\n  {len(candidates)} subscriber(s) above are on an upgrade-only tier, signed up "
                "over 30 days ago, and have zero guarantee_credits rows — check delivery history "
                "for each around their signup-to-upgrade window to see if a shortfall cycle closed "
                "before they upgraded."
            )
        else:
            print("  none found")

        print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    run_audit()
