"""
One-shot backfill for Referral Core Loop milestones.

Grants retroactive free_month_3 and lock_slot_5 milestones to referrers
who already have 3+ or 5+ confirmed referrals under the old 20-credit regime.

- Per-referral 5cr backfill is NOT performed.
  Historical referrers were paid 20cr each; no re-credit or clawback.
- notified_at is set immediately on inserted rows to suppress SMS.

Run once in production, review the printed summary.

Usage:
    python scripts/backfill_referral_milestones.py
    python scripts/backfill_referral_milestones.py --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import select

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run(dry_run: bool = False) -> None:
    from src.core.database import Database
    from src.core.models import ReferralEvent, ReferralMilestoneAward, Subscriber
    from src.services.milestone_grants import grant_free_month, grant_lock_slot

    db_factory = Database()
    processed = free_month_granted = lock_slot_granted = 0
    stripe_failures: list[str] = []

    with db_factory.session_scope() as db:
        # All subscribers who have ever referred someone
        referrers = db.execute(
            select(Subscriber).where(Subscriber.referral_code.is_not(None))
        ).scalars().all()

        logger.info("Scanning %d referrers...", len(referrers))

        for sub in referrers:
            processed += 1
            events = db.execute(
                select(ReferralEvent).where(
                    ReferralEvent.referrer_subscriber_id == sub.id,
                    ReferralEvent.status.in_(("confirmed", "rewarded")),
                )
            ).scalars().all()
            n = len(events)
            if n == 0:
                continue

            triggering_event_id = events[0].id  # arbitrary — backfill context

            if n >= 3:
                existing_fm = db.execute(
                    select(ReferralMilestoneAward).where(
                        ReferralMilestoneAward.referrer_subscriber_id == sub.id,
                        ReferralMilestoneAward.milestone == "free_month_3",
                    )
                ).scalar_one_or_none()
                if not existing_fm:
                    if dry_run:
                        logger.info("[DRY-RUN] would grant free_month_3 to subscriber=%d (n=%d)", sub.id, n)
                    else:
                        try:
                            award = grant_free_month(sub.id, triggering_event_id, db)
                            # Suppress SMS — backfill is silent
                            award.notified_at = datetime.now(timezone.utc)
                            db.flush()
                            free_month_granted += 1
                            logger.info("Granted free_month_3 to subscriber=%d (n=%d) grant_ref=%s",
                                        sub.id, n, award.grant_ref)
                        except Exception as exc:
                            msg = f"subscriber={sub.id}: {exc}"
                            stripe_failures.append(msg)
                            logger.error("free_month_3 grant FAILED for %s", msg)

            if n >= 5:
                existing_ls = db.execute(
                    select(ReferralMilestoneAward).where(
                        ReferralMilestoneAward.referrer_subscriber_id == sub.id,
                        ReferralMilestoneAward.milestone == "lock_slot_5",
                    )
                ).scalar_one_or_none()
                if not existing_ls:
                    if dry_run:
                        logger.info("[DRY-RUN] would grant lock_slot_5 to subscriber=%d (n=%d)", sub.id, n)
                    else:
                        try:
                            award = grant_lock_slot(sub.id, triggering_event_id, db)
                            award.notified_at = datetime.now(timezone.utc)
                            db.flush()
                            lock_slot_granted += 1
                            logger.info("Granted lock_slot_5 to subscriber=%d (n=%d)", sub.id, n)
                        except Exception as exc:
                            msg = f"subscriber={sub.id}: {exc}"
                            stripe_failures.append(msg)
                            logger.error("lock_slot_5 grant FAILED for %s", msg)

    print("\n─── Backfill Summary ───────────────────────────────")
    print(f"  Referrers scanned : {processed}")
    print(f"  free_month_3 granted : {free_month_granted}")
    print(f"  lock_slot_5 granted  : {lock_slot_granted}")
    if stripe_failures:
        print(f"\n  ⚠ Stripe failures ({len(stripe_failures)}):")
        for f in stripe_failures:
            print(f"    - {f}")
    else:
        print("  No Stripe failures.")
    if dry_run:
        print("\n  [DRY-RUN] No changes written to DB.")
    print("────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill referral milestones")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; no DB writes")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
