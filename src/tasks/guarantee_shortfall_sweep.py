"""Tiered volume guarantee shortfall sweep.

Each paid tier promises TIER_LEAD_QUOTAS[tier] delivered leads per ~30-day
cycle (config/guarantees.py). For every active subscriber whose most recent
cycle has fully elapsed, this sweep counts leads actually delivered to their
bridged CustomerAccount (src/services/lead_delivery.py) in that window and,
when short of quota, issues a Stripe balance credit prorated by
shortfall/quota against the subscriber's actual plan_price.

One guarantee_credits row per (subscriber, period_end) — the unique
constraint (migrations/apply_guarantee_credits.py) is the idempotency guard,
so re-running the sweep never double-evaluates or double-credits a cycle.
Cycles are calendar windows anchored to the subscriber's signup date or the
end of their last evaluated cycle — independent of Stripe's own billing-cycle
timestamps, since customer_accounts.current_period_end is not yet populated
by any live webhook path (see src/services/revenue_engine.py).

Run daily via cron so each subscriber's cycle is evaluated shortly after it
closes:
    0 9 * * * $PROJECT/scripts/cron/run.sh src.tasks.guarantee_shortfall_sweep

Supports --dry-run to preview without touching Stripe or the DB.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from config.guarantees import TIER_LEAD_QUOTAS
from src.core.database import get_db_context
from src.core.models import GuaranteeCredit
from src.services.email import send_email
from src.services.stripe_service import issue_guarantee_credit

logger = logging.getLogger(__name__)

CYCLE_DAYS = 30


def _as_utc(dt: datetime) -> datetime:
    """subscribers.created_at is a naive TIMESTAMP column (no tz), while
    guarantee_credits.period_end is TIMESTAMPTZ — normalize both to
    tz-aware UTC so they're safe to compare against datetime.now(timezone.utc)."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _period_bounds(db, subscriber_id: int, created_at: datetime) -> tuple[datetime, datetime]:
    """Next unevaluated cycle for this subscriber: starts where the last
    evaluated cycle ended, or at signup if never evaluated."""
    last_end = db.execute(text("""
        SELECT period_end FROM guarantee_credits
        WHERE subscriber_id = :sid ORDER BY period_end DESC LIMIT 1
    """), {"sid": subscriber_id}).scalar()
    start = _as_utc(last_end or created_at)
    return start, start + timedelta(days=CYCLE_DAYS)


def _delivered_count(db, subscriber_id: int, start: datetime, end: datetime) -> int:
    """Delivered (non-rejected) leads for this subscriber's bridged account
    in [start, end)."""
    return int(db.execute(text("""
        SELECT count(*) FROM deliveries d
        JOIN customer_accounts ca ON ca.account_id = d.account_id
        WHERE ca.subscriber_id = :sid
          AND d.status = 'delivered'
          AND d.delivered_at >= :start AND d.delivered_at < :end
    """), {"sid": subscriber_id, "start": start, "end": end}).scalar() or 0)


def _send_guarantee_notice(sub, quota: int, delivered: int, credit_cents: int) -> None:
    name = sub.name or "there"
    tier_label = (sub.tier or "").title()
    credit_str = f"${credit_cents / 100:,.2f}"
    try:
        send_email(
            to=sub.email,
            subject="Your Forced Action lead guarantee credit",
            body_text=(
                f"Hi {name},\n\n"
                f"Your {tier_label} plan guarantees {quota} delivered leads per cycle. "
                f"You received {delivered} this cycle, so we've credited {credit_str} "
                f"to your account — it will automatically reduce your next invoice.\n\n"
                f"— Forced Action Team"
            ),
        )
    except Exception:
        logger.error("[GuaranteeSweep] notice email failed for subscriber %s", sub.id, exc_info=True)


def run_guarantee_shortfall_sweep(db=None, *, dry_run: bool = False) -> dict:
    own = db is None
    ctx = get_db_context() if own else None
    db = ctx.__enter__() if own else db
    stats = {"checked": 0, "shortfall": 0, "credited": 0, "failed": 0, "dry_run": dry_run}
    try:
        now = datetime.now(timezone.utc)
        tiers = list(TIER_LEAD_QUOTAS.keys())
        subs = db.execute(text("""
            SELECT id, tier, plan_price, stripe_customer_id, email, name, created_at
            FROM subscribers
            WHERE status = 'active' AND tier = ANY(:tiers)
        """), {"tiers": tiers}).fetchall()

        for sub in subs:
            quota = TIER_LEAD_QUOTAS.get(sub.tier)
            if not quota:
                continue

            start, end = _period_bounds(db, sub.id, sub.created_at)
            if end > now:
                continue  # cycle not finished yet
            stats["checked"] += 1

            delivered = _delivered_count(db, sub.id, start, end)
            shortfall = max(0, quota - delivered)

            if shortfall == 0:
                if not dry_run:
                    db.add(GuaranteeCredit(
                        subscriber_id=sub.id, period_start=start, period_end=end,
                        tier=sub.tier, quota=quota, delivered=delivered,
                        shortfall=0, credit_cents=0, status="met",
                    ))
                    db.commit()
                continue

            stats["shortfall"] += 1
            plan_price_cents = int((sub.plan_price or 0) * 100)
            credit_cents = round(plan_price_cents * shortfall / quota) if plan_price_cents else 0

            if dry_run:
                logger.info(
                    "[GuaranteeSweep] DRY RUN — subscriber %s (%s) delivered %d/%d leads, "
                    "would credit $%.2f",
                    sub.id, sub.tier, delivered, quota, credit_cents / 100,
                )
                continue

            status = "issued"
            txn_id = None
            if credit_cents <= 0 or not sub.stripe_customer_id:
                status = "skipped_no_charge_basis"
            else:
                try:
                    txn_id = issue_guarantee_credit(
                        sub.stripe_customer_id, credit_cents,
                        description=f"Lead guarantee credit — {delivered}/{quota} delivered ({sub.tier})",
                    )
                except Exception:
                    logger.error(
                        "[GuaranteeSweep] credit failed for subscriber %s", sub.id, exc_info=True,
                    )
                    status = "failed"
                    stats["failed"] += 1

            db.add(GuaranteeCredit(
                subscriber_id=sub.id, period_start=start, period_end=end,
                tier=sub.tier, quota=quota, delivered=delivered, shortfall=shortfall,
                credit_cents=credit_cents, stripe_balance_txn_id=txn_id, status=status,
            ))
            db.commit()

            if status == "issued":
                stats["credited"] += 1
                if sub.email:
                    _send_guarantee_notice(sub, quota, delivered, credit_cents)

        logger.info(
            "[GuaranteeSweep] checked=%d shortfall=%d credited=%d failed=%d%s",
            stats["checked"], stats["shortfall"], stats["credited"], stats["failed"],
            " (DRY RUN)" if dry_run else "",
        )
        return stats
    finally:
        if own:
            ctx.__exit__(None, None, None)


def main() -> int:
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    parser = argparse.ArgumentParser(description="Evaluate tiered lead-volume guarantees")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    args = parser.parse_args()

    run_guarantee_shortfall_sweep(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
