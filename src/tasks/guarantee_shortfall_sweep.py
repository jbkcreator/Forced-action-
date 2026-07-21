"""Tiered volume guarantee shortfall sweep.

Each paid tier promises TIER_LEAD_QUOTAS[tier] delivered leads per ~30-day
cycle (config/guarantees.py). For every active subscriber whose most recent
cycle has fully elapsed, this sweep counts leads actually delivered to their
bridged CustomerAccount (src/services/lead_delivery.py) in that window and,
when short of quota, issues a Stripe balance credit prorated by
shortfall/quota against the subscriber's actual plan_price.

One guarantee_credits row per (subscriber, period_end). A cycle is claimed
with a 'pending' row (INSERT .. ON CONFLICT) before Stripe is called, and the
Stripe call itself carries a idempotency key deterministic on
(subscriber, period_end) — together these mean a crash or an overlapping
sweep run can retry a cycle without ever crediting Stripe twice for it.
'failed' and 'pending' rows are not terminal: _period_bounds re-selects them
on the next run instead of advancing past them, so a Stripe outage doesn't
permanently skip a customer's credit.
Cycles are calendar windows anchored to the subscriber's signup date or the
end of their last terminally-resolved cycle — independent of Stripe's own
billing-cycle timestamps, since customer_accounts.current_period_end is not
yet populated by any live webhook path (see src/services/revenue_engine.py).

evaluate_subscriber_guarantee() evaluates a single subscriber's next
outstanding cycle and is reused by /api/upgrade (src/api/main.py) to settle
any already-closed cycle on the outgoing tier before a plan switch — a
subscriber's current tier alone can't gate evaluation, since upgrading off a
guaranteed tier must not erase a guarantee period that already closed on it.

Run daily via cron so each subscriber's cycle is evaluated shortly after it
closes:
    0 9 * * * $PROJECT/scripts/cron/run.sh src.tasks.guarantee_shortfall_sweep

Supports --dry-run to preview without touching Stripe or the DB.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from config.guarantees import TIER_LEAD_QUOTAS, DELIVERY_TRACKING_START_UTC
from src.core.database import get_db_context
from src.services.email import send_email
from src.services.stripe_service import issue_guarantee_credit

logger = logging.getLogger(__name__)

CYCLE_DAYS = 30


def _as_utc(dt: datetime) -> datetime:
    """subscribers.created_at is a naive TIMESTAMP column (no tz), while
    guarantee_credits.period_end is TIMESTAMPTZ — normalize both to
    tz-aware UTC so they're safe to compare against datetime.now(timezone.utc)."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _period_bounds(
    db, subscriber_id: int, created_at: datetime, floor: datetime | None = None
) -> tuple[datetime, datetime]:
    """Next cycle for this subscriber to evaluate. An unresolved ('pending'
    or 'failed') cycle is retried in place — a crash or a Stripe outage must
    not be skipped by advancing past it. Otherwise starts where the last
    terminally-resolved cycle ended, or at signup if never evaluated.

    `floor` clamps the cycle start no earlier than the point delivery was
    actually tracked (max of the bridged customer_account creation and the
    deliveries-ledger go-live). Without it, the first cycle anchors to signup
    and retroactively bills a backlog of pre-tracking cycles that show 0
    delivered simply because the deliveries ledger didn't exist yet.

    The floor also filters the unresolved lookup: a legacy 'pending'/'failed'
    row whose period_start predates the floor is NOT handed back for retry —
    it would recount the same pre-tracking window as a false shortfall. Such
    rows are terminally resolved out of band by _skip_pretracking_unresolved."""
    unresolved = db.execute(text("""
        SELECT period_start, period_end FROM guarantee_credits
        WHERE subscriber_id = :sid AND status IN ('pending', 'failed')
          AND (:floor IS NULL OR period_start >= :floor)
        ORDER BY period_end ASC LIMIT 1
    """), {"sid": subscriber_id, "floor": floor}).first()
    if unresolved:
        return _as_utc(unresolved.period_start), _as_utc(unresolved.period_end)

    last_end = db.execute(text("""
        SELECT period_end FROM guarantee_credits
        WHERE subscriber_id = :sid AND status IN ('met', 'issued', 'skipped_no_charge_basis')
        ORDER BY period_end DESC LIMIT 1
    """), {"sid": subscriber_id}).scalar()
    start = _as_utc(last_end or created_at)
    if floor is not None:
        start = max(start, _as_utc(floor))
    return start, start + timedelta(days=CYCLE_DAYS)


def _skip_pretracking_unresolved(db, subscriber_id: int, floor: datetime) -> int:
    """Terminally resolve any unresolved ('pending'/'failed') guarantee cycle
    that starts before `floor`. These are legacy periods created before delivery
    tracking existed; retrying them recounts an all-zero window and issues a
    false credit. The row is kept (status -> 'skipped_no_charge_basis') so the
    audit history survives; it is never deleted, and Stripe is never called.
    Returns the number of rows resolved. No-op-safe under repeat runs."""
    result = db.execute(text("""
        UPDATE guarantee_credits
           SET status = 'skipped_no_charge_basis'
         WHERE subscriber_id = :sid
           AND status IN ('pending', 'failed')
           AND period_start < :floor
    """), {"sid": subscriber_id, "floor": floor})
    db.commit()
    resolved = result.rowcount or 0
    if resolved:
        logger.info(
            "[GuaranteeSweep] skipped %d pre-tracking unresolved cycle(s) for subscriber %s",
            resolved, subscriber_id,
        )
    return resolved


def _reserve_period(
    db, *, subscriber_id: int, period_start: datetime, period_end: datetime,
    tier: str, quota: int, delivered: int, shortfall: int, credit_cents: int, status: str,
) -> int | None:
    """Atomically claim (subscriber_id, period_end) with the given status
    before any Stripe call is made, committing immediately. Returns the row
    id this call claimed, or None if another (concurrent or prior) run
    already claimed/resolved it — the caller must skip in that case.

    A pre-existing 'pending' or 'failed' row for the same period is retried
    in place (its counts refreshed); a row already 'met', 'issued', or
    'skipped_no_charge_basis' is terminal and left untouched."""
    row = db.execute(text("""
        INSERT INTO guarantee_credits
            (subscriber_id, period_start, period_end, tier, quota, delivered,
             shortfall, credit_cents, status)
        VALUES
            (:sid, :start, :end, :tier, :quota, :delivered, :shortfall, :credit_cents, :status)
        ON CONFLICT (subscriber_id, period_end) DO UPDATE SET
            delivered = EXCLUDED.delivered,
            shortfall = EXCLUDED.shortfall,
            credit_cents = EXCLUDED.credit_cents,
            status = EXCLUDED.status
        WHERE guarantee_credits.status IN ('pending', 'failed')
        RETURNING id
    """), {
        "sid": subscriber_id, "start": period_start, "end": period_end, "tier": tier,
        "quota": quota, "delivered": delivered, "shortfall": shortfall,
        "credit_cents": credit_cents, "status": status,
    }).first()
    db.commit()
    return row.id if row else None


def _resolve_period(db, row_id: int, *, status: str, stripe_balance_txn_id: str | None) -> None:
    db.execute(text("""
        UPDATE guarantee_credits SET status = :status, stripe_balance_txn_id = :txn_id
        WHERE id = :id
    """), {"status": status, "txn_id": stripe_balance_txn_id, "id": row_id})
    db.commit()


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


def evaluate_subscriber_guarantee(db, sub, *, dry_run: bool = False) -> dict | None:
    """Evaluate one subscriber's next outstanding guarantee cycle, if it has
    closed. `sub` needs .id, .tier, .plan_price, .stripe_customer_id, .email,
    .name, .created_at — either a `subscribers` row or a Subscriber ORM
    instance both satisfy this.

    Returns None if there's nothing to evaluate yet (tier isn't guaranteed,
    or the next cycle hasn't closed). Otherwise returns
    {"shortfall": bool, "status": str|None} — status is None in dry_run mode.

    Reused by the daily sweep and by /api/upgrade, which calls this for the
    outgoing tier before switching so a plan change can't erase a guarantee
    cycle that already closed on it.
    """
    quota = TIER_LEAD_QUOTAS.get(sub.tier)
    if not quota:
        return None

    # The guarantee is measured against the deliveries ledger, which only
    # covers subscribers bridged to a customer_account. A subscriber with no
    # account has no measurable delivery history — skip rather than treat an
    # unmeasurable cycle as a 0-delivered shortfall. The account's creation
    # also floors the cycle start so pre-tracking history isn't back-credited.
    account_created_at = db.execute(text("""
        SELECT min(created_at) FROM customer_accounts WHERE subscriber_id = :sid
    """), {"sid": sub.id}).scalar()
    if account_created_at is None:
        return None

    # Floor at the later of account creation and the deliveries-ledger go-live:
    # a delivery can't predate either, so an earlier window would count false
    # zeros. Legacy unresolved cycles below the floor are terminally skipped
    # (audit kept) so they can't be retried into a false credit.
    floor = max(_as_utc(account_created_at), DELIVERY_TRACKING_START_UTC)
    if not dry_run:
        _skip_pretracking_unresolved(db, sub.id, floor)

    now = datetime.now(timezone.utc)
    start, end = _period_bounds(db, sub.id, sub.created_at, floor=floor)
    if end > now:
        return None  # cycle not finished yet

    delivered = _delivered_count(db, sub.id, start, end)
    shortfall = max(0, quota - delivered)

    if shortfall == 0:
        if not dry_run:
            _reserve_period(
                db, subscriber_id=sub.id, period_start=start, period_end=end,
                tier=sub.tier, quota=quota, delivered=delivered, shortfall=0,
                credit_cents=0, status="met",
            )
        return {"shortfall": False, "status": None if dry_run else "met"}

    plan_price_cents = int((sub.plan_price or 0) * 100)
    credit_cents = round(plan_price_cents * shortfall / quota) if plan_price_cents else 0

    if dry_run:
        logger.info(
            "[GuaranteeSweep] DRY RUN — subscriber %s (%s) delivered %d/%d leads, "
            "would credit $%.2f",
            sub.id, sub.tier, delivered, quota, credit_cents / 100,
        )
        return {"shortfall": True, "status": None}

    pending_status = "skipped_no_charge_basis" if (credit_cents <= 0 or not sub.stripe_customer_id) else "pending"
    row_id = _reserve_period(
        db, subscriber_id=sub.id, period_start=start, period_end=end,
        tier=sub.tier, quota=quota, delivered=delivered, shortfall=shortfall,
        credit_cents=credit_cents, status=pending_status,
    )
    if row_id is None:
        # Another run already claimed or resolved this cycle.
        return {"shortfall": True, "status": None}

    if pending_status == "skipped_no_charge_basis":
        return {"shortfall": True, "status": pending_status}

    idempotency_key = f"guarantee-credit-{sub.id}-{end.date().isoformat()}"
    try:
        txn_id = issue_guarantee_credit(
            sub.stripe_customer_id, credit_cents,
            description=f"Lead guarantee credit — {delivered}/{quota} delivered ({sub.tier})",
            idempotency_key=idempotency_key,
        )
    except Exception:
        logger.error("[GuaranteeSweep] credit failed for subscriber %s", sub.id, exc_info=True)
        _resolve_period(db, row_id, status="failed", stripe_balance_txn_id=None)
        return {"shortfall": True, "status": "failed"}

    _resolve_period(db, row_id, status="issued", stripe_balance_txn_id=txn_id)
    if sub.email:
        _send_guarantee_notice(sub, quota, delivered, credit_cents)
    return {"shortfall": True, "status": "issued"}


def run_guarantee_shortfall_sweep(db=None, *, dry_run: bool = False) -> dict:
    own = db is None
    ctx = get_db_context() if own else None
    db = ctx.__enter__() if own else db
    stats = {"checked": 0, "shortfall": 0, "credited": 0, "failed": 0, "dry_run": dry_run}
    try:
        tiers = list(TIER_LEAD_QUOTAS.keys())
        subs = db.execute(text("""
            SELECT id, tier, plan_price, stripe_customer_id, email, name, created_at
            FROM subscribers s
            WHERE status = 'active' AND tier = ANY(:tiers)
              AND EXISTS (
                SELECT 1 FROM customer_accounts ca WHERE ca.subscriber_id = s.id
              )
        """), {"tiers": tiers}).fetchall()

        for sub in subs:
            result = evaluate_subscriber_guarantee(db, sub, dry_run=dry_run)
            if result is None:
                continue
            stats["checked"] += 1
            if result["shortfall"]:
                stats["shortfall"] += 1
            if result["status"] == "issued":
                stats["credited"] += 1
            elif result["status"] == "failed":
                stats["failed"] += 1

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
