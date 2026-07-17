"""
Stripe Subscription Reconciliation Task
========================================
Runs daily to catch any subscribers who paid via Stripe but were never
activated in our DB — e.g. if all 87 Stripe webhook retries failed.

For each active Stripe subscription with no matching subscriber record,
this task re-fires the checkout.session.completed logic by fetching the
original checkout session and replaying it.

Usage:
    python -m src.tasks.stripe_reconcile
    python -m src.tasks.stripe_reconcile --dry-run
"""

import argparse
import logging
import sys

import stripe

from config.settings import settings
from src.core.database import get_db_context
from src.services.stripe_webhooks import _on_checkout_completed

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def _init_stripe() -> bool:
    try:
        secret = settings.active_stripe_secret_key
        if not secret:
            return False
        stripe.api_key = secret.get_secret_value()
        return True
    except Exception:
        return False


def reconcile_subscriptions(dry_run: bool = False) -> dict:
    """
    Compare active Stripe subscriptions against our subscribers table.
    Activate any that are missing.

    Returns:
        dict with keys: checked, already_active, activated, failed
    """
    if not _init_stripe():
        logger.error("Stripe not configured — cannot reconcile")
        return {"checked": 0, "already_active": 0, "activated": 0, "failed": 0}

    stats = {"checked": 0, "already_active": 0, "activated": 0, "failed": 0}

    # Fetch all active Stripe subscriptions (paginated)
    logger.info("Fetching active Stripe subscriptions...")
    subscriptions = []
    params = {"status": "active", "limit": 100, "expand": ["data.customer"]}
    while True:
        page = stripe.Subscription.list(**params)
        subscriptions.extend(page.data)
        if not page.has_more:
            break
        params["starting_after"] = page.data[-1].id

    logger.info("Found %d active Stripe subscriptions", len(subscriptions))

    with get_db_context() as db:
        from sqlalchemy import select
        from src.core.models import Subscriber

        for sub in subscriptions:
            stats["checked"] += 1
            customer_id = sub.customer.id if hasattr(sub.customer, "id") else sub.customer

            # Check if subscriber exists in our DB
            existing = db.execute(
                select(Subscriber).where(Subscriber.stripe_customer_id == customer_id)
            ).scalar_one_or_none()

            if existing and existing.status == "active":
                stats["already_active"] += 1
                logger.debug("OK: customer %s already active (subscriber_id=%s)", customer_id, existing.id)
                continue

            logger.warning(
                "GAP DETECTED: Stripe customer %s has active subscription %s but %s in our DB",
                customer_id, sub.id,
                f"status={existing.status}" if existing else "no record"
            )

            if dry_run:
                logger.info("[DRY RUN] Would activate customer %s", customer_id)
                stats["activated"] += 1
                continue

            # Find the original checkout session to replay metadata
            try:
                sessions = stripe.checkout.Session.list(
                    customer=customer_id,
                    limit=10,
                )
                checkout_session = None
                for s in sessions.data:
                    if s.subscription == sub.id and s.status == "complete":
                        checkout_session = s
                        break

                if not checkout_session:
                    logger.error(
                        "Could not find completed checkout session for customer %s / subscription %s",
                        customer_id, sub.id
                    )
                    stats["failed"] += 1
                    continue

                # Replay the checkout handler
                session_dict = checkout_session.to_dict_recursive() if hasattr(checkout_session, "to_dict_recursive") else dict(checkout_session)
                _on_checkout_completed(session_dict, db)
                db.commit()

                logger.info(
                    "RECONCILED: customer %s activated via checkout session %s",
                    customer_id, checkout_session.id
                )
                stats["activated"] += 1

            except stripe.error.StripeError as e:
                logger.error("Stripe error reconciling customer %s: %s", customer_id, e)
                stats["failed"] += 1
            except Exception as e:
                logger.error("Error reconciling customer %s: %s", customer_id, e, exc_info=True)
                db.rollback()
                stats["failed"] += 1

    logger.info(
        "Reconciliation complete — checked=%d already_active=%d activated=%d failed=%d",
        stats["checked"], stats["already_active"], stats["activated"], stats["failed"]
    )
    return stats


def _utc_day_window(as_of=None) -> tuple[int, int]:
    """UTC day boundaries as epoch seconds, for Stripe's `created` filter.

    Every same-day Stripe pull (fees, net revenue, bankruptcy invoices) must
    share these exact boundaries — computing `datetime.now()` separately in
    each one risks a few seconds' drift reclassifying a transaction into the
    wrong day between two calls in the same run.

    `as_of` accepts a `date` so callers/tests can pin a specific day instead
    of always using real "now".
    """
    from datetime import timezone as _tz, datetime as _dt
    day = as_of or _dt.now(_tz.utc).date()
    day_start = int(_dt(day.year, day.month, day.day, tzinfo=_tz.utc).timestamp())
    return day_start, day_start + 86400


def log_stripe_daily_fees(dry_run: bool = False) -> dict:
    """
    Fetch today's Stripe balance transactions, sum processing fees,
    and write a single ApiUsageLog row (service="stripe") for vendor cost reporting.

    Stripe is alert-only in v1 — no auto-pause is triggered regardless of spend.
    Returns: { "fee_usd": float, "transactions": int, "logged": bool }
    """
    if not _init_stripe():
        logger.warning("[StripeReconcile] Stripe not configured — skipping fee logging")
        return {"fee_usd": 0.0, "transactions": 0, "logged": False}

    day_start, day_end = _utc_day_window()

    total_fee_usd = 0.0
    tx_count = 0
    try:
        params = {
            "type": "charge",
            "created": {"gte": day_start, "lt": day_end},
            "limit": 100,
        }
        while True:
            page = stripe.BalanceTransaction.list(**params)
            for bt in page.data:
                total_fee_usd += bt.fee / 100.0
                tx_count += 1
            if not page.has_more:
                break
            params["starting_after"] = page.data[-1].id
    except stripe.error.StripeError as exc:
        logger.error("[StripeReconcile] Fee fetch failed: %s", exc)
        return {"fee_usd": 0.0, "transactions": 0, "logged": False}

    if dry_run:
        logger.info("[StripeReconcile][DRY RUN] Would log Stripe fee: $%.4f (%d tx)", total_fee_usd, tx_count)
        return {"fee_usd": total_fee_usd, "transactions": tx_count, "logged": False}

    if total_fee_usd > 0:
        with get_db_context() as db:
            from src.core.models import ApiUsageLog
            db.add(ApiUsageLog(
                service="stripe",
                task_type="daily_fee_summary",
                cost_usd=total_fee_usd,
                blocked_by_pause=False,
            ))
            db.commit()
        logger.info("[StripeReconcile] Logged Stripe daily fees: $%.4f (%d tx)", total_fee_usd, tx_count)

    return {"fee_usd": total_fee_usd, "transactions": tx_count, "logged": total_fee_usd > 0 and not dry_run}


def compute_stripe_net_revenue(as_of=None) -> dict:
    """Net Stripe settlement for the UTC day: sum of `type=charge` balance
    transactions PLUS sum of `type=refund` (already negative amounts) — NOT
    just gross charges.

    A same-day refund does NOT reduce a charge's own balance-transaction
    amount; Stripe posts it as a separate, negative `type=refund`
    transaction. platform_revenue_ledger nets out refunded rows via
    `refunded_at` whenever the `charge.refunded` webhook fires — which is
    event-driven, not necessarily on the same calendar day as the original
    charge. So both sides of the revenue_fulfillment_heartbeat comparison
    are "net effect processed today," not "gross of charges created today"
    — do not "simplify" this back to charges-only, that reintroduces a
    false mismatch on every day with a refund.

    Returns: { stripe_net_cents, charge_count, refund_count, configured,
    error }. `error` is None on success, or the exception string — a fetch
    failure must show as "could not verify," never as a false $0 day.
    """
    if not _init_stripe():
        return {
            "stripe_net_cents": 0, "charge_count": 0, "refund_count": 0,
            "configured": False, "error": None,
        }

    day_start, day_end = _utc_day_window(as_of)
    net_cents = 0
    charge_count = 0
    refund_count = 0
    try:
        for txn_type in ("charge", "refund"):
            params = {"type": txn_type, "created": {"gte": day_start, "lt": day_end}, "limit": 100}
            while True:
                page = stripe.BalanceTransaction.list(**params)
                for bt in page.data:
                    net_cents += bt.amount
                    if txn_type == "charge":
                        charge_count += 1
                    else:
                        refund_count += 1
                if not page.has_more:
                    break
                params["starting_after"] = page.data[-1].id
    except stripe.error.StripeError as exc:
        logger.error("[StripeReconcile] Net revenue fetch failed: %s", exc)
        return {
            "stripe_net_cents": 0, "charge_count": 0, "refund_count": 0,
            "configured": True, "error": str(exc),
        }

    return {
        "stripe_net_cents": net_cents, "charge_count": charge_count,
        "refund_count": refund_count, "configured": True, "error": None,
    }


def fetch_bankruptcy_alert_revenue(db, as_of=None) -> dict:
    """That day's paid Stripe invoice revenue for the bankruptcy-alert
    product (src/services/bankruptcy_alert/subscription.py).

    bankruptcy_alert_subscriptions is deliberately decoupled from
    `subscribers` (no FK — see that module's docstring), so this product's
    payments can't be joined into platform_revenue_ledger without a schema
    change to a table other systems (Task 6.1/6.2 margin reporting) already
    rely on. Pull directly from Stripe instead: one paginated Invoice.list
    for the day, filtered in-process against the locally-known set of this
    product's subscription ids — platform daily invoice volume is small
    enough that this is simpler than one Stripe call per subscription.

    Returns: { revenue_cents, invoice_count, configured, error }.
    """
    if not _init_stripe():
        return {"revenue_cents": 0, "invoice_count": 0, "configured": False, "error": None}

    from sqlalchemy import text as sa_text
    sub_ids = {
        row[0] for row in db.execute(sa_text(
            "SELECT stripe_subscription_id FROM bankruptcy_alert_subscriptions "
            "WHERE stripe_subscription_id IS NOT NULL"
        )).all()
    }
    if not sub_ids:
        return {"revenue_cents": 0, "invoice_count": 0, "configured": True, "error": None}

    day_start, day_end = _utc_day_window(as_of)
    revenue_cents = 0
    invoice_count = 0
    try:
        params = {"created": {"gte": day_start, "lt": day_end}, "status": "paid", "limit": 100}
        while True:
            page = stripe.Invoice.list(**params)
            for inv in page.data:
                if getattr(inv, "subscription", None) in sub_ids:
                    revenue_cents += getattr(inv, "amount_paid", None) or 0
                    invoice_count += 1
            if not page.has_more:
                break
            params["starting_after"] = page.data[-1].id
    except stripe.error.StripeError as exc:
        logger.error("[StripeReconcile] Bankruptcy-alert invoice fetch failed: %s", exc)
        return {"revenue_cents": 0, "invoice_count": 0, "configured": True, "error": str(exc)}

    return {
        "revenue_cents": revenue_cents, "invoice_count": invoice_count,
        "configured": True, "error": None,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile Stripe subscriptions with local DB")
    parser.add_argument("--dry-run", action="store_true", help="Check only, do not activate")
    parser.add_argument("--log-fees", action="store_true", help="Also log today's Stripe processing fees")
    args = parser.parse_args()

    result = reconcile_subscriptions(dry_run=args.dry_run)
    if args.log_fees:
        fee_result = log_stripe_daily_fees(dry_run=args.dry_run)
        print("Stripe fees:", fee_result)
    if result["failed"] > 0:
        sys.exit(1)
