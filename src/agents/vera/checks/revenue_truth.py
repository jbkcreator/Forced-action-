"""
Vera — revenue truth report (VERA-v2.2 sub-task V3).

Runs once daily via `python -m src.agents.vera --revenue-truth`. Answers the
constitution's standing job #2 by asking Stripe directly, never by trusting
the platform's own database or its own webhook log about itself:

    1. Two-way subscriber reconciliation (paying-no-access AND
       access-not-paying)                        -> check_subscriber_reconciliation()
    2. Real MRR — Stripe vs DB, plus "new MRR
       added yesterday" (THE ONE NUMBER)          -> check_mrr()
    3. New/failed payments today, split by
       subscription vs one-time                   -> check_payment_activity()
    4. Refunds & disputes today, every product     -> check_refunds_and_disputes()

Every result is written as a dated row into vera_facts via
src.agents.vera.facts.write_fact(), then rendered into one plain-text report
and emailed to REPORT_RECIPIENTS. See
tasks/New-agent-lane/VERA-V3-Implementation-Plan.md for the full design,
including the two locked decisions this module depends on: the access
record checked is Subscriber (not the parallel CustomerAccount/S1
structure), and every payment/refund/dispute number is pulled live from
Stripe rather than from the webhook_events audit table (confirmed unreliable
for this job — see the plan's locked decision #2).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Mapping, Optional

import stripe
from sqlalchemy import text

from src.agents.vera.checks._shared import report_recipients
from src.agents.vera.config import FRESHNESS_REVENUE_24H, KILL_SWITCH_FEATURE
from src.agents.vera.db import vera_db
from src.agents.vera.facts import read_facts, write_fact

logger = logging.getLogger(__name__)

# Capped so a fact's sample-IDs field can't grow unbounded if the mismatch
# count were ever large. At current scale (~142 total subscribers, all test
# accounts) this is headroom, not a real constraint.
SAMPLE_CAP = 20


# ─────────────────────────────────────────────────────────────────────────────
# Stripe plumbing — same pattern as stripe_reconcile.py / stripe_webhooks.py /
# stripe_service.py, each of which keeps its own tiny copy rather than sharing
# one. V3 follows suit rather than importing from stripe_reconcile.py, which
# stays untouched per the dev-split doc's explicit instruction.
# ─────────────────────────────────────────────────────────────────────────────

def _init_stripe() -> bool:
    from config.settings import get_settings
    try:
        settings = get_settings()
        secret = settings.active_stripe_secret_key
        if not secret:
            return False
        stripe.api_key = secret.get_secret_value()
        return True
    except Exception:
        return False


def _paginate(list_fn, **params) -> Optional[list]:
    """Generic Stripe list-pagination helper (starting_after cursor — same
    shape as stripe_reconcile.py:56-65, written once here since no shared
    helper exists in stripe_service.py).

    Returns None (never an empty list) on failure — a Stripe outage and a
    genuinely empty result must never look the same to a caller. Confirmed
    the alternative is dangerous: if a failed pull returned [] like a real
    empty result, check_subscriber_reconciliation() would read "zero active
    Stripe subscriptions" and flag every real active DB subscriber as
    access-not-paying — a false-positive storm on every Stripe outage,
    exactly the kind of unverified assertion Vera's ACCURACY value forbids.
    """
    items: list = []
    params = dict(params)
    params.setdefault("limit", 100)
    try:
        while True:
            page = list_fn(**params)
            items.extend(page.data)
            if not page.has_more:
                break
            params["starting_after"] = page.data[-1].id
    except Exception as exc:
        logger.warning("[Vera] Stripe pagination failed after %d item(s): %s", len(items), exc)
        return None
    return items


def _day_window_utc(as_of: Optional[date] = None) -> tuple[int, int]:
    """UTC day boundaries as Unix timestamps, for Stripe's created={gte,lt} filter."""
    day = as_of or datetime.now(timezone.utc).date()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


def _field(obj, key: str, default=None):
    """Safely read a field from either a real Stripe response object or a
    plain dict (tests use plain dicts).

    Confirmed against the installed stripe-python 15.1.0: its response
    objects do NOT implement .get() at all — hasattr(sub, 'get') is False,
    and calling .get() raises AttributeError (it gets routed through
    __getattr__ looking for a field literally named "get", which doesn't
    exist). Only bracket access (obj[key]) works, same as it does on a
    plain dict — so this helper uses that uniformly instead of .get(),
    which crashed check_mrr() outright the first time this ran against
    real Stripe data (see VERA-V3-Implementation-Plan.md validation notes).
    Never call .get() directly on anything that might be a real Stripe
    object in this module — use this helper."""
    try:
        value = obj[key]
        return default if value is None else value
    except (KeyError, TypeError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# 3A — Two-way subscriber reconciliation
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReconciliationResult:
    paying_no_access_count: int
    paying_no_access_sample_ids: list
    access_not_paying_count: int
    access_not_paying_sample_ids: list
    access_not_paying_details: list
    active_subscriptions_by_customer: Optional[dict] = field(default_factory=dict)
    stripe_ok: bool = True


def _fetch_active_stripe_subscriptions() -> Optional[dict]:
    """One paginated pull — {stripe_customer_id: subscription}. Reused by both
    reconciliation directions here and by Stripe-side MRR (check_mrr) — one
    Stripe call, not two.

    Returns None (not {}) if Stripe isn't configured or the pull failed —
    see _paginate()'s docstring for why that distinction matters."""
    if not _init_stripe():
        logger.error("[Vera] Stripe not configured — cannot check revenue truth")
        return None
    subs = _paginate(stripe.Subscription.list, status="active", expand=["data.customer"])
    if subs is None:
        return None
    result: dict = {}
    for sub in subs:
        customer = sub["customer"]
        customer_id = customer.id if hasattr(customer, "id") else customer
        result[customer_id] = sub
    return result


def _classify_paying_no_access(active_subs_by_customer: dict, subscriber_by_customer: dict) -> list:
    """Pure function. For every Stripe-active customer, flag if no Subscriber
    row exists or the row's status isn't 'active' — this deliberately
    INCLUDES 'grace': if Stripe still says the subscription is live but
    we've already marked the subscriber grace/churned, that's a real
    mismatch (we may be about to release their ZIP while they're still
    actually paying)."""
    flagged = []
    for customer_id in active_subs_by_customer:
        row = subscriber_by_customer.get(customer_id)
        if row is None or row.get("status") != "active":
            flagged.append(customer_id)
    return flagged


def _classify_access_not_paying(active_subs_by_customer: dict, active_db_subscribers: list) -> list:
    """Pure function. `active_db_subscribers` must already be filtered to
    status == 'active' by the caller — 'grace' is deliberately EXCLUDED here:
    a subscriber in the 48h post-cancellation grace window is expected to no
    longer show as Stripe-active, so flagging it would be a guaranteed false
    positive every single day."""
    flagged = []
    for row in active_db_subscribers:
        customer_id = row.get("stripe_customer_id")
        subscription_id = row.get("stripe_subscription_id")
        if not subscription_id:
            flagged.append({"customer_id": customer_id, "reason": "no_stripe_subscription_id"})
        elif customer_id not in active_subs_by_customer:
            flagged.append({"customer_id": customer_id, "reason": "not_active_in_stripe"})
    return flagged


def _retrieve_subscription_status(subscription_id: str) -> str:
    """Only called for actual access-not-paying mismatches, not every active
    row — cheap because it fires on discrepancies, not the whole table."""
    try:
        sub = stripe.Subscription.retrieve(subscription_id)
        return _field(sub, "status", "unknown")
    except Exception as exc:
        logger.warning("[Vera] could not retrieve Stripe subscription %s: %s", subscription_id, exc)
        return "lookup_failed"


def check_subscriber_reconciliation() -> ReconciliationResult:
    active_subs_by_customer = _fetch_active_stripe_subscriptions()

    with vera_db.session_scope() as session:
        rows = session.execute(
            text("SELECT stripe_customer_id, stripe_subscription_id, status FROM subscribers")
        ).mappings().all()

    if active_subs_by_customer is None:
        # Stripe couldn't be reached — abstain rather than report every real
        # active DB subscriber as a false "access but not paying" mismatch.
        logger.error(
            "[Vera] Stripe subscription pull failed — abstaining from reconciliation, "
            "not reporting a result"
        )
        return ReconciliationResult(
            paying_no_access_count=0, paying_no_access_sample_ids=[],
            access_not_paying_count=0, access_not_paying_sample_ids=[],
            access_not_paying_details=[], active_subscriptions_by_customer=None,
            stripe_ok=False,
        )

    subscriber_by_customer = {row["stripe_customer_id"]: dict(row) for row in rows}
    active_db_subscribers = [dict(row) for row in rows if row["status"] == "active"]

    paying_no_access = _classify_paying_no_access(active_subs_by_customer, subscriber_by_customer)
    access_not_paying = _classify_access_not_paying(active_subs_by_customer, active_db_subscribers)

    details = []
    for item in access_not_paying[:SAMPLE_CAP]:
        detail = dict(item)
        if item["reason"] == "not_active_in_stripe":
            row = subscriber_by_customer.get(item["customer_id"], {})
            sub_id = row.get("stripe_subscription_id")
            detail["stripe_status"] = _retrieve_subscription_status(sub_id) if sub_id else "unknown"
        else:
            detail["stripe_status"] = "no_subscription_id_on_record"
        details.append(detail)

    return ReconciliationResult(
        paying_no_access_count=len(paying_no_access),
        paying_no_access_sample_ids=paying_no_access[:SAMPLE_CAP],
        access_not_paying_count=len(access_not_paying),
        access_not_paying_sample_ids=[d["customer_id"] for d in access_not_paying[:SAMPLE_CAP]],
        access_not_paying_details=details,
        active_subscriptions_by_customer=active_subs_by_customer,
    )


def _write_reconciliation_facts(result: ReconciliationResult) -> None:
    write_fact(
        "revenue.reconciliation.paying_no_access.count", str(result.paying_no_access_count),
        value_numeric=Decimal(result.paying_no_access_count),
        source="stripe",
        method="stripe.Subscription.list(status=active) cross-referenced against Subscriber.status",
        freshness_class=FRESHNESS_REVENUE_24H,
    )
    write_fact(
        "revenue.reconciliation.paying_no_access.sample_ids",
        ",".join(result.paying_no_access_sample_ids) or "none",
        source="stripe",
        method="stripe.Subscription.list(status=active) cross-referenced against Subscriber.status",
        freshness_class=FRESHNESS_REVENUE_24H,
    )
    write_fact(
        "revenue.reconciliation.access_not_paying.count", str(result.access_not_paying_count),
        value_numeric=Decimal(result.access_not_paying_count),
        source="subscribers",
        method="Subscriber.status=active cross-referenced against stripe.Subscription.list(status=active)",
        freshness_class=FRESHNESS_REVENUE_24H,
    )
    write_fact(
        "revenue.reconciliation.access_not_paying.sample_ids",
        ",".join(result.access_not_paying_sample_ids) or "none",
        source="subscribers",
        method="Subscriber.status=active cross-referenced against stripe.Subscription.list(status=active)",
        freshness_class=FRESHNESS_REVENUE_24H,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3B — Real MRR (Stripe vs DB) + "new MRR added yesterday"
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MrrResult:
    db_total_cents: int
    active_null_plan_price_count: int
    stripe_total_cents: int
    drift_cents: int
    stripe_ok: bool = True


def _db_mrr(session) -> tuple:
    """(db_total_cents, active_null_plan_price_count). Matches
    daily_dashboard.py's exact formula: SUM(plan_price) WHERE status='active'
    AND plan_price IS NOT NULL — so V3's number matches what's already on the
    operator dashboard, not a competing definition."""
    total = session.execute(
        text("SELECT SUM(plan_price) FROM subscribers WHERE status='active' AND plan_price IS NOT NULL")
    ).scalar()
    null_count = session.execute(
        text("SELECT COUNT(*) FROM subscribers WHERE status='active' AND plan_price IS NULL")
    ).scalar()
    total_cents = int(round((total or Decimal("0")) * 100))
    return total_cents, int(null_count or 0)


def _stripe_mrr(active_subs_by_customer: dict) -> int:
    """Sum normalized_monthly_price() across each active subscription's first
    line item — same field-access pattern as price_escalation.py:128."""
    from src.services.stripe_webhooks import normalized_monthly_price

    total_cents = 0
    for sub in active_subs_by_customer.values():
        try:
            item = sub["items"]["data"][0]
            unit_amount = item["price"]["unit_amount"]
            quantity = _field(item, "quantity", 1) or 1
            interval = item["price"]["recurring"]["interval"]
        except (KeyError, IndexError, TypeError):
            continue
        monthly = normalized_monthly_price(unit_amount * quantity, interval)
        total_cents += int(round(monthly * 100))
    return total_cents


def check_mrr(active_subs_by_customer: Optional[dict]) -> MrrResult:
    """`active_subs_by_customer` is `None` when check_subscriber_reconciliation()
    couldn't reach Stripe (not `{}` — see _fetch_active_stripe_subscriptions()).
    In that case the DB-side number is still real and reported, but the
    Stripe-side number is unknown — never fabricated as $0, which would read
    as a false drift/churn signal."""
    with vera_db.session_scope() as session:
        db_total_cents, null_count = _db_mrr(session)

    if active_subs_by_customer is None:
        return MrrResult(
            db_total_cents=db_total_cents, active_null_plan_price_count=null_count,
            stripe_total_cents=0, drift_cents=0, stripe_ok=False,
        )

    stripe_total_cents = _stripe_mrr(active_subs_by_customer)
    return MrrResult(
        db_total_cents=db_total_cents,
        active_null_plan_price_count=null_count,
        stripe_total_cents=stripe_total_cents,
        drift_cents=stripe_total_cents - db_total_cents,
        stripe_ok=True,
    )


def _select_prior_day_row(rows: list, today: date) -> Optional[Mapping]:
    """Pure function. Given recent revenue.mrr.stripe_total rows (newest
    first, from read_facts(fresh_only=False)), returns the first one that is
    BOTH from a prior calendar day AND carries a real numeric value —
    skipping two kinds of row that must never be mistaken for a valid prior
    value:
      - today's own row (guards a same-day retry — cron misfire, manual
        re-run — from comparing today against itself instead of yesterday);
      - a Stripe-outage placeholder row with no value_numeric (written by
        _write_mrr_facts() on a day Stripe couldn't be reached) — walks
        further back rather than treating a missing number as zero."""
    for row in rows:
        observed_at = row.get("observed_at")
        if observed_at is None:
            continue
        row_date = observed_at.date() if hasattr(observed_at, "date") else observed_at
        if row_date >= today:
            continue
        if row.get("value_numeric") is None:
            continue
        return row
    return None


def _compute_mrr_delta(today_stripe_total_cents: int, prior_row: Optional[Mapping]) -> Optional[int]:
    """Pure function. `prior_row` must be the prior CALENDAR DAY's fact (see
    _select_prior_day_row) — not merely the most recent one, which could be
    today's own row on a retry. Returns None when there's no prior day to
    compare (first-ever run, or the prior row carries no numeric value),
    never a fabricated zero."""
    if prior_row is None:
        return None
    yesterday_cents = prior_row.get("value_numeric")
    if yesterday_cents is None:
        return None
    try:
        return today_stripe_total_cents - int(yesterday_cents)
    except (TypeError, ValueError):
        return None


def _write_mrr_facts(mrr: MrrResult, new_yesterday_cents: Optional[int]) -> None:
    # DB-side numbers are always real, regardless of Stripe reachability.
    write_fact(
        "revenue.mrr.db_total", str(mrr.db_total_cents),
        value_numeric=Decimal(mrr.db_total_cents), source="subscribers",
        method="SUM(plan_price) WHERE status='active' AND plan_price IS NOT NULL",
        freshness_class=FRESHNESS_REVENUE_24H,
    )
    write_fact(
        "revenue.mrr.active_null_plan_price_count", str(mrr.active_null_plan_price_count),
        value_numeric=Decimal(mrr.active_null_plan_price_count), source="subscribers",
        method="COUNT(*) WHERE status='active' AND plan_price IS NULL",
        freshness_class=FRESHNESS_REVENUE_24H,
    )

    if not mrr.stripe_ok:
        # No value_numeric on either fact — a future run's _select_prior_day_row
        # skips these and keeps searching further back for a real prior value,
        # rather than treating today's outage as "MRR went to zero."
        write_fact(
            "revenue.mrr.stripe_total", "stripe unreachable", source="stripe",
            method="stripe.Subscription.list(status=active) — pull failed",
            freshness_class=FRESHNESS_REVENUE_24H,
        )
        write_fact(
            "revenue.mrr.new_yesterday", "stripe unreachable today", source="stripe",
            method="today's revenue.mrr.stripe_total minus yesterday's",
            freshness_class=FRESHNESS_REVENUE_24H,
        )
        return

    write_fact(
        "revenue.mrr.stripe_total", str(mrr.stripe_total_cents),
        value_numeric=Decimal(mrr.stripe_total_cents), source="stripe",
        method="sum of normalized_monthly_price() across active subscriptions",
        freshness_class=FRESHNESS_REVENUE_24H,
    )
    write_fact(
        "revenue.mrr.drift_cents", str(mrr.drift_cents),
        value_numeric=Decimal(mrr.drift_cents), source="stripe",
        method="stripe_total - db_total", freshness_class=FRESHNESS_REVENUE_24H,
    )
    if new_yesterday_cents is not None:
        write_fact(
            "revenue.mrr.new_yesterday", str(new_yesterday_cents),
            value_numeric=Decimal(new_yesterday_cents), source="stripe",
            method="today's revenue.mrr.stripe_total minus yesterday's",
            freshness_class=FRESHNESS_REVENUE_24H,
        )
    else:
        write_fact(
            "revenue.mrr.new_yesterday", "no prior day to compare",
            source="stripe", method="today's revenue.mrr.stripe_total minus yesterday's",
            freshness_class=FRESHNESS_REVENUE_24H,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 3C — New/failed payments today, split by subscription vs one-time
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PaymentActivityResult:
    new_count: int = 0
    new_amount_cents: int = 0
    failed_count: int = 0
    failed_amount_cents: int = 0
    subscription_count: int = 0
    subscription_amount_cents: int = 0
    one_time_count: int = 0
    one_time_amount_cents: int = 0
    stripe_ok: bool = True


def _classify_charges(charges: list) -> PaymentActivityResult:
    """Pure function — client-side filtering of already-fetched Charge
    objects (or plain dicts, for tests). One list call covers succeeded +
    failed + the subscription/one-time split (charge.invoice is populated
    for subscription-billed charges, null for one-time purchases) — not
    separate Stripe queries."""
    result = PaymentActivityResult()
    for charge in charges:
        status = _field(charge, "status")
        amount = _field(charge, "amount", 0) or 0
        if status == "succeeded":
            result.new_count += 1
            result.new_amount_cents += amount
            if _field(charge, "invoice"):
                result.subscription_count += 1
                result.subscription_amount_cents += amount
            else:
                result.one_time_count += 1
                result.one_time_amount_cents += amount
        elif status == "failed":
            result.failed_count += 1
            result.failed_amount_cents += amount
    return result


def check_payment_activity(as_of: Optional[date] = None) -> PaymentActivityResult:
    if not _init_stripe():
        logger.error("[Vera] Stripe not configured — cannot check payment activity")
        return PaymentActivityResult(stripe_ok=False)
    gte, lt = _day_window_utc(as_of)
    charges = _paginate(stripe.Charge.list, created={"gte": gte, "lt": lt})
    if charges is None:
        logger.error("[Vera] Stripe charge pull failed — abstaining from payment-activity check")
        return PaymentActivityResult(stripe_ok=False)
    return _classify_charges(charges)


def _write_payment_activity_facts(result: PaymentActivityResult) -> None:
    facts = [
        ("revenue.new_payments.count", result.new_count,
         "stripe.Charge.list(created=today) filtered status=succeeded"),
        ("revenue.new_payments.amount_cents", result.new_amount_cents,
         "stripe.Charge.list(created=today) filtered status=succeeded"),
        ("revenue.failed_payments.count", result.failed_count,
         "stripe.Charge.list(created=today) filtered status=failed"),
        ("revenue.failed_payments.amount_cents", result.failed_amount_cents,
         "stripe.Charge.list(created=today) filtered status=failed"),
        ("revenue.new_payments.subscription_count", result.subscription_count,
         "succeeded charges with invoice != null"),
        ("revenue.new_payments.subscription_amount_cents", result.subscription_amount_cents,
         "succeeded charges with invoice != null"),
        ("revenue.new_payments.one_time_count", result.one_time_count,
         "succeeded charges with invoice == null"),
        ("revenue.new_payments.one_time_amount_cents", result.one_time_amount_cents,
         "succeeded charges with invoice == null"),
    ]
    for fact_key, value, method in facts:
        write_fact(
            fact_key, str(value), value_numeric=Decimal(value), source="stripe",
            method=method, freshness_class=FRESHNESS_REVENUE_24H,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 3D — Refunds & disputes today (product-agnostic)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RefundsDisputesResult:
    refunds_count: int = 0
    refunds_amount_cents: int = 0
    disputes_count: int = 0
    disputes_amount_cents: int = 0
    disputes: list = field(default_factory=list)
    stripe_ok: bool = True


def _summarize_refunds_disputes(refunds: list, disputes: list) -> RefundsDisputesResult:
    """Pure function — summarizes already-fetched Refund/Dispute objects (or
    plain dicts, for tests)."""
    disputes_detail = [
        {"id": _field(d, "id"), "amount": _field(d, "amount", 0) or 0, "reason": _field(d, "reason")}
        for d in disputes
    ]
    return RefundsDisputesResult(
        refunds_count=len(refunds),
        refunds_amount_cents=sum((_field(r, "amount", 0) or 0) for r in refunds),
        disputes_count=len(disputes),
        disputes_amount_cents=sum(d["amount"] for d in disputes_detail),
        disputes=disputes_detail,
    )


def check_refunds_and_disputes(as_of: Optional[date] = None) -> RefundsDisputesResult:
    """Product-agnostic — closes the confirmed gap in
    stripe_webhooks._on_dispute_created, which only resolves disputes tied to
    a PremiumPurchase row."""
    if not _init_stripe():
        logger.error("[Vera] Stripe not configured — cannot check refunds/disputes")
        return RefundsDisputesResult(stripe_ok=False)
    gte, lt = _day_window_utc(as_of)
    refunds = _paginate(stripe.Refund.list, created={"gte": gte, "lt": lt})
    disputes = _paginate(stripe.Dispute.list, created={"gte": gte, "lt": lt})
    if refunds is None or disputes is None:
        logger.error("[Vera] Stripe refund/dispute pull failed — abstaining")
        return RefundsDisputesResult(stripe_ok=False)
    return _summarize_refunds_disputes(refunds, disputes)


def _write_refunds_disputes_facts(result: RefundsDisputesResult) -> None:
    facts = [
        ("revenue.refunds.count", result.refunds_count, "stripe.Refund.list(created=today)"),
        ("revenue.refunds.amount_cents", result.refunds_amount_cents, "stripe.Refund.list(created=today)"),
        ("revenue.disputes.count", result.disputes_count, "stripe.Dispute.list(created=today)"),
        ("revenue.disputes.amount_cents", result.disputes_amount_cents, "stripe.Dispute.list(created=today)"),
    ]
    for fact_key, value, method in facts:
        write_fact(
            fact_key, str(value), value_numeric=Decimal(value), source="stripe",
            method=method, freshness_class=FRESHNESS_REVENUE_24H,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Report renderer + delivery + orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def render_revenue_truth_report(
    reconciliation: ReconciliationResult,
    mrr: MrrResult,
    new_yesterday_cents: Optional[int],
    payments: PaymentActivityResult,
    refunds_disputes: RefundsDisputesResult,
    report_date: Optional[date] = None,
) -> tuple:
    """Returns (subject, body). Plain text, numbers first, Vera's voice."""
    report_date = report_date or datetime.now(timezone.utc).date()

    lines = [
        f"Vera — Revenue Truth Report — {report_date.isoformat()}",
        "=" * 60,
        "",
    ]

    if new_yesterday_cents is not None:
        lines.append(f"NEW MRR ADDED YESTERDAY: ${new_yesterday_cents / 100:,.2f}")
    else:
        lines.append("NEW MRR ADDED YESTERDAY: no prior day to compare (first run)")

    lines += ["", "RECONCILIATION"]
    if not reconciliation.stripe_ok:
        lines.append("  STRIPE UNREACHABLE — this section could not be verified today.")
    else:
        lines.append(f"  Paying but no access: {reconciliation.paying_no_access_count}")
        if reconciliation.paying_no_access_sample_ids:
            lines.append(f"    sample: {', '.join(reconciliation.paying_no_access_sample_ids)}")
        lines.append(f"  Access but not paying: {reconciliation.access_not_paying_count}")
        for detail in reconciliation.access_not_paying_details:
            lines.append(
                f"    - {detail['customer_id']}: {detail['reason']} "
                f"(stripe status: {detail.get('stripe_status')})"
            )

    lines += ["", "MRR"]
    lines.append(f"  DB: ${mrr.db_total_cents / 100:,.2f}")
    if not mrr.stripe_ok:
        lines.append("  Stripe: UNREACHABLE — MRR drift could not be verified today.")
    else:
        lines.append(f"  Stripe: ${mrr.stripe_total_cents / 100:,.2f}")
        lines.append(f"  Drift:  ${mrr.drift_cents / 100:,.2f} (stripe - db)")
    if mrr.active_null_plan_price_count:
        lines.append(
            f"  {mrr.active_null_plan_price_count} active subscriber(s) excluded "
            f"from DB MRR (plan_price is NULL)"
        )

    lines += ["", "PAYMENTS TODAY"]
    if not payments.stripe_ok:
        lines.append("  STRIPE UNREACHABLE — payment activity could not be verified today.")
    else:
        lines.append(
            f"  New:    {payments.new_count} (${payments.new_amount_cents / 100:,.2f}) "
            f"— subscription {payments.subscription_count} "
            f"(${payments.subscription_amount_cents / 100:,.2f}), "
            f"one-time {payments.one_time_count} (${payments.one_time_amount_cents / 100:,.2f})"
        )
        lines.append(f"  Failed: {payments.failed_count} (${payments.failed_amount_cents / 100:,.2f})")

    lines += ["", "REFUNDS & DISPUTES"]
    if not refunds_disputes.stripe_ok:
        lines.append("  STRIPE UNREACHABLE — refunds/disputes could not be verified today.")
    else:
        lines.append(
            f"  Refunds:  {refunds_disputes.refunds_count} "
            f"(${refunds_disputes.refunds_amount_cents / 100:,.2f})"
        )
        lines.append(
            f"  Disputes: {refunds_disputes.disputes_count} "
            f"(${refunds_disputes.disputes_amount_cents / 100:,.2f})"
        )
        for d in refunds_disputes.disputes:
            lines.append(f"    - {d['id']}: ${d['amount'] / 100:,.2f} ({d['reason']})")

    lines += ["", "— Vera."]
    body = "\n".join(lines)

    unreachable_count = sum(
        0 if ok else 1
        for ok in (reconciliation.stripe_ok, mrr.stripe_ok, payments.stripe_ok, refunds_disputes.stripe_ok)
    )
    if unreachable_count:
        subject = (
            f"[Vera] Revenue Truth Report {report_date.isoformat()} — "
            f"STRIPE UNREACHABLE for {unreachable_count} section(s), rest below"
        )
    else:
        total_mismatches = reconciliation.paying_no_access_count + reconciliation.access_not_paying_count
        subject = (
            f"[Vera] Revenue Truth Report {report_date.isoformat()} — "
            f"{total_mismatches} reconciliation mismatch(es), "
            f"{refunds_disputes.disputes_count} dispute(s)"
        )
    return subject, body


def run_revenue_truth() -> int:
    """Entry point for `python -m src.agents.vera --revenue-truth`."""
    from src.services.kill_switch_service import get_kill_switch_status

    status = get_kill_switch_status(KILL_SWITCH_FEATURE)
    if status.get("color") == "red":
        logger.warning(
            "[Vera] kill switch [%s] = red — skipping revenue truth report", KILL_SWITCH_FEATURE
        )
        return 1

    reconciliation = check_subscriber_reconciliation()
    mrr = check_mrr(reconciliation.active_subscriptions_by_customer)

    # Read the prior CALENDAR DAY's stripe-total BEFORE writing today's, so
    # the delta compares against yesterday — not today-vs-itself on a
    # same-day retry (limit=10 is generous headroom against retry storms;
    # cheap for Postgres either way).
    today = datetime.now(timezone.utc).date()
    recent = read_facts("revenue.mrr.stripe_total", fresh_only=False, limit=10)
    prior_row = _select_prior_day_row(recent, today)
    new_yesterday_cents = _compute_mrr_delta(mrr.stripe_total_cents, prior_row)

    payments = check_payment_activity()
    refunds_disputes = check_refunds_and_disputes()

    _write_reconciliation_facts(reconciliation)
    _write_mrr_facts(mrr, new_yesterday_cents)
    _write_payment_activity_facts(payments)
    _write_refunds_disputes_facts(refunds_disputes)

    subject, body = render_revenue_truth_report(
        reconciliation, mrr, new_yesterday_cents, payments, refunds_disputes,
    )

    from src.services.email import send_alert

    recipients = report_recipients()
    if not recipients:
        logger.info("[Vera] no REPORT_RECIPIENTS configured — report generated but not emailed")
    for addr in recipients:
        try:
            send_alert(subject, body, to=addr)
        except Exception as exc:
            logger.warning("[Vera] failed to send revenue-truth report to %s: %s", addr, exc)

    logger.info(
        "[Vera] revenue truth report complete: paying_no_access=%d access_not_paying=%d "
        "mrr_drift_cents=%d disputes=%d",
        reconciliation.paying_no_access_count, reconciliation.access_not_paying_count,
        mrr.drift_cents, refunds_disputes.disputes_count,
    )
    return 0
