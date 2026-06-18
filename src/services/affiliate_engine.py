"""Affiliate engine — minting and (later) attribution/commission.

An Affiliate is an external promoter paid a cash Commission for referred paying
subscribers. Distinct from the peer referral loop (referral_engine.py) and the
Partner subscription tier (partner_tier.py). See docs/adr/0005.
"""
import calendar
import logging
import secrets
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.core.models import Affiliate, AffiliateReferral, SubscriptionInvoice
from src.services.phone_utils import normalize as normalize_phone

_COMMISSION_WINDOW_MONTHS = 12

logger = logging.getLogger(__name__)

_REF_CODE_BYTES = 12          # secrets.token_urlsafe(12) -> 16-char opaque slug
_MINT_RETRIES = 3

# Tracking-cookie contract (Phase 2). Persistent 60-day Attribution Window —
# deliberately NOT a session cookie, so a click survives to a later paid upgrade.
AFFILIATE_COOKIE_NAME = "fa_ref"
AFFILIATE_COOKIE_MAX_AGE = 60 * 24 * 60 * 60      # 60 days, in seconds
AFFILIATE_REF_MAX_LEN = 40                         # matches Affiliate.ref_code


def mint_affiliate(
    db: Session,
    name: str,
    contact_email: Optional[str] = None,
    contact_phone: Optional[str] = None,
    commission_rate: Optional[Decimal] = None,
) -> Affiliate:
    """Create an Affiliate with an opaque, non-enumerable ref_code.

    commission_rate is optional — the DB default (0.20) applies when omitted.
    Retries on the vanishingly rare ref_code collision.
    """
    phone = normalize_phone(contact_phone) if contact_phone else None

    last_err: Optional[IntegrityError] = None
    for _ in range(_MINT_RETRIES):
        aff = Affiliate(
            ref_code=secrets.token_urlsafe(_REF_CODE_BYTES),
            name=name,
            contact_email=contact_email,
            contact_phone=phone,
        )
        if commission_rate is not None:
            aff.commission_rate = Decimal(str(commission_rate))
        db.add(aff)
        try:
            db.flush()
            logger.info("Minted affiliate id=%s", aff.id)
            return aff
        except IntegrityError as exc:
            last_err = exc
            db.rollback()
    raise RuntimeError("Failed to mint affiliate after retries") from last_err


def _is_self_referral(sub_phone, sub_email, aff_phone, aff_email) -> bool:
    """An affiliate cannot earn Commission on themselves — match on either
    normalized phone or lowercased email."""
    if sub_phone and aff_phone and sub_phone == aff_phone:
        return True
    if sub_email and aff_email and sub_email.strip().lower() == aff_email.strip().lower():
        return True
    return False


def attribute_signup(db: Session, subscriber, affiliate_ref: Optional[str]):
    """Stamp a newly-registered Subscriber to an Affiliate and create a pending
    AffiliateReferral. Returns the referral, or None when not attributable.

    Must be called only at registration of a *new* subscriber — that is what
    enforces "stamp at registration only" (a deduped existing subscriber never
    reaches here). Refuses: no/unknown/disabled ref, self-referral, or a
    subscriber already attributed (idempotent).
    """
    if not affiliate_ref:
        return None

    row = db.execute(
        text(
            "SELECT id, contact_phone, contact_email FROM affiliates "
            "WHERE ref_code = :rc AND status = 'active'"
        ),
        {"rc": affiliate_ref},
    ).first()
    if row is None:
        return None

    if _is_self_referral(subscriber.phone, subscriber.email, row.contact_phone, row.contact_email):
        logger.info("Affiliate self-referral blocked: affiliate=%s sub=%s", row.id, subscriber.id)
        return None

    if subscriber.affiliate_ref:
        return None
    already = db.execute(
        text("SELECT 1 FROM affiliate_referrals WHERE subscriber_id = :s"),
        {"s": subscriber.id},
    ).first()
    if already:
        return None

    subscriber.affiliate_ref = affiliate_ref
    subscriber.signup_source = "affiliate"
    referral = AffiliateReferral(
        affiliate_id=row.id, subscriber_id=subscriber.id, status="pending"
    )
    db.add(referral)
    db.flush()
    logger.info("Affiliate attribution: affiliate=%s sub=%s", row.id, subscriber.id)
    return referral


def confirm_referral(db: Session, subscriber_id: int):
    """Flip a pending AffiliateReferral to active when its subscriber reaches a
    paid plan. Idempotent — only a pending referral is confirmed, so a replayed
    checkout webhook never re-stamps confirmed_at. Returns the referral or None.

    The 12-month window anchor (paid_tenure_start/window_end) is set on the
    first paid invoice (Phase 5), not here — this only flips status.
    """
    row = db.execute(
        text("SELECT id, status FROM affiliate_referrals WHERE subscriber_id = :s"),
        {"s": subscriber_id},
    ).first()
    if row is None or row.status != "pending":
        return None

    referral = db.get(AffiliateReferral, row.id)
    referral.status = "active"
    referral.confirmed_at = datetime.now(timezone.utc)
    db.flush()
    logger.info("Affiliate referral confirmed: id=%s sub=%s", referral.id, subscriber_id)
    return referral


def _add_months(dt: datetime, months: int) -> datetime:
    m = dt.month - 1 + months
    year = dt.year + m // 12
    month = m % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)


def record_subscription_invoice(
    db: Session,
    *,
    stripe_invoice_id: str,
    subscriber_id: int,
    amount_collected_cents: int,
    period_month: date,
    paid_at: datetime,
    is_subscription: bool,
    payment_intent_id: Optional[str] = None,
):
    """Record a collected recurring-subscription invoice — the source of truth
    for Commission. One-time charges (bundles/lead packs) are ignored. Idempotent
    on stripe_invoice_id. On the subscriber's first paid invoice, anchors the
    12-month Commission window on their Affiliate Referral.

    payment_intent_id is stored so refunds/disputes can be matched back to this
    invoice even on Stripe API versions where charge.invoice is null.
    """
    if not is_subscription:
        return None

    existing = db.execute(
        text("SELECT id FROM subscription_invoices WHERE stripe_invoice_id = :i"),
        {"i": stripe_invoice_id},
    ).first()
    if existing:
        return db.get(SubscriptionInvoice, existing.id)

    invoice = SubscriptionInvoice(
        subscriber_id=subscriber_id,
        stripe_invoice_id=stripe_invoice_id,
        stripe_payment_intent_id=payment_intent_id,
        amount_collected_cents=amount_collected_cents,
        period_month=period_month,
        paid_at=paid_at,
    )
    db.add(invoice)
    db.flush()
    _maybe_anchor_window(db, subscriber_id, paid_at)
    return invoice


def _maybe_anchor_window(db: Session, subscriber_id: int, paid_at: datetime) -> None:
    row = db.execute(
        text(
            "SELECT id, paid_tenure_start FROM affiliate_referrals WHERE subscriber_id = :s"
        ),
        {"s": subscriber_id},
    ).first()
    if row is None or row.paid_tenure_start is not None:
        return
    referral = db.get(AffiliateReferral, row.id)
    referral.paid_tenure_start = paid_at
    referral.window_end = _add_months(paid_at, _COMMISSION_WINDOW_MONTHS)
    db.flush()


def _reverse_invoice_row(db: Session, where_sql: str, params: dict, reason: str, label: str):
    """Shared reversal: idempotent, returns the invoice or None."""
    row = db.execute(
        text(f"SELECT id, reversed_at FROM subscription_invoices WHERE {where_sql}"),
        params,
    ).first()
    if row is None or row.reversed_at is not None:
        return None
    invoice = db.get(SubscriptionInvoice, row.id)
    invoice.reversed_at = datetime.now(timezone.utc)
    invoice.reversed_reason = reason
    db.flush()
    logger.info("Subscription invoice reversed: %s reason=%s", label, reason)
    return invoice


def mark_invoice_reversed(db: Session, stripe_invoice_id: str, reason: str):
    """Reverse by Stripe invoice id (works when charge.invoice is populated —
    older Stripe API versions). Idempotent. Returns the invoice or None."""
    return _reverse_invoice_row(
        db, "stripe_invoice_id = :i", {"i": stripe_invoice_id}, reason, stripe_invoice_id
    )


def mark_invoice_reversed_by_payment_intent(db: Session, payment_intent_id: str, reason: str):
    """Reverse by Stripe PaymentIntent id — the version-robust path. Stripe API
    2026-02-25 nulls charge.invoice, but charge.payment_intent is always present,
    and it was stored on the invoice at capture. Idempotent. Returns invoice/None.
    """
    if not payment_intent_id:
        return None
    return _reverse_invoice_row(
        db, "stripe_payment_intent_id = :pi", {"pi": payment_intent_id}, reason, payment_intent_id
    )


_ACCRUAL_SQL = text(
    """
    INSERT INTO affiliate_payout_ledger
        (affiliate_id, affiliate_referral_id, period_month, line_type, amount_cents, created_at)
    SELECT ar.affiliate_id, ar.id, :pm, 'accrual',
           ROUND(a.commission_rate * SUM(si.amount_collected_cents))::int, now()
    FROM affiliate_referrals ar
    JOIN affiliates a ON a.id = ar.affiliate_id
    JOIN subscription_invoices si ON si.subscriber_id = ar.subscriber_id
    WHERE ar.status = 'active'
      AND si.period_month = :pm
      AND si.reversed_at IS NULL
      AND ar.window_end IS NOT NULL
      AND si.period_month <= ar.window_end
    GROUP BY ar.id, ar.affiliate_id, a.commission_rate
    ON CONFLICT (affiliate_referral_id, period_month, line_type) DO NOTHING
    """
)

_CLAWBACK_SQL = text(
    """
    INSERT INTO affiliate_payout_ledger
        (affiliate_id, affiliate_referral_id, period_month, line_type, amount_cents,
         source_invoice_id, created_at)
    SELECT ar.affiliate_id, ar.id, si.period_month, 'clawback',
           -ROUND(a.commission_rate * si.amount_collected_cents)::int, si.id, now()
    FROM subscription_invoices si
    JOIN affiliate_referrals ar ON ar.subscriber_id = si.subscriber_id
    JOIN affiliates a ON a.id = ar.affiliate_id
    WHERE si.reversed_at IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM affiliate_payout_ledger l
          WHERE l.affiliate_referral_id = ar.id
            AND l.period_month = si.period_month
            AND l.line_type = 'accrual'
      )
    ON CONFLICT (affiliate_referral_id, period_month, line_type) DO NOTHING
    """
)


def run_monthly_payout(db: Session, period_month: date) -> dict:
    """Write Commission accrual lines for the given month and Commission Clawback
    lines for any reversed invoice that previously accrued. Append-only and
    idempotent (UNIQUE per referral+month+line_type), so reruns are safe.

    Log-only — records amounts owed in the Payout Ledger; no Stripe disbursement.
    """
    pm = period_month.replace(day=1)
    accruals = db.execute(_ACCRUAL_SQL, {"pm": pm}).rowcount
    clawbacks = db.execute(_CLAWBACK_SQL).rowcount
    db.flush()
    logger.info("Affiliate payout run for %s: accruals=%s clawbacks=%s", pm, accruals, clawbacks)
    return {"period_month": pm.isoformat(), "accruals": accruals, "clawbacks": clawbacks}


def get_affiliate_ledger(db: Session, affiliate_id: int) -> dict:
    """The Affiliate's Payout Ledger: every accrual/clawback line plus the
    running balance (sum of all line amounts, clawbacks already negative)."""
    rows = db.execute(
        text(
            "SELECT id, affiliate_referral_id, period_month, line_type, amount_cents, "
            "source_invoice_id, created_at "
            "FROM affiliate_payout_ledger WHERE affiliate_id = :a "
            "ORDER BY period_month, id"
        ),
        {"a": affiliate_id},
    ).all()
    lines = [
        {
            "id": r.id,
            "affiliate_referral_id": r.affiliate_referral_id,
            "period_month": r.period_month.isoformat(),
            "line_type": r.line_type,
            "amount_cents": r.amount_cents,
            "source_invoice_id": r.source_invoice_id,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
    return {
        "affiliate_id": affiliate_id,
        "balance_cents": sum(r.amount_cents for r in rows),
        "lines": lines,
    }
