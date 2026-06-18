"""Phase 5 — collected subscription-invoice capture + reversal."""
from datetime import date, datetime, timezone

from sqlalchemy import text

from src.services.signup_engine import create_free_account
from src.services.affiliate_engine import (
    mint_affiliate,
    record_subscription_invoice,
    mark_invoice_reversed,
)

PHONE = "+18135550444"
PAID_AT = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
PERIOD = date(2026, 5, 1)


def _invoice(db, stripe_invoice_id, subscriber_id, *, is_subscription=True, cents=19700):
    return record_subscription_invoice(
        db,
        stripe_invoice_id=stripe_invoice_id,
        subscriber_id=subscriber_id,
        amount_collected_cents=cents,
        period_month=PERIOD,
        paid_at=PAID_AT,
        is_subscription=is_subscription,
    )


def test_records_subscription_invoice(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    inv = _invoice(db, "in_test_1", sub.id)
    assert inv is not None
    row = db.execute(
        text("SELECT amount_collected_cents, period_month FROM subscription_invoices WHERE stripe_invoice_id='in_test_1'")
    ).first()
    assert row.amount_collected_cents == 19700
    assert row.period_month == PERIOD


def test_one_time_invoice_not_recorded(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    assert _invoice(db, "in_onetime", sub.id, is_subscription=False) is None


def test_duplicate_invoice_is_idempotent(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    _invoice(db, "in_dup", sub.id)
    _invoice(db, "in_dup", sub.id)
    count = db.execute(
        text("SELECT count(*) FROM subscription_invoices WHERE stripe_invoice_id='in_dup'")
    ).scalar()
    assert count == 1


def test_first_invoice_sets_12_month_window(fresh_db):
    db = fresh_db
    aff = mint_affiliate(db, name="Joe")
    sub = create_free_account(PHONE, "landing_page", db, affiliate_ref=aff.ref_code)
    _invoice(db, "in_anchor", sub.id)
    row = db.execute(
        text("SELECT paid_tenure_start, window_end FROM affiliate_referrals WHERE subscriber_id=:s"),
        {"s": sub.id},
    ).first()
    # column is timestamp-without-tz, so compare naive
    assert row.paid_tenure_start == PAID_AT.replace(tzinfo=None)
    # +12 months from the first paid invoice
    assert row.window_end.year == PAID_AT.year + 1
    assert row.window_end.month == PAID_AT.month


def test_mark_reversed_refund(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    _invoice(db, "in_refund", sub.id)
    mark_invoice_reversed(db, "in_refund", "refund")
    row = db.execute(
        text("SELECT reversed_at, reversed_reason FROM subscription_invoices WHERE stripe_invoice_id='in_refund'")
    ).first()
    assert row.reversed_at is not None
    assert row.reversed_reason == "refund"


def test_mark_reversed_dispute_is_idempotent(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    _invoice(db, "in_disp", sub.id)
    first = mark_invoice_reversed(db, "in_disp", "dispute")
    assert first.reversed_reason == "dispute"
    assert mark_invoice_reversed(db, "in_disp", "dispute") is None  # already reversed


# ── Version-robust refund linkage (Stripe API 2026-02-25 nulls charge.invoice) ──

def _invoice_with_pi(db, stripe_invoice_id, subscriber_id, pi):
    return record_subscription_invoice(
        db, stripe_invoice_id=stripe_invoice_id, subscriber_id=subscriber_id,
        amount_collected_cents=19700, period_month=PERIOD, paid_at=PAID_AT,
        is_subscription=True, payment_intent_id=pi,
    )


def test_capture_stores_payment_intent(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    _invoice_with_pi(db, "in_pi", sub.id, "pi_abc123")
    row = db.execute(
        text("SELECT stripe_payment_intent_id FROM subscription_invoices WHERE stripe_invoice_id='in_pi'")
    ).first()
    assert row.stripe_payment_intent_id == "pi_abc123"


def test_reverse_by_payment_intent(fresh_db):
    from src.services.affiliate_engine import mark_invoice_reversed_by_payment_intent
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    _invoice_with_pi(db, "in_pi2", sub.id, "pi_xyz")
    inv = mark_invoice_reversed_by_payment_intent(db, "pi_xyz", "refund")
    assert inv is not None and inv.reversed_reason == "refund"
    # idempotent + unknown PI
    assert mark_invoice_reversed_by_payment_intent(db, "pi_xyz", "refund") is None
    assert mark_invoice_reversed_by_payment_intent(db, "pi_nope", "refund") is None


def test_reverse_from_charge_new_api_uses_payment_intent(fresh_db):
    # 2026-02-25 shape: charge.invoice is null, only payment_intent present
    from src.services.stripe_webhooks import _affiliate_reverse_from_charge
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    _invoice_with_pi(db, "in_newapi", sub.id, "pi_new")
    _affiliate_reverse_from_charge({"invoice": None, "payment_intent": "pi_new"}, "refund", db)
    row = db.execute(
        text("SELECT reversed_reason FROM subscription_invoices WHERE stripe_invoice_id='in_newapi'")
    ).first()
    assert row.reversed_reason == "refund"


def test_reverse_from_charge_old_api_uses_invoice(fresh_db):
    # older shape: charge.invoice present — still works
    from src.services.stripe_webhooks import _affiliate_reverse_from_charge
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)
    _invoice_with_pi(db, "in_oldapi", sub.id, "pi_old")
    _affiliate_reverse_from_charge({"invoice": "in_oldapi", "payment_intent": "pi_old"}, "dispute", db)
    row = db.execute(
        text("SELECT reversed_reason FROM subscription_invoices WHERE stripe_invoice_id='in_oldapi'")
    ).first()
    assert row.reversed_reason == "dispute"
