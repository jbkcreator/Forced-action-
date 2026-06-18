"""Phase 6 — monthly payout job: accrual + clawback, idempotent, log-only."""
from datetime import date, datetime, timezone

from sqlalchemy import text

from src.services.signup_engine import create_free_account
from src.services.affiliate_engine import (
    mint_affiliate,
    confirm_referral,
    record_subscription_invoice,
    mark_invoice_reversed,
    run_monthly_payout,
)

PERIOD = date(2026, 5, 1)
PAID_AT = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


def _setup(db, phone, *, rate=None, cents=19700, invoice_id="in_a", period=PERIOD, paid_at=PAID_AT):
    aff = mint_affiliate(db, name="Aff", commission_rate=rate)
    sub = create_free_account(phone, "landing_page", db, affiliate_ref=aff.ref_code)
    confirm_referral(db, sub.id)
    record_subscription_invoice(
        db, stripe_invoice_id=invoice_id, subscriber_id=sub.id,
        amount_collected_cents=cents, period_month=period, paid_at=paid_at,
        is_subscription=True,
    )
    return aff, sub


def _lines(db, subscriber_id, line_type):
    return db.execute(
        text(
            "SELECT amount_cents FROM affiliate_payout_ledger l "
            "JOIN affiliate_referrals ar ON ar.id=l.affiliate_referral_id "
            "WHERE ar.subscriber_id=:s AND l.line_type=:t"
        ),
        {"s": subscriber_id, "t": line_type},
    ).scalars().all()


def test_accrues_20pct_of_collected(fresh_db):
    db = fresh_db
    _, sub = _setup(db, "+18135551001")
    run_monthly_payout(db, PERIOD)
    lines = _lines(db, sub.id, "accrual")
    assert lines == [3940]  # round(0.20 * 19700)


def test_per_affiliate_rate_honored(fresh_db):
    db = fresh_db
    from decimal import Decimal
    _, sub = _setup(db, "+18135551002", rate=Decimal("0.30"))
    run_monthly_payout(db, PERIOD)
    assert _lines(db, sub.id, "accrual") == [5910]  # round(0.30 * 19700)


def test_no_accrual_past_window(fresh_db):
    db = fresh_db
    _, sub = _setup(db, "+18135551003")
    # push the window to before the period
    db.execute(
        text("UPDATE affiliate_referrals SET window_end=:we WHERE subscriber_id=:s"),
        {"we": datetime(2026, 4, 1), "s": sub.id},
    )
    run_monthly_payout(db, PERIOD)
    assert _lines(db, sub.id, "accrual") == []


def test_no_accrual_without_paid_invoice(fresh_db):
    db = fresh_db
    aff = mint_affiliate(db, name="NoPay")
    sub = create_free_account("+18135551004", "landing_page", db, affiliate_ref=aff.ref_code)
    confirm_referral(db, sub.id)  # active but no invoice this month
    run_monthly_payout(db, PERIOD)
    assert _lines(db, sub.id, "accrual") == []


def test_accrual_is_idempotent(fresh_db):
    db = fresh_db
    _, sub = _setup(db, "+18135551005")
    run_monthly_payout(db, PERIOD)
    run_monthly_payout(db, PERIOD)  # rerun
    assert _lines(db, sub.id, "accrual") == [3940]  # not doubled


def test_reversed_invoice_produces_clawback(fresh_db):
    db = fresh_db
    _, sub = _setup(db, "+18135551006", invoice_id="in_claw")
    run_monthly_payout(db, PERIOD)            # accrue first
    mark_invoice_reversed(db, "in_claw", "refund")
    run_monthly_payout(db, PERIOD)            # clawback this run
    assert _lines(db, sub.id, "clawback") == [-3940]
    run_monthly_payout(db, PERIOD)            # rerun — no duplicate clawback
    assert _lines(db, sub.id, "clawback") == [-3940]
