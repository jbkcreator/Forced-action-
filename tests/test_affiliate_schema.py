"""
Phase 0 — Affiliate Program schema contracts.

These tests verify the DB-level guarantees later phases depend on:
  - ref_code is globally unique (Phase 1 mint)
  - one affiliate per subscriber (Phase 3 attribution)
  - the payout ledger refuses duplicate accruals (Phase 6 idempotent rerun)
  - a new affiliate defaults to the 20% commission rate

Run against real Postgres (fresh_db) — each test rolls back.
"""
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.core.models import (
    Affiliate,
    AffiliateReferral,
    AffiliatePayoutLedger,
)


def _affiliate(db, ref_code, name="Acme Promotions"):
    a = Affiliate(ref_code=ref_code, name=name)
    db.add(a)
    db.flush()
    return a


def _a_subscriber_id(db):
    """An existing subscriber id to satisfy the FK; skip if the table is empty."""
    row = db.execute(text("SELECT id FROM subscribers LIMIT 1")).first()
    if row is None:
        pytest.skip("no subscribers in DB to satisfy FK")
    return row[0]


def test_ref_code_is_unique(fresh_db):
    db = fresh_db
    _affiliate(db, "uq-refcode-aaa")
    with pytest.raises(IntegrityError):
        _affiliate(db, "uq-refcode-aaa", name="Impostor")


def test_one_affiliate_per_subscriber(fresh_db):
    db = fresh_db
    sub_id = _a_subscriber_id(db)
    a1 = _affiliate(db, "uq-sub-aff-1")
    a2 = _affiliate(db, "uq-sub-aff-2")
    db.add(AffiliateReferral(affiliate_id=a1.id, subscriber_id=sub_id))
    db.flush()
    with pytest.raises(IntegrityError):
        db.add(AffiliateReferral(affiliate_id=a2.id, subscriber_id=sub_id))
        db.flush()


def test_new_affiliate_defaults_to_20_percent(fresh_db):
    db = fresh_db
    aff = _affiliate(db, "uq-default-rate")
    db.refresh(aff)
    assert aff.commission_rate == Decimal("0.20")


def test_ledger_rejects_duplicate_accrual(fresh_db):
    db = fresh_db
    sub_id = _a_subscriber_id(db)
    aff = _affiliate(db, "uq-ledger-aff")
    ref = AffiliateReferral(affiliate_id=aff.id, subscriber_id=sub_id, status="active")
    db.add(ref)
    db.flush()
    month = date(2026, 5, 1)
    db.add(AffiliatePayoutLedger(
        affiliate_id=aff.id, affiliate_referral_id=ref.id,
        period_month=month, line_type="accrual", amount_cents=1000,
    ))
    db.flush()
    with pytest.raises(IntegrityError):
        db.add(AffiliatePayoutLedger(
            affiliate_id=aff.id, affiliate_referral_id=ref.id,
            period_month=month, line_type="accrual", amount_cents=1000,
        ))
        db.flush()
