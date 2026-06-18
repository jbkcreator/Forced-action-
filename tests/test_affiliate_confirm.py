"""Phase 4 — confirm an Affiliate Referral at paid upgrade."""
from sqlalchemy import text

from src.services.signup_engine import create_free_account
from src.services.affiliate_engine import mint_affiliate, confirm_referral

PHONE = "+18135550333"


def _make_attributed_sub(db):
    aff = mint_affiliate(db, name="Joe")
    sub = create_free_account(PHONE, "landing_page", db, affiliate_ref=aff.ref_code)
    return sub


def _referral(db, subscriber_id):
    return db.execute(
        text("SELECT status, confirmed_at FROM affiliate_referrals WHERE subscriber_id=:s"),
        {"s": subscriber_id},
    ).first()


def test_confirm_activates_pending_referral(fresh_db):
    db = fresh_db
    sub = _make_attributed_sub(db)
    confirm_referral(db, sub.id)
    row = _referral(db, sub.id)
    assert row.status == "active"
    assert row.confirmed_at is not None


def test_confirm_is_noop_without_affiliate(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db)  # no affiliate
    assert confirm_referral(db, sub.id) is None


def test_confirm_is_idempotent(fresh_db):
    db = fresh_db
    sub = _make_attributed_sub(db)
    confirm_referral(db, sub.id)
    first = _referral(db, sub.id).confirmed_at
    confirm_referral(db, sub.id)  # replay
    row = _referral(db, sub.id)
    assert row.status == "active"
    assert row.confirmed_at == first  # unchanged
