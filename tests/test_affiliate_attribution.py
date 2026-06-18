"""Phase 3 — affiliate attribution at registration.

Exercised through the public create_free_account (phone flow — no Stripe), so
the tests describe behavior, not internals. Real PG via fresh_db (rolled back).
"""
from unittest.mock import patch

import pytest
from sqlalchemy import text

from src.services.signup_engine import create_free_account
from src.services.affiliate_engine import mint_affiliate

PHONE = "+18135550111"
PHONE2 = "+18135550222"


def _referral_count(db, subscriber_id):
    return db.execute(
        text("SELECT count(*) FROM affiliate_referrals WHERE subscriber_id=:s"),
        {"s": subscriber_id},
    ).scalar()


def test_valid_ref_stamps_and_creates_pending_referral(fresh_db):
    db = fresh_db
    aff = mint_affiliate(db, name="Joe")
    sub = create_free_account(PHONE, "landing_page", db, affiliate_ref=aff.ref_code)
    assert sub.signup_source == "affiliate"
    assert sub.affiliate_ref == aff.ref_code
    row = db.execute(
        text("SELECT affiliate_id, status FROM affiliate_referrals WHERE subscriber_id=:s"),
        {"s": sub.id},
    ).first()
    assert row is not None
    assert row[0] == aff.id
    assert row[1] == "pending"


def test_unknown_ref_creates_no_referral(fresh_db):
    db = fresh_db
    sub = create_free_account(PHONE, "landing_page", db, affiliate_ref="does-not-exist")
    assert sub is not None
    assert _referral_count(db, sub.id) == 0
    assert sub.signup_source != "affiliate"


def test_disabled_affiliate_creates_no_referral(fresh_db):
    db = fresh_db
    aff = mint_affiliate(db, name="Disabled")
    aff.status = "disabled"
    db.flush()
    sub = create_free_account(PHONE, "landing_page", db, affiliate_ref=aff.ref_code)
    assert _referral_count(db, sub.id) == 0


def test_preexisting_subscriber_not_attributed(fresh_db):
    # (c) someone signs up first with no affiliate, later arrives via a ref link
    db = fresh_db
    aff = mint_affiliate(db, name="Latecomer")
    first = create_free_account(PHONE, "landing_page", db)  # no ref
    again = create_free_account(PHONE, "landing_page", db, affiliate_ref=aff.ref_code)
    assert again.id == first.id          # deduped to the same subscriber
    assert _referral_count(db, again.id) == 0
    assert again.affiliate_ref is None


def test_self_referral_is_blocked(fresh_db):
    db = fresh_db
    aff = mint_affiliate(db, name="Self", contact_phone=PHONE)
    sub = create_free_account(PHONE, "landing_page", db, affiliate_ref=aff.ref_code)
    assert _referral_count(db, sub.id) == 0


def test_email_signup_path_attributes_affiliate(fresh_db):
    # the real /api/free-signup path goes through create_free_account_by_email
    db = fresh_db
    from src.services.signup_engine import create_free_account_by_email
    aff = mint_affiliate(db, name="EmailAff")
    with patch("src.services.signup_engine._create_stripe_customer", return_value="cus_test_aff"):
        sub = create_free_account_by_email(
            "aff_signup@example.com", db,
            affiliate_ref=aff.ref_code, send_welcome=False,
        )
    assert sub.signup_source == "affiliate"
    assert _referral_count(db, sub.id) == 1


def test_affiliate_and_peer_referral_are_independent(fresh_db):
    # (b) a signup carrying both pays the affiliate AND fires the peer loop
    db = fresh_db
    aff = mint_affiliate(db, name="Both")
    with patch("src.services.referral_engine.process_signup") as peer:
        peer.return_value = None
        sub = create_free_account(
            PHONE, "landing_page", db,
            affiliate_ref=aff.ref_code, referral_code="PEERCODE",
        )
    assert _referral_count(db, sub.id) == 1   # affiliate attributed
    peer.assert_called_once()                  # peer credit loop still fired
