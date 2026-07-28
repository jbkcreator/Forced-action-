"""
Tests for T-B12-07 win-back offer redemption (PR #172 review fix).

Covers the actual redemption mechanism the review flagged as missing:
  - a token is minted at send time, not the benefit itself
  - the token is reusable while pending (no duplicate mint on retry)
  - an invalid/expired/already-redeemed token grants nothing
  - redemption is one-time-use (a second redeem is a no-op)
  - the zip_released credit grant only happens via redemption, and is itself
    idempotent (never double-credits)
"""
from datetime import datetime, timedelta, timezone

import pytest

from src.core.models import Subscriber


@pytest.fixture
def subscriber(fresh_db):
    sub = Subscriber(
        email="winback-test@example.com",
        tier="starter",
        status="churned",
        vertical="roofing",
        county_id="hillsborough",
        stripe_customer_id="cus_winback_test",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


class TestCreateOrReuseOffer:
    def test_creates_a_token(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer
        token = create_or_reuse_offer(subscriber.id, "zip_held", fresh_db)
        assert token
        assert len(token) > 20

    def test_reuses_pending_unexpired_offer_instead_of_minting_a_duplicate(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer
        first = create_or_reuse_offer(subscriber.id, "zip_held", fresh_db)
        second = create_or_reuse_offer(subscriber.id, "zip_held", fresh_db)
        assert first == second

    def test_different_branch_gets_a_different_token(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer
        held = create_or_reuse_offer(subscriber.id, "zip_held", fresh_db)
        released = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        assert held != released


class TestGetValidOffer:
    def test_valid_token_returns_subscriber_and_branch(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer, get_valid_offer
        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        offer = get_valid_offer(token, fresh_db)
        assert offer == {"subscriber_id": subscriber.id, "branch": "zip_released"}

    def test_unknown_token_is_invalid(self, fresh_db):
        from src.services.winback_offers import get_valid_offer
        assert get_valid_offer("not-a-real-token", fresh_db) is None

    def test_expired_token_is_invalid(self, fresh_db, subscriber):
        from src.services.winback_offers import get_valid_offer
        from src.core.models import WinbackOffer
        expired = WinbackOffer(
            subscriber_id=subscriber.id, branch="zip_held", token="expired-token-123",
            expires_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
        fresh_db.add(expired)
        fresh_db.flush()
        assert get_valid_offer("expired-token-123", fresh_db) is None


class TestRedeemOffer:
    def test_redeeming_marks_it_used_and_returns_branch(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer, redeem_offer
        token = create_or_reuse_offer(subscriber.id, "zip_held", fresh_db)
        result = redeem_offer(token, fresh_db)
        assert result == {"subscriber_id": subscriber.id, "branch": "zip_held"}

    def test_redeeming_twice_is_a_no_op_the_second_time(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer, redeem_offer
        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        first = redeem_offer(token, fresh_db)
        second = redeem_offer(token, fresh_db)
        assert first is not None
        assert second is None  # already redeemed — no benefit re-granted

    def test_a_redeemed_token_can_no_longer_mint_credits_via_get_valid_offer(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer, redeem_offer, get_valid_offer
        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        redeem_offer(token, fresh_db)
        assert get_valid_offer(token, fresh_db) is None


class TestGrantWinbackCredits:
    def test_grants_credits_once(self, fresh_db, subscriber):
        from src.services.winback_offers import grant_winback_credits
        from sqlalchemy import text
        ok = grant_winback_credits(subscriber.id, fresh_db)
        assert ok is True
        rows = fresh_db.execute(
            text(
                "SELECT amount FROM wallet_transactions "
                "WHERE subscriber_id = :sid AND description = 'tier3_winback_reactivation'"
            ),
            {"sid": subscriber.id},
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 5

    def test_idempotent_on_repeated_call(self, fresh_db, subscriber):
        from src.services.winback_offers import grant_winback_credits
        from sqlalchemy import text
        grant_winback_credits(subscriber.id, fresh_db)
        grant_winback_credits(subscriber.id, fresh_db)  # duplicate call — must not double-credit
        rows = fresh_db.execute(
            text(
                "SELECT amount FROM wallet_transactions "
                "WHERE subscriber_id = :sid AND description = 'tier3_winback_reactivation'"
            ),
            {"sid": subscriber.id},
        ).fetchall()
        assert len(rows) == 1

    def test_success_stamps_credits_granted_at_on_the_offer(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer, redeem_offer, grant_winback_credits
        from sqlalchemy import text
        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        redeem_offer(token, fresh_db)
        grant_winback_credits(subscriber.id, fresh_db, token=token)
        row = fresh_db.execute(
            text("SELECT credits_granted_at FROM winback_offers WHERE token = :t"),
            {"t": token},
        ).first()
        assert row[0] is not None

    def test_failed_grant_returns_false_and_leaves_credits_granted_at_null(
        self, fresh_db, subscriber, monkeypatch
    ):
        """
        Regression (PR #172 follow-up review): a grant failure must not look
        like success. redeemed_at (set by redeem_offer, tested separately)
        proves the reactivation happened and must stay set either way, but
        credits_granted_at must stay NULL so the reconciliation sweep can
        find and retry this row.
        """
        from src.services.winback_offers import create_or_reuse_offer, redeem_offer, grant_winback_credits
        from sqlalchemy import text
        import src.services.wallet_engine as wallet_engine

        def _boom(*a, **kw):
            raise RuntimeError("simulated wallet failure")

        monkeypatch.setattr(wallet_engine, "add_bonus", _boom)

        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        redeem_offer(token, fresh_db)
        ok = grant_winback_credits(subscriber.id, fresh_db, token=token)

        assert ok is False
        row = fresh_db.execute(
            text("SELECT redeemed_at, credits_granted_at FROM winback_offers WHERE token = :t"),
            {"t": token},
        ).first()
        assert row.redeemed_at is not None  # reactivation is still proven
        assert row.credits_granted_at is None  # but credit was NOT granted
        no_credit = fresh_db.execute(
            text("SELECT 1 FROM wallet_transactions WHERE subscriber_id = :sid"),
            {"sid": subscriber.id},
        ).first()
        assert no_credit is None


class TestReconcilePendingCreditGrants:
    def test_retries_a_redeemed_offer_whose_grant_previously_failed(
        self, fresh_db, subscriber, monkeypatch
    ):
        from src.services.winback_offers import (
            create_or_reuse_offer, redeem_offer, grant_winback_credits,
            reconcile_pending_credit_grants,
        )
        from sqlalchemy import text
        import src.services.wallet_engine as wallet_engine

        real_add_bonus = wallet_engine.add_bonus
        monkeypatch.setattr(wallet_engine, "add_bonus", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        redeem_offer(token, fresh_db)
        assert grant_winback_credits(subscriber.id, fresh_db, token=token) is False

        # "Wallet service recovers" — reconciliation should now succeed.
        monkeypatch.setattr(wallet_engine, "add_bonus", real_add_bonus)
        result = reconcile_pending_credit_grants(fresh_db)

        assert result["checked"] == 1
        assert result["granted"] == 1
        assert result["failed"] == 0
        row = fresh_db.execute(
            text("SELECT amount FROM wallet_transactions WHERE subscriber_id = :sid"),
            {"sid": subscriber.id},
        ).first()
        assert row[0] == 5

    def test_does_not_touch_zip_held_offers(self, fresh_db, subscriber):
        """zip_held never has a credit grant to reconcile — only zip_released does."""
        from src.services.winback_offers import create_or_reuse_offer, redeem_offer, reconcile_pending_credit_grants
        token = create_or_reuse_offer(subscriber.id, "zip_held", fresh_db)
        redeem_offer(token, fresh_db)
        result = reconcile_pending_credit_grants(fresh_db)
        assert result["checked"] == 0

    def test_already_credited_offers_are_not_reconsidered(self, fresh_db, subscriber):
        from src.services.winback_offers import (
            create_or_reuse_offer, redeem_offer, grant_winback_credits, reconcile_pending_credit_grants,
        )
        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        redeem_offer(token, fresh_db)
        grant_winback_credits(subscriber.id, fresh_db, token=token)  # succeeds normally
        result = reconcile_pending_credit_grants(fresh_db)
        assert result["checked"] == 0


class TestGrantFounderGraceExtension:
    """
    Founder-tier (tier == 'founder') zip_held substitute for the 50%-off
    coupon — a one-time +14-day territory grace extension, granted
    immediately rather than on checkout redemption (wayfinder map
    notion-pending-tasks, tickets F1/F2/F3).
    """

    @pytest.fixture
    def founder_subscriber(self, fresh_db):
        sub = Subscriber(
            email="founder-winback-test@example.com",
            tier="founder",
            status="churned",
            vertical="roofing",
            county_id="hillsborough",
            stripe_customer_id="cus_founder_winback_test",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        return sub

    @pytest.fixture
    def grace_territory(self, fresh_db, founder_subscriber):
        from src.core.models import ZipTerritory
        expires = datetime.now(timezone.utc) + timedelta(days=5)
        terr = ZipTerritory(
            zip_code="99901",
            vertical="roofing",
            county_id="test_founder_grace_county",
            subscriber_id=founder_subscriber.id,
            status="grace",
            grace_expires_at=expires,
        )
        fresh_db.add(terr)
        fresh_db.flush()
        return terr

    def test_extends_grace_expires_at_by_14_days(
        self, fresh_db, founder_subscriber, grace_territory
    ):
        from src.services.winback_offers import grant_founder_grace_extension
        original_expiry = grace_territory.grace_expires_at.replace(tzinfo=None)
        ok = grant_founder_grace_extension(founder_subscriber.id, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(grace_territory)
        assert ok is True
        assert grace_territory.grace_expires_at == original_expiry + timedelta(days=14)

    def test_stamps_founder_grace_extension_granted_at(
        self, fresh_db, founder_subscriber, grace_territory
    ):
        from src.services.winback_offers import grant_founder_grace_extension
        grant_founder_grace_extension(founder_subscriber.id, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(founder_subscriber)
        assert founder_subscriber.founder_grace_extension_granted_at is not None

    def test_one_time_only_second_call_is_a_no_op(
        self, fresh_db, founder_subscriber, grace_territory
    ):
        from src.services.winback_offers import grant_founder_grace_extension
        first_ok = grant_founder_grace_extension(founder_subscriber.id, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(grace_territory)
        expiry_after_first = grace_territory.grace_expires_at

        second_ok = grant_founder_grace_extension(founder_subscriber.id, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(grace_territory)

        assert first_ok is True
        assert second_ok is False
        assert grace_territory.grace_expires_at == expiry_after_first  # unchanged

    def test_no_grace_territory_is_a_no_op(self, fresh_db, founder_subscriber):
        from src.services.winback_offers import grant_founder_grace_extension
        ok = grant_founder_grace_extension(founder_subscriber.id, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(founder_subscriber)
        assert ok is False
        assert founder_subscriber.founder_grace_extension_granted_at is None

    def test_only_extends_grace_status_rows_not_locked(
        self, fresh_db, founder_subscriber, grace_territory
    ):
        """A founder may hold both a grace-status and a locked-status
        territory (different ZIPs) — only the grace one is at risk, so only
        it should move."""
        from src.core.models import ZipTerritory
        from src.services.winback_offers import grant_founder_grace_extension

        locked = ZipTerritory(
            zip_code="99902",
            vertical="roofing",
            county_id="test_founder_grace_county",
            subscriber_id=founder_subscriber.id,
            status="locked",
        )
        fresh_db.add(locked)
        fresh_db.flush()

        grant_founder_grace_extension(founder_subscriber.id, fresh_db)
        fresh_db.flush()
        fresh_db.refresh(locked)
        assert locked.grace_expires_at is None  # untouched — never had a grace window


class TestFullRedemptionFlow:
    """End-to-end: mint at send time, redeem at checkout — credits only land on redemption."""

    def test_zip_released_credits_only_granted_after_redemption_not_at_mint(self, fresh_db, subscriber):
        from src.services.winback_offers import create_or_reuse_offer, redeem_offer, grant_winback_credits
        from sqlalchemy import text

        token = create_or_reuse_offer(subscriber.id, "zip_released", fresh_db)
        # Minting the token (equivalent to the message being sent) must NOT
        # itself grant anything — this is exactly the bug the review found.
        rows_before = fresh_db.execute(
            text("SELECT 1 FROM wallet_transactions WHERE subscriber_id = :sid"),
            {"sid": subscriber.id},
        ).fetchall()
        assert rows_before == []

        redeemed = redeem_offer(token, fresh_db)
        assert redeemed["branch"] == "zip_released"
        grant_winback_credits(redeemed["subscriber_id"], fresh_db)

        rows_after = fresh_db.execute(
            text("SELECT amount FROM wallet_transactions WHERE subscriber_id = :sid"),
            {"sid": subscriber.id},
        ).fetchall()
        assert len(rows_after) == 1
        assert rows_after[0][0] == 5
