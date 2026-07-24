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
        grant_winback_credits(subscriber.id, fresh_db)
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
