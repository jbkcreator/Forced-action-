"""Tests for referral_engine.py"""

import pytest
from unittest.mock import MagicMock, patch

from src.core.models import Subscriber


def _make_sub(sub_id, referral_code=None):
    sub = MagicMock()
    sub.id = sub_id
    sub.referral_code = referral_code
    return sub


class TestReferralUnit:
    def test_ensure_referral_code_generates_unique(self, mock_db):
        from src.services.referral_engine import ensure_referral_code
        sub = _make_sub(42, referral_code=None)
        mock_db.get.return_value = sub
        mock_db.flush = MagicMock()

        code = ensure_referral_code(42, mock_db)
        assert len(code) == 8
        assert code.isalnum()

    def test_ensure_referral_code_idempotent(self, mock_db):
        from src.services.referral_engine import ensure_referral_code
        sub = _make_sub(42, referral_code="0000002a")
        mock_db.get.return_value = sub
        code = ensure_referral_code(42, mock_db)
        assert code == "0000002a"

    def test_cannot_refer_yourself(self, mock_db):
        from src.services.referral_engine import process_signup
        referrer = _make_sub(1, referral_code="testcode1")
        mock_db.execute.return_value.scalar_one_or_none.return_value = referrer
        result = process_signup(1, "testcode1", mock_db)  # referee_id == referrer_id
        assert result is None

    def test_referrer_credit_amount(self):
        from src.services.referral_engine import REFERRER_CREDIT, REFEREE_CREDIT
        assert REFERRER_CREDIT == 20
        assert REFEREE_CREDIT == 10

    def test_invalid_referral_code_returns_none(self, mock_db):
        from src.services.referral_engine import process_signup
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = process_signup(5, "badcode00", mock_db)
        assert result is None


class TestReferralIntegration:
    def test_full_referral_flow(self, fresh_db):
        import uuid
        from src.services.referral_engine import (
            confirm_purchase,
            ensure_referral_code,
            process_signup,
            reward_referrer,
        )
        from src.services.wallet_engine import get_balance

        uid = str(uuid.uuid4())[:8]
        referrer = Subscriber(
            stripe_customer_id=f"cus_referrer_{uid}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"ref-uuid-r-{uid}",
        )
        referee = Subscriber(
            stripe_customer_id=f"cus_referee_{uid}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"ref-uuid-e-{uid}",
        )
        fresh_db.add(referrer)
        fresh_db.add(referee)
        fresh_db.flush()

        code = ensure_referral_code(referrer.id, fresh_db)
        assert code

        event = process_signup(referee.id, code, fresh_db)
        assert event is not None
        assert event.status == "pending"

        confirmed = confirm_purchase(referee.id, fresh_db)
        assert confirmed.status == "confirmed"

        with patch("src.services.referral_engine._notify_referrer"):
            rewarded = reward_referrer(confirmed.id, fresh_db)
        assert rewarded.status == "rewarded"
        assert get_balance(referrer.id, fresh_db) >= 20
