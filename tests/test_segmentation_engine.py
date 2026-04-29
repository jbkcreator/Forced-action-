"""Tests for segmentation_engine.py"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock


def _make_sub(status="active", days_old=30, days_inactive=0):
    now = datetime.now(timezone.utc)
    sub = MagicMock()
    sub.id = 1
    sub.status = status
    sub.created_at = now - timedelta(days=days_old)
    sub.updated_at = now - timedelta(days=days_inactive)
    return sub


def _make_wallet(tier="starter_wallet", credits=10):
    wallet = MagicMock()
    wallet.wallet_tier = tier
    wallet.credits_remaining = credits
    return wallet


class TestSegmentLogicUnit:
    def test_churned_status(self):
        from src.services.segmentation_engine import _compute_segment
        sub = _make_sub(status="churned")
        seg, reason = _compute_segment(sub, None, 0)
        assert seg == "churned"

    def test_cancelled_status(self):
        from src.services.segmentation_engine import _compute_segment
        sub = _make_sub(status="cancelled")
        seg, reason = _compute_segment(sub, None, 0)
        assert seg == "churned"

    def test_new_account(self):
        from src.services.segmentation_engine import _compute_segment
        sub = _make_sub(days_old=3)
        seg, reason = _compute_segment(sub, None, 0)
        assert seg == "new"

    def test_at_risk_inactive(self):
        from src.services.segmentation_engine import _compute_segment
        sub = _make_sub(days_old=30, days_inactive=15)
        seg, reason = _compute_segment(sub, None, 15)
        assert seg == "at_risk"

    def test_wallet_active(self):
        from src.services.segmentation_engine import _compute_segment
        sub = _make_sub(days_old=30, days_inactive=1)
        wallet = _make_wallet(credits=10)
        seg, reason = _compute_segment(sub, wallet, 1)
        assert seg == "wallet_active"

    def test_browsing_fallback(self):
        from src.services.segmentation_engine import _compute_segment
        sub = _make_sub(days_old=30, days_inactive=1)
        seg, reason = _compute_segment(sub, None, 1)
        assert seg == "browsing"

    def test_churn_takes_precedence_over_new(self):
        from src.services.segmentation_engine import _compute_segment
        sub = _make_sub(status="churned", days_old=2)
        seg, _ = _compute_segment(sub, None, 0)
        assert seg == "churned"


class TestSegmentIntegration:
    def test_classify_subscriber(self, fresh_db):
        from src.core.models import Subscriber
        from src.services.segmentation_engine import classify

        sub = Subscriber(
            stripe_customer_id="cus_seg_test_1",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="seg-test-uuid-1",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        segment = classify(sub.id, fresh_db)
        assert segment in ("new", "browsing", "wallet_active", "at_risk", "churned", "engaged", "high_intent", "lock_candidate")
