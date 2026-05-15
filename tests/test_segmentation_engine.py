"""Tests for segmentation_engine.py"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock


def _make_sub(status="active", days_old=30, lock_zip=None, tier="starter"):
    now = datetime.now(timezone.utc)
    sub = MagicMock()
    sub.id = 1
    sub.status = status
    sub.created_at = now - timedelta(days=days_old)
    sub.tier = tier
    sub.lock_candidate_zip = lock_zip
    return sub


def _make_wallet(tier="starter_wallet", credits=10):
    wallet = MagicMock()
    wallet.wallet_tier = tier
    wallet.credits_remaining = credits
    return wallet


def _seg(sub, wallet=None, rss=0, last_activity=None, engaged=False):
    from src.services.segmentation_engine import _compute_segment
    return _compute_segment(sub, wallet, rss, last_activity, engaged)


class TestSegmentLogicUnit:
    def test_churned_status(self):
        sub = _make_sub(status="churned")
        seg, reason = _seg(sub)
        assert seg == "churned"
        assert "churned:status=churned" == reason

    def test_cancelled_status(self):
        sub = _make_sub(status="cancelled")
        seg, reason = _seg(sub)
        assert seg == "churned"
        assert "churned:status=cancelled" == reason

    def test_at_risk_inactive(self):
        now = datetime.now(timezone.utc)
        sub = _make_sub(days_old=30)
        last = now - timedelta(days=15)
        seg, reason = _seg(sub, last_activity=last)
        assert seg == "at_risk"
        assert "at_risk:inactive=15d" == reason

    def test_lock_candidate_when_zip_set(self):
        sub = _make_sub(lock_zip="33647", tier="starter")
        seg, reason = _seg(sub)
        assert seg == "lock_candidate"
        assert "lock_candidate:zip=33647" == reason

    def test_lock_candidate_skipped_when_tier_locked(self):
        from config.wallet_to_lock import LOCK_OR_ABOVE_TIERS
        locked_tier = next(iter(LOCK_OR_ABOVE_TIERS))
        sub = _make_sub(lock_zip="33647", tier=locked_tier)
        seg, _ = _seg(sub)
        assert seg != "lock_candidate"

    def test_high_intent_when_rss_70(self):
        sub = _make_sub()
        seg, reason = _seg(sub, rss=70)
        assert seg == "high_intent"
        assert "high_intent:rss=70" == reason

    def test_high_intent_skipped_when_rss_69(self):
        sub = _make_sub()
        seg, _ = _seg(sub, rss=69)
        assert seg != "high_intent"

    def test_wallet_active(self):
        sub = _make_sub()
        wallet = _make_wallet(credits=10)
        seg, reason = _seg(sub, wallet=wallet)
        assert seg == "wallet_active"
        assert "wallet_active:tier=starter_wallet" == reason

    def test_new_account_no_wallet_no_engagement(self):
        sub = _make_sub(days_old=3)
        seg, reason = _seg(sub)
        assert seg == "new"
        assert "new:account_age=3d" == reason

    def test_new_requires_no_wallet_and_no_engagement(self):
        sub = _make_sub(days_old=3)
        wallet = _make_wallet(credits=5)
        seg, _ = _seg(sub, wallet=wallet)
        assert seg != "new"

    def test_engaged_when_recent_reply(self):
        sub = _make_sub(days_old=30)
        seg, reason = _seg(sub, engaged=True)
        assert seg == "engaged"
        assert "engaged:recent_msg_activity" == reason

    def test_engaged_skipped_when_no_signals(self):
        sub = _make_sub(days_old=30)
        seg, _ = _seg(sub, engaged=False)
        assert seg == "browsing"

    def test_browsing_fallback_when_old_account_no_signals(self):
        sub = _make_sub(days_old=30)
        seg, reason = _seg(sub)
        assert seg == "browsing"
        assert "browsing:no_signals" == reason

    # ── Adjacent-pair precedence tests ────────────────────────────────────────

    def test_churned_beats_at_risk(self):
        now = datetime.now(timezone.utc)
        sub = _make_sub(status="churned", days_old=30)
        last = now - timedelta(days=20)
        seg, _ = _seg(sub, last_activity=last)
        assert seg == "churned"

    def test_at_risk_beats_lock_candidate(self):
        now = datetime.now(timezone.utc)
        sub = _make_sub(lock_zip="33647", tier="starter")
        last = now - timedelta(days=20)
        seg, _ = _seg(sub, last_activity=last)
        assert seg == "at_risk"

    def test_lock_candidate_beats_high_intent(self):
        sub = _make_sub(lock_zip="33647", tier="starter")
        seg, _ = _seg(sub, rss=80)
        assert seg == "lock_candidate"

    def test_high_intent_beats_wallet_active(self):
        sub = _make_sub()
        wallet = _make_wallet(credits=10)
        seg, _ = _seg(sub, wallet=wallet, rss=75)
        assert seg == "high_intent"

    def test_wallet_active_beats_new(self):
        sub = _make_sub(days_old=3)
        wallet = _make_wallet(credits=10)
        seg, _ = _seg(sub, wallet=wallet)
        assert seg == "wallet_active"

    def test_new_beats_engaged(self):
        sub = _make_sub(days_old=3)
        # new requires no engagement — to test precedence swap, use engaged=False
        seg, _ = _seg(sub, engaged=False)
        assert seg == "new"

    def test_engaged_beats_browsing(self):
        sub = _make_sub(days_old=30)
        seg, _ = _seg(sub, engaged=True)
        assert seg == "engaged"

    def test_churn_takes_precedence_over_new(self):
        sub = _make_sub(status="churned", days_old=2)
        seg, _ = _seg(sub)
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

    def test_classify_marks_lock_candidate(self, fresh_db):
        from src.core.models import Subscriber, UserSegment
        from src.services.segmentation_engine import classify

        sub = Subscriber(
            stripe_customer_id="cus_seg_lc_1",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="seg-lc-uuid-1",
            lock_candidate_zip="33647",
        )
        # 10 days old: old enough to skip "new" (>7d), recent enough to skip "at_risk" (<14d)
        from datetime import timezone
        sub.created_at = datetime.now(timezone.utc) - timedelta(days=10)
        fresh_db.add(sub)
        fresh_db.flush()

        segment = classify(sub.id, fresh_db)
        assert segment == "lock_candidate"

    def test_classify_marks_high_intent(self, fresh_db):
        from src.core.models import Subscriber, UserSegment
        from src.services.segmentation_engine import classify

        sub = Subscriber(
            stripe_customer_id="cus_seg_hi_1",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="seg-hi-uuid-1",
        )
        # 10 days old: skips "new" (>7d) and "at_risk" (<14d) so rss=72 reaches high_intent
        sub.created_at = datetime.now(timezone.utc) - timedelta(days=10)
        fresh_db.add(sub)
        fresh_db.flush()

        seg_row = UserSegment(subscriber_id=sub.id, revenue_signal_score=72, segment="browsing")
        fresh_db.add(seg_row)
        fresh_db.flush()

        segment = classify(sub.id, fresh_db)
        assert segment == "high_intent"

    def test_classify_reason_format(self, fresh_db):
        from src.core.models import Subscriber, UserSegment
        from src.services.segmentation_engine import classify, upsert_segment

        sub = Subscriber(
            stripe_customer_id="cus_seg_fmt_1",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="seg-fmt-uuid-1",
            lock_candidate_zip="33601",
        )
        sub.created_at = datetime.now(timezone.utc) - timedelta(days=30)
        fresh_db.add(sub)
        fresh_db.flush()

        classify(sub.id, fresh_db)

        seg_row = fresh_db.execute(
            __import__("sqlalchemy").select(UserSegment).where(UserSegment.subscriber_id == sub.id)
        ).scalar_one()
        # reason must follow {bucket}:{signal}={value} pattern
        import re
        assert re.match(r"^\w+:\w+=.+$", seg_row.classification_reason), seg_row.classification_reason
