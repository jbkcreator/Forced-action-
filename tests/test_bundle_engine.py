"""Tests for bundle_engine.py"""

import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.core.models import BundlePurchase, Subscriber


class TestBundleAvailabilityUnit:
    def test_weekend_friday(self, mock_db):
        from src.services.bundle_engine import is_available
        friday = datetime(2026, 4, 17, tzinfo=timezone.utc)  # known Friday (weekday=4)
        with patch("src.services.bundle_engine.datetime") as dt:
            dt.now.return_value = friday
            result = is_available("weekend", 1, mock_db)
        assert result is True

    def test_weekend_monday(self, mock_db):
        from src.services.bundle_engine import is_available
        monday = datetime(2026, 4, 14, tzinfo=timezone.utc)  # known Monday (weekday=0)
        with patch("src.services.bundle_engine.datetime") as dt:
            dt.now.return_value = monday
            result = is_available("weekend", 1, mock_db)
        assert result is False

    def test_storm_availability_with_redis_key(self, mock_db):
        from src.services.bundle_engine import is_available
        sub = MagicMock()
        mock_db.get.return_value = sub
        with patch("src.services.bundle_engine._get_subscriber_zips", return_value=["33601"]), \
             patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.rget", return_value="1"):
            result = is_available("storm", 1, mock_db)
            assert result is True

    def test_storm_unavailable_without_redis_key(self, mock_db):
        from src.services.bundle_engine import is_available
        sub = MagicMock()
        mock_db.get.return_value = sub
        with patch("src.services.bundle_engine._get_subscriber_zips", return_value=["33601"]), \
             patch("src.core.redis_client.redis_available", return_value=True), \
             patch("src.core.redis_client.rget", return_value=None):
            result = is_available("storm", 1, mock_db)
            assert result is False

    def test_zip_booster_always_available(self, mock_db):
        from src.services.bundle_engine import is_available
        assert is_available("zip_booster", 1, mock_db) is True

    def test_monthly_reload_always_available(self, mock_db):
        from src.services.bundle_engine import is_available
        assert is_available("monthly_reload", 1, mock_db) is True


class TestBundleEngineIntegration:
    def test_create_and_deliver_monthly_reload(self, fresh_db):
        from src.services.bundle_engine import deliver
        from src.services.wallet_engine import get_balance

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_bundle_{uid}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"bundle-uuid-{uid}",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        purchase = BundlePurchase(
            subscriber_id=sub.id,
            bundle_type="monthly_reload",
            stripe_payment_intent_id=f"pi_bundle_{uid}",
            status="pending",
            zip_code="33601",
            vertical="roofing",
        )
        fresh_db.add(purchase)
        fresh_db.flush()

        delivered = deliver(purchase.id, fresh_db)
        assert delivered.status == "active"
        assert delivered.credits_awarded == 30
        assert get_balance(sub.id, fresh_db) >= 30
