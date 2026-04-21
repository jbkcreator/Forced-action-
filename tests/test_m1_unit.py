"""
M1 unit tests — no live DB, no Stripe, no GHL required.

Run with:
    pytest tests/test_m1_unit.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call
import pytest

# Pre-import service modules so patch() can resolve them
import src.services.stripe_service       # noqa: F401
import src.services.stripe_webhooks      # noqa: F401
import src.tasks.grace_expiry            # noqa: F401


# ---------------------------------------------------------------------------
# Helpers — build lightweight mock model instances
# ---------------------------------------------------------------------------

def _make_founding_count(tier="starter", vertical="roofing", county_id="hillsborough", count=0):
    obj = MagicMock()
    obj.tier = tier
    obj.vertical = vertical
    obj.county_id = county_id
    obj.count = count
    return obj


def _make_subscriber(
    id=1,
    stripe_customer_id="cus_test",
    stripe_subscription_id="sub_test",
    tier="starter",
    vertical="roofing",
    county_id="hillsborough",
    founding_member=False,
    founding_price_id=None,
    rate_locked_at=None,
    status="active",
    grace_expires_at=None,
    ghl_contact_id=None,
    event_feed_uuid=None,
    email="test@example.com",
    name="Test User",
    ghl_stage=None,
):
    obj = MagicMock()
    obj.id = id
    obj.stripe_customer_id = stripe_customer_id
    obj.stripe_subscription_id = stripe_subscription_id
    obj.tier = tier
    obj.vertical = vertical
    obj.county_id = county_id
    obj.founding_member = founding_member
    obj.founding_price_id = founding_price_id
    obj.rate_locked_at = rate_locked_at
    obj.status = status
    obj.grace_expires_at = grace_expires_at
    obj.ghl_contact_id = ghl_contact_id
    obj.event_feed_uuid = event_feed_uuid
    obj.email = email
    obj.name = name
    obj.ghl_stage = ghl_stage
    return obj


def _make_territory(
    id=1,
    zip_code="33601",
    vertical="roofing",
    county_id="hillsborough",
    subscriber_id=None,
    status="available",
    locked_at=None,
    grace_expires_at=None,
    waitlist_emails=None,
):
    obj = MagicMock()
    obj.id = id
    obj.zip_code = zip_code
    obj.vertical = vertical
    obj.county_id = county_id
    obj.subscriber_id = subscriber_id
    obj.status = status
    obj.locked_at = locked_at
    obj.grace_expires_at = grace_expires_at
    obj.waitlist_emails = waitlist_emails or []
    return obj


# ---------------------------------------------------------------------------
# 1. stripe_service — get_price_id_for_checkout
# ---------------------------------------------------------------------------

class TestGetPriceIdForCheckout:

    def _run(self, db, tier="starter", vertical="roofing", county_id="hillsborough"):
        from src.services.stripe_service import get_price_id_for_checkout
        return get_price_id_for_checkout(db, tier, vertical, county_id)

    def _mock_prices(self, mock_settings, founding="price_founding_123", regular="price_regular_456"):
        """Wire mock_settings.active_stripe_price() to return correct IDs by name."""
        price_map = {"starter_founding": founding, "starter_regular": regular}
        mock_settings.active_stripe_price.side_effect = lambda name: price_map.get(name)

    @patch("src.services.stripe_service._founding_limit", return_value=10)
    @patch("src.services.stripe_service.settings")
    def test_founding_when_count_is_zero(self, mock_settings, _limit):
        """count=0 → founding price, is_founding=True"""
        self._mock_prices(mock_settings)

        from src.services.stripe_service import get_price_id_for_checkout
        row = _make_founding_count(count=0)
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = row

        price_id, is_founding = get_price_id_for_checkout(db, "starter", "roofing", "hillsborough")

        assert is_founding is True
        assert price_id == "price_founding_123"

    @patch("src.services.stripe_service._founding_limit", return_value=10)
    @patch("src.services.stripe_service.settings")
    def test_founding_when_count_is_9(self, mock_settings, _limit):
        """count=9 → last founding spot, is_founding=True"""
        self._mock_prices(mock_settings)

        from src.services.stripe_service import get_price_id_for_checkout
        row = _make_founding_count(count=9)
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = row

        price_id, is_founding = get_price_id_for_checkout(db, "starter", "roofing", "hillsborough")

        assert is_founding is True
        assert price_id == "price_founding_123"

    @patch("src.services.stripe_service._founding_limit", return_value=10)
    @patch("src.services.stripe_service.settings")
    def test_regular_when_count_is_10(self, mock_settings, _limit):
        """count=10 → founding slots full, is_founding=False"""
        self._mock_prices(mock_settings)

        from src.services.stripe_service import get_price_id_for_checkout
        row = _make_founding_count(count=10)
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = row

        price_id, is_founding = get_price_id_for_checkout(db, "starter", "roofing", "hillsborough")

        assert is_founding is False
        assert price_id == "price_regular_456"

    @patch("src.services.stripe_service._founding_limit", return_value=10)
    @patch("src.services.stripe_service.settings")
    def test_creates_row_when_none_exists(self, mock_settings, _limit):
        """No existing row → creates FoundingSubscriberCount with count=0"""
        self._mock_prices(mock_settings)

        from src.services.stripe_service import get_price_id_for_checkout
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        price_id, is_founding = get_price_id_for_checkout(db, "starter", "roofing", "hillsborough")

        assert is_founding is True
        db.add.assert_called_once()
        db.flush.assert_called_once()

    @patch("src.services.stripe_service._founding_limit", return_value=10)
    @patch("src.services.stripe_service.settings")
    def test_raises_when_price_id_not_configured(self, mock_settings, _limit):
        """Missing price_id → ValueError"""
        self._mock_prices(mock_settings, founding=None, regular=None)

        from src.services.stripe_service import get_price_id_for_checkout
        row = _make_founding_count(count=0)
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = row

        with pytest.raises(ValueError, match="not configured"):
            get_price_id_for_checkout(db, "starter", "roofing", "hillsborough")


# ---------------------------------------------------------------------------
# 2. stripe_webhooks — _on_checkout_completed
# ---------------------------------------------------------------------------

class TestOnCheckoutCompleted:

    def _session_data(self, is_founding=True, zip_codes="33601,33602", payment_status="paid"):
        return {
            "customer": "cus_test",
            "subscription": "sub_test",
            "payment_status": payment_status,
            "customer_details": {"email": "buyer@example.com", "name": "Jane Buyer"},
            "metadata": {
                "tier": "starter",
                "vertical": "roofing",
                "county_id": "hillsborough",
                "zip_codes": zip_codes,
                "is_founding": str(is_founding),
                "founding_price_id": "price_founding_123" if is_founding else "",
            },
        }

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_creates_new_subscriber(self, mock_ghl):
        from src.services.stripe_webhooks import _on_checkout_completed

        db = MagicMock()
        # No existing subscriber, no founding row, no existing territories
        db.execute.return_value.scalar_one_or_none.return_value = None
        db.execute.return_value.scalars.return_value.all.return_value = []

        _on_checkout_completed(self._session_data(is_founding=True), db)

        db.add.assert_called()  # Subscriber was added
        db.flush.assert_called()

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_founding_count_incremented(self, mock_ghl):
        from src.services.stripe_webhooks import _on_checkout_completed

        founding_row = _make_founding_count(count=3)
        subscriber = _make_subscriber()

        db = MagicMock()
        # founding count → subscriber by stripe_id → subscriber by email → territory × 2
        db.execute.return_value.scalar_one_or_none.side_effect = [
            founding_row,  # founding count row
            None,          # no existing subscriber by stripe_customer_id
            None,          # no existing subscriber by email → create new
            None,          # territory 1 → create new
            None,          # territory 2 → create new
        ]

        _on_checkout_completed(self._session_data(is_founding=True), db)

        assert founding_row.count == 4

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_zip_territories_locked(self, mock_ghl):
        from src.services.stripe_webhooks import _on_checkout_completed

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        _on_checkout_completed(self._session_data(zip_codes="33601,33602"), db)

        # db.add called at least twice (subscriber + 2 territories)
        assert db.add.call_count >= 3

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_existing_available_territory_gets_locked(self, mock_ghl):
        from src.services.stripe_webhooks import _on_checkout_completed

        territory = _make_territory(status="available")
        subscriber = _make_subscriber(id=99)

        db = MagicMock()
        # founding row → subscriber by stripe_id → subscriber by email → territory
        db.execute.return_value.scalar_one_or_none.side_effect = [
            None,        # no founding row
            None,        # no existing subscriber by stripe_customer_id
            None,        # no existing subscriber by email → create new
            territory,   # existing available territory
        ]

        _on_checkout_completed(self._session_data(zip_codes="33601"), db)

        assert territory.status == "locked"
        assert territory.locked_at is not None

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_ghl_stage_5_pushed(self, mock_ghl):
        from src.services.stripe_webhooks import _on_checkout_completed

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        _on_checkout_completed(self._session_data(), db)

        mock_ghl.assert_called_once()
        _, kwargs = mock_ghl.call_args
        assert mock_ghl.call_args[1].get("stage") == 5 or mock_ghl.call_args[0][1] == 5

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_missing_metadata_exits_early(self, mock_ghl):
        from src.services.stripe_webhooks import _on_checkout_completed

        db = MagicMock()
        bad_session = {"customer": "cus_test", "metadata": {}}  # missing tier/vertical/county_id

        _on_checkout_completed(bad_session, db)

        db.add.assert_not_called()
        mock_ghl.assert_not_called()

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_failed_payment_creates_churned_subscriber(self, mock_ghl):
        """payment_status='unpaid' → churned subscriber at GHL stage 7, no ZIP lock."""
        from src.services.stripe_webhooks import _on_checkout_completed

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        _on_checkout_completed(self._session_data(payment_status="unpaid"), db)

        db.add.assert_called_once()
        db.flush.assert_called()

        added_obj = db.add.call_args[0][0]
        assert added_obj.status == "churned"
        assert added_obj.ghl_stage == 7
        assert added_obj.founding_member is False

        mock_ghl.assert_called_once()
        call_args = mock_ghl.call_args
        assert call_args[1].get("stage") == 7 or call_args[0][1] == 7
        tags = call_args[1].get("tags") or (call_args[0][2] if len(call_args[0]) > 2 else [])
        assert "checkout_payment_failed" in tags

        assert db.add.call_count == 1  # no territory rows


# ---------------------------------------------------------------------------
# 3. stripe_webhooks — _on_subscription_deleted
# ---------------------------------------------------------------------------

class TestOnSubscriptionDeleted:

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_subscriber_set_to_grace(self, mock_ghl):
        from src.services.stripe_webhooks import _on_subscription_deleted

        subscriber = _make_subscriber(status="active")
        territory = _make_territory(status="locked", subscriber_id=1)

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber
        db.execute.return_value.scalars.return_value.all.return_value = [territory]

        _on_subscription_deleted({"customer": "cus_test"}, db)

        assert subscriber.status == "grace"
        assert subscriber.ghl_stage == 7
        assert subscriber.grace_expires_at is not None

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_grace_expires_in_48_hours(self, mock_ghl):
        from src.services.stripe_webhooks import _on_subscription_deleted

        subscriber = _make_subscriber(status="active")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber
        db.execute.return_value.scalars.return_value.all.return_value = []

        before = datetime.now(timezone.utc)
        _on_subscription_deleted({"customer": "cus_test"}, db)
        after = datetime.now(timezone.utc)

        grace = subscriber.grace_expires_at
        assert before + timedelta(hours=47) < grace < after + timedelta(hours=49)

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_zip_territories_set_to_grace(self, mock_ghl):
        from src.services.stripe_webhooks import _on_subscription_deleted

        subscriber = _make_subscriber(status="active")
        t1 = _make_territory(status="locked")
        t2 = _make_territory(status="locked", zip_code="33602")

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber
        db.execute.return_value.scalars.return_value.all.return_value = [t1, t2]

        _on_subscription_deleted({"customer": "cus_test"}, db)

        assert t1.status == "grace"
        assert t2.status == "grace"
        assert t1.grace_expires_at is not None
        assert t2.grace_expires_at is not None

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_churned_founding_tag(self, mock_ghl):
        from src.services.stripe_webhooks import _on_subscription_deleted

        subscriber = _make_subscriber(founding_member=True)
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber
        db.execute.return_value.scalars.return_value.all.return_value = []

        _on_subscription_deleted({"customer": "cus_test"}, db)

        mock_ghl.assert_called_once()
        tags = mock_ghl.call_args[1].get("tags") or mock_ghl.call_args[0][2]
        assert "churned_founding" in tags

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_churned_regular_tag(self, mock_ghl):
        from src.services.stripe_webhooks import _on_subscription_deleted

        subscriber = _make_subscriber(founding_member=False)
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber
        db.execute.return_value.scalars.return_value.all.return_value = []

        _on_subscription_deleted({"customer": "cus_test"}, db)

        tags = mock_ghl.call_args[1].get("tags") or mock_ghl.call_args[0][2]
        assert "churned_regular" in tags

    @patch("src.services.stripe_webhooks.push_subscriber_to_ghl")
    def test_no_subscriber_found_exits_gracefully(self, mock_ghl):
        from src.services.stripe_webhooks import _on_subscription_deleted

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        _on_subscription_deleted({"customer": "cus_missing"}, db)

        mock_ghl.assert_not_called()


# ---------------------------------------------------------------------------
# 4. grace_expiry — expire_zip_grace_periods
# ---------------------------------------------------------------------------

class TestExpireZipGracePeriods:

    def test_expired_territory_released(self):
        from src.tasks.grace_expiry import expire_zip_grace_periods

        now = datetime.now(timezone.utc)
        expired = _make_territory(
            status="grace",
            grace_expires_at=now - timedelta(minutes=1),
            subscriber_id=42,
            waitlist_emails=[],
        )

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [expired]

        count = expire_zip_grace_periods(db)

        assert count == 1
        assert expired.status == "available"
        assert expired.subscriber_id is None
        assert expired.locked_at is None
        assert expired.grace_expires_at is None

    def test_not_yet_expired_territory_untouched(self):
        from src.tasks.grace_expiry import expire_zip_grace_periods

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []

        count = expire_zip_grace_periods(db)

        assert count == 0

    @patch("src.tasks.grace_expiry._send_waitlist_email")
    def test_waitlist_emails_fired(self, mock_send):
        from src.tasks.grace_expiry import expire_zip_grace_periods

        now = datetime.now(timezone.utc)
        expired = _make_territory(
            status="grace",
            grace_expires_at=now - timedelta(hours=1),
            waitlist_emails=["a@test.com", "b@test.com"],
        )

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [expired]

        expire_zip_grace_periods(db)

        mock_send.assert_called_once_with(
            expired.zip_code,
            expired.vertical,
            expired.county_id,
            ["a@test.com", "b@test.com"],
        )

    @patch("src.tasks.grace_expiry._send_waitlist_email")
    def test_no_email_when_waitlist_empty(self, mock_send):
        from src.tasks.grace_expiry import expire_zip_grace_periods

        now = datetime.now(timezone.utc)
        expired = _make_territory(
            status="grace",
            grace_expires_at=now - timedelta(hours=1),
            waitlist_emails=[],
        )

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [expired]

        expire_zip_grace_periods(db)

        mock_send.assert_not_called()

    def test_multiple_territories_all_released(self):
        from src.tasks.grace_expiry import expire_zip_grace_periods

        now = datetime.now(timezone.utc)
        t1 = _make_territory(zip_code="33601", status="grace", grace_expires_at=now - timedelta(hours=1))
        t2 = _make_territory(zip_code="33602", status="grace", grace_expires_at=now - timedelta(hours=2))
        t3 = _make_territory(zip_code="33603", status="grace", grace_expires_at=now - timedelta(hours=3))

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [t1, t2, t3]

        count = expire_zip_grace_periods(db)

        assert count == 3
        for t in [t1, t2, t3]:
            assert t.status == "available"


# ---------------------------------------------------------------------------
# 5. grace_expiry — expire_subscriber_grace_periods
# ---------------------------------------------------------------------------

class TestExpireSubscriberGracePeriods:

    def test_expired_subscriber_churned(self):
        from src.tasks.grace_expiry import expire_subscriber_grace_periods

        now = datetime.now(timezone.utc)
        sub = _make_subscriber(
            status="grace",
            grace_expires_at=now - timedelta(minutes=5),
        )

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [sub]

        count = expire_subscriber_grace_periods(db)

        assert count == 1
        assert sub.status == "churned"

    def test_no_expired_subscribers(self):
        from src.tasks.grace_expiry import expire_subscriber_grace_periods

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []

        count = expire_subscriber_grace_periods(db)

        assert count == 0

    def test_multiple_subscribers_all_churned(self):
        from src.tasks.grace_expiry import expire_subscriber_grace_periods

        now = datetime.now(timezone.utc)
        subs = [
            _make_subscriber(id=i, status="grace", grace_expires_at=now - timedelta(hours=i))
            for i in range(1, 4)
        ]

        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = subs

        count = expire_subscriber_grace_periods(db)

        assert count == 3
        for s in subs:
            assert s.status == "churned"


# ---------------------------------------------------------------------------
# 6. stripe_webhooks — _on_payment_succeeded
# ---------------------------------------------------------------------------

class TestOnPaymentSucceeded:

    def test_billing_date_updated(self):
        from src.services.stripe_webhooks import _on_payment_succeeded

        subscriber = _make_subscriber()
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber

        invoice = {
            "customer": "cus_test",
            "lines": {"data": [{"period": {"end": 1800000000}}]},
        }

        _on_payment_succeeded(invoice, db)

        assert subscriber.billing_date is not None

    def test_no_subscriber_exits_gracefully(self):
        from src.services.stripe_webhooks import _on_payment_succeeded

        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = None

        # Should not raise
        _on_payment_succeeded({"customer": "cus_missing"}, db)


# ---------------------------------------------------------------------------
# 7. stripe_webhooks — _on_subscription_updated
# ---------------------------------------------------------------------------

class TestOnSubscriptionUpdated:

    def test_active_stripe_status_maps_to_active(self):
        from src.services.stripe_webhooks import _on_subscription_updated

        subscriber = _make_subscriber(status="active")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber

        _on_subscription_updated({"customer": "cus_test", "status": "active", "id": "sub_new"}, db)

        assert subscriber.status == "active"

    def test_unpaid_stripe_status_maps_to_churned(self):
        from src.services.stripe_webhooks import _on_subscription_updated

        subscriber = _make_subscriber(status="active")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber

        _on_subscription_updated({"customer": "cus_test", "status": "unpaid", "id": "sub_new"}, db)

        assert subscriber.status == "churned"

    def test_canceled_stripe_status_maps_to_cancelled(self):
        from src.services.stripe_webhooks import _on_subscription_updated

        subscriber = _make_subscriber(status="active")
        db = MagicMock()
        db.execute.return_value.scalar_one_or_none.return_value = subscriber

        _on_subscription_updated({"customer": "cus_test", "status": "canceled", "id": "sub_new"}, db)

        assert subscriber.status == "cancelled"
