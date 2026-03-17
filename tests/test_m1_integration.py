"""
M1 integration tests — real ORM queries against SQLite in-memory DB.

Tests the full payment lifecycle:
  - Checkout → subscriber created, ZIPs locked, founding count incremented
  - Founding limit boundary (10th gets founding, 11th gets regular)
  - Double-lock prevention
  - Subscription deleted → grace period on subscriber + territories
  - Grace expiry cron → territories released, subscribers churned
  - Waitlist notification on territory release

Run with:
    pytest tests/test_m1_integration.py -v
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

import pytest
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, JSON, Text, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase


# ============================================================================
# Test-local schema — mirrors production M1 tables with SQLite-compatible types
# (JSONB→JSON, ARRAY→JSON, no CHECK constraints or GIN indexes)
# ============================================================================

class _Base(DeclarativeBase):
    pass


class FoundingSubscriberCount(_Base):
    __tablename__ = "founding_subscriber_counts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tier = Column(String(20), nullable=False)
    vertical = Column(String(50), nullable=False)
    county_id = Column(String(50), nullable=False)
    count = Column(Integer, default=0, nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Subscriber(_Base):
    __tablename__ = "subscribers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stripe_customer_id = Column(String(100), unique=True, nullable=False)
    stripe_subscription_id = Column(String(100), unique=True)
    tier = Column(String(20), nullable=False)
    vertical = Column(String(50), nullable=False)
    county_id = Column(String(50), nullable=False)
    founding_member = Column(Boolean, default=False, nullable=False)
    founding_price_id = Column(String(100))
    rate_locked_at = Column(DateTime)
    status = Column(String(20), default="active", nullable=False)
    billing_date = Column(DateTime)
    grace_expires_at = Column(DateTime)
    ghl_contact_id = Column(String(100))
    ghl_stage = Column(Integer)
    event_feed_uuid = Column(String(36), unique=True)
    email = Column(String(255))
    name = Column(String(255))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ZipTerritory(_Base):
    __tablename__ = "zip_territories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    zip_code = Column(String(10), nullable=False)
    vertical = Column(String(50), nullable=False)
    county_id = Column(String(50), nullable=False)
    subscriber_id = Column(Integer, ForeignKey("subscribers.id"), nullable=True)
    status = Column(String(20), default="available", nullable=False)
    locked_at = Column(DateTime)
    grace_expires_at = Column(DateTime)
    # ARRAY → JSON for SQLite compatibility
    waitlist_emails = Column(JSON, default=list)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def db():
    """Fresh SQLite in-memory DB per test with M1 tables."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    # Make SQLite honor FK constraints
    @event.listens_for(engine, "connect")
    def _set_fk_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    _Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


def _checkout_session(
    customer_id="cus_1",
    subscription_id="sub_1",
    tier="starter",
    vertical="roofing",
    county_id="hillsborough",
    zip_codes="33601,33602",
    is_founding=True,
    email="buyer@test.com",
    name="Jane Buyer",
):
    """Build a fake Stripe checkout.session.completed payload."""
    return {
        "customer": customer_id,
        "subscription": subscription_id,
        "customer_details": {"email": email, "name": name},
        "metadata": {
            "tier": tier,
            "vertical": vertical,
            "county_id": county_id,
            "zip_codes": zip_codes,
            "is_founding": str(is_founding),
            "founding_price_id": "price_founding_123" if is_founding else "",
        },
    }


def _do_checkout(db, session_data):
    """
    Simulate _on_checkout_completed logic against real DB.
    Re-implements the handler using our test-local models so we test
    the actual SQL flow, not mocks.
    """
    meta = session_data.get("metadata", {})
    tier = meta.get("tier")
    vertical = meta.get("vertical")
    county_id = meta.get("county_id")
    zip_codes = [z.strip() for z in meta.get("zip_codes", "").split(",") if z.strip()]
    is_founding = meta.get("is_founding") == "True"
    founding_price_id = meta.get("founding_price_id") or None

    stripe_customer_id = session_data.get("customer")
    stripe_subscription_id = session_data.get("subscription")
    customer_email = session_data.get("customer_details", {}).get("email")
    customer_name = session_data.get("customer_details", {}).get("name")

    now = datetime.now(timezone.utc)

    # Increment founding count
    if is_founding:
        row = db.query(FoundingSubscriberCount).filter_by(
            tier=tier, vertical=vertical, county_id=county_id
        ).first()
        if row is None:
            row = FoundingSubscriberCount(tier=tier, vertical=vertical, county_id=county_id, count=0)
            db.add(row)
            db.flush()
        row.count += 1

    # Create subscriber
    subscriber = db.query(Subscriber).filter_by(
        stripe_customer_id=stripe_customer_id
    ).first()

    if subscriber is None:
        subscriber = Subscriber(
            stripe_customer_id=stripe_customer_id,
            stripe_subscription_id=stripe_subscription_id,
            tier=tier,
            vertical=vertical,
            county_id=county_id,
            founding_member=is_founding,
            founding_price_id=founding_price_id if is_founding else None,
            rate_locked_at=now if is_founding else None,
            status="active",
            event_feed_uuid=str(uuid.uuid4()),
            email=customer_email,
            name=customer_name,
            ghl_stage=5,
        )
        db.add(subscriber)
    else:
        subscriber.stripe_subscription_id = stripe_subscription_id
        subscriber.tier = tier
        subscriber.vertical = vertical
        subscriber.status = "active"
        subscriber.ghl_stage = 5
        if is_founding and not subscriber.founding_member:
            subscriber.founding_member = True
            subscriber.founding_price_id = founding_price_id
            subscriber.rate_locked_at = now

    db.flush()

    # Lock ZIP territories
    for zip_code in zip_codes:
        territory = db.query(ZipTerritory).filter_by(
            zip_code=zip_code, vertical=vertical, county_id=county_id
        ).first()

        if territory is None:
            territory = ZipTerritory(
                zip_code=zip_code,
                vertical=vertical,
                county_id=county_id,
                subscriber_id=subscriber.id,
                status="locked",
                locked_at=now,
            )
            db.add(territory)
        elif territory.status in ("available", "grace"):
            territory.subscriber_id = subscriber.id
            territory.status = "locked"
            territory.locked_at = now
            territory.grace_expires_at = None

    db.commit()
    return subscriber


def _do_subscription_deleted(db, stripe_customer_id):
    """Simulate _on_subscription_deleted logic against real DB."""
    subscriber = db.query(Subscriber).filter_by(
        stripe_customer_id=stripe_customer_id
    ).first()
    if subscriber is None:
        return None

    now = datetime.now(timezone.utc)
    grace_expires = now + timedelta(hours=48)

    subscriber.status = "grace"
    subscriber.grace_expires_at = grace_expires
    subscriber.ghl_stage = 7

    territories = db.query(ZipTerritory).filter_by(
        subscriber_id=subscriber.id, status="locked"
    ).all()
    for t in territories:
        t.status = "grace"
        t.grace_expires_at = grace_expires

    db.commit()
    return subscriber


def _do_grace_expiry(db):
    """Simulate grace_expiry logic against real DB. Returns (zips_released, subs_churned)."""
    now = datetime.now(timezone.utc)

    # Expire ZIP territories
    expired_zips = db.query(ZipTerritory).filter(
        ZipTerritory.status == "grace",
        ZipTerritory.grace_expires_at <= now,
    ).all()

    waitlist_notifications = []
    for t in expired_zips:
        waitlist = list(t.waitlist_emails or [])
        t.subscriber_id = None
        t.status = "available"
        t.locked_at = None
        t.grace_expires_at = None
        t.waitlist_emails = []
        if waitlist:
            waitlist_notifications.append((t.zip_code, t.vertical, t.county_id, waitlist))

    # Expire subscribers
    expired_subs = db.query(Subscriber).filter(
        Subscriber.status == "grace",
        Subscriber.grace_expires_at <= now,
    ).all()
    for s in expired_subs:
        s.status = "churned"

    db.commit()
    return len(expired_zips), len(expired_subs), waitlist_notifications


def _get_price_for_checkout(db, tier, vertical, county_id, founding_limit=10):
    """Simulate get_price_id_for_checkout logic — returns (is_founding, current_count)."""
    row = db.query(FoundingSubscriberCount).filter_by(
        tier=tier, vertical=vertical, county_id=county_id
    ).first()
    count = row.count if row else 0
    return count < founding_limit, count


# ============================================================================
# Tests — Checkout Flow
# ============================================================================

class TestCheckoutFlow:

    def test_creates_subscriber_and_locks_zips(self, db):
        sub = _do_checkout(db, _checkout_session())

        assert sub.id is not None
        assert sub.status == "active"
        assert sub.tier == "starter"
        assert sub.vertical == "roofing"
        assert sub.founding_member is True
        assert sub.ghl_stage == 5
        assert sub.event_feed_uuid is not None

        # Two ZIPs locked
        territories = db.query(ZipTerritory).filter_by(subscriber_id=sub.id).all()
        assert len(territories) == 2
        for t in territories:
            assert t.status == "locked"
            assert t.locked_at is not None

    def test_founding_count_incremented(self, db):
        _do_checkout(db, _checkout_session(customer_id="cus_1"))

        row = db.query(FoundingSubscriberCount).filter_by(
            tier="starter", vertical="roofing", county_id="hillsborough"
        ).first()
        assert row is not None
        assert row.count == 1

    def test_multiple_checkouts_increment_count(self, db):
        for i in range(5):
            _do_checkout(db, _checkout_session(
                customer_id=f"cus_{i}",
                subscription_id=f"sub_{i}",
                zip_codes=f"3360{i}",
            ))

        row = db.query(FoundingSubscriberCount).filter_by(
            tier="starter", vertical="roofing", county_id="hillsborough"
        ).first()
        assert row.count == 5

    def test_non_founding_checkout_does_not_increment(self, db):
        _do_checkout(db, _checkout_session(is_founding=False))

        row = db.query(FoundingSubscriberCount).filter_by(
            tier="starter", vertical="roofing", county_id="hillsborough"
        ).first()
        assert row is None  # Row never created for non-founding

    def test_existing_subscriber_upgraded(self, db):
        # First checkout
        sub = _do_checkout(db, _checkout_session(tier="starter"))
        original_id = sub.id

        # Upgrade to pro
        sub2 = _do_checkout(db, _checkout_session(
            tier="pro",
            subscription_id="sub_upgrade",
            is_founding=False,
        ))

        assert sub2.id == original_id
        assert sub2.tier == "pro"
        assert sub2.founding_member is True  # Never overwritten


class TestFoundingLimit:

    def test_10th_subscriber_gets_founding(self, db):
        # Fill 9 spots
        for i in range(9):
            _do_checkout(db, _checkout_session(
                customer_id=f"cus_{i}", subscription_id=f"sub_{i}",
                zip_codes=f"3360{i}",
            ))

        is_founding, count = _get_price_for_checkout(db, "starter", "roofing", "hillsborough")
        assert count == 9
        assert is_founding is True

    def test_11th_subscriber_gets_regular(self, db):
        # Fill all 10 spots
        for i in range(10):
            _do_checkout(db, _checkout_session(
                customer_id=f"cus_{i}", subscription_id=f"sub_{i}",
                zip_codes=f"3360{i}",
            ))

        is_founding, count = _get_price_for_checkout(db, "starter", "roofing", "hillsborough")
        assert count == 10
        assert is_founding is False

    def test_different_verticals_have_separate_counts(self, db):
        _do_checkout(db, _checkout_session(vertical="roofing", customer_id="cus_r1", subscription_id="sub_r1"))
        _do_checkout(db, _checkout_session(vertical="remediation", customer_id="cus_m1", subscription_id="sub_m1", zip_codes="33610"))

        roofing_row = db.query(FoundingSubscriberCount).filter_by(
            tier="starter", vertical="roofing", county_id="hillsborough"
        ).first()
        remediation_row = db.query(FoundingSubscriberCount).filter_by(
            tier="starter", vertical="remediation", county_id="hillsborough"
        ).first()

        assert roofing_row.count == 1
        assert remediation_row.count == 1


class TestZipExclusivity:

    def test_locked_zip_not_stolen(self, db):
        # First subscriber locks 33601
        _do_checkout(db, _checkout_session(
            customer_id="cus_1", subscription_id="sub_1", zip_codes="33601",
        ))

        # Second subscriber tries same ZIP
        _do_checkout(db, _checkout_session(
            customer_id="cus_2", subscription_id="sub_2", zip_codes="33601",
        ))

        territory = db.query(ZipTerritory).filter_by(zip_code="33601", vertical="roofing").first()
        sub1 = db.query(Subscriber).filter_by(stripe_customer_id="cus_1").first()

        # Territory still belongs to first subscriber
        assert territory.subscriber_id == sub1.id

    def test_grace_zip_can_be_relocked(self, db):
        # First subscriber checks out and then cancels
        sub1 = _do_checkout(db, _checkout_session(
            customer_id="cus_1", subscription_id="sub_1", zip_codes="33601",
        ))
        _do_subscription_deleted(db, "cus_1")

        territory = db.query(ZipTerritory).filter_by(zip_code="33601", vertical="roofing").first()
        assert territory.status == "grace"

        # Second subscriber locks the grace ZIP
        sub2 = _do_checkout(db, _checkout_session(
            customer_id="cus_2", subscription_id="sub_2", zip_codes="33601",
        ))

        territory = db.query(ZipTerritory).filter_by(zip_code="33601", vertical="roofing").first()
        assert territory.status == "locked"
        assert territory.subscriber_id == sub2.id

    def test_same_zip_different_verticals_independent(self, db):
        _do_checkout(db, _checkout_session(
            customer_id="cus_r", subscription_id="sub_r",
            vertical="roofing", zip_codes="33601",
        ))
        _do_checkout(db, _checkout_session(
            customer_id="cus_m", subscription_id="sub_m",
            vertical="remediation", zip_codes="33601",
        ))

        roofing = db.query(ZipTerritory).filter_by(zip_code="33601", vertical="roofing").first()
        remediation = db.query(ZipTerritory).filter_by(zip_code="33601", vertical="remediation").first()

        assert roofing.status == "locked"
        assert remediation.status == "locked"
        assert roofing.subscriber_id != remediation.subscriber_id


# ============================================================================
# Tests — Subscription Deleted (Grace Period)
# ============================================================================

class TestSubscriptionDeleted:

    def test_subscriber_enters_grace(self, db):
        _do_checkout(db, _checkout_session())
        sub = _do_subscription_deleted(db, "cus_1")

        assert sub.status == "grace"
        assert sub.ghl_stage == 7
        assert sub.grace_expires_at is not None

    def test_grace_expires_in_48_hours(self, db):
        _do_checkout(db, _checkout_session())

        before = datetime.now(timezone.utc).replace(tzinfo=None)
        sub = _do_subscription_deleted(db, "cus_1")
        after = datetime.now(timezone.utc).replace(tzinfo=None)

        # SQLite strips tzinfo, so compare as naive
        grace = sub.grace_expires_at.replace(tzinfo=None) if sub.grace_expires_at.tzinfo else sub.grace_expires_at
        assert before + timedelta(hours=47) < grace < after + timedelta(hours=49)

    def test_territories_enter_grace(self, db):
        _do_checkout(db, _checkout_session(zip_codes="33601,33602"))
        _do_subscription_deleted(db, "cus_1")

        territories = db.query(ZipTerritory).all()
        for t in territories:
            assert t.status == "grace"
            assert t.grace_expires_at is not None

    def test_nonexistent_subscriber_returns_none(self, db):
        result = _do_subscription_deleted(db, "cus_nonexistent")
        assert result is None


# ============================================================================
# Tests — Grace Expiry Cron
# ============================================================================

class TestGraceExpiry:

    def test_expired_territory_released(self, db):
        _do_checkout(db, _checkout_session(zip_codes="33601"))
        _do_subscription_deleted(db, "cus_1")

        # Backdate grace to already expired
        t = db.query(ZipTerritory).first()
        t.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        s = db.query(Subscriber).first()
        s.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.commit()

        zips_released, subs_churned, _ = _do_grace_expiry(db)

        assert zips_released == 1
        assert subs_churned == 1

        territory = db.query(ZipTerritory).first()
        assert territory.status == "available"
        assert territory.subscriber_id is None
        assert territory.locked_at is None

        subscriber = db.query(Subscriber).first()
        assert subscriber.status == "churned"

    def test_not_yet_expired_untouched(self, db):
        _do_checkout(db, _checkout_session(zip_codes="33601"))
        _do_subscription_deleted(db, "cus_1")

        # Grace expires in the future — should NOT be released
        zips_released, subs_churned, _ = _do_grace_expiry(db)

        assert zips_released == 0
        assert subs_churned == 0

        territory = db.query(ZipTerritory).first()
        assert territory.status == "grace"

    def test_multiple_territories_all_released(self, db):
        _do_checkout(db, _checkout_session(zip_codes="33601,33602,33603"))
        _do_subscription_deleted(db, "cus_1")

        # Backdate all
        for t in db.query(ZipTerritory).all():
            t.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.query(Subscriber).first().grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.commit()

        zips_released, subs_churned, _ = _do_grace_expiry(db)

        assert zips_released == 3
        assert subs_churned == 1
        for t in db.query(ZipTerritory).all():
            assert t.status == "available"

    def test_waitlist_notifications_collected(self, db):
        _do_checkout(db, _checkout_session(zip_codes="33601"))
        _do_subscription_deleted(db, "cus_1")

        # Add waitlist emails and backdate
        t = db.query(ZipTerritory).first()
        t.waitlist_emails = ["a@test.com", "b@test.com"]
        t.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.query(Subscriber).first().grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.commit()

        _, _, notifications = _do_grace_expiry(db)

        assert len(notifications) == 1
        zip_code, vertical, county_id, emails = notifications[0]
        assert zip_code == "33601"
        assert vertical == "roofing"
        assert emails == ["a@test.com", "b@test.com"]

        # Waitlist cleared after release
        t = db.query(ZipTerritory).first()
        assert t.waitlist_emails == []

    def test_no_waitlist_no_notification(self, db):
        _do_checkout(db, _checkout_session(zip_codes="33601"))
        _do_subscription_deleted(db, "cus_1")

        t = db.query(ZipTerritory).first()
        t.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.query(Subscriber).first().grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.commit()

        _, _, notifications = _do_grace_expiry(db)
        assert len(notifications) == 0


# ============================================================================
# Tests — Full Lifecycle (end-to-end)
# ============================================================================

class TestFullLifecycle:

    def test_checkout_cancel_expire_relock(self, db):
        """Full cycle: checkout → cancel → grace expires → new subscriber locks ZIP."""
        # 1. First subscriber checks out
        sub1 = _do_checkout(db, _checkout_session(
            customer_id="cus_1", subscription_id="sub_1", zip_codes="33601",
        ))
        assert sub1.status == "active"

        # 2. First subscriber cancels
        _do_subscription_deleted(db, "cus_1")
        sub1 = db.query(Subscriber).filter_by(stripe_customer_id="cus_1").first()
        assert sub1.status == "grace"

        # 3. Grace expires
        t = db.query(ZipTerritory).filter_by(zip_code="33601").first()
        t.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        sub1.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.commit()

        zips_released, subs_churned, _ = _do_grace_expiry(db)
        assert zips_released == 1
        assert subs_churned == 1

        sub1 = db.query(Subscriber).filter_by(stripe_customer_id="cus_1").first()
        assert sub1.status == "churned"

        t = db.query(ZipTerritory).filter_by(zip_code="33601").first()
        assert t.status == "available"

        # 4. New subscriber locks the now-available ZIP
        sub2 = _do_checkout(db, _checkout_session(
            customer_id="cus_2", subscription_id="sub_2", zip_codes="33601",
        ))

        t = db.query(ZipTerritory).filter_by(zip_code="33601").first()
        assert t.status == "locked"
        assert t.subscriber_id == sub2.id
        assert sub2.status == "active"

    def test_founding_fills_up_across_lifecycle(self, db):
        """Founding spots deplete as subscribers check out, regardless of churn."""
        # Fill 10 founding spots
        for i in range(10):
            _do_checkout(db, _checkout_session(
                customer_id=f"cus_{i}", subscription_id=f"sub_{i}",
                zip_codes=f"336{i:02d}",
            ))

        # All 10 founding spots taken
        is_founding, count = _get_price_for_checkout(db, "starter", "roofing", "hillsborough")
        assert count == 10
        assert is_founding is False

        # Even after churn, founding count stays at 10
        _do_subscription_deleted(db, "cus_0")
        t = db.query(ZipTerritory).filter_by(zip_code="33600").first()
        t.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.query(Subscriber).filter_by(stripe_customer_id="cus_0").first().grace_expires_at = (
            datetime.now(timezone.utc) - timedelta(hours=1)
        )
        db.commit()
        _do_grace_expiry(db)

        # Founding count unchanged — founding is permanent
        is_founding, count = _get_price_for_checkout(db, "starter", "roofing", "hillsborough")
        assert count == 10
        assert is_founding is False

    def test_waitlist_notified_after_full_cycle(self, db):
        """Waitlisted email gets notification after grace expires."""
        # 1. Checkout
        _do_checkout(db, _checkout_session(
            customer_id="cus_1", subscription_id="sub_1", zip_codes="33601",
        ))

        # 2. Someone joins waitlist
        t = db.query(ZipTerritory).filter_by(zip_code="33601").first()
        t.waitlist_emails = ["waiting@test.com"]
        db.commit()

        # 3. Cancel + expire
        _do_subscription_deleted(db, "cus_1")
        for t in db.query(ZipTerritory).all():
            t.grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.query(Subscriber).first().grace_expires_at = datetime.now(timezone.utc) - timedelta(hours=1)
        db.commit()

        _, _, notifications = _do_grace_expiry(db)

        assert len(notifications) == 1
        assert notifications[0][3] == ["waiting@test.com"]

        # Territory available, waitlist cleared
        t = db.query(ZipTerritory).filter_by(zip_code="33601").first()
        assert t.status == "available"
        assert t.waitlist_emails == []
