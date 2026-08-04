import uuid
from unittest.mock import patch

from sqlalchemy import text

from src.core.models import Subscriber
from src.services.stripe_webhooks import _on_subscription_deleted


def _make_subscriber(fresh_db, stripe_customer_id: str) -> Subscriber:
    """Mirrors tests/test_outcome_dispatch.py's _mk(fresh_db) subscriber block —
    same required fields, same flush-then-commit pattern."""
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=stripe_customer_id,
        tier="starter", vertical="roofing", county_id="hillsborough",
        event_feed_uuid=f"fleetq1-{uid}", email=f"fleetq1_{uid}@example.com",
        name=f"FleetQ1 {uid}", status="active",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


def test_subscription_deleted_emits_fleet_cancellation_event(fresh_db):
    # NOTE: brief specified a hardcoded "cus_test123" — switched to a uuid-suffixed
    # id, matching the established convention in tests/test_subscriber_memory.py's
    # _make_subscriber(db) (uses uuid4().hex[:8] for stripe_customer_id). This test
    # calls fresh_db.commit() (required so the assertion query, run on the same
    # session, sees the emitted row), and commit() here flattens through the
    # fresh_db fixture's SAVEPOINT to the real shared dev Postgres — a hardcoded
    # id would collide with a stale row on any rerun.
    customer_id = f"cus_fleetq1_{uuid.uuid4().hex[:8]}"
    subscription_id = f"sub_fleetq1_{uuid.uuid4().hex[:8]}"
    subscriber = _make_subscriber(fresh_db, stripe_customer_id=customer_id)
    fresh_db.commit()

    # Match the repo's established convention for testing this function directly
    # (see tests/test_subscriber_memory.py:230) — patch the external side effects
    # so the test never depends on GHL/email being configured in this environment.
    with patch("src.services.stripe_webhooks.push_subscriber_to_ghl"):
        _on_subscription_deleted({"customer": customer_id, "id": subscription_id}, fresh_db)
    fresh_db.commit()

    row = fresh_db.execute(text("""
        SELECT event_type, priority, subscriber_id
        FROM fleet_events
        WHERE event_type = 'subscription.cancelled' AND subscriber_id = :sid
    """), {"sid": subscriber.id}).fetchone()

    assert row is not None
    assert row.event_type == "subscription.cancelled"
