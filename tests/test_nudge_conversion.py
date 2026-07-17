"""
record_nudge_conversion — last-touch nudge attribution stamp on unlock (D7).

A hot-lead unlock fires `record_nudge_conversion`, which looks for the most
recent qualifying `message_outcomes` row (sent, not yet converted) within 48h
of the purchase and stamps it as the touch that converted.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.core.models import MessageOutcome, Subscriber
from src.services.nudge_conversion import record_nudge_conversion


def _seed_subscriber(db):
    sub = Subscriber(
        stripe_customer_id=f"cus_{uuid.uuid4().hex[:8]}", tier="pro", vertical="roofing",
        county_id="hillsborough", status="active",
        event_feed_uuid=f"nc-{uuid.uuid4().hex[:8]}",
        email=f"{uuid.uuid4().hex[:8]}@example.com",
    )
    db.add(sub)
    db.flush()
    return sub


def _seed_message_outcome(db, subscriber_id, sent_at, **overrides):
    fields = dict(
        subscriber_id=subscriber_id, message_type="sms", channel="telnyx",
        sent_at=sent_at, send_status="sent", conversion_type=None,
    )
    fields.update(overrides)
    m = MessageOutcome(**fields)
    db.add(m)
    db.flush()
    return m


class TestRecordNudgeConversion:
    def test_nudge_3h_ago_stamps_within_4h(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        now = datetime.now(timezone.utc)
        nudge = _seed_message_outcome(fresh_db, sub.id, now - timedelta(hours=3))

        result_id = record_nudge_conversion(sub.id, revenue=99.0, occurred_at=now, db=fresh_db)

        assert result_id == nudge.id
        fresh_db.refresh(nudge)
        assert nudge.conversion_type == "unlock"
        assert nudge.conversion_within_4h is True
        assert nudge.conversion_within_24h is True
        assert nudge.conversion_within_48h is True
        assert float(nudge.revenue_attributed) == 99.0

    def test_nudge_40h_ago_stamps_within_48h_only(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        now = datetime.now(timezone.utc)
        nudge = _seed_message_outcome(fresh_db, sub.id, now - timedelta(hours=40))

        result_id = record_nudge_conversion(sub.id, revenue=150.0, occurred_at=now, db=fresh_db)

        assert result_id == nudge.id
        fresh_db.refresh(nudge)
        assert nudge.conversion_type == "unlock"
        assert nudge.conversion_within_4h is False
        assert nudge.conversion_within_24h is False
        assert nudge.conversion_within_48h is True

    def test_nudge_50h_ago_is_a_no_op(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        now = datetime.now(timezone.utc)
        nudge = _seed_message_outcome(fresh_db, sub.id, now - timedelta(hours=50))

        result_id = record_nudge_conversion(sub.id, revenue=150.0, occurred_at=now, db=fresh_db)

        assert result_id is None
        fresh_db.refresh(nudge)
        assert nudge.conversion_type is None

    def test_no_nudge_is_a_no_op(self, fresh_db):
        sub = _seed_subscriber(fresh_db)
        result_id = record_nudge_conversion(sub.id, revenue=150.0, db=fresh_db)
        assert result_id is None

    def test_already_converted_nudge_is_skipped(self, fresh_db):
        """A nudge already credited with a different conversion must not be
        double-stamped by a later, unrelated unlock."""
        sub = _seed_subscriber(fresh_db)
        now = datetime.now(timezone.utc)
        nudge = _seed_message_outcome(
            fresh_db, sub.id, now - timedelta(hours=1), conversion_type="wallet",
        )

        result_id = record_nudge_conversion(sub.id, revenue=150.0, occurred_at=now, db=fresh_db)

        assert result_id is None
        fresh_db.refresh(nudge)
        assert nudge.conversion_type == "wallet"
