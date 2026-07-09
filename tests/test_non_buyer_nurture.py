"""
Tests for the non-buyer nurture sequence — schema, service core, and hooks.
Real DB (fresh_db), Instantly mocked.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.core.models import NonBuyerNurtureSequence


def test_non_buyer_nurture_sequence_row_round_trips(fresh_db):
    now = datetime.now(timezone.utc)
    row = NonBuyerNurtureSequence(
        email="tracer@example.com",
        source="free_signup",
        captured_at=now,
    )
    fresh_db.add(row)
    fresh_db.flush()

    fetched = (
        fresh_db.query(NonBuyerNurtureSequence)
        .filter_by(email="tracer@example.com")
        .one()
    )
    assert fetched.status == "eligible"
    assert fetched.subscriber_id is None
    assert fetched.source == "free_signup"
