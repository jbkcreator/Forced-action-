"""
Tests for the non-buyer nurture sequence — schema, service core, and hooks.
Real DB (fresh_db), Instantly mocked.
"""

from datetime import datetime, timedelta, timezone

import pytest
from unittest.mock import patch

from src.core.models import NonBuyerNurtureSequence, Subscriber
from src.services import non_buyer_nurture


def _free_subscriber(email, created_at, **kw):
    now = datetime.now(timezone.utc)
    return Subscriber(
        stripe_customer_id=f"cus_{email}",
        tier="free",
        vertical="roofing",
        county_id="hillsborough",
        status="active",
        email=email,
        created_at=created_at,
        event_feed_uuid=f"feed_{email}",
        **kw,
    )


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


def test_find_candidates_returns_aged_free_signup(fresh_db):
    now = datetime.now(timezone.utc)
    sub = _free_subscriber("aged@example.com", created_at=now - timedelta(hours=30))
    fresh_db.add(sub)
    fresh_db.flush()

    candidates = non_buyer_nurture.find_candidates(fresh_db, limit=10)

    emails = [c["email"] for c in candidates]
    assert "aged@example.com" in emails


def test_find_candidates_skips_too_recent_and_too_stale(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(_free_subscriber("too_recent@example.com", created_at=now - timedelta(hours=1)))
    fresh_db.add(_free_subscriber("too_stale@example.com", created_at=now - timedelta(days=91)))
    fresh_db.flush()

    candidates = non_buyer_nurture.find_candidates(fresh_db, limit=10)

    emails = {c["email"] for c in candidates}
    assert "too_recent@example.com" not in emails
    assert "too_stale@example.com" not in emails


def test_find_candidates_skips_email_already_in_nurture_table(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(_free_subscriber("already@example.com", created_at=now - timedelta(hours=30)))
    fresh_db.add(NonBuyerNurtureSequence(
        email="already@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="converted",
    ))
    fresh_db.flush()

    candidates = non_buyer_nurture.find_candidates(fresh_db, limit=10)

    emails = {c["email"] for c in candidates}
    assert "already@example.com" not in emails


def test_find_candidates_newest_first_and_cap_respected(fresh_db):
    # Shared dev DB has real pre-existing subscribers in-window, so we can't
    # assert absolute top-N — assert relative order of our own rows, and that
    # cap trims the result set at all.
    now = datetime.now(timezone.utc)
    fresh_db.add(_free_subscriber("oldest@example.com", created_at=now - timedelta(days=80)))
    fresh_db.add(_free_subscriber("middle@example.com", created_at=now - timedelta(days=40)))
    fresh_db.add(_free_subscriber("newest@example.com", created_at=now - timedelta(hours=25)))
    fresh_db.flush()

    all_candidates = non_buyer_nurture.find_candidates(fresh_db, limit=10_000)
    ours = [c["email"] for c in all_candidates if c["email"].endswith("@example.com")]
    assert ours == ["newest@example.com", "middle@example.com", "oldest@example.com"]

    capped = non_buyer_nurture.find_candidates(fresh_db, limit=1)
    assert len(capped) == 1


def test_record_checkout_abandon_candidate_is_idempotent(fresh_db):
    non_buyer_nurture.record_checkout_abandon_candidate(fresh_db, "abandoned@example.com")
    non_buyer_nurture.record_checkout_abandon_candidate(fresh_db, "abandoned@example.com")
    fresh_db.flush()

    rows = (
        fresh_db.query(NonBuyerNurtureSequence)
        .filter_by(email="abandoned@example.com")
        .all()
    )
    assert len(rows) == 1
    assert rows[0].source == "checkout_abandon"
    assert rows[0].status == "eligible"


def test_find_candidates_surfaces_checkout_abandon_eligible_row(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="abandon_cand@example.com", source="checkout_abandon",
        captured_at=now - timedelta(hours=30), status="eligible",
    ))
    fresh_db.flush()

    candidates = non_buyer_nurture.find_candidates(fresh_db, limit=10_000)
    emails = [c["email"] for c in candidates]
    assert emails.count("abandon_cand@example.com") == 1


def test_enroll_marks_enrolled_on_instantly_success(fresh_db):
    now = datetime.now(timezone.utc)
    candidate = {
        "email": "enroll_ok@example.com",
        "subscriber_id": None,
        "source": "free_signup",
        "captured_at": now - timedelta(hours=30),
    }

    with patch.object(non_buyer_nurture.instantly, "add_leads", return_value={"leads_created": 1}) as mock_add:
        non_buyer_nurture.enroll(fresh_db, [candidate], campaign_id="camp_123")

    mock_add.assert_called_once()
    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="enroll_ok@example.com").one()
    assert row.status == "enrolled"
    assert row.instantly_campaign_id == "camp_123"
    assert row.enrolled_at is not None
    assert row.eligible_at is not None


def test_enroll_leaves_eligible_on_instantly_failure(fresh_db):
    now = datetime.now(timezone.utc)
    candidate = {
        "email": "enroll_fail@example.com",
        "subscriber_id": None,
        "source": "free_signup",
        "captured_at": now - timedelta(hours=30),
    }

    with patch.object(non_buyer_nurture.instantly, "add_leads", return_value=None):
        non_buyer_nurture.enroll(fresh_db, [candidate], campaign_id="camp_123")

    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="enroll_fail@example.com").one()
    assert row.status == "eligible"
    assert row.enrolled_at is None


def test_enroll_updates_existing_eligible_row_not_duplicate(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="retry@example.com", source="checkout_abandon",
        captured_at=now - timedelta(hours=30), status="eligible",
    ))
    fresh_db.flush()

    candidate = {
        "email": "retry@example.com",
        "subscriber_id": None,
        "source": "checkout_abandon",
        "captured_at": now - timedelta(hours=30),
    }
    with patch.object(non_buyer_nurture.instantly, "add_leads", return_value={"leads_created": 1}):
        non_buyer_nurture.enroll(fresh_db, [candidate], campaign_id="camp_123")

    rows = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="retry@example.com").all()
    assert len(rows) == 1
    assert rows[0].status == "enrolled"
