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


def _paid_subscriber(email, **kw):
    now = datetime.now(timezone.utc)
    return Subscriber(
        stripe_customer_id=f"cus_paid_{email}",
        tier="pro",
        vertical="roofing",
        county_id="hillsborough",
        status="active",
        email=email,
        created_at=now,
        event_feed_uuid=f"feed_paid_{email}",
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


def test_mark_converted_removes_lead_and_marks_converted(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="convert_me@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id="lead_1",
        enrolled_at=now,
    ))
    fresh_db.flush()

    with patch.object(non_buyer_nurture.instantly, "remove_lead", return_value=True) as mock_remove:
        non_buyer_nurture.mark_converted(fresh_db, "convert_me@example.com")

    mock_remove.assert_called_once_with("lead_1")
    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="convert_me@example.com").one()
    assert row.status == "converted"
    assert row.removal_reason == "paid_conversion"
    assert row.converted_at is not None
    assert row.removed_at is not None


def test_mark_converted_without_lead_id_still_converts(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="convert_nolead@example.com", source="waitlist",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id=None,
        enrolled_at=now,
    ))
    fresh_db.flush()

    with patch.object(non_buyer_nurture.instantly, "remove_lead") as mock_remove:
        non_buyer_nurture.mark_converted(fresh_db, "convert_nolead@example.com")

    mock_remove.assert_not_called()
    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="convert_nolead@example.com").one()
    assert row.status == "converted"


def test_mark_converted_no_row_is_noop(fresh_db):
    non_buyer_nurture.mark_converted(fresh_db, "never_enrolled@example.com")
    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="never_enrolled@example.com").one_or_none()
    assert row is None


def test_mark_converted_idempotent_on_replay(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="replay@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id="lead_2",
        enrolled_at=now,
    ))
    fresh_db.flush()

    with patch.object(non_buyer_nurture.instantly, "remove_lead", return_value=True) as mock_remove:
        non_buyer_nurture.mark_converted(fresh_db, "replay@example.com")
        non_buyer_nurture.mark_converted(fresh_db, "replay@example.com")

    mock_remove.assert_called_once()


def test_apply_instantly_status_unsubscribed_is_terminal(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="unsub@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id="lead_3",
        enrolled_at=now,
    ))
    fresh_db.flush()

    non_buyer_nurture.apply_instantly_status(fresh_db, "unsub@example.com", "unsubscribed")

    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="unsub@example.com").one()
    assert row.status == "unsubscribed"
    assert row.removal_reason == "unsubscribe"
    assert row.removed_at is not None


def test_apply_instantly_status_bounced_is_terminal(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="bounce@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id="lead_4",
        enrolled_at=now,
    ))
    fresh_db.flush()

    non_buyer_nurture.apply_instantly_status(fresh_db, "bounce@example.com", "bounced")

    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="bounce@example.com").one()
    assert row.status == "bounced"
    assert row.removal_reason == "bounce"


def test_apply_instantly_status_backfills_lead_id_without_status_change(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="active@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id=None,
        enrolled_at=now,
    ))
    fresh_db.flush()

    non_buyer_nurture.apply_instantly_status(fresh_db, "active@example.com", "active", instantly_lead_id="lead_5")

    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="active@example.com").one()
    assert row.status == "enrolled"
    assert row.instantly_lead_id == "lead_5"


def test_apply_instantly_status_does_not_downgrade_converted(fresh_db):
    now = datetime.now(timezone.utc)
    fresh_db.add(NonBuyerNurtureSequence(
        email="already_converted@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="converted",
        removal_reason="paid_conversion", converted_at=now, removed_at=now,
    ))
    fresh_db.flush()

    non_buyer_nurture.apply_instantly_status(fresh_db, "already_converted@example.com", "bounced")

    row = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="already_converted@example.com").one()
    assert row.status == "converted"


def test_find_candidates_excludes_email_with_paid_subscriber(fresh_db):
    now = datetime.now(timezone.utc)
    # Eligible checkout-abandon candidate — but the same email is now a paying customer.
    fresh_db.add(NonBuyerNurtureSequence(
        email="alreadypaid@example.com", source="checkout_abandon",
        captured_at=now - timedelta(hours=30), status="eligible",
    ))
    fresh_db.add(_paid_subscriber("alreadypaid@example.com"))
    fresh_db.flush()

    candidates = non_buyer_nurture.find_candidates(fresh_db, limit=10_000)
    emails = {c["email"] for c in candidates}
    assert "alreadypaid@example.com" not in emails


def test_reconcile_conversions_marks_paid_enrolled_rows_converted(fresh_db):
    now = datetime.now(timezone.utc)
    # Enrolled lead who has since become a paying subscriber but the webhook missed it.
    fresh_db.add(NonBuyerNurtureSequence(
        email="missed@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id=None, enrolled_at=now,
    ))
    fresh_db.add(_paid_subscriber("missed@example.com"))
    # Enrolled lead with no paid subscriber — must stay enrolled.
    fresh_db.add(NonBuyerNurtureSequence(
        email="stillfree@example.com", source="free_signup",
        captured_at=now - timedelta(hours=30), status="enrolled",
        instantly_campaign_id="camp_1", instantly_lead_id=None, enrolled_at=now,
    ))
    fresh_db.flush()

    n = non_buyer_nurture.reconcile_conversions(fresh_db)

    assert n >= 1
    missed = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="missed@example.com").one()
    assert missed.status == "converted"
    assert missed.removal_reason == "paid_conversion"
    still = fresh_db.query(NonBuyerNurtureSequence).filter_by(email="stillfree@example.com").one()
    assert still.status == "enrolled"
