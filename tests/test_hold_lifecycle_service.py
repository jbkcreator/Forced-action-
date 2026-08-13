"""
Hold-lifecycle service tests — Ticket 02.

Uses real Postgres (fresh_db fixture — rolls back after each test) and a fake
Stripe client so no live Stripe calls are made.

Run:
    pytest tests/test_hold_lifecycle_service.py -v
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from src.services.hold_lifecycle_service import (
    apply_hold_payment,
    create_deal_room,
    expire_holds,
    refund_on_conversion,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


class FakeStripe:
    """Minimal Stripe-client substitute — records calls, returns dummy objects."""

    def __init__(self, *, refund_raises: Exception | None = None):
        self._refund_raises = refund_raises
        self.refunds = _FakeRefunds(raises=refund_raises)
        self.refund_calls: list[dict] = self.refunds.calls


class _FakeRefunds:
    def __init__(self, raises: Exception | None = None):
        self.calls: list[dict] = []
        self._raises = raises

    def create(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        if self._raises is not None:
            raise self._raises
        return {"id": "re_fake_" + str(uuid.uuid4())[:8]}


def _seed_zip(
    db,
    zip_code: str = "33601",
    status: str = "available",
    *,
    vertical: str = "general",
    county_id: str = "1",
) -> None:
    """Upsert the specific (zip_code, vertical, county_id) territory row to `status`.

    Scoped to one territory row so other verticals/counties for the same ZIP are
    left untouched — the whole point of the bug #3 fix.
    """
    db.execute(
        text(
            "INSERT INTO zip_territories "
            "(zip_code, vertical, county_id, status, updated_at) "
            "VALUES (:zip, :v, :c, :status, NOW()) "
            "ON CONFLICT (zip_code, vertical, county_id) "
            "DO UPDATE SET status = :status, updated_at = NOW()"
        ),
        {"zip": zip_code, "v": vertical, "c": county_id, "status": status},
    )
    db.flush()


def _zip_status(db, zip_code, *, vertical="general", county_id="1"):
    row = db.execute(
        text(
            "SELECT status FROM zip_territories "
            "WHERE zip_code = :zip AND vertical = :v AND county_id = :c"
        ),
        {"zip": zip_code, "v": vertical, "c": county_id},
    ).fetchone()
    return row.status if row else None


def _make_deal_room(
    db,
    zip_code: str = "33601",
    *,
    stripe_pi: str | None = None,
    vertical: str = "general",
    county_id: str = "1",
):
    """Create a deal-room record, seeding the specific territory row as available."""
    _seed_zip(db, zip_code, "available", vertical=vertical, county_id=county_id)
    deal_room = create_deal_room(
        db,
        prospect_name="Test Prospect",
        prospect_email="prospect@example.com",
        zip_code=zip_code,
        vertical=vertical,
        county_id=county_id,
        tier="starter",
        job_value=5000.0,
        close_rate=0.3,
        properties_snapshot={"properties": []},
    )
    if stripe_pi:
        deal_room.stripe_payment_intent_id = stripe_pi
        db.flush()
    return deal_room


# Silence Slack alerts in all tests
@pytest.fixture(autouse=True)
def _no_slack(monkeypatch):
    monkeypatch.setattr(
        "src.services.hold_lifecycle_service.post_incident_alert",
        lambda *a, **kw: None,
    )


# ---------------------------------------------------------------------------
# create_deal_room
# ---------------------------------------------------------------------------


class TestCreateDealRoom:
    def test_raises_409_when_zip_not_available(self, fresh_db):
        _seed_zip(fresh_db, "33601", "locked")
        with pytest.raises(HTTPException) as exc_info:
            create_deal_room(
                fresh_db,
                prospect_name="Jane",
                prospect_email="jane@example.com",
                zip_code="33601",
                vertical="general",
                county_id="1",
                tier="starter",
                job_value=3000.0,
                close_rate=0.25,
                properties_snapshot={},
            )
        assert exc_info.value.status_code == 409

    def test_raises_409_when_zip_held(self, fresh_db):
        _seed_zip(fresh_db, "33602", "held")
        with pytest.raises(HTTPException) as exc_info:
            create_deal_room(
                fresh_db,
                prospect_name="Jane",
                prospect_email="jane@example.com",
                zip_code="33602",
                vertical="general",
                county_id="1",
                tier="starter",
                job_value=3000.0,
                close_rate=0.25,
                properties_snapshot={},
            )
        assert exc_info.value.status_code == 409

    def test_raises_409_when_zip_not_found(self, fresh_db):
        # No row at all for this ZIP
        with pytest.raises(HTTPException) as exc_info:
            create_deal_room(
                fresh_db,
                prospect_name="Jane",
                prospect_email="jane@example.com",
                zip_code="99999",
                vertical="general",
                county_id="1",
                tier="starter",
                job_value=3000.0,
                close_rate=0.25,
                properties_snapshot={},
            )
        assert exc_info.value.status_code == 409

    def test_creates_deal_room_when_zip_available(self, fresh_db):
        _seed_zip(fresh_db, "33603", "available")
        dr = create_deal_room(
            fresh_db,
            prospect_name="Bob",
            prospect_email="bob@example.com",
            zip_code="33603",
            vertical="general",
            county_id="1",
            tier="pro",
            job_value=8000.0,
            close_rate=0.4,
            properties_snapshot={"count": 3},
        )
        assert dr.id is not None
        assert dr.token is not None
        assert dr.held_at is None
        assert dr.expires_at is None
        assert dr.zip_code == "33603"


# ---------------------------------------------------------------------------
# apply_hold_payment
# ---------------------------------------------------------------------------


class TestApplyHoldPayment:
    def test_flips_available_to_held(self, fresh_db):
        _seed_zip(fresh_db, "33610", "available")
        dr = _make_deal_room(fresh_db, "33610", stripe_pi="pi_test_1")
        stripe = FakeStripe()

        result = apply_hold_payment(fresh_db, stripe, token=dr.token)

        assert result is True
        assert dr.held_at is not None
        assert dr.expires_at is not None
        assert dr.expires_at > dr.held_at

        # The exact (zip, vertical, county) territory should now be 'held'.
        assert _zip_status(fresh_db, "33610") == "held"

        # No refund should have been issued
        assert len(stripe.refund_calls) == 0

    def test_returns_true_on_duplicate_webhook(self, fresh_db):
        """Second call with same token is idempotent — returns True, no refund."""
        _seed_zip(fresh_db, "33611", "available")
        dr = _make_deal_room(fresh_db, "33611", stripe_pi="pi_test_2")
        stripe = FakeStripe()

        apply_hold_payment(fresh_db, stripe, token=dr.token)
        result2 = apply_hold_payment(fresh_db, stripe, token=dr.token)

        assert result2 is True
        assert len(stripe.refund_calls) == 0

    def test_race_loser_gets_refund_no_double_hold(self, fresh_db):
        """When another process already holds the ZIP, 0 rows updated → refund fired, no double-hold."""
        _seed_zip(fresh_db, "33612", "available")
        dr = _make_deal_room(fresh_db, "33612", stripe_pi="pi_test_3")
        stripe = FakeStripe()

        # Simulate the ZIP being grabbed by another path before our update
        fresh_db.execute(
            text("UPDATE zip_territories SET status = 'held' WHERE zip_code = '33612'")
        )
        fresh_db.flush()

        result = apply_hold_payment(fresh_db, stripe, token=dr.token)

        assert result is False
        assert len(stripe.refund_calls) == 1
        assert stripe.refund_calls[0]["payment_intent"] == "pi_test_3"
        assert dr.refund_status == "refunded"


# ---------------------------------------------------------------------------
# expire_holds
# ---------------------------------------------------------------------------


class TestExpireHolds:
    def _create_expired_hold(self, db, zip_code: str, stripe_pi: str = "pi_exp_1"):
        dr = _make_deal_room(db, zip_code, stripe_pi=stripe_pi)
        now = datetime.now(timezone.utc)
        dr.held_at = now - timedelta(hours=72)
        dr.expires_at = now - timedelta(hours=24)
        db.flush()
        return dr

    def test_releases_zip_and_returns_count(self, fresh_db):
        dr = self._create_expired_hold(fresh_db, "33620")
        stripe = FakeStripe()

        count = expire_holds(fresh_db, stripe)

        assert count >= 1

        row = fresh_db.execute(
            text("SELECT status FROM zip_territories WHERE zip_code = '33620'")
        ).fetchone()
        assert row.status == "available"

    def test_no_refund_issued_on_expiry(self, fresh_db):
        self._create_expired_hold(fresh_db, "33621", stripe_pi="pi_exp_2")
        stripe = FakeStripe()

        expire_holds(fresh_db, stripe)

        assert len(stripe.refund_calls) == 0

    def test_does_not_release_converted_holds(self, fresh_db):
        """Holds that were converted are not processed by expire_holds."""
        dr = _make_deal_room(fresh_db, "33622", stripe_pi="pi_conv_1")
        now = datetime.now(timezone.utc)
        dr.held_at = now - timedelta(hours=72)
        dr.expires_at = now - timedelta(hours=24)
        dr.converted_at = now - timedelta(hours=10)  # already converted
        _seed_zip(fresh_db, "33622", "held")  # simulate ZIP already held
        fresh_db.flush()
        stripe = FakeStripe()

        count = expire_holds(fresh_db, stripe)

        # The converted hold should not be counted
        row = fresh_db.execute(
            text("SELECT status FROM zip_territories WHERE zip_code = '33622'")
        ).fetchone()
        assert row.status == "held"  # untouched

    def test_does_not_process_already_refunded(self, fresh_db):
        dr = _make_deal_room(fresh_db, "33623", stripe_pi="pi_ref_1")
        now = datetime.now(timezone.utc)
        dr.held_at = now - timedelta(hours=72)
        dr.expires_at = now - timedelta(hours=24)
        dr.refund_status = "refunded"
        _seed_zip(fresh_db, "33623", "held")  # simulate ZIP already held
        fresh_db.flush()
        stripe = FakeStripe()

        expire_holds(fresh_db, stripe)

        row = fresh_db.execute(
            text("SELECT status FROM zip_territories WHERE zip_code = '33623'")
        ).fetchone()
        assert row.status == "held"  # untouched — already refunded row excluded


# ---------------------------------------------------------------------------
# refund_on_conversion
# ---------------------------------------------------------------------------


class TestRefundOnConversion:
    def test_refunds_once(self, fresh_db):
        dr = _make_deal_room(fresh_db, "33630", stripe_pi="pi_conv_2")
        now = datetime.now(timezone.utc)
        dr.held_at = now - timedelta(hours=1)
        dr.expires_at = now + timedelta(hours=47)
        fresh_db.flush()
        stripe = FakeStripe()

        refund_on_conversion(fresh_db, stripe, token=dr.token)

        assert dr.refund_status == "refunded"
        assert dr.converted_at is not None
        assert len(stripe.refund_calls) == 1

    def test_idempotent_second_call_is_noop(self, fresh_db):
        dr = _make_deal_room(fresh_db, "33631", stripe_pi="pi_conv_3")
        now = datetime.now(timezone.utc)
        dr.held_at = now - timedelta(hours=1)
        dr.expires_at = now + timedelta(hours=47)
        fresh_db.flush()
        stripe = FakeStripe()

        refund_on_conversion(fresh_db, stripe, token=dr.token)
        refund_on_conversion(fresh_db, stripe, token=dr.token)  # second call

        assert len(stripe.refund_calls) == 1  # only one refund

    def test_refund_failure_marks_refund_failed(self, fresh_db):
        dr = _make_deal_room(fresh_db, "33632", stripe_pi="pi_fail_1")
        now = datetime.now(timezone.utc)
        dr.held_at = now - timedelta(hours=1)
        dr.expires_at = now + timedelta(hours=47)
        fresh_db.flush()
        stripe = FakeStripe(refund_raises=Exception("Stripe card error"))

        refund_on_conversion(fresh_db, stripe, token=dr.token)

        assert dr.refund_status == "refund_failed"
        assert dr.converted_at is not None  # still marked converted
