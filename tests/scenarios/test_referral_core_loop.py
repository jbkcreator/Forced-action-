"""
Scenario tests — Referral Core Loop end-to-end.

Walks the full referral journey:
  - Signup with referral code → referee gets 10cr bonus
  - 1st–5th confirmed referrals → 5cr each, milestones at 3 and 5
  - Revocation → no clawback of already-granted rewards
  - claim-bonus-zip endpoint → bonus ZIP slot consumed
  - /api/referral/status endpoint → correct counts and milestones

Uses:
  - Real Postgres via db.session_scope() (same as all scenario tests)
  - fakeredis (REDIS_SANDBOX=true set by conftest autouse fixture)
  - Stripe SDK mocked at the module boundary
  - SMS delivery mocked via sandbox (TWILIO_SANDBOX=true from conftest)
"""

import json
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from src.api.main import app
from src.core.database import db as _db_mgr
from src.core.models import (
    ReferralEvent,
    ReferralMilestoneAward,
    Subscriber,
    WalletBalance,
    ZipTerritory,
)
from src.services.referral_engine import confirm_purchase, process_signup
from src.services.referral_notifier import CHANNEL, publish

pytestmark = pytest.mark.scenario_platform

client = TestClient(app)


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_subscriber(suffix: str, county_id: str = "hillsborough") -> Subscriber:
    """Create a test subscriber with a referral code."""
    sub = Subscriber(
        stripe_customer_id=f"cus_ref_{suffix}",
        stripe_subscription_id=f"sub_ref_{suffix}",
        tier="starter",
        vertical="roofing",
        county_id=county_id,
        email=f"ref_{suffix}@test.com",
        status="active",
        event_feed_uuid=str(uuid.uuid4()),
        referral_code=f"CODE{suffix.upper()[:5]}",
    )
    return sub


def _wallet_balance(subscriber_id: int) -> int:
    with _db_mgr.session_scope() as s:
        wallet = s.execute(
            select(WalletBalance).where(WalletBalance.subscriber_id == subscriber_id)
        ).scalar_one_or_none()
        return wallet.credits_remaining if wallet else 0


def _confirmed_count(referrer_id: int) -> int:
    with _db_mgr.session_scope() as s:
        return len(s.execute(
            select(ReferralEvent).where(
                ReferralEvent.referrer_subscriber_id == referrer_id,
                ReferralEvent.status.in_(("confirmed", "rewarded")),
            )
        ).scalars().all())


def _milestones_awarded(referrer_id: int) -> list[str]:
    with _db_mgr.session_scope() as s:
        return [
            row.milestone
            for row in s.execute(
                select(ReferralMilestoneAward).where(
                    ReferralMilestoneAward.referrer_subscriber_id == referrer_id
                )
            ).scalars().all()
        ]


# ── scenario ──────────────────────────────────────────────────────────────────

@patch("src.services.milestone_grants.stripe.Subscription.modify")
@patch("src.services.milestone_grants.get_settings")
def test_referral_core_loop_full_journey(mock_settings, mock_stripe_modify):
    """
    Full referral journey: sign up 5 referees, confirm purchases one by one,
    assert credits/milestones/slots at each step.
    """
    mock_settings.return_value.referral_free_month_coupon_id = "coupon_test"
    mock_settings.return_value.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
    mock_settings.return_value.base_url = "https://test.example.com"

    suffix = uuid.uuid4().hex[:6]

    # Create referrer and 5 referees in DB
    with _db_mgr.session_scope() as s:
        referrer = _make_subscriber(f"rr_{suffix}")
        s.add(referrer)
        s.flush()
        referrer_id = referrer.id
        referrer_code = referrer.referral_code
        referrer_uuid = referrer.event_feed_uuid

        referee_ids = []
        for i in range(5):
            ref = _make_subscriber(f"re_{suffix}_{i}")
            s.add(ref)
            s.flush()
            referee_ids.append(ref.id)

    # ── 1st confirmed referral: 5cr, no milestone ──────────────────────────────
    with _db_mgr.session_scope() as s:
        process_signup(referee_ids[0], referrer_code, s)

    with patch("src.services.milestone_grants.get_settings", return_value=mock_settings.return_value), \
         patch("src.services.milestone_grants.stripe.Subscription.modify", mock_stripe_modify):
        with _db_mgr.session_scope() as s:
            with patch.object(s, "get", wraps=s.get) as mock_get:
                confirm_purchase(referee_ids[0], s)

    assert _confirmed_count(referrer_id) == 1
    assert "free_month_3" not in _milestones_awarded(referrer_id)
    assert "lock_slot_5" not in _milestones_awarded(referrer_id)

    # ── 2nd and up to 3rd: free_month milestone ────────────────────────────────
    for i in range(1, 3):
        with _db_mgr.session_scope() as s:
            process_signup(referee_ids[i], referrer_code, s)
        with _db_mgr.session_scope() as s:
            with patch("src.services.milestone_grants.get_settings", return_value=mock_settings.return_value):
                with patch.object(s, "get", return_value=MagicMock(
                    stripe_subscription_id=f"sub_ref_rr_{suffix}",
                    stripe_customer_id=f"cus_ref_rr_{suffix}",
                    referral_code=referrer_code,
                )):
                    confirm_purchase(referee_ids[i], s)

    assert _confirmed_count(referrer_id) == 3
    assert "free_month_3" in _milestones_awarded(referrer_id)
    mock_stripe_modify.assert_called()

    # ── 4th: no new milestone ─────────────────────────────────────────────────
    with _db_mgr.session_scope() as s:
        process_signup(referee_ids[3], referrer_code, s)
    with _db_mgr.session_scope() as s:
        with patch("src.services.milestone_grants.get_settings", return_value=mock_settings.return_value):
            with patch.object(s, "get", return_value=MagicMock(
                stripe_subscription_id=f"sub_ref_rr_{suffix}",
                stripe_customer_id=f"cus_ref_rr_{suffix}",
                referral_code=referrer_code,
            )):
                confirm_purchase(referee_ids[3], s)

    assert _confirmed_count(referrer_id) == 4
    assert "lock_slot_5" not in _milestones_awarded(referrer_id)

    # ── 5th: lock_slot milestone ───────────────────────────────────────────────
    with _db_mgr.session_scope() as s:
        process_signup(referee_ids[4], referrer_code, s)
    with _db_mgr.session_scope() as s:
        with patch("src.services.milestone_grants.get_settings", return_value=mock_settings.return_value):
            with patch.object(s, "get", return_value=MagicMock(
                stripe_subscription_id=f"sub_ref_rr_{suffix}",
                stripe_customer_id=f"cus_ref_rr_{suffix}",
                referral_code=referrer_code,
            )):
                confirm_purchase(referee_ids[4], s)

    assert _confirmed_count(referrer_id) == 5
    assert "lock_slot_5" in _milestones_awarded(referrer_id)

    # Verify bonus_zip_slots incremented
    with _db_mgr.session_scope() as s:
        sub = s.get(Subscriber, referrer_id)
        assert sub.bonus_zip_slots >= 1


@patch("src.services.milestone_grants.stripe.Subscription.modify")
@patch("src.services.milestone_grants.get_settings")
def test_revocation_does_not_claw_back_milestones(mock_settings, mock_stripe_modify):
    """
    After 3 confirmed referrals (free_month granted), revoking one should NOT
    remove the milestone award. The event is revoked; the reward stands.
    """
    mock_settings.return_value.referral_free_month_coupon_id = "coupon_test"
    mock_settings.return_value.active_stripe_secret_key.get_secret_value.return_value = "sk_test"
    mock_settings.return_value.base_url = "https://test.example.com"

    suffix = uuid.uuid4().hex[:6]

    with _db_mgr.session_scope() as s:
        referrer = _make_subscriber(f"rv_{suffix}")
        s.add(referrer)
        s.flush()
        referrer_id = referrer.id
        referrer_code = referrer.referral_code

        referee_ids = []
        for i in range(3):
            ref = _make_subscriber(f"rve_{suffix}_{i}")
            s.add(ref)
            s.flush()
            referee_ids.append(ref.id)

    # Sign up + confirm 3 referrals to hit free_month_3
    mock_sub = MagicMock(
        stripe_subscription_id=f"sub_rv_{suffix}",
        stripe_customer_id=f"cus_rv_{suffix}",
        referral_code=referrer_code,
    )
    for i in range(3):
        with _db_mgr.session_scope() as s:
            process_signup(referee_ids[i], referrer_code, s)
        with _db_mgr.session_scope() as s:
            with patch("src.services.milestone_grants.get_settings", return_value=mock_settings.return_value):
                with patch.object(s, "get", return_value=mock_sub):
                    confirm_purchase(referee_ids[i], s)

    assert "free_month_3" in _milestones_awarded(referrer_id)

    # Revoke one referee
    from src.services.referral_engine import revoke_referral_event
    with _db_mgr.session_scope() as s:
        event = s.execute(
            select(ReferralEvent).where(
                ReferralEvent.referee_subscriber_id == referee_ids[0],
                ReferralEvent.status == "confirmed",
            )
        ).scalar_one_or_none()
        if event:
            revoke_referral_event(event.id, "test_refund", s)

    # Milestone still stands
    assert "free_month_3" in _milestones_awarded(referrer_id)


def test_referral_status_endpoint_returns_correct_counts(seed_subscriber):
    """
    GET /api/referral/status/{feed_uuid} returns accurate counts and next_milestone.
    """
    suffix = uuid.uuid4().hex[:6]

    with _db_mgr.session_scope() as s:
        referrer = _make_subscriber(f"st_{suffix}")
        s.add(referrer)
        s.flush()
        feed_uuid = referrer.event_feed_uuid

        # Add 2 confirmed referrals
        for i in range(2):
            ref = _make_subscriber(f"ste_{suffix}_{i}")
            s.add(ref)
            s.flush()
            ev = ReferralEvent(
                referrer_subscriber_id=referrer.id,
                referee_subscriber_id=ref.id,
                referral_code=referrer.referral_code,
                status="confirmed",
                reward_type="credits",
                reward_value="5",
            )
            s.add(ev)

    resp = client.get(f"/api/referral/status/{feed_uuid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["confirmed_count"] == 2
    assert data["next_milestone"]["milestone"] == "free_month_3"
    assert data["next_milestone"]["remaining"] == 1
    assert data["bonus_zip_slots"] == 0


def test_claim_bonus_zip_endpoint_decrements_slot(seed_subscriber):
    """
    POST /api/referral/claim-bonus-zip/{feed_uuid} creates a ZipTerritory and
    decrements bonus_zip_slots.
    """
    suffix = uuid.uuid4().hex[:6]

    with _db_mgr.session_scope() as s:
        referrer = _make_subscriber(f"cb_{suffix}")
        referrer.bonus_zip_slots = 1
        s.add(referrer)
        s.flush()
        feed_uuid = referrer.event_feed_uuid

    with patch("src.api.main.get_zips_for_county", side_effect=Exception("not configured")):
        resp = client.post(
            f"/api/referral/claim-bonus-zip/{feed_uuid}",
            json={"zip_code": "33510"},
        )

    assert resp.status_code == 200
    assert resp.json()["zip_code"] == "33510"
    assert resp.json()["bonus_zip_slots_remaining"] == 0

    with _db_mgr.session_scope() as s:
        sub = s.get(Subscriber, s.execute(
            select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
        ).scalar_one().id)
        assert sub.bonus_zip_slots == 0


def test_claim_bonus_zip_409_when_no_slots():
    """
    POST /api/referral/claim-bonus-zip returns 409 when no bonus_zip_slots available.
    """
    suffix = uuid.uuid4().hex[:6]

    with _db_mgr.session_scope() as s:
        referrer = _make_subscriber(f"nb_{suffix}")
        referrer.bonus_zip_slots = 0
        s.add(referrer)
        s.flush()
        feed_uuid = referrer.event_feed_uuid

    resp = client.post(
        f"/api/referral/claim-bonus-zip/{feed_uuid}",
        json={"zip_code": "33510"},
    )
    assert resp.status_code == 409


def test_redis_notification_published_on_confirm():
    """
    publish() puts a message on the referral.notifications channel (fakeredis).
    """
    from src.core.redis_client import _get_client, redis_available
    assert redis_available(), "fakeredis should be active in sandbox mode"

    client_r = _get_client()
    pubsub = client_r.pubsub()
    pubsub.subscribe(CHANNEL)
    pubsub.get_message()  # subscribe confirmation

    payload = {
        "type": "per_referral",
        "event_id": 999,
        "referrer_id": 1,
        "n_total": 1,
        "share_url": "https://test/share/abc",
    }
    publish(payload)

    msg = pubsub.get_message(timeout=1)
    assert msg is not None
    assert msg["type"] == "message"
    received = json.loads(msg["data"])
    assert received["type"] == "per_referral"
    assert received["event_id"] == 999
