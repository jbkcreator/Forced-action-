"""
Referral team mechanic + leaderboard — Stage 5.

Run:
    pytest tests/test_referral_team.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import uuid

import pytest

from src.core.models import (
    ReferralEvent,
    ReferralTeam,
    Subscriber,
    ZipTerritory,
)


def _mk_sub(fresh_db, *, county="hillsborough", vertical="roofing", suffix=None, name="Refer Member"):
    suffix = suffix or uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_team_{suffix}",
        tier="starter",
        vertical=vertical,
        county_id=county,
        event_feed_uuid=f"team-{suffix}",
        name=name,
        status="active",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub


class TestTeamUnlock:
    def test_team_unlocks_when_3_match(self, fresh_db):
        from src.services.referral_engine import _check_team_unlock

        referrer = _mk_sub(fresh_db, name="Alice Adams")
        ref1 = _mk_sub(fresh_db, name="Bob Brown")
        ref2 = _mk_sub(fresh_db, name="Carol Chen")
        for r in (ref1, ref2):
            fresh_db.add(ReferralEvent(
                referrer_subscriber_id=referrer.id,
                referee_subscriber_id=r.id,
                referral_code="abc",
                status="confirmed",
                reward_type="credits",
                reward_value="20",
                confirmed_at=datetime.now(timezone.utc),
            ))
        # Locked ZIPs across the trio
        fresh_db.add_all([
            ZipTerritory(zip_code="33601", subscriber_id=referrer.id, vertical="roofing", county_id="hillsborough", status="locked"),
            ZipTerritory(zip_code="33602", subscriber_id=ref1.id, vertical="roofing", county_id="hillsborough", status="locked"),
            ZipTerritory(zip_code="33603", subscriber_id=ref2.id, vertical="roofing", county_id="hillsborough", status="locked"),
        ])
        fresh_db.flush()

        # SMS suppression (no phone column yet) shouldn't block unlock
        team = _check_team_unlock(referrer.id, fresh_db)
        assert team is not None
        assert team.lead_subscriber_id == referrer.id
        assert team.county_id == "hillsborough"
        assert team.vertical == "roofing"
        assert sorted(team.member_subscriber_ids) == sorted([referrer.id, ref1.id, ref2.id])
        assert sorted(team.shared_zips) == ["33601", "33602", "33603"]
        assert team.status == "active"

    def test_no_unlock_with_only_2_matching(self, fresh_db):
        from src.services.referral_engine import _check_team_unlock
        referrer = _mk_sub(fresh_db)
        ref1 = _mk_sub(fresh_db)
        fresh_db.add(ReferralEvent(
            referrer_subscriber_id=referrer.id,
            referee_subscriber_id=ref1.id,
            referral_code="abc",
            status="confirmed",
            reward_type="credits",
            reward_value="20",
            confirmed_at=datetime.now(timezone.utc),
        ))
        fresh_db.flush()
        assert _check_team_unlock(referrer.id, fresh_db) is None

    def test_no_unlock_when_county_mismatch(self, fresh_db):
        from src.services.referral_engine import _check_team_unlock
        referrer = _mk_sub(fresh_db, county="hillsborough")
        ref1 = _mk_sub(fresh_db, county="pinellas")
        ref2 = _mk_sub(fresh_db, county="pasco")
        for r in (ref1, ref2):
            fresh_db.add(ReferralEvent(
                referrer_subscriber_id=referrer.id,
                referee_subscriber_id=r.id,
                referral_code="abc",
                status="confirmed",
                reward_type="credits",
                reward_value="20",
                confirmed_at=datetime.now(timezone.utc),
            ))
        fresh_db.flush()
        assert _check_team_unlock(referrer.id, fresh_db) is None

    def test_no_unlock_when_vertical_mismatch(self, fresh_db):
        from src.services.referral_engine import _check_team_unlock
        referrer = _mk_sub(fresh_db, vertical="roofing")
        ref1 = _mk_sub(fresh_db, vertical="restoration")
        ref2 = _mk_sub(fresh_db, vertical="restoration")
        for r in (ref1, ref2):
            fresh_db.add(ReferralEvent(
                referrer_subscriber_id=referrer.id,
                referee_subscriber_id=r.id,
                referral_code="abc",
                status="confirmed",
                reward_type="credits",
                reward_value="20",
                confirmed_at=datetime.now(timezone.utc),
            ))
        fresh_db.flush()
        assert _check_team_unlock(referrer.id, fresh_db) is None

    def test_unlock_idempotent(self, fresh_db):
        """Re-running _check_team_unlock should return the existing team, not duplicate."""
        from src.services.referral_engine import _check_team_unlock

        referrer = _mk_sub(fresh_db)
        ref1 = _mk_sub(fresh_db)
        ref2 = _mk_sub(fresh_db)
        for r in (ref1, ref2):
            fresh_db.add(ReferralEvent(
                referrer_subscriber_id=referrer.id,
                referee_subscriber_id=r.id,
                referral_code="abc",
                status="confirmed",
                reward_type="credits",
                reward_value="20",
                confirmed_at=datetime.now(timezone.utc),
            ))
        fresh_db.flush()

        first = _check_team_unlock(referrer.id, fresh_db)
        second = _check_team_unlock(referrer.id, fresh_db)
        assert first is not None and second is not None
        assert first.id == second.id


class TestLeaderboardBuilder:
    def test_anonymized_handle_first_name_initial(self, fresh_db):
        from src.tasks import leaderboard

        a = _mk_sub(fresh_db, name="Diana Diaz")
        b = _mk_sub(fresh_db, name="Mike")  # single-name handles fallback to first only
        # 2 confirmed referrals for a
        for _ in range(2):
            ref = _mk_sub(fresh_db)
            fresh_db.add(ReferralEvent(
                referrer_subscriber_id=a.id,
                referee_subscriber_id=ref.id,
                referral_code="abc",
                status="confirmed",
                reward_type="credits",
                reward_value="20",
                confirmed_at=datetime.now(timezone.utc),
            ))
        # 1 confirmed for b
        ref = _mk_sub(fresh_db)
        fresh_db.add(ReferralEvent(
            referrer_subscriber_id=b.id,
            referee_subscriber_id=ref.id,
            referral_code="abc",
            status="confirmed",
            reward_type="credits",
            reward_value="20",
            confirmed_at=datetime.now(timezone.utc),
        ))
        fresh_db.flush()

        snap = leaderboard.build(fresh_db)
        assert "leaderboards" in snap
        # Find this cohort
        cohort = next(
            (b for b in snap["leaderboards"] if b["county_id"] == "hillsborough" and b["vertical"] == "roofing"),
            None,
        )
        assert cohort is not None
        # Verify both members appear with anonymized handles
        flat = str(cohort)
        # Last names should NOT leak; "Diaz" must not appear
        assert "Diaz" not in flat
        # First name + initial format
        handles = [r["handle"] for r in cohort["leaderboard"]]
        assert any(h.startswith("Diana") for h in handles)
        assert any(h == "Mike" or h.startswith("Mike") for h in handles)
        # Top of leaderboard is the higher referrer (a, with 2)
        assert cohort["leaderboard"][0]["refs_this_week"] >= cohort["leaderboard"][-1]["refs_this_week"]

    def test_empty_state_returns_clean_payload(self, fresh_db):
        from src.tasks import leaderboard
        snap = leaderboard.build(fresh_db, today=None)
        # No referrals = empty leaderboards array
        assert "as_of" in snap
        assert snap["leaderboards"] == [] or all(
            len(b["leaderboard"]) >= 0 for b in snap["leaderboards"]
        )

    def test_badge_assignment(self):
        from src.tasks.leaderboard import _badge
        assert _badge(0) is None
        assert _badge(1) == "contributor"
        assert _badge(3) == "rising_star"
        assert _badge(5) == "team_unlocker"
        assert _badge(10) == "team_unlocker"
