"""
Tests for Sprint S0 reactivation foundation.

Covers:
  - Lifecycle flags: is_past_subscriber, is_unconverted, on_cooldown,
                     is_dormant, is_recently_contacted
  - Geo-interest: get_subscriber_zips_of_interest, get_subscriber_counties_of_interest
  - Cooldown gate
  - Sold-out ZIP supply gate: _has_sold_out_zip_supply (snapshot + direct fallback)
  - Eligibility gates: check_county_live_eligibility, check_sold_out_zip_eligibility
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch


# ── Subscriber factory ────────────────────────────────────────────────────────

def _sub(
    status="active",
    churned_at=None,
    is_trial=False,
    trial_ends_at=None,
    last_reactivation_attempt_at=None,
    email="user@example.com",
    phone="+13055551234",
    county_id="hillsborough",
    subscriber_id=1,
):
    sub = MagicMock()
    sub.id = subscriber_id
    sub.status = status
    sub.churned_at = churned_at
    sub.is_trial = is_trial
    sub.trial_ends_at = trial_ends_at
    sub.last_reactivation_attempt_at = last_reactivation_attempt_at
    sub.email = email
    sub.phone = phone
    sub.county_id = county_id
    return sub


def _mock_db(first_return=None, scalar_return=None, iter_returns=None):
    """
    Build a MagicMock Session whose execute().first() and execute().scalar()
    return the supplied values. Pass iter_returns as a list of iterables for
    sequential db.execute() calls that are iterated over directly.
    """
    db = MagicMock()
    if iter_returns is not None:
        db.execute.side_effect = iter_returns
    else:
        db.execute.return_value.first.return_value = first_return
        db.execute.return_value.scalar.return_value = scalar_return
    return db


# ═══════════════════════════════════════════════════════════════════════════════
# 1. LIFECYCLE FLAGS
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsPastSubscriber:
    def test_churned_at_set(self):
        from src.services.reactivation_eligibility import is_past_subscriber
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        assert is_past_subscriber(sub) is True

    def test_churned_at_none(self):
        from src.services.reactivation_eligibility import is_past_subscriber
        assert is_past_subscriber(_sub()) is False

    def test_status_churned_without_churned_at_is_not_past(self):
        from src.services.reactivation_eligibility import is_past_subscriber
        assert is_past_subscriber(_sub(status="churned", churned_at=None)) is False

    def test_active_subscriber_with_churned_at_is_past(self):
        # Handles re-subscribed users who previously churned
        from src.services.reactivation_eligibility import is_past_subscriber
        sub = _sub(status="active", churned_at=datetime.now(timezone.utc) - timedelta(days=60))
        assert is_past_subscriber(sub) is True


class TestIsUnconverted:
    def test_trial_expired_and_not_active(self):
        from src.services.reactivation_eligibility import is_unconverted
        sub = _sub(
            is_trial=True,
            status="grace",
            trial_ends_at=datetime.now(timezone.utc) - timedelta(days=3),
        )
        assert is_unconverted(sub) is True

    def test_active_trial_is_not_unconverted(self):
        from src.services.reactivation_eligibility import is_unconverted
        assert is_unconverted(_sub(is_trial=True, status="active")) is False

    def test_non_trial_subscriber_is_not_unconverted(self):
        from src.services.reactivation_eligibility import is_unconverted
        assert is_unconverted(_sub(is_trial=False, status="churned")) is False

    def test_trial_flag_set_but_no_end_date(self):
        from src.services.reactivation_eligibility import is_unconverted
        sub = _sub(is_trial=True, status="cancelled", trial_ends_at=None)
        assert is_unconverted(sub) is True

    def test_trial_end_in_future_is_not_unconverted(self):
        from src.services.reactivation_eligibility import is_unconverted
        sub = _sub(
            is_trial=True,
            status="active",
            trial_ends_at=datetime.now(timezone.utc) + timedelta(days=5),
        )
        assert is_unconverted(sub) is False

    def test_trial_ends_at_timezone_naive_handled(self):
        from src.services.reactivation_eligibility import is_unconverted
        naive_past = datetime.now() - timedelta(days=2)
        sub = _sub(is_trial=True, status="cancelled", trial_ends_at=naive_past)
        assert is_unconverted(sub) is True


class TestOnCooldown:
    def test_no_prior_attempt(self):
        from src.services.reactivation_eligibility import on_cooldown
        assert on_cooldown(_sub(last_reactivation_attempt_at=None)) is False

    def test_attempt_within_3_days_is_on_cooldown(self):
        from src.services.reactivation_eligibility import on_cooldown
        sub = _sub(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(days=1))
        assert on_cooldown(sub) is True

    def test_attempt_beyond_3_days_is_not_on_cooldown(self):
        from src.services.reactivation_eligibility import on_cooldown
        sub = _sub(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(days=4))
        assert on_cooldown(sub) is False

    def test_just_past_cooldown_boundary(self):
        from src.services.reactivation_eligibility import on_cooldown
        sub = _sub(
            last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(days=3, seconds=1)
        )
        assert on_cooldown(sub) is False

    def test_custom_cooldown_days(self):
        from src.services.reactivation_eligibility import on_cooldown
        sub = _sub(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(days=6))
        assert on_cooldown(sub, cooldown_days=7) is True
        assert on_cooldown(sub, cooldown_days=5) is False

    def test_timezone_naive_last_attempt_handled(self):
        from src.services.reactivation_eligibility import on_cooldown
        naive_recent = datetime.now() - timedelta(hours=12)
        sub = _sub(last_reactivation_attempt_at=naive_recent)
        assert on_cooldown(sub) is True


class TestIsDormant:
    def test_recent_reply_means_not_dormant(self):
        from src.services.reactivation_eligibility import is_dormant
        db = _mock_db(first_return=(1,))
        assert is_dormant(1, db) is False

    def test_no_user_activity_is_dormant(self):
        from src.services.reactivation_eligibility import is_dormant
        db = _mock_db(first_return=None)
        assert is_dormant(1, db) is True

    def test_custom_inactive_days_passed_to_query(self):
        from src.services.reactivation_eligibility import is_dormant
        db = _mock_db(first_return=None)
        result = is_dormant(1, db, inactive_days=60)
        assert result is True
        db.execute.assert_called_once()
        call_params = db.execute.call_args[0][1]
        assert "cutoff" in call_params


class TestIsRecentlyContacted:
    def test_recent_outbound_message(self):
        from src.services.reactivation_eligibility import is_recently_contacted
        db = _mock_db(first_return=(1,))
        assert is_recently_contacted(1, db) is True

    def test_no_outbound_message(self):
        from src.services.reactivation_eligibility import is_recently_contacted
        db = _mock_db(first_return=None)
        assert is_recently_contacted(1, db) is False

    def test_custom_within_days(self):
        from src.services.reactivation_eligibility import is_recently_contacted
        db = _mock_db(first_return=None)
        is_recently_contacted(1, db, within_days=7)
        call_params = db.execute.call_args[0][1]
        assert "cutoff" in call_params


# ═══════════════════════════════════════════════════════════════════════════════
# 2. GEO-INTEREST
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetSubscriberZipsOfInterest:
    def _db_with_zip_results(self, locked, waitlist, engagement, wallet):
        db = MagicMock()
        db.execute.side_effect = [
            iter([(z,) for z in locked]),
            iter([(z,) for z in waitlist]),
            iter([(z,) for z in engagement]),
            iter([(z,) for z in wallet]),
        ]
        return db

    def test_returns_all_four_categories(self):
        from src.services.geo_interest import get_subscriber_zips_of_interest
        db = self._db_with_zip_results(["33601"], ["33701"], [], [])
        result = get_subscriber_zips_of_interest(1, db)
        assert set(result.keys()) == {"locked", "waitlist", "engagement", "wallet"}

    def test_locked_zips_populated(self):
        from src.services.geo_interest import get_subscriber_zips_of_interest
        db = self._db_with_zip_results(["33601", "33602"], [], [], [])
        result = get_subscriber_zips_of_interest(1, db)
        assert result["locked"] == ["33601", "33602"]

    def test_empty_categories_return_empty_lists(self):
        from src.services.geo_interest import get_subscriber_zips_of_interest
        db = self._db_with_zip_results([], [], [], [])
        result = get_subscriber_zips_of_interest(1, db)
        assert result["locked"] == []
        assert result["waitlist"] == []
        assert result["engagement"] == []
        assert result["wallet"] == []

    def test_all_categories_independent(self):
        from src.services.geo_interest import get_subscriber_zips_of_interest
        db = self._db_with_zip_results(["33601"], ["33701"], ["33612"], ["33647"])
        result = get_subscriber_zips_of_interest(1, db)
        assert "33601" in result["locked"]
        assert "33701" in result["waitlist"]
        assert "33612" in result["engagement"]
        assert "33647" in result["wallet"]


class TestGetSubscriberCountiesOfInterest:
    def test_returns_list_of_strings(self):
        from src.services.geo_interest import get_subscriber_counties_of_interest
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = ["hillsborough"]
        result = get_subscriber_counties_of_interest(1, db)
        assert isinstance(result, list)
        assert "hillsborough" in result

    def test_filters_out_none_values(self):
        from src.services.geo_interest import get_subscriber_counties_of_interest
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = [
            "hillsborough", None, "pinellas"
        ]
        result = get_subscriber_counties_of_interest(1, db)
        assert None not in result
        assert len(result) == 2

    def test_single_execute_call_uses_union(self):
        from src.services.geo_interest import get_subscriber_counties_of_interest
        db = MagicMock()
        db.execute.return_value.scalars.return_value.all.return_value = []
        get_subscriber_counties_of_interest(1, db)
        assert db.execute.call_count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SOLD-OUT ZIP SUPPLY GATE
# ═══════════════════════════════════════════════════════════════════════════════

class TestHasSoldOutZipSupply:
    def _db_snapshot(self, count):
        snap = MagicMock()
        snap.first.return_value = (count,) if count is not None else None
        db = MagicMock()
        db.execute.return_value = snap
        return db

    def test_snapshot_with_leads_returns_true(self):
        from src.services.reactivation_eligibility import _has_sold_out_zip_supply
        db = MagicMock()
        snap_result = MagicMock()
        snap_result.first.return_value = (3,)
        direct_result = MagicMock()
        direct_result.scalar.return_value = 3
        db.execute.side_effect = [snap_result, direct_result]
        assert _has_sold_out_zip_supply("33701", "pinellas", "roofing", db) is True

    def test_snapshot_with_zero_leads_short_circuits_to_false(self):
        from src.services.reactivation_eligibility import _has_sold_out_zip_supply
        db = MagicMock()
        snap_result = MagicMock()
        snap_result.first.return_value = (0,)
        db.execute.return_value = snap_result
        assert _has_sold_out_zip_supply("33701", "pinellas", "roofing", db) is False
        assert db.execute.call_count == 1

    def test_missing_snapshot_falls_back_to_direct_query(self):
        from src.services.reactivation_eligibility import _has_sold_out_zip_supply
        db = MagicMock()
        snap_result = MagicMock()
        snap_result.first.return_value = None
        direct_result = MagicMock()
        direct_result.scalar.return_value = 2
        db.execute.side_effect = [snap_result, direct_result]
        assert _has_sold_out_zip_supply("33701", "pinellas", "roofing", db) is True
        assert db.execute.call_count == 2

    def test_missing_snapshot_no_direct_leads_returns_false(self):
        from src.services.reactivation_eligibility import _has_sold_out_zip_supply
        db = MagicMock()
        snap_result = MagicMock()
        snap_result.first.return_value = None
        direct_result = MagicMock()
        direct_result.scalar.return_value = 0
        db.execute.side_effect = [snap_result, direct_result]
        assert _has_sold_out_zip_supply("33701", "pinellas", "roofing", db) is False

    def test_direct_query_receives_vertical_param(self):
        from src.services.reactivation_eligibility import _has_sold_out_zip_supply_direct
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 0
        _has_sold_out_zip_supply_direct("33701", "pinellas", "restoration", db)
        call_params = db.execute.call_args[0][1]
        assert call_params["vertical"] == "restoration"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. ELIGIBILITY GATES
# ═══════════════════════════════════════════════════════════════════════════════

class TestCheckCountyLiveEligibility:
    def test_on_cooldown_blocks(self):
        from src.services.reactivation_eligibility import check_county_live_eligibility
        sub = _sub(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(hours=6))
        db = MagicMock()
        eligible, reason = check_county_live_eligibility(sub, "pinellas", db)
        assert eligible is False
        assert reason == "on_cooldown"

    def test_no_contact_info_blocks(self):
        from src.services.reactivation_eligibility import check_county_live_eligibility
        sub = _sub(
            email=None,
            phone=None,
            churned_at=datetime.now(timezone.utc) - timedelta(days=60),
        )
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False):
            db = MagicMock()
            eligible, reason = check_county_live_eligibility(sub, "pinellas", db)
        assert eligible is False
        assert reason == "no_contact_info"

    def test_no_geo_interest_blocks(self):
        from src.services.reactivation_eligibility import check_county_live_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False), \
             patch("src.services.geo_interest.get_subscriber_counties_of_interest",
                   return_value=["hillsborough"]):
            db = MagicMock()
            eligible, reason = check_county_live_eligibility(sub, "pinellas", db)
        assert eligible is False
        assert "no_county_interest" in reason

    def test_past_subscriber_with_county_interest_eligible(self):
        from src.services.reactivation_eligibility import check_county_live_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False), \
             patch("src.services.geo_interest.get_subscriber_counties_of_interest",
                   return_value=["pinellas"]):
            db = MagicMock()
            eligible, reason = check_county_live_eligibility(sub, "pinellas", db)
        assert eligible is True
        assert reason == "eligible"

    def test_active_but_dormant_subscriber_eligible(self):
        from src.services.reactivation_eligibility import check_county_live_eligibility
        sub = _sub(status="active")
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=True), \
             patch("src.services.geo_interest.get_subscriber_counties_of_interest",
                   return_value=["pinellas"]):
            db = MagicMock()
            eligible, reason = check_county_live_eligibility(sub, "pinellas", db)
        assert eligible is True


class TestCheckSoldOutZipEligibility:
    def test_on_cooldown_blocks(self):
        from src.services.reactivation_eligibility import check_sold_out_zip_eligibility
        sub = _sub(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(hours=12))
        db = MagicMock()
        eligible, reason = check_sold_out_zip_eligibility(sub, "33701", "roofing", "pinellas", db)
        assert eligible is False
        assert reason == "on_cooldown"

    def test_no_contact_info_blocks(self):
        from src.services.reactivation_eligibility import check_sold_out_zip_eligibility
        sub = _sub(
            email=None,
            phone=None,
            churned_at=datetime.now(timezone.utc) - timedelta(days=30),
        )
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False):
            db = MagicMock()
            eligible, reason = check_sold_out_zip_eligibility(
                sub, "33701", "roofing", "pinellas", db
            )
        assert eligible is False
        assert reason == "no_contact_info"

    def test_no_zip_interest_blocks(self):
        from src.services.reactivation_eligibility import check_sold_out_zip_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        empty_zips = {"locked": [], "waitlist": [], "engagement": [], "wallet": []}
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False), \
             patch("src.services.geo_interest.get_subscriber_zips_of_interest",
                   return_value=empty_zips):
            db = MagicMock()
            eligible, reason = check_sold_out_zip_eligibility(
                sub, "33701", "roofing", "pinellas", db
            )
        assert eligible is False
        assert "no_zip_interest" in reason

    def test_no_supply_blocks(self):
        from src.services.reactivation_eligibility import check_sold_out_zip_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        matching_zips = {"locked": ["33701"], "waitlist": [], "engagement": [], "wallet": []}
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False), \
             patch("src.services.geo_interest.get_subscriber_zips_of_interest",
                   return_value=matching_zips), \
             patch("src.services.reactivation_eligibility._has_sold_out_zip_supply",
                   return_value=False):
            db = MagicMock()
            eligible, reason = check_sold_out_zip_eligibility(
                sub, "33701", "roofing", "pinellas", db
            )
        assert eligible is False
        assert "no_supply" in reason

    def test_all_gates_pass_eligible(self):
        from src.services.reactivation_eligibility import check_sold_out_zip_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        matching_zips = {"locked": ["33701"], "waitlist": [], "engagement": [], "wallet": []}
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False), \
             patch("src.services.geo_interest.get_subscriber_zips_of_interest",
                   return_value=matching_zips), \
             patch("src.services.reactivation_eligibility._has_sold_out_zip_supply",
                   return_value=True):
            db = MagicMock()
            eligible, reason = check_sold_out_zip_eligibility(
                sub, "33701", "roofing", "pinellas", db
            )
        assert eligible is True
        assert reason == "eligible"

    def test_interest_from_waitlist_counts(self):
        from src.services.reactivation_eligibility import check_sold_out_zip_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        waitlist_zips = {"locked": [], "waitlist": ["33701"], "engagement": [], "wallet": []}
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False), \
             patch("src.services.geo_interest.get_subscriber_zips_of_interest",
                   return_value=waitlist_zips), \
             patch("src.services.reactivation_eligibility._has_sold_out_zip_supply",
                   return_value=True):
            db = MagicMock()
            eligible, _ = check_sold_out_zip_eligibility(
                sub, "33701", "roofing", "pinellas", db
            )
        assert eligible is True

    def test_interest_from_engagement_counts(self):
        from src.services.reactivation_eligibility import check_sold_out_zip_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=30))
        engagement_zips = {"locked": [], "waitlist": [], "engagement": ["33701"], "wallet": []}
        with patch("src.services.reactivation_eligibility.is_dormant", return_value=False), \
             patch("src.services.geo_interest.get_subscriber_zips_of_interest",
                   return_value=engagement_zips), \
             patch("src.services.reactivation_eligibility._has_sold_out_zip_supply",
                   return_value=True):
            db = MagicMock()
            eligible, _ = check_sold_out_zip_eligibility(
                sub, "33701", "roofing", "pinellas", db
            )
        assert eligible is True


# ═══════════════════════════════════════════════════════════════════════════════
# 5. INTEGRATION (requires Postgres — skipped if DATABASE_URL not set)
# ═══════════════════════════════════════════════════════════════════════════════

class TestLifecycleIntegration:
    def test_is_dormant_with_real_db(self, fresh_db):
        from src.core.models import Subscriber
        from src.services.reactivation_eligibility import is_dormant

        sub = Subscriber(
            stripe_customer_id="cus_s0_dormant_1",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        assert is_dormant(sub.id, fresh_db) is True

    def test_is_recently_contacted_no_messages(self, fresh_db):
        from src.core.models import Subscriber
        from src.services.reactivation_eligibility import is_recently_contacted

        sub = Subscriber(
            stripe_customer_id="cus_s0_contact_1",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        assert is_recently_contacted(sub.id, fresh_db) is False

    def test_geo_interest_county_includes_subscriber_county(self, fresh_db):
        from src.core.models import Subscriber
        from src.services.geo_interest import get_subscriber_counties_of_interest

        sub = Subscriber(
            stripe_customer_id="cus_s0_geo_1",
            tier="starter",
            vertical="roofing",
            county_id="pinellas",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        counties = get_subscriber_counties_of_interest(sub.id, fresh_db)
        assert "pinellas" in counties

    def test_geo_interest_zips_empty_for_new_subscriber(self, fresh_db):
        from src.core.models import Subscriber
        from src.services.geo_interest import get_subscriber_zips_of_interest

        sub = Subscriber(
            stripe_customer_id="cus_s0_geo_2",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        zips = get_subscriber_zips_of_interest(sub.id, fresh_db)
        assert zips["locked"] == []
        assert zips["waitlist"] == []
        assert zips["engagement"] == []
        assert zips["wallet"] == []


# ═══════════════════════════════════════════════════════════════════════════════
# 6. TIER3 WIN-BACK ELIGIBILITY (T-B12-07)
# ═══════════════════════════════════════════════════════════════════════════════

class TestCheckTier3WinbackEligibility:
    def test_on_cooldown_blocks(self):
        from src.services.reactivation_eligibility import check_tier3_winback_eligibility
        sub = _sub(last_reactivation_attempt_at=datetime.now(timezone.utc) - timedelta(hours=6))
        db = MagicMock()
        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        assert eligible is False
        assert reason == "on_cooldown"
        assert branch is None

    def test_never_subscribed_blocks(self):
        from src.services.reactivation_eligibility import check_tier3_winback_eligibility
        sub = _sub(churned_at=None)
        db = MagicMock()
        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        assert eligible is False
        assert reason == "not_lapsed"
        assert branch is None

    def test_no_contact_info_blocks(self):
        from src.services.reactivation_eligibility import check_tier3_winback_eligibility
        sub = _sub(email=None, phone=None, churned_at=datetime.now(timezone.utc) - timedelta(days=10))
        db = MagicMock()
        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        assert eligible is False
        assert reason == "no_contact_info"
        assert branch is None

    def test_lapsed_under_30d_with_zip_still_held_is_zip_held_branch(self):
        from src.services.reactivation_eligibility import check_tier3_winback_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=10))
        db = _mock_db(first_return=(1,))  # zip_territories row found → still held
        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        assert eligible is True
        assert reason == "eligible"
        assert branch == "zip_held"

    def test_lapsed_under_30d_with_no_zip_held_is_zip_released_branch(self):
        from src.services.reactivation_eligibility import check_tier3_winback_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=10))
        db = _mock_db(first_return=None)  # no locked/grace zip_territories row
        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        assert eligible is True
        assert branch == "zip_released"

    def test_lapsed_over_30d_is_zip_released_branch_even_if_zip_row_exists(self):
        from src.services.reactivation_eligibility import check_tier3_winback_eligibility
        sub = _sub(churned_at=datetime.now(timezone.utc) - timedelta(days=45))
        db = _mock_db(first_return=(1,))
        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        assert eligible is True
        assert branch == "zip_released"
