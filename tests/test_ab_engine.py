"""Tests for ab_engine.py"""

import pytest
import uuid
from unittest.mock import MagicMock

from sqlalchemy import select

from src.core.models import AbAssignment, AbTest


class TestAbEngineUnit:
    def test_variant_assignment_is_deterministic(self, mock_db):
        from src.services.ab_engine import assign_variant
        test = MagicMock()
        test.id = 1
        test.test_name = "test_a"
        test.traffic_pct = 100
        test.status = "active"
        # First call: return test, no existing assignment
        # Second call: return test, no existing assignment
        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None, test, None]
        mock_db.add = MagicMock()
        mock_db.flush = MagicMock()

        v1 = assign_variant(42, "test_a", mock_db)

        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None]
        v2 = assign_variant(42, "test_a", mock_db)
        assert v1 == v2
        assert v1 in ("a", "b")

    def test_traffic_pct_zero_excludes_all(self, mock_db):
        from src.services.ab_engine import assign_variant
        test = MagicMock()
        test.id = 1
        test.traffic_pct = 0
        test.status = "active"
        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None]
        result = assign_variant(1, "test_b", mock_db)
        assert result is None

    def test_inactive_test_returns_none(self, mock_db):
        from src.services.ab_engine import assign_variant
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = assign_variant(1, "nonexistent_test", mock_db)
        assert result is None

    def test_variant_is_a_or_b(self, mock_db):
        from src.services.ab_engine import assign_variant
        test = MagicMock()
        test.id = 1
        test.traffic_pct = 100
        test.status = "active"
        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None]
        mock_db.add = MagicMock()
        mock_db.flush = MagicMock()
        result = assign_variant(1, "test_c", mock_db)
        assert result in ("a", "b")


class TestAbEngineIntegration:
    def test_create_assign_record(self, fresh_db):
        import hashlib
        from src.services.ab_engine import assign_variant, record_outcome

        test_name = f"integration_test_{uuid.uuid4().hex[:8]}"

        # Insert test directly with traffic_pct=100 to guarantee assignment
        test = AbTest(
            test_name=test_name,
            segment="new",
            variant_a={"copy": "version_a"},
            variant_b={"copy": "version_b"},
            traffic_pct=100,
            status="active",
        )
        fresh_db.add(test)
        fresh_db.flush()

        from src.core.models import Subscriber
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_ab_{uid}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"ab-uuid-{uid}",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        variant = assign_variant(sub.id, test_name, fresh_db)
        assert variant in ("a", "b")

        # Same subscriber always gets same variant (idempotent)
        variant2 = assign_variant(sub.id, test_name, fresh_db)
        assert variant == variant2

        record_outcome(sub.id, test_name, "converted", fresh_db)

    def test_record_outcome_preserves_first_conversion(self, fresh_db):
        """PR #133 finding 3: record_revenue fires on every paid action, so a
        second 'converted' call must NOT advance outcome_at past the first
        conversion (which a time-windowed verdict relies on)."""
        from src.services.ab_engine import get_or_create_holdout_test, record_outcome
        from src.core.models import Subscriber, AbAssignment
        from sqlalchemy import select as _select

        test_name = f"holdout_first_{uuid.uuid4().hex[:8]}"
        test = get_or_create_holdout_test(
            test_name=test_name, segment="all", traffic_pct=90, db=fresh_db,
        )
        u = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_fc_{u}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"fc-{u}",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        fresh_db.add(AbAssignment(test_id=test.id, subscriber_id=sub.id, variant="variant"))
        fresh_db.flush()

        record_outcome(sub.id, test_name, "converted", fresh_db)
        row = fresh_db.execute(
            _select(AbAssignment).where(
                AbAssignment.test_id == test.id, AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one()
        first_ts = row.outcome_at
        assert first_ts is not None

        # Second paid action — must be a no-op on the already-converted row.
        record_outcome(sub.id, test_name, "converted", fresh_db)
        fresh_db.refresh(row)
        assert row.outcome_at == first_ts

    def test_holdout_test_traffic_pct_not_capped(self, fresh_db):
        """get_or_create_holdout_test must NOT apply the ab_test_traffic_cap
        (10%) that get_or_create_test does — a holdout's traffic_pct is the
        treatment majority (e.g. 90), not an AI-copy exposure cap. Capping it
        would invert the split to 90% control."""
        from src.services.ab_engine import get_or_create_holdout_test

        test_name = f"holdout_cap_{uuid.uuid4().hex[:8]}"
        test = get_or_create_holdout_test(
            test_name=test_name, segment="all", traffic_pct=90, db=fresh_db,
        )
        assert test.traffic_pct == 90  # not capped down to 10

    def test_holdout_split_is_roughly_control_pct(self, fresh_db):
        """End-to-end: a holdout registered at traffic_pct=90 must assign
        ~10% of subscribers to 'control', not ~90%. This is the assertion
        that catches the traffic-cap inversion."""
        from src.services.ab_engine import get_or_create_holdout_test, assign_rollout_arm
        from src.core.models import Subscriber

        test_name = f"holdout_split_{uuid.uuid4().hex[:8]}"
        get_or_create_holdout_test(
            test_name=test_name, segment="all", traffic_pct=90, db=fresh_db,
        )

        control = 0
        for _ in range(200):
            u = uuid.uuid4().hex[:8]
            sub = Subscriber(
                stripe_customer_id=f"cus_hs_{u}", tier="starter", vertical="roofing",
                county_id="hillsborough", event_feed_uuid=f"hs-{u}",
            )
            fresh_db.add(sub)
            fresh_db.flush()
            if assign_rollout_arm(sub.id, test_name, fresh_db) == "control":
                control += 1

        # ~10% control expected; generous band rules out the ~90% inversion.
        assert 5 <= control <= 35, f"expected ~10% control, got {control}/200"

    def test_record_pregenerated_arm(self, fresh_db):
        from src.services.ab_engine import record_pregenerated_arm
        from src.core.models import Subscriber

        test_name = f"pregenerated_test_{uuid.uuid4().hex[:8]}"
        test = AbTest(
            test_name=test_name,
            segment="new_signups",
            variant_a={"path": "control"},
            variant_b={"path": "annual_offer_shown"},
            traffic_pct=100,
            status="active",
        )
        fresh_db.add(test)
        fresh_db.flush()

        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_pregen_{uid}",
            tier="free",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"pregen-uuid-{uid}",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        # Trusts the caller-supplied arm rather than deriving one.
        arm = record_pregenerated_arm(sub.id, test_name, "variant", fresh_db)
        assert arm == "variant"

        # Idempotent — repeat calls (even with a different arm) return the
        # first recorded assignment unchanged.
        arm2 = record_pregenerated_arm(sub.id, test_name, "control", fresh_db)
        assert arm2 == "variant"

        assignment = fresh_db.execute(
            select(AbAssignment).where(
                AbAssignment.test_id == test.id,
                AbAssignment.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()
        assert assignment is not None
        assert assignment.variant == "variant"

    def test_record_pregenerated_arm_rejects_invalid_arm(self, fresh_db):
        from src.services.ab_engine import record_pregenerated_arm

        result = record_pregenerated_arm(1, "any_test", "not_a_real_arm", fresh_db)
        assert result is None

    def test_record_pregenerated_arm_missing_test_returns_none(self, fresh_db):
        from src.services.ab_engine import record_pregenerated_arm

        result = record_pregenerated_arm(1, f"nonexistent_{uuid.uuid4().hex[:8]}", "variant", fresh_db)
        assert result is None


class TestHoldoutVerdict:
    def test_no_test_returns_no_test_status(self, fresh_db):
        from src.services.ab_engine import holdout_verdict

        result = holdout_verdict(f"nonexistent_holdout_{uuid.uuid4().hex[:8]}", fresh_db)
        assert result["status"] == "no_test"

    def test_insufficient_data_below_min_per_arm(self, fresh_db):
        from src.services.ab_engine import assign_rollout_arm, holdout_verdict
        from src.core.models import Subscriber

        test_name = f"holdout_{uuid.uuid4().hex[:8]}"
        fresh_db.add(AbTest(
            test_name=test_name, segment="all",
            variant_a={"path": "control"}, variant_b={"path": "variant"},
            traffic_pct=90, status="active",
        ))
        fresh_db.flush()

        # Only 5 assignments total — nowhere near min_per_arm=30 default.
        for i in range(5):
            uid = uuid.uuid4().hex[:8]
            sub = Subscriber(
                stripe_customer_id=f"cus_hv_{uid}", tier="starter", vertical="roofing",
                county_id="hillsborough", event_feed_uuid=f"hv-uuid-{uid}",
            )
            fresh_db.add(sub)
            fresh_db.flush()
            assign_rollout_arm(sub.id, test_name, fresh_db)

        result = holdout_verdict(test_name, fresh_db)
        assert result["status"] == "insufficient_data"

    def test_proven_when_variant_beats_control_significantly(self, fresh_db):
        """40 control @ 5% conv vs 40 variant @ 40% conv — clear, well-powered win."""
        from src.services.ab_engine import record_outcome, holdout_verdict
        from src.core.models import Subscriber, AbAssignment

        test_name = f"holdout_{uuid.uuid4().hex[:8]}"
        test = AbTest(
            test_name=test_name, segment="all",
            variant_a={"path": "control"}, variant_b={"path": "variant"},
            traffic_pct=90, status="active",
        )
        fresh_db.add(test)
        fresh_db.flush()

        def _seed(arm: str, n: int, n_converted: int):
            for i in range(n):
                uid = uuid.uuid4().hex[:8]
                sub = Subscriber(
                    stripe_customer_id=f"cus_hv_{uid}", tier="starter", vertical="roofing",
                    county_id="hillsborough", event_feed_uuid=f"hv-uuid-{uid}",
                )
                fresh_db.add(sub)
                fresh_db.flush()
                assignment = AbAssignment(
                    test_id=test.id, subscriber_id=sub.id, variant=arm,
                    outcome="converted" if i < n_converted else None,
                )
                fresh_db.add(assignment)
            fresh_db.flush()

        _seed("control", 40, 2)   # 5%
        _seed("variant", 40, 16)  # 40%

        result = holdout_verdict(test_name, fresh_db)
        assert result["status"] == "proven"
        assert result["z_score"] > 2.0
        assert result["control_rate_pct"] == pytest.approx(5.0, abs=0.1)
        assert result["variant_rate_pct"] == pytest.approx(40.0, abs=0.1)

    def test_not_significant_when_rates_close(self, fresh_db):
        """40 control @ 30% vs 40 variant @ 32% — well-powered but no real gap."""
        from src.services.ab_engine import holdout_verdict
        from src.core.models import Subscriber, AbAssignment

        test_name = f"holdout_{uuid.uuid4().hex[:8]}"
        test = AbTest(
            test_name=test_name, segment="all",
            variant_a={"path": "control"}, variant_b={"path": "variant"},
            traffic_pct=90, status="active",
        )
        fresh_db.add(test)
        fresh_db.flush()

        def _seed(arm: str, n: int, n_converted: int):
            for i in range(n):
                uid = uuid.uuid4().hex[:8]
                sub = Subscriber(
                    stripe_customer_id=f"cus_hv_{uid}", tier="starter", vertical="roofing",
                    county_id="hillsborough", event_feed_uuid=f"hv-uuid-{uid}",
                )
                fresh_db.add(sub)
                fresh_db.flush()
                fresh_db.add(AbAssignment(
                    test_id=test.id, subscriber_id=sub.id, variant=arm,
                    outcome="converted" if i < n_converted else None,
                ))
            fresh_db.flush()

        _seed("control", 40, 12)  # 30%
        _seed("variant", 40, 13)  # 32.5%

        result = holdout_verdict(test_name, fresh_db)
        assert result["status"] == "not_significant"

    def test_conversion_window_excludes_late_outcomes(self, fresh_db):
        """retention_v1's 'any paid action within 7 days' shape: an outcome
        recorded 10 days after assignment must NOT count toward the
        conversion rate when conversion_window_days=7, even though
        outcome == 'converted'."""
        from datetime import datetime, timedelta, timezone
        from src.services.ab_engine import holdout_verdict
        from src.core.models import Subscriber, AbAssignment

        test_name = f"holdout_{uuid.uuid4().hex[:8]}"
        test = AbTest(
            test_name=test_name, segment="all",
            variant_a={"path": "control"}, variant_b={"path": "variant"},
            traffic_pct=90, status="active",
        )
        fresh_db.add(test)
        fresh_db.flush()

        assigned_at = datetime.now(timezone.utc) - timedelta(days=20)

        def _seed(arm: str, n: int, n_converted_in_window: int, n_converted_late: int = 0):
            for i in range(n):
                uid = uuid.uuid4().hex[:8]
                sub = Subscriber(
                    stripe_customer_id=f"cus_hv_{uid}", tier="starter", vertical="roofing",
                    county_id="hillsborough", event_feed_uuid=f"hv-uuid-{uid}",
                )
                fresh_db.add(sub)
                fresh_db.flush()
                if i < n_converted_in_window:
                    outcome, outcome_at = "converted", assigned_at + timedelta(days=3)
                elif i < n_converted_in_window + n_converted_late:
                    outcome, outcome_at = "converted", assigned_at + timedelta(days=10)
                else:
                    outcome, outcome_at = None, None
                fresh_db.add(AbAssignment(
                    test_id=test.id, subscriber_id=sub.id, variant=arm,
                    outcome=outcome, outcome_at=outcome_at, created_at=assigned_at,
                ))
            fresh_db.flush()

        # Both arms: 2 converted within the window, 20 converted LATE (day 10).
        # Unbounded, the late conversions would swamp the signal; windowed,
        # they must not count at all.
        _seed("control", 40, n_converted_in_window=2, n_converted_late=20)
        _seed("variant", 40, n_converted_in_window=2, n_converted_late=20)

        # window_hours must cover the 20-day-old assignments themselves —
        # this is a separate concept from conversion_window_days, which
        # bounds the gap between assignment and *outcome*, not how far back
        # assignments are queried from.
        result = holdout_verdict(test_name, fresh_db, window_hours=24 * 30, conversion_window_days=7)
        assert result["control_rate_pct"] == pytest.approx(5.0, abs=0.1)   # 2/40
        assert result["variant_rate_pct"] == pytest.approx(5.0, abs=0.1)   # 2/40, not 55/40

        # Sanity: without the window, the late conversions DO count.
        unbounded = holdout_verdict(test_name, fresh_db, window_hours=24 * 30)
        assert unbounded["control_rate_pct"] == pytest.approx(55.0, abs=0.1)  # 22/40

    def test_immature_assignments_excluded_from_denominator(self, fresh_db):
        """PR #133 finding 3: assignments whose 7-day window hasn't elapsed
        must not count in either arm — otherwise recent, not-yet-converted
        assignments dilute the rate. 40 mature converters + 40 assigned-today
        (immature) per arm → verdict sees 40/40 = 100%, not 40/80 = 50%."""
        from datetime import datetime, timedelta, timezone
        from src.services.ab_engine import holdout_verdict
        from src.core.models import Subscriber, AbAssignment

        test_name = f"holdout_{uuid.uuid4().hex[:8]}"
        test = AbTest(
            test_name=test_name, segment="all",
            variant_a={"path": "control"}, variant_b={"path": "variant"},
            traffic_pct=90, status="active",
        )
        fresh_db.add(test)
        fresh_db.flush()

        now = datetime.now(timezone.utc)
        mature_at = now - timedelta(days=20)   # window fully elapsed
        fresh_at = now                          # assigned "today" — immature

        def _seed(arm, created_at, n, converted):
            for i in range(n):
                uid = uuid.uuid4().hex[:8]
                sub = Subscriber(
                    stripe_customer_id=f"cus_im_{uid}", tier="starter", vertical="roofing",
                    county_id="hillsborough", event_feed_uuid=f"im-{uid}",
                )
                fresh_db.add(sub)
                fresh_db.flush()
                fresh_db.add(AbAssignment(
                    test_id=test.id, subscriber_id=sub.id, variant=arm,
                    outcome="converted" if i < converted else None,
                    outcome_at=created_at + timedelta(days=1) if i < converted else None,
                    created_at=created_at,
                ))
            fresh_db.flush()

        # Mature: 40/arm all converted. Immature: 40/arm none converted yet.
        _seed("control", mature_at, 40, 40)
        _seed("control", fresh_at, 40, 0)
        _seed("variant", mature_at, 40, 40)
        _seed("variant", fresh_at, 40, 0)

        result = holdout_verdict(test_name, fresh_db, window_hours=24 * 30, conversion_window_days=7)
        # Immature 40 excluded → n=40/arm, all converted → 100%, not 50%.
        assert result["n_ctrl"] == 40
        assert result["n_var"] == 40
        assert result["control_rate_pct"] == pytest.approx(100.0, abs=0.1)
