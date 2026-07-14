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
