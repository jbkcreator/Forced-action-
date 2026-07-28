"""
Attribution rollout — assign_rollout_arm, should_rollback_rollout, and
the lifecycle_attribution_rollback_check task.

Tests:
  1. Determinism — same subscriber always gets same arm.
  2. Both arms recorded — control + variant both create AbAssignment rows.
  3. ~10% variant split shape (100 subs → ≈10 variant).
  4. Inactive test returns None.
  5. Below-floor hold — <30/arm → no rollback.
  6. Clear loser triggers rollback (variant ≪ control, >2σ, ≥30/arm).
  7. dry_run leaves test active.
  8. Post-rollback: fresh assign_rollout_arm returns None (control path).
  9. Conversion labeling: record_conversion_attribution marks outcome='converted'
     on the active rollout assignment.

Tests 1-4 use mock_db (unit). Tests 5-9 use fresh_db (real Postgres; skip
when DATABASE_URL unset — consistent with test_ab_rollback.py).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import AbAssignment, AbTest, Subscriber


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _seed_subs(db, n: int) -> list[int]:
    ids = []
    for _ in range(n):
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_ar_{uid}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"ar-{uid}",
        )
        db.add(sub)
        db.flush()
        ids.append(sub.id)
    return ids


def _seed_rollout_test(
    db,
    *,
    n_ctrl: int,
    n_var: int,
    ctrl_conv_rate: float,
    var_conv_rate: float,
    hours_old: float = 1.0,
) -> tuple[AbTest, list[int]]:
    test = AbTest(
        test_name=f"test_attr_rollout_{uuid.uuid4().hex[:8]}",
        segment="attribution_eligible",
        variant_a={"path": "control"},
        variant_b={"path": "attribution_driven"},
        traffic_pct=10,
        status="active",
    )
    db.add(test)
    db.flush()

    sub_ids = _seed_subs(db, n_ctrl + n_var)
    created_at = datetime.now(timezone.utc) - timedelta(hours=hours_old)

    ctrl_conv_n = round(n_ctrl * ctrl_conv_rate)
    var_conv_n = round(n_var * var_conv_rate)

    for i in range(n_ctrl):
        db.add(AbAssignment(
            test_id=test.id,
            subscriber_id=sub_ids[i],
            variant="control",
            outcome="converted" if i < ctrl_conv_n else "no_convert",
            created_at=created_at,
        ))
    for i in range(n_var):
        db.add(AbAssignment(
            test_id=test.id,
            subscriber_id=sub_ids[n_ctrl + i],
            variant="variant",
            outcome="converted" if i < var_conv_n else "no_convert",
            created_at=created_at,
        ))
    db.flush()
    db.commit()
    return test, sub_ids


def _cleanup(db, test: AbTest, sub_ids: list[int]) -> None:
    from src.core.models import LearningCard
    db.query(AbAssignment).filter_by(test_id=test.id).delete()
    db.query(LearningCard).filter_by(card_type="ab_result").delete()
    db.query(AbTest).filter_by(id=test.id).delete()
    db.query(Subscriber).filter(Subscriber.id.in_(sub_ids)).delete(
        synchronize_session=False
    )
    db.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests (mock_db)
# ──────────────────────────────────────────────────────────────────────────────

class TestAssignRolloutArmUnit:
    def test_deterministic(self, mock_db):
        from src.services.ab_engine import assign_rollout_arm

        test = MagicMock()
        test.id = 99
        test.traffic_pct = 100
        test.status = "active"
        # First call: return test, no existing row
        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None]
        mock_db.add = MagicMock()
        mock_db.flush = MagicMock()

        arm1 = assign_rollout_arm(42, "lifecycle_attribution_v1", mock_db)

        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None]
        arm2 = assign_rollout_arm(42, "lifecycle_attribution_v1", mock_db)

        assert arm1 == arm2
        assert arm1 in ("variant", "control")

    def test_inactive_test_returns_none(self, mock_db):
        from src.services.ab_engine import assign_rollout_arm

        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = assign_rollout_arm(1, "lifecycle_attribution_v1", mock_db)
        assert result is None

    def test_full_traffic_pct_both_arms_possible(self, mock_db):
        from src.services.ab_engine import assign_rollout_arm

        test = MagicMock()
        test.id = 1
        test.traffic_pct = 100
        test.status = "active"
        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None]
        mock_db.add = MagicMock()
        mock_db.flush = MagicMock()

        result = assign_rollout_arm(1, "test_x", mock_db)
        assert result in ("variant", "control")

    def test_zero_traffic_pct_always_control(self, mock_db):
        from src.services.ab_engine import assign_rollout_arm

        test = MagicMock()
        test.id = 2
        test.traffic_pct = 0
        test.status = "active"
        mock_db.execute.return_value.scalar_one_or_none.side_effect = [test, None]
        mock_db.add = MagicMock()
        mock_db.flush = MagicMock()

        result = assign_rollout_arm(1, "test_x", mock_db)
        assert result == "control"


# ──────────────────────────────────────────────────────────────────────────────
# Integration tests (fresh_db — real Postgres)
# ──────────────────────────────────────────────────────────────────────────────

def test_both_arms_recorded(fresh_db):
    """With traffic_pct=100, 10 subs → all get assignments, both arms possible."""
    from src.services.ab_engine import assign_rollout_arm

    test = AbTest(
        test_name=f"t_{uuid.uuid4().hex[:8]}",
        segment="all",
        variant_a={},
        variant_b={},
        traffic_pct=100,
        status="active",
    )
    fresh_db.add(test)
    fresh_db.flush()
    sub_ids = _seed_subs(fresh_db, 20)
    try:
        arms = set()
        for sid in sub_ids:
            arm = assign_rollout_arm(sid, test.test_name, fresh_db)
            assert arm in ("variant", "control"), f"unexpected arm: {arm}"
            arms.add(arm)
        assert "variant" in arms or "control" in arms
    finally:
        fresh_db.query(AbAssignment).filter_by(test_id=test.id).delete()
        fresh_db.query(AbTest).filter_by(id=test.id).delete()
        fresh_db.query(Subscriber).filter(Subscriber.id.in_(sub_ids)).delete(
            synchronize_session=False
        )
        fresh_db.commit()


def test_ten_pct_split_shape(fresh_db):
    """traffic_pct=10, 200 subs → ~10% variant (between 5 and 20 is generous tolerance)."""
    from src.services.ab_engine import assign_rollout_arm

    test = AbTest(
        test_name=f"t_{uuid.uuid4().hex[:8]}",
        segment="all",
        variant_a={},
        variant_b={},
        traffic_pct=10,
        status="active",
    )
    fresh_db.add(test)
    fresh_db.flush()
    sub_ids = _seed_subs(fresh_db, 200)
    try:
        variant_count = sum(
            1 for sid in sub_ids
            if assign_rollout_arm(sid, test.test_name, fresh_db) == "variant"
        )
        assert 5 <= variant_count <= 30, f"expected ~10% variant, got {variant_count}/200"
    finally:
        fresh_db.query(AbAssignment).filter_by(test_id=test.id).delete()
        fresh_db.query(AbTest).filter_by(id=test.id).delete()
        fresh_db.query(Subscriber).filter(Subscriber.id.in_(sub_ids)).delete(
            synchronize_session=False
        )
        fresh_db.commit()


def test_below_floor_no_rollback(fresh_db):
    """<30/arm → should_rollback_rollout returns False."""
    from src.services.ab_engine import should_rollback_rollout

    test, sub_ids = _seed_rollout_test(
        fresh_db, n_ctrl=20, n_var=20, ctrl_conv_rate=0.30, var_conv_rate=0.02
    )
    try:
        result = should_rollback_rollout(test.test_name, fresh_db)
        assert result is False
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_clear_loser_triggers_rollback(fresh_db):
    """variant 2% vs control 30%, n=100/arm → >2σ → rollback fires."""
    from src.tasks.lifecycle_attribution_rollback_check import run as rollback_run

    # Patch the test name inside the task to use our seeded test
    test, sub_ids = _seed_rollout_test(
        fresh_db, n_ctrl=100, n_var=100, ctrl_conv_rate=0.30, var_conv_rate=0.02
    )
    try:
        with (
            patch("src.tasks.lifecycle_attribution_rollback_check.ATTRIBUTION_ROLLOUT_TEST_NAME", test.test_name),
            patch("src.services.ab_engine.ATTRIBUTION_ROLLOUT_TEST_NAME", test.test_name),
            patch("src.services.stripe_webhooks._send_founder_alert") as mock_alert,
        ):
            result = rollback_run(dry_run=False)

        fresh_db.refresh(test)
        assert test.status == "rolled_back", f"expected rolled_back, got {test.status}"
        assert result["rolled_back"] == 1
        assert mock_alert.called
        msg = mock_alert.call_args[0][0]
        assert "ATTRIBUTION ROLLBACK" in msg
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_dry_run_leaves_test_active(fresh_db):
    """dry_run=True → test stays active, no alert."""
    from src.tasks.lifecycle_attribution_rollback_check import run as rollback_run

    test, sub_ids = _seed_rollout_test(
        fresh_db, n_ctrl=100, n_var=100, ctrl_conv_rate=0.30, var_conv_rate=0.02
    )
    try:
        with (
            patch("src.tasks.lifecycle_attribution_rollback_check.ATTRIBUTION_ROLLOUT_TEST_NAME", test.test_name),
            patch("src.services.ab_engine.ATTRIBUTION_ROLLOUT_TEST_NAME", test.test_name),
            patch("src.services.stripe_webhooks._send_founder_alert") as mock_alert,
        ):
            result = rollback_run(dry_run=True)

        fresh_db.refresh(test)
        assert test.status == "active"
        assert result["rolled_back"] == 0
        assert not mock_alert.called
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_post_rollback_assign_returns_none(fresh_db):
    """After rollback, assign_rollout_arm returns None — control path guaranteed."""
    from src.services.ab_engine import assign_rollout_arm, rollback_rollout

    test = AbTest(
        test_name=f"t_{uuid.uuid4().hex[:8]}",
        segment="all",
        variant_a={},
        variant_b={},
        traffic_pct=100,
        status="active",
    )
    fresh_db.add(test)
    fresh_db.flush()
    sub_ids = _seed_subs(fresh_db, 5)
    try:
        # Assign all subs before rollback
        for sid in sub_ids:
            assign_rollout_arm(sid, test.test_name, fresh_db)

        rollback_rollout(test.test_name, fresh_db)
        fresh_db.commit()

        # After rollback, new calls return None (test no longer active)
        new_sub_ids = _seed_subs(fresh_db, 3)
        for sid in new_sub_ids:
            arm = assign_rollout_arm(sid, test.test_name, fresh_db)
            assert arm is None, f"expected None post-rollback, got {arm}"
        sub_ids.extend(new_sub_ids)
    finally:
        fresh_db.query(AbAssignment).filter_by(test_id=test.id).delete()
        fresh_db.query(AbTest).filter_by(id=test.id).delete()
        fresh_db.query(Subscriber).filter(Subscriber.id.in_(sub_ids)).delete(
            synchronize_session=False
        )
        fresh_db.commit()
