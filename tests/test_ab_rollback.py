"""
A/B auto-rollback monitor tests.

Verifies:
  - Test with insufficient samples is left active.
  - Test where variant A loses by >2σ after 200+ sends gets retired.
  - Founder alert is invoked on rollback (mocked).
  - LearningCard row is written.

Run:
    pytest tests/test_ab_rollback.py -v
"""
from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import patch

import pytest

from src.core.models import AbAssignment, AbTest, LearningCard, Subscriber


def _seed_subs(db, n: int) -> list[int]:
    """Seed `n` real Subscriber rows (FK requirement) and return their ids."""
    ids = []
    for i in range(n):
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_abr_{uid}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"abr-{uid}",
        )
        db.add(sub)
        db.flush()
        ids.append(sub.id)
    return ids


def _seed_test(db, *, n_per_arm: int, a_conv_rate: float, b_conv_rate: float) -> tuple[AbTest, list[int]]:
    """Seed an AbTest with `n_per_arm` assignments per variant at the given conversion rates."""
    test = AbTest(
        test_name=f"test_rollback_{uuid.uuid4().hex[:8]}",
        segment="wallet_active",
        variant_a={"price_cents": 1900},
        variant_b={"price_cents": 2400},
        traffic_pct=10,
        status="active",
    )
    db.add(test)
    db.flush()

    sub_ids = _seed_subs(db, n_per_arm * 2)

    a_conv = round(n_per_arm * a_conv_rate)
    b_conv = round(n_per_arm * b_conv_rate)

    for i in range(n_per_arm):
        db.add(AbAssignment(
            test_id=test.id,
            subscriber_id=sub_ids[i],
            variant="a",
            outcome="converted" if i < a_conv else "no_convert",
        ))
    for i in range(n_per_arm):
        db.add(AbAssignment(
            test_id=test.id,
            subscriber_id=sub_ids[n_per_arm + i],
            variant="b",
            outcome="converted" if i < b_conv else "no_convert",
        ))
    db.flush()
    db.commit()
    return test, sub_ids


def _cleanup(db, test: AbTest, sub_ids: list[int]) -> None:
    db.query(AbAssignment).filter_by(test_id=test.id).delete()
    db.query(LearningCard).filter_by(card_date=date.today(), card_type="ab_result").delete()
    db.query(AbTest).filter_by(id=test.id).delete()
    db.query(Subscriber).filter(Subscriber.id.in_(sub_ids)).delete(synchronize_session=False)
    db.commit()


def test_insufficient_samples_leaves_test_active(fresh_db):
    """Below 200 total assignments, no rollback regardless of conversion gap."""
    test, sub_ids = _seed_test(fresh_db, n_per_arm=20, a_conv_rate=0.05, b_conv_rate=0.50)
    try:
        from src.tasks.ab_rollback_check import run
        with patch("src.services.stripe_webhooks._send_founder_alert"):
            result = run(dry_run=False)
        fresh_db.refresh(test)
        assert test.status == "active"
        assert result["rolled_back"] == 0
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_clear_loser_after_200_sends_gets_rolled_back(fresh_db):
    """Variant A converts at 5%, variant B at 25%, n=120/arm (240 total).
    z-score should easily clear 2σ → rollback fires."""
    test, sub_ids = _seed_test(fresh_db, n_per_arm=120, a_conv_rate=0.05, b_conv_rate=0.25)
    try:
        from src.tasks.ab_rollback_check import run
        with patch("src.services.stripe_webhooks._send_founder_alert") as mock_alert:
            result = run(dry_run=False)
        fresh_db.refresh(test)

        assert test.status == "completed", f"expected completed, got {test.status}"
        assert test.winner == "b"
        assert result["rolled_back"] == 1
        # Founder alert called once
        assert mock_alert.called
        msg_arg = mock_alert.call_args[0][0]
        assert "AB ROLLBACK" in msg_arg
        assert test.test_name in msg_arg

        # LearningCard row written
        card = fresh_db.query(LearningCard).filter_by(
            card_date=date.today(), card_type="ab_result",
        ).first()
        assert card is not None
        assert test.test_name in card.summary_text
        assert card.data_json["winner"] == "b"
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_dry_run_does_not_complete_test(fresh_db):
    """Same setup as the rollback test, but --dry-run should leave status active."""
    test, sub_ids = _seed_test(fresh_db, n_per_arm=120, a_conv_rate=0.05, b_conv_rate=0.25)
    try:
        from src.tasks.ab_rollback_check import run
        with patch("src.services.stripe_webhooks._send_founder_alert") as mock_alert:
            result = run(dry_run=True)
        fresh_db.refresh(test)

        assert test.status == "active"
        assert result["rolled_back"] == 0
        assert not mock_alert.called
    finally:
        _cleanup(fresh_db, test, sub_ids)
