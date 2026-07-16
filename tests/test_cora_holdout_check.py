"""
Task 4.1 — cora_holdout_check scheduled task.

Iterates active `*_holdout` AbTest rows, calls ab_engine.holdout_verdict,
and on a 'proven' verdict writes a cora_playbook recommendation (human
adopts via the existing admin flow — no auto-promote) + a LearningCard.
Mirrors tests/test_attribution_rollout.py's integration style.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.core.models import AbAssignment, AbTest, Subscriber


def _seed_subs(db, n: int) -> list[int]:
    ids = []
    for _ in range(n):
        uid = uuid.uuid4().hex[:8]
        sub = Subscriber(
            stripe_customer_id=f"cus_hc_{uid}", tier="starter", vertical="roofing",
            county_id="hillsborough", event_feed_uuid=f"hc-{uid}",
        )
        db.add(sub)
        db.flush()
        ids.append(sub.id)
    return ids


def _seed_holdout_test(
    db,
    *,
    n_ctrl: int,
    n_var: int,
    ctrl_conv_rate: float,
    var_conv_rate: float,
) -> tuple[AbTest, list[int]]:
    test = AbTest(
        test_name=f"test_holdout_{uuid.uuid4().hex[:8]}_holdout",
        segment="all",
        variant_a={"path": "control"}, variant_b={"path": "variant"},
        traffic_pct=90, status="active",
    )
    db.add(test)
    db.flush()

    sub_ids = _seed_subs(db, n_ctrl + n_var)
    created_at = datetime.now(timezone.utc) - timedelta(hours=1)
    ctrl_conv_n = round(n_ctrl * ctrl_conv_rate)
    var_conv_n = round(n_var * var_conv_rate)

    for i in range(n_ctrl):
        db.add(AbAssignment(
            test_id=test.id, subscriber_id=sub_ids[i], variant="control",
            outcome="converted" if i < ctrl_conv_n else "no_convert",
            created_at=created_at,
        ))
    for i in range(n_var):
        db.add(AbAssignment(
            test_id=test.id, subscriber_id=sub_ids[n_ctrl + i], variant="variant",
            outcome="converted" if i < var_conv_n else "no_convert",
            created_at=created_at,
        ))
    db.flush()
    db.commit()
    return test, sub_ids


def _cleanup(db, test: AbTest, sub_ids: list[int]) -> None:
    from src.core.models import LearningCard, CoraPlaybook
    db.query(AbAssignment).filter_by(test_id=test.id).delete()
    db.query(CoraPlaybook).filter_by(source_type="holdout_test", source_id=test.test_name).delete()
    db.query(LearningCard).filter_by(card_type="holdout_result").delete()
    db.query(AbTest).filter_by(id=test.id).delete()
    db.query(Subscriber).filter(Subscriber.id.in_(sub_ids)).delete(synchronize_session=False)
    db.commit()


def test_proven_verdict_writes_playbook_and_learning_card(fresh_db):
    """40/arm, variant 40% vs control 5% — clear, well-powered win."""
    from src.tasks.cora_holdout_check import run
    from src.core.models import CoraPlaybook, LearningCard

    test, sub_ids = _seed_holdout_test(
        fresh_db, n_ctrl=40, n_var=40, ctrl_conv_rate=0.05, var_conv_rate=0.40,
    )
    try:
        result = run(dry_run=False)
        assert result["checked"] >= 1

        playbook = fresh_db.query(CoraPlaybook).filter_by(
            source_type="holdout_test", source_id=test.test_name,
        ).one_or_none()
        assert playbook is not None
        assert playbook.status == "recommended"
        assert playbook.authored_by == "cora"

        card = fresh_db.query(LearningCard).filter_by(card_type="holdout_result").one_or_none()
        assert card is not None
        assert test.test_name in card.summary_text
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_not_proven_writes_nothing(fresh_db):
    """Rates too close — must not write a recommendation or learning card."""
    from src.tasks.cora_holdout_check import run
    from src.core.models import CoraPlaybook

    test, sub_ids = _seed_holdout_test(
        fresh_db, n_ctrl=40, n_var=40, ctrl_conv_rate=0.30, var_conv_rate=0.32,
    )
    try:
        run(dry_run=False)
        playbook = fresh_db.query(CoraPlaybook).filter_by(
            source_type="holdout_test", source_id=test.test_name,
        ).one_or_none()
        assert playbook is None
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_dry_run_writes_nothing(fresh_db):
    """dry_run=True must never write, even on a proven verdict."""
    from src.tasks.cora_holdout_check import run
    from src.core.models import CoraPlaybook

    test, sub_ids = _seed_holdout_test(
        fresh_db, n_ctrl=40, n_var=40, ctrl_conv_rate=0.05, var_conv_rate=0.40,
    )
    try:
        result = run(dry_run=True)
        assert result["proven"] == 0
        playbook = fresh_db.query(CoraPlaybook).filter_by(
            source_type="holdout_test", source_id=test.test_name,
        ).one_or_none()
        assert playbook is None
    finally:
        _cleanup(fresh_db, test, sub_ids)


def test_baseline_drift_skips_verdict(fresh_db):
    """PR #133 finding 2: if the graph's base prompt changed since the test
    was created (stored baseline_fingerprint != current), a proven split must
    NOT produce a recommendation — the control condition drifted mid-test.
    Uses the real config name 'wallet_push_holdout' so the drift guard's
    get_holdout_config_by_test_name lookup resolves to a real graph."""
    from src.tasks.cora_holdout_check import run
    from src.core.models import CoraPlaybook, LearningCard

    # A genuinely-winning split, but stored fingerprint is stale → drift.
    test = AbTest(
        test_name="wallet_push_holdout", segment="all",
        variant_a={"path": "variant"},
        variant_b={"path": "control", "baseline_fingerprint": "STALE_DOES_NOT_MATCH"},
        traffic_pct=90, status="active",
    )
    fresh_db.add(test)
    fresh_db.flush()

    sub_ids = _seed_subs(fresh_db, 80)
    created_at = datetime.now(timezone.utc) - timedelta(hours=1)
    for i in range(40):
        fresh_db.add(AbAssignment(
            test_id=test.id, subscriber_id=sub_ids[i], variant="control",
            outcome="converted" if i < 2 else "no_convert", created_at=created_at,
        ))
    for i in range(40):
        fresh_db.add(AbAssignment(
            test_id=test.id, subscriber_id=sub_ids[40 + i], variant="variant",
            outcome="converted" if i < 16 else "no_convert", created_at=created_at,
        ))
    fresh_db.flush()
    fresh_db.commit()

    try:
        result = run(dry_run=False)
        assert result["proven"] == 0  # drift → not counted as proven
        playbook = fresh_db.query(CoraPlaybook).filter_by(
            source_type="holdout_test", source_id="wallet_push_holdout",
        ).one_or_none()
        assert playbook is None
    finally:
        _cleanup(fresh_db, test, sub_ids)
