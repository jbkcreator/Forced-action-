"""
A/B testing engine — deterministic assignment, outcome recording, auto-rollback.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.cora_guardrails import get_guardrail, is_within_guardrail
from src.core.models import AbAssignment, AbTest

logger = logging.getLogger(__name__)


def get_or_create_test(
    test_name: str,
    segment: str,
    variant_a: dict,
    variant_b: dict,
    traffic_pct: int,
    db: Session,
) -> AbTest:
    existing = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if existing:
        return existing
    test = AbTest(
        test_name=test_name,
        segment=segment,
        variant_a=variant_a,
        variant_b=variant_b,
        traffic_pct=min(traffic_pct, get_guardrail("ab_test_traffic_cap")["max_pct"]),
        status="active",
    )
    db.add(test)
    db.flush()
    return test


def assign_variant(subscriber_id: int, test_name: str, db: Session) -> Optional[str]:
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name, AbTest.status == "active")
    ).scalar_one_or_none()
    if not test:
        return None

    existing = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.subscriber_id == subscriber_id,
        )
    ).scalar_one_or_none()
    if existing:
        return existing.variant

    # Deterministic hash — same subscriber always gets same variant
    h = int(hashlib.md5(f"{test_name}{subscriber_id}".encode()).hexdigest(), 16) % 100
    if h >= test.traffic_pct:
        return None

    variant = "a" if h % 2 == 0 else "b"
    assignment = AbAssignment(
        test_id=test.id,
        subscriber_id=subscriber_id,
        variant=variant,
    )
    db.add(assignment)
    db.flush()
    return variant


def record_outcome(subscriber_id: int, test_name: str, outcome: str, db: Session) -> None:
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if not test:
        return
    assignment = db.execute(
        select(AbAssignment).where(
            AbAssignment.test_id == test.id,
            AbAssignment.subscriber_id == subscriber_id,
        )
    ).scalar_one_or_none()
    if assignment:
        assignment.outcome = outcome
        db.flush()


def should_rollback(test_name: str, db: Session) -> bool:
    import math
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if not test or test.status != "active":
        return False

    guardrail = get_guardrail("message_variant_swap")
    retire_after = guardrail.get("retire_after_sends", 200)

    assignments = db.execute(
        select(AbAssignment).where(AbAssignment.test_id == test.id)
    ).scalars().all()

    if len(assignments) < retire_after:
        return False

    a_total = sum(1 for a in assignments if a.variant == "a")
    b_total = sum(1 for a in assignments if a.variant == "b")
    a_conv = sum(1 for a in assignments if a.variant == "a" and a.outcome == "converted")
    b_conv = sum(1 for a in assignments if a.variant == "b" and a.outcome == "converted")

    if a_total < 10 or b_total < 10:
        return False

    p_a = a_conv / a_total
    p_b = b_conv / b_total
    p_pool = (a_conv + b_conv) / (a_total + b_total)

    if p_pool == 0 or p_pool == 1:
        return False

    se = math.sqrt(p_pool * (1 - p_pool) * (1 / a_total + 1 / b_total))
    if se == 0:
        return False

    z = (p_a - p_b) / se
    return abs(z) > 2.0 and (p_a < p_b)  # variant A is losing by >2 std devs


def complete_test(test_name: str, winner: str, db: Session) -> None:
    test = db.execute(
        select(AbTest).where(AbTest.test_name == test_name)
    ).scalar_one_or_none()
    if not test:
        return
    test.status = "completed"
    test.winner = winner
    test.ended_at = datetime.now(timezone.utc)
    db.flush()
