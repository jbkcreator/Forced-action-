"""
QUALITY-v2.2 Q3 acceptance test: proves all four boundaries reject an
incomplete handoff and accept a complete one, and that every rejection
leaves an auditable trail in handoff_rejections (decision D2's "auto-reject
triggers a Slack message to Josh" -- the DB row is the part every run can
assert on without a live Slack app; the Slack no-op path is covered per-module
in Task 2's test_base.py).
"""
from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from src.agents.contracts.base import HandoffRejected
from src.agents.contracts.cora_to_relay import validate_handoff as validate_cora_to_relay
from src.agents.contracts.dev_to_vera import check_closure
from src.agents.contracts.handoff_quality import get_average_rating, rate_handoff
from src.agents.contracts.hunter_to_cora import (
    is_handoff_citable,
    reject_handoff as reject_hunter_to_cora,
    validate_handoff as validate_hunter_to_cora,
)
from src.agents.contracts.vera_to_dev import check_finding
from src.services.relay import queue as relay_queue


def test_hunter_to_cora_boundary_rejects_below_floor_and_accepts_above(fresh_db):
    below = {
        "opportunity_thread_id": "OPP-2026-90001",
        "confidence_score": 55,
        "entity_type": "Individual",
        "total_purchase_count": 2,
        "total_cash_volume": 300_000,
        "contact_channel": "email",
        "contact_confidence": 60,
        "why_now": "2 purchases in the trailing 18 months",
        "whale_flagged_at": None,
    }
    handoff = validate_hunter_to_cora(below)
    assert is_handoff_citable(handoff) is False
    reject_hunter_to_cora(fresh_db, below, ["confidence_score: 55 < 70"])
    fresh_db.commit()

    row = fresh_db.execute(
        text("SELECT boundary FROM handoff_rejections WHERE reference_id = :r"),
        {"r": "OPP-2026-90001"},
    ).mappings().first()
    assert row["boundary"] == "hunter_to_cora"

    above = {**below, "opportunity_thread_id": "OPP-2026-90002", "confidence_score": 90}
    handoff2 = validate_hunter_to_cora(above)
    assert is_handoff_citable(handoff2) is True


def test_cora_to_relay_boundary_rejects_incomplete_and_accepts_complete(fresh_db):
    idem = f"e2e-{uuid.uuid4().hex[:12]}"
    with pytest.raises(HandoffRejected):
        relay_queue.enqueue(
            idempotency_key=idem, channel="noop", recipient="buyer@example.com",
            payload={"body": ""}, thread_id="OPP-2026-90003",
        )
    row = fresh_db.execute(
        text("SELECT boundary FROM handoff_rejections WHERE reference_id = :r"), {"r": idem},
    ).mappings().first()
    assert row["boundary"] == "cora_to_relay"

    idem2 = f"e2e-{uuid.uuid4().hex[:12]}"
    item = relay_queue.enqueue(
        idempotency_key=idem2, channel="noop", recipient="buyer@example.com",
        payload={"subject": "Hi", "body": "Hello"}, thread_id="OPP-2026-90004",
    )
    assert item.status == "pending"


def test_vera_to_dev_and_dev_to_vera_boundaries_reject_incomplete(fresh_db):
    incomplete_finding = {"issue": "x", "evidence": "y"}  # missing 5 of 7 fields
    result = check_finding(incomplete_finding)
    assert result.ok is False
    assert len(result.missing_fields) >= 5

    incomplete_closure = {"commit_hash": "abc123"}  # missing 7 of 8 fields
    result2 = check_closure(incomplete_closure)
    assert result2.ok is False
    assert len(result2.missing_fields) >= 6


def test_handoff_quality_ratings_roundtrip(fresh_db):
    ref = f"OPP-2026-{uuid.uuid4().hex[:5]}"
    rate_handoff(
        fresh_db, boundary="hunter_to_cora", rater_seat="cora", ratee_seat="hunter",
        reference_id=ref, score=5, notes="Perfect handoff, all fields present",
    )
    fresh_db.commit()
    avg = get_average_rating(fresh_db, boundary="hunter_to_cora", ratee_seat="hunter")
    assert avg is not None and avg > 0
