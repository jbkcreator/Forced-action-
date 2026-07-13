"""
Tests for the founding-price gate helper (Task 8, ADR 0029).

Single source of truth for whether founding pricing is active — used by
/api/founding-summary, /api/founding-spots, and /api/landing-data so all
three agree on the same answer at the same moment.
"""

from datetime import datetime, timedelta, timezone

from src.services.founding_gate import evaluate_founding_gate


def test_available_when_spots_remain_and_no_deadline_set():
    result = evaluate_founding_gate(remaining_spots=5, deadline_at=None, now=datetime.now(timezone.utc))

    assert result["available"] is True
    assert result["deadline_passed"] is False


def test_unavailable_when_deadline_passed_even_with_spots_remaining():
    now = datetime.now(timezone.utc)
    result = evaluate_founding_gate(remaining_spots=5, deadline_at=now - timedelta(days=1), now=now)

    assert result["available"] is False
    assert result["deadline_passed"] is True


def test_available_when_spots_remain_and_deadline_in_future():
    now = datetime.now(timezone.utc)
    result = evaluate_founding_gate(remaining_spots=5, deadline_at=now + timedelta(days=1), now=now)

    assert result["available"] is True
    assert result["deadline_passed"] is False


def test_unavailable_when_no_spots_remain_regardless_of_deadline():
    now = datetime.now(timezone.utc)
    result = evaluate_founding_gate(remaining_spots=0, deadline_at=now + timedelta(days=1), now=now)

    assert result["available"] is False
