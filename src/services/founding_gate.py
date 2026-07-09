"""Founding-price gate (Task 8, ADR 0029) — single source of truth for
whether founding pricing is still active."""

from datetime import datetime
from typing import Optional


def evaluate_founding_gate(remaining_spots: int, deadline_at: Optional[datetime], now: datetime) -> dict:
    deadline_passed = deadline_at is not None and now >= deadline_at
    available = remaining_spots > 0 and not deadline_passed
    return {
        "available": available,
        "deadline_passed": deadline_passed,
    }
