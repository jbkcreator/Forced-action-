"""
PR-290 review-fix regression tests.

Covers:
  1. enqueue_sequence returns 0 (not a partial count) when a mid-loop
     insert fails and the transaction is rolled back.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.agents.reply_concierge.abandonment_agent import enqueue_sequence


def test_enqueue_sequence_returns_zero_on_partial_failure():
    db = MagicMock()
    db.execute.return_value.scalar.return_value = 0  # no active sequence

    # First execute() call is the "active sequence" SELECT (handled above).
    # Then each touch INSERT is its own execute() call — fail on the 3rd.
    call_count = {"n": 0}

    def side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return db.execute.return_value  # the SELECT COUNT(*)
        if call_count["n"] == 4:  # 3rd INSERT (calls 2,3,4 = touches 1,2,3)
            raise RuntimeError("simulated FK violation")
        return MagicMock()

    db.execute.side_effect = side_effect

    result = enqueue_sequence(
        person_id="p1",
        contact_email="a@b.com",
        db=db,
    )

    assert result == 0
    db.rollback.assert_called_once()
    db.commit.assert_not_called()
