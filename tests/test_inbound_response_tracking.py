"""TDD for Block 11 / B11-04 — inbound_response tracking + reporting.

Covers:
  - record_inbound_response: writes t0/score/matched_signals/outcome at scoring time
  - sync_inbound_response_outcomes: backfills t1/outcome from agent_decisions
  - get_inbound_velocity_stats: computes counts/percentiles/rates for the admin endpoint
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.services.inbound_response_tracking import (
    get_inbound_velocity_stats,
    record_inbound_response,
    sync_inbound_response_outcomes,
)


class TestRecordInboundResponse:
    def test_writes_row_with_expected_fields(self):
        db = MagicMock()
        t0 = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)

        record_inbound_response(
            db=db,
            subscriber_id=42,
            decision_id="call-1",
            t0=t0,
            score=65,
            matched_signals=["intent_slot", "known_caller"],
        )

        db.execute.assert_called_once()
        (_stmt, params), _kwargs = db.execute.call_args
        assert params["subscriber_id"] == 42
        assert params["decision_id"] == "call-1"
        assert params["t0"] == t0
        assert params["score"] == 65
        assert json.loads(params["matched_signals"]) == ["intent_slot", "known_caller"]


class TestSyncInboundResponseOutcomes:
    def test_reconciliation_query_runs_and_commits(self):
        db = MagicMock()
        sync_inbound_response_outcomes(db)
        assert db.execute.call_count == 1
        db.commit.assert_called_once()


class TestGetInboundVelocityStats:
    def test_computes_stats_from_query_row(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = (
            10,   # total
            6,    # called
            2,    # consent_blocked
            1,    # dnc_blocked
            1,    # failed
            45.0,  # p50 seconds
            90.0,  # p95 seconds
        )
        stats = get_inbound_velocity_stats(db)

        assert stats["total"] == 10
        assert stats["called"] == 6
        assert stats["consent_blocked"] == 2
        assert stats["dnc_blocked"] == 1
        assert stats["failed"] == 1
        assert stats["p50_seconds"] == 45.0
        assert stats["p95_seconds"] == 90.0
        assert stats["hot_callback_rate"] == 0.6

    def test_no_rows_returns_zeroed_stats(self):
        db = MagicMock()
        db.execute.return_value.first.return_value = None
        stats = get_inbound_velocity_stats(db)

        assert stats["total"] == 0
        assert stats["hot_callback_rate"] == 0.0
        assert stats["p50_seconds"] is None
