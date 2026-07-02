"""Tests for Broker State Machine Layer 3B — service integration."""
from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import text as sa_text

from src.services.broker_state_machine import (
    BrokerInactive,
    BrokerNotFound,
    ClosedWonPayloadRequired,
    IllegalTransition,
    InvalidGrossAmount,
    LaneNotFound,
    LaneOwnershipError,
    assign_broker,
    current_state,
    list_transitions,
    reassign_lane,
    sms_eligible,
    transition,
)
from src.services.loan_lane_service import enter_lane

LANE_TYPE = "distressed-payoff"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _property(session) -> int:
    return session.execute(
        sa_text("""
            INSERT INTO properties (parcel_id, county_id, created_at, updated_at)
            VALUES (:pc, 'hillsborough', NOW(), NOW())
            RETURNING id
        """),
        {"pc": f"PARC-{uuid.uuid4().hex[:12]}"},
    ).scalar()


def _prospect(session) -> str:
    prop_id = _property(session)
    return str(session.execute(
        sa_text("""
            INSERT INTO prospects (prospect_id, property_id, contactability_state)
            VALUES (gen_random_uuid(), :pid, 'contactable')
            RETURNING prospect_id
        """),
        {"pid": prop_id},
    ).scalar())


def _broker(session, *, active: bool = True) -> str:
    return str(session.execute(
        sa_text("""
            INSERT INTO brokers (email, name, is_active)
            VALUES (:e, :n, :a)
            RETURNING broker_id
        """),
        {"e": f"{uuid.uuid4().hex[:8]}@test.com", "n": "Test Broker", "a": active},
    ).scalar())


def _lane(session) -> tuple[str, str]:
    pid = _prospect(session)
    lane_id = enter_lane(session, pid, lane_type=LANE_TYPE)
    return lane_id, pid


def _advance_to(session, lane_id: str, broker_id: str, *states: str) -> None:
    """Drive the broker through a sequence of states via transition()."""
    for to_state in states:
        reason = "funded" if to_state == "closed_won" else "qualified"
        transition(session, lane_id, to_state, broker_id, reason)


# ── current_state ─────────────────────────────────────────────────────────────

class TestCurrentState:
    def test_returns_unassigned_when_lane_unclaimed(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        assert current_state(fresh_db, lane_id) == "unassigned"

    def test_returns_assigned_when_lane_claimed_but_no_transitions(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        # Claim via the lane service only (no BSM transition row)
        fresh_db.execute(
            sa_text("""
                UPDATE lanes SET assigned_broker_id = CAST(:bid AS uuid),
                       updated_at = NOW()
                WHERE lane_id = CAST(:lid AS uuid)
            """),
            {"lid": lane_id, "bid": broker_id},
        )
        assert current_state(fresh_db, lane_id) == "assigned"

    def test_returns_latest_to_state_from_transitions(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        transition(fresh_db, lane_id, "working", broker_id, "qualified")
        assert current_state(fresh_db, lane_id) == "working"

    def test_raises_lane_not_found_for_unknown_lane(self, fresh_db):
        with pytest.raises(LaneNotFound):
            current_state(fresh_db, str(uuid.uuid4()))


# ── list_transitions ──────────────────────────────────────────────────────────

class TestListTransitions:
    def test_returns_empty_list_when_no_transitions(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        assert list_transitions(fresh_db, lane_id) == []

    def test_returns_transitions_in_chronological_order(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        transition(fresh_db, lane_id, "working", broker_id, "qualified")

        rows = list_transitions(fresh_db, lane_id)
        assert len(rows) == 2
        assert rows[0]["to_state"] == "assigned"
        assert rows[1]["to_state"] == "working"
        assert "transition_id" in rows[0]
        assert "occurred_at" in rows[0]


# ── assign_broker ─────────────────────────────────────────────────────────────

class TestAssignBroker:
    def test_returns_true_and_claims_lane(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        result = assign_broker(fresh_db, lane_id, broker_id)
        assert result is True
        row = fresh_db.execute(
            sa_text("SELECT assigned_broker_id FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        ).fetchone()
        assert str(row.assigned_broker_id) == broker_id

    def test_returns_false_when_lane_already_claimed(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_a = _broker(fresh_db)
        broker_b = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_a)
        result = assign_broker(fresh_db, lane_id, broker_b)
        assert result is False

    def test_inserts_unassigned_to_assigned_transition_row(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)

        rows = list_transitions(fresh_db, lane_id)
        assert len(rows) == 1
        assert rows[0]["from_state"] == "unassigned"
        assert rows[0]["to_state"] == "assigned"
        assert rows[0]["broker_id"] == broker_id

    def test_emits_broker_transition_event(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)

        with patch("src.services.broker_state_machine.emit_event") as mock_emit:
            assign_broker(fresh_db, lane_id, broker_id)

        mock_emit.assert_called_once()
        kwargs = mock_emit.call_args.kwargs
        assert kwargs["event_type"] == "broker.transition"
        assert kwargs["payload"]["to_state"] == "assigned"
        assert kwargs["payload"]["from_state"] == "unassigned"

    def test_raises_broker_not_found_for_unknown_broker(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        with pytest.raises(BrokerNotFound):
            assign_broker(fresh_db, lane_id, str(uuid.uuid4()))

    def test_raises_broker_inactive(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db, active=False)
        with pytest.raises(BrokerInactive):
            assign_broker(fresh_db, lane_id, broker_id)


# ── reassign_lane ─────────────────────────────────────────────────────────────

class TestReassignLane:
    def test_updates_assigned_broker_and_inserts_transition(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_a = _broker(fresh_db)
        broker_b = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_a)

        tid = reassign_lane(fresh_db, lane_id, broker_b, actor="admin@fa.com")
        assert isinstance(tid, str) and len(tid) > 0

        row = fresh_db.execute(
            sa_text("SELECT assigned_broker_id FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        ).fetchone()
        assert str(row.assigned_broker_id) == broker_b

        transitions = list_transitions(fresh_db, lane_id)
        last = transitions[-1]
        assert last["to_state"] == "assigned"
        assert last["broker_id"] == broker_b


# ── transition ────────────────────────────────────────────────────────────────

class TestTransition:
    def test_assigned_to_working_succeeds(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)

        tid = transition(fresh_db, lane_id, "working", broker_id, "qualified")
        assert isinstance(tid, str)
        assert current_state(fresh_db, lane_id) == "working"

    def test_working_to_quoted_succeeds(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        _advance_to(fresh_db, lane_id, broker_id, "working")

        transition(fresh_db, lane_id, "quoted", broker_id, "qualified")
        assert current_state(fresh_db, lane_id) == "quoted"

    def test_illegal_transition_raises(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)

        with pytest.raises(IllegalTransition):
            transition(fresh_db, lane_id, "closed_won", broker_id, "funded")

    def test_ownership_error_when_broker_does_not_own_lane(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_a = _broker(fresh_db)
        broker_b = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_a)

        with pytest.raises(LaneOwnershipError):
            transition(fresh_db, lane_id, "working", broker_b, "qualified")

    def test_inactive_broker_raises(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db, active=False)
        # Force-assign without BSM to get around the active check in assign_broker
        fresh_db.execute(
            sa_text("""
                UPDATE lanes
                   SET assigned_broker_id = CAST(:bid AS uuid), updated_at = NOW()
                 WHERE lane_id = CAST(:lid AS uuid)
            """),
            {"lid": lane_id, "bid": broker_id},
        )
        with pytest.raises(BrokerInactive):
            transition(fresh_db, lane_id, "working", broker_id, "qualified")

    def test_closed_won_without_payload_raises(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        _advance_to(fresh_db, lane_id, broker_id, "working", "quoted", "committed")

        with pytest.raises(ClosedWonPayloadRequired):
            transition(fresh_db, lane_id, "closed_won", broker_id, "funded")

    def test_closed_won_negative_amount_raises(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        _advance_to(fresh_db, lane_id, broker_id, "working", "quoted", "committed")

        with pytest.raises(InvalidGrossAmount):
            transition(
                fresh_db, lane_id, "closed_won", broker_id, "funded",
                gross_amount_cents=-1,
                split_config_id="platform_50_broker_50",
            )

    def test_closed_won_event_includes_gross_amount_and_split(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        _advance_to(fresh_db, lane_id, broker_id, "working", "quoted", "committed")

        with patch("src.services.broker_state_machine.emit_event") as mock_emit:
            transition(
                fresh_db, lane_id, "closed_won", broker_id, "funded",
                gross_amount_cents=500_000,
                split_config_id="platform_50_broker_50",
            )

        kwargs = mock_emit.call_args.kwargs
        assert kwargs["payload"]["gross_amount_cents"] == 500_000
        assert kwargs["payload"]["split_config_id"] == "platform_50_broker_50"
        assert kwargs["payload"]["to_state"] == "closed_won"

    def test_closed_lost_emits_broker_transition_event(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        _advance_to(fresh_db, lane_id, broker_id, "working")

        with patch("src.services.broker_state_machine.emit_event") as mock_emit:
            transition(fresh_db, lane_id, "closed_lost", broker_id, "not_interested")

        kwargs = mock_emit.call_args.kwargs
        assert kwargs["event_type"] == "broker.transition"
        assert kwargs["payload"]["to_state"] == "closed_lost"

    def test_transition_bumps_last_activity_at(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)

        # Null it out so we can prove transition() sets it (NOW() is tx-stable)
        fresh_db.execute(
            sa_text("UPDATE lanes SET last_activity_at = NULL WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        )
        assert fresh_db.execute(
            sa_text("SELECT last_activity_at FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        ).scalar() is None

        transition(fresh_db, lane_id, "working", broker_id, "qualified")

        after = fresh_db.execute(
            sa_text("SELECT last_activity_at FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        ).scalar()
        assert after is not None


# ── sms_eligible ──────────────────────────────────────────────────────────────

class TestSmsEligible:
    def test_returns_false_when_lane_not_found(self, fresh_db):
        assert sms_eligible(fresh_db, str(uuid.uuid4())) is False

    def test_returns_false_for_unassigned_broker_state(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        # Lane starts at 'entered' stage (sms_allowed=False) with broker state=unassigned
        assert sms_eligible(fresh_db, lane_id) is False

    def test_returns_false_for_closed_won_state(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        _advance_to(fresh_db, lane_id, broker_id, "working", "quoted", "committed")

        # Force lane stage to quoted (sms_allowed=True) so only broker state gates it
        fresh_db.execute(
            sa_text("UPDATE lanes SET current_stage = 'quoted' WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        )
        transition(
            fresh_db, lane_id, "closed_won", broker_id, "funded",
            gross_amount_cents=100_000,
            split_config_id="platform_50_broker_50",
        )
        assert sms_eligible(fresh_db, lane_id) is False

    def test_returns_true_when_stage_allows_and_state_is_working(self, fresh_db):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id)
        transition(fresh_db, lane_id, "working", broker_id, "qualified")

        # Advance lane stage to 'quoted' where sms_allowed=True
        fresh_db.execute(
            sa_text("UPDATE lanes SET current_stage = 'quoted' WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        )
        assert sms_eligible(fresh_db, lane_id) is True
