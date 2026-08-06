"""Layer 3E — E2E backend tests for the Broker State Machine happy path.

Exercises the full create→claim→transition→closed_won sequence against a real
Postgres instance (fresh_db fixture with SAVEPOINT rollback per test).

Assertions:
  - Each broker.transition event is emitted with the correct payload
  - handle_lane_closer updates lane.outcome to 'funded' (closed_won) or 'dead' (closed_lost)
  - handle_commission_poster writes a commission_ledger row on closed_won
  - All steps are idempotent where the spec requires it
"""
from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import text as sa_text

from src.consumers.loan_lane_consumers import handle_commission_poster, handle_lane_closer
from src.services.broker_state_machine import (
    assign_broker,
    current_state,
    transition,
)
from src.services.commission_ledger import post_commission
from src.services.loan_lane_service import enter_lane, set_lane_outcome

LANE_TYPE = "distressed-payoff"
SPLIT = "platform_50_broker_50"
GROSS = 500_000  # $5,000


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


def _prospect(session) -> tuple[int, str]:
    prop_id = _property(session)
    prospect_id = str(session.execute(
        sa_text("""
            INSERT INTO prospects (prospect_id, property_id, contactability_state)
            VALUES (gen_random_uuid(), :pid, 'contactable')
            RETURNING prospect_id
        """),
        {"pid": prop_id},
    ).scalar())
    return prop_id, prospect_id


def _broker(session) -> str:
    return str(session.execute(
        sa_text("""
            INSERT INTO brokers (email, name, is_active)
            VALUES (:e, 'E2E Broker', true)
            RETURNING broker_id
        """),
        {"e": f"{uuid.uuid4().hex[:8]}@e2e.com"},
    ).scalar())


def _lane(session) -> tuple[str, str]:
    prop_id, pid = _prospect(session)
    lane_id = enter_lane(session, prop_id, lane_type=LANE_TYPE)
    return lane_id, pid


def _make_event_row(payload: dict, prospect_id=None):
    """Build a minimal event-row-like object that consumers accept."""
    class _Row:
        event_id = uuid.uuid4()
        pass
    row = _Row()
    row.prospect_id = prospect_id
    row.payload = payload
    return row


def _last_event(session, event_type: str) -> dict | None:
    row = session.execute(
        sa_text("""
            SELECT payload FROM events
            WHERE event_type = :et
            ORDER BY occurred_at DESC, event_id DESC
            LIMIT 1
        """),
        {"et": event_type},
    ).fetchone()
    return row.payload if row else None


# ── Happy path: unassigned → closed_won ──────────────────────────────────────

class TestClosedWonPath:
    def test_assign_emits_broker_transition_event(self, fresh_db):
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)

        assign_broker(fresh_db, lane_id, broker_id)

        ev = _last_event(fresh_db, "broker.transition")
        assert ev is not None
        assert ev["lane_id"] == lane_id
        assert ev["broker_id"] == broker_id
        assert ev["from_state"] == "unassigned"
        assert ev["to_state"] == "assigned"

    def test_full_happy_path_event_payload(self, fresh_db):
        """closed_won event carries all required payload fields."""
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, pid = _lane(fresh_db)
        broker_id = _broker(fresh_db)

        assign_broker(fresh_db, lane_id, broker_id)
        transition(fresh_db, lane_id, "working", broker_id, "no_contact")
        transition(fresh_db, lane_id, "quoted", broker_id, "price")
        transition(fresh_db, lane_id, "committed", broker_id, "qualified")
        tid = transition(
            fresh_db, lane_id, "closed_won", broker_id, "funded",
            gross_amount_cents=GROSS, split_config_id=SPLIT,
        )

        ev = _last_event(fresh_db, "broker.transition")
        assert ev["lane_id"] == lane_id
        assert ev["prospect_id"] == pid
        assert ev["transition_id"] == tid
        assert ev["broker_id"] == broker_id
        assert ev["from_state"] == "committed"
        assert ev["to_state"] == "closed_won"
        assert ev["reason_code"] == "funded"
        assert ev["gross_amount_cents"] == GROSS
        assert ev["split_config_id"] == SPLIT

    def test_lane_closer_sets_outcome_funded_on_closed_won(self, fresh_db):
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, _ = _lane(fresh_db)

        event_row = _make_event_row({"to_state": "closed_won", "lane_id": lane_id})
        with patch("src.services.broker_state_machine.emit_event"):
            handle_lane_closer(fresh_db, event_row)

        outcome = fresh_db.execute(
            sa_text("SELECT outcome FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        ).scalar()
        assert outcome == "funded"

    def test_lane_closer_idempotent(self, fresh_db):
        """Calling handle_lane_closer twice on a funded lane is safe."""
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, _ = _lane(fresh_db)
            set_lane_outcome(fresh_db, lane_id, "funded", actor="test")

        event_row = _make_event_row({"to_state": "closed_won", "lane_id": lane_id})
        with patch("src.services.broker_state_machine.emit_event"):
            handle_lane_closer(fresh_db, event_row)  # must not raise

        outcome = fresh_db.execute(
            sa_text("SELECT outcome FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        ).scalar()
        assert outcome == "funded"

    def test_commission_poster_writes_ledger_entry(self, fresh_db):
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, _ = _lane(fresh_db)
            broker_id = _broker(fresh_db)
            assign_broker(fresh_db, lane_id, broker_id)
            transition(fresh_db, lane_id, "working", broker_id, "no_contact")
            transition(fresh_db, lane_id, "quoted", broker_id, "price")
            transition(fresh_db, lane_id, "committed", broker_id, "qualified")
            tid = transition(
                fresh_db, lane_id, "closed_won", broker_id, "funded",
                gross_amount_cents=GROSS, split_config_id=SPLIT,
            )

        event_row = _make_event_row({
            "to_state": "closed_won",
            "transition_id": tid,
            "gross_amount_cents": GROSS,
            "split_config_id": SPLIT,
        })
        handle_commission_poster(fresh_db, event_row)

        row = fresh_db.execute(
            sa_text(
                "SELECT gross_amount_cents FROM commission_ledger "
                "WHERE trigger_transition_id = CAST(:tid AS uuid)"
            ),
            {"tid": tid},
        ).fetchone()
        assert row is not None
        assert row.gross_amount_cents == GROSS

    def test_commission_poster_idempotent_via_consumer(self, fresh_db):
        """Running handle_commission_poster twice inserts exactly one row."""
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, _ = _lane(fresh_db)
            broker_id = _broker(fresh_db)
            assign_broker(fresh_db, lane_id, broker_id)
            transition(fresh_db, lane_id, "working", broker_id, "no_contact")
            transition(fresh_db, lane_id, "quoted", broker_id, "price")
            transition(fresh_db, lane_id, "committed", broker_id, "qualified")
            tid = transition(
                fresh_db, lane_id, "closed_won", broker_id, "funded",
                gross_amount_cents=GROSS, split_config_id=SPLIT,
            )

        event_row = _make_event_row({
            "to_state": "closed_won",
            "transition_id": tid,
            "gross_amount_cents": GROSS,
            "split_config_id": SPLIT,
        })
        handle_commission_poster(fresh_db, event_row)
        handle_commission_poster(fresh_db, event_row)  # replay

        count = fresh_db.execute(
            sa_text(
                "SELECT COUNT(*) FROM commission_ledger "
                "WHERE trigger_transition_id = CAST(:tid AS uuid)"
            ),
            {"tid": tid},
        ).scalar()
        assert count == 1


# ── Closed-lost path ──────────────────────────────────────────────────────────

class TestClosedLostPath:
    def test_lane_closer_sets_outcome_dead_on_closed_lost(self, fresh_db):
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, _ = _lane(fresh_db)

        event_row = _make_event_row({"to_state": "closed_lost", "lane_id": lane_id})
        with patch("src.services.broker_state_machine.emit_event"):
            handle_lane_closer(fresh_db, event_row)

        outcome = fresh_db.execute(
            sa_text("SELECT outcome FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
            {"lid": lane_id},
        ).scalar()
        assert outcome == "dead"

    def test_closed_lost_event_payload(self, fresh_db):
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, pid = _lane(fresh_db)
        broker_id = _broker(fresh_db)

        assign_broker(fresh_db, lane_id, broker_id)
        transition(fresh_db, lane_id, "working", broker_id, "no_contact")
        transition(fresh_db, lane_id, "closed_lost", broker_id, "not_interested")

        ev = _last_event(fresh_db, "broker.transition")
        assert ev["to_state"] == "closed_lost"
        assert ev["reason_code"] == "not_interested"
        assert ev["lane_id"] == lane_id
        assert ev["broker_id"] == broker_id

    def test_commission_poster_skips_closed_lost(self, fresh_db):
        """commission_poster must NOT post an entry on closed_lost."""
        event_row = _make_event_row({"to_state": "closed_lost"})
        handle_commission_poster(fresh_db, event_row)
        # No assertion needed beyond "did not raise" — but verify DB is clean
        count = fresh_db.execute(
            sa_text("SELECT COUNT(*) FROM commission_ledger")
        ).scalar()
        assert count == 0

    def test_lane_closer_ignores_intermediate_state(self, fresh_db):
        with patch("src.services.broker_state_machine.emit_event"):
            lane_id, _ = _lane(fresh_db)

        event_row = _make_event_row({"to_state": "working", "lane_id": lane_id})
        with patch("src.services.broker_state_machine.emit_event"):
            handle_lane_closer(fresh_db, event_row)

        row = fresh_db.execute(
            sa_text(
                "SELECT current_stage, outcome FROM lanes "
                "WHERE lane_id = CAST(:lid AS uuid)"
            ),
            {"lid": lane_id},
        ).fetchone()
        assert row.outcome == "open"
