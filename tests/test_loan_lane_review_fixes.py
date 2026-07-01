"""Coverage for the PR review fixes on loan_lane_service (issues #2, #3)."""
from __future__ import annotations

import uuid

from sqlalchemy import text

from src.services.loan_lane_service import advance_lane, enter_lane

LT = "distressed-payoff"


def _property(session) -> int:
    return session.execute(
        text("INSERT INTO properties (parcel_id, county_id, created_at, updated_at) "
             "VALUES (:pc,'hillsborough',NOW(),NOW()) RETURNING id"),
        {"pc": f"P-{uuid.uuid4().hex[:10]}"},
    ).scalar()


def test_enter_lane_idempotent_returns_same_lane(fresh_db):
    """Issue #2 — repeated entry returns the same lane, one row (conflict-safe)."""
    pid = _property(fresh_db)
    a = enter_lane(fresh_db, pid, lane_type=LT)
    b = enter_lane(fresh_db, pid, lane_type=LT)
    assert a == b
    n = fresh_db.execute(
        text("SELECT COUNT(*) FROM lanes WHERE property_id = :p AND lane_type = :lt"),
        {"p": pid, "lt": LT},
    ).scalar()
    assert n == 1


def test_advance_lane_refreshes_last_activity(fresh_db):
    """Issue #3 — advancing a stage bumps last_activity_at."""
    pid = _property(fresh_db)
    lane_id = enter_lane(fresh_db, pid, lane_type=LT)
    # advance_lane requires the broker work-state to permit the target stage —
    # seed a transition to 'quoted' so the stage advance is allowed.
    broker_id = fresh_db.execute(
        text("INSERT INTO brokers (email, name) VALUES (:e,'B') RETURNING broker_id"),
        {"e": f"{uuid.uuid4().hex[:8]}@x.test"},
    ).scalar()
    fresh_db.execute(
        text("INSERT INTO broker_transitions (lane_id, broker_id, from_state, to_state, reason_code, actor) "
             "VALUES (CAST(:l AS uuid), :b, 'working', 'quoted', 'price', :b)"),
        {"l": lane_id, "b": str(broker_id)},
    )
    # Force an old activity timestamp so the refresh is detectable (NOW() is
    # constant within a transaction, so compare against a distinctly older value).
    fresh_db.execute(
        text("UPDATE lanes SET last_activity_at = '2020-01-01' WHERE lane_id = CAST(:l AS uuid)"),
        {"l": lane_id},
    )
    advance_lane(fresh_db, lane_id, "quoted", actor="admin_1")
    la = fresh_db.execute(
        text("SELECT last_activity_at FROM lanes WHERE lane_id = CAST(:l AS uuid)"),
        {"l": lane_id},
    ).scalar()
    assert la.year >= 2026  # refreshed to NOW(), not the forced 2020 value
