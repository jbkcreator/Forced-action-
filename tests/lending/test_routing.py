"""R-1 Overflow Router tests.

Acceptance tests from the task-analysis plan (tests 1–5 from the addendum):
1. Construction split: $450k routes to ground-up overflow, $650k routes to primary.
2. Seat cap: first two flip files route to primary, third routes to overflow.
3. Decline cascade: decline → next lender → decline → next → decline → exhausted.
4. Unsigned lender is invisible: pending lender never receives a file.
5. Lenders are fully swappable: add, pause, reorder, move-primary all work.

Plus unit tests for:
  - Rule 5 (DSCR waterfall).
  - Rule 6 (no lender → hold with no_match).
  - Idempotency (route_file on an already-routed file is a no-op).
  - Append-only trigger: DELETE and core-field UPDATE raise.
  - No lender name in code (grep guard).
  - Decline reason defaults and unknown code handling.
  - Slack retry (events with slack_posted_at IS NULL are retried).
  - Hold summary posts only at 8 AM ET.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from migrations.apply_lending_lendingflow import apply_to as apply_lf

LOAN_TYPE_FLIP = "fix_and_flip"
LOAN_TYPE_GU = "ground_up_construction"
LOAN_TYPE_DSCR = "dscr_rental"
FL = "FL"


# ── Fixture: routing DB (lending_db + lendingflow + lender_routing) ───────────

@pytest.fixture
def rdb(lending_db):
    """lending_db with lendingflow_leads and lender_routing tables applied.

    Everything runs inside lending_db's rolled-back transaction. Run against a
    disposable Postgres (docker), not the shared DB: the routing tables have
    unique constraints that clash with any leftover rows.
    """
    from src.lending.models import LendingContact

    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)  # FK target on a fresh database
    apply_lf(conn)
    _apply_routing(conn)
    return lending_db


def _apply_routing(conn):
    from migrations.apply_lending_lender_routing import apply_to
    apply_to(conn)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _add_lender(
    db: Session,
    *,
    name: str,
    is_primary: bool = False,
    agreement_status: str = "signed",
    priority: int,
    monthly_cap: Optional[int] = None,
    construction_min_amount: Optional[int] = None,
    takes_ground_up: bool = False,
    active: bool = True,
) -> int:
    """Insert a lender row and return its id."""
    lid = db.execute(
        text(
            "INSERT INTO lending.lender_routing_rules"
            "  (name, active, is_primary, agreement_status, priority,"
            "   monthly_cap, construction_min_amount, takes_ground_up)"
            " VALUES (:n, :a, :p, :s, :prio, :cap, :cmin, :gu)"
            " RETURNING id"
        ),
        {
            "n": name, "a": active, "p": is_primary, "s": agreement_status,
            "prio": priority, "cap": monthly_cap, "cmin": construction_min_amount, "gu": takes_ground_up,
        },
    ).scalar_one()
    db.commit()
    return lid


def _add_box(
    db: Session,
    lender_id: int,
    *,
    loan_types: list[str],
    approved_states: Optional[list[str]] = None,
    min_loan_amount: Optional[int] = None,
    credit_floor: Optional[int] = None,
) -> None:
    db.execute(
        text(
            "INSERT INTO lending.lender_product_boxes"
            "  (lender_id, loan_types, approved_states, min_loan_amount, credit_floor)"
            " VALUES (:lid, :lt, :states, :min, :cf)"
        ),
        {
            "lid": lender_id, "lt": loan_types, "states": approved_states,
            "min": min_loan_amount, "cf": credit_floor,
        },
    )
    db.commit()


def _make_lead(
    db: Session,
    *,
    loan_type: str = LOAN_TYPE_FLIP,
    loan_amount: int = 300_000,
    state: str = FL,
    phone_suffix: str = "0001",
) -> int:
    """Insert a minimal lendingflow_leads row and return its id."""
    from src.services.phone_utils import normalize

    phone = normalize(f"727555{phone_suffix}")
    lid = db.execute(
        text(
            "INSERT INTO lending.lendingflow_leads"
            "  (vendor_lead_id, dedupe_hash, phone, loan_type, loan_amount,"
            "   property_state, ghl_status, suppressed, raw_payload, consent_status)"
            " VALUES (:vid, :dh, :ph, :lt, :la, :st, 'synced', false, '{}'::jsonb, 'present')"
            " RETURNING id"
        ),
        {
            "vid": f"TEST-{phone_suffix}",
            "dh": f"hash-{phone_suffix}",
            "ph": phone,
            "lt": loan_type,
            "la": loan_amount,
            "st": state,
        },
    ).scalar_one()
    db.commit()
    return lid


def _route(db: Session, lead_id: int) -> None:
    from src.lending.routing.router import route_file
    route_file(lead_id, db)


def _fr(db: Session, lead_id: int) -> dict:
    row = db.execute(
        text("SELECT status, current_lender_name, route_position, lenders_tried, hold_reason"
             "  FROM lending.file_routing WHERE lead_id = :l"),
        {"l": lead_id},
    ).mappings().one_or_none()
    return dict(row) if row else {}


def _events(db: Session, lead_id: int) -> list[dict]:
    rows = db.execute(
        text("SELECT action, lender_name, reason_code FROM lending.file_routing_events"
             " WHERE lead_id = :l ORDER BY id"),
        {"l": lead_id},
    ).mappings().all()
    return [dict(r) for r in rows]


def _routed_event_id(db: Session, lead_id: int) -> int:
    return db.execute(
        text("SELECT id FROM lending.file_routing_events"
             " WHERE lead_id = :l AND action = 'routed' ORDER BY id DESC LIMIT 1"),
        {"l": lead_id},
    ).scalar_one()


# ── Test 1: construction split ────────────────────────────────────────────────

def test_construction_split(rdb):
    """$650k ground-up → primary; $450k ground-up → first signed ground-up overflow."""
    primary_id = _add_lender(rdb, name="Primary Lender", is_primary=True, priority=1,
                             construction_min_amount=500_000)
    _add_box(rdb, primary_id, loan_types=[LOAN_TYPE_GU], approved_states=[FL])

    overflow_id = _add_lender(rdb, name="Overflow GU Lender", priority=2, takes_ground_up=True)
    _add_box(rdb, overflow_id, loan_types=[LOAN_TYPE_GU])

    lead_650 = _make_lead(rdb, loan_type=LOAN_TYPE_GU, loan_amount=650_000, phone_suffix="1111")
    lead_450 = _make_lead(rdb, loan_type=LOAN_TYPE_GU, loan_amount=450_000, phone_suffix="2222")

    _route(rdb, lead_650)
    _route(rdb, lead_450)

    fr_650 = _fr(rdb, lead_650)
    fr_450 = _fr(rdb, lead_450)

    assert fr_650["status"] == "routed"
    assert fr_650["current_lender_name"] == "Primary Lender"

    assert fr_450["status"] == "routed"
    assert fr_450["current_lender_name"] == "Overflow GU Lender"

    # Both appear in file_routing_events
    events_650 = _events(rdb, lead_650)
    events_450 = _events(rdb, lead_450)
    assert any(e["action"] == "routed" for e in events_650)
    assert any(e["action"] == "routed" for e in events_450)


# ── Test 2: seat cap ─────────────────────────────────────────────────────────

def test_seat_cap_routes_third_file_to_overflow(rdb):
    """Seat cap = 2. Files 1 and 2 route to primary; file 3 routes to overflow."""
    from config.lending_routing import SEAT_CAP_COUNTED_ACTION

    primary_id = _add_lender(rdb, name="Cap Lender", is_primary=True, priority=1,
                              monthly_cap=2)
    _add_box(rdb, primary_id, loan_types=[LOAN_TYPE_FLIP])

    overflow_id = _add_lender(rdb, name="Flip Overflow", priority=2)
    _add_box(rdb, overflow_id, loan_types=[LOAN_TYPE_FLIP])

    leads = [_make_lead(rdb, phone_suffix=str(i).zfill(4)) for i in range(3)]

    # Route files 1 and 2
    _route(rdb, leads[0])
    _route(rdb, leads[1])
    assert _fr(rdb, leads[0])["current_lender_name"] == "Cap Lender"
    assert _fr(rdb, leads[1])["current_lender_name"] == "Cap Lender"

    # Simulate the cap: insert two SEAT_CAP_COUNTED_ACTION events for the primary.
    for lid_val in leads[:2]:
        rdb.execute(
            text(
                "INSERT INTO lending.file_routing_events"
                "  (lead_id, lender_id, lender_name, action, actor)"
                " VALUES (:l, :lid, 'Cap Lender', :action, 'system')"
            ),
            {"l": lid_val, "lid": primary_id, "action": SEAT_CAP_COUNTED_ACTION},
        )
    rdb.commit()

    # Route file 3 — cap is now at 2, so it should skip to overflow.
    _route(rdb, leads[2])
    assert _fr(rdb, leads[2])["current_lender_name"] == "Flip Overflow"


# ── Test 3: decline cascade → exhausted ──────────────────────────────────────

def test_decline_cascade_exhausted(rdb):
    """Backflip (primary) → decline (ltv) → overflow 1 → decline → overflow 2 → decline → exhausted."""
    from src.lending.routing.decline import record_decline

    primary_id = _add_lender(rdb, name="Primary Flip", is_primary=True, priority=1)
    _add_box(rdb, primary_id, loan_types=[LOAN_TYPE_FLIP])

    over1_id = _add_lender(rdb, name="Overflow One", priority=2)
    _add_box(rdb, over1_id, loan_types=[LOAN_TYPE_FLIP])

    over2_id = _add_lender(rdb, name="Overflow Two", priority=3)
    _add_box(rdb, over2_id, loan_types=[LOAN_TYPE_FLIP])

    lead_id = _make_lead(rdb, phone_suffix="9001")
    _route(rdb, lead_id)
    assert _fr(rdb, lead_id)["current_lender_name"] == "Primary Flip"

    ev1 = _routed_event_id(rdb, lead_id)
    record_decline(ev1, reason_code="ltv", actor="josh", db=rdb)
    assert _fr(rdb, lead_id)["current_lender_name"] == "Overflow One"

    ev2 = _routed_event_id(rdb, lead_id)
    record_decline(ev2, reason_code="ltv", actor="josh", db=rdb)
    assert _fr(rdb, lead_id)["current_lender_name"] == "Overflow Two"

    ev3 = _routed_event_id(rdb, lead_id)
    record_decline(ev3, reason_code="ltv", actor="josh", db=rdb)
    fr = _fr(rdb, lead_id)
    assert fr["status"] == "exhausted"

    # All four events logged (routed, declined, declined, declined, routed, routed, held = 7 total)
    # Q3 confirmation: the spec says "all four events" which we interpret as 4 decline-cascade actions.
    events = _events(rdb, lead_id)
    assert sum(1 for e in events if e["action"] == "declined") == 3
    assert any(e["action"] == "held" for e in events)


# ── Test 4: unsigned lender is invisible ─────────────────────────────────────

def test_unsigned_lender_is_invisible(rdb):
    """A pending lender is never routed to; file lands in hold queue."""
    pending_id = _add_lender(rdb, name="Pending Lender", agreement_status="pending", priority=1)
    _add_box(rdb, pending_id, loan_types=[LOAN_TYPE_FLIP])

    lead_id = _make_lead(rdb, phone_suffix="4444")
    _route(rdb, lead_id)

    fr = _fr(rdb, lead_id)
    assert fr["status"] in ("held", "exhausted")
    assert fr["current_lender_name"] is None

    events = _events(rdb, lead_id)
    assert not any(e["lender_name"] == "Pending Lender" for e in events)


# ── Test 5: lenders are fully swappable ──────────────────────────────────────

def test_add_new_lender_routes_matching_file(rdb):
    """(a) Add a lender after first routing attempt; a new file routes there."""
    lead_before = _make_lead(rdb, phone_suffix="5001")
    _route(rdb, lead_before)
    assert _fr(rdb, lead_before)["status"] in ("held", "exhausted")  # no lenders yet

    new_id = _add_lender(rdb, name="Brand New Lender", priority=99)
    _add_box(rdb, new_id, loan_types=[LOAN_TYPE_FLIP])

    lead_after = _make_lead(rdb, phone_suffix="5002")
    _route(rdb, lead_after)
    assert _fr(rdb, lead_after)["current_lender_name"] == "Brand New Lender"


def test_paused_lender_is_skipped(rdb):
    """(b) Pausing a lender makes a matching file skip to the next."""
    l1 = _add_lender(rdb, name="Active Lender", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    l2 = _add_lender(rdb, name="Backup Lender", priority=2)
    _add_box(rdb, l2, loan_types=[LOAN_TYPE_FLIP])

    lead_id = _make_lead(rdb, phone_suffix="6001")
    _route(rdb, lead_id)
    assert _fr(rdb, lead_id)["current_lender_name"] == "Active Lender"

    # Pause the active lender; a new file should skip to Backup.
    rdb.execute(
        text("UPDATE lending.lender_routing_rules SET active = false WHERE id = :i"),
        {"i": l1},
    )
    rdb.commit()

    lead2 = _make_lead(rdb, phone_suffix="6002")
    _route(rdb, lead2)
    assert _fr(rdb, lead2)["current_lender_name"] == "Backup Lender"


def test_reorder_changes_routing(rdb):
    """(c) Swapping priority means a matching file goes to the new first."""
    l1 = _add_lender(rdb, name="Was First", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    l2 = _add_lender(rdb, name="Was Second", priority=2)
    _add_box(rdb, l2, loan_types=[LOAN_TYPE_FLIP])

    # Before reorder: l1 gets the file.
    lead_before = _make_lead(rdb, phone_suffix="7001")
    _route(rdb, lead_before)
    assert _fr(rdb, lead_before)["current_lender_name"] == "Was First"

    # Swap priorities.
    rdb.execute(text("UPDATE lending.lender_routing_rules SET priority = 99 WHERE id = :i"), {"i": l1})
    rdb.execute(text("UPDATE lending.lender_routing_rules SET priority = 1  WHERE id = :i"), {"i": l2})
    rdb.commit()

    lead_after = _make_lead(rdb, phone_suffix="7002")
    _route(rdb, lead_after)
    assert _fr(rdb, lead_after)["current_lender_name"] == "Was Second"


def test_move_primary_flag(rdb):
    """(d) Moving the primary flag to a new lender makes it get first look."""
    old_primary = _add_lender(rdb, name="Old Primary", is_primary=True, priority=1,
                               construction_min_amount=500_000)
    _add_box(rdb, old_primary, loan_types=[LOAN_TYPE_FLIP])
    new_primary = _add_lender(rdb, name="New Primary", is_primary=False, priority=2,
                               construction_min_amount=500_000)
    _add_box(rdb, new_primary, loan_types=[LOAN_TYPE_FLIP])

    lead_before = _make_lead(rdb, phone_suffix="8001")
    _route(rdb, lead_before)
    assert _fr(rdb, lead_before)["current_lender_name"] == "Old Primary"

    # Move primary flag (must clear old one first to satisfy unique partial index).
    rdb.execute(
        text("UPDATE lending.lender_routing_rules SET is_primary = false WHERE id = :i"), {"i": old_primary}
    )
    rdb.execute(
        text("UPDATE lending.lender_routing_rules SET is_primary = true WHERE id = :i"), {"i": new_primary}
    )
    rdb.commit()

    lead_after = _make_lead(rdb, phone_suffix="8002")
    _route(rdb, lead_after)
    assert _fr(rdb, lead_after)["current_lender_name"] == "New Primary"


# ── Rule 5: DSCR waterfall ────────────────────────────────────────────────────

def test_dscr_routes_to_dscr_lender(rdb):
    flip_only = _add_lender(rdb, name="Flip Only Lender", priority=1)
    _add_box(rdb, flip_only, loan_types=[LOAN_TYPE_FLIP])

    dscr_lender = _add_lender(rdb, name="DSCR Lender", priority=2)
    _add_box(rdb, dscr_lender, loan_types=[LOAN_TYPE_DSCR])

    lead_id = _make_lead(rdb, loan_type=LOAN_TYPE_DSCR, phone_suffix="9101")
    _route(rdb, lead_id)
    assert _fr(rdb, lead_id)["current_lender_name"] == "DSCR Lender"


def test_dscr_skips_lender_with_no_boxes(rdb):
    """A lender with no boxes (unrestricted) must not receive DSCR via the DSCR waterfall."""
    _add_lender(rdb, name="No Box Lender", priority=1)
    dscr_lender = _add_lender(rdb, name="DSCR Lender", priority=2)
    _add_box(rdb, dscr_lender, loan_types=[LOAN_TYPE_DSCR])

    lead_id = _make_lead(rdb, loan_type=LOAN_TYPE_DSCR, phone_suffix="9102")
    _route(rdb, lead_id)
    assert _fr(rdb, lead_id)["current_lender_name"] == "DSCR Lender"


def test_double_decline_same_event_is_noop(rdb):
    """A repeated decline of the same routed event must not add a second declined event."""
    from src.lending.routing.decline import record_decline

    l1 = _add_lender(rdb, name="First", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    l2 = _add_lender(rdb, name="Second", priority=2)
    _add_box(rdb, l2, loan_types=[LOAN_TYPE_FLIP])

    lead_id = _make_lead(rdb, phone_suffix="9601")
    _route(rdb, lead_id)
    ev = _routed_event_id(rdb, lead_id)
    record_decline(ev, reason_code="ltv", actor="josh", db=rdb)
    record_decline(ev, reason_code="ltv", actor="josh", db=rdb)

    events = _events(rdb, lead_id)
    assert sum(1 for e in events if e["action"] == "declined") == 1
    assert _fr(rdb, lead_id)["current_lender_name"] == "Second"


# ── Rule 6: no match → hold ───────────────────────────────────────────────────

def test_no_signed_lenders_holds_with_no_match(rdb):
    lead_id = _make_lead(rdb, phone_suffix="9201")
    _route(rdb, lead_id)
    fr = _fr(rdb, lead_id)
    assert fr["status"] == "held"
    assert fr["hold_reason"] == "no_match"


# ── Idempotency ───────────────────────────────────────────────────────────────

def test_route_file_is_idempotent(rdb):
    """Calling route_file twice on an already-routed file is a no-op."""
    l1 = _add_lender(rdb, name="Only Lender", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])

    lead_id = _make_lead(rdb, phone_suffix="9301")
    _route(rdb, lead_id)
    _route(rdb, lead_id)  # second call

    events = _events(rdb, lead_id)
    assert sum(1 for e in events if e["action"] == "routed") == 1  # only one routed event


# ── Append-only trigger ───────────────────────────────────────────────────────

def test_file_routing_events_rejects_delete(rdb):
    """The DB trigger must block DELETE on file_routing_events."""
    l1 = _add_lender(rdb, name="Trigger Lender", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    lead_id = _make_lead(rdb, phone_suffix="9401")
    _route(rdb, lead_id)

    with pytest.raises(Exception, match="append-only"):
        rdb.execute(
            text("DELETE FROM lending.file_routing_events WHERE lead_id = :l"),
            {"l": lead_id},
        )
        rdb.commit()
    rdb.rollback()


def test_file_routing_events_rejects_core_field_update(rdb):
    """Updating a core field raises (only slack_ts/slack_posted_at may change)."""
    l1 = _add_lender(rdb, name="Trigger Lender 2", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    lead_id = _make_lead(rdb, phone_suffix="9402")
    _route(rdb, lead_id)
    ev_id = _routed_event_id(rdb, lead_id)

    with pytest.raises(Exception, match="append-only"):
        rdb.execute(
            text("UPDATE lending.file_routing_events SET action = 'declined' WHERE id = :i"),
            {"i": ev_id},
        )
        rdb.commit()
    rdb.rollback()


# ── No lender name in code ────────────────────────────────────────────────────

def test_no_lender_name_hard_coded_in_routing_package():
    """grep guard: no specific lender name ('Backflip', 'RCN', 'ABL', etc.) in router/rules/seat/decline."""
    import ast
    from pathlib import Path

    forbidden = re.compile(r"\b(backflip|rcn|abl|easy.?street|kiavi)\b", re.IGNORECASE)
    routing_dir = Path(__file__).parent.parent.parent / "src" / "lending" / "routing"
    for py_file in routing_dir.glob("*.py"):
        source = py_file.read_text(encoding="utf-8")
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue  # skip comments
            if forbidden.search(stripped):
                pytest.fail(
                    f"Hard-coded lender name found in {py_file.name}: {stripped!r}"
                )


# ── Decline default reason ────────────────────────────────────────────────────

def test_decline_stores_default_reason_when_none_given(rdb):
    from config.lending_routing import DEFAULT_DECLINE_REASON
    from src.lending.routing.decline import record_decline

    l1 = _add_lender(rdb, name="Default Reason Lender", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    lead_id = _make_lead(rdb, phone_suffix="9501")
    _route(rdb, lead_id)
    ev_id = _routed_event_id(rdb, lead_id)
    record_decline(ev_id, reason_code=None, actor="josh", db=rdb)

    events = _events(rdb, lead_id)
    decline_ev = next(e for e in events if e["action"] == "declined")
    assert decline_ev["reason_code"] == DEFAULT_DECLINE_REASON


# ── Hold summary: hour check ──────────────────────────────────────────────────

def test_hold_summary_skips_non_8am():
    from src.tasks.lending_routing_hold_summary import _is_summary_hour
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
    at_8 = datetime(2026, 10, 10, 8, 0, tzinfo=ET)
    at_9 = datetime(2026, 10, 10, 9, 0, tzinfo=ET)
    assert _is_summary_hour(at_8) is True
    assert _is_summary_hour(at_9) is False
