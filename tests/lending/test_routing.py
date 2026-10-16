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

import json
import re
from datetime import datetime, timezone
from typing import Optional

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from migrations.apply_lending_lendingflow import apply_to as apply_lf

from src.lending.contracts import LoanType

# The values LendingFlow leads actually carry (the T-11 parser stores LoanType values).
LOAN_TYPE_FLIP = LoanType.FIX_AND_FLIP.value
LOAN_TYPE_GU = LoanType.GROUND_UP_CONSTRUCTION.value
LOAN_TYPE_DSCR = LoanType.DSCR_RENTAL.value
FL = "FL"


# ── Fixture: routing DB (lending_db + lendingflow + lender_routing) ───────────

@pytest.fixture
def rdb(lending_db):
    """lending_db with lendingflow_leads and lender_routing tables applied.

    Everything runs inside lending_db's rolled-back transaction. Run against a
    disposable Postgres (docker), not the shared DB: the routing tables have
    unique constraints that clash with any leftover rows.
    """
    from migrations.apply_lending_lender_rule_values import apply_to as apply_rule_values
    from migrations.apply_lending_lendingflow_enrichment import apply_to as apply_enrichment
    from migrations.apply_lending_partner_door import apply_to as apply_partner_door
    from src.lending.models import LendingContact

    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)  # FK target on a fresh database
    apply_lf(conn)
    apply_enrichment(conn)
    apply_rule_values(conn)
    apply_partner_door(conn)  # the router also routes partner files, so it needs loan_files
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
    rule_keys: Optional[list[str]] = None,
) -> int:
    """Insert a lender row and return its id."""
    lid = db.execute(
        text(
            "INSERT INTO lending.lender_routing_rules"
            "  (name, active, is_primary, agreement_status, priority,"
            "   monthly_cap, construction_min_amount, takes_ground_up, rule_keys)"
            " VALUES (:n, :a, :p, :s, :prio, :cap, :cmin, :gu, :rk)"
            " RETURNING id"
        ),
        {
            "n": name, "a": active, "p": is_primary, "s": agreement_status,
            "prio": priority, "cap": monthly_cap, "cmin": construction_min_amount, "gu": takes_ground_up,
            "rk": rule_keys,
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
    suppressed: bool = False,
    ghl_status: str = "synced",
    last_name: Optional[str] = None,
) -> int:
    """Insert a minimal lendingflow_leads row and return its id."""
    from src.services.phone_utils import normalize

    phone = normalize(f"727555{phone_suffix}")
    lid = db.execute(
        text(
            "INSERT INTO lending.lendingflow_leads"
            "  (vendor_lead_id, dedupe_hash, phone, loan_type, loan_amount,"
            "   property_state, ghl_status, suppressed, raw_payload, consent_status, last_name)"
            " VALUES (:vid, :dh, :ph, :lt, :la, :st, :gs, :sup, '{}'::jsonb, 'present', :ln)"
            " RETURNING id"
        ),
        {
            "vid": f"TEST-{phone_suffix}",
            "dh": f"hash-{phone_suffix}",
            "ph": phone,
            "lt": loan_type,
            "la": loan_amount,
            "st": state,
            "gs": ghl_status,
            "sup": suppressed,
            "ln": last_name,
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
    record_decline(ev1, reason_code="ltv_over_max", actor="josh", db=rdb)
    assert _fr(rdb, lead_id)["current_lender_name"] == "Overflow One"

    ev2 = _routed_event_id(rdb, lead_id)
    record_decline(ev2, reason_code="ltv_over_max", actor="josh", db=rdb)
    assert _fr(rdb, lead_id)["current_lender_name"] == "Overflow Two"

    ev3 = _routed_event_id(rdb, lead_id)
    record_decline(ev3, reason_code="ltv_over_max", actor="josh", db=rdb)
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
    record_decline(ev, reason_code="ltv_over_max", actor="josh", db=rdb)
    record_decline(ev, reason_code="ltv_over_max", actor="josh", db=rdb)

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


# ── A decline always needs a reason (Josh: no default) ────────────────────────

def test_a_decline_needs_a_reason_and_other_needs_details(rdb):
    from src.lending.routing.decline import record_decline

    l1 = _add_lender(rdb, name="Reason Lender", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    lead_id = _make_lead(rdb, phone_suffix="9501")
    _route(rdb, lead_id)
    ev_id = _routed_event_id(rdb, lead_id)
    for missing in (None, "", "   "):
        with pytest.raises(ValueError, match="reason is required"):
            record_decline(ev_id, reason_code=missing, actor="josh", db=rdb)
    for no_details in (None, "", "  "):
        with pytest.raises(ValueError, match="say why"):
            record_decline(ev_id, reason_code="other", actor="josh", db=rdb, free_text=no_details)
    assert [e["action"] for e in _events(rdb, lead_id)] == ["routed"]  # nothing was recorded

    record_decline(ev_id, reason_code="other", actor="josh", db=rdb, free_text="Lender is out of the market")
    declined = next(e for e in _events(rdb, lead_id) if e["action"] == "declined")
    assert declined["reason_code"] == "other"


# ── Hold summary: hour check ──────────────────────────────────────────────────

def test_hold_summary_skips_non_8am():
    from src.tasks.lending_routing_hold_summary import _is_summary_hour
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
    at_8 = datetime(2026, 10, 10, 8, 0, tzinfo=ET)
    at_9 = datetime(2026, 10, 10, 9, 0, tzinfo=ET)
    assert _is_summary_hour(at_8) is True
    assert _is_summary_hour(at_9) is False


# ── Spec gap fixes (A–F, sweep trigger, suppressed leads) ─────────────────────

class _FakeSlack:
    def __init__(self):
        self.sent: list[str] = []

    def chat_postMessage(self, channel, text, blocks=None):  # noqa: N802 - slack_sdk API name
        self.sent.append(text)
        self.blocks = getattr(self, "blocks", []) + [blocks]
        return {"ts": f"{len(self.sent)}.0"}


@pytest.fixture
def fake_slack(monkeypatch):
    client = _FakeSlack()
    monkeypatch.setattr("src.lending.routing.notify._client", lambda: client)
    monkeypatch.setattr("src.lending.routing.notify._channel", lambda: "C-TEST")
    return client


def test_funded_outcome_moves_seat_cap_end_to_end(rdb):
    """A: the cap counts funded files, and record_outcome is the production path that writes them."""
    from src.lending.routing.outcome import record_outcome

    primary = _add_lender(rdb, name="Cap Primary", is_primary=True, priority=1, monthly_cap=2)
    _add_box(rdb, primary, loan_types=[LOAN_TYPE_FLIP])
    overflow = _add_lender(rdb, name="Overflow", priority=2)
    _add_box(rdb, overflow, loan_types=[LOAN_TYPE_FLIP])

    leads = [_make_lead(rdb, phone_suffix=f"11{i}0") for i in range(4)]
    _route(rdb, leads[0])
    _route(rdb, leads[1])
    _route(rdb, leads[2])  # nothing funded yet, so the cap has not filled
    assert _fr(rdb, leads[2])["current_lender_name"] == "Cap Primary"

    assert record_outcome(leads[0], action="funded", actor="josh", db=rdb) is True
    assert record_outcome(leads[1], action="funded", actor="josh", db=rdb) is True
    _route(rdb, leads[3])
    assert _fr(rdb, leads[3])["current_lender_name"] == "Overflow"


def test_outcome_idempotent_and_guarded(rdb):
    from src.lending.routing.outcome import record_outcome

    l1 = _add_lender(rdb, name="Only", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="1201")
    _route(rdb, lead)

    assert record_outcome(lead, action="approved", actor="josh", db=rdb) is True
    assert record_outcome(lead, action="funded", actor="josh", db=rdb) is True
    assert record_outcome(lead, action="funded", actor="josh", db=rdb) is False
    assert sum(1 for e in _events(rdb, lead) if e["action"] == "funded") == 1

    with pytest.raises(ValueError):
        record_outcome(lead, action="declined", actor="josh", db=rdb)

    held = _make_lead(rdb, loan_type="other", phone_suffix="1202")
    _route(rdb, held)
    assert _fr(rdb, held)["status"] == "held"
    with pytest.raises(ValueError):
        record_outcome(held, action="funded", actor="josh", db=rdb)


def test_decline_rejects_unknown_reason(rdb):
    """E: the reason must be in the fixed enum; nothing is written for an unknown code."""
    from src.lending.routing.decline import record_decline

    l1 = _add_lender(rdb, name="A", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="1301")
    _route(rdb, lead)
    ev = _routed_event_id(rdb, lead)

    with pytest.raises(ValueError):
        record_decline(ev, reason_code="not_a_code", actor="josh", db=rdb)
    assert not any(e["action"] == "declined" for e in _events(rdb, lead))
    assert _fr(rdb, lead)["status"] == "routed"


def test_make_primary_is_atomic_and_applies_spec_defaults(rdb):
    """F + C: one call moves the flag; the new primary gets cap 15 and the $500k ground-up minimum."""
    from src.lending.routing.lender_service import make_primary

    old = _add_lender(rdb, name="Old", is_primary=True, priority=1, monthly_cap=5)
    new = _add_lender(rdb, name="New", priority=2)

    result = make_primary(new, rdb)
    assert result["demoted"] == [old]
    rows = {r["name"]: r for r in rdb.execute(text(
        "SELECT name, is_primary, monthly_cap, construction_min_amount FROM lending.lender_routing_rules"
    )).mappings()}
    assert rows["Old"]["is_primary"] is False and rows["Old"]["monthly_cap"] == 5
    assert rows["New"]["is_primary"] is True
    assert rows["New"]["monthly_cap"] == 15
    assert rows["New"]["construction_min_amount"] == 500_000


def test_real_loan_type_values_route_not_hold(rdb):
    """The router must match the values the T-11 parser stores (uppercase LoanType values).

    Guards the production bug where lowercase constants sent every real file to the hold queue.
    """
    assert LOAN_TYPE_FLIP == "FIX_AND_FLIP" and LOAN_TYPE_GU == "GROUND_UP_CONSTRUCTION"
    lender = _add_lender(rdb, name="Real Values", priority=1)
    _add_box(rdb, lender, loan_types=["fix_and_flip"])  # a hand-typed lowercase box still matches
    lead = _make_lead(rdb, loan_type="FIX_AND_FLIP", phone_suffix="1801")
    _route(rdb, lead)
    assert _fr(rdb, lead)["current_lender_name"] == "Real Values"


def test_product_box_rejects_unknown_loan_types():
    """A typo in a box would silently never match, so the service refuses it and normalises case."""
    from pydantic import ValidationError

    from src.lending.routing.lender_service import ProductBoxIn

    assert ProductBoxIn(loan_types=["fix_and_flip", "Dscr_Rental"]).loan_types == ["FIX_AND_FLIP", "DSCR_RENTAL"]
    for bad in (["flip"], ["refi"], []):
        with pytest.raises(ValidationError):
            ProductBoxIn(loan_types=bad)


def test_create_primary_defaults_cap_to_15(rdb):
    from src.lending.routing.lender_service import LenderIn, create_lender

    created = create_lender(LenderIn(name="P", is_primary=True, priority=1), rdb)
    cap = rdb.execute(text("SELECT monthly_cap FROM lending.lender_routing_rules WHERE id = :i"),
                      {"i": created["id"]}).scalar()
    assert cap == 15


def test_slack_message_carries_all_spec_fields():
    """D: file ID, last name, product, amount, lender and position on route and decline posts."""
    from src.lending.routing.notify import build_message

    for action in ("routed", "declined"):
        msg = build_message(action, lead_id=42, last_name="Kowalski", loan_type="fix_and_flip",
                            loan_amount=300000, lender_name="Lender X", position=2, reason_code="ltv")
        for part in ("42", "Kowalski", "fix_and_flip", "$300,000", "Lender X", "position 2"):
            assert part in msg, f"{part!r} missing from {action} message"
    assert build_message("funded", lead_id=1, last_name=None, loan_type=None, loan_amount=None,
                         lender_name=None, position=None) is None


def test_retry_posts_full_messages_and_skips_outcomes(rdb, fake_slack):
    """D: the retry path posts the same fields, with the real position, and never clogs on outcomes."""
    from src.lending.routing.decline import record_decline
    from src.lending.routing.notify import retry_unposted_events
    from src.lending.routing.outcome import record_outcome

    a = _add_lender(rdb, name="First", priority=1)
    _add_box(rdb, a, loan_types=[LOAN_TYPE_FLIP])
    b = _add_lender(rdb, name="Second", priority=2)
    _add_box(rdb, b, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="1401", last_name="Nowak")

    _route(rdb, lead)
    record_decline(_routed_event_id(rdb, lead), reason_code="ltv_over_max", actor="josh", db=rdb)
    record_outcome(lead, action="funded", actor="josh", db=rdb)
    rdb.execute(text("UPDATE lending.file_routing_events SET slack_ts = NULL, slack_posted_at = NULL"))
    rdb.commit()
    fake_slack.sent.clear()

    posted = retry_unposted_events(rdb)
    assert posted == 3  # routed (First), declined (First), routed (Second); funded is not posted
    text_all = "\n".join(fake_slack.sent)
    assert "Nowak" in text_all and "$300,000" in text_all and LOAN_TYPE_FLIP in text_all
    assert "position 1" in text_all and "position 2" in text_all
    assert "funded" not in text_all.lower()
    assert retry_unposted_events(rdb) == 0


def test_sweep_routes_enriched_files_regardless_of_ghl_status(rdb):
    """Q4: routing triggers on finished enrichment, not on GoHighLevel sync state."""
    from src.tasks.lending_routing_sweep import sweep

    l1 = _add_lender(rdb, name="Sweep Lender", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])

    ready_pending_ghl = _make_lead(rdb, phone_suffix="1501", ghl_status="pending")
    ready_failed_ghl = _make_lead(rdb, phone_suffix="1502", ghl_status="failed")
    not_enriched = _make_lead(rdb, phone_suffix="1503")
    suppressed = _make_lead(rdb, phone_suffix="1504", suppressed=True)
    for lead, status in ((ready_pending_ghl, "ready"), (ready_failed_ghl, "ready"),
                         (not_enriched, "pending"), (suppressed, "ready")):
        rdb.execute(text("INSERT INTO lending.lendingflow_enrichment (lead_id, status) VALUES (:l, :s)"),
                    {"l": lead, "s": status})
    rdb.commit()

    sweep(rdb)

    assert _fr(rdb, ready_pending_ghl)["current_lender_name"] == "Sweep Lender"
    assert _fr(rdb, ready_failed_ghl)["current_lender_name"] == "Sweep Lender"
    assert _fr(rdb, not_enriched) == {}
    assert _fr(rdb, suppressed) == {}


def test_route_file_holds_suppressed_lead(rdb):
    """Q8: a suppressed lead is never routed, even by a direct route_file call; it stays visible."""
    l1 = _add_lender(rdb, name="Would Match", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="1601", suppressed=True)
    _route(rdb, lead)
    fr = _fr(rdb, lead)
    assert fr["status"] == "held" and fr["hold_reason"] == "suppressed"
    assert not any(e["lender_name"] == "Would Match" for e in _events(rdb, lead))


def test_seed_from_t05_matrix_is_inert_idempotent_and_correct(rdb):
    """B: the six T-05 rows seed the table as pending lenders; nothing routes until they are signed."""
    from migrations.apply_lending_lender_routing_seed import apply_to as seed

    conn = rdb.get_bind()
    assert seed(conn) == 5  # six matrix rows, two of which share the primary lender
    assert seed(conn) == 0  # idempotent

    lenders = rdb.execute(text(
        "SELECT id, name, is_primary, agreement_status, monthly_cap, construction_min_amount"
        "  FROM lending.lender_routing_rules ORDER BY priority"
    )).mappings().all()
    assert len(lenders) == 5
    assert all(r["agreement_status"] == "pending" for r in lenders)
    primaries = [r for r in lenders if r["is_primary"]]
    assert len(primaries) == 1
    assert primaries[0]["monthly_cap"] == 15 and primaries[0]["construction_min_amount"] == 500_000
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_product_boxes")).scalar() == 0  # numbers live in T-05
    keyed = {r["name"]: r["rule_keys"] for r in rdb.execute(text(
        "SELECT name, rule_keys FROM lending.lender_routing_rules")).mappings()}
    assert all(keyed.values()) and sum(len(v) for v in keyed.values()) == 6  # six T-05 rows, five lenders

    flip = _make_lead(rdb, phone_suffix="1701")
    _route(rdb, flip)
    assert _fr(rdb, flip)["status"] == "held" and _fr(rdb, flip)["hold_reason"] == "no_match"

    # Once signed, the seeded boxes route real files: primary first for flips and for $650k ground-up,
    # the first signed ground-up overflow lender for $450k ground-up.
    rdb.execute(text("UPDATE lending.lender_routing_rules SET agreement_status = 'signed'"))
    rdb.commit()
    primary_name = primaries[0]["name"]
    flip2 = _make_lead(rdb, phone_suffix="1702")
    gu_650 = _make_lead(rdb, loan_type=LOAN_TYPE_GU, loan_amount=650_000, phone_suffix="1703")
    gu_450 = _make_lead(rdb, loan_type=LOAN_TYPE_GU, loan_amount=450_000, phone_suffix="1704")
    for lead in (flip2, gu_650, gu_450):
        _route(rdb, lead)
    assert _fr(rdb, flip2)["current_lender_name"] == primary_name
    assert _fr(rdb, gu_650)["current_lender_name"] == primary_name
    assert _fr(rdb, gu_450)["current_lender_name"] not in (None, primary_name)


# ── Admin screen backend: file record, reorder, safe PATCH ────────────────────

def test_file_record_shows_current_next_and_history(rdb):
    from src.lending.routing.decline import record_decline
    from src.lending.routing.file_key import lead_key
    from src.lending.routing.lender_service import NOT_FOUND, RoutingError, file_record

    a = _add_lender(rdb, name="First", priority=1)
    _add_box(rdb, a, loan_types=[LOAN_TYPE_FLIP])
    b = _add_lender(rdb, name="Second", priority=2)
    _add_box(rdb, b, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="1901", last_name="Wozniak")
    _route(rdb, lead)

    rec = file_record(lead_key(lead), rdb)
    assert rec["file"]["last_name"] == "Wozniak" and rec["file"]["status"] == "routed"
    assert rec["current"]["name"] == "First" and rec["current"]["position"] == 1
    assert rec["next"]["name"] == "Second" and rec["next"]["position"] == 2
    assert [h["action"] for h in rec["history"]] == ["routed"]

    record_decline(_routed_event_id(rdb, lead), reason_code="other", free_text="Lender left the market", actor="josh", db=rdb)
    rec = file_record(lead_key(lead), rdb)
    assert rec["current"]["name"] == "Second" and rec["next"] is None
    assert [h["action"] for h in rec["history"]] == ["routed", "declined", "routed"]
    assert rec["history"][1]["reason_code"] == "other" and rec["history"][1]["actor"] == "josh"

    record_decline(_routed_event_id(rdb, lead), reason_code="ltv_over_max", actor="josh", db=rdb)
    rec = file_record(lead_key(lead), rdb)
    assert rec["file"]["status"] == "exhausted" and rec["current"] is None
    with pytest.raises(RoutingError) as missing:
        file_record(lead_key(999999), rdb)
    assert missing.value.kind == NOT_FOUND


def test_preview_next_agrees_with_the_real_router(rdb):
    """The screen's "who is next" must be exactly what the router then does."""
    from src.lending.routing.router import preview_next

    a = _add_lender(rdb, name="Alpha", priority=1)
    _add_box(rdb, a, loan_types=[LOAN_TYPE_FLIP])
    b = _add_lender(rdb, name="Beta", priority=2)
    _add_box(rdb, b, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="1902")

    predicted = preview_next(lead, rdb)  # an unrouted file: the lender it would go to first
    assert predicted == {"lender_id": a, "name": "Alpha", "position": 1}
    assert _fr(rdb, lead) == {}  # a preview writes nothing
    _route(rdb, lead)
    assert _fr(rdb, lead)["current_lender_name"] == predicted["name"]


def test_reorder_is_atomic_validated_and_changes_routing(rdb):
    from src.lending.routing.lender_service import NOT_FOUND, ReorderIn, RoutingError, reorder_lenders

    ids = {}
    for i, name in enumerate(("Aye", "Bee", "Sea"), start=1):
        ids[name] = _add_lender(rdb, name=name, priority=i)
        _add_box(rdb, ids[name], loan_types=[LOAN_TYPE_FLIP])
    listed = lambda: [r[0] for r in rdb.execute(text("SELECT name FROM lending.lender_routing_rules ORDER BY priority"))]  # noqa: E731
    first = _make_lead(rdb, phone_suffix="2101")
    _route(rdb, first)
    assert _fr(rdb, first)["current_lender_name"] == "Aye"

    order = [ids["Sea"], ids["Aye"], ids["Bee"]]
    reorder_lenders(ReorderIn(ordered_ids=order), rdb)
    assert listed() == ["Sea", "Aye", "Bee"]
    second = _make_lead(rdb, phone_suffix="2102")
    _route(rdb, second)
    assert _fr(rdb, second)["current_lender_name"] == "Sea"

    with pytest.raises(RoutingError, match="Every active lender"):  # an active one left out
        reorder_lenders(ReorderIn(ordered_ids=[ids["Sea"], ids["Aye"]]), rdb)
    with pytest.raises(RoutingError) as unknown:
        reorder_lenders(ReorderIn(ordered_ids=order + [99999]), rdb)
    assert unknown.value.kind == NOT_FOUND
    with pytest.raises(RoutingError, match="duplicates"):
        reorder_lenders(ReorderIn(ordered_ids=[order[0], order[0], order[1], order[2]]), rdb)
    assert listed() == ["Sea", "Aye", "Bee"]  # every refusal left the order alone


def test_a_priority_clash_is_a_conflict_and_a_seat_cap_can_be_cleared(rdb):
    from src.lending.routing.lender_service import CONFLICT, LenderIn, LenderPatch, RoutingError, create_lender, update_lender

    a = _add_lender(rdb, name="One", priority=1, monthly_cap=4)
    b = _add_lender(rdb, name="Two", priority=2)

    with pytest.raises(RoutingError) as clash:
        update_lender(b, LenderPatch(priority=1), rdb)
    assert clash.value.kind == CONFLICT
    with pytest.raises(RoutingError) as duplicate:
        create_lender(LenderIn(name="Dup", priority=1), rdb)
    assert duplicate.value.kind == CONFLICT
    update_lender(a, LenderPatch(active=False), rdb)  # pausing frees the priority for the active lenders
    update_lender(b, LenderPatch(priority=1), rdb)

    update_lender(a, LenderPatch(monthly_cap=None), rdb)  # an explicit None clears the cap
    assert rdb.execute(text("SELECT monthly_cap FROM lending.lender_routing_rules WHERE id = :i"), {"i": a}).scalar() is None
    with pytest.raises(RoutingError, match="cannot be empty"):
        update_lender(a, LenderPatch(name=None), rdb)


def test_a_failing_route_hook_does_not_poison_the_enrichment_session(rdb, monkeypatch):
    """The R-1 hook after enrichment is best-effort: when routing fails (say the R-1 tables are not migrated
    yet), the caller's session must stay usable so the next lead in the same sweep can still be enriched."""
    from src.lending.enrichment import service
    from src.lending.enrichment.ports import FakeDealThread

    monkeypatch.setattr(service, "match_property", lambda *a, **k: None)
    lead = _make_lead(rdb, phone_suffix="2201")
    service.ensure_row(rdb, lead)

    def boom(lead_id, db):
        db.execute(text("SELECT 1 FROM lending.table_that_does_not_exist"))  # aborts the transaction

    monkeypatch.setattr("src.lending.routing.router.route_file", boom)
    assert service.enrich_lead(rdb, lead, thread=FakeDealThread()).status == "ready"
    status = rdb.execute(text("SELECT status FROM lending.lendingflow_enrichment WHERE lead_id = :l"),
                         {"l": lead}).scalar()
    assert status == "ready"  # raises InFailedSqlTransaction if the failed route left the session aborted


# ── Reconcile with T-05: one source of truth for lender rule values ───────────

FLIP_KEY = "generic_backflip_flip"  # a T-05 lender key (config/lender_matrix.py): flip, min loan $100,000


def test_a_lender_with_rule_keys_is_judged_by_the_t05_values_not_by_its_own_boxes(rdb):
    """T-05's numbers win. A box added directly on the lender (which the API forbids) is ignored."""
    lender = _add_lender(rdb, name="T05 Lender", priority=1, rule_keys=[FLIP_KEY])
    _add_box(rdb, lender, loan_types=[LOAN_TYPE_FLIP])  # would accept anything; must be ignored

    small = _make_lead(rdb, loan_amount=50_000, phone_suffix="3001")  # below the T-05 flip minimum of $100,000
    fits = _make_lead(rdb, loan_amount=300_000, phone_suffix="3002")
    _route(rdb, small)
    _route(rdb, fits)
    assert _fr(rdb, small)["status"] == "held" and _fr(rdb, small)["hold_reason"] == "no_match"
    assert _fr(rdb, fits)["current_lender_name"] == "T05 Lender"


def test_routing_follows_edits_to_the_t05_rule_values_when_the_switch_is_on(rdb, monkeypatch):
    """Same switch as the fit card: LENDING_RULES_FROM_DB reads lending.lender_rule_values for routing too."""
    from config.settings import get_settings

    _add_lender(rdb, name="Edited In Table", priority=1, rule_keys=[FLIP_KEY])
    rdb.execute(text(
        "UPDATE lending.lender_rule_values SET value = '400000'::jsonb, status = 'confirmed'"
        " WHERE lender_key = :k AND field = 'min_loan_amount'"), {"k": FLIP_KEY})
    rdb.commit()

    switch_off = _make_lead(rdb, loan_amount=300_000, phone_suffix="3101")
    _route(rdb, switch_off)  # switch off: the config value ($100,000) applies, the edit is not read
    assert _fr(rdb, switch_off)["current_lender_name"] == "Edited In Table"

    monkeypatch.setattr(get_settings(), "lending_rules_from_db", True)
    switch_on = _make_lead(rdb, loan_amount=300_000, phone_suffix="3102")
    _route(rdb, switch_on)  # switch on: the edited minimum ($400,000) applies
    assert _fr(rdb, switch_on)["status"] == "held"


def test_a_lender_with_no_rules_at_all_is_never_routed_to(rdb):
    """Unknown eligibility must not mean "accepts everything": a signed lender with no rules gets nothing."""
    _add_lender(rdb, name="No Rules Yet", priority=1)  # signed and active, no rule_keys, no boxes
    lead = _make_lead(rdb, phone_suffix="3201")
    _route(rdb, lead)
    assert _fr(rdb, lead)["status"] == "held" and _fr(rdb, lead)["hold_reason"] == "no_match"


def test_an_unknown_rule_key_fails_closed_without_crashing(rdb):
    _add_lender(rdb, name="Typo Key", priority=1, rule_keys=["not_a_t05_lender"])
    lead = _make_lead(rdb, phone_suffix="3301")
    _route(rdb, lead)
    assert _fr(rdb, lead)["status"] == "held"


def test_rule_keys_are_validated_and_a_t05_lender_refuses_boxes(rdb):
    from pydantic import ValidationError

    from src.lending.routing.lender_service import (
        CONFLICT, LenderIn, LenderPatch, ProductBoxIn, RoutingError, add_box, create_lender, update_lender)

    with pytest.raises(ValidationError):
        LenderIn(name="Bad", priority=1, rule_keys=["nope"])
    lender_id = create_lender(LenderIn(name="Linked", priority=1, rule_keys=[FLIP_KEY]), rdb)["id"]
    assert rdb.execute(text("SELECT rule_keys FROM lending.lender_routing_rules WHERE id = :i"), {"i": lender_id}).scalar() == [FLIP_KEY]

    with pytest.raises(RoutingError) as blocked:
        add_box(lender_id, ProductBoxIn(loan_types=["fix_and_flip"]), rdb)
    assert blocked.value.kind == CONFLICT and "lender_rule_values" in str(blocked.value)

    update_lender(lender_id, LenderPatch(rule_keys=None), rdb)  # unlink
    assert add_box(lender_id, ProductBoxIn(loan_types=["fix_and_flip"]), rdb)["id"]


# ── Josh's decline-reason enum ────────────────────────────────────────────────

def test_decline_reasons_are_joshs_enum_and_the_old_placeholders_are_off(rdb):
    from config.lending_routing import LEGACY_PLACEHOLDER_REASONS, SEED_DECLINE_REASONS

    active = {code for code, on in rdb.execute(text("SELECT code, active FROM lending.decline_reason_codes")) if on}
    josh = {code for code, _ in SEED_DECLINE_REASONS}
    assert len(josh) == 14 and josh <= active  # codes Josh adds later may sit alongside; his 14 must all be on
    assert {"credit_below_min", "ltv_over_max", "seat_cap_reached", "borrower_withdrew", "other"} <= active
    legacy_codes = {code for code, _ in LEGACY_PLACEHOLDER_REASONS}
    assert not (legacy_codes - {"other"}) & active  # the old placeholders are off


def test_a_decline_must_use_a_code_from_joshs_enum(rdb):
    from src.lending.routing.decline import record_decline

    a = _add_lender(rdb, name="Enum A", priority=1)
    _add_box(rdb, a, loan_types=[LOAN_TYPE_FLIP])
    b = _add_lender(rdb, name="Enum B", priority=2)
    _add_box(rdb, b, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="3401")
    _route(rdb, lead)
    ev = _routed_event_id(rdb, lead)

    for retired in ("unspecified", "ltv", "credit"):  # the placeholders used before Josh's list
        with pytest.raises(ValueError):
            record_decline(ev, reason_code=retired, actor="josh", db=rdb)
    assert not any(e["action"] == "declined" for e in _events(rdb, lead))
    record_decline(ev, reason_code="credit_below_min", actor="josh", db=rdb)
    assert [e["reason_code"] for e in _events(rdb, lead) if e["action"] == "declined"] == ["credit_below_min"]


def test_rerunning_the_migration_retires_old_placeholders_but_keeps_codes_added_later(rdb):
    from migrations.apply_lending_lender_routing import apply_to

    conn = rdb.get_bind()
    conn.execute(text("INSERT INTO lending.decline_reason_codes (code, label, active) VALUES"
                      " ('unspecified', 'Not specified', TRUE), ('appraisal_low', 'added by Josh later', TRUE)"
                      " ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label, active = TRUE"))
    apply_to(conn)
    state = {r["code"]: r["active"] for r in rdb.execute(text(
        "SELECT code, active FROM lending.decline_reason_codes WHERE code IN ('unspecified', 'appraisal_low', 'other')"
    )).mappings()}
    assert state == {"unspecified": False, "appraisal_low": True, "other": True}  # off, never deleted


# ── Partner files (P-1) go through the same router and keep the partner informed ──

def _make_partner(db: Session, *, name: str = "Maria Lopez", email: str = "maria@example.com") -> int:
    import hashlib
    import uuid

    pid = db.execute(
        text(
            "INSERT INTO lending.partners (name, partner_type, email, agreement_status, token_hash)"
            " VALUES (:n, 'wholesaler', :e, 'signed', :h) RETURNING id"
        ),
        {"n": name, "e": email, "h": hashlib.sha256(uuid.uuid4().bytes).hexdigest()},
    ).scalar_one()
    db.commit()
    return pid


def _make_partner_file(
    db: Session,
    partner_id: int,
    *,
    product: str = "flip",
    amount: int = 300_000,
    state: str = FL,
    band: Optional[str] = "680-719",
    projects: Optional[int] = 4,
    last_name: str = "Reyes",
    status: str = "received",
    status_reason: Optional[str] = None,
) -> int:
    fid = db.execute(
        text(
            "INSERT INTO lending.loan_files (source, partner_id, borrower_first_name, borrower_last_name, product,"
            "   loan_amount, property_state, credit_band, completed_projects, status, status_reason)"
            " VALUES ('partner', :p, 'Ana', :ln, :prod, :amt, :st, :band, :proj, :status, :reason) RETURNING id"
        ),
        {"p": partner_id, "ln": last_name, "prod": product, "amt": amount, "st": state, "band": band,
         "proj": projects, "status": status, "reason": status_reason},
    ).scalar_one()
    db.commit()
    return fid


def _pfr(db: Session, file_id: int) -> dict:
    row = db.execute(
        text("SELECT status, current_lender_name, route_position, hold_reason"
             "  FROM lending.file_routing WHERE loan_file_id = :f"), {"f": file_id},
    ).mappings().one_or_none()
    return dict(row) if row else {}


def _pevents(db: Session, file_id: int) -> list[str]:
    return [r[0] for r in db.execute(
        text("SELECT action FROM lending.file_routing_events WHERE loan_file_id = :f ORDER BY id"), {"f": file_id})]


def _file_status(db: Session, file_id: int) -> tuple:
    row = db.execute(text("SELECT status, status_reason, funded_at FROM lending.loan_files WHERE id = :f"),
                     {"f": file_id}).one()
    return row[0], row[1], row[2]


def _two_flip_lenders(db: Session) -> tuple[int, int]:
    a = _add_lender(db, name="Alpha Capital", priority=1)
    _add_box(db, a, loan_types=[LOAN_TYPE_FLIP])
    b = _add_lender(db, name="Beta Funding", priority=2)
    _add_box(db, b, loan_types=[LOAN_TYPE_FLIP])
    return a, b


@pytest.fixture
def partner_mail(rdb, monkeypatch):
    """P-1's real ``change_status`` on the test session, with the partner's emails captured.

    The router calls ``partner_link.mirror_status``; here it reaches the real status change and notice, so the
    tests prove the whole chain (router, then file status, then partner notice), not just that a call was made.
    """
    from contextlib import contextmanager

    from src.lending import partner_pipeline
    from src.lending.routing import partner_link

    sent: list[tuple[str, str, str]] = []
    services = partner_pipeline.PipelineServices(
        email=lambda to, subject, body: sent.append((to, subject, body)), sms=None, slack=None, render_pdf=None,
        evaluate_fit=lambda *a, **k: None, fit_limits=lambda *a, **k: [], load_matrix=lambda: (),
        opt_outs=lambda phone, email: partner_pipeline.OptOuts(False, False),
        text_window_open=lambda phone: False, upload_dir="", prequal_pct=10,
    )

    @contextmanager
    def session_factory():
        yield rdb
        rdb.commit()

    def mirror(key, status, reason=None):
        if key.kind == "loan_file":
            partner_pipeline.change_status(key.id, status, reason, services=services, session_factory=session_factory)

    monkeypatch.setattr(partner_link, "mirror_status", mirror)
    return sent


def test_a_partner_file_is_routed_like_any_file_and_the_partner_is_told(rdb, partner_mail):
    from src.lending.routing.router import route_loan_file

    _two_flip_lenders(rdb)
    fid = _make_partner_file(rdb, _make_partner(rdb))
    route_loan_file(fid, rdb)

    assert _pfr(rdb, fid)["current_lender_name"] == "Alpha Capital" and _pfr(rdb, fid)["route_position"] == 1
    assert _pevents(rdb, fid) == ["routed"]
    assert _file_status(rdb, fid)[0] == "routed"
    assert len(partner_mail) == 1
    to, subject, body = partner_mail[0]
    assert to == "maria@example.com" and "sent to a lender" in body and f"#{fid}" in body
    assert "Alpha" not in body and "Alpha" not in subject  # the partner never learns which lender


def test_a_partner_file_walks_the_decline_cascade_and_each_step_reaches_the_partner(rdb, partner_mail):
    from src.lending.routing.decline import record_decline
    from src.lending.routing.router import route_loan_file

    _two_flip_lenders(rdb)
    fid = _make_partner_file(rdb, _make_partner(rdb))
    route_loan_file(fid, rdb)

    def routed_event() -> int:
        return rdb.execute(text("SELECT id FROM lending.file_routing_events"
                                " WHERE loan_file_id = :f AND action = 'routed' ORDER BY id DESC LIMIT 1"),
                           {"f": fid}).scalar_one()

    record_decline(routed_event(), reason_code="ltv_over_max", actor="josh", db=rdb)
    assert _pfr(rdb, fid)["current_lender_name"] == "Beta Funding"
    assert _file_status(rdb, fid)[0] == "routed"

    record_decline(routed_event(), reason_code="credit_below_min", actor="josh", db=rdb)
    assert _pfr(rdb, fid)["status"] == "exhausted"
    assert _file_status(rdb, fid)[:2] == ("held", "exhausted")  # "held" on the shared file, the reason says why
    assert _pevents(rdb, fid) == ["routed", "declined", "routed", "declined", "held"]

    wording = [m[2] for m in partner_mail]  # routed, declined, routed, declined; a hold is internal, no notice
    assert [("sent to a lender" in w, "declined" in w) for w in wording] == [
        (True, False), (False, True), (True, False), (False, True)]


def test_funding_a_partner_file_counts_against_the_seat_cap_and_tells_the_partner(rdb, partner_mail):
    from src.lending.routing.outcome import record_outcome
    from src.lending.routing.router import route_loan_file

    primary = _add_lender(rdb, name="Capped Primary", is_primary=True, priority=1, monthly_cap=1)
    _add_box(rdb, primary, loan_types=[LOAN_TYPE_FLIP])
    overflow = _add_lender(rdb, name="Overflow", priority=2)
    _add_box(rdb, overflow, loan_types=[LOAN_TYPE_FLIP])
    fid = _make_partner_file(rdb, _make_partner(rdb))
    route_loan_file(fid, rdb)
    assert _pfr(rdb, fid)["current_lender_name"] == "Capped Primary"

    assert record_outcome(fid, action="funded", actor="josh", db=rdb, kind="loan_file") is True
    assert record_outcome(fid, action="funded", actor="josh", db=rdb, kind="loan_file") is False  # idempotent
    status, _, funded_at = _file_status(rdb, fid)
    assert status == "funded" and funded_at is not None
    assert "has funded" in partner_mail[-1][2]

    later_lead = _make_lead(rdb, phone_suffix="4001")  # a LendingFlow lead now sees the primary at its cap
    _route(rdb, later_lead)
    assert _fr(rdb, later_lead)["current_lender_name"] == "Overflow"


def test_a_partner_file_with_no_matching_lender_is_held_visibly_and_not_announced(rdb, partner_mail):
    from src.lending.routing.router import route_loan_file

    lender = _add_lender(rdb, name="Flip Only", priority=1)
    _add_box(rdb, lender, loan_types=[LOAN_TYPE_FLIP])
    other = _make_partner_file(rdb, _make_partner(rdb), product="other")  # no lender-box loan type
    route_loan_file(other, rdb)

    assert _pfr(rdb, other)["status"] == "held" and _pfr(rdb, other)["hold_reason"] == "no_match"
    assert _file_status(rdb, other)[:2] == ("held", "no_match")
    assert partner_mail == []  # a hold is internal; the partner dashboard simply keeps saying "in review"


def test_the_partners_credit_band_and_experience_are_read_by_the_boxes(rdb, partner_mail):
    from src.lending.routing.router import route_loan_file

    strict = _add_lender(rdb, name="Strict", priority=1)
    rdb.execute(text(
        "INSERT INTO lending.lender_product_boxes (lender_id, loan_types, credit_floor, min_completed_projects)"
        " VALUES (:l, ARRAY['FIX_AND_FLIP'], 640, 3)"), {"l": strict})
    easy = _add_lender(rdb, name="Easy", priority=2)
    _add_box(rdb, easy, loan_types=[LOAN_TYPE_FLIP])
    rdb.commit()
    partner = _make_partner(rdb)

    low_credit = _make_partner_file(rdb, partner, band="Below 640", projects=5)
    green = _make_partner_file(rdb, partner, band="720-759", projects=1)
    fits = _make_partner_file(rdb, partner, band="720-759", projects=4)
    for fid in (low_credit, green, fits):
        route_loan_file(fid, rdb)
    assert _pfr(rdb, low_credit)["current_lender_name"] == "Easy"  # band below the floor
    assert _pfr(rdb, green)["current_lender_name"] == "Easy"  # too few projects
    assert _pfr(rdb, fits)["current_lender_name"] == "Strict"


def test_the_sweep_routes_partner_files_parked_before_the_router_existed(rdb, partner_mail):
    from config.lending_partners import AWAITING_ROUTER_REASON
    from src.tasks.lending_routing_sweep import sweep

    _two_flip_lenders(rdb)
    partner = _make_partner(rdb)
    parked = _make_partner_file(rdb, partner, status="held", status_reason=AWAITING_ROUTER_REASON)
    still_received = _make_partner_file(rdb, partner, status="received")  # the Partner Door's own sweep owns this
    sweep(rdb)

    assert _pfr(rdb, parked)["current_lender_name"] == "Alpha Capital"
    assert _file_status(rdb, parked)[0] == "routed" and len(partner_mail) == 1
    assert _pfr(rdb, still_received) == {}


def test_a_routed_partner_file_that_never_took_its_status_is_healed_without_undoing_an_outcome(
        rdb, partner_mail, monkeypatch):
    from src.lending.routing import partner_link
    from src.lending.routing.outcome import record_outcome
    from src.lending.routing.router import route_loan_file

    _two_flip_lenders(rdb)
    fid = _make_partner_file(rdb, _make_partner(rdb))
    real_mirror = partner_link.mirror_status
    monkeypatch.setattr(partner_link, "mirror_status", lambda *a, **k: None)  # the sync after the commit failed
    route_loan_file(fid, rdb)
    assert _pfr(rdb, fid)["status"] == "routed" and _file_status(rdb, fid)[0] == "received"

    monkeypatch.setattr(partner_link, "mirror_status", real_mirror)
    route_loan_file(fid, rdb)  # a rerun sees it is routed and gives the file the status it already earned
    assert _file_status(rdb, fid)[0] == "routed" and len(partner_mail) == 1

    record_outcome(fid, action="funded", actor="josh", db=rdb, kind="loan_file")
    route_loan_file(fid, rdb)  # a later rerun must never pull a funded file back to "routed"
    assert _file_status(rdb, fid)[0] == "funded"


def test_the_partner_door_hands_a_file_to_the_router_when_it_is_installed(rdb, partner_mail):
    from src.lending import partner_pipeline

    _two_flip_lenders(rdb)
    fid = _make_partner_file(rdb, _make_partner(rdb))
    partner_pipeline.route_file(rdb, fid)  # the Partner Door's hand-off, not the router's own entry point
    assert _pfr(rdb, fid)["current_lender_name"] == "Alpha Capital"
    assert _file_status(rdb, fid)[0] == "routed"  # not "held, awaiting router"


def test_a_partner_file_has_its_own_record_beside_leads(rdb, partner_mail):
    from src.lending.routing.decline import record_decline
    from src.lending.routing.file_key import lead_key, loan_file_key
    from src.lending.routing.lender_service import NOT_FOUND, RoutingError, file_record
    from src.lending.routing.outcome import record_outcome
    from src.lending.routing.router import route_loan_file

    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="4101", last_name="Lead")
    _route(rdb, lead)
    fid = _make_partner_file(rdb, _make_partner(rdb, name="Maria Lopez"), last_name="Partnerfile")
    route_loan_file(fid, rdb)

    rec = file_record(loan_file_key(fid), rdb)
    assert rec["file"]["source"] == "partner" and rec["file"]["partner_name"] == "Maria Lopez" and rec["file"]["file_id"] == fid
    assert rec["file"]["loan_type"] == "FIX_AND_FLIP"  # the product, shown as the lender-box loan type
    assert rec["current"]["name"] == "Alpha Capital" and rec["next"]["name"] == "Beta Funding"
    assert [h["action"] for h in rec["history"]] == ["routed"]
    assert file_record(lead_key(lead), rdb)["file"]["source"] == "lendingflow"  # the same id as a lead is a different file

    routed_event = rdb.execute(text("SELECT id FROM lending.file_routing_events WHERE loan_file_id = :f AND action = 'routed'"),
                               {"f": fid}).scalar_one()
    record_decline(routed_event, reason_code="ltv_over_max", actor="josh", db=rdb)
    rec = file_record(loan_file_key(fid), rdb)
    assert rec["current"]["name"] == "Beta Funding"
    assert [h["action"] for h in rec["history"]] == ["routed", "declined", "routed"]
    key = loan_file_key(fid)
    record_outcome(key.id, action="funded", actor="josh", db=rdb, kind=key.kind)
    assert _file_status(rdb, fid)[0] == "funded"
    with pytest.raises(RoutingError) as missing:
        file_record(loan_file_key(999999), rdb)
    assert missing.value.kind == NOT_FOUND


def test_slack_posts_and_retries_say_partner_file_and_never_mix_ids_with_leads(rdb, fake_slack, partner_mail):
    from src.lending.routing.notify import retry_unposted_events
    from src.lending.routing.router import route_loan_file

    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="4201", last_name="Leadname")
    fid = _make_partner_file(rdb, _make_partner(rdb), last_name="Partnername")
    _route(rdb, lead)
    route_loan_file(fid, rdb)
    posted = "\n".join(fake_slack.sent)
    assert f"File #{lead} " in posted and f"Partner file #{fid} " in posted
    assert "Partnername" in posted and "Leadname" in posted

    rdb.execute(text("UPDATE lending.file_routing_events SET slack_ts = NULL, slack_posted_at = NULL"))
    rdb.commit()
    fake_slack.sent.clear()
    assert retry_unposted_events(rdb) == 2
    retried = "\n".join(fake_slack.sent)
    assert f"Partner file #{fid} " in retried and "position 1" in retried and "FIX_AND_FLIP" in retried


def test_a_file_has_exactly_one_key_and_the_event_log_stays_append_only(rdb):
    from sqlalchemy.exc import DatabaseError

    lender = _add_lender(rdb, name="Keyed", priority=1)
    lead = _make_lead(rdb, phone_suffix="4301")
    fid = _make_partner_file(rdb, _make_partner(rdb))
    insert = ("INSERT INTO lending.file_routing_events (lead_id, loan_file_id, lender_id, action)"
              " VALUES (:lead, :file, :lender, 'routed')")
    for lead_id, file_id in ((lead, fid), (None, None)):  # both keys, or neither
        with pytest.raises(DatabaseError):
            with rdb.begin_nested():
                rdb.execute(text(insert), {"lead": lead_id, "file": file_id, "lender": lender})

    rdb.execute(text(insert), {"lead": None, "file": fid, "lender": lender})
    with pytest.raises(DatabaseError, match="append-only"):
        with rdb.begin_nested():
            rdb.execute(text("UPDATE lending.file_routing_events SET loan_file_id = NULL, lead_id = :l"
                             " WHERE loan_file_id = :f"), {"l": lead, "f": fid})


# ── Code-review follow-ups ────────────────────────────────────────────────────

def test_ground_up_above_the_minimum_goes_to_overflow_after_the_primary_declines(rdb):
    """The decline cascade must not dead-end a large ground-up file at 'exhausted' while ground-up lenders remain."""
    from src.lending.routing.decline import record_decline

    primary = _add_lender(rdb, name="Big Primary", is_primary=True, priority=1, construction_min_amount=500_000)
    _add_box(rdb, primary, loan_types=[LOAN_TYPE_GU])
    overflow = _add_lender(rdb, name="GU Overflow", priority=2, takes_ground_up=True)
    _add_box(rdb, overflow, loan_types=[LOAN_TYPE_GU])
    lead = _make_lead(rdb, loan_type=LOAN_TYPE_GU, loan_amount=800_000, phone_suffix="5001")
    _route(rdb, lead)
    assert _fr(rdb, lead)["current_lender_name"] == "Big Primary"

    record_decline(_routed_event_id(rdb, lead), reason_code="ltv_over_max", actor="josh", db=rdb)
    assert _fr(rdb, lead)["current_lender_name"] == "GU Overflow"  # not "exhausted"


def test_the_ground_up_minimum_is_inclusive_at_exactly_500000(rdb):
    primary = _add_lender(rdb, name="Edge Primary", is_primary=True, priority=1, construction_min_amount=500_000)
    _add_box(rdb, primary, loan_types=[LOAN_TYPE_GU])
    overflow = _add_lender(rdb, name="Edge Overflow", priority=2, takes_ground_up=True)
    _add_box(rdb, overflow, loan_types=[LOAN_TYPE_GU])
    at_minimum = _make_lead(rdb, loan_type=LOAN_TYPE_GU, loan_amount=500_000, phone_suffix="5011")
    one_below = _make_lead(rdb, loan_type=LOAN_TYPE_GU, loan_amount=499_999, phone_suffix="5012")
    _route(rdb, at_minimum)
    _route(rdb, one_below)
    assert _fr(rdb, at_minimum)["current_lender_name"] == "Edge Primary"
    assert _fr(rdb, one_below)["current_lender_name"] == "Edge Overflow"


def test_the_seat_is_open_one_below_the_cap_and_closes_at_it(rdb):
    from src.lending.routing.outcome import record_outcome

    primary = _add_lender(rdb, name="Two Seats", is_primary=True, priority=1, monthly_cap=2)
    _add_box(rdb, primary, loan_types=[LOAN_TYPE_FLIP])
    overflow = _add_lender(rdb, name="Spill", priority=2)
    _add_box(rdb, overflow, loan_types=[LOAN_TYPE_FLIP])
    leads = [_make_lead(rdb, phone_suffix=f"502{i}") for i in range(3)]
    _route(rdb, leads[0])
    record_outcome(leads[0], action="funded", actor="josh", db=rdb)  # 1 funded of 2
    _route(rdb, leads[1])
    assert _fr(rdb, leads[1])["current_lender_name"] == "Two Seats"  # one below the cap: still open
    record_outcome(leads[1], action="funded", actor="josh", db=rdb)  # 2 funded of 2
    _route(rdb, leads[2])
    assert _fr(rdb, leads[2])["current_lender_name"] == "Spill"


def test_the_seat_count_resets_on_the_first_of_the_month_in_eastern_time_not_utc(rdb, monkeypatch):
    """23:59:59 on 31 October Eastern is already 1 November in UTC; the month (and DST edge) must follow Eastern."""
    from zoneinfo import ZoneInfo

    from src.lending.routing import seat

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 15, 12, 0, tzinfo=ZoneInfo("America/New_York")).astimezone(tz)

    monkeypatch.setattr(seat, "datetime", Frozen)
    primary = _add_lender(rdb, name="Monthly", is_primary=True, priority=1, monthly_cap=1)
    _add_box(rdb, primary, loan_types=[LOAN_TYPE_FLIP])
    overflow = _add_lender(rdb, name="Next Month Spill", priority=2)
    _add_box(rdb, overflow, loan_types=[LOAN_TYPE_FLIP])
    holder = _make_lead(rdb, phone_suffix="5031")

    def funded_at(moment: datetime) -> None:
        rdb.execute(
            text("INSERT INTO lending.file_routing_events (lead_id, lender_id, lender_name, action, created_at)"
                 " VALUES (:l, :lid, 'Monthly', 'funded', :ts)"),
            {"l": holder, "lid": primary, "ts": moment},
        )
        rdb.commit()

    # 3:59:59 UTC on 1 November is 23:59:59 EDT on 31 October: October, so it does not count in November.
    funded_at(datetime(2026, 11, 1, 3, 59, 59, tzinfo=timezone.utc))
    first = _make_lead(rdb, phone_suffix="5032")
    _route(rdb, first)
    assert _fr(rdb, first)["current_lender_name"] == "Monthly"

    # 4:00:00 UTC on 1 November is 00:00:00 EDT, the very first second of November: it counts.
    funded_at(datetime(2026, 11, 1, 4, 0, 0, tzinfo=timezone.utc))
    second = _make_lead(rdb, phone_suffix="5033")
    _route(rdb, second)
    assert _fr(rdb, second)["current_lender_name"] == "Next Month Spill"


def test_a_route_and_a_decline_post_to_slack_at_once_and_a_slack_failure_never_blocks_routing(
        rdb, fake_slack, monkeypatch):
    from src.lending.routing.decline import record_decline

    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="5201", last_name="Slackname")
    _route(rdb, lead)
    assert len(fake_slack.sent) == 1 and "Routed" in fake_slack.sent[0] and "Slackname" in fake_slack.sent[0]
    posted = rdb.execute(text("SELECT slack_posted_at FROM lending.file_routing_events WHERE lead_id = :l"),
                         {"l": lead}).scalar()
    assert posted is not None  # marked posted at the moment it was sent, not only by the retry

    record_decline(_routed_event_id(rdb, lead), reason_code="ltv_over_max", actor="josh", db=rdb)
    assert any("Declined" in m for m in fake_slack.sent) and len(fake_slack.sent) == 3  # route, decline, re-route

    class SlackDown:
        def chat_postMessage(self, **kwargs):  # noqa: N802 - slack_sdk API name
            raise RuntimeError("slack is down")

    monkeypatch.setattr("src.lending.routing.notify._client", lambda: SlackDown())
    other = _make_lead(rdb, phone_suffix="5202")
    _route(rdb, other)
    assert _fr(rdb, other)["status"] == "routed"  # the committed decision stands
    assert rdb.execute(text("SELECT slack_posted_at FROM lending.file_routing_events WHERE lead_id = :l"),
                       {"l": other}).scalar() is None  # left for the retry
    assert rdb.execute(text("SELECT 1")).scalar() == 1  # and the session is still usable


def _held_event_id(db: Session, key_column: str, key_id: int) -> int:
    return db.execute(text(f"SELECT id FROM lending.file_routing_events WHERE {key_column} = :i AND action = 'held'"
                           " ORDER BY id DESC LIMIT 1"), {"i": key_id}).scalar_one()


def test_a_held_file_is_routed_after_a_lender_is_signed_once_it_is_sent_back(rdb, buttons):
    lender = _add_lender(rdb, name="Signs Later", priority=1, agreement_status="pending")
    _add_box(rdb, lender, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="5301")
    _route(rdb, lead)
    assert _fr(rdb, lead)["hold_reason"] == "no_match"
    _route(rdb, lead)
    assert _fr(rdb, lead)["status"] == "held"  # a held file stays held on its own

    rdb.execute(text("UPDATE lending.lender_routing_rules SET agreement_status = 'signed' WHERE id = :i"), {"i": lender})
    rdb.commit()
    held_post = _held_event_id(rdb, "lead_id", lead)
    buttons.click("routing_reroute", held_post)
    assert _fr(rdb, lead)["current_lender_name"] == "Signs Later"
    assert "Sent back to the router" in buttons.calls["updated"][-1][0] and "now with *Signs Later*" in buttons.calls["updated"][-1][0]
    buttons.click("routing_reroute", held_post)  # a second click: it is no longer held
    assert "no longer in the hold queue" in buttons.calls["told"][-1][1]


def test_the_whole_no_match_backlog_is_reopened_and_the_sweep_routes_it(rdb, slack_form):
    from src.tasks.lending_routing_sweep import sweep

    lender = _add_lender(rdb, name="Backlog Lender", priority=1, agreement_status="pending")
    _add_box(rdb, lender, loan_types=[LOAN_TYPE_FLIP])
    leads = [_make_lead(rdb, phone_suffix=f"530{i}") for i in range(2, 5)]
    for lead in leads:
        _route(rdb, lead)
    rdb.execute(text("UPDATE lending.lender_routing_rules SET agreement_status = 'signed' WHERE id = :i"), {"i": lender})
    rdb.commit()

    assert slack_form(_form("reopen_held")) == {}
    assert "sent 3 held file(s) back" in slack_form.posted[-1]
    sweep(rdb)
    assert all(_fr(rdb, lead)["current_lender_name"] == "Backlog Lender" for lead in leads)


def test_a_requeued_exhausted_file_skips_lenders_that_already_declined_and_tries_a_new_one(rdb, buttons):
    from src.lending.routing.decline import record_decline

    a, b = _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="5401")
    _route(rdb, lead)
    record_decline(_routed_event_id(rdb, lead), reason_code="ltv_over_max", actor="josh", db=rdb)
    record_decline(_routed_event_id(rdb, lead), reason_code="ltv_over_max", actor="josh", db=rdb)
    assert _fr(rdb, lead)["status"] == "exhausted"

    new = _add_lender(rdb, name="Third Option", priority=3)
    _add_box(rdb, new, loan_types=[LOAN_TYPE_FLIP])
    buttons.click("routing_reroute", _held_event_id(rdb, "lead_id", lead))
    assert _fr(rdb, lead)["current_lender_name"] == "Third Option"  # never Alpha or Beta again


def test_a_held_partner_file_is_sent_back_the_same_way_and_the_partner_hears_of_it(rdb, buttons, partner_mail):
    from src.lending.routing.router import route_loan_file

    lender = _add_lender(rdb, name="Partner Signs Later", priority=1, agreement_status="pending")
    _add_box(rdb, lender, loan_types=[LOAN_TYPE_FLIP])
    fid = _make_partner_file(rdb, _make_partner(rdb))
    route_loan_file(fid, rdb)
    assert _pfr(rdb, fid)["status"] == "held" and partner_mail == []

    rdb.execute(text("UPDATE lending.lender_routing_rules SET agreement_status = 'signed' WHERE id = :i"), {"i": lender})
    rdb.commit()
    buttons.click("routing_reroute", _held_event_id(rdb, "loan_file_id", fid))
    assert _pfr(rdb, fid)["current_lender_name"] == "Partner Signs Later"
    assert _file_status(rdb, fid)[0] == "routed" and len(partner_mail) == 1


def test_a_file_that_keeps_failing_is_moved_back_so_healthy_files_still_get_routed(rdb, monkeypatch):
    import config.lending_routing as routing_config
    from src.lending.routing import router
    from src.tasks.lending_routing_sweep import sweep

    _two_flip_lenders(rdb)
    poison = _make_lead(rdb, phone_suffix="5601")
    healthy = _make_lead(rdb, phone_suffix="5602")
    for lead in (poison, healthy):
        rdb.execute(text("INSERT INTO lending.file_routing (lead_id, status) VALUES (:l, 'unrouted')"), {"l": lead})
        rdb.commit()
    real_route = router.route_file

    def flaky(lead_id, db):
        if lead_id == poison:
            raise RuntimeError("cannot route this one")
        return real_route(lead_id, db)

    monkeypatch.setattr(router, "route_file", flaky)
    monkeypatch.setattr(routing_config, "SWEEP_BATCH_SIZE", 1)
    sweep(rdb)  # the oldest file is the poisoned one: it fails and goes to the back
    assert _fr(rdb, poison)["status"] == "unrouted" and _fr(rdb, healthy)["status"] == "unrouted"
    sweep(rdb)  # the healthy file is now first
    assert _fr(rdb, healthy)["status"] == "routed"


def test_slack_retry_still_works_when_the_partner_door_table_is_not_installed(rdb, fake_slack):
    from src.lending.routing.notify import retry_unposted_events

    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="5701", last_name="Retry")
    _route(rdb, lead)
    rdb.execute(text("UPDATE lending.file_routing_events SET slack_ts = NULL, slack_posted_at = NULL"))
    rdb.execute(text("DROP TABLE lending.loan_files CASCADE"))  # inside the test's rolled-back transaction
    fake_slack.sent.clear()
    assert retry_unposted_events(rdb) == 1 and "Routed" in fake_slack.sent[0]


def test_the_real_partner_status_sync_ignores_leads_and_never_raises(rdb, monkeypatch, caplog):
    from src.lending import partner_pipeline
    from src.lending.routing import partner_link
    from src.lending.routing.file_key import lead_key, loan_file_key

    calls: list = []
    monkeypatch.setattr(partner_pipeline, "change_status", lambda *args, **kwargs: calls.append(args))
    partner_link.mirror_status(lead_key(5), "routed")
    assert calls == []  # a LendingFlow lead has no partner
    partner_link.mirror_status(loan_file_key(5), "routed", None)
    assert calls == [(5, "routed", None)]

    def down(*args, **kwargs):
        raise RuntimeError("database down")

    monkeypatch.setattr(partner_pipeline, "change_status", down)
    partner_link.mirror_status(loan_file_key(5), "declined", "ltv_over_max")  # must not raise
    assert "partner status sync failed" in caplog.text and "ltv_over_max" not in caplog.text


def test_inputs_that_would_overflow_or_mean_nothing_are_refused_before_the_database(rdb):
    from pydantic import ValidationError

    from src.lending.routing.lender_service import LenderIn, ProductBoxIn, ReorderIn

    for body in (
        {"name": "x", "priority": 1, "points_paid_to_broker": 10},
        {"name": "x", "priority": 0},
        {"name": "x", "priority": 1_000_000},
        {"name": "   ", "priority": 1},
        {"name": "y" * 101, "priority": 1},
        {"name": "x", "priority": 1, "monthly_cap": -1},
    ):
        with pytest.raises(ValidationError):
            LenderIn(**body)
    for box in (
        {"loan_types": ["FIX_AND_FLIP"], "max_ltv": 10},
        {"loan_types": ["FIX_AND_FLIP"], "min_loan_amount": 9, "max_loan_amount": 1},
        {"loan_types": ["FIX_AND_FLIP"], "approved_states": ["Florida"]},
        {"loan_types": ["FIX_AND_FLIP"], "credit_floor": 901},
    ):
        with pytest.raises(ValidationError):
            ProductBoxIn(**box)
    assert ProductBoxIn(loan_types=["fix_and_flip"], approved_states=["fl", "ga"]).approved_states == ["FL", "GA"]
    with pytest.raises(ValidationError):
        ReorderIn(ordered_ids=[99999999999])  # outside the id range: not a database error later


def test_make_primary_refuses_a_paused_lender_and_conflicts_say_which_rule_was_broken(rdb):
    from sqlalchemy.exc import IntegrityError

    from src.lending.routing.lender_service import CONFLICT, RoutingError, _conflict, make_primary

    paused = _add_lender(rdb, name="Paused One", priority=1, active=False)
    with pytest.raises(RoutingError, match="Resume the lender"):
        make_primary(paused, rdb)

    class _Diag:
        def __init__(self, name):
            self.constraint_name = name

    class _Orig(Exception):
        def __init__(self, name):
            super().__init__("clash")
            self.diag = _Diag(name)

    primary_clash = _conflict(IntegrityError("stmt", {}, _Orig("uq_lender_routing_rules_primary")))
    priority_clash = _conflict(IntegrityError("stmt", {}, _Orig("uq_lender_routing_rules_priority")))
    assert primary_clash.kind == CONFLICT and "primary" in str(primary_clash)
    assert priority_clash.kind == CONFLICT and "priority" in str(priority_clash)


def test_refusals_do_not_expose_table_or_column_names(rdb):
    from src.lending.routing.decline import record_decline
    from src.lending.routing.file_key import lead_key
    from src.lending.routing.lender_service import RoutingError, file_record
    from src.lending.routing.outcome import record_outcome

    for call in (
        lambda: record_decline(999999, reason_code="ltv_over_max", actor="josh", db=rdb),
        lambda: record_outcome(999999, action="funded", actor="josh", db=rdb),
        lambda: file_record(lead_key(999999), rdb),
    ):
        with pytest.raises((ValueError, RoutingError)) as refused:
            call()
        assert not any(word in str(refused.value) for word in ("file_routing", "lead_id", "loan_file_id", "lendingflow", "lending."))


def test_the_migration_retires_untouched_placeholder_reasons_but_never_one_someone_relabelled(rdb):
    from migrations.apply_lending_lender_routing import apply_to

    conn = rdb.get_bind()
    conn.execute(text(
        "INSERT INTO lending.decline_reason_codes (code, label, active) VALUES"
        " ('unspecified', 'Not specified', TRUE), ('credit', 'Our own credit wording', TRUE),"
        " ('appraisal_low', 'added by Josh later', TRUE)"
        " ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label, active = TRUE"))
    apply_to(conn)
    state = {r["code"]: r["active"] for r in rdb.execute(text(
        "SELECT code, active FROM lending.decline_reason_codes"
        " WHERE code IN ('unspecified', 'credit', 'appraisal_low', 'other')")).mappings()}
    assert state == {"unspecified": False, "credit": True, "appraisal_low": True, "other": True}
    apply_to(conn)  # and a second run changes nothing
    assert {r["code"]: r["active"] for r in rdb.execute(text(
        "SELECT code, active FROM lending.decline_reason_codes"
        " WHERE code IN ('unspecified', 'credit', 'appraisal_low', 'other')")).mappings()} == state


def test_reseeding_never_adds_a_duplicate_of_a_renamed_lender(rdb):
    from migrations.apply_lending_lender_routing_seed import apply_to as seed

    conn = rdb.get_bind()
    assert seed(conn) == 5
    rdb.execute(text("UPDATE lending.lender_routing_rules SET name = 'Renamed Primary' WHERE is_primary"))
    rdb.commit()
    assert seed(conn) == 0
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_routing_rules")).scalar() == 5


# ── Slack lender form (R1.4: edit the rules from Slack, not a web screen) ─────

def _form(action: str, *, lender: Optional[int] = None, value: str = "", types=(), states: str = "",
          min_amount: str = "", max_amount: str = "", credit: str = "") -> dict:
    def txt(raw):
        return {"v": {"type": "plain_text_input", "value": raw}}

    values = {
        "action_b": {"v": {"type": "static_select", "selected_option": {"value": action}}},
        "value_b": txt(value), "states_b": txt(states), "min_b": txt(min_amount), "max_b": txt(max_amount),
        "credit_b": txt(credit),
        "types_b": {"v": {"type": "multi_static_select", "selected_options": [{"value": t} for t in types]}},
    }
    if lender is not None:
        values["lender_b"] = {"v": {"type": "static_select", "selected_option": {"value": str(lender)}}}
    return values


@pytest.fixture
def slack_form(rdb, monkeypatch):
    """The real submission handler on the test session, with Slack captured and Josh as the only editor."""
    from contextlib import contextmanager

    from src.lending.routing import slack_admin

    posted: list[str] = []

    class Client:
        def chat_postMessage(self, channel, text, blocks=None):  # noqa: N802 - slack_sdk API name
            posted.append(text)
            return {"ts": f"{len(posted)}.0"}

    @contextmanager
    def session():
        yield rdb

    monkeypatch.setattr(slack_admin, "lending_session", session)
    monkeypatch.setattr(slack_admin.notify, "_client", lambda: Client())
    monkeypatch.setattr(slack_admin.notify, "_channel", lambda: "C-TEST")
    monkeypatch.setattr(slack_admin, "allowed_user_ids", lambda: frozenset({"UJOSH"}))

    def submit(values, user="UJOSH"):
        payload = {"type": "view_submission", "user": {"id": user},
                   "view": {"callback_id": slack_admin.CALLBACK_ID, "state": {"values": values}}}
        return slack_admin.handle_submission(payload)

    submit.posted = posted
    return submit


def test_slack_form_adds_a_lender_with_a_box_and_it_then_receives_a_matching_file(rdb, slack_form):
    slack_form(_form("add_lender", value="Form Lender", types=[LOAN_TYPE_FLIP], states="fl"))
    row = rdb.execute(text("SELECT id, agreement_status, priority FROM lending.lender_routing_rules"
                           " WHERE name = 'Form Lender'")).one()
    assert row.agreement_status == "pending"  # safe default: nothing routes until Josh signs it
    assert rdb.execute(text("SELECT approved_states FROM lending.lender_product_boxes WHERE lender_id = :i"),
                       {"i": row.id}).scalar() == ["FL"]
    lead = _make_lead(rdb, phone_suffix="6101")
    _route(rdb, lead)
    assert _fr(rdb, lead)["status"] == "held"  # unsigned is invisible
    assert slack_form(_form("agreement", lender=row.id, value="signed")) == {}
    rdb.execute(text("UPDATE lending.file_routing SET status = 'unrouted' WHERE lead_id = :l"), {"l": lead})
    rdb.commit()
    _route(rdb, lead)
    assert _fr(rdb, lead)["current_lender_name"] == "Form Lender"
    changes = [m for m in slack_form.posted if "Lender rules changed" in m]
    assert len(changes) == 2 and all("<@UJOSH>" in m for m in changes)


def test_slack_form_pause_resume_cap_reorder_and_primary(rdb, slack_form):
    a, b = _two_flip_lenders(rdb)
    assert slack_form(_form("pause", lender=a)) == {}
    assert rdb.execute(text("SELECT active FROM lending.lender_routing_rules WHERE id = :i"), {"i": a}).scalar() is False
    slack_form(_form("resume", lender=a))
    slack_form(_form("seat_cap", lender=a, value="3"))
    assert rdb.execute(text("SELECT monthly_cap FROM lending.lender_routing_rules WHERE id = :i"), {"i": a}).scalar() == 3
    slack_form(_form("seat_cap", lender=a, value="none"))
    assert rdb.execute(text("SELECT monthly_cap FROM lending.lender_routing_rules WHERE id = :i"), {"i": a}).scalar() is None
    slack_form(_form("move_down", lender=a))
    order = [r[0] for r in rdb.execute(text("SELECT id FROM lending.lender_routing_rules ORDER BY priority"))]
    assert order == [b, a]
    slack_form(_form("make_primary", lender=b))
    assert rdb.execute(text("SELECT is_primary FROM lending.lender_routing_rules WHERE id = :i"), {"i": b}).scalar() is True


def test_slack_form_refuses_strangers_and_bad_input_and_changes_nothing(rdb, slack_form):
    a, _ = _two_flip_lenders(rdb)
    stranger = slack_form(_form("pause", lender=a), user="USTRANGER")
    assert stranger["response_action"] == "errors" and slack_form.posted == []
    assert rdb.execute(text("SELECT active FROM lending.lender_routing_rules WHERE id = :i"), {"i": a}).scalar() is True
    # Each problem is shown under the field it is about, in plain words (never an internal field name).
    for values, block in (
        (_form("agreement", lender=a, value="maybe"), "value_b"),
        (_form("seat_cap", lender=a, value="lots"), "value_b"),
        (_form("add_lender", value=""), "value_b"),
        (_form("add_lender", value="No Box Types", types=[LOAN_TYPE_FLIP], min_amount="900", max_amount="100"), "max_b"),
        (_form("add_lender", value="No Box Types", types=[LOAN_TYPE_FLIP], min_amount="abc"), "min_b"),
        (_form("add_lender", value="No Box Types", types=[LOAN_TYPE_FLIP], states="Florida"), "states_b"),
        (_form("add_lender", value="No Box Types", types=[LOAN_TYPE_FLIP], credit="950"), "credit_b"),
        (_form("add_box", lender=a), "types_b"),
        (_form("move_up", lender=a), "lender_b"),  # already first
        (_form("pause"), "lender_b"),  # no lender chosen
    ):
        response = slack_form(values)
        assert response["response_action"] == "errors" and list(response["errors"]) == [block], (block, response)
        assert "_amount" not in response["errors"][block] and "loan_types" not in response["errors"][block], response
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_routing_rules WHERE name = 'No Box Types'")).scalar() == 0
    assert slack_form.posted == []  # nothing applied, nothing announced


def test_the_slash_command_opens_the_form_for_an_allowed_user_only(rdb, monkeypatch):
    from src.lending.routing import slack_admin
    from src.lending.routing.slack_extension import RoutingSlack

    monkeypatch.setattr(slack_admin, "allowed_user_ids", lambda: frozenset({"UJOSH"}))

    class Client:
        def __init__(self):
            self.opened, self.told = [], []

        def views_open(self, trigger_id, view):
            self.opened.append((trigger_id, view["callback_id"]))

        def chat_postEphemeral(self, channel, user, text):  # noqa: N802 - slack_sdk API name
            self.told.append((user, text))

    from contextlib import contextmanager

    @contextmanager
    def session():
        yield rdb

    monkeypatch.setattr(slack_admin, "lending_session", session)
    client = Client()
    extension = RoutingSlack(client)
    assert extension.handle("slash_commands", {"command": "/lenders", "user_id": "UJOSH", "trigger_id": "T1"}) is True
    assert client.opened == [("T1", slack_admin.CALLBACK_ID)]
    assert extension.handle("slash_commands", {"command": "/lenders", "user_id": "UOTHER", "trigger_id": "T2",
                                               "channel_id": "C1"}) is True
    assert client.opened == [("T1", slack_admin.CALLBACK_ID)] and "Only approved users" in client.told[0][1]
    assert extension.handle("slash_commands", {"command": "/something-else"}) is False  # not ours: left alone
    assert extension.respond("events_api", {}) is None


def test_only_coras_approvers_may_edit_lenders_and_act_on_files(monkeypatch):
    from config.settings import get_settings
    from src.lending.routing import slack_admin

    settings = get_settings()
    monkeypatch.setattr(settings, "lending_cora_approver_user_ids", "UJOSH, UOTHER", raising=False)
    assert slack_admin.allowed_user_ids() == frozenset({"UJOSH", "UOTHER"})
    monkeypatch.setattr(settings, "lending_cora_approver_user_ids", "", raising=False)
    assert slack_admin.allowed_user_ids() == frozenset()  # nobody configured: nobody allowed
    assert not hasattr(settings, "lending_routing_editor_user_ids")  # there is no second list to fall back to


def test_routing_posts_use_only_coras_bot_and_channel(monkeypatch):
    from pydantic import SecretStr

    from config.settings import get_settings
    from src.lending.routing import notify

    settings = get_settings()
    assert not hasattr(settings, "lending_routing_channel")  # there is no other routing channel
    monkeypatch.setattr(settings, "lending_cora_channel", "C_CORA", raising=False)
    monkeypatch.setattr(settings, "lending_cora_slack_bot_token", SecretStr("xoxb-cora"), raising=False)
    monkeypatch.setattr(settings, "lending_slack_bot_token", SecretStr("xoxb-lending"), raising=False)
    assert notify._channel() == "C_CORA" and notify._client().token == "xoxb-cora"
    monkeypatch.setattr(settings, "lending_cora_slack_bot_token", None, raising=False)
    assert notify._client() is None  # the lending bot is never a fallback


def test_the_lender_form_lists_the_waterfall_and_nobody_may_edit_when_no_editor_is_configured(rdb):
    from src.lending.routing import slack_admin

    _two_flip_lenders(rdb)
    view = slack_admin.build_form_view(slack_admin._lenders(rdb))
    assert view["callback_id"] == slack_admin.CALLBACK_ID
    assert "Alpha Capital" in view["blocks"][0]["text"]["text"] and "Beta Funding" in view["blocks"][0]["text"]["text"]
    assert slack_admin.allowed_user_ids() == frozenset()  # nobody configured: fail closed (blanked by the autouse fixture)


# ── Slack buttons on a routed post (Decline needs a reason; Approved; Funded) ─

def test_only_a_routed_post_carries_the_buttons(rdb, fake_slack):
    from src.lending.routing.decline import record_decline

    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="6201")
    _route(rdb, lead)
    actions = [b for b in fake_slack.blocks[0] if b["type"] == "actions"][0]["elements"]
    assert [e["action_id"] for e in actions] == ["routing_decline", "routing_approved", "routing_funded", "routing_details"]
    assert {e["value"] for e in actions} == {str(_routed_event_id(rdb, lead))}
    assert "confirm" in actions[2]  # funded asks "are you sure" first
    record_decline(_routed_event_id(rdb, lead), reason_code="ltv_over_max", actor="josh", db=rdb)
    assert [bool(b) for b in fake_slack.blocks] == [True, True, True]  # routed, declined, re-routed: each has Details
    declined_buttons = [b for b in fake_slack.blocks[1] if b["type"] == "actions"][0]["elements"]
    assert [e["action_id"] for e in declined_buttons] == ["routing_details"]  # a decline post only offers Details


@pytest.fixture
def buttons(rdb, monkeypatch):
    """The real button and form handlers on the test session, Josh the only allowed user, Slack calls recorded."""
    from contextlib import contextmanager

    from src.lending.routing import slack_actions
    from src.lending.routing.slack_extension import RoutingSlack

    calls: dict[str, list] = {"opened": [], "updated": [], "told": []}

    class Client:
        def views_open(self, trigger_id, view):
            calls["opened"].append(view)

        def chat_update(self, channel, ts, text, blocks):
            calls["updated"].append((text, blocks))

        def chat_postEphemeral(self, channel, user, text):  # noqa: N802 - slack_sdk API name
            calls["told"].append((user, text))

    @contextmanager
    def session():
        yield rdb

    monkeypatch.setattr(slack_actions, "lending_session", session)
    monkeypatch.setattr(slack_actions, "allowed_user_ids", lambda: frozenset({"UJOSH"}))
    extension = RoutingSlack(Client(), run_later=lambda fn, *args: fn(*args))

    class Env:
        pass

    env = Env()
    env.calls = calls

    def click(action_id, event_id, user="UJOSH"):
        assert extension.handle("interactive", {
            "type": "block_actions", "user": {"id": user}, "trigger_id": "T1", "channel": {"id": "C1"},
            "message": {"ts": "1.0", "text": "Routed | File", "blocks": [
                {"type": "section"}, {"type": "actions", "elements": [
                    {"action_id": "routing_decline"}, {"action_id": "routing_approved"}, {"action_id": "routing_funded"},
                    {"action_id": "routing_details"}]}]},
            "actions": [{"action_id": action_id, "value": str(event_id)}]}) is True

    def submit(event_id, reason="", details="", user="UJOSH"):
        values = {"reason_b": {"v": {"type": "static_select", "selected_option": {"value": reason} if reason else None}},
                  "text_b": {"v": {"type": "plain_text_input", "value": details}}}
        payload = {"type": "view_submission", "user": {"id": user}, "view": {
            "callback_id": slack_actions.DECLINE_CALLBACK_ID, "state": {"values": values},
            "private_metadata": json.dumps({"event_id": event_id, "channel": "C1", "ts": "1.0", "original": "Routed | File"})}}
        response = extension.respond("interactive", payload) or {}
        assert extension.handle("interactive", payload) is True  # consumed, not handled twice
        return response

    env.click, env.submit = click, submit
    return env


def test_decline_button_opens_a_form_with_josh_reasons_and_a_stranger_is_refused(rdb, buttons, fake_slack):
    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="6301")
    _route(rdb, lead)
    event = _routed_event_id(rdb, lead)
    buttons.click("routing_decline", event, user="USTRANGER")
    assert buttons.calls["opened"] == [] and "Only approved users" in buttons.calls["told"][0][1]
    buttons.click("routing_decline", event)
    view = buttons.calls["opened"][0]
    reasons = [o["value"] for o in view["blocks"][1]["element"]["options"]]
    assert "ltv_over_max" in reasons and view["blocks"][1]["optional"] is False  # a reason is mandatory


def test_a_decline_from_slack_needs_a_reason_and_other_needs_details(rdb, buttons, fake_slack):
    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="6401")
    _route(rdb, lead)
    event = _routed_event_id(rdb, lead)
    assert buttons.submit(event)["errors"]["reason_b"]
    assert buttons.submit(event, reason="other")["errors"]["text_b"]
    assert buttons.submit(event, reason="ltv_over_max", user="USTRANGER")["response_action"] == "errors"
    assert _fr(rdb, lead)["current_lender_name"] == "Alpha Capital"  # nothing happened yet

    assert buttons.submit(event, reason="other", details="Lender paused new loans") == {}
    declined = next(e for e in _events(rdb, lead) if e["action"] == "declined")
    assert declined["reason_code"] == "other"
    assert rdb.execute(text("SELECT free_text, actor FROM lending.file_routing_events WHERE action = 'declined'"
                            " AND lead_id = :l"), {"l": lead}).one() == ("Lender paused new loans", "slack:UJOSH")
    assert _fr(rdb, lead)["current_lender_name"] == "Beta Funding"  # re-routed to the next lender
    assert "Declined" in buttons.calls["updated"][-1][0]  # the old post no longer offers buttons


def test_an_old_post_cannot_act_on_a_later_lender(rdb, buttons, fake_slack):
    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="6501")
    _route(rdb, lead)
    first = _routed_event_id(rdb, lead)
    assert buttons.submit(first, reason="ltv_over_max") == {}  # declined; the file is now with Beta
    assert "no longer" in buttons.submit(first, reason="ltv_over_max")["errors"]["reason_b"]
    buttons.click("routing_funded", first)
    assert "no longer" in buttons.calls["told"][-1][1]
    assert [e["action"] for e in _events(rdb, lead)] == ["routed", "declined", "routed"]  # no funded on Beta


def test_approved_and_funded_buttons_record_the_outcome_and_funded_uses_a_seat(rdb, buttons, fake_slack):
    primary = _add_lender(rdb, name="One Seat", is_primary=True, priority=1, monthly_cap=1)
    _add_box(rdb, primary, loan_types=[LOAN_TYPE_FLIP])
    spill = _add_lender(rdb, name="Spill Over", priority=2)
    _add_box(rdb, spill, loan_types=[LOAN_TYPE_FLIP])
    first, second = _make_lead(rdb, phone_suffix="6601"), _make_lead(rdb, phone_suffix="6602")
    _route(rdb, first)
    event = _routed_event_id(rdb, first)
    buttons.click("routing_approved", event)
    assert [e["action"] for e in _events(rdb, first)] == ["routed", "approved"]
    kept = buttons.calls["updated"][-1][1][-1]
    assert kept["type"] == "actions"  # the file can still fund or decline
    assert [e["action_id"] for e in kept["elements"]] == ["routing_decline", "routing_funded", "routing_details"]  # Approved is done
    buttons.click("routing_funded", event)
    assert [e["action"] for e in _events(rdb, first)] == ["routed", "approved", "funded"]
    closed = [b for b in buttons.calls["updated"][-1][1] if b["type"] == "actions"]
    assert [e["action_id"] for e in closed[0]["elements"]] == ["routing_details"]  # funded closes the post but keeps Details
    assert rdb.execute(text("SELECT actor FROM lending.file_routing_events WHERE action = 'funded'")).scalar() == "slack:UJOSH"
    buttons.click("routing_funded", event)  # a second click records nothing more
    assert [e["action"] for e in _events(rdb, first)].count("funded") == 1
    _route(rdb, second)
    assert _fr(rdb, second)["current_lender_name"] == "Spill Over"  # the seat is used


def test_clicks_that_are_not_routings_are_left_to_cora(rdb):
    from src.lending.routing.slack_extension import RoutingSlack

    extension = RoutingSlack(object())
    approval = {"type": "block_actions", "actions": [{"action_id": "agent_core_approve", "value": "7"}]}
    assert extension.handle("interactive", approval) is False  # Cora's own approval card
    assert extension.respond("interactive", approval) is None
    assert extension.handle("events_api", {"event": {"type": "message"}}) is False


def test_slack_form_reports_every_problem_in_one_pass_each_under_its_own_field(rdb, slack_form):
    a, _ = _two_flip_lenders(rdb)
    # Four mistakes at once: no name, box fields filled with no loan type, a min loan that is not a number, a bad max.
    response = slack_form(_form("add_lender", value="", states="FL", min_amount="abc", max_amount="-5"))
    assert set(response["errors"]) == {"value_b", "types_b", "min_b", "max_b"}, response
    # With a loan type: bad number, bad states and bad credit are all shown together.
    response = slack_form(_form("add_lender", value="Many Mistakes", types=[LOAN_TYPE_FLIP],
                                min_amount="abc", states="Florida", credit="950"))
    assert set(response["errors"]) == {"min_b", "states_b", "credit_b"}, response
    # No lender and a bad value together.
    response = slack_form(_form("agreement", value="maybe"))
    assert set(response["errors"]) == {"lender_b", "value_b"}, response
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_routing_rules WHERE name = 'Many Mistakes'")).scalar() == 0
    assert slack_form.posted == []  # nothing applied, nothing announced
    # Fixing them all in one go works.
    assert slack_form(_form("add_lender", value="Fixed Lender", types=[LOAN_TYPE_FLIP], min_amount="100000",
                            max_amount="900000", states="fl, ga", credit="680")) == {}
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_routing_rules WHERE name = 'Fixed Lender'")).scalar() == 1


def test_a_file_can_still_be_declined_after_it_was_approved_but_not_after_it_funded(rdb, buttons, fake_slack):
    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="6701")
    _route(rdb, lead)
    event = _routed_event_id(rdb, lead)
    buttons.click("routing_approved", event)
    assert buttons.submit(event, reason="ltv_over_max") == {}  # the lender approved, then changed its mind
    assert [e["action"] for e in _events(rdb, lead)] == ["routed", "approved", "declined", "routed"]
    assert _fr(rdb, lead)["current_lender_name"] == "Beta Funding"

    second = _make_lead(rdb, phone_suffix="6702")
    _route(rdb, second)
    funded_event = _routed_event_id(rdb, second)
    buttons.click("routing_funded", funded_event)
    assert "no longer" in buttons.submit(funded_event, reason="ltv_over_max")["errors"]["reason_b"]  # funded is final
    assert [e["action"] for e in _events(rdb, second)] == ["routed", "funded"]


# ── Details button, remove a lender, boxes and ground-up settings from Slack ──

def test_details_button_shows_current_next_and_the_route_history_privately(rdb, buttons, fake_slack):
    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="6801", last_name="Detailname")
    _route(rdb, lead)
    event = _routed_event_id(rdb, lead)
    buttons.click("routing_decline", event)  # opens the form, changes nothing
    assert buttons.submit(event, reason="ltv_over_max", details="") == {}
    buttons.calls["told"].clear()
    buttons.click("routing_details", event, user="UJOSH")
    shown = buttons.calls["told"][-1][1]
    assert "Detailname" in shown and "status: routed" in shown
    assert "Current lender: *Beta Funding* (position 2)" in shown and "Next in line: none" in shown
    assert shown.count("routed") >= 3 and "declined" in shown and "ltv_over_max" in shown and "slack:UJOSH" in shown
    buttons.click("routing_details", event, user="USTRANGER")
    assert "Only approved users" in buttons.calls["told"][-1][1]


def test_slack_form_removes_a_lender_only_with_its_name_typed_and_never_one_that_routed(rdb, slack_form):
    a, b = _two_flip_lenders(rdb)
    spare = _add_lender(rdb, name="Spare Lender", priority=3)
    assert "Spare Lender" in slack_form(_form("remove", lender=spare, value="wrong name"))["errors"]["value_b"]
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_routing_rules WHERE id = :i"), {"i": spare}).scalar() == 1
    assert slack_form(_form("remove", lender=spare, value="spare lender")) == {}
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_routing_rules WHERE id = :i"), {"i": spare}).scalar() == 0
    assert "removed lender" in slack_form.posted[-1]

    lead = _make_lead(rdb, phone_suffix="6802")
    _route(rdb, lead)  # Alpha now has routing history
    blocked = slack_form(_form("remove", lender=a, value="Alpha Capital"))
    assert "routing history" in blocked["errors"]["lender_b"] and "Pause" in blocked["errors"]["lender_b"]
    rdb.execute(text("UPDATE lending.lender_routing_rules SET is_primary = TRUE WHERE id = :i"), {"i": b})
    rdb.commit()
    assert "primary" in slack_form(_form("remove", lender=b, value="Beta Funding"))["errors"]["lender_b"]


def test_slack_form_replaces_and_clears_boxes_and_sets_ground_up_settings(rdb, slack_form):
    a, _ = _two_flip_lenders(rdb)
    boxes = lambda: rdb.execute(text("SELECT loan_types, approved_states FROM lending.lender_product_boxes"  # noqa: E731
                                     " WHERE lender_id = :i"), {"i": a}).all()
    assert len(boxes()) == 1
    slack_form(_form("replace_boxes", lender=a, types=[LOAN_TYPE_GU], states="tx"))
    assert [(list(t), st) for t, st in boxes()] == [([LOAN_TYPE_GU], ["TX"])]  # the old flip box is gone
    # A refused replacement keeps what was there.
    refused = slack_form(_form("replace_boxes", lender=a, types=[LOAN_TYPE_FLIP], min_amount="900", max_amount="100"))
    assert "max_b" in refused["errors"] and len(boxes()) == 1 and boxes()[0][0] == [LOAN_TYPE_GU]
    assert "types_b" in slack_form(_form("replace_boxes", lender=a))["errors"]
    slack_form(_form("clear_boxes", lender=a))
    assert boxes() == []

    slack_form(_form("ground_up_min", lender=a, value="$750,000"))
    slack_form(_form("takes_ground_up", lender=a, value="yes"))
    row = rdb.execute(text("SELECT construction_min_amount, takes_ground_up FROM lending.lender_routing_rules"
                           " WHERE id = :i"), {"i": a}).one()
    assert tuple(row) == (750000, True)
    slack_form(_form("ground_up_min", lender=a, value="none"))
    assert rdb.execute(text("SELECT construction_min_amount FROM lending.lender_routing_rules WHERE id = :i"),
                       {"i": a}).scalar() is None
    for bad in (_form("ground_up_min", lender=a, value="lots"), _form("takes_ground_up", lender=a, value="maybe")):
        assert list(slack_form(bad)["errors"]) == ["value_b"]


def test_the_form_lists_each_lenders_boxes_and_ground_up_settings(rdb):
    from src.lending.routing import slack_admin

    a, _ = _two_flip_lenders(rdb)
    rdb.execute(text("UPDATE lending.lender_routing_rules SET construction_min_amount = 500000, takes_ground_up = TRUE,"
                     " monthly_cap = 4 WHERE id = :i"), {"i": a})
    rdb.execute(text("UPDATE lending.lender_product_boxes SET approved_states = ARRAY['FL','GA'], min_loan_amount = 100000,"
                     " max_loan_amount = 900000, credit_floor = 680 WHERE lender_id = :i"), {"i": a})
    rdb.commit()
    listing = slack_admin.build_form_view(slack_admin._lenders(rdb), slack_admin._boxes_by_lender(rdb))["blocks"][0]["text"]["text"]
    assert "cap 4" in listing and "ground-up min $500,000" in listing and "takes ground-up" in listing
    assert "Fix And Flip · FL, GA · $100,000-$900,000 · min credit 680" in listing


def test_a_lender_that_never_routed_can_be_deleted_and_the_rest_are_refused(rdb):
    from src.lending.routing.lender_service import CONFLICT, NOT_FOUND, RoutingError, delete_lender

    a, _ = _two_flip_lenders(rdb)
    spare = _add_lender(rdb, name="Spare", priority=3)
    _add_box(rdb, spare, loan_types=[LOAN_TYPE_FLIP])
    delete_lender(spare, rdb)
    assert rdb.execute(text("SELECT COUNT(*) FROM lending.lender_product_boxes WHERE lender_id = :i"), {"i": spare}).scalar() == 0
    with pytest.raises(RoutingError) as gone:
        delete_lender(spare, rdb)
    assert gone.value.kind == NOT_FOUND
    _route(rdb, _make_lead(rdb, phone_suffix="6803"))
    with pytest.raises(RoutingError) as used:
        delete_lender(a, rdb)  # it routed a file
    assert used.value.kind == CONFLICT

def test_a_held_post_offers_re_route_and_details(rdb, fake_slack):
    lead = _make_lead(rdb, phone_suffix="6901")
    _route(rdb, lead)  # no lenders: the file is held
    held = [b for b in fake_slack.blocks[-1] if b["type"] == "actions"][0]["elements"]
    assert [e["action_id"] for e in held] == ["routing_reroute", "routing_details"]


def test_reroute_is_refused_for_a_post_that_is_not_a_held_file(rdb, buttons):
    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="6902")
    _route(rdb, lead)
    buttons.click("routing_reroute", _routed_event_id(rdb, lead))  # a Routed post, not a Held one
    assert "not a held file" in buttons.calls["told"][-1][1]
    assert _fr(rdb, lead)["status"] == "routed"


def test_slack_form_adds_a_decline_reason_that_the_decline_form_then_offers(rdb, slack_form, buttons):
    def label_form(code, label):
        values = _form("add_reason", value=code)
        values["label_b"] = {"v": {"type": "plain_text_input", "value": label}}
        return values

    assert slack_form(label_form("title_issue_found", "Title issue found")) == {}
    assert "added decline reason *title_issue_found*" in slack_form.posted[-1]
    assert rdb.execute(text("SELECT label, active FROM lending.decline_reason_codes WHERE code = 'title_issue_found'")).one() == (
        "Title issue found", True)
    slack_form(label_form("title_issue_found", "Title problem found"))  # the same code again changes the wording
    assert "updated decline reason" in slack_form.posted[-1]
    assert rdb.execute(text("SELECT label FROM lending.decline_reason_codes WHERE code = 'title_issue_found'")).scalar() == "Title problem found"

    _two_flip_lenders(rdb)
    lead = _make_lead(rdb, phone_suffix="7001")
    _route(rdb, lead)
    buttons.click("routing_decline", _routed_event_id(rdb, lead))
    offered = [o["value"] for o in buttons.calls["opened"][-1]["blocks"][1]["element"]["options"]]
    assert "title_issue_found" in offered and "ltv_over_max" in offered

    for bad, blocks in ((label_form("Has Space", "x"), {"value_b"}), (label_form("ok_code", ""), {"label_b"}),
                        (label_form("", ""), {"value_b", "label_b"}), (label_form("x" * 51, "long"), {"value_b"})):
        assert set(slack_form(bad)["errors"]) == blocks, bad


def test_reroute_says_so_when_no_lender_matches_yet_and_the_file_is_held_again(rdb, buttons):
    lead = _make_lead(rdb, phone_suffix="7101")
    _route(rdb, lead)  # no lenders at all: held
    buttons.click("routing_reroute", _held_event_id(rdb, "lead_id", lead))
    said = buttons.calls["updated"][-1][0]
    assert "held again" in said and "no lender matches yet" in said
    assert _fr(rdb, lead)["status"] == "held"


# ── The form follows the chosen action ────────────────────────────────────────

def _ids(view: dict) -> list[str]:
    return [b["block_id"] for b in view["blocks"] if b.get("type") == "input"]


def test_the_form_shows_only_the_fields_the_chosen_action_needs(rdb):
    from src.lending.routing import slack_admin

    _two_flip_lenders(rdb)
    lenders = slack_admin._lenders(rdb)
    build = lambda action=None, values=None: slack_admin.build_form_view(lenders, {}, action, values)  # noqa: E731

    assert _ids(build()) == ["action_b"]  # nothing chosen yet: just the Action field
    action_block = [b for b in build()["blocks"] if b.get("block_id") == "action_b"][0]
    assert action_block["dispatch_action"] is True  # Slack tells us when it changes
    assert _ids(build("pause")) == ["action_b", "lender_b"]
    assert _ids(build("agreement")) == ["action_b", "lender_b", "value_b"]
    assert _ids(build("add_reason")) == ["action_b", "value_b", "label_b"]  # the Label appears only here
    assert _ids(build("add_lender")) == ["action_b", "value_b", "types_b", "states_b", "min_b", "max_b", "credit_b"]
    assert _ids(build("replace_boxes")) == ["action_b", "lender_b", "types_b", "states_b", "min_b", "max_b", "credit_b"]
    assert _ids(build("reopen_held")) == ["action_b"]
    for action in slack_admin.FIELDS:  # every action's fields are exactly what the checks can complain about
        assert set(_ids(build(action))) == slack_admin.shown_blocks(action)

    labelled = {b["block_id"]: b["label"]["text"] for b in build("seat_cap")["blocks"] if b.get("type") == "input"}
    assert labelled["value_b"] == "Monthly seat cap"  # Value says what it is for this action


def test_changing_the_action_rebuilds_the_form_and_keeps_what_was_typed(rdb, monkeypatch):
    from contextlib import contextmanager

    from src.lending.routing import slack_admin
    from src.lending.routing.slack_extension import RoutingSlack

    a, _ = _two_flip_lenders(rdb)
    monkeypatch.setattr(slack_admin, "allowed_user_ids", lambda: frozenset({"UJOSH"}))

    @contextmanager
    def session():
        yield rdb

    monkeypatch.setattr(slack_admin, "lending_session", session)

    class Client:
        def __init__(self):
            self.updated = []

        def views_update(self, view_id, view):
            self.updated.append((view_id, view))

    client = Client()
    values = {"lender_b": {"v": {"type": "static_select", "selected_option": {"value": str(a)}}},
              "value_b": {"v": {"type": "plain_text_input", "value": "typed earlier"}}}

    def change(to, user="UJOSH"):
        payload = {"type": "block_actions", "user": {"id": user}, "actions": [
            {"block_id": "action_b", "action_id": "v", "selected_option": {"value": to}}],
            "view": {"id": "V1", "callback_id": slack_admin.CALLBACK_ID, "state": {"values": values}}}
        return RoutingSlack(client).handle("interactive", payload)

    assert change("agreement") is True
    view_id, view = client.updated[-1]
    assert view_id == "V1" and _ids(view) == ["action_b", "lender_b", "value_b"]
    value_block = [b for b in view["blocks"] if b.get("block_id") == "value_b"][0]
    lender_block = [b for b in view["blocks"] if b.get("block_id") == "lender_b"][0]
    assert value_block["element"]["initial_value"] == "typed earlier"
    assert lender_block["element"]["initial_option"]["value"] == str(a)
    assert change("add_reason") is True and _ids(client.updated[-1][1]) == ["action_b", "value_b", "label_b"]
    count = len(client.updated)
    assert change("pause", user="USTRANGER") is True and len(client.updated) == count  # a stranger changes nothing


def test_an_error_on_a_field_the_form_does_not_show_moves_to_the_action_field():
    from src.lending.routing import slack_admin

    values = {"action_b": {"v": {"type": "static_select", "selected_option": {"value": "reopen_held"}}}}
    assert slack_admin._on_shown_fields({"value_b": "oops", "lender_b": "oops too"}, values) == {"action_b": "oops"}
    pause = {"action_b": {"v": {"type": "static_select", "selected_option": {"value": "pause"}}}}
    assert slack_admin._on_shown_fields({"lender_b": "needs a lender", "value_b": "x"}, pause) == {
        "lender_b": "needs a lender", "action_b": "x"}
