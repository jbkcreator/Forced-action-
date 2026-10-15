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
    from src.lending.models import LendingContact

    conn = lending_db.get_bind()
    LendingContact.__table__.create(bind=conn, checkfirst=True)  # FK target on a fresh database
    apply_lf(conn)
    apply_enrichment(conn)
    apply_rule_values(conn)
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


# ── Spec gap fixes (A–F, sweep trigger, suppressed leads) ─────────────────────

class _FakeSlack:
    def __init__(self):
        self.sent: list[str] = []

    def chat_postMessage(self, channel, text):  # noqa: N802 - slack_sdk API name
        self.sent.append(text)
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
    from src.lending.routing.admin_api import make_primary

    old = _add_lender(rdb, name="Old", is_primary=True, priority=1, monthly_cap=5)
    new = _add_lender(rdb, name="New", priority=2)

    result = make_primary(new, None, rdb)
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
    """A typo in a box would silently never match, so the admin API refuses it and normalises case."""
    from pydantic import ValidationError

    from src.lending.routing.admin_api import ProductBoxIn

    assert ProductBoxIn(loan_types=["fix_and_flip", "Dscr_Rental"]).loan_types == ["FIX_AND_FLIP", "DSCR_RENTAL"]
    for bad in (["flip"], ["refi"], []):
        with pytest.raises(ValidationError):
            ProductBoxIn(loan_types=bad)


def test_create_primary_defaults_cap_to_15(rdb):
    from src.lending.routing.admin_api import LenderIn, create_lender

    created = create_lender(LenderIn(name="P", is_primary=True, priority=1), None, rdb)
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

@pytest.fixture
def api(rdb):
    """The routing admin router on its own throwaway app, bound to the test transaction."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.lending.db import get_lending_db
    from src.lending.routing.admin_api import _verify_admin, router

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[_verify_admin] = lambda: None
    app.dependency_overrides[get_lending_db] = lambda: rdb
    return TestClient(app)


def test_file_record_shows_current_next_and_history_and_one_click_decline(rdb, api):
    a = _add_lender(rdb, name="First", priority=1)
    _add_box(rdb, a, loan_types=[LOAN_TYPE_FLIP])
    b = _add_lender(rdb, name="Second", priority=2)
    _add_box(rdb, b, loan_types=[LOAN_TYPE_FLIP])
    lead = _make_lead(rdb, phone_suffix="1901", last_name="Wozniak")
    _route(rdb, lead)

    rec = api.get(f"/api/lending/routing/files/{lead}").json()
    assert rec["file"]["last_name"] == "Wozniak" and rec["file"]["status"] == "routed"
    assert rec["current"]["name"] == "First" and rec["current"]["position"] == 1
    assert rec["next"]["name"] == "Second" and rec["next"]["position"] == 2
    assert [h["action"] for h in rec["history"]] == ["routed"]
    assert rec["can_decline"] is True

    assert api.post(f"/api/lending/routing/files/{lead}/decline", json={}).status_code == 200
    rec = api.get(f"/api/lending/routing/files/{lead}").json()
    assert rec["current"]["name"] == "Second" and rec["next"] is None
    assert [h["action"] for h in rec["history"]] == ["routed", "declined", "routed"]
    assert rec["history"][1]["reason_code"] == "other" and rec["history"][1]["actor"] == "josh"

    assert api.post(f"/api/lending/routing/files/{lead}/decline",
                    json={"reason_code": "ltv_over_max", "actor": "josh"}).status_code == 200
    rec = api.get(f"/api/lending/routing/files/{lead}").json()
    assert rec["file"]["status"] == "exhausted" and rec["current"] is None and rec["can_decline"] is False
    assert api.post(f"/api/lending/routing/files/{lead}/decline", json={}).status_code == 422
    assert api.get("/api/lending/routing/files/999999").status_code == 404


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


def test_file_list_filters_by_status_and_last_name(rdb, api):
    l1 = _add_lender(rdb, name="Lister", priority=1)
    _add_box(rdb, l1, loan_types=[LOAN_TYPE_FLIP])
    routed = _make_lead(rdb, phone_suffix="2001", last_name="Alvarez")
    held = _make_lead(rdb, loan_type="other", phone_suffix="2002", last_name="Brandt")
    _route(rdb, routed)
    _route(rdb, held)

    everything = api.get("/api/lending/routing/files").json()
    assert {f["lead_id"] for f in everything["files"]} == {routed, held}
    only_held = api.get("/api/lending/routing/files", params={"status": "held"}).json()
    assert [f["lead_id"] for f in only_held["files"]] == [held]
    by_name = api.get("/api/lending/routing/files", params={"q": "alvar"}).json()
    assert [f["lead_id"] for f in by_name["files"]] == [routed]
    assert api.get("/api/lending/routing/files", params={"status": "bogus"}).status_code == 422


def test_reorder_is_atomic_validated_and_changes_routing(rdb, api):
    ids = {}
    for i, name in enumerate(("Aye", "Bee", "Sea"), start=1):
        ids[name] = _add_lender(rdb, name=name, priority=i)
        _add_box(rdb, ids[name], loan_types=[LOAN_TYPE_FLIP])
    first = _make_lead(rdb, phone_suffix="2101")
    _route(rdb, first)
    assert _fr(rdb, first)["current_lender_name"] == "Aye"

    order = [ids["Sea"], ids["Aye"], ids["Bee"]]
    assert api.post("/api/lending/routing/lenders/reorder", json={"ordered_ids": order}).status_code == 200
    listed = [r["name"] for r in api.get("/api/lending/routing/lenders").json()]
    assert listed == ["Sea", "Aye", "Bee"]
    second = _make_lead(rdb, phone_suffix="2102")
    _route(rdb, second)
    assert _fr(rdb, second)["current_lender_name"] == "Sea"

    assert api.post("/api/lending/routing/lenders/reorder",
                    json={"ordered_ids": [ids["Sea"], ids["Aye"]]}).status_code == 422  # an active one left out
    assert api.post("/api/lending/routing/lenders/reorder",
                    json={"ordered_ids": order + [99999]}).status_code == 404
    assert api.post("/api/lending/routing/lenders/reorder",
                    json={"ordered_ids": [order[0], order[0], order[1], order[2]]}).status_code == 422
    assert [r["name"] for r in api.get("/api/lending/routing/lenders").json()] == ["Sea", "Aye", "Bee"]


def test_patch_and_create_report_priority_clash_as_409_and_allow_clearing_a_cap(rdb, api):
    a = _add_lender(rdb, name="One", priority=1, monthly_cap=4)
    b = _add_lender(rdb, name="Two", priority=2)

    assert api.patch(f"/api/lending/routing/lenders/{b}", json={"priority": 1}).status_code == 409
    assert api.post("/api/lending/routing/lenders", json={"name": "Dup", "priority": 1}).status_code == 409
    # Pausing frees the priority for the active lenders.
    assert api.patch(f"/api/lending/routing/lenders/{a}", json={"active": False}).status_code == 200
    assert api.patch(f"/api/lending/routing/lenders/{b}", json={"priority": 1}).status_code == 200

    assert api.patch(f"/api/lending/routing/lenders/{a}", json={"monthly_cap": None}).status_code == 200
    cap = rdb.execute(text("SELECT monthly_cap FROM lending.lender_routing_rules WHERE id = :i"), {"i": a}).scalar()
    assert cap is None
    assert api.patch(f"/api/lending/routing/lenders/{a}", json={"name": None}).status_code == 422


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


def test_meta_lists_the_vocabularies(api):
    meta = api.get("/api/lending/routing/meta").json()
    assert meta["loan_types"] == ["FIX_AND_FLIP", "GROUND_UP_CONSTRUCTION", "DSCR_RENTAL", "BRIDGE"]
    assert "signed" in meta["agreement_statuses"] and meta["default_decline_reason"] == "other"


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


def test_admin_api_validates_rule_keys_and_refuses_boxes_on_a_t05_lender(api):
    base = "/api/lending/routing"
    assert api.post(f"{base}/lenders", json={"name": "Bad", "priority": 1, "rule_keys": ["nope"]}).status_code == 422
    made = api.post(f"{base}/lenders", json={"name": "Linked", "priority": 1, "rule_keys": [FLIP_KEY]})
    assert made.status_code == 201
    lender_id = made.json()["id"]
    listed = {r["name"]: r for r in api.get(f"{base}/lenders").json()}
    assert listed["Linked"]["rule_keys"] == [FLIP_KEY]

    blocked = api.post(f"{base}/lenders/{lender_id}/boxes", json={"loan_types": ["fix_and_flip"]})
    assert blocked.status_code == 409 and "lender_rule_values" in blocked.json()["detail"]

    assert api.patch(f"{base}/lenders/{lender_id}", json={"rule_keys": None}).status_code == 200  # unlink
    assert api.post(f"{base}/lenders/{lender_id}/boxes", json={"loan_types": ["fix_and_flip"]}).status_code == 201


# ── Josh's decline-reason enum ────────────────────────────────────────────────

def test_decline_reasons_are_joshs_enum_and_the_old_placeholders_are_off(api):
    from config.lending_routing import LEGACY_PLACEHOLDER_REASONS, SEED_DECLINE_REASONS

    rows = api.get("/api/lending/routing/decline-reasons").json()
    active = {r["code"] for r in rows if r["active"]}
    josh = {code for code, _ in SEED_DECLINE_REASONS}
    assert len(josh) == 14 and josh <= active  # codes Josh adds later may sit alongside; his 14 must all be on
    assert {"credit_below_min", "ltv_over_max", "seat_cap_reached", "borrower_withdrew", "other"} <= active
    assert not (set(LEGACY_PLACEHOLDER_REASONS) - {"other"}) & active  # the old placeholders are off


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
                      " ('unspecified', 'old placeholder', TRUE), ('appraisal_low', 'added by Josh later', TRUE)"
                      " ON CONFLICT (code) DO UPDATE SET active = TRUE"))
    apply_to(conn)
    state = {r["code"]: r["active"] for r in rdb.execute(text(
        "SELECT code, active FROM lending.decline_reason_codes WHERE code IN ('unspecified', 'appraisal_low', 'other')"
    )).mappings()}
    assert state == {"unspecified": False, "appraisal_low": True, "other": True}  # off, never deleted
