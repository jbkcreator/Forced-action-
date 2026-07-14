"""Loan Lane test suite — rewritten against the v6 refactor.

v6 model under test:
  - Lanes are anchored on property_id (no prospect path); seeded from financing intent.
  - Stage legality comes from lane_stage_config.allowed_next, AND intermediate/
    terminal stages are gated on the broker work-state (_STAGE_WORK_STATE_GATE).
  - Broker SM is append-only broker_transitions; claim = atomic assign_broker;
    transitions validate matrix + reason_code + ownership; lender_rejected cycle.
  - No event emission in any loan-lane module — tests run unpatched on Postgres.
  - Consumers (lane_closer / commission_poster) take row-like objects with a payload.

Covers: lane core, work-state machine, SMS gate, commission ledger, consumers,
fee gate, claim/reassign/release, pool, staleness, lender attachment.
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from config.broker_states import IllegalBrokerTransition
from src.consumers.loan_lane_consumers import handle_commission_poster, handle_lane_closer
from src.services.broker_state_machine import (
    ClosedWonPayloadRequired,
    InvalidGrossAmount,
    LaneOwnershipError,
    assign_broker,
    current_state,
    sms_eligible,
    transition,
)
from src.services.commission_ledger import (
    compute_net_lines,
    dispute_entry,
    post_commission,
    post_offset,
    resolve_split_config,
)
from src.services.loan_lane_service import (
    advance_lane,
    enter_lane,
    fee_surfaces_enabled,
    get_pool,
    get_stale_lanes,
    reassign_lane,
    release_lane,
    set_lane_lender,
    set_lane_outcome,
)

LANE_TYPE = "distressed-payoff"
SPLIT = "platform_50_broker_50"


# ---------------------------------------------------------------------------
# Seed helpers (property_id anchor — the v6 lane has no prospect linkage)
# ---------------------------------------------------------------------------

def _property(session) -> int:
    return session.execute(
        text("INSERT INTO properties (parcel_id, county_id, created_at, updated_at) "
             "VALUES (:pc, 'hillsborough', NOW(), NOW()) RETURNING id"),
        {"pc": f"PARC-{uuid.uuid4().hex[:12]}"},
    ).scalar()


def _fi_score(session, property_id: int, *, score: float = 80.0) -> None:
    session.execute(
        text("""INSERT INTO financing_intent_scores
                    (property_id, score_date, financing_intent_score, intent_tier)
                VALUES (:p, CURRENT_DATE, :s, 'high')"""),
        {"p": property_id, "s": score},
    )


def _broker(session, *, active: bool = True) -> str:
    return str(session.execute(
        text("INSERT INTO brokers (email, name, is_active) "
             "VALUES (:e, 'Test Broker', :a) RETURNING broker_id"),
        {"e": f"{uuid.uuid4().hex[:10]}@lender.test", "a": active},
    ).scalar())


def _lender(session, *, cleared: bool = True, active: bool = True) -> str:
    return str(session.execute(
        text("INSERT INTO lenders (name, is_cleared, is_active) "
             "VALUES (:n, :c, :a) RETURNING lender_id"),
        {"n": f"Lender {uuid.uuid4().hex[:8]}", "c": cleared, "a": active},
    ).scalar())


def _lane(session) -> tuple[str, int]:
    pid = _property(session)
    lane_id = enter_lane(session, pid, lane_type=LANE_TYPE, loan_program="bridge")
    return lane_id, pid


def _fetch_lane(session, lane_id: str):
    return session.execute(
        text("SELECT current_stage, outcome, assigned_broker_id, loan_program, "
             "fee_config_flag, claimed_at, last_activity_at, lender_id "
             "FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
        {"lid": lane_id},
    ).fetchone()


def _seed_transition(session, lane_id: str, broker_id: str, to_state: str,
                     from_state: str = "unassigned", reason: str = "qualified") -> str:
    """Insert a broker_transitions row directly — for matrix tests that need a
    specific from-state without walking the whole chain."""
    return str(session.execute(
        text("""INSERT INTO broker_transitions
                    (lane_id, broker_id, from_state, to_state, reason_code, actor)
                VALUES (CAST(:l AS uuid), CAST(:b AS uuid), :f, :t, :r, 'seed')
                RETURNING transition_id"""),
        {"l": lane_id, "b": broker_id, "f": from_state, "t": to_state, "r": reason},
    ).scalar())


def _claimed_lane(session) -> tuple[str, str]:
    """Lane claimed by a fresh broker — returns (lane_id, broker_id)."""
    lane_id, _ = _lane(session)
    broker_id = _broker(session)
    assert assign_broker(session, lane_id, broker_id) is True
    return lane_id, broker_id


def _walk_to_closed_won(session, gross_cents: int = 500_000) -> tuple[str, str, str]:
    """Full legal chain to closed_won. Returns (lane_id, broker_id, transition_id)."""
    lane_id, broker_id = _claimed_lane(session)
    transition(session, lane_id, "working", broker_id, "qualified")
    transition(session, lane_id, "quoted", broker_id, "qualified")
    transition(session, lane_id, "committed", broker_id, "docs_received")
    tid = transition(
        session, lane_id, "closed_won", broker_id, "funded",
        gross_amount_cents=gross_cents, split_config_id=SPLIT,
    )
    return lane_id, broker_id, tid


def _event_row(payload: dict):
    return SimpleNamespace(event_id=uuid.uuid4(), payload=payload)


def _seed_tier(session, split_id: str, *, min_cents: int, max_cents: int | None,
               platform_pct: int, broker_pct: int) -> str:
    """Insert a deal-size fee tier (Task 3.2). Rolled back with the test savepoint."""
    import json
    session.execute(
        text("""INSERT INTO commission_splits
                    (split_config_id, name, parties, is_active, min_gross_cents, max_gross_cents)
                VALUES (:sid, :sid, CAST(:parties AS jsonb), true, :mn, :mx)"""),
        {
            "sid": split_id,
            "parties": json.dumps(
                [{"party": "platform", "pct": platform_pct},
                 {"party": "broker", "pct": broker_pct}]
            ),
            "mn": min_cents,
            "mx": max_cents,
        },
    )
    return split_id


# ---------------------------------------------------------------------------
# Lane core
# ---------------------------------------------------------------------------

def test_enter_lane_starts_at_lowest_stage_open(fresh_db):
    pid = _property(fresh_db)
    lane_id = enter_lane(fresh_db, pid, lane_type=LANE_TYPE, loan_program="bridge")
    lane = _fetch_lane(fresh_db, lane_id)
    assert lane.current_stage == "entered"
    assert lane.outcome == "open"
    assert lane.assigned_broker_id is None
    assert lane.loan_program == "bridge"
    assert lane.fee_config_flag is False


def test_enter_lane_idempotent(fresh_db):
    pid = _property(fresh_db)
    a = enter_lane(fresh_db, pid, lane_type=LANE_TYPE)
    b = enter_lane(fresh_db, pid, lane_type=LANE_TYPE)
    assert a == b
    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM lanes WHERE property_id = :p AND lane_type = :lt"),
        {"p": pid, "lt": LANE_TYPE},
    ).scalar()
    assert count == 1


def test_advance_lane_legal_when_work_state_permits(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "qualified")
    transition(fresh_db, lane_id, "quoted", broker_id, "qualified")
    advance_lane(fresh_db, lane_id, "quoted", actor="admin_1")
    assert _fetch_lane(fresh_db, lane_id).current_stage == "quoted"


def test_advance_lane_illegal_stage_skip_rejected(fresh_db):
    lane_id, _ = _lane(fresh_db)
    with pytest.raises(ValueError, match="illegal lane advance"):
        advance_lane(fresh_db, lane_id, "funded", actor="admin_1")  # entered→funded not in allowed_next


def test_advance_lane_blocked_by_work_state_gate(fresh_db):
    # entered→quoted IS in allowed_next, but the broker work-state is still
    # unassigned — the v6 gate must refuse the stage advance.
    lane_id, _ = _lane(fresh_db)
    with pytest.raises(ValueError, match="work state"):
        advance_lane(fresh_db, lane_id, "quoted", actor="admin_1")


def test_advance_lane_refreshes_last_activity(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "qualified")
    transition(fresh_db, lane_id, "quoted", broker_id, "qualified")
    fresh_db.execute(
        text("UPDATE lanes SET last_activity_at = '2020-01-01' WHERE lane_id = CAST(:l AS uuid)"),
        {"l": lane_id},
    )
    advance_lane(fresh_db, lane_id, "quoted", actor="admin_1")
    assert _fetch_lane(fresh_db, lane_id).last_activity_at.year >= 2026


def test_set_lane_outcome_idempotent_and_mirrors_stage(fresh_db):
    lane_id, _ = _lane(fresh_db)
    set_lane_outcome(fresh_db, lane_id, "funded")
    set_lane_outcome(fresh_db, lane_id, "dead")  # second close is a no-op
    lane = _fetch_lane(fresh_db, lane_id)
    assert lane.outcome == "funded"
    assert lane.current_stage == "funded"


# ---------------------------------------------------------------------------
# Broker state machine
# ---------------------------------------------------------------------------

def test_current_state_defaults_unassigned(fresh_db):
    lane_id, _ = _lane(fresh_db)
    assert current_state(fresh_db, lane_id) == "unassigned"


def test_claim_is_atomic_and_sets_assignment_metadata(fresh_db):
    lane_id, _ = _lane(fresh_db)
    b1 = _broker(fresh_db)
    b2 = _broker(fresh_db)

    assert assign_broker(fresh_db, lane_id, b1) is True
    assert assign_broker(fresh_db, lane_id, b2) is False  # loser of the race

    lane = _fetch_lane(fresh_db, lane_id)
    assert str(lane.assigned_broker_id) == b1
    assert lane.claimed_at is not None
    assert current_state(fresh_db, lane_id) == "assigned"
    # claiming never advances the funnel stage
    assert lane.current_stage == "entered"


def test_legal_transition_path(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "no_contact")
    transition(fresh_db, lane_id, "quoted", broker_id, "price")
    assert current_state(fresh_db, lane_id) == "quoted"


@pytest.mark.parametrize("frm,to", [
    ("assigned", "committed"),   # skip-ahead
    ("quoted", "assigned"),      # backward
    ("closed_won", "working"),   # reopen a terminal state
])
def test_illegal_transitions_rejected(fresh_db, frm, to):
    lane_id, broker_id = _claimed_lane(fresh_db)
    if frm != "assigned":  # claim already put the lane at 'assigned'
        _seed_transition(fresh_db, lane_id, broker_id, frm)
    with pytest.raises(IllegalBrokerTransition):
        transition(fresh_db, lane_id, to, broker_id, "qualified")


def test_unknown_reason_code_rejected(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    with pytest.raises(IllegalBrokerTransition, match="reason_code"):
        transition(fresh_db, lane_id, "working", broker_id, "bogus_reason")


def test_transition_requires_lane_ownership(fresh_db):
    lane_id, _owner = _claimed_lane(fresh_db)
    intruder = _broker(fresh_db)
    with pytest.raises(LaneOwnershipError):
        transition(fresh_db, lane_id, "working", intruder, "qualified")


@pytest.mark.parametrize("active_state", ["assigned", "working", "quoted", "committed"])
def test_closed_lost_legal_from_any_active(fresh_db, active_state):
    lane_id, broker_id = _claimed_lane(fresh_db)
    if active_state != "assigned":
        _seed_transition(fresh_db, lane_id, broker_id, active_state)
    transition(fresh_db, lane_id, "closed_lost", broker_id, "lost_other")
    assert current_state(fresh_db, lane_id) == "closed_lost"


def test_lender_rejected_cycles_back_to_quoted(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "qualified")
    transition(fresh_db, lane_id, "quoted", broker_id, "qualified")
    transition(fresh_db, lane_id, "lender_rejected", broker_id, "lender_declined")
    transition(fresh_db, lane_id, "quoted", broker_id, "price")  # re-quote after rejection
    assert current_state(fresh_db, lane_id) == "quoted"


def test_closed_won_requires_gross_and_split(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "qualified")
    transition(fresh_db, lane_id, "quoted", broker_id, "qualified")
    transition(fresh_db, lane_id, "committed", broker_id, "docs_received")

    with pytest.raises(ClosedWonPayloadRequired):
        transition(fresh_db, lane_id, "closed_won", broker_id, "funded")

    with pytest.raises(InvalidGrossAmount):
        transition(fresh_db, lane_id, "closed_won", broker_id, "funded",
                   gross_amount_cents=0, split_config_id=SPLIT)


# ---------------------------------------------------------------------------
# SMS eligibility (stage sms_allowed AND active work-state)
# ---------------------------------------------------------------------------

def test_sms_not_eligible_at_entered_stage(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "qualified")
    assert sms_eligible(fresh_db, lane_id) is False  # 'entered' has sms_allowed=false


def test_sms_eligible_at_quoted_stage_with_active_state(fresh_db):
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "qualified")
    transition(fresh_db, lane_id, "quoted", broker_id, "qualified")
    advance_lane(fresh_db, lane_id, "quoted", actor="admin_1")
    assert sms_eligible(fresh_db, lane_id) is True


# ---------------------------------------------------------------------------
# Commission ledger
# ---------------------------------------------------------------------------

def test_compute_net_lines_50_50(fresh_db):
    lines = compute_net_lines(fresh_db, 1_000_000, SPLIT)
    by_party = {l["party"]: l["amount_cents"] for l in lines}
    assert by_party == {"platform": 500_000, "broker": 500_000}


def test_compute_net_lines_remainder_deterministic(fresh_db):
    lines = compute_net_lines(fresh_db, 1_000_001, SPLIT)
    assert sum(l["amount_cents"] for l in lines) == 1_000_001
    assert lines[0]["amount_cents"] == 500_001  # remainder cent to first party


def test_post_commission_idempotent(fresh_db):
    _lane_id, _broker_id, tid = _walk_to_closed_won(fresh_db, 500_000)
    e1 = post_commission(fresh_db, tid, 500_000, SPLIT)
    e2 = post_commission(fresh_db, tid, 500_000, SPLIT)  # replay
    assert e1 is not None and e2 is None
    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM commission_ledger WHERE trigger_transition_id = CAST(:t AS uuid)"),
        {"t": tid},
    ).scalar()
    assert count == 1


def test_flat_split_posts_correct_net_lines_e2e(fresh_db):
    """E2E-verify the flat 50/50 split (Task 3.2). Tiers deferred, ADR 0031."""
    _lane_id, _broker_id, tid = _walk_to_closed_won(fresh_db, 500_000)
    payload = {"to_state": "closed_won", "transition_id": tid,
               "gross_amount_cents": 500_000, "split_config_id": SPLIT}
    handle_commission_poster(fresh_db, _event_row(payload))

    net_lines = fresh_db.execute(
        text("SELECT net_lines FROM commission_ledger WHERE trigger_transition_id = CAST(:t AS uuid)"),
        {"t": tid},
    ).scalar()
    by_party = {l["party"]: l["amount_cents"] for l in net_lines}
    assert by_party == {"platform": 250_000, "broker": 250_000}
    assert sum(by_party.values()) == 500_000


# ---------------------------------------------------------------------------
# Deal-size fee tiers (Task 3.2 / ADR 0031)
# ---------------------------------------------------------------------------

def test_resolve_split_picks_tier_by_deal_size(fresh_db):
    uniq = uuid.uuid4().hex[:8]
    small = _seed_tier(fresh_db, f"tier_small_{uniq}", min_cents=0, max_cents=1_000_000,
                       platform_pct=70, broker_pct=30)
    large = _seed_tier(fresh_db, f"tier_large_{uniq}", min_cents=1_000_000, max_cents=None,
                       platform_pct=40, broker_pct=60)
    # Below 1M → small tier; at/above 1M → large tier (min inclusive, max exclusive).
    assert resolve_split_config(fresh_db, 500_000) == small
    assert resolve_split_config(fresh_db, 999_999) == small
    assert resolve_split_config(fresh_db, 1_000_000) == large
    assert resolve_split_config(fresh_db, 5_000_000) == large


def test_resolve_split_prefers_specific_band_over_catch_all(fresh_db):
    """A real [0, 1M) band beats the platform_50_broker_50 catch-all [0, +inf)."""
    uniq = uuid.uuid4().hex[:8]
    small = _seed_tier(fresh_db, f"tier_small_{uniq}", min_cents=0, max_cents=1_000_000,
                       platform_pct=70, broker_pct=30)
    assert resolve_split_config(fresh_db, 500_000) == small
    # Above the specific band, resolution falls through to the catch-all default.
    assert resolve_split_config(fresh_db, 5_000_000) == SPLIT


def test_post_commission_auto_resolves_tier_when_split_omitted(fresh_db):
    """The ledger reflects the deal-size tier even when no split is passed."""
    uniq = uuid.uuid4().hex[:8]
    _seed_tier(fresh_db, f"tier_big_{uniq}", min_cents=1_000_000, max_cents=None,
               platform_pct=40, broker_pct=60)
    _lane_id, _broker_id, tid = _walk_to_closed_won(fresh_db, 2_000_000)
    entry_id = post_commission(fresh_db, tid, 2_000_000, split_config_id=None)
    assert entry_id is not None
    row = fresh_db.execute(
        text("SELECT split_config_id, net_lines FROM commission_ledger "
             "WHERE entry_id = CAST(:e AS uuid)"),
        {"e": entry_id},
    ).fetchone()
    assert row.split_config_id == f"tier_big_{uniq}"
    by_party = {l["party"]: l["amount_cents"] for l in row.net_lines}
    assert by_party == {"platform": 800_000, "broker": 1_200_000}  # 40/60 of $20k


def test_transition_closed_won_resolves_tier_without_explicit_split(fresh_db):
    """closed_won may omit split_config_id when a deal-size tier covers the gross."""
    uniq = uuid.uuid4().hex[:8]
    _seed_tier(fresh_db, f"tier_any_{uniq}", min_cents=0, max_cents=None,
               platform_pct=55, broker_pct=45)
    lane_id, broker_id = _claimed_lane(fresh_db)
    transition(fresh_db, lane_id, "working", broker_id, "qualified")
    transition(fresh_db, lane_id, "quoted", broker_id, "qualified")
    transition(fresh_db, lane_id, "committed", broker_id, "docs_received")
    # No split_config_id — must resolve from the seeded tier, not raise.
    tid = transition(fresh_db, lane_id, "closed_won", broker_id, "funded",
                     gross_amount_cents=300_000)
    assert tid is not None


def test_dispute_and_offset_are_append_only(fresh_db):
    _lane_id, _broker_id, tid = _walk_to_closed_won(fresh_db, 400_000)
    entry_id = post_commission(fresh_db, tid, 400_000, SPLIT)
    dispute_entry(fresh_db, entry_id, actor="admin_1")
    offset_id = post_offset(fresh_db, entry_id, actor="admin_1")

    orig = fresh_db.execute(
        text("SELECT status, gross_amount_cents, net_lines FROM commission_ledger "
             "WHERE entry_id = CAST(:e AS uuid)"),
        {"e": entry_id},
    ).fetchone()
    offset = fresh_db.execute(
        text("SELECT net_lines FROM commission_ledger WHERE entry_id = CAST(:e AS uuid)"),
        {"e": offset_id},
    ).fetchone()

    assert orig.status == "disputed"
    assert orig.gross_amount_cents == 400_000  # original amounts untouched
    orig_broker = {l["party"]: l["amount_cents"] for l in orig.net_lines}["broker"]
    off_broker = {l["party"]: l["amount_cents"] for l in offset.net_lines}["broker"]
    assert off_broker == -orig_broker


# ---------------------------------------------------------------------------
# Consumers (row-like objects with .payload; no event bus involved)
# ---------------------------------------------------------------------------

def test_lane_closer_funds_on_closed_won(fresh_db):
    lane_id, _ = _lane(fresh_db)
    handle_lane_closer(fresh_db, _event_row({"to_state": "closed_won", "lane_id": lane_id}))
    assert _fetch_lane(fresh_db, lane_id).outcome == "funded"


def test_lane_closer_ignores_intermediate_state(fresh_db):
    lane_id, _ = _lane(fresh_db)
    handle_lane_closer(fresh_db, _event_row({"to_state": "quoted", "lane_id": lane_id}))
    lane = _fetch_lane(fresh_db, lane_id)
    assert lane.current_stage == "entered"
    assert lane.outcome == "open"


def test_lane_closer_kills_on_closed_lost(fresh_db):
    lane_id, _ = _lane(fresh_db)
    handle_lane_closer(fresh_db, _event_row({"to_state": "closed_lost", "lane_id": lane_id}))
    assert _fetch_lane(fresh_db, lane_id).outcome == "dead"


def test_commission_poster_posts_once_on_closed_won(fresh_db):
    _lane_id, _broker_id, tid = _walk_to_closed_won(fresh_db, 300_000)
    payload = {"to_state": "closed_won", "transition_id": tid,
               "gross_amount_cents": 300_000, "split_config_id": SPLIT}
    handle_commission_poster(fresh_db, _event_row(payload))
    handle_commission_poster(fresh_db, _event_row(payload))  # replay
    rows = fresh_db.execute(
        text("SELECT gross_amount_cents FROM commission_ledger "
             "WHERE trigger_transition_id = CAST(:t AS uuid)"),
        {"t": tid},
    ).fetchall()
    assert len(rows) == 1
    assert rows[0].gross_amount_cents == 300_000


# ---------------------------------------------------------------------------
# Fee gate
# ---------------------------------------------------------------------------

def test_fee_surfaces_disabled_by_default(fresh_db):
    lane_id, _ = _lane(fresh_db)
    assert fee_surfaces_enabled(fresh_db, lane_id) is False


def test_fee_surfaces_enabled_when_flag_on(fresh_db):
    lane_id, _ = _lane(fresh_db)
    fresh_db.execute(
        text("UPDATE lanes SET fee_config_flag = true WHERE lane_id = CAST(:l AS uuid)"),
        {"l": lane_id},
    )
    assert fee_surfaces_enabled(fresh_db, lane_id) is True


# ---------------------------------------------------------------------------
# Ownership admin ops, pool, staleness, lender
# ---------------------------------------------------------------------------

def test_reassign_and_release_lane_change_owner(fresh_db):
    lane_id, _b1 = _claimed_lane(fresh_db)
    b2 = _broker(fresh_db)
    reassign_lane(fresh_db, lane_id, b2, actor="admin_1")
    assert str(_fetch_lane(fresh_db, lane_id).assigned_broker_id) == b2
    release_lane(fresh_db, lane_id, actor="admin_1")
    lane = _fetch_lane(fresh_db, lane_id)
    assert lane.assigned_broker_id is None
    assert lane.claimed_at is None


def test_get_pool_lists_open_unclaimed_scored_lanes_only(fresh_db):
    # in pool: open + unclaimed + has a financing intent score
    pool_lane, pool_prop = _lane(fresh_db)
    _fi_score(fresh_db, pool_prop, score=91.5)

    # excluded: claimed
    claimed_lane, claimed_prop = _lane(fresh_db)
    _fi_score(fresh_db, claimed_prop)
    assign_broker(fresh_db, claimed_lane, _broker(fresh_db))

    # excluded: no financing intent score (inner lateral join)
    unscored_lane, _ = _lane(fresh_db)

    ids = {row["lane_id"] for row in get_pool(fresh_db, limit=500)}
    assert pool_lane in ids
    assert claimed_lane not in ids
    assert unscored_lane not in ids

    mine = next(row for row in get_pool(fresh_db, limit=500) if row["lane_id"] == pool_lane)
    assert mine["financing_intent_score"] == pytest.approx(91.5)
    assert mine["intent_tier"] == "high"


def test_get_stale_lanes_uses_activity_window(fresh_db):
    stale_lane, _ = _lane(fresh_db)
    fresh_lane, _ = _lane(fresh_db)
    fresh_db.execute(
        text("UPDATE lanes SET last_activity_at = NOW() - INTERVAL '40 days' "
             "WHERE lane_id = CAST(:l AS uuid)"),
        {"l": stale_lane},
    )
    ids = {row["lane_id"] for row in get_stale_lanes(fresh_db, days=30)}
    assert stale_lane in ids
    assert fresh_lane not in ids


def test_set_lane_lender_requires_active_and_cleared_lender(fresh_db):
    lane_id, _ = _lane(fresh_db)
    not_cleared = _lender(fresh_db, cleared=False)
    inactive = _lender(fresh_db, cleared=True, active=False)
    good = _lender(fresh_db)

    with pytest.raises(ValueError, match="not eligible"):
        set_lane_lender(fresh_db, lane_id, not_cleared, actor="broker_1")
    with pytest.raises(ValueError, match="not eligible"):
        set_lane_lender(fresh_db, lane_id, inactive, actor="broker_1")

    set_lane_lender(fresh_db, lane_id, good, actor="broker_1")
    assert str(_fetch_lane(fresh_db, lane_id).lender_id) == good
