"""TDD test suite for Loan Lane Core & Broker State Machine (S1, spec v6).

Covers:
  Lane core   — enter_lane (idempotent), advance_lane legality, set_lane_outcome
  Broker SM   — current_state, transition matrix (legal/illegal), reason_code enum,
                assign_broker, closed_won payload requirement, sms_eligible
  Commission  — compute_net_lines split + remainder, post_commission idempotent,
                dispute + offset
  Consumers   — verdict→lane, lane_closer (terminal only), commission_poster
  Fee gate    — fee_surfaces_enabled
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text

from src.consumers.loan_lane_consumers import (
    handle_commission_poster,
    handle_lane_closer,
)
from src.services.broker_state_machine import (
    IllegalTransition,
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
)
from src.services.loan_lane_service import (
    advance_lane,
    claim_lane,
    enter_lane,
    fee_surfaces_enabled,
    get_pool,
    get_stale_lanes,
    release_lane,
    reassign_lane,
    set_lane_lender,
    set_lane_outcome,
)

LANE_TYPE = "distressed-payoff"
SPLIT = "platform_50_broker_50"


# ---------------------------------------------------------------------------
# Helpers — build the prospect → lane → broker chain on real Postgres
# ---------------------------------------------------------------------------

def _property(session) -> int:
    return session.execute(
        text("""
            INSERT INTO properties (parcel_id, county_id, created_at, updated_at)
            VALUES (:pc, 'hillsborough', NOW(), NOW())
            RETURNING id
        """),
        {"pc": f"PARC-{uuid.uuid4().hex[:12]}"},
    ).scalar()


def _prospect(session) -> str:
    prop_id = _property(session)
    return str(session.execute(
        text("""
            INSERT INTO prospects (prospect_id, property_id, contactability_state)
            VALUES (gen_random_uuid(), :pid, 'contactable') RETURNING prospect_id
        """),
        {"pid": prop_id},
    ).scalar())


def _broker(session, name="Prime Capital") -> str:
    return str(session.execute(
        text("INSERT INTO brokers (email, name) VALUES (:e, :n) RETURNING broker_id"),
        {"e": f"{uuid.uuid4().hex[:8]}@lender.test", "n": name},
    ).scalar())


def _lender(session, name="Cleared Lender", *, cleared=True, active=True) -> str:
    return str(session.execute(
        text("""
            INSERT INTO lenders (name, is_cleared, is_active)
            VALUES (:n, :cleared, :active)
            RETURNING lender_id
        """),
        {"n": name, "cleared": cleared, "active": active},
    ).scalar())


def _lane(session) -> tuple[str, str]:
    pid = _prospect(session)
    lane_id = enter_lane(session, pid, lane_type=LANE_TYPE, loan_program="bridge")
    return lane_id, pid


def _fetch_lane(session, lane_id: str):
    return session.execute(
        text("SELECT current_stage, outcome, assigned_broker_id, loan_program, fee_config_flag, "
             "claimed_at, last_activity_at, lender_id "
             "FROM lanes WHERE lane_id = CAST(:lid AS uuid)"),
        {"lid": lane_id},
    ).fetchone()


def _seed_score(session, property_id: int, *, score=72, tier="Platinum", guess=False):
    session.execute(
        text("""
            INSERT INTO distress_scores
                (property_id, final_cds_score, lead_tier, distress_types, urgency_level,
                 vertical_scores, lead_confidence, is_guess_lead, score_date)
            VALUES
                (:pid, :score, :tier, '{"default": 1}'::jsonb, 'High',
                 '{"default": 1}'::jsonb, 0.9, :guess, NOW())
        """),
        {"pid": property_id, "score": score, "tier": tier, "guess": guess},
    )


def _event_row(prospect_id, payload):
    row = MagicMock()
    row.event_id = uuid.uuid4()
    row.prospect_id = prospect_id
    row.payload = payload
    return row


# ---------------------------------------------------------------------------
# Lane core
# ---------------------------------------------------------------------------

def test_enter_lane_starts_at_lowest_stage_open(fresh_db):
    pid = _prospect(fresh_db)
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id = enter_lane(fresh_db, pid, lane_type=LANE_TYPE, loan_program="bridge")
    lane = _fetch_lane(fresh_db, lane_id)
    assert lane.current_stage == "entered"
    assert lane.outcome == "open"
    assert lane.assigned_broker_id is None
    assert lane.loan_program == "bridge"
    assert lane.fee_config_flag is False


def test_enter_lane_idempotent(fresh_db):
    pid = _prospect(fresh_db)
    with patch("src.services.loan_lane_service.emit_event"):
        a = enter_lane(fresh_db, pid, lane_type=LANE_TYPE)
        b = enter_lane(fresh_db, pid, lane_type=LANE_TYPE)
    assert a == b
    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM lanes WHERE prospect_id = CAST(:p AS uuid)"), {"p": pid}
    ).scalar()
    assert count == 1


def test_advance_lane_legal(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        advance_lane(fresh_db, lane_id, "quoted", actor="admin_1")
    assert _fetch_lane(fresh_db, lane_id).current_stage == "quoted"


def test_advance_lane_illegal_skip_rejected(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        with pytest.raises(ValueError, match="illegal lane advance"):
            advance_lane(fresh_db, lane_id, "funded", actor="admin_1")  # entered→funded not allowed


def test_set_lane_outcome_idempotent(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
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
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
    assert current_state(fresh_db, lane_id) == "unassigned"


def test_assign_broker_sets_field_and_state(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id, actor="admin_1")
    assert current_state(fresh_db, lane_id) == "assigned"
    assert str(_fetch_lane(fresh_db, lane_id).assigned_broker_id) == broker_id
    # assignment does NOT advance the lane
    assert _fetch_lane(fresh_db, lane_id).current_stage == "entered"


def test_legal_transition_path(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id, actor="admin_1")
        transition(fresh_db, lane_id, "working", broker_id, "no_contact", actor=broker_id)
        transition(fresh_db, lane_id, "quoted", broker_id, "price", actor=broker_id)
    assert current_state(fresh_db, lane_id) == "quoted"


@pytest.mark.parametrize("frm,to", [
    ("assigned", "closed_won"),   # skip-ahead
    ("quoted", "assigned"),       # backward
    ("closed_won", "working"),    # reopen
])
def test_illegal_transitions_rejected(fresh_db, frm, to):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        # Seed the from_state via direct insert (testing the matrix, not the path)
        fresh_db.execute(
            text("""
                INSERT INTO broker_transitions
                    (lane_id, prospect_id, broker_id, from_state, to_state, reason_code, actor)
                SELECT CAST(:lid AS uuid), prospect_id, CAST(:bid AS uuid),
                       'unassigned', :frm, 'qualified', 'seed'
                FROM lanes WHERE lane_id = CAST(:lid AS uuid)
            """),
            {"lid": lane_id, "bid": broker_id, "frm": frm},
        )
        with pytest.raises(IllegalTransition):
            transition(fresh_db, lane_id, to, broker_id, "qualified", actor=broker_id)


def test_unknown_reason_code_rejected(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        with pytest.raises(IllegalTransition, match="reason_code"):
            transition(fresh_db, lane_id, "assigned", broker_id, "bogus_reason", actor="admin_1")


def test_closed_lost_legal_from_any_active(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id, actor="admin_1")
        transition(fresh_db, lane_id, "working", broker_id, "no_contact", actor=broker_id)
        transition(fresh_db, lane_id, "closed_lost", broker_id, "lost_other", actor=broker_id)
    assert current_state(fresh_db, lane_id) == "closed_lost"


def test_closed_won_requires_gross_and_split(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id, actor="admin_1")
        transition(fresh_db, lane_id, "working", broker_id, "no_contact", actor=broker_id)
        transition(fresh_db, lane_id, "quoted", broker_id, "price", actor=broker_id)
        transition(fresh_db, lane_id, "committed", broker_id, "qualified", actor=broker_id)
        with pytest.raises(IllegalTransition, match="gross_amount_cents"):
            transition(fresh_db, lane_id, "closed_won", broker_id, "funded", actor=broker_id)


# ---------------------------------------------------------------------------
# SMS eligibility
# ---------------------------------------------------------------------------

def test_sms_not_eligible_at_entered_stage(fresh_db):
    # 'entered' has sms_allowed=false in seed
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id, actor="admin_1")
    assert sms_eligible(fresh_db, lane_id) is False


def test_sms_eligible_at_quoted_stage(fresh_db):
    # 'quoted' has sms_allowed=true; work-state active
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_id = _broker(fresh_db)
        assign_broker(fresh_db, lane_id, broker_id, actor="admin_1")
        transition(fresh_db, lane_id, "working", broker_id, "no_contact", actor=broker_id)
        transition(fresh_db, lane_id, "quoted", broker_id, "price", actor=broker_id)
        advance_lane(fresh_db, lane_id, "quoted", actor="admin_1")
    assert sms_eligible(fresh_db, lane_id) is True


# ---------------------------------------------------------------------------
# Commission ledger
# ---------------------------------------------------------------------------

def test_compute_net_lines_50_50(fresh_db):
    lines = compute_net_lines(fresh_db, 1_000_000, SPLIT)  # $10,000
    by_party = {l["party"]: l["amount_cents"] for l in lines}
    assert by_party == {"platform": 500_000, "broker": 500_000}


def test_compute_net_lines_remainder_deterministic(fresh_db):
    # odd cent → remainder goes to first party
    lines = compute_net_lines(fresh_db, 1_000_001, SPLIT)
    assert sum(l["amount_cents"] for l in lines) == 1_000_001
    assert lines[0]["amount_cents"] == 500_001  # platform gets the extra cent


def _commit_to_closed_won(fresh_db, gross_cents=500_000):
    """Walk a lane to closed_won, return (lane_id, broker_id, transition_id)."""
    lane_id, _ = _lane(fresh_db)
    broker_id = _broker(fresh_db)
    assign_broker(fresh_db, lane_id, broker_id, actor="admin_1")
    transition(fresh_db, lane_id, "working", broker_id, "no_contact", actor=broker_id)
    transition(fresh_db, lane_id, "quoted", broker_id, "price", actor=broker_id)
    transition(fresh_db, lane_id, "committed", broker_id, "qualified", actor=broker_id)
    tid = transition(
        fresh_db, lane_id, "closed_won", broker_id, "funded", actor=broker_id,
        gross_amount_cents=gross_cents, split_config_id=SPLIT,
    )
    return lane_id, broker_id, tid


def test_post_commission_idempotent(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"), \
         patch("src.services.commission_ledger.emit_event"):
        lane_id, broker_id, tid = _commit_to_closed_won(fresh_db, 500_000)
        e1 = post_commission(fresh_db, tid, 500_000, SPLIT)
        e2 = post_commission(fresh_db, tid, 500_000, SPLIT)  # replay
    assert e1 is not None
    assert e2 is None
    count = fresh_db.execute(
        text("SELECT COUNT(*) FROM commission_ledger WHERE trigger_transition_id = CAST(:t AS uuid)"),
        {"t": tid},
    ).scalar()
    assert count == 1


def test_dispute_and_offset(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"), \
         patch("src.services.commission_ledger.emit_event"):
        lane_id, broker_id, tid = _commit_to_closed_won(fresh_db, 400_000)
        entry_id = post_commission(fresh_db, tid, 400_000, SPLIT)
        dispute_entry(fresh_db, entry_id, actor="admin_1")
        offset_id = post_offset(fresh_db, entry_id, actor="admin_1")

    orig = fresh_db.execute(
        text("SELECT status, net_lines FROM commission_ledger WHERE entry_id = CAST(:e AS uuid)"),
        {"e": entry_id},
    ).fetchone()
    offset = fresh_db.execute(
        text("SELECT status, net_lines FROM commission_ledger WHERE entry_id = CAST(:e AS uuid)"),
        {"e": offset_id},
    ).fetchone()
    assert orig.status == "disputed"
    # original net_lines untouched (still positive); offset negates them
    orig_broker = {l["party"]: l["amount_cents"] for l in orig.net_lines}["broker"]
    off_broker = {l["party"]: l["amount_cents"] for l in offset.net_lines}["broker"]
    assert off_broker == -orig_broker


# ---------------------------------------------------------------------------
# Consumers
# ---------------------------------------------------------------------------

# test_consumer_verdict_creates_lane / test_consumer_ignores_non_loan_lane_verdict
# removed: handle_truth_verdict was dropped in the spec-v6 refactor (lanes are
# created from broker events by property_id — "There is no CDS/truth_verdict →
# lane path", see src/consumers/loan_lane_consumers.py docstring). The tests
# outlived the code and broke collection of this module on dev.


def test_lane_closer_funds_on_closed_won(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        handle_lane_closer(fresh_db, _event_row(None, {"to_state": "closed_won", "lane_id": lane_id}))
    assert _fetch_lane(fresh_db, lane_id).outcome == "funded"


def test_lane_closer_ignores_intermediate_state(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        handle_lane_closer(fresh_db, _event_row(None, {"to_state": "quoted", "lane_id": lane_id}))
    # intermediate state must NOT move the lane (spec D9)
    lane = _fetch_lane(fresh_db, lane_id)
    assert lane.current_stage == "entered"
    assert lane.outcome == "open"


def test_lane_closer_kills_on_closed_lost(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        handle_lane_closer(fresh_db, _event_row(None, {"to_state": "closed_lost", "lane_id": lane_id}))
    assert _fetch_lane(fresh_db, lane_id).outcome == "dead"


def test_commission_poster_posts_on_closed_won(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"), \
         patch("src.services.broker_state_machine.emit_event"), \
         patch("src.services.commission_ledger.emit_event"):
        lane_id, broker_id, tid = _commit_to_closed_won(fresh_db, 300_000)
        handle_commission_poster(fresh_db, _event_row(None, {
            "to_state": "closed_won", "transition_id": tid,
            "gross_amount_cents": 300_000, "split_config_id": SPLIT,
        }))
    row = fresh_db.execute(
        text("SELECT gross_amount_cents FROM commission_ledger WHERE trigger_transition_id = CAST(:t AS uuid)"),
        {"t": tid},
    ).fetchone()
    assert row.gross_amount_cents == 300_000


# ---------------------------------------------------------------------------
# Fee gate
# ---------------------------------------------------------------------------

def test_fee_surfaces_disabled_by_default(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
    assert fee_surfaces_enabled(fresh_db, lane_id) is False


def test_fee_surfaces_enabled_when_flag_on(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        fresh_db.execute(
            text("UPDATE lanes SET fee_config_flag = true WHERE lane_id = CAST(:l AS uuid)"),
            {"l": lane_id},
        )
    assert fee_surfaces_enabled(fresh_db, lane_id) is True


# ---------------------------------------------------------------------------
# WS-A remainder: claim / pool / staleness / lender tracking
# ---------------------------------------------------------------------------

def test_claim_lane_is_atomic_and_sets_assignment_metadata(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_a = _broker(fresh_db, "Alpha Broker")
        broker_b = _broker(fresh_db, "Beta Broker")

        assert claim_lane(fresh_db, lane_id, broker_a) is True
        assert claim_lane(fresh_db, lane_id, broker_b) is False

    lane = _fetch_lane(fresh_db, lane_id)
    assert str(lane.assigned_broker_id) == broker_a
    assert lane.claimed_at is not None
    assert lane.last_activity_at is not None


def test_reassign_and_release_lane_change_owner(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        broker_a = _broker(fresh_db, "Alpha Broker")
        broker_b = _broker(fresh_db, "Beta Broker")
        claim_lane(fresh_db, lane_id, broker_a)

        reassign_lane(fresh_db, lane_id, broker_b, actor="admin_1")
        assert str(_fetch_lane(fresh_db, lane_id).assigned_broker_id) == broker_b

        release_lane(fresh_db, lane_id, actor="admin_1")
        assert _fetch_lane(fresh_db, lane_id).assigned_broker_id is None


def test_get_pool_excludes_claimed_and_guess_leads(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_open, pid_open = _lane(fresh_db)
        lane_claimed, pid_claimed = _lane(fresh_db)
        lane_guess, pid_guess = _lane(fresh_db)

        prop_open = fresh_db.execute(
            text("SELECT property_id FROM prospects WHERE prospect_id = CAST(:p AS uuid)"),
            {"p": pid_open},
        ).scalar()
        _seed_score(fresh_db, prop_open, guess=False)

        prop_claimed = fresh_db.execute(
            text("SELECT property_id FROM prospects WHERE prospect_id = CAST(:p AS uuid)"),
            {"p": pid_claimed},
        ).scalar()
        _seed_score(fresh_db, prop_claimed, guess=False)

        prop_guess = fresh_db.execute(
            text("SELECT property_id FROM prospects WHERE prospect_id = CAST(:p AS uuid)"),
            {"p": pid_guess},
        ).scalar()
        _seed_score(fresh_db, prop_guess, guess=True)
        broker = _broker(fresh_db, "Pool Broker")
        claim_lane(fresh_db, lane_claimed, broker)

        pool = get_pool(fresh_db, limit=10)

    assert len(pool) == 1
    row = pool[0]
    assert row["lane_id"] == lane_open
    assert "owner_name" not in row
    assert "distress_reason" in row and row["distress_reason"]
    assert row["is_guess_lead"] is False


def test_get_stale_lanes_uses_activity_window(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        fresh_id, _ = _lane(fresh_db)
        stale_id, _ = _lane(fresh_db)
        fresh_db.execute(
            text("""
                UPDATE lanes
                   SET last_activity_at = NOW() - INTERVAL '31 days'
                 WHERE lane_id = CAST(:lid AS uuid)
            """),
            {"lid": stale_id},
        )
        stale = get_stale_lanes(fresh_db, days=30)

    ids = {row["lane_id"] for row in stale}
    assert stale_id in ids
    assert fresh_id not in ids


def test_set_lane_lender_requires_active_and_cleared_lender(fresh_db):
    with patch("src.services.loan_lane_service.emit_event"):
        lane_id, _ = _lane(fresh_db)
        good = _lender(fresh_db, "Good Lender", cleared=True, active=True)
        bad = _lender(fresh_db, "Bad Lender", cleared=False, active=True)

        set_lane_lender(fresh_db, lane_id, good, actor="admin_1")
        assert str(_fetch_lane(fresh_db, lane_id).lender_id) == good

        with pytest.raises(ValueError):
            set_lane_lender(fresh_db, lane_id, bad, actor="admin_1")
