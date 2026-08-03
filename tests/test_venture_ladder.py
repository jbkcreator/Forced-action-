"""
Tests for the autonomous venture ladder (CLONE-v2.2 / CL4).

Covers the four cases the brief requires — successful advancement, blocked
advancement, failed presell validation, auto-double triggering — plus the
auto-double negatives that actually bite in production: below the minimum
sample, inside the cooldown, at the ceiling cap, and twice in a day.

The pure-config and gate-colouring tests need no database. The rest use
`fresh_db` (real Postgres, rolled back per test) because the ladder's queries
are Postgres-specific: JSONB operators, `make_interval`, and
`jsonb_array_elements_text` have no SQLite equivalent.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest
from sqlalchemy import text

from config.venture_ladder import (
    AUTO_DOUBLE_COOLDOWN_DAYS,
    AUTO_DOUBLE_MAX_CEILING,
    CLONE_PACK_IO,
    AUTO_DOUBLE_MIN_SAMPLE,
    AUTO_DOUBLE_MULTIPLIER,
    AUTO_DOUBLE_REPLY_RATE_PCT,
    EVIDENCE_MARKET_SCORE,
    EVIDENCE_PRESELL_COMMITMENT,
    EVIDENCE_SCRAPE_SAMPLE,
    LADDER_STAGES,
    PRESELL_GATED_TRANSITIONS,
    PRESELL_MIN_AMOUNT_CENTS,
    PRESELL_MIN_COMMITMENTS,
    STAGE_GATES,
    TERMINAL_STAGE,
    next_stage,
    presell_required,
    validate_ladder_config,
)
from config.venture_template import REQUIRED_SIGNAL_TYPES
from src.services import venture_ladder
from src.services.venture_ladder import _gate_color

# ── pure config ──────────────────────────────────────────────────────────────


def test_ladder_config_is_internally_consistent():
    assert validate_ladder_config() == []


def test_every_stage_has_a_gate_entry_and_terminal_has_none():
    for stage in LADDER_STAGES:
        assert stage in STAGE_GATES
    assert STAGE_GATES[TERMINAL_STAGE] == {}


def test_ladder_is_linear_and_terminates():
    stage = LADDER_STAGES[0]
    visited = [stage]
    while (stage := next_stage(stage)) is not None:
        visited.append(stage)
    assert tuple(visited) == LADDER_STAGES


def test_every_gate_declares_no_metric_behavior():
    """ADR 0006 in test form: the existing expansion-gate machine is inert
    because a gate with no metric silently became red. A gate that does not say
    what a missing value means is the same bug waiting to happen."""
    for stage, gates in STAGE_GATES.items():
        for name, cfg in gates.items():
            assert cfg.get("no_metric_behavior") in {"green", "yellow", "red"}, (
                f"{stage}.{name} does not declare no_metric_behavior"
            )


def test_presell_gated_transitions_are_real_ladder_steps():
    for from_stage, to_stage in PRESELL_GATED_TRANSITIONS:
        assert next_stage(from_stage) == to_stage
        assert presell_required(from_stage, to_stage)


def test_presell_is_not_required_on_ungated_steps():
    assert not presell_required("radar", "probe")
    assert not presell_required("spin_up", TERMINAL_STAGE)


# ── gate colouring ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value,expected",
    [(70.0, "green"), (60.0, "green"), (50.0, "yellow"), (10.0, "red")],
)
def test_gate_color_higher_is_better(value, expected):
    cfg = {
        "threshold": 60.0, "yellow_floor": 45.0,
        "direction": "higher_is_better", "no_metric_behavior": "red",
    }
    assert _gate_color(cfg, value) == (expected, False)


@pytest.mark.parametrize(
    "value,expected",
    [(5.0, "green"), (10.0, "green"), (15.0, "yellow"), (30.0, "red")],
)
def test_gate_color_lower_is_better(value, expected):
    cfg = {
        "threshold": 10.0, "yellow_floor": 20.0,
        "direction": "lower_is_better", "no_metric_behavior": "green",
    }
    assert _gate_color(cfg, value) == (expected, False)


def test_gate_color_binary():
    cfg = {"threshold": 1.0, "direction": "binary", "no_metric_behavior": "red"}
    assert _gate_color(cfg, 1.0) == ("green", False)
    assert _gate_color(cfg, 0.0) == ("red", False)


def test_missing_metric_uses_declared_behavior_and_is_flagged_imputed():
    red_cfg = {"threshold": 1.0, "direction": "binary", "no_metric_behavior": "red"}
    green_cfg = {"threshold": 10.0, "direction": "lower_is_better", "no_metric_behavior": "green"}
    assert _gate_color(red_cfg, None) == ("red", True)
    assert _gate_color(green_cfg, None) == ("green", True)


# ── DB fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def ladder_db(fresh_db):
    """`fresh_db`, skipped unless the CL4 migration has been applied."""
    exists = fresh_db.execute(text("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name IN ('venture_ladder_events', 'venture_ladder_evidence')
    """)).scalar_one()
    if exists < 2:
        pytest.skip(
            "CL4 tables absent — run "
            "`PYTHONPATH=. python migrations/apply_cl4_venture_ladder.py` first"
        )
    return fresh_db


@pytest.fixture
def stub_venture(ladder_db):
    """An inactive stub venture with one county, at the radar rung.

    is_active=false is what a radar candidate looks like: the CL3 resolver falls
    back to env settings for an inactive venture, so an unproven candidate
    cannot govern real sends.
    """
    key = f"test_ladder_{uuid.uuid4().hex[:10]}"
    county_id = f"{key}_county"

    ladder_db.execute(
        text("""
            INSERT INTO ventures (
                venture_key, display_name, brand_name, state,
                relay_daily_ceiling, ladder_stage, is_active
            ) VALUES (:key, :name, :brand, 'FL', 20, 'radar', false)
        """),
        {"key": key, "name": "Test Ladder Venture", "brand": "Test Ladder"},
    )
    ladder_db.execute(
        text("""
            INSERT INTO counties (
                county_id, display_name, venture_key, zip_prefixes, is_active
            ) VALUES (:county_id, :name, :key, CAST(:zips AS jsonb), true)
        """),
        {
            "county_id": county_id,
            "name": "Test Ladder County",
            "key": key,
            # Deliberately outside any real ZIP range so the radar rung's
            # county-overlap gate cannot collide with a live venture.
            "zips": json.dumps(["00951", "00952"]),
        },
    )
    ladder_db.flush()
    return key


def _set_stage(db, venture_key: str, stage: str) -> None:
    db.execute(
        text("UPDATE ventures SET ladder_stage = :stage WHERE venture_key = :key"),
        {"stage": stage, "key": venture_key},
    )


def _seed_market_score(db, venture_key: str, score: float = 80.0) -> None:
    venture_ladder.record_evidence(
        db, venture_key,
        evidence_type=EVIDENCE_MARKET_SCORE,
        stage="radar",
        payload={"score": score},
        source_ref="test-market-score",
        verified=True,
        recorded_by="test",
    )


def _seed_presell(
    db,
    venture_key: str,
    *,
    count: int,
    per_cents: int,
    verified: bool = True,
    customer_id: Optional[str] = None,
) -> None:
    """Seed `count` deposits. Distinct customers unless `customer_id` pins them
    all to one buyer, which is how the dedup rule is exercised."""
    for index in range(count):
        venture_ladder.record_evidence(
            db, venture_key,
            evidence_type=EVIDENCE_PRESELL_COMMITMENT,
            stage="probe",
            payload={
                "kind": "deposit",
                "amount_cents": per_cents,
                "stripe_customer_id": customer_id or f"cus_test_{venture_key}_{index}",
            },
            source_ref=f"pi_test_{index}",
            verified=verified,
            recorded_by="test",
        )


def _seed_traffic(db, venture_key: str, *, sends: int, replies: int, cell_id: str = "founder_tier_blitz") -> None:
    """Dispatched queue rows plus matching drafts.

    Both are needed: cell_reply_rates() counts a draft only when its thread has
    a dispatched relay_approval_queue row, so drafts alone yield no sends.
    """
    now = datetime.now(timezone.utc)
    queue_rows = []
    draft_rows = []
    for n in range(sends):
        thread_id = f"OPP-TEST-{venture_key}-{n:05d}"
        dispatched_at = now - timedelta(days=1, minutes=n)
        queue_rows.append({
            "idempotency_key": f"test-{venture_key}-{n}",
            "venture_key": venture_key,
            "batch_id": f"test-batch-{n % 3}",
            "thread_id": thread_id,
            "channel": "email",
            "recipient": f"t{n}@test.invalid",
            "payload": json.dumps({"subject": "t"}),
            "dispatched_at": dispatched_at,
        })
        draft_rows.append({
            "draft_id": str(uuid.uuid4()),
            "opportunity_thread_id": thread_id,
            "venture_key": venture_key,
            "cell_id": cell_id,
            "created_at": dispatched_at,
            "replied_at": dispatched_at + timedelta(hours=1) if n < replies else None,
        })

    db.execute(
        text("""
            INSERT INTO relay_approval_queue (
                idempotency_key, venture_key, batch_id, thread_id, channel,
                recipient, payload, status, dispatched_at
            ) VALUES (
                :idempotency_key, :venture_key, :batch_id, :thread_id, :channel,
                :recipient, CAST(:payload AS jsonb), 'sent', :dispatched_at
            )
        """),
        queue_rows,
    )
    # schema_version / published / is_followup are NOT NULL with Python-side
    # defaults only (no server_default), so a raw SQL insert must supply them.
    db.execute(
        text("""
            INSERT INTO outbound_drafts (
                draft_id, opportunity_thread_id, buyer_entity_id, venture_key,
                cell_id, offer, avenue, angle, subject, body, facts_used,
                source_refs, recommended_channel, confidence_score, status,
                schema_version, published, is_followup, created_at, replied_at
            ) VALUES (
                :draft_id, :opportunity_thread_id, 1, :venture_key,
                :cell_id, 'founder_tier', 'flippers', 'scarcity_seat_number',
                's', 'b', '[]'::jsonb, '[]'::jsonb, 'email', 80,
                'approved_pending_send', 1, false, false, :created_at, :replied_at
            )
        """),
        draft_rows,
    )
    db.flush()


# ── advancement ──────────────────────────────────────────────────────────────


def test_successful_advancement_radar_to_probe(ladder_db, stub_venture):
    _seed_market_score(ladder_db, stub_venture)

    evaluation = venture_ladder.evaluate(ladder_db, stub_venture)
    assert evaluation.current_stage == "radar"
    assert evaluation.next_stage == "probe"
    assert evaluation.blocked_reasons == ()
    assert evaluation.may_advance

    result = venture_ladder.advance(ladder_db, stub_venture, actor="test")
    assert result.current_stage == "probe"

    stored = ladder_db.execute(
        text("SELECT ladder_stage FROM ventures WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one()
    assert stored == "probe"


def test_advancement_writes_an_audit_row_with_gate_values(ladder_db, stub_venture):
    _seed_market_score(ladder_db, stub_venture, score=91.0)
    venture_ladder.advance(ladder_db, stub_venture, actor="test-actor")

    row = ladder_db.execute(
        text("""
            SELECT from_stage, to_stage, decision, gate_results, actor
            FROM venture_ladder_events WHERE venture_key = :k
        """),
        {"k": stub_venture},
    ).one()
    assert (row.from_stage, row.to_stage, row.decision) == ("radar", "probe", "advanced")
    assert row.actor == "test-actor"
    # The computed number is frozen into the audit row, so the decision is
    # reconstructable later without re-running the query.
    assert row.gate_results["market_score"]["value"] == 91.0
    assert row.gate_results["market_score"]["color"] == "green"


def test_blocked_advancement_leaves_the_stage_alone_and_is_audited(ladder_db, stub_venture):
    """No market-score evidence — the radar gate must refuse."""
    evaluation = venture_ladder.evaluate(ladder_db, stub_venture)
    assert evaluation.blocked_reasons
    assert any("market_score" in reason for reason in evaluation.blocked_reasons)
    assert not evaluation.may_advance

    result = venture_ladder.advance(ladder_db, stub_venture, actor="test")
    assert result.current_stage == "radar"

    stored = ladder_db.execute(
        text("SELECT ladder_stage FROM ventures WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one()
    assert stored == "radar"

    row = ladder_db.execute(
        text("""
            SELECT decision, from_stage, to_stage, blocked_reasons
            FROM venture_ladder_events WHERE venture_key = :k
        """),
        {"k": stub_venture},
    ).one()
    # A refusal is as auditable as a promotion, and does not move the venture.
    assert row.decision == "blocked"
    assert row.from_stage == row.to_stage == "radar"
    assert row.blocked_reasons


def test_force_advances_despite_red_gates_and_records_it(ladder_db, stub_venture):
    result = venture_ladder.advance(ladder_db, stub_venture, actor="ops", force=True)
    assert result.current_stage == "probe"

    row = ladder_db.execute(
        text("SELECT decision, actor FROM venture_ladder_events WHERE venture_key = :k"),
        {"k": stub_venture},
    ).one()
    assert row.decision == "advanced"
    # Never silent: a forced advance is attributed as forced.
    assert "forced" in row.actor


def test_terminal_stage_cannot_advance(ladder_db, stub_venture):
    _set_stage(ladder_db, stub_venture, TERMINAL_STAGE)
    evaluation = venture_ladder.evaluate(ladder_db, stub_venture)
    assert evaluation.next_stage is None
    assert evaluation.blocked_reasons == ()

    result = venture_ladder.advance(ladder_db, stub_venture, actor="test")
    assert result.current_stage == TERMINAL_STAGE
    assert ladder_db.execute(
        text("SELECT COUNT(*) FROM venture_ladder_events WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one() == 0


def test_unknown_venture_raises(ladder_db):
    with pytest.raises(LookupError):
        venture_ladder.evaluate(ladder_db, "no_such_venture_at_all")


def test_county_overlap_gate_blocks_a_territory_collision(ladder_db, stub_venture):
    """A second active venture claiming the same ZIP prefixes must block radar."""
    _seed_market_score(ladder_db, stub_venture)
    rival = f"rival_{uuid.uuid4().hex[:8]}"
    ladder_db.execute(
        text("""
            INSERT INTO ventures (venture_key, display_name, brand_name, is_active)
            VALUES (:key, 'Rival', 'Rival', true)
        """),
        {"key": rival},
    )
    ladder_db.execute(
        text("""
            INSERT INTO counties (
                county_id, display_name, venture_key, zip_prefixes, is_active
            ) VALUES (:county_id, 'Rival County', :key, CAST(:zips AS jsonb), true)
        """),
        {"county_id": f"{rival}_county", "key": rival, "zips": json.dumps(["00951"])},
    )
    ladder_db.flush()

    evaluation = venture_ladder.evaluate(ladder_db, stub_venture)
    assert any("county_overlap" in reason for reason in evaluation.blocked_reasons)


# ── presell gate ─────────────────────────────────────────────────────────────


def test_failed_presell_validation_blocks_probe_to_pilot(ladder_db, stub_venture):
    _set_stage(ladder_db, stub_venture, "probe")
    reasons = venture_ladder.presell_gate_blocked(ladder_db, stub_venture)
    assert reasons
    assert any("distinct verified customer" in reason for reason in reasons)

    evaluation = venture_ladder.evaluate(ladder_db, stub_venture)
    assert any("presell gate" in reason for reason in evaluation.blocked_reasons)


def test_presell_gate_ignores_unverified_commitments(ladder_db, stub_venture):
    """A hand-entered claim is not evidence. Only a machine-verified payment
    counts, which is what keeps the gate autonomous."""
    _seed_presell(
        ladder_db, stub_venture,
        count=PRESELL_MIN_COMMITMENTS + 5,
        per_cents=PRESELL_MIN_AMOUNT_CENTS,
        verified=False,
    )
    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)
    assert status.verified_count == 0
    assert status.verified_amount_cents == 0
    assert not status.satisfied
    assert venture_ladder.presell_gate_blocked(ladder_db, stub_venture)


def test_presell_gate_needs_both_count_and_amount(ladder_db, stub_venture):
    # Enough people, not enough money.
    _seed_presell(ladder_db, stub_venture, count=PRESELL_MIN_COMMITMENTS, per_cents=100)
    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)
    assert status.verified_count == PRESELL_MIN_COMMITMENTS
    assert not status.satisfied
    reasons = venture_ladder.presell_gate_blocked(ladder_db, stub_venture)
    assert any("committed" in reason for reason in reasons)


def test_presell_gate_rejects_a_kind_that_is_not_accepted(ladder_db, stub_venture):
    for index in range(PRESELL_MIN_COMMITMENTS):
        venture_ladder.record_evidence(
            ladder_db, stub_venture,
            evidence_type=EVIDENCE_PRESELL_COMMITMENT,
            stage="probe",
            payload={"kind": "letter_of_intent", "amount_cents": PRESELL_MIN_AMOUNT_CENTS},
            source_ref=f"loi_{index}",
            verified=True,
            recorded_by="test",
        )
    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)
    assert status.verified_count == 0
    assert any("not accepted evidence" in reason for reason in status.rejected)


def test_presell_gate_passes_on_verified_deposits(ladder_db, stub_venture):
    _seed_presell(
        ladder_db, stub_venture,
        count=PRESELL_MIN_COMMITMENTS,
        per_cents=(PRESELL_MIN_AMOUNT_CENTS // PRESELL_MIN_COMMITMENTS) + 1,
    )
    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)
    assert status.verified_count == PRESELL_MIN_COMMITMENTS
    assert status.satisfied
    assert venture_ladder.presell_gate_blocked(ladder_db, stub_venture) == []


def test_presell_gate_counts_distinct_customers_not_rows(ladder_db, stub_venture):
    """Five deposits from ONE buyer is not evidence of a market.

    The source_ref UNIQUE stops a webhook retry re-inserting the same
    PaymentIntent; it says nothing about one customer depositing five times.
    """
    _seed_presell(
        ladder_db, stub_venture,
        count=PRESELL_MIN_COMMITMENTS + 3,
        per_cents=PRESELL_MIN_AMOUNT_CENTS,
        customer_id="cus_one_enthusiast",
    )
    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)

    assert status.verified_count == 1
    # Only the first commitment's amount counts, so a single buyer cannot clear
    # the money threshold by depositing repeatedly either.
    assert status.verified_amount_cents == PRESELL_MIN_AMOUNT_CENTS
    assert not status.satisfied
    assert any("distinct buyers" in reason for reason in status.rejected)

    reasons = venture_ladder.presell_gate_blocked(ladder_db, stub_venture)
    assert any("distinct verified customer" in reason for reason in reasons)


def test_presell_gate_excludes_another_ventures_existing_subscriber(
    ladder_db, stub_venture
):
    """A deposit from someone who already pays another venture is the existing
    book buying again, not new demand."""
    rival = f"rival_{uuid.uuid4().hex[:8]}"
    rival_county = f"{rival}_county"
    ladder_db.execute(
        text("""
            INSERT INTO ventures (venture_key, display_name, brand_name, is_active)
            VALUES (:key, 'Rival', 'Rival', true)
        """),
        {"key": rival},
    )
    ladder_db.execute(
        text("""
            INSERT INTO counties (
                county_id, display_name, venture_key, zip_prefixes, is_active
            ) VALUES (:cid, 'Rival County', :key, '[]'::jsonb, true)
        """),
        {"cid": rival_county, "key": rival},
    )
    from src.core.models import Subscriber

    existing = Subscriber(
        stripe_customer_id="cus_already_paying",
        tier="pro", vertical="investor", county_id=rival_county,
        status="active", email="existing@test.invalid",
    )
    ladder_db.add(existing)
    ladder_db.flush()

    # Four genuinely new buyers plus one who already pays the rival venture.
    _seed_presell(
        ladder_db, stub_venture,
        count=PRESELL_MIN_COMMITMENTS - 1,
        per_cents=PRESELL_MIN_AMOUNT_CENTS,
    )
    venture_ladder.record_evidence(
        ladder_db, stub_venture,
        evidence_type=EVIDENCE_PRESELL_COMMITMENT, stage="probe",
        payload={
            "kind": "deposit",
            "amount_cents": PRESELL_MIN_AMOUNT_CENTS,
            "stripe_customer_id": "cus_already_paying",
        },
        source_ref="pi_existing_subscriber", verified=True, recorded_by="test",
    )
    ladder_db.flush()

    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)
    assert status.verified_count == PRESELL_MIN_COMMITMENTS - 1
    assert not status.satisfied
    assert any("existing book" in reason for reason in status.rejected)


def test_presell_gate_does_not_exclude_this_ventures_own_subscriber(
    ladder_db, stub_venture
):
    """The exclusion is scoped to OTHER ventures. Someone who only ever
    subscribed to this venture is still valid demand for it."""
    own_county = ladder_db.execute(
        text("SELECT county_id FROM counties WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one()
    from src.core.models import Subscriber

    ladder_db.add(Subscriber(
        stripe_customer_id="cus_own_customer",
        tier="pro", vertical="investor", county_id=own_county,
        status="active", email="own@test.invalid",
    ))
    ladder_db.flush()

    venture_ladder.record_evidence(
        ladder_db, stub_venture,
        evidence_type=EVIDENCE_PRESELL_COMMITMENT, stage="probe",
        payload={
            "kind": "deposit",
            "amount_cents": PRESELL_MIN_AMOUNT_CENTS,
            "stripe_customer_id": "cus_own_customer",
        },
        source_ref="pi_own", verified=True, recorded_by="test",
    )
    ladder_db.flush()

    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)
    assert status.verified_count == 1


def test_presell_commitments_without_a_customer_id_fall_back_to_contact_ref(
    ladder_db, stub_venture
):
    """A missing stripe_customer_id must not collapse every such row into one
    bucket keyed on None."""
    for index in range(PRESELL_MIN_COMMITMENTS):
        venture_ladder.record_evidence(
            ladder_db, stub_venture,
            evidence_type=EVIDENCE_PRESELL_COMMITMENT, stage="probe",
            payload={
                "kind": "deposit",
                "amount_cents": (PRESELL_MIN_AMOUNT_CENTS // PRESELL_MIN_COMMITMENTS) + 1,
                "contact_ref": f"contact-{index}",
            },
            source_ref=f"pi_no_customer_{index}", verified=True, recorded_by="test",
        )
    ladder_db.flush()

    status = venture_ladder.presell_gate_status(ladder_db, stub_venture)
    assert status.verified_count == PRESELL_MIN_COMMITMENTS
    assert status.satisfied


def test_evidence_source_ref_is_idempotent(ladder_db, stub_venture):
    """A Stripe webhook retry must not inflate a presell count."""
    first = venture_ladder.record_evidence(
        ladder_db, stub_venture,
        evidence_type=EVIDENCE_PRESELL_COMMITMENT, stage="probe",
        payload={"kind": "deposit", "amount_cents": 50_000},
        source_ref="pi_retry_me", verified=True, recorded_by="test",
    )
    second = venture_ladder.record_evidence(
        ladder_db, stub_venture,
        evidence_type=EVIDENCE_PRESELL_COMMITMENT, stage="probe",
        payload={"kind": "deposit", "amount_cents": 50_000},
        source_ref="pi_retry_me", verified=True, recorded_by="test",
    )
    assert first is True
    assert second is False
    assert venture_ladder.presell_gate_status(ladder_db, stub_venture).verified_count == 1


def test_evidence_without_source_ref_is_repeatable(ladder_db, stub_venture):
    """NULL source_refs are distinct in Postgres, which is correct for
    repeatable evidence like a fresh scrape sample."""
    for _ in range(3):
        assert venture_ladder.record_evidence(
            ladder_db, stub_venture,
            evidence_type=EVIDENCE_SCRAPE_SAMPLE, stage="probe",
            payload={"rows": 10}, verified=True, recorded_by="test",
        ) is True


# ── auto-double ──────────────────────────────────────────────────────────────


def test_auto_double_fires_above_the_reply_rate_threshold(ladder_db, stub_venture):
    sends = AUTO_DOUBLE_MIN_SAMPLE
    replies = int(sends * (AUTO_DOUBLE_REPLY_RATE_PCT + 5) / 100)
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=replies)

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert result.fired
    assert result.previous_ceiling == 20
    assert result.new_ceiling == 20 * AUTO_DOUBLE_MULTIPLIER

    stored = ladder_db.execute(
        text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one()
    assert stored == 20 * AUTO_DOUBLE_MULTIPLIER


def test_auto_double_audits_the_decision(ladder_db, stub_venture):
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))
    venture_ladder.maybe_auto_double(ladder_db, stub_venture)

    row = ladder_db.execute(
        text("""
            SELECT decision, gate_results FROM venture_ladder_events
            WHERE venture_key = :k AND decision = 'auto_double'
        """),
        {"k": stub_venture},
    ).one()
    assert row.gate_results["scope"] == "venture"
    assert row.gate_results["previous_ceiling"] == 20
    assert row.gate_results["new_ceiling"] == 40


def test_auto_double_flushes_the_venture_config_cache(ladder_db, stub_venture):
    """A raised ceiling must take effect immediately.

    ventures is read through a 5-minute cache; without the flush the new ceiling
    silently does not apply for up to 5 minutes and the venture under-sends
    exactly when it has earned the right to send more.
    """
    from src.utils import venture_config

    # The resolver only reads ACTIVE rows — an inactive radar candidate
    # deliberately falls through to env settings, so a ceiling change on one
    # would be invisible to it by design. Only a live venture has a cache entry
    # worth invalidating.
    ladder_db.execute(
        text("UPDATE ventures SET is_active = true WHERE venture_key = :k"),
        {"k": stub_venture},
    )
    ladder_db.flush()
    venture_config.invalidate_cache(stub_venture)

    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    # Warm the cache with the pre-double ceiling.
    before = venture_config.get_venture_config(stub_venture, session=ladder_db)
    assert before.relay_daily_ceiling == 20

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert result.fired

    after = venture_config.get_venture_config(stub_venture, session=ladder_db)
    assert after.relay_daily_ceiling == 20 * AUTO_DOUBLE_MULTIPLIER


def test_auto_double_cache_flush_failure_does_not_lose_the_ceiling(
    ladder_db, stub_venture, monkeypatch
):
    """A cache-flush problem must never roll back a ceiling change that already
    succeeded — the write is the important half."""
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    def _boom(_key=None):
        raise RuntimeError("cache backend unavailable")

    monkeypatch.setattr("src.utils.venture_config.invalidate_cache", _boom)

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert result.fired
    assert ladder_db.execute(
        text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one() == 20 * AUTO_DOUBLE_MULTIPLIER


def test_auto_double_does_not_fire_below_minimum_sample(ladder_db, stub_venture):
    """8% of a dozen sends is one reply — noise, not signal."""
    sends = AUTO_DOUBLE_MIN_SAMPLE - 1
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=sends)  # 100% reply rate

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert not result.fired
    assert "sample too small" in result.reason
    assert ladder_db.execute(
        text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one() == 20


def test_auto_double_does_not_fire_at_or_below_threshold(ladder_db, stub_venture):
    sends = AUTO_DOUBLE_MIN_SAMPLE
    replies = int(sends * AUTO_DOUBLE_REPLY_RATE_PCT / 100)  # exactly 8%
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=replies)

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert not result.fired
    assert "does not exceed" in result.reason


def test_auto_double_respects_the_cooldown(ladder_db, stub_venture):
    """Doubling twice in a week on a warming domain is how a sender gets
    blacklisted, and lowering the number back does not undo it."""
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    first = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert first.fired

    second = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert not second.fired
    assert "cooldown" in second.reason
    assert ladder_db.execute(
        text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one() == first.new_ceiling


def test_auto_double_fires_again_once_the_cooldown_elapses(ladder_db, stub_venture):
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    first = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert first.fired

    later = datetime.now(timezone.utc) + timedelta(days=AUTO_DOUBLE_COOLDOWN_DAYS + 1)
    second = venture_ladder.maybe_auto_double(ladder_db, stub_venture, now=later)
    assert second.fired
    assert second.new_ceiling == first.new_ceiling * AUTO_DOUBLE_MULTIPLIER


def test_auto_double_is_bounded_by_the_max_ceiling(ladder_db, stub_venture):
    ladder_db.execute(
        text("UPDATE ventures SET relay_daily_ceiling = :c WHERE venture_key = :k"),
        {"c": AUTO_DOUBLE_MAX_CEILING, "k": stub_venture},
    )
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert not result.fired
    assert "cap" in result.reason


def test_auto_double_clamps_rather_than_overshooting_the_cap(ladder_db, stub_venture):
    just_under = AUTO_DOUBLE_MAX_CEILING - 1
    ladder_db.execute(
        text("UPDATE ventures SET relay_daily_ceiling = :c WHERE venture_key = :k"),
        {"c": just_under, "k": stub_venture},
    )
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert result.fired
    assert result.new_ceiling == AUTO_DOUBLE_MAX_CEILING


def test_auto_double_blocked_by_send_failures(ladder_db, stub_venture):
    """A high reply rate next to a high failure rate means the list is dirty,
    not that the copy is good."""
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))
    # Flip a tenth of the dispatched rows to failed — far above the 2% cap.
    ladder_db.execute(
        text("""
            UPDATE relay_approval_queue SET status = 'failed'
            WHERE venture_key = :k
              AND id IN (
                  SELECT id FROM relay_approval_queue
                  WHERE venture_key = :k ORDER BY id LIMIT :n
              )
        """),
        {"k": stub_venture, "n": max(1, sends // 10)},
    )
    ladder_db.flush()

    result = venture_ladder.maybe_auto_double(ladder_db, stub_venture)
    assert not result.fired
    assert "failure rate" in result.reason


def test_cell_reply_rates_only_count_dispatched_drafts(ladder_db, stub_venture):
    """An approved-but-unsent draft must not inflate the number that decides
    how much mail goes out."""
    _seed_traffic(ladder_db, stub_venture, sends=10, replies=5)
    ladder_db.execute(
        text("""
            INSERT INTO outbound_drafts (
                draft_id, opportunity_thread_id, buyer_entity_id, venture_key,
                cell_id, offer, avenue, angle, subject, body, facts_used,
                source_refs, recommended_channel, confidence_score, status,
                schema_version, published, is_followup
            ) VALUES (
                :draft_id, 'OPP-NEVER-DISPATCHED', 1, :key,
                'founder_tier_blitz', 'founder_tier', 'flippers',
                'scarcity_seat_number', 's', 'b', '[]'::jsonb, '[]'::jsonb,
                'email', 80, 'approved_pending_send', 1, false, false
            )
        """),
        {"draft_id": str(uuid.uuid4()), "key": stub_venture},
    )
    ladder_db.flush()

    stats = venture_ladder.cell_reply_rates(ladder_db, stub_venture)
    assert stats["founder_tier_blitz"].sends == 10
    assert stats["founder_tier_blitz"].reply_rate_pct == 50.0


# ── per-cell auto-double ─────────────────────────────────────────────────────


def test_cell_auto_double_raises_only_that_cells_multiplier(ladder_db, stub_venture):
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(
        ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15),
        cell_id="founder_tier_blitz",
    )

    result = venture_ladder.maybe_auto_double_cell(
        ladder_db, stub_venture, "founder_tier_blitz"
    )
    assert result.fired
    assert result.previous_multiplier == 1
    assert result.new_multiplier == AUTO_DOUBLE_MULTIPLIER

    multipliers = venture_ladder.cell_production_multipliers(ladder_db, stub_venture)
    assert multipliers == {"founder_tier_blitz": AUTO_DOUBLE_MULTIPLIER}


def test_cell_auto_double_does_not_touch_the_venture_ceiling(ladder_db, stub_venture):
    """The cell rule is a production knob. There is exactly one send cap in this
    system and the mix shift happens underneath it."""
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    venture_ladder.maybe_auto_double_cell(ladder_db, stub_venture, "founder_tier_blitz")
    assert ladder_db.execute(
        text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
        {"k": stub_venture},
    ).scalar_one() == 20


def test_cell_auto_double_respects_its_own_cooldown(ladder_db, stub_venture):
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    assert venture_ladder.maybe_auto_double_cell(
        ladder_db, stub_venture, "founder_tier_blitz"
    ).fired
    second = venture_ladder.maybe_auto_double_cell(
        ladder_db, stub_venture, "founder_tier_blitz"
    )
    assert not second.fired
    assert "cooldown" in second.reason


def test_cell_auto_double_declines_an_unknown_cell(ladder_db, stub_venture):
    sends = AUTO_DOUBLE_MIN_SAMPLE
    _seed_traffic(ladder_db, stub_venture, sends=sends, replies=int(sends * 0.15))

    result = venture_ladder.maybe_auto_double_cell(ladder_db, stub_venture, "no_such_cell")
    assert not result.fired
    assert "sample too small" in result.reason


# ── Clone-Pack wiring ────────────────────────────────────────────────────────


def test_every_stage_declares_its_clone_pack_io():
    """Requirement: each stage is connected to Clone-Pack inputs and outputs.
    A new rung cannot be added without saying how it relates to the pack."""
    for stage in LADDER_STAGES:
        io = CLONE_PACK_IO[stage]
        assert io["inputs"], f"{stage} declares no Clone-Pack inputs"
        assert io["outputs"], f"{stage} declares no Clone-Pack outputs"


def test_spin_up_gates_on_live_clone_pack_completeness(ladder_db, stub_venture):
    """The spin_up rung must COMPUTE pack completeness, not read a claim.

    The stub has no county-specific source URLs and no cron line, so the pack
    has gaps and the gate must be red — even though nothing recorded that fact.
    """
    _set_stage(ladder_db, stub_venture, "spin_up")
    evaluation = venture_ladder.evaluate(ladder_db, stub_venture)

    gate = next(g for g in evaluation.gates if g.name == "clone_pack_complete")
    assert gate.value == 0.0
    assert gate.color == "red"
    # Computed, not imputed from a missing metric.
    assert gate.imputed is False
    assert any("clone_pack_complete" in reason for reason in evaluation.blocked_reasons)


def test_probe_coverage_agrees_with_clone_pack(ladder_db, stub_venture):
    """The probe gate and the Clone-Pack must never disagree about coverage —
    they read the same function."""
    from src.services.clone_pack import source_coverage

    _set_stage(ladder_db, stub_venture, "probe")
    venture_ladder.record_evidence(
        ladder_db, stub_venture,
        evidence_type=EVIDENCE_SCRAPE_SAMPLE, stage="probe",
        payload={"rows": 1}, verified=True, recorded_by="test",
    )

    evaluation = venture_ladder.evaluate(ladder_db, stub_venture)
    gate = next(g for g in evaluation.gates if g.name == "source_coverage_pct")

    coverage = source_coverage(ladder_db, stub_venture, template_county_id=None)
    total_missing = sum(len(gaps) for gaps in coverage.values())

    # The stub county has no sources at all, so every required type is missing
    # and the gate reports 0% — the same fact, from the same query.
    assert total_missing == len(REQUIRED_SIGNAL_TYPES)
    assert gate.value == 0.0
    assert gate.color == "red"


# ── reply-rate dual-write ────────────────────────────────────────────────────


def test_mark_replied_stamps_replied_at(ladder_db, stub_venture, monkeypatch):
    """Without this dual-write the auto-double rule has no reply rate to read."""
    from src.agents.cora import opportunity_state, store

    _seed_traffic(ladder_db, stub_venture, sends=3, replies=0)
    thread_id = f"OPP-TEST-{stub_venture}-00001"

    # The file-store transition is irrelevant here and writes outside the test
    # transaction, so it is stubbed out.
    monkeypatch.setattr(store, "transition_opportunity", lambda *a, **k: True)

    from contextlib import contextmanager

    @contextmanager
    def _session():
        yield ladder_db

    monkeypatch.setattr("src.core.database.get_db_context", _session)
    monkeypatch.setattr(ladder_db, "commit", ladder_db.flush)

    assert opportunity_state.mark_replied(thread_id) is True

    stamped = ladder_db.execute(
        text("""
            SELECT replied_at FROM outbound_drafts
            WHERE opportunity_thread_id = :t
        """),
        {"t": thread_id},
    ).scalar_one()
    assert stamped is not None


def test_mark_replied_survives_a_db_failure(monkeypatch):
    """A received reply must never be lost to a DB hiccup — the file-store
    transition is what callers depend on."""
    from src.agents.cora import opportunity_state, store

    monkeypatch.setattr(store, "transition_opportunity", lambda *a, **k: True)

    def _boom():
        raise RuntimeError("database unavailable")

    monkeypatch.setattr("src.core.database.get_db_context", lambda: _boom())

    assert opportunity_state.mark_replied("OPP-DOES-NOT-MATTER") is True
