from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
from sqlalchemy import text

from config.cora_cell_grid import get_cell
from config.venture_template import DEFAULT_VENTURE_KEY
from src.agents.cora import store
from src.agents.cora.subgraphs import outreach
from tests.agents.cora.conftest import compose_result
from tests.agents.cora.fixtures.whales import WHALES, facts_for

CELL_ID = "founder_tier_blitz"


def _run(whale, db, mock_claude, *, contact_email="prospect@example.com", **extra):
    mock_claude.return_value = compose_result(
        "Founding seat at Forced Action",
        f"Noticed your {whale['total_purchase_count']} purchases — reach out if interested.",
    )
    return outreach.run_outreach(
        {
            "buyer_entity": whale,
            "cell_id": CELL_ID,
            "facts_used": facts_for(whale),
            "contact_email": contact_email,
            "contact_phone": None,
            **extra,
        },
        db=db,
    )


# Acceptance item 1: 10 outreach drafts (9 valid whales + the 1 deliberately below-threshold one).
def test_ten_whales_each_produce_a_correct_or_correctly_rejected_outcome(not_suppressed_db, mock_claude):
    completed = 0
    for whale in WHALES:
        result = _run(whale, not_suppressed_db, mock_claude)
        if whale["confidence_score"] < 70:
            assert result["terminal_status"] == "rejected"
            assert result["reject_reason"] == "low_confidence"
        else:
            assert result["terminal_status"] == "completed", result
            completed += 1
    assert completed == 9  # every whale except the deliberately-below-floor one (WHALES[8])


# Acceptance item 3: correct offer x avenue x angle tags.
def test_draft_carries_correct_cell_tags(not_suppressed_db, mock_claude):
    whale = WHALES[0]
    result = _run(whale, not_suppressed_db, mock_claude)
    cell = get_cell(CELL_ID)
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["offer"] == cell["offer"]
    assert draft["avenue"] == cell["avenue"]
    assert draft["angle"] == cell["angle"]
    assert draft["draft_id"] == result["draft_id"]


# Acceptance item 4: correct opportunity_thread_id end-to-end.
def test_draft_carries_correct_opportunity_thread_id(not_suppressed_db, mock_claude):
    whale = WHALES[1]
    _run(whale, not_suppressed_db, mock_claude)
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["opportunity_thread_id"] == whale["opportunity_thread_id"]


# Acceptance item 5: working applicable links (booking real, payment None — documented).
def test_draft_link_resolution(not_suppressed_db, mock_claude, monkeypatch):
    from config.settings import get_settings
    monkeypatch.setattr(get_settings(), "demo_calendly_url", "https://calendly.com/test-rep", raising=False)

    whale = dict(WHALES[2])
    mock_claude.return_value = compose_result("subj", "body")
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": "hard_money_intro_lenders",
            "facts_used": facts_for(whale), "contact_email": "x@example.com", "contact_phone": None,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["booking_link"] == "https://calendly.com/test-rep"
    assert draft["payment_link"] is None


# Acceptance item 7: suppressed target -> no actionable draft.
def test_suppressed_target_produces_no_draft(suppressed_db, mock_claude):
    whale = WHALES[3]
    result = _run(whale, suppressed_db, mock_claude)
    assert result["terminal_status"] == "rejected"
    assert result["reject_reason"] == "suppressed"
    assert store.read_drafts(suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"]) == []


# Acceptance item 8: below-threshold Hunter record rejected.
def test_below_threshold_confidence_rejected(not_suppressed_db, mock_claude):
    whale = WHALES[8]
    result = _run(whale, not_suppressed_db, mock_claude)
    assert result["terminal_status"] == "rejected"
    assert result["reject_reason"] == "low_confidence"


# Acceptance item 9: duplicate event -> no duplicate output.
def test_duplicate_draft_attempt_is_rejected_not_duplicated(not_suppressed_db, mock_claude):
    whale = WHALES[4]
    first = _run(whale, not_suppressed_db, mock_claude)
    assert first["terminal_status"] == "completed"
    second = _run(whale, not_suppressed_db, mock_claude)
    assert second["terminal_status"] == "rejected"
    assert second["reject_reason"] == "duplicate_actionable"
    assert len(store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])) == 1


# Acceptance item 12: draft older than 72h is treated as expired at read time.
def test_draft_older_than_72h_is_expired(not_suppressed_db, mock_claude):
    from sqlalchemy import text

    whale = WHALES[5]
    result = _run(whale, not_suppressed_db, mock_claude)
    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert store.is_draft_expired(draft) is False

    backdated_at = store.parse_dt(draft["created_at"]) - timedelta(hours=73)
    not_suppressed_db.execute(
        text("UPDATE outbound_drafts SET created_at = :created_at WHERE draft_id = :draft_id"),
        {"created_at": backdated_at, "draft_id": result["draft_id"]},
    )
    refreshed = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert store.is_draft_expired(refreshed) is True

    expired_count = store.expire_stale_drafts(not_suppressed_db)
    assert expired_count == 1
    final = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert final["status"] == "expired"


# Reject path: empty facts_used never reaches compose.
def test_missing_facts_rejected_before_compose(not_suppressed_db, mock_claude):
    whale = WHALES[6]
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": CELL_ID, "facts_used": [],
            "contact_email": "x@example.com", "contact_phone": None,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "rejected"
    assert result["reject_reason"] == "facts_missing"
    mock_claude.assert_not_called()


def test_invalid_cell_id_fails(not_suppressed_db, mock_claude):
    whale = WHALES[7]
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": "not_a_real_cell", "facts_used": facts_for(whale),
            "contact_email": "x@example.com", "contact_phone": None,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "failed"
    assert result["reject_reason"] == "invalid_cell_id"


# ─────────────────────────────────────────────────────────────────────────────
# LEARN-v2.2 Layer 1 — price-variant wiring (ensure_price_band_experiment /
# get_price_variant / record_decision_snapshot called from the new
# price_variant node, gate -> price_variant -> compose -> resolve_links -> persist)
# ─────────────────────────────────────────────────────────────────────────────

def test_founder_tier_draft_persists_floor_price_with_flag_off(not_suppressed_db, mock_claude):
    """founder_tier has a real price band. PRICE_BAND_TESTING_ENABLED is
    False by default, so the draft must persist the floor price and no
    experiment_assignment_id (get_price_variant()'s documented flag-off
    contract) — and the LLM prompt must carry that price as an explicit
    fact, since its own system prompt forbids inventing numbers."""
    from src.services.price_assignment import PRICE_BANDS

    whale = WHALES[9]
    result = _run(whale, not_suppressed_db, mock_claude)
    assert result["terminal_status"] == "completed", result

    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["price_cents"] == PRICE_BANDS["founder_tier"]["floor"]
    assert draft["experiment_assignment_id"] is None

    prompt = mock_claude.call_args.kwargs["messages"][0]["content"]
    expected_price = f"${PRICE_BANDS['founder_tier']['floor'] / 100:,.0f}/mo"
    assert expected_price in prompt


def test_respa_excluded_offer_has_no_price_fact(not_suppressed_db, mock_claude):
    """hard_money_intro is RESPA-excluded (price_assignment.is_respa_excluded)
    — no price band exists to test, so the draft must persist NULL for both
    new columns and the prompt must carry no Price: line at all, exactly as
    it did before this feature existed."""
    whale = WHALES[9]
    mock_claude.return_value = compose_result("subj", "body")
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": "hard_money_intro_lenders",
            "facts_used": facts_for(whale), "contact_email": "x@example.com", "contact_phone": None,
        },
        db=not_suppressed_db,
    )
    assert result["terminal_status"] == "completed"

    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["price_cents"] is None
    assert draft["experiment_assignment_id"] is None

    prompt = mock_claude.call_args.kwargs["messages"][0]["content"]
    assert "Price:" not in prompt


def test_no_decision_snapshot_when_flag_off(not_suppressed_db, mock_claude):
    """get_price_variant()'s flag-off path returns control without ever
    calling assign_variant_by_thread() (price_assignment.assign_price()'s
    own documented flag-off contract) — so no AgentLaneExperimentAssignment
    row exists yet, and record_decision_snapshot() correctly finds nothing
    to snapshot. No assignment happened; there's nothing real to capture."""
    from sqlalchemy import select
    from src.core.models import ExperimentDecisionSnapshot

    whale = WHALES[9]
    _run(whale, not_suppressed_db, mock_claude)

    snapshot = not_suppressed_db.execute(
        select(ExperimentDecisionSnapshot).where(
            ExperimentDecisionSnapshot.opportunity_thread_id == whale["opportunity_thread_id"],
        )
    ).scalar_one_or_none()
    assert snapshot is None


def test_decision_snapshot_written_when_flag_on(not_suppressed_db, mock_claude, monkeypatch):
    """The price-band experiment is capped at 10% traffic by
    config/agent_lane_guardrails.py's agent_lane_experiment_traffic_cap
    (get_or_create_experiment enforces it regardless of what's requested) —
    so not every opportunity_thread_id lands in-test. OPP-TEST-PRICE-0005
    is deterministically in-window for test_name="price_band_founder_tier"
    (same md5 hash assign_variant_by_thread uses), confirmed directly
    rather than picking an arbitrary whale and hoping."""
    from sqlalchemy import select
    import src.services.price_assignment as pa_mod
    from src.core.models import ExperimentDecisionSnapshot

    monkeypatch.setattr(pa_mod, "PRICE_BAND_TESTING_ENABLED", True)

    whale = dict(WHALES[9], opportunity_thread_id="OPP-TEST-PRICE-0005")
    _run(whale, not_suppressed_db, mock_claude)

    snapshot = not_suppressed_db.execute(
        select(ExperimentDecisionSnapshot).where(
            ExperimentDecisionSnapshot.opportunity_thread_id == whale["opportunity_thread_id"],
        )
    ).scalar_one_or_none()
    assert snapshot is not None
    assert snapshot.offer == "founder_tier"
    assert snapshot.message_angle == get_cell(CELL_ID)["angle"]
    assert snapshot.chosen_action == f"draft_founder_tier_cell_{CELL_ID}"
    assert snapshot.assigned_variant in ("a", "b")


def test_price_variant_reuses_one_experiment_across_whales(not_suppressed_db, mock_claude):
    """Two different whales through the same offer must share one
    AgentLaneExperiment row (get_or_create_experiment's idempotency) — not
    a new experiment per draft."""
    from sqlalchemy import select
    from src.core.models import AgentLaneExperiment

    _run(WHALES[0], not_suppressed_db, mock_claude)
    _run(WHALES[9], not_suppressed_db, mock_claude)

    rows = not_suppressed_db.execute(
        select(AgentLaneExperiment).where(AgentLaneExperiment.test_name == "price_band_founder_tier")
    ).scalars().all()
    assert len(rows) == 1


# ── venture_key attribution (CLONE-v2.2 / CL4) ──────────────────────────────


def test_draft_is_attributed_to_the_targets_own_venture_not_the_default(
    not_suppressed_db, mock_claude
):
    """Issue: OutboundDraftRecord.venture_key defaulted to the primary venture
    because no draft-creation path ever passed one, so a second venture's
    drafts (and its reply rate — cell_reply_rates, which auto-double reads)
    were silently misattributed."""
    venture_key = f"test_outreach_{uuid.uuid4().hex[:8]}"
    county_id = f"{venture_key}_county"
    not_suppressed_db.execute(
        text("INSERT INTO ventures (venture_key, display_name, brand_name, is_active) "
             "VALUES (:k, 'Second Venture', 'Second Venture', true)"),
        {"k": venture_key},
    )
    not_suppressed_db.execute(
        text("INSERT INTO counties (county_id, display_name, venture_key, zip_prefixes, is_active) "
             "VALUES (:c, 'Second County', :k, '[]'::jsonb, true)"),
        {"c": county_id, "k": venture_key},
    )
    not_suppressed_db.flush()

    whale = dict(WHALES[2], county_id=county_id)
    result = _run(whale, not_suppressed_db, mock_claude)
    assert result["terminal_status"] == "completed"

    draft = store.read_drafts(not_suppressed_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["venture_key"] == venture_key


def test_second_venture_reply_rate_is_not_attributed_to_the_default_venture(
    not_suppressed_db, mock_claude
):
    """End-to-end: a second-venture target drafted, dispatched, and replied to
    must show up in ITS venture's cell_reply_rates() and not the default
    venture's — that per-venture number is what auto-double scales on."""
    from src.services import venture_ladder

    venture_key = f"test_outreach_{uuid.uuid4().hex[:8]}"
    county_id = f"{venture_key}_county"
    not_suppressed_db.execute(
        text("INSERT INTO ventures (venture_key, display_name, brand_name, is_active) "
             "VALUES (:k, 'Second Venture', 'Second Venture', true)"),
        {"k": venture_key},
    )
    not_suppressed_db.execute(
        text("INSERT INTO counties (county_id, display_name, venture_key, zip_prefixes, is_active) "
             "VALUES (:c, 'Second County', :k, '[]'::jsonb, true)"),
        {"c": county_id, "k": venture_key},
    )
    not_suppressed_db.flush()

    whale = dict(WHALES[3], county_id=county_id)
    result = _run(whale, not_suppressed_db, mock_claude)
    assert result["terminal_status"] == "completed"

    not_suppressed_db.execute(
        text("""
            INSERT INTO relay_approval_queue (
                idempotency_key, venture_key, thread_id, channel, recipient,
                payload, status, dispatched_at
            ) VALUES (
                :idem, :k, :thread_id, 'email', 'prospect@example.com',
                '{}'::jsonb, 'sent', now()
            )
        """),
        {
            "idem": f"idem-{venture_key}",
            "k": venture_key,
            "thread_id": whale["opportunity_thread_id"],
        },
    )
    not_suppressed_db.execute(
        text("UPDATE outbound_drafts SET replied_at = now() WHERE draft_id = :d"),
        {"d": result["draft_id"]},
    )
    not_suppressed_db.flush()

    stats = venture_ladder.cell_reply_rates(not_suppressed_db, venture_key)
    assert stats[CELL_ID].sends == 1
    assert stats[CELL_ID].replies == 1

    # Not attributed to the default venture: cell_reply_rates() is a real DB
    # query with unrelated production rows already in it, so assert directly
    # against this thread rather than the venture's aggregate count.
    leaked = not_suppressed_db.execute(
        text("""
            SELECT COUNT(*) FROM outbound_drafts
            WHERE opportunity_thread_id = :t AND venture_key = :default_key
        """),
        {"t": whale["opportunity_thread_id"], "default_key": DEFAULT_VENTURE_KEY},
    ).scalar_one()
    assert leaked == 0


@pytest.mark.integration
def test_real_claude_draft_grounds_facts_no_invention(fresh_db):
    """Best-effort no-hallucination check against the REAL Claude API — not mocked."""
    whale = WHALES[0]
    facts = facts_for(whale)
    result = outreach.run_outreach(
        {
            "buyer_entity": whale, "cell_id": CELL_ID, "facts_used": facts,
            "contact_email": "real-claude-test@example.com", "contact_phone": None,
        },
        db=fresh_db,
    )
    assert result["terminal_status"] == "completed", result
    draft = store.read_drafts(fresh_db, opportunity_thread_id=whale["opportunity_thread_id"])[0]
    assert draft["subject"]
    assert draft["body"]
    # Best-effort containment: at least one fact value shows up verbatim in the copy.
    combined = f"{draft['subject']} {draft['body']}"
    assert any(str(f["value"]) in combined for f in facts)
